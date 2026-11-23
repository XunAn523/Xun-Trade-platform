from __future__ import annotations

import json
import socket
import unittest
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread, current_thread
from types import SimpleNamespace
from unittest.mock import patch

from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2
from xuanling_tradecore.consumer.snapshot_index_v2 import ConsumerSnapshotVersionV2
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1
from xuanling_tradecore.persistence.frozen_consumer_snapshot import (
    FrozenConsumerSnapshot,
    build_frozen_consumer_snapshot,
)

import test_consumer_reactor_v2 as fixtures
import test_consumer_snapshot_index_v2 as snapshot_fixtures


@unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "requires Linux SO_PEERCRED")
class DeferredReactorTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ConsumerReactorV2Tests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        old = self.fixture.service
        self.listener = old._listener
        self.service = ConsumerUnixServiceV2(
            self.listener,
            request_reactor=True,
            deferred_read_workers=1,
            deferred_read_capacity=1,
            managed_components=old._managed_components,
        )
        self.service.set_request_trace(self.fixture.trace)
        self.fixture.service = self.service
        self.fixture.runner = Thread(target=self.service.serve_forever)
        self.entered, self.release = Event(), Event()
        self.addCleanup(self.release.set)
        self.calls = []

    @staticmethod
    def page(name):
        return encode_frame(
            MessageKind.REQUEST,
            7,
            {
                "message_type": "request",
                "schema_version": 2,
                "request_id": name,
                "operation": "ReadOrderSnapshotPage",
                "payload": {"page_cursor": "test-cursor"},
            },
        )

    def receive(self, peer):
        decoder = fixtures.ConsumerWireStreamDecoderV2()
        while True:
            chunk = peer.recv(65536)
            self.assertTrue(chunk, "closed before complete response")
            frames = decoder.feed(chunk)
            if frames:
                return frames[0].payload

    def handler(self, _session, request):
        self.calls.append((request["request_id"], current_thread().name))
        if request["request_id"] == "slow":
            self.entered.set()
            if not self.release.wait(3):
                raise RuntimeError("test release timed out")
        if request["request_id"] == "fail":
            raise OSError("private credential never disclosed")
        return {
            "message_type": "response",
            "schema_version": 2,
            "request_id": request["request_id"],
            "status": "COMPLETED",
            "payload": {},
        }

    def start(self):
        # Transport-only fixtures use a synthetic cursor. The cold-file test
        # below exercises the actual signed page guard instead.
        guard = patch.object(self.listener._gateway, "validate_snapshot_page_delivery")
        guard.start()
        self.addCleanup(guard.stop)
        patched = patch.object(self.listener, "_request_handler", side_effect=self.handler)
        patched.start()
        self.addCleanup(patched.stop)
        self.service.start()
        self.fixture.runner.start()

    def test_slow_read_does_not_stall_hot_request_and_excess_gets_backpressure(self):
        self.start()
        slow = self.fixture.connect()
        slow.sendall(self.page("slow"))
        self.assertTrue(self.entered.wait(2))
        excess = self.fixture.connect()
        excess.sendall(self.page("excess"))
        reply = self.receive(excess)
        self.assertEqual(("RATE_LIMITED", "READ_BACKPRESSURE"), (reply["code"], reply["detail"]))
        hot = self.fixture.connect()
        hot.sendall(self.fixture.frame("hot"))
        self.assertEqual("COMPLETED", self.receive(hot)["status"])
        self.assertFalse(self.release.is_set())
        self.release.set()
        self.assertEqual("slow", self.receive(slow)["request_id"])
        self.assertTrue(dict(self.calls)["slow"].startswith("consumer-cold-read-"))
        self.assertFalse(dict(self.calls)["hot"].startswith("consumer-cold-read-"))
        self.assertNotIn("excess", dict(self.calls))
        self.assertEqual(1, self.service.ingress_stats().deferred_reads_rejected)
        self.assertEqual(0, len(self.fixture.journal.scan().records))

    def test_pipelined_connection_preserves_order_and_complete_trace_pairs(self):
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.page("slow") + self.fixture.frame("after"))
        self.assertTrue(self.entered.wait(2))
        self.assertNotIn("after", dict(self.calls))
        self.release.set()
        decoder = fixtures.ConsumerWireStreamDecoderV2()
        replies = []
        while len(replies) < 2:
            replies.extend(decoder.feed(peer.recv(65536)))
        self.assertEqual(["slow", "after"], [r.payload["request_id"] for r in replies])
        self.service.stop()
        self.fixture.runner.join(3)
        self.fixture.trace.close()
        records = [json.loads(line) for line in self.fixture.trace_path.read_text().splitlines()]
        self.assertEqual(2, len(records))
        record = next(r for r in records if r["operation"] == "ReadOrderSnapshotPage")
        stages = [
            record[name]
            for name in (
                "deferred_enqueued_ns",
                "deferred_worker_started_ns",
                "schema_validated_ns",
                "handler_finished_ns",
                "response_encoded_ns",
                "deferred_delivery_ns",
                "send_finished_ns",
            )
        ]
        self.assertEqual(sorted(stages), stages)
        self.assertEqual(0, self.service.ingress_stats().deferred_reads_pending)

    def test_worker_error_returns_redacted_error_and_worker_survives(self):
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.page("fail"))
        response = self.receive(peer)
        self.assertEqual("INTERNAL_ERROR", response["code"])
        self.assertNotIn("credential", str(response))
        peer.sendall(self.page("healthy"))
        self.assertEqual("COMPLETED", self.receive(peer)["status"])

    def test_epoch_change_while_reading_cannot_disclose_old_result(self):
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.page("slow"))
        self.assertTrue(self.entered.wait(2))
        gateway = self.listener._gateway
        current = gateway._binding_snapshot()
        with patch.object(
            gateway,
            "_binding_snapshot",
            return_value=replace(current, authorization_epoch=current.authorization_epoch + 1),
        ):
            self.release.set()
            self.assertEqual(b"", peer.recv(65536))
        self.assertEqual(0, len(self.fixture.journal.scan().records))

    def test_stop_waits_for_read_before_closing_backing_components(self):
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.page("slow"))
        self.assertTrue(self.entered.wait(2))
        with patch.object(
            self.service._managed_components[0],
            "close",
            wraps=self.service._managed_components[0].close,
        ) as close:
            self.service.stop()
            self.assertFalse(close.called)
            self.assertTrue(self.fixture.runner.is_alive())
            self.release.set()
            self.fixture.runner.join(3)
            self.assertFalse(self.fixture.runner.is_alive())
            self.assertEqual(1, close.call_count)
        self.assertEqual(["slow"], [name for name, _ in self.calls])
        self.assertEqual(0, self.service.ingress_stats().deferred_reads_pending)

    def test_disconnect_during_read_does_not_retry_or_leak_session(self):
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.page("slow"))
        self.assertTrue(self.entered.wait(2))
        peer.close()
        self.release.set()
        # Shutdown is a completion barrier, not an arbitrary timing sleep.
        self.service.request_stop()
        self.fixture.runner.join(3)
        self.assertFalse(self.fixture.runner.is_alive())
        self.assertEqual(["slow"], [name for name, _ in self.calls])
        self.assertEqual({}, self.listener._gateway._sessions)
        self.assertEqual(0, self.service.ingress_stats().deferred_reads_pending)

    def test_stop_during_delivery_defers_component_close_until_reactor_barrier(self):
        self.start()
        original = self.listener.validate_deferred_delivery
        component = self.service._managed_components[0]
        entered = Event()

        def delivery(*args):
            self.assertEqual(0, self.service.ingress_stats().deferred_reads_pending)
            self.service.stop()
            self.assertFalse(close.called)
            entered.set()
            return original(*args)

        with (
            patch.object(component, "close", wraps=component.close) as close,
            patch.object(self.listener, "validate_deferred_delivery", side_effect=delivery),
        ):
            peer = self.fixture.connect()
            peer.sendall(self.page("healthy"))
            self.assertTrue(entered.wait(2))
            self.fixture.runner.join(3)
            self.assertFalse(self.fixture.runner.is_alive())
            self.assertEqual(1, close.call_count)

    def test_mutating_operations_are_never_selected_for_deferred_execution(self):
        from xuanling_tradecore.consumer.wire_v2 import decode_frame

        frame = decode_frame(self.fixture.frame("hot"))
        for operation in (
            "SubmitSignal",
            "CancelOrder",
            "SubscribeOrderEvents",
        ):
            with self.subTest(operation=operation):
                self.listener._parallel_read_operations = frozenset({operation})
                changed = replace(frame, payload={**frame.payload, "operation": operation})
                self.assertFalse(self.listener.can_defer_read(changed))

    def test_real_signed_cold_file_page_does_not_block_real_hot_query(self):
        gateway = self.listener._gateway
        index = gateway._paged_snapshots
        binding = gateway._binding_snapshot().accounts_for("test-consumer")[0]
        stream = index._registry(binding).current
        context = snapshot_fixtures.ConsumerSnapshotIndexV2Tests()
        context.setUp()
        position = ConsumerEventPositionV2(RedisStreamIdV1(3, 0), 0)
        rows = []
        for n in range(1, 4):
            order = context._order(n)
            rows.append(
                replace(
                    order,
                    journal_id=stream.journal_id,
                    account_id=binding.account_id,
                    consumer_trace=replace(
                        order.consumer_trace,
                        consumer_id=binding.consumer_id,
                        consumer_account_id=binding.consumer_account_id,
                        account_id=binding.account_id,
                    ),
                )
            )
        root = Path(self.fixture.directory.name) / "cold-page"
        build_frozen_consumer_snapshot(
            root=root,
            scope=(binding.consumer_id, binding.consumer_account_id, stream.journal_id),
            account_id=binding.account_id,
            position=position,
            orders=rows,
        )
        sources, opens, threads = [], [], []

        def capture(**_):
            opens.append(current_thread().name)
            source = FrozenConsumerSnapshot(root)
            sources.append(source)
            return ConsumerSnapshotVersionV2(position, (), order_source=source)

        index._reader = SimpleNamespace(capture_snapshot_version=capture)
        try:
            self.service.start()
            self.fixture.runner.start()
            creator = self.fixture.connect()
            creator.sendall(
                encode_frame(
                    MessageKind.REQUEST,
                    9,
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "request_id": "real-create",
                        "operation": "CreateOrderSnapshot",
                        "payload": {"consumer_account_id": "test-account", "page_size": 2},
                    },
                )
            )
            plan = self.receive(creator)
            self.assertEqual("COMPLETED", plan["status"])
            self.assertEqual(["consumer-cold-read-0"], opens)
            source = sources[0]
            original = source.read_orders

            def slow_file_read(start, end):
                threads.append(current_thread().name)
                self.entered.set()
                self.release.wait(3)
                return original(start, end)

            with patch.object(source, "read_orders", side_effect=slow_file_read):
                peer = self.fixture.connect()
                peer.sendall(
                    encode_frame(
                        MessageKind.REQUEST,
                        8,
                        {
                            "message_type": "request",
                            "schema_version": 2,
                            "request_id": "real-cold-page",
                            "operation": "ReadOrderSnapshotPage",
                            "payload": {"page_cursor": plan["payload"]["page_cursor"]},
                        },
                    )
                )
                self.assertTrue(self.entered.wait(2))
                hot = self.fixture.connect()
                hot.sendall(self.fixture.frame("real-hot"))
                self.assertEqual("COMPLETED", self.receive(hot)["status"])
                self.assertFalse(self.release.is_set())
                self.release.set()
                response = self.receive(peer)
                self.assertEqual("COMPLETED", response["status"])
                self.assertEqual(2, len(response["payload"]["orders"]))
                self.assertTrue(response["payload"]["has_more"])
                self.assertEqual(["consumer-cold-read-0"], threads)
        finally:
            self.release.set()
            self.service.request_stop()
            if self.fixture.runner.is_alive():
                self.fixture.runner.join(3)
            index.close()
