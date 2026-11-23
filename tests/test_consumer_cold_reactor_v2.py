from __future__ import annotations

import json
import socket
import time
import unittest
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from queue import Empty, Queue
from threading import Thread, current_thread
from unittest.mock import patch

from xuanling_tradecore.consumer.oms_operations_v2 import OrderBookConsumerOperationsV2
from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame
from xuanling_tradecore.oms.cold_intent import ColdIntentKey
from xuanling_tradecore.oms.retained_order_book import RetainedOrderBook
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex

import test_consumer_reactor_v2 as fixtures


class ControlledCold:
    def __init__(self, cold):
        self.cold = cold
        self.pending = Queue()

    def probe(self, key):
        return None  # exercise exact negatives as well as real cold hits

    def validate(self, result):
        return self.cold.validate(result)

    def resolve(self, key):
        future = Future()
        future.set_running_or_notify_cancel()
        self.pending.put((key, future))
        return future

    def next(self):
        return self.pending.get(timeout=2)

    def finish(self, pending, proof=None):
        key, future = pending
        future.set_result(proof if proof is not None else self.cold.resolve(key).result(3))

    def fail_remaining(self):
        while True:
            try:
                _, future = self.pending.get_nowait()
            except Empty:
                return
            future.set_exception(OSError("shutdown"))


@unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "requires Linux SO_PEERCRED")
class ColdReactorTests(unittest.TestCase):
    SCOPED_COLD_LOOKUP = False
    ISOLATED_SUBMIT = False
    OWNER_NAME = "test-sole-submit-reactor"

    def setUp(self):
        self.fixture = fixtures.ConsumerReactorV2Tests()
        self.fixture.setUp()
        original = self.fixture.service
        self.listener = original._listener
        self.gateway = self.listener._gateway
        self.cold = ColdIntentIndex(
            root=Path(self.fixture.directory.name) / "cold",
            journal_id=self.fixture.journal.journal_id,
            initialize=True,
            filter_bytes=64,
            scoped_lookup_epochs=self.SCOPED_COLD_LOOKUP,
        )
        self.control = ControlledCold(self.cold)
        self.wakeups = []
        self.book = RetainedOrderBook(journal=self.fixture.journal, cold_index=self.control)
        old_ops = self.gateway._operations
        self.operations = OrderBookConsumerOperationsV2(
            order_book=self.book,
            admission_context_for=old_ops._admission_context_for,
            snapshot_for=old_ops._snapshot_for,
            cold_resolver=self.control,
            on_execution_committed=lambda order: self.wakeups.append(
                (current_thread().name, order.order.oms_order_id)
            ),
        )
        self.gateway._operations = self.operations
        self.service = ConsumerUnixServiceV2(
            self.listener,
            request_reactor=True,
            cold_admission_capacity=8,
            submit_execution_capacity=2 if self.ISOLATED_SUBMIT else 0,
            managed_components=original._managed_components,
        )
        self.service.set_request_trace(self.fixture.trace)
        self.fixture.service = self.service
        self.errors = []

        def run():
            try:
                self.service.serve_forever()
            except BaseException as exc:
                self.errors.append(exc)

        self.fixture.runner = Thread(target=run, name="test-sole-submit-reactor")
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.service.request_stop()
        self.control.fail_remaining()
        self.fixture.tearDown()
        self.cold.close()
        self.assertEqual([], self.errors)

    def start(self, capacity=8):
        self.service._cold_admission_capacity = capacity
        self.service.start()
        self.fixture.runner.start()

    def submit(self, ref="same", quantity=1):
        peer = self.fixture.connect()
        peer.sendall(self.frame(ref, quantity))
        return peer

    @staticmethod
    def frame(ref="same", quantity=1):
        return encode_frame(
            MessageKind.REQUEST,
            17,
            {
                "message_type": "request",
                "schema_version": 2,
                "request_id": "cold-test",
                "operation": "SubmitSignal",
                "payload": {
                    "consumer_account_id": "test-account",
                    "strategy_id": "test-strategy",
                    "strategy_order_id": ref,
                    "trading_day": "2026-09-30",
                    "symbol": "TEST",
                    "market": "SIM",
                    "side": "BUY",
                    "price_type": "LIMIT",
                    "business_type": "CASH",
                    "quantity": quantity,
                    "protection_price_ticks": 1,
                },
            },
        )

    def receive(self, peer):
        decoder = fixtures.ConsumerWireStreamDecoderV2()
        while True:
            chunk = peer.recv(65536)
            self.assertTrue(chunk, "no complete response")
            frames = decoder.feed(chunk)
            if frames:
                return frames[0].payload

    def hot_barrier(self):
        peer = self.fixture.connect()
        peer.sendall(self.fixture.frame("hot-read"))
        self.assertEqual("COMPLETED", self.receive(peer)["status"])

    def archive(self, order_id):
        self.book.reject(oms_order_id=order_id)
        candidate = self.book.prepare_terminal_archive(order_id)
        receipt = self.cold.archive((candidate.encode(),)).result(3)
        self.book.retire_terminal(candidate, receipt)
        return candidate

    def key_in_stripe(self, *, collision):
        archived = ColdIntentKey("test-account", "test-strategy", "archived")
        bucket = self.cold._bits(archived)[0] % 65_536
        return next(
            f"new-{n}"
            for n in range(20_000)
            if (
                (
                    self.cold._bits(ColdIntentKey("test-account", "test-strategy", f"new-{n}"))[0]
                    % 65_536
                )
                == bucket
            )
            is collision
        )

    def test_pending_lookup_leaves_hot_requests_live_and_queue_full_is_explicit(self):
        self.start(capacity=1)
        first = self.submit()
        pending = self.control.next()
        self.assertEqual(0, len(self.fixture.journal.scan().records))
        self.hot_barrier()
        excess = self.submit("excess")
        error = self.receive(excess)
        self.assertEqual(
            ("RATE_LIMITED", "COLD_LOOKUP_BACKPRESSURE"), (error["code"], error["detail"])
        )
        self.control.finish(pending)
        self.assertEqual("ACCEPTED", self.receive(first)["status"])
        self.assertEqual(1, self.book.order_count("test-account"))

    def test_eight_same_key_negatives_resume_only_on_reactor_with_one_order_and_hold(self):
        self.start()
        peers = [self.submit() for _ in range(8)]
        pending = [self.control.next() for _ in peers]
        writes = []
        original = self.fixture.journal.append_batch

        def commit(*args, **kwargs):
            writes.append(current_thread().name)
            return original(*args, **kwargs)

        with patch.object(self.fixture.journal, "append_batch", side_effect=commit):
            for item in pending:
                self.control.finish(item)
            responses = [self.receive(peer) for peer in peers]
        self.assertTrue(all(r["status"] == "ACCEPTED" for r in responses))
        self.assertEqual(1, len({r["payload"]["oms_order_id"] for r in responses}))
        self.assertEqual([self.OWNER_NAME], writes)
        self.assertEqual(1, len(self.book.reservations("test-account")))
        self.assertEqual([(self.OWNER_NAME, responses[0]["payload"]["oms_order_id"])], self.wakeups)
        self.assertEqual(0, self.service.ingress_stats().cold_admissions_pending)

    def test_cold_terminal_retry_returns_original_id_without_second_commit_or_hold(self):
        self.start()
        first = self.submit()
        self.control.finish(self.control.next())
        response = self.receive(first)
        order_id = response["payload"]["oms_order_id"]
        self.archive(order_id)
        before = len(self.fixture.journal.scan().records)
        retry = self.submit()
        self.control.finish(self.control.next())
        replay = self.receive(retry)
        self.assertEqual(order_id, replay["payload"]["oms_order_id"])
        self.assertEqual(before, len(self.fixture.journal.scan().records))
        self.assertEqual((), self.book.reservations("test-account"))
        conflict = self.submit(quantity=2)
        self.control.finish(self.control.next())
        self.assertEqual("NOT_FOUND", self.receive(conflict)["code"])
        self.assertEqual(before + 1, len(self.fixture.journal.scan().records))

    def test_revoked_epoch_during_lookup_cannot_commit(self):
        self.start()
        peer = self.submit()
        pending = self.control.next()
        current = self.gateway._binding_snapshot()
        with patch.object(
            self.gateway,
            "_binding_snapshot",
            return_value=replace(current, authorization_epoch=current.authorization_epoch + 1),
        ):
            self.control.finish(pending)
            self.assertEqual("UNAUTHORIZED", self.receive(peer)["code"])
        self.assertEqual(0, len(self.fixture.journal.scan().records))

    def test_storage_failure_is_internal_error_not_absence_and_persistent_order_is_kept(self):
        self.start()
        peer = self.submit()
        _, future = self.control.next()
        future.set_exception(OSError("private credential"))
        error = self.receive(peer)
        self.assertEqual("INTERNAL_ERROR", error["code"])
        self.assertNotIn("credential", str(error))
        peer.sendall(self.fixture.frame("another-hot"))
        self.assertEqual("another-hot", self.receive(peer)["request_id"])
        self.assertEqual(0, len(self.fixture.journal.scan().records))

    def test_stale_negative_is_relooked_up_and_churn_has_three_attempt_limit(self):
        self.start()
        initial = self.submit("archived")
        self.control.finish(self.control.next())
        candidate = self.archive(self.receive(initial)["payload"]["oms_order_id"])
        before = len(self.fixture.journal.scan().records)
        peer = self.submit(self.key_in_stripe(collision=True))
        for _ in range(3):
            key, future = self.control.next()
            proof = self.cold.resolve(key).result(3)
            self.cold.archive((candidate.encode(),)).result(3)  # advance publication generation
            future.set_result(proof)
        self.assertEqual("RATE_LIMITED", self.receive(peer)["code"])
        self.assertEqual(before, len(self.fixture.journal.scan().records))

    def test_timeout_or_shutdown_drops_late_proof_without_creating_order(self):
        self.listener._connection_timeout_seconds = 0.05
        self.start()
        peer = self.submit()
        pending = self.control.next()
        self.assertEqual(b"", peer.recv(1))
        self.control.finish(pending)
        self.service.request_stop()
        self.fixture.runner.join(3)
        self.assertFalse(self.fixture.runner.is_alive())
        self.assertEqual(0, len(self.fixture.journal.scan().records))
        self.assertEqual({}, self.gateway._sessions)

    def test_one_request_session_survives_suspend_but_is_closed_after_reply(self):
        self.listener._max_requests_per_connection = 1
        self.start()
        peer = self.submit()
        self.control.finish(self.control.next())
        self.assertEqual("ACCEPTED", self.receive(peer)["status"])
        self.assertEqual(b"", peer.recv(1))
        # The response side is half-closed after the complete frame, while a
        # bounded read-side tombstone protects against response-less EOF/RST.
        # EOF alone does not prove the client has released its session.
        peer.close()
        deadline = time.monotonic() + 3
        while self.gateway._sessions and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual({}, self.gateway._sessions)

    def test_full_disconnect_before_proof_prevents_late_resume(self):
        self.start()
        peer = self.submit()
        pending = self.control.next()
        peer.close()
        self.control.finish(pending)
        # A subsequent request is a reactor barrier, not a timing sleep.
        self.hot_barrier()
        self.service.request_stop()
        self.fixture.runner.join(3)
        self.assertEqual(0, len(self.fixture.journal.scan().records))
        self.assertEqual({}, self.gateway._sessions)

    def test_write_half_close_is_not_misclassified_as_full_disconnect(self):
        self.start()
        peer = self.submit()
        pending = self.control.next()
        peer.shutdown(socket.SHUT_WR)
        self.control.finish(pending)
        self.assertEqual("ACCEPTED", self.receive(peer)["status"])
        self.assertEqual(b"", peer.recv(1))

    def test_resume_rebuilds_risk_context_instead_of_reusing_prelookup_snapshot(self):
        self.start()
        peer = self.submit()
        pending = self.control.next()
        original = self.operations._admission_context_for
        calls = []

        def expired(*args):
            calls.append(1)
            context = original(*args)
            return replace(
                context,
                risk_snapshot=replace(
                    context.risk_snapshot, captured_monotonic_ns=0, expires_monotonic_ns=1
                ),
            )

        with patch.object(self.operations, "_admission_context_for", side_effect=expired):
            self.control.finish(pending)
            self.assertEqual("INTERNAL_ERROR", self.receive(peer)["code"])
        self.assertEqual([1], calls)
        self.assertEqual(0, len(self.fixture.journal.scan().records))

    def test_generation_change_requeries_then_accepts_fresh_proof(self):
        self.start()
        archived = self.submit("archived")
        self.control.finish(self.control.next())
        candidate = self.archive(self.receive(archived)["payload"]["oms_order_id"])
        before = len(self.fixture.journal.scan().records)
        peer = self.submit(self.key_in_stripe(collision=True))
        key, future = self.control.next()
        proof = self.cold.resolve(key).result(3)
        self.cold.archive((candidate.encode(),)).result(3)
        future.set_result(proof)
        pending = self.control.next()
        self.assertEqual(before, len(self.fixture.journal.scan().records))
        self.control.finish(pending)
        self.assertEqual("ACCEPTED", self.receive(peer)["status"])
        self.assertEqual(1, self.book.order_count("test-account"))

    def test_unrelated_archives_before_resume_do_not_expire_scoped_miss(self):
        if not self.SCOPED_COLD_LOOKUP:
            self.skipTest("scoped candidate only; legacy global behavior tested separately")
        self.start()
        initial = self.submit("archived")
        self.control.finish(self.control.next())
        candidate = self.archive(self.receive(initial)["payload"]["oms_order_id"])
        peer = self.submit(self.key_in_stripe(collision=False))
        key, future = self.control.next()
        proof = self.cold.resolve(key).result(3)
        before = len(self.fixture.journal.scan().records)
        for _ in range(4):
            self.cold.archive((candidate.encode(),)).result(3)
        future.set_result(proof)
        self.assertEqual("ACCEPTED", self.receive(peer)["status"])
        self.assertTrue(self.control.pending.empty())
        self.assertEqual(before + 2, len(self.fixture.journal.scan().records))
        self.assertEqual(0, self.service.ingress_stats().cold_admissions_rejected)

    def test_stop_with_lookup_inflight_does_not_resume_after_completion(self):
        self.start()
        self.submit()
        pending = self.control.next()
        self.service.stop()
        self.assertTrue(self.fixture.runner.is_alive())
        self.control.finish(pending)
        self.fixture.runner.join(3)
        self.assertFalse(self.fixture.runner.is_alive())
        self.assertEqual(0, len(self.fixture.journal.scan().records))
        self.assertEqual({}, self.gateway._sessions)

    def test_pipelined_frame_is_consumed_once_and_resume_is_not_a_new_request(self):
        self.listener._max_requests_per_connection = 2
        self.start()
        peer = self.fixture.connect()
        peer.sendall(self.frame() + self.fixture.frame("after-submit"))
        pending = self.control.next()
        self.hot_barrier()
        self.control.finish(pending)
        decoder = fixtures.ConsumerWireStreamDecoderV2()
        frames = []
        while len(frames) < 2:
            chunk = peer.recv(65536)
            self.assertTrue(chunk)
            frames.extend(decoder.feed(chunk))
        self.assertEqual(["ACCEPTED", "COMPLETED"], [f.payload["status"] for f in frames])
        self.assertEqual(b"", peer.recv(1))
        self.assertEqual(1, len(self.wakeups))

    def test_forged_negative_for_cold_hit_is_rechecked_not_used_to_create_order(self):
        self.start()
        first = self.submit()
        self.control.finish(self.control.next())
        order_id = self.receive(first)["payload"]["oms_order_id"]
        self.archive(order_id)
        before = len(self.fixture.journal.scan().records)
        peer = self.submit()
        key, future = self.control.next()
        proof = self.cold.resolve(key).result(3)
        future.set_result(replace(proof, checkpoint=None))
        pending = self.control.next()
        self.assertEqual(before, len(self.fixture.journal.scan().records))
        self.control.finish(pending)
        self.assertEqual(order_id, self.receive(peer)["payload"]["oms_order_id"])
        self.assertEqual(before, len(self.fixture.journal.scan().records))
        self.assertEqual(1, len(self.wakeups))

    def test_hot_order_capacity_after_lookup_is_explicit_backpressure_before_commit(self):
        self.book._maximum = 1
        self.start()
        first = self.submit()
        self.control.finish(self.control.next())
        self.receive(first)
        before = len(self.fixture.journal.scan().records)
        peer = self.submit("over-capacity")
        self.control.finish(self.control.next())
        result = self.receive(peer)
        self.assertEqual("RATE_LIMITED", result["code"])
        self.assertTrue(result["retryable"])
        self.assertEqual(before, len(self.fixture.journal.scan().records))
        self.assertEqual(1, len(self.wakeups))

    def test_completed_trace_has_submit_resume_and_send_pairs_without_keys_or_proofs(self):
        self.start()
        peer = self.submit()
        self.control.finish(self.control.next())
        self.receive(peer)
        self.service.request_stop()
        self.fixture.runner.join(3)
        self.fixture.trace.close()
        records = [json.loads(line) for line in self.fixture.trace_path.read_text().splitlines()]
        self.assertEqual(1, len(records))
        record = records[0]
        times = [
            record[k]
            for k in (
                "dispatch_started_ns",
                "cold_lookup_enqueued_ns",
                "cold_lookup_completed_ns",
                "cold_resume_started_ns",
                "handler_finished_ns",
                "send_finished_ns",
            )
        ]
        self.assertEqual(sorted(times), times)
        self.assertEqual("sent", record["outcome"])
        self.assertNotIn("strategy_order_id", record)
        self.assertNotIn("resolution", record)


class ScopedColdReactorTests(ColdReactorTests):
    SCOPED_COLD_LOOKUP = True


class IsolatedColdReactorTests(ColdReactorTests):
    ISOLATED_SUBMIT = True
    OWNER_NAME = "consumer-submit-owner"


class IsolatedScopedColdReactorTests(IsolatedColdReactorTests):
    SCOPED_COLD_LOOKUP = True
