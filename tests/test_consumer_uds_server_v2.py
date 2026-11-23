from __future__ import annotations

import json
import os
import socket
import stat
import tempfile
import time
import unittest
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Thread
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from xuanling_tradecore.consumer import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
    ConsumerWireStreamDecoderV2,
    MessageKind,
    encode_frame,
)
from xuanling_tradecore.consumer.request_trace_v2 import request_trace_scope
from xuanling_tradecore.consumer.snapshot_v2 import ConsumerSnapshotCapacityErrorV2
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2
from xuanling_tradecore.consumer.uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    _OrderBookProjectionV2,
    build_consumer_uds_service_v2,
    load_consumer_uds_server_config_v2,
)
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerEventKind,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    TradingDayEpoch,
)


class ConsumerSnapshotGuardV2Tests(unittest.TestCase):
    def test_profile_accepts_bounded_additional_peers_and_shared_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumer-uds.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "socket_path": str(Path(directory) / "consumer-v2.sock"),
                        "journal_root": str(Path(directory) / "journal"),
                        "journal_capacity": 16_384,
                        "peer_uid": 1000,
                        "peer_gid": 1000,
                        "additional_peer_credentials": [{"uid": 1002, "gid": 1002}],
                        "socket_mode": 0o660,
                        "listen_backlog": 8_192,
                        "pending_capacity": 2_048,
                        "preframe_capacity": 8_192,
                        "consumer_id": "test-consumer",
                        "login_name": "test-login",
                        "legal_name": "test-name",
                        "consumer_account_id": "test-account",
                        "account_id": "test-account",
                        "account_display_name": "Test",
                        "venue_id": "NO_ADAPTER_BENCHMARK",
                        "adapter_instance_id": "disabled-no-dispatch",
                        "strategy_id": "test-strategy",
                    }
                ),
                encoding="utf-8",
            )
            path.chmod(0o600)
            with patch.object(
                type(path),
                "lstat",
                return_value=SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0),
            ):
                config = load_consumer_uds_server_config_v2(path)
        self.assertEqual(((1002, 1002),), config.additional_peer_credentials)
        self.assertEqual(0o660, config.socket_mode)
        self.assertEqual(8_192, config.listen_backlog)
        self.assertEqual(2_048, config.pending_capacity)
        self.assertEqual(8_192, config.preframe_capacity)

    def test_profile_rejects_unbounded_ingress_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "consumer-uds.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "socket_path": str(Path(directory) / "consumer-v2.sock"),
                        "journal_root": str(Path(directory) / "journal"),
                        "journal_capacity": 16_384,
                        "peer_uid": 1000,
                        "peer_gid": 1000,
                        "consumer_id": "test-consumer",
                        "login_name": "test-login",
                        "legal_name": "test-name",
                        "consumer_account_id": "test-account",
                        "account_id": "test-account",
                        "account_display_name": "Test",
                        "venue_id": "NO_ADAPTER_BENCHMARK",
                        "adapter_instance_id": "disabled-no-dispatch",
                        "strategy_id": "test-strategy",
                        "pending_capacity": 32_769,
                        "preframe_capacity": 32_768,
                    }
                ),
                encoding="utf-8",
            )
            path.chmod(0o600)
            with (
                patch.object(
                    type(path),
                    "lstat",
                    return_value=SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_uid=0),
                ),
                self.assertRaisesRegex(ValueError, "ingress capacity"),
            ):
                load_consumer_uds_server_config_v2(path)

    def test_committed_events_incrementally_publish_replay_index(self) -> None:
        class _Book:
            count = 0

            def event_count(self, _account_id: str) -> int:
                return self.count

            def events_from(self, _account_id: str, _start: int) -> object:
                raise AssertionError("incremental replay must not scan order history")

        book = _Book()
        binding = ConsumerAccountBinding(
            "binding-1",
            1,
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "sim",
            "NO_ADAPTER_BENCHMARK",
            "disabled-no-dispatch",
        )
        stream = CursorStreamRefV2("journal-1", 1)
        projection = _OrderBookProjectionV2(
            order_book=book,  # type: ignore[arg-type]
            binding=binding,
            stream=stream,
        )

        def event(index: int) -> ConsumerOrderEvent:
            return ConsumerOrderEvent(
                f"event-{index}",
                ConsumerEventKind.ORDER,
                2,
                binding.account_id,
                TradingDayEpoch("2026-10-09"),
                stream.journal_id,
                index,
                ConsumerTrace(
                    binding.consumer_id,
                    binding.consumer_account_id,
                    binding.account_id,
                    "strategy-1",
                    f"strategy-order-{index}",
                    "attribution-1",
                    1,
                    OrderOrigin.STRATEGY,
                ),
                f"oms-{index}",
                f"execution-{index}",
                OmsOrderState.ACCEPTED,
                0,
            )

        for index in (1, 2):
            book.count = index
            projection.publish(event(index))

        bounds = projection.replay_partition_bounds(
            binding=binding, stream=stream, partition_count=2
        )
        self.assertEqual((1, 1), tuple(item.event_count for item in bounds))
        self.assertEqual(
            (1, 2),
            tuple(item.high_watermark_position.stream_id.redis_ms for item in bounds),
        )

    def test_legacy_snapshot_rejects_large_account_before_projection(self) -> None:
        class _Book:
            def order_count(self, _account_id: str) -> int:
                return _OrderBookProjectionV2.LEGACY_SNAPSHOT_MAX_ORDERS + 1

            def last_event(self, _account_id: str) -> object:
                raise AssertionError("guard must run before projection")

        binding = SimpleNamespace(account_id="test-account")
        stream = object()
        projection = _OrderBookProjectionV2(
            order_book=_Book(),  # type: ignore[arg-type]
            binding=binding,  # type: ignore[arg-type]
            stream=stream,  # type: ignore[arg-type]
        )
        trace: dict[str, object] = {}
        with request_trace_scope(trace):
            with self.assertRaisesRegex(
                ConsumerSnapshotCapacityErrorV2, "bounded single-frame"
            ) as raised:
                projection.read_snapshot(binding=binding, stream=stream)  # type: ignore[arg-type]
        self.assertEqual("SNAPSHOT_REQUIRES_PAGINATION", raised.exception.code)
        self.assertEqual(1_025, trace["snapshot_order_count"])
        self.assertNotIn("snapshot_projection_started_ns", trace)


@unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "requires Linux SO_PEERCRED")
class ConsumerUdsServerV2Tests(unittest.TestCase):
    def test_fresh_peers_are_reaped_during_sustained_burst(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=root / "fresh-burst.sock",
                journal_root=root,
                journal_capacity=16_384,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="test-consumer",
                login_name="test-login",
                legal_name="test-name",
                consumer_account_id="test-account",
                account_id="test-account",
                account_display_name="Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="test-strategy",
            )
            service, journal = build_consumer_uds_service_v2(
                config=config,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            )
            service.start()
            runner = Thread(target=service.serve_forever)
            runner.start()

            def submit(number: int) -> Mapping[str, object]:
                return cast(
                    Mapping[str, object],
                    ConsumerUnixClientV2(
                        ConsumerUnixClientConfigV2(config.socket_path, timeout_seconds=10.0)
                    ).call(
                        {
                            "message_type": "request",
                            "schema_version": 2,
                            "operation": "SubmitSignal",
                            "request_id": f"fresh-submit-{number}",
                            "payload": {
                                "consumer_account_id": "test-account",
                                "strategy_id": "test-strategy",
                                "strategy_order_id": f"fresh-unique-{number}",
                                "trading_day": "2026-10-12",
                                "symbol": "TEST",
                                "market": "SIM",
                                "side": "BUY",
                                "price_type": "LIMIT",
                                "business_type": "CASH",
                                "quantity": 1,
                                "protection_price_ticks": 1,
                            },
                        }
                    ),
                )

            try:
                # More requests than the total 6,144 connection capacity prove
                # that response-complete peers are reaped while new fresh
                # connections continue to arrive; they must not accumulate in
                # the selector until healthy requests are rejected.
                with ThreadPoolExecutor(max_workers=256) as pool:
                    responses = tuple(pool.map(submit, range(7_000)))
                self.assertTrue(all(item["status"] == "ACCEPTED" for item in responses))
                deadline = time.monotonic() + 3
                while service.ingress_stats().active_connections:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                stats = service.ingress_stats()
                self.assertEqual(7_000, stats.socket_accepted_connections)
                self.assertEqual(7_000, stats.served_connections)
                self.assertEqual(0, stats.overloaded_connections)
                self.assertEqual(0, stats.lane_rejected_connections)
                self.assertLessEqual(stats.peak_pending_connections, 24)
                self.assertTrue(journal.scan().clean)
            finally:
                service.stop()
                runner.join(timeout=3)
                journal.close()
            self.assertFalse(runner.is_alive())

    def test_persistent_peers_do_not_pin_workers_and_pipeline_stays_ordered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=root / "probe.sock",
                journal_root=root,
                journal_capacity=256,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="test-consumer",
                login_name="test-login",
                legal_name="test-name",
                consumer_account_id="test-account",
                account_id="test-account",
                account_display_name="Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="test-strategy",
            )
            service, journal = build_consumer_uds_service_v2(
                config=config,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            )
            service.start()
            runner = Thread(target=service.serve_forever)
            runner.start()
            peers: list[socket.socket] = []
            try:
                # Every peer remains open after both replies. The old 16 blocking
                # workers could not reach peer 17 until earlier clients closed.
                for index in range(32):
                    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    peer.settimeout(2)
                    peer.connect(str(config.socket_path))
                    peers.append(peer)
                    frames = b"".join(
                        encode_frame(
                            MessageKind.REQUEST,
                            sequence,
                            {
                                "message_type": "request",
                                "schema_version": 2,
                                "operation": "SubmitSignal",
                                "request_id": f"{index}-{sequence}",
                                "payload": {
                                    "consumer_account_id": "test-account",
                                    "strategy_id": "test-strategy",
                                    "strategy_order_id": f"{index}-{sequence}",
                                    "trading_day": "2026-09-29",
                                    "symbol": "TEST",
                                    "market": "SIM",
                                    "side": "BUY",
                                    "price_type": "LIMIT",
                                    "business_type": "CASH",
                                    "quantity": 1,
                                    "protection_price_ticks": 1,
                                },
                            },
                        )
                        for sequence in (1, 2)
                    )
                    peer.sendall(frames[:7])
                    peer.sendall(frames[7:])
                    decoder = ConsumerWireStreamDecoderV2()
                    responses = []
                    while len(responses) < 2:
                        chunk = peer.recv(65_536)
                        self.assertTrue(chunk)
                        responses.extend(decoder.feed(chunk))
                    self.assertEqual([1, 2], [r.header.correlation_id for r in responses])
                    self.assertTrue(all(r.payload["status"] == "ACCEPTED" for r in responses))
                self.assertEqual(128, len(journal.scan().records))
                self.assertTrue(journal.scan().clean)
                self.assertEqual(0, service.ingress_stats().overloaded_connections)
                self.assertEqual(32, service.ingress_stats().active_connections)
            finally:
                for peer in peers:
                    peer.close()
                service.stop()
                runner.join(timeout=3)
                journal.close()
            self.assertFalse(runner.is_alive())

    def test_single_request_mode_closes_after_complete_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=root / "single-request.sock",
                journal_root=root,
                journal_capacity=16,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="test-consumer",
                login_name="test-login",
                legal_name="test-name",
                consumer_account_id="test-account",
                account_id="test-account",
                account_display_name="Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="test-strategy",
                max_requests_per_connection=1,
            )
            service, journal = build_consumer_uds_service_v2(
                config=config,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            )
            service.start()
            runner = Thread(target=service.serve_forever)
            runner.start()
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peer.settimeout(2)
            try:
                peer.connect(str(config.socket_path))
                peer.sendall(
                    encode_frame(
                        MessageKind.REQUEST,
                        1,
                        {
                            "message_type": "request",
                            "schema_version": 2,
                            "operation": "ListAccounts",
                            "request_id": "single-request",
                            "payload": {},
                        },
                    )
                )
                decoder = ConsumerWireStreamDecoderV2()
                responses = ()
                while not responses:
                    responses = decoder.feed(peer.recv(65_536))
                self.assertEqual("COMPLETED", responses[0].payload["status"])
                self.assertEqual(b"", peer.recv(1))
                # EOF only confirms the server's response-side SHUT_WR.  The
                # read-side tombstone remains owned until the client closes;
                # served_connections is an eventual *reaped* connection count.
                peer.close()
                deadline = time.monotonic() + 3
                while service.ingress_stats().active_connections:
                    self.assertLess(time.monotonic(), deadline, "connection was not reaped")
                    time.sleep(0.005)
                stats = service.ingress_stats()
                self.assertEqual(1, stats.accepted_connections)
                self.assertEqual(1, stats.served_connections)
                self.assertEqual(0, stats.active_connections)
            finally:
                peer.close()
                service.stop()
                runner.join(timeout=3)
                journal.close()
            self.assertFalse(runner.is_alive())

    def test_complete_frame_waiting_in_listen_backlog_is_read_on_accept(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            socket_root = root / "run"
            socket_root.mkdir(mode=0o700)
            journal_root = root / "journal"
            journal_root.mkdir(mode=0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=socket_root / "consumer-v2.sock",
                journal_root=journal_root,
                journal_capacity=64,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="uds-test-consumer",
                login_name="uds-test-login",
                legal_name="uds-test-name",
                consumer_account_id="uds-test-account",
                account_id="uds-test-account",
                account_display_name="UDS Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="uds-test-strategy",
            )
            service, journal = build_consumer_uds_service_v2(
                config=config,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            )
            service.start()
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peer.settimeout(3)
            worker = Thread(target=service.serve_forever)
            try:
                peer.connect(str(config.socket_path))
                peer.sendall(
                    encode_frame(
                        MessageKind.REQUEST,
                        1,
                        {
                            "message_type": "request",
                            "schema_version": 2,
                            "operation": "GetAccountOrderSnapshot",
                            "request_id": "pre-sent-snapshot",
                            "payload": {"consumer_account_id": "uds-test-account"},
                        },
                    )
                )
                worker.start()
                decoder = ConsumerWireStreamDecoderV2()
                response = ()
                while not response:
                    response = decoder.feed(peer.recv(65_536))
                self.assertEqual("COMPLETED", response[0].payload["status"])
                stats = service.ingress_stats()
                self.assertEqual(1, stats.ready_on_accept_connections)
                self.assertEqual(0, stats.preframe_connections)
                self.assertEqual(0, stats.overloaded_connections)
            finally:
                peer.close()
                service.stop()
                if worker.is_alive():
                    worker.join(timeout=3)
                journal.close()

    def test_concurrent_connections_commit_distinct_intents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            socket_root = root / "run"
            socket_root.mkdir(mode=0o700)
            journal_root = root / "journal"
            journal_root.mkdir(mode=0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=socket_root / "consumer-v2.sock",
                journal_root=journal_root,
                journal_capacity=256,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="uds-test-consumer",
                login_name="uds-test-login",
                legal_name="uds-test-name",
                consumer_account_id="uds-test-account",
                account_id="uds-test-account",
                account_display_name="UDS Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="uds-test-strategy",
            )
            service, journal = build_consumer_uds_service_v2(
                config=config,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            )
            service.start()
            worker = Thread(target=service.serve_forever)
            worker.start()
            release = Barrier(33)
            stalled: list[socket.socket] = []

            def submit(number: int) -> object:
                client = ConsumerUnixClientV2(
                    ConsumerUnixClientConfigV2(config.socket_path, timeout_seconds=10.0)
                )
                release.wait(timeout=5)
                return client.call(
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "operation": "SubmitSignal",
                        "request_id": f"submit-{number}",
                        "payload": {
                            "consumer_account_id": "uds-test-account",
                            "strategy_id": "uds-test-strategy",
                            "strategy_order_id": f"unique-{number}",
                            "trading_day": "2026-09-21",
                            "symbol": "TEST",
                            "market": "SIM",
                            "side": "BUY",
                            "price_type": "LIMIT",
                            "business_type": "CASH",
                            "quantity": 1,
                            "protection_price_ticks": 1,
                        },
                    }
                )

            try:
                # A burst of authorized but incomplete frames must not take
                # all 16 business workers away from complete requests.
                for _ in range(16):
                    peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    peer.connect(str(config.socket_path))
                    peer.sendall(b"XTC2")
                    stalled.append(peer)
                deadline = time.monotonic() + 2
                while service.ingress_stats().preframe_connections < 16:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                self.assertEqual(0, service.ingress_stats().active_connections)
                with ThreadPoolExecutor(max_workers=32) as pool:
                    futures = [pool.submit(submit, index) for index in range(32)]
                    release.wait(timeout=5)
                    responses = [future.result(timeout=15) for future in futures]
                self.assertTrue(all(response["status"] == "ACCEPTED" for response in responses))
                self.assertEqual(64, len(journal.scan().records))
                self.assertTrue(journal.scan().clean)
                self.assertEqual(0, service.ingress_stats().overloaded_connections)
            finally:
                for peer in stalled:
                    peer.close()
                service.stop()
                worker.join(timeout=3)
                journal.close()

    def test_real_uds_commits_mmap_fact_before_returning_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            socket_root = root / "run"
            socket_root.mkdir(mode=0o700)
            journal_root = root / "journal"
            journal_root.mkdir(mode=0o700)
            config = ConsumerUdsServerConfigV2(
                socket_path=socket_root / "consumer-v2.sock",
                journal_root=journal_root,
                journal_capacity=64,
                peer_uid=os.geteuid(),
                peer_gid=os.getegid(),
                consumer_id="uds-test-consumer",
                login_name="uds-test-login",
                legal_name="uds-test-name",
                consumer_account_id="uds-test-account",
                account_id="uds-test-account",
                account_display_name="UDS Test",
                venue_id="NO_ADAPTER_BENCHMARK",
                adapter_instance_id="disabled-no-dispatch",
                strategy_id="uds-test-strategy",
            )
            contract_root = Path(__file__).parents[1] / "contracts" / "consumer" / "v2"
            service, journal = build_consumer_uds_service_v2(
                config=config, contract_root=contract_root
            )
            service.start()
            worker = Thread(target=service.serve_forever)
            worker.start()
            try:
                client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path))
                listed = client.call(
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "operation": "ListAccounts",
                        "request_id": "list-1",
                        "payload": {},
                    }
                )
                self.assertEqual("COMPLETED", listed["status"])
                signal_request = {
                    "message_type": "request",
                    "schema_version": 2,
                    "operation": "SubmitSignal",
                    "request_id": "submit-1",
                    "payload": {
                        "consumer_account_id": "uds-test-account",
                        "strategy_id": "uds-test-strategy",
                        "strategy_order_id": "unique-1",
                        "trading_day": "2026-09-21",
                        "symbol": "TEST",
                        "market": "SIM",
                        "side": "BUY",
                        "price_type": "LIMIT",
                        "business_type": "CASH",
                        "quantity": 1,
                        "protection_price_ticks": 1,
                    },
                }
                submitted = client.call(signal_request)
                self.assertEqual("ACCEPTED", submitted["status"])
                self.assertEqual(2, len(journal.scan().records))
                self.assertTrue(journal.scan().clean)
                snapshot_request = {
                    "message_type": "request",
                    "schema_version": 2,
                    "operation": "GetAccountOrderSnapshot",
                    "request_id": "snapshot-1",
                    "payload": {"consumer_account_id": "uds-test-account"},
                }
                first = client.call(snapshot_request)
                second = client.call({**snapshot_request, "request_id": "snapshot-2"})
                self.assertEqual(1, len(first["payload"]["orders"]))
                self.assertEqual(
                    first["payload"]["snapshot_position"], second["payload"]["snapshot_position"]
                )
                self.assertEqual(first["payload"]["orders"], second["payload"]["orders"])
                next_signal = {
                    **signal_request,
                    "request_id": "submit-2",
                    "payload": {**signal_request["payload"], "strategy_order_id": "unique-2"},
                }
                self.assertEqual("ACCEPTED", client.call(next_signal)["status"])
                updated = client.call({**snapshot_request, "request_id": "snapshot-3"})
                self.assertEqual(2, len(updated["payload"]["orders"]))
                self.assertNotEqual(
                    first["payload"]["snapshot_position"], updated["payload"]["snapshot_position"]
                )
            finally:
                service.stop()
                worker.join(timeout=2)
                journal.close()


if __name__ == "__main__":
    unittest.main()
