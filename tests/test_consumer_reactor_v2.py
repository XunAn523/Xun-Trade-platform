from __future__ import annotations

import json
import os
import socket
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path
from threading import Thread
from unittest.mock import patch

from xuanling_tradecore.consumer import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
    ConsumerWireStreamDecoderV2,
    MessageKind,
    encode_frame,
)
from xuanling_tradecore.consumer.reactor_budget import FixedReactorBudgets
from xuanling_tradecore.consumer.replay_evidence_v2 import build_replay_partition_evidence_v2
from xuanling_tradecore.consumer.request_trace_v2 import BoundedRequestTraceWriterV2
from xuanling_tradecore.consumer.uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    build_consumer_uds_service_v2,
)


@unittest.skipUnless(hasattr(socket, "SO_PEERCRED"), "requires Linux SO_PEERCRED")
class ConsumerReactorV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
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
        self.path = config.socket_path
        self.trace_path = root / "requests.jsonl"
        self.trace = BoundedRequestTraceWriterV2(self.trace_path, journal_id="test-journal")
        self.service, self.journal = build_consumer_uds_service_v2(
            config=config,
            contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
        )
        self.service.set_request_trace(self.trace)
        self.runner = Thread(target=self.service.serve_forever)
        self.peers: list[socket.socket] = []

    def tearDown(self) -> None:
        for peer in self.peers:
            peer.close()
        self.service.stop()
        if self.runner.is_alive():
            self.runner.join(timeout=3)
        self.trace.close()
        self.journal.close()
        self.directory.cleanup()
        self.assertFalse(self.runner.is_alive())

    def connect(self) -> socket.socket:
        peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        peer.settimeout(2)
        peer.connect(str(self.path))
        self.peers.append(peer)
        return peer

    @staticmethod
    def frame(request_id: str) -> bytes:
        return encode_frame(
            MessageKind.REQUEST,
            3,
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": "GetAccountOrderSnapshot",
                "request_id": request_id,
                "payload": {"consumer_account_id": "test-account"},
            },
        )

    def test_ingress_stats_publish_counters_and_fixed_capacities(self) -> None:
        stats = self.service.ingress_stats()
        self.assertEqual(0, stats.socket_accepted_connections)
        self.assertEqual(2_048, stats.pending_capacity)
        self.assertEqual(8_192, stats.preframe_capacity)
        self.assertEqual(10_240, stats.total_connection_capacity)
        self.assertEqual(1_024, stats.overload_response_capacity)
        self.assertEqual(64 * 1_024 * 1_024, stats.reactor_output_capacity_bytes)
        self.assertEqual(0, stats.observed_preframes_pending)

    def test_fixed_turns_do_not_switch_policy_at_24_or_512_connections(self) -> None:
        import resource

        previous = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft, hard = previous
        self.assertTrue(
            hard == resource.RLIM_INFINITY or hard >= 4096,
            "same-process server/client fixture requires 4096 FD hard allowance",
        )
        self.addCleanup(resource.setrlimit, resource.RLIMIT_NOFILE, previous)
        if soft != resource.RLIM_INFINITY and soft < 4096:
            resource.setrlimit(resource.RLIMIT_NOFILE, (4096, hard))
        self.service.set_reactor_turn_budgets(FixedReactorBudgets())
        self.service.start()
        self.runner.start()
        for count in (24, 511, 512, 513):
            peers = []
            for n in range(count):
                peer = self.connect()
                peer.sendall(self.frame(f"fixed-{count}-{n}"))
                peers.append(peer)
            for n, peer in enumerate(peers):
                decoder = ConsumerWireStreamDecoderV2()
                frames = ()
                while not frames:
                    frames = decoder.feed(peer.recv(65536))
                self.assertEqual(f"fixed-{count}-{n}", frames[0].payload["request_id"])
                peer.close()
        stats = self.service.ingress_stats()
        self.assertLessEqual(stats.peak_pending_connections, 24)
        self.assertEqual(0, stats.overloaded_connections)
        self.assertEqual(0, len(self.journal.scan().records))

    def test_tiny_byte_budgets_preserve_partial_frame_and_ordered_reply(self) -> None:
        self.service.set_reactor_turn_budgets(
            FixedReactorBudgets(
                accepts=1, reads=1, writes=1, read_bytes=64, write_bytes=64, ready=1, dispatches=1
            )
        )
        self.service.start()
        self.runner.start()
        peer = self.connect()
        peer.sendall(self.frame("fixed-partial"))
        peer.shutdown(socket.SHUT_WR)
        decoder = ConsumerWireStreamDecoderV2()
        frames = ()
        while not frames:
            frames = decoder.feed(peer.recv(65536))
        self.assertEqual("fixed-partial", frames[0].payload["request_id"])
        self.assertEqual(0, self.service.ingress_stats().overloaded_connections)

    def test_readable_first_frames_are_observed_once_until_capacity_resumes(self) -> None:
        # All peers arrive before the runner. Only two decoded slots are
        # available and one is dispatched per turn, deliberately exercising
        # the full-window path without relying on wall-clock performance.
        with patch.multiple(
            "xuanling_tradecore.consumer.service_v2",
            _REACTOR_READY_HIGH_WATERMARK=2,
            _REACTOR_DISPATCH_BATCH=1,
            _REACTOR_BURST_CONNECTION_THRESHOLD=10_000,
        ):
            self.service.start()
            for n in range(64):
                self.connect().sendall(self.frame(f"observed-{n}"))
            self.runner.start()
            for n, peer in enumerate(self.peers):
                decoder = ConsumerWireStreamDecoderV2()
                frames = ()
                while not frames:
                    frames = decoder.feed(peer.recv(65_536))
                self.assertEqual(f"observed-{n}", frames[0].payload["request_id"])
            stats = self.service.ingress_stats()
            self.assertGreater(stats.peak_observed_preframes, 32)
            # A still-readable socket must not be reported on every turn.
            self.assertLessEqual(stats.preframe_readiness_events, 64)
            self.assertEqual(0, stats.observed_preframes_pending)
            self.assertEqual(0, stats.overloaded_connections)

    def test_observed_partial_frames_rearm_and_disconnected_peers_do_not_stall(self) -> None:
        with patch.multiple(
            "xuanling_tradecore.consumer.service_v2",
            _REACTOR_READY_HIGH_WATERMARK=1,
            _REACTOR_DISPATCH_BATCH=1,
            _REACTOR_BURST_CONNECTION_THRESHOLD=10_000,
        ):
            self.service.start()
            self.connect().sendall(self.frame("ahead"))
            gone = self.connect()
            gone.shutdown(socket.SHUT_WR)
            partial = self.connect()
            frame = self.frame("partial-rearmed")
            partial.sendall(frame[:8])
            fast = self.connect()
            fast.sendall(self.frame("after-partial"))
            self.runner.start()
            decoder = ConsumerWireStreamDecoderV2()
            frames = ()
            while not frames:
                frames = decoder.feed(fast.recv(65_536))
            self.assertEqual("after-partial", frames[0].payload["request_id"])
            partial.sendall(frame[8:])
            decoder = ConsumerWireStreamDecoderV2()
            frames = ()
            while not frames:
                frames = decoder.feed(partial.recv(65_536))
            self.assertEqual("partial-rearmed", frames[0].payload["request_id"])
            self.assertEqual(b"", gone.recv(1))
            self.assertEqual(0, self.service.ingress_stats().observed_preframes_pending)
            self.assertEqual(0, len(self.journal.scan().records))

    def test_request_reactor_never_logs_periodic_stats_on_socket_owner(self) -> None:
        with patch.object(
            self.service,
            "_maybe_log_ingress_stats",
            side_effect=AssertionError("periodic logging entered the reactor hot path"),
        ):
            self.service.start()
            self.runner.start()
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(self.path, timeout_seconds=3))
            response = client.call(
                {
                    "message_type": "request",
                    "schema_version": 2,
                    "request_id": "no-reactor-log",
                    "operation": "GetAccountOrderSnapshot",
                    "payload": {"consumer_account_id": "test-account"},
                }
            )
            self.assertEqual("COMPLETED", response["status"])

    def test_opt_in_cycle_evidence_is_separate_bounded_and_keeps_request_count(self) -> None:
        stage_path = self.trace_path.with_name("stages.jsonl")
        stages = BoundedRequestTraceWriterV2(
            stage_path, journal_id="test-journal", record_schema="consumer-stage-trace-v1"
        )
        try:
            self.service.set_stage_trace(stages)
            self.service.start()
            self.runner.start()
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(self.path))
            for n in range(32):
                response = client.call(
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "operation": "GetAccountOrderSnapshot",
                        "request_id": str(n),
                        "payload": {"consumer_account_id": "test-account"},
                    }
                )
                self.assertEqual("COMPLETED", response["status"])
            self.service.request_stop()
            self.runner.join(3)
        finally:
            self.service.request_stop()
            if self.runner.is_alive():
                self.runner.join(3)
            stages.close()
        rows = [json.loads(line) for line in stage_path.read_text().splitlines()]
        self.assertTrue(stages.coverage_verified)
        self.assertEqual(32, sum(row["calls"].get("decode", 0) for row in rows))
        self.assertEqual(32, sum(row["values"].get("dispatch_attempts", 0) for row in rows))
        self.assertTrue(all(row["inclusive_stages"] for row in rows))
        self.assertTrue(all(row["component"] == "reactor" for row in rows))
        self.assertNotIn("test-account", stage_path.read_text())

    def test_slow_reader_partial_write_does_not_block_another_connection(self) -> None:
        listener = self.service._listener  # pyright: ignore[reportPrivateUsage]
        original_accept = listener.accept_connection

        def accept():
            peer, principal = original_accept()
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            return peer, principal

        def handler(_session, request):
            return {
                "message_type": "response",
                "schema_version": 2,
                "request_id": request["request_id"],
                "status": "COMPLETED",
                "payload": {"data": "x" * 500_000 if request["request_id"] == "slow" else "ok"},
            }

        with (
            patch.object(listener, "accept_connection", side_effect=accept),
            patch.object(listener, "_request_handler", side_effect=handler),
        ):
            self.service.start()
            self.runner.start()
            slow = self.connect()
            slow.sendall(self.frame("slow"))
            fast = self.connect()
            fast.sendall(self.frame("fast"))
            decoder = ConsumerWireStreamDecoderV2()
            frames = ()
            while not frames:
                frames = decoder.feed(fast.recv(65536))
            self.assertEqual("ok", frames[0].payload["payload"]["data"])
            decoder = ConsumerWireStreamDecoderV2()
            frames = ()
            while not frames:
                chunk = slow.recv(65536)
                self.assertTrue(chunk)
                frames = decoder.feed(chunk)
            self.assertEqual("x" * 500_000, frames[0].payload["payload"]["data"])
            self.assertEqual(0, len(self.journal.scan().records))

    def test_bad_crc_and_incomplete_peer_are_closed_without_journal_commit(self) -> None:
        self.service._listener._connection_timeout_seconds = 0.1  # pyright: ignore[reportPrivateUsage]
        self.service._preframe_timeout_ns = 100_000_000  # pyright: ignore[reportPrivateUsage]
        self.service.start()
        self.runner.start()
        corrupt = self.connect()
        frame = bytearray(self.frame("bad"))
        frame[-1] ^= 1
        corrupt.sendall(frame)
        self.assertEqual(b"", corrupt.recv(1))
        incomplete = self.connect()
        incomplete.sendall(b"XTC2")
        self.assertEqual(b"", incomplete.recv(1))
        self.assertEqual(0, len(self.journal.scan().records))
        self.assertEqual(1, self.service.ingress_stats().expired_preframe_connections)

    def test_signed_partitions_cover_once_and_exclude_later_writes(self) -> None:
        self.service.start()
        self.runner.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(self.path, timeout_seconds=3))

        def call(operation, payload):
            return client.call(
                {
                    "message_type": "request",
                    "schema_version": 2,
                    "request_id": "partition-test",
                    "operation": operation,
                    "payload": payload,
                }
            )

        def submit(index):
            return call(
                "SubmitSignal",
                {
                    "consumer_account_id": "test-account",
                    "strategy_id": "test-strategy",
                    "strategy_order_id": str(index),
                    "trading_day": "2026-09-30",
                    "symbol": "TEST",
                    "market": "SIM",
                    "side": "BUY",
                    "price_type": "LIMIT",
                    "business_type": "CASH",
                    "quantity": 1,
                    "protection_price_ticks": 1,
                },
            )

        empty = call(
            "CreateReplayPartitions", {"consumer_account_id": "test-account", "partition_count": 4}
        )
        self.assertEqual([], empty["payload"]["partitions"])
        for index in range(11):
            self.assertEqual("ACCEPTED", submit(index)["status"])
        plan = call(
            "CreateReplayPartitions", {"consumer_account_id": "test-account", "partition_count": 4}
        )["payload"]
        self.assertEqual(11, plan["event_count"])
        evidence = build_replay_partition_evidence_v2(plan)
        self.assertEqual(plan["journal_id"], evidence["journal_id"])
        self.assertEqual(plan["stream_generation"], evidence["stream_generation"])
        self.assertTrue(evidence["bounds_valid"])
        self.assertNotIn("cursor", evidence["partitions"][0])
        self.assertEqual([2, 3, 3, 3], [p["event_count"] for p in plan["partitions"]])
        self.assertIsNone(plan["partitions"][0]["after_position"])
        for previous, current in zip(plan["partitions"], plan["partitions"][1:]):
            self.assertEqual(previous["high_watermark_position"], current["after_position"])
        # The partition plan's upper fence must survive later OMS commits and
        # every continuation page, even when the cache refreshes for a new plan.
        self.assertEqual("ACCEPTED", submit(11)["status"])
        self.assertEqual(
            12,
            call(
                "CreateReplayPartitions",
                {"consumer_account_id": "test-account", "partition_count": 4},
            )["payload"]["event_count"],
        )
        ids = []
        for partition in plan["partitions"]:
            token = partition["cursor"]
            count = 0
            positions = []
            while True:
                page = call("PollEvents", {"cursor": token, "limit": 2})["payload"]
                self.assertEqual(
                    partition["high_watermark_position"], page["high_watermark_position"]
                )
                ids.extend(e["event_id"] for e in page["events"])
                positions.extend(e["position"] for e in page["events"])
                count += len(page["events"])
                token = page["next_cursor"]
                if not page["has_more"]:
                    break
            self.assertEqual(partition["event_count"], count)
            self.assertEqual(partition["high_watermark_position"], positions[-1])
            self.assertEqual(
                [], call("PollEvents", {"cursor": token, "limit": 2})["payload"]["events"]
            )
        self.assertEqual(11, len(ids))
        self.assertEqual(11, len(set(ids)))
        bad = plan["partitions"][0]["cursor"]
        bad = bad[:-8] + ("A" if bad[-8] != "A" else "B") + bad[-7:]
        self.assertEqual("CURSOR_INVALID", call("PollEvents", {"cursor": bad, "limit": 2})["code"])
        self.assertEqual(
            "UNAUTHORIZED",
            call(
                "CreateReplayPartitions", {"consumer_account_id": "foreign", "partition_count": 4}
            )["code"],
        )
        self.service.stop()
        self.runner.join(timeout=3)
        self.trace.close()
        records = [json.loads(line) for line in self.trace_path.read_text().splitlines()]
        polls = [r for r in records if r.get("operation") == "PollEvents"]
        self.assertGreater(len(polls), 4)
        self.assertTrue(all(r["outcome"] == "sent" for r in polls))
        self.assertTrue(
            all(r["request_id_sha256"] == sha256(b"partition-test").hexdigest() for r in polls)
        )
        self.assertEqual("ERROR", polls[-1]["response_kind"])
        for record in polls:
            names = [
                "first_byte_observed_ns",
                "frame_ready_ns",
                "frame_decoded_ns",
                "dispatch_started_ns",
                "schema_validated_ns",
                "cursor_authorized_ns",
                "replay_read_finished_ns",
                "replay_validated_ns",
                "next_cursor_signed_ns",
                "event_payload_built_ns",
                "handler_finished_ns",
                "response_encoded_ns",
                "send_finished_ns",
            ]
            times = [record[name] for name in names if name in record]
            self.assertEqual(sorted(times), times)
            self.assertNotIn("cursor", record)
            self.assertNotIn("request_id", record)
        status = json.loads(self.trace_path.with_suffix(".status.json").read_text())
        self.assertEqual(len(records), status["written"])
        self.assertEqual(0, status["queue_dropped"] + status["capacity_dropped"])


if __name__ == "__main__":
    unittest.main()
