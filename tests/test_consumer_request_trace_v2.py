from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch

from xuanling_tradecore.consumer.request_trace_v2 import (
    BoundedRequestTraceWriterV2,
    mark_request_failure,
    mark_request_stage,
    mark_request_value,
    request_trace_scope,
)


class RequestTraceTests(unittest.TestCase):
    def test_missing_final_status_cannot_attest_complete_stage_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            writer = BoundedRequestTraceWriterV2(
                Path(tmp) / "stage.jsonl",
                journal_id="test",
                record_schema="consumer-stage-trace-v1",
            )
            with patch.object(writer, "_publish_status", side_effect=OSError("private")):
                writer.submit({"sequence": 1})
                writer.close()
            self.assertFalse(writer.coverage_verified)

    def test_status_is_single_encoded_with_byte_compatible_ascii_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            with patch(
                "xuanling_tradecore.consumer.request_trace_v2.json.dump",
                side_effect=AssertionError("status used Python per-chunk dump"),
            ):
                writer = BoundedRequestTraceWriterV2(path, journal_id="test-journal")
                writer.set_status_provider(lambda: {"diagnostic_label": "中文"})
                writer.submit({"sequence": 1})
                writer.close()
            text = path.with_suffix(".status.json").read_text(encoding="utf-8")
            value = json.loads(text)
            self.assertEqual(text, json.dumps(value, separators=(",", ":")))
            self.assertEqual("中文", value["diagnostic_label"])
            self.assertTrue(value["drain_complete"])

    def test_scope_isolation_and_flush(self) -> None:
        outer, inner = {}, {}
        with request_trace_scope(outer):
            mark_request_stage("before_ns")
            mark_request_value("order_count", 7)
            mark_request_failure("snapshot", "SNAPSHOT_NOT_READY")
            with request_trace_scope(inner):
                mark_request_stage("inner_ns")
            mark_request_stage("after_ns")
        mark_request_stage("outside_ns")
        self.assertEqual(
            {
                "before_ns",
                "after_ns",
                "order_count",
                "failure_stage",
                "failure_code",
            },
            set(outer),
        )
        self.assertEqual({"inner_ns"}, set(inner))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            writer = BoundedRequestTraceWriterV2(path, journal_id="test-journal")
            writer.set_status_provider(
                lambda: {"ingress": {"overloaded_connections": 7, "pending_capacity": 2048}}
            )
            for i in range(300):
                writer.submit({"sequence": i, **outer})
            writer.close()
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(list(range(300)), [r["sequence"] for r in rows])
            status = json.loads(path.with_suffix(".status.json").read_text())
            self.assertEqual(300, status["written"])
            self.assertEqual(0, status["queue_dropped"] + status["capacity_dropped"])
            self.assertFalse(status["writer_failed"])
            self.assertFalse(status["status_provider_failed"])
            self.assertEqual(7, status["ingress"]["overloaded_connections"])
            self.assertEqual(2048, status["ingress"]["pending_capacity"])
            self.assertTrue(status["closed"])
            self.assertTrue(status["drain_complete"])
            with self.assertRaises(FileExistsError):
                BoundedRequestTraceWriterV2(path, journal_id="replacement")

    def test_drain_timeout_is_explicit_and_cannot_publish_healthy_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            writer = BoundedRequestTraceWriterV2(path, journal_id="slow-test")
            entered, release = Event(), Event()
            target = writer._file

            class SlowFile:
                def write(self, data):
                    entered.set()
                    release.wait(3)
                    return target.write(data)

                def flush(self):
                    return target.flush()

                def close(self):
                    return target.close()

            writer._file = SlowFile()
            try:
                writer.submit({"sequence": 1})
                self.assertTrue(entered.wait(2))
                with self.assertRaisesRegex(RuntimeError, "drain timed out"):
                    writer.close(timeout_seconds=0.001)
                self.assertTrue(writer._failed)
                status = json.loads(path.with_suffix(".status.json").read_text())
                self.assertFalse(status["closed"] and status["drain_complete"])
            finally:
                release.set()
                writer.close(timeout_seconds=3)
            status = json.loads(path.with_suffix(".status.json").read_text())
            self.assertTrue(status["writer_failed"])
            self.assertFalse(status["drain_complete"])

    def test_drain_deadline_rejects_bool_nan_and_unbounded_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            writer = BoundedRequestTraceWriterV2(Path(tmp) / "trace.jsonl", journal_id="limits")
            try:
                for value in (True, 0, -1, float("nan"), float("inf"), 61):
                    with self.subTest(value=value), self.assertRaises(ValueError):
                        writer.close(timeout_seconds=value)
            finally:
                writer.close()

    def test_file_capacity_is_bounded_and_loss_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            writer = BoundedRequestTraceWriterV2(path, journal_id="test-journal", max_bytes=1)
            writer.submit({"sequence": 1})
            writer.close()
            status = json.loads(path.with_suffix(".status.json").read_text())
            self.assertEqual(1, status["capacity_dropped"])
            self.assertEqual(0, status["written"])
            self.assertLessEqual(path.stat().st_size, 1)

    def test_queue_full_does_not_block_producer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            with patch("xuanling_tradecore.consumer.request_trace_v2.Thread.start"):
                writer = BoundedRequestTraceWriterV2(path, journal_id="test", capacity=1)
            writer.submit({"sequence": 1})
            writer.submit({"sequence": 2})
            writer._stop.set()
            writer._run()
            status = json.loads(path.with_suffix(".status.json").read_text())
            self.assertEqual(1, status["queue_dropped"])
            self.assertEqual(1, status["written"])
