from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.consumer.request_trace_v2 import BoundedRequestTraceWriterV2
from xuanling_tradecore.persistence.rotating_request_trace import (
    RotatingRequestTraceFile,
    verify_closed_request_trace,
)


def test_hundred_rotations_are_continuous_and_original_writer_remains_compatible(
    tmp_path: Path,
) -> None:
    path = tmp_path / "requests.jsonl"
    writer = BoundedRequestTraceWriterV2(
        path, journal_id="j", rotation_bytes=512, max_bytes=10 * 1024**2
    )
    for n in range(4000):
        writer.submit({"request_hash": str(n), "operation": "test", "accepted_ns": n})
    # >100-rotation/fsync integrity stress; not the production 128 MiB profile
    # or a 10-second shutdown performance assertion. Keep the 4,000-row gate.
    writer.close(timeout_seconds=30)
    status = json.loads(path.with_suffix(".status.json").read_text())
    assert not status["writer_failed"] and status["written"] == 4000
    assert status["closed"] and status["drain_complete"]
    assert status["queue_dropped"] == status["capacity_dropped"] == 0
    assert status["trace_storage"]["sealed_segments"] >= 100
    assert verify_closed_request_trace(path.with_suffix(".manifest.json")) == 4000
    with pytest.raises(FileExistsError):
        BoundedRequestTraceWriterV2(path, journal_id="j", rotation_bytes=512)


def test_interval_rotation_and_unsealed_manifest_are_explicit(tmp_path: Path) -> None:
    now = [1]
    storage = RotatingRequestTraceFile(
        tmp_path / "requests.jsonl",
        journal_id="j",
        segment_bytes=1024,
        interval_ns=100,
        clock=lambda: now[0],
    )
    storage.write(msgspec.json.encode({"trace_sequence": 1}) + b"\n")
    with pytest.raises(ValueError, match="unsealed"):
        verify_closed_request_trace(storage.manifest)
    now[0] = 102
    storage.flush()
    assert storage.stats()["sealed_segments"] == 1
    storage.write(msgspec.json.encode({"trace_sequence": 2}) + b"\n")
    storage.close()
    assert verify_closed_request_trace(storage.manifest) == 2


def test_corrupt_or_missing_segment_invalidates_evidence(tmp_path: Path) -> None:
    path = tmp_path / "requests.jsonl"
    storage = RotatingRequestTraceFile(path, journal_id="j", segment_bytes=512)
    storage.write(msgspec.json.encode({"trace_sequence": 1}) + b"\n")
    storage.close()
    with path.open("ab") as target:
        target.write(b"corrupt\n")
    with pytest.raises((ValueError, msgspec.DecodeError)):
        verify_closed_request_trace(storage.manifest)


def test_segment_budget_and_sequence_rejection_are_bounded(tmp_path: Path) -> None:
    storage = RotatingRequestTraceFile(
        tmp_path / "requests.jsonl", journal_id="j", segment_bytes=256, maximum_segments=2
    )
    try:
        with pytest.raises(ValueError):
            storage.write(b'{"trace_sequence":2}\n')
        n = 1
        with pytest.raises(OSError, match="budget"):
            while True:
                storage.write(
                    msgspec.json.encode({"trace_sequence": n, "padding": "x" * 100}) + b"\n"
                )
                n += 1
        assert len(list(tmp_path.glob("*.jsonl"))) <= 2
    finally:
        storage.close()


def test_structured_append_encodes_once_without_reparsing_and_retains_raw_format(tmp_path):
    storage = RotatingRequestTraceFile(
        tmp_path / "requests.jsonl", journal_id="j", segment_bytes=512
    )
    expected = []
    try:
        with patch(
            "xuanling_tradecore.persistence.rotating_request_trace.msgspec.json.decode",
            side_effect=AssertionError("structured record reparsed"),
        ):
            for n in range(1, 7):
                row = {"trace_sequence": n, "operation": "test", "clock": n}
                line = msgspec.json.encode(row, order="deterministic") + b"\n"
                assert storage.append_record(row, maximum_remaining=10_000) == len(line)
                expected.append(line)
    finally:
        storage.close()
    assert verify_closed_request_trace(storage.manifest) == 6
    manifest = json.loads(storage.manifest.read_text())
    assert b"".join((tmp_path / r["file"]).read_bytes() for r in manifest["segments"]) == b"".join(expected)


def test_structured_quota_does_not_consume_sequence_and_bad_sequence_still_fails(tmp_path):
    storage = RotatingRequestTraceFile(
        tmp_path / "requests.jsonl", journal_id="j", segment_bytes=512
    )
    try:
        assert storage.append_record({"trace_sequence": 1}, maximum_remaining=1) == 0
        assert storage.append_record({"trace_sequence": 1}, maximum_remaining=100)
        with pytest.raises(ValueError, match="sequence"):
            storage.append_record({"trace_sequence": 3}, maximum_remaining=100)
    finally:
        storage.close()
    with pytest.raises(ValueError, match="unsealed"):
        verify_closed_request_trace(storage.manifest)
