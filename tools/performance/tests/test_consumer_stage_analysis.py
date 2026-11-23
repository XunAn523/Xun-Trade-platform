import json
from unittest.mock import patch

import pytest
from analyze_consumer_stage_trace import analyze
from xuanling_tradecore.consumer.request_trace_v2 import BoundedRequestTraceWriterV2


def transaction(first=1, final=2):
    return dict(
        schema="consumer-stage-timing-v1",
        component="projector",
        start_ns=100,
        end_ns=1000,
        inclusive_stages=True,
        calls={"sql_commit": 1},
        total_ns={"sql_commit": 100},
        maximum_ns={"sql_commit": 100},
        values=dict(
            committed=True,
            committed_facts=final - first + 1,
            committed_batches=1,
            first_lsn=first,
            final_lsn=final,
        ),
    )


def fixture(tmp_path, rows):
    path = tmp_path / "stages.jsonl"
    with patch(
        "xuanling_tradecore.consumer.request_trace_v2.monotonic_clock_identity",
        return_value="a" * 64,
    ):
        writer = BoundedRequestTraceWriterV2(
            path,
            journal_id="fixture",
            rotation_bytes=65536,
            record_schema="consumer-stage-trace-v1",
        )
    for row in rows:
        writer.submit(row)
    writer.close()
    return path.with_suffix(".manifest.json")


def test_stage_quantiles_are_group_totals_and_source_coverage_is_separate(tmp_path):
    path = fixture(tmp_path, [transaction()])
    report = analyze(
        [path], expected_clock="a" * 64, expected_journal="fixture", expected_final_lsn=2
    )
    assert report["observation_integrity_verified"] and report["source_prefix_complete"]
    assert report["committed_facts"] == 2 and report["stages"]["projector.sql_commit"]["calls"] == 1
    assert report["runtime_payload_identity"] == "NOT_VERIFIED_BY_THIS_ANALYZER"
    assert report["performance_status"] == "NOT_EVALUATED" and not report["formal_qualification"]
    partial = analyze(
        [path], expected_clock="a" * 64, expected_journal="fixture", expected_final_lsn=4
    )
    assert partial["source_prefix_complete"] is False and partial["observed_committed_lsn"] == 2


@pytest.mark.parametrize("change", ["clock", "journal", "drop", "truncated"])
def test_clock_scope_drop_and_corrupt_hash_rejected(tmp_path, change):
    path = fixture(tmp_path, [transaction()])
    clock, journal = "a" * 64, "fixture"
    if change == "clock":
        clock = "b" * 64
    if change == "journal":
        journal = "wrong"
    if change == "drop":
        status_path = tmp_path / "stages.status.json"
        status = json.loads(status_path.read_text())
        status["queue_dropped"] = 1
        status_path.write_text(json.dumps(status))
    if change == "truncated":
        raw = tmp_path / "stages.jsonl"
        raw.write_bytes(raw.read_bytes()[:-1])
    with pytest.raises(ValueError):
        analyze([path], expected_clock=clock, expected_journal=journal, expected_final_lsn=2)


@pytest.mark.parametrize("rows", [[transaction(), transaction()], [transaction(3, 4)]])
def test_duplicate_or_missing_commit_range_cannot_prove_prefix(tmp_path, rows):
    path = fixture(tmp_path, rows)
    with pytest.raises(ValueError):
        analyze([path], expected_clock="a" * 64, expected_journal="fixture", expected_final_lsn=4)


def test_reversed_duration_is_not_silently_clamped(tmp_path):
    row = transaction()
    row["end_ns"] = 1
    path = fixture(tmp_path, [row])
    with pytest.raises(ValueError):
        analyze([path], expected_clock="a" * 64, expected_journal="fixture", expected_final_lsn=2)
