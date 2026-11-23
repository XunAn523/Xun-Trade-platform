"""Reports retain real ordering for legacy and pre-dispatch decoding."""

import json

from tools.performance.consumer_uds_trace_report import report


def write_trace(tmp_path, rows):
    path = tmp_path / "requests.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    path.with_suffix(".status.json").write_text(
        json.dumps(
            {
                "writer_failed": False,
                "queue_dropped": 0,
                "capacity_dropped": 0,
                "queue_depth": 0,
                "submitted": len(rows),
                "written": len(rows),
            }
        ),
        encoding="utf-8",
    )
    return path


def row(pre_dispatch):
    return {
        "operation": "PollEvents",
        "outcome": "sent",
        "response_kind": "RESPONSE",
        "first_byte_observed_ns": 1,
        "frame_ready_ns": 2,
        "frame_decoded_ns": 3 if pre_dispatch else 4,
        "dispatch_started_ns": 4 if pre_dispatch else 3,
        "schema_validated_ns": 5,
        "send_finished_ns": 10,
        "decode_before_dispatch": pre_dispatch,
    }


def test_mixed_trace_layouts_keep_nonnegative_stages(tmp_path):
    result = report(write_trace(tmp_path, [row(False), row(True)]), expected_count=2)
    assert result["request_count_coverage_pass"]
    assert result["stages"]["session_and_decode"]["count"] == 1
    assert result["stages"]["frame_decode"]["count"] == 1
    assert result["stages"]["ready_queue"]["count"] == 2
    assert all(stage["negative_pairs"] == 0 for stage in result["stages"].values())


def test_invalid_times_are_not_sorted_into_a_passing_report(tmp_path):
    invalid = row(True)
    invalid["frame_decoded_ns"] = 1
    result = report(write_trace(tmp_path, [invalid]), expected_count=1)
    assert not result["request_count_coverage_pass"]
    assert result["stages"]["frame_decode"]["negative_pairs"] == 1
