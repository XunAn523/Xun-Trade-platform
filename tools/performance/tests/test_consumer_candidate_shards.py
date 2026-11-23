import pytest
from hdrh.histogram import HdrHistogram
from tools.performance.consumer_uds_candidate_shards import merge_reports


def report(values):
    h = HdrHistogram(1, 60_000_000_000, 3)
    for value in values:
        h.record_value(value)
    return {
        "start_ns": 1,
        "end_ns": 1_000_000_001,
        "planned_each": len(values),
        "formal_qualification": False,
        "target_kind": "TRADECORE_PUBLIC_UDS",
        "tool_revision": "a" * 40,
        "clock_identity_sha256": "b" * 64,
        "release_clock": {
            "schema": "completion-aware-100hz-v1",
            "completion_reserve_ns": 60_000_000,
            "first_release_ns": 1,
            "last_release_ns": 940_000_001,
            "observation_window_ns": 1_000_000_000,
            "planned_each": len(values),
            "window_average_tps": len(values),
        },
        "counts": {"completed": len(values)},
        "hdr_encoded": {"scheduled": h.encode().decode("ascii")},
        "raw_max_ns": {"scheduled": max(values)},
        "unissued": 0,
        "failed": False,
    }


def test_hdr_merge_uses_samples_not_average_of_child_percentiles():
    merged = merge_reports([report([100] * 99), report([10000])], start_ns=1, planned_each=100)
    assert merged["histograms"]["scheduled"]["count"] == 100
    assert merged["histograms"]["scheduled"]["p95_ns"] < 200
    assert merged["raw_max_ns"]["scheduled"] == 10000
    assert merged["counts"]["completed"] == 100
    assert merged["release_clock"]["planned_each"] == 100
    assert merged["release_clock"]["window_average_tps"] == 100


def test_child_wrong_window_cannot_be_merged_into_qualification():
    with pytest.raises(ValueError):
        merge_reports([report([100])], start_ns=2, planned_each=1)
    with pytest.raises(ValueError):
        merge_reports([], start_ns=1, planned_each=0)
