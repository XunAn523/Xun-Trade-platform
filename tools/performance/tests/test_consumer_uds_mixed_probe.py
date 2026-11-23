from __future__ import annotations

from dataclasses import replace
from threading import Event
from time import monotonic_ns
from unittest.mock import patch

from consumer_uds_mixed_probe import (
    _aggregate_s1_shards,
    _aggregate_s3_shards,
    _await_start_gate,
    _bounded_queue_capacity,
    _checkpoint_stage_durations_ms,
    _Common,
    _evaluate,
    _parse_cpu_set,
    _partition_cpu_lane,
    _percentile,
    _position_key,
    _profile_name,
    _resolve_cpu_lanes,
    _resolve_s3_queue_capacity,
    _run_s2,
    _StartGate,
    _write_complete_checkpoint,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1


def test_checkpoint_stages_are_per_poll_and_require_ordered_boundaries() -> None:
    value = _checkpoint_stage_durations_ms(0, 1_000_000, 3_000_000, 6_000_000, 10_000_000, 15_000_000)
    assert value == {
        "scheduled_to_call": 1.0,
        "call_to_response": 2.0,
        "response_to_write": 3.0,
        "write_call": 4.0,
        "fdatasync_call": 5.0,
        "scheduled_to_checkpoint": 15.0,
    }
    for invalid in ((0, 2, 1, 3, 4, 5), (False, 1, 2, 3, 4, 5)):
        try:
            _checkpoint_stage_durations_ms(*invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid checkpoint timestamps were accepted")


def test_checkpoint_short_write_is_rejected() -> None:
    with patch("consumer_uds_mixed_probe.os.write", return_value=2):
        try:
            _write_complete_checkpoint(7, b"hello")
        except OSError:
            pass
        else:
            raise AssertionError("partial checkpoint was accepted")
    with patch("consumer_uds_mixed_probe.os.write", return_value=5) as writer:
        _write_complete_checkpoint(7, b"hello")
        writer.assert_called_once_with(7, b"hello")


def test_s2_checkpoint_failure_fences_partition_before_next_cursor(tmp_path) -> None:
    position = ConsumerEventPositionV2(RedisStreamIdV1(12, 34), 5).to_external_id()
    calls: list[dict] = []

    class FakeClient:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def call(self, request):
            calls.append(request)
            return (
                {
                    "status": "COMPLETED",
                    "payload": {
                        "events": [{"event_id": "event-1", "position": position}],
                        "next_cursor": "next-signed-token",
                    },
                },
                monotonic_ns(),
            )

        def close(self) -> None:
            pass

    common = _Common("socket", "account", "strategy", "day", "run", monotonic_ns(), 1, "out", "mode")
    with (
        patch("consumer_uds_mixed_probe._WindowClient", FakeClient),
        patch("consumer_uds_mixed_probe._await_start_gate", return_value=(common, 0.0)),
        patch(
            "consumer_uds_mixed_probe.os.fdatasync",
            side_effect=OSError("injected failure"),
            create=True,
        ),
    ):
        result = _run_s2(common, 200, 100, [{"cursor": "original-token"}], str(tmp_path / "p"), None)
    assert len(calls) == 1
    assert calls[0]["payload"]["cursor"] == "original-token"
    assert result["checkpointed_events"] == 0
    assert result["error_counts"]["OSError"] == 1
    assert result["error_counts"]["CHECKPOINT_PARTITION_FAILED_CLOSED"] == 1


def test_percentile_and_position_key_are_deterministic() -> None:
    assert _percentile([4.0, 1.0, 2.0, 3.0], 0.50) == 2.0
    assert _percentile([], 0.99) is None
    value = ConsumerEventPositionV2(RedisStreamIdV1(12, 34), 5).to_external_id()
    assert _position_key(value) == (12, 34, 5)
    assert _bounded_queue_capacity(64, 50) == 128
    assert _profile_name("fresh-per-request") == "UDS-S5-MIX-PAGED-FRESH"
    assert _profile_name("persistent-per-vu") == "UDS-S5-MIX-PAGED-PERSISTENT"


def test_generator_cpu_lanes_are_explicit_bounded_and_disjoint() -> None:
    assert _parse_cpu_set("80-82,85") == (80, 81, 82, 85)
    lanes = _resolve_cpu_lanes(
        "80-85",
        "86-91",
        "92-95",
        allowed=set(range(80, 96)),
    )
    assert lanes == {
        "s1": tuple(range(80, 86)),
        "s2": tuple(range(86, 92)),
        "s3": tuple(range(92, 96)),
    }


def test_generator_cpu_lanes_reject_partial_overlap_and_cgroup_widening() -> None:
    for values in (
        ("80", None, "82"),
        ("80-82", "82-84", "85"),
        ("80", "81", "96"),
    ):
        try:
            _resolve_cpu_lanes(*values, allowed=set(range(80, 96)))
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid CPU lanes were accepted: {values}")


def test_s3_process_cpu_partitions_are_disjoint_and_complete() -> None:
    partitions = _partition_cpu_lane(tuple(range(86, 96)), 5)
    assert partitions == ((86, 91), (87, 92), (88, 93), (89, 94), (90, 95))
    assert sorted(cpu for partition in partitions for cpu in partition) == list(range(86, 96))
    try:
        _partition_cpu_lane((92, 93, 94, 95), 5)
    except ValueError:
        pass
    else:
        raise AssertionError("accepted more S3 processes than dedicated CPUs")


def test_start_gate_publishes_t0_only_after_child_is_ready() -> None:
    common = _Common("socket", "account", "strategy", "day", "run", 0, 1, "out", "mode")
    ready = Event()
    release = Event()

    class Shared:
        value = monotonic_ns() + 1_000_000_000

    release.set()
    synchronized, lead_ms = _await_start_gate(common, _StartGate(ready, release, Shared()))
    assert ready.is_set()
    assert synchronized == replace(common, start_ns=Shared.value)
    assert lead_ms > 900


def test_s3_queue_capacity_is_bounded_to_one_tenth_second() -> None:
    assert _resolve_s3_queue_capacity(64, 50, None) == 128
    assert _resolve_s3_queue_capacity(64, 50, 256) == 256
    for invalid in (127, 501):
        try:
            _resolve_s3_queue_capacity(64, 50, invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid S3 queue capacity was accepted: {invalid}")


def test_s3_shard_aggregation_preserves_counts_and_raw_percentiles() -> None:
    shards = []
    for index in range(2):
        shards.append(
            {
                "profile": "S3",
                "target_qps": 2_500,
                "workers": 16,
                "offered": 4,
                "completed": 4,
                "completed_in_window": 4,
                "errors": 0,
                "error_counts": {},
                "release_errors": 0,
                "queue_capacity": 32,
                "max_queue_depth": 2,
                "call_start_samples": 4,
                "drain_ms": float(index),
                "load_generator_automatic_gc_disabled": True,
                "load_generator_cpu_affinity": [90 + index],
                "page_cursor_sha256": "same",
                "shard_index": index,
                "_latency_samples_ms": [1.0 + index, 2.0, 3.0, 4.0],
                "_queue_delay_samples_ms": [0.1, 0.2, 0.3, 0.4 + index],
                "ready_before_start_ms": 1_500.0 + index,
            }
        )
    result = _aggregate_s3_shards(shards)
    assert result["target_qps"] == 5_000
    assert result["offered"] == result["completed"] == 8
    assert result["call_start_samples"] == 8
    assert result["latency_ms"]["p100"] == 4.0
    assert result["offered_to_call_start_ms"]["p100"] == 1.4
    assert result["load_generator_cpu_affinity"] == [90, 91]
    assert result["ready_before_start_ms"] == 1_500.0


def test_s1_shard_aggregation_preserves_unique_lsns_and_percentiles() -> None:
    shards = []
    for index in range(2):
        shards.append(
            {
                "profile": "S1",
                "target_tps": 1_000,
                "workers": 16,
                "offered": 2,
                "completed": 2,
                "completed_in_window": 2,
                "errors": 0,
                "error_counts": {},
                "release_errors": 0,
                "queue_capacity": 32,
                "max_queue_depth": 2,
                "call_start_samples": 2,
                "drain_ms": float(index),
                "load_generator_automatic_gc_disabled": True,
                "load_generator_cpu_affinity": [80 + index],
                "shard_index": index,
                "_latency_samples_ms": [1.0 + index, 2.0],
                "_queue_delay_samples_ms": [0.1, 0.2 + index],
                "_accepted_values": [index * 2 + 1, index * 2 + 2],
                "ready_before_start_ms": 1_500.0 + index,
            }
        )
    result = _aggregate_s1_shards(shards)
    assert result["target_tps"] == 2_000
    assert result["offered"] == result["completed"] == 4
    assert result["unique_journal_lsns"] == 4
    assert result["latency_ms"]["p100"] == 2.0
    assert result["offered_to_call_start_ms"]["p100"] == 1.2
    assert result["load_generator_cpu_affinity"] == [80, 81]
    assert result["ready_before_start_ms"] == 1_500.0


def test_evaluate_requires_all_counts_integrity_and_latency_gates() -> None:
    duration = 30
    s1 = {
        "completed": 2_000 * duration,
        "completed_in_window": 2_000 * duration,
        "drain_ms": 0.5,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 2_000 * duration,
        "offered": 2_000 * duration,
        "offered_to_call_start_ms": {"p99": 1.0},
        "latency_ms": {"p95": 19.0, "p99": 59.0},
    }
    s2 = {
        "checkpointed_in_window": 5_000 * duration,
        "checkpointed_events": 5_000 * duration,
        "drain_ms": 0.5,
        "unique_events": 5_000 * duration,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 1_500,
        "offered_poll_requests": 1_500,
        "offered_to_call_start_ms": {"p99": 1.0},
        "checkpoint_latency_ms": {"p95": 9.0, "p99": 39.0},
    }
    s3 = {
        "completed": 5_000 * duration,
        "completed_in_window": 5_000 * duration,
        "drain_ms": 0.5,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 5_000 * duration,
        "offered": 5_000 * duration,
        "offered_to_call_start_ms": {"p99": 1.0},
        "latency_ms": {"p95": 9.0, "p99": 39.0},
    }
    assert _evaluate(s1, s2, s3, duration)["passed"] is True
    s2_target_2k = {**s2, "checkpointed_events": 60_000, "unique_events": 60_000}
    s3_target_2k = {**s3, "completed": 60_000}
    assert _evaluate(s1, s2_target_2k, s3_target_2k, duration)["passed"] is False
    assert (
        _evaluate(s1, s2_target_2k, s3_target_2k, duration, s2_eps=2_000, s3_qps=2_000)["passed"]
        is True
    )
    s2_target_2k["unique_events"] = 59_999
    assert (
        _evaluate(s1, s2_target_2k, s3_target_2k, duration, s2_eps=2_000, s3_qps=2_000)["passed"]
        is False
    )
    s2["unique_events"] = 149_999
    assert _evaluate(s1, s2, s3, duration)["passed"] is False


def test_evaluate_allows_only_latency_bounded_final_release_drain() -> None:
    duration = 30
    s1 = {
        "completed": 60_000,
        "completed_in_window": 59_997,
        "drain_ms": 0.9,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 60_000,
        "offered": 60_000,
        "offered_to_call_start_ms": {"p99": 1.0},
        "latency_ms": {"p95": 9.0, "p99": 12.0},
    }
    s2 = {
        "checkpointed_events": 150_000,
        "checkpointed_in_window": 150_000,
        "unique_events": 150_000,
        "drain_ms": 0.0,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 1_500,
        "offered_poll_requests": 1_500,
        "offered_to_call_start_ms": {"p99": 1.0},
        "checkpoint_latency_ms": {"p95": 7.0, "p99": 9.0},
    }
    s3 = {
        "completed": 150_000,
        "completed_in_window": 149_994,
        "drain_ms": 1.0,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 150_000,
        "offered": 150_000,
        "offered_to_call_start_ms": {"p99": 1.0},
        "latency_ms": {"p95": 9.0, "p99": 13.0},
    }
    assert _evaluate(s1, s2, s3, duration)["passed"] is True
    s3["drain_ms"] = 40.001
    assert _evaluate(s1, s2, s3, duration)["passed"] is False


def test_evaluate_rejects_generator_queue_debt() -> None:
    duration = 1
    s1 = {
        "completed": 2_000,
        "drain_ms": 1.0,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 2_000,
        "offered": 2_000,
        "offered_to_call_start_ms": {"p99": 10.001},
        "latency_ms": {"p95": 9.0, "p99": 12.0},
    }
    s2 = {
        "checkpointed_events": 5_000,
        "unique_events": 5_000,
        "drain_ms": 1.0,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 50,
        "offered_poll_requests": 50,
        "offered_to_call_start_ms": {"p99": 1.0},
        "checkpoint_latency_ms": {"p95": 7.0, "p99": 9.0},
    }
    s3 = {
        "completed": 5_000,
        "drain_ms": 1.0,
        "errors": 0,
        "release_errors": 0,
        "call_start_samples": 5_000,
        "offered": 5_000,
        "offered_to_call_start_ms": {"p99": 1.0},
        "latency_ms": {"p95": 9.0, "p99": 13.0},
    }
    result = _evaluate(s1, s2, s3, duration)
    assert result["checks"]["s1_generator"] is False
    assert result["passed"] is False
