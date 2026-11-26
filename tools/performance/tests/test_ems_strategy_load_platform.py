"""Contract tests for BenchmarkStandardV1 EMS and strategy load surfaces."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from tools.ems_strategy_load_platform.run_hot_capacity import (
    _rates,
    _summary_metrics,
    _summary_threshold_failures,
)
from tools.ems_strategy_load_platform.run_w7_ems_str import (
    _base_urls,
    _cpu_list,
    _k6_api_address,
)
from tools.ems_strategy_load_platform.server import create_app
from tools.ems_strategy_load_platform.service_v1 import (
    EmsAdmissionPausedError,
    EmsPlanInputV1,
    EmsStrategyLoadPlatformV1,
    StrategyConsumerPausedError,
    StrategyEventGapError,
    StrategyEventInputV1,
)


def test_ems_plan_admission_uses_real_control_plane_and_routes_a_child() -> None:
    async def scenario() -> None:
        runtime = EmsStrategyLoadPlatformV1()
        await runtime.start()
        try:
            result = await runtime.submit_plan(EmsPlanInputV1("plan-a", wait_for_child=True))
            assert result["status"] == "READY"
            assert result["child_count"] == 1
            assert result["shard_ids"]
            snapshot = runtime.plan_snapshot()
            assert snapshot["plan_count"] == runtime.settings.seed_plans + 1
            assert snapshot["ready_plan_count"] == runtime.settings.seed_plans + 1
            metrics = runtime.metrics.summary()
            histograms = metrics["histograms_ns"]
            assert histograms["plan_submit_to_admitted_ns"]["count"] == 1
            assert histograms["plan_admitted_to_child_instruction_ready_ns"]["count"] == 1
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_strategy_signal_uses_gateway_authorization_before_ems_admission() -> None:
    async def scenario() -> None:
        runtime = EmsStrategyLoadPlatformV1()
        await runtime.start()
        try:
            result = await runtime.submit_strategy_signal("strategy-a", wait_for_child=True)
            assert result["status"] == "READY"
            assert result["execution_id"] == "perf-exec-strategy-a"
            counters = runtime.metrics.summary()["counters"]
            assert counters["strategy_signal_admitted_total"] == 1
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_consumer_checkpoint_is_ordered_idempotent_and_fail_closed_when_paused() -> None:
    runtime = EmsStrategyLoadPlatformV1()
    first = runtime.consume_strategy_event(StrategyEventInputV1("consumer-a", "scope-a", 1, "e1"))
    assert first["status"] == "CHECKPOINTED"
    assert (
        runtime.consume_strategy_event(StrategyEventInputV1("consumer-a", "scope-a", 1, "e1"))[
            "status"
        ]
        == "DUPLICATE"
    )
    with pytest.raises(StrategyEventGapError):
        runtime.consume_strategy_event(StrategyEventInputV1("consumer-a", "scope-a", 3, "e3"))
    runtime.control_strategy("pause")
    with pytest.raises(StrategyConsumerPausedError):
        runtime.consume_strategy_event(StrategyEventInputV1("consumer-a", "scope-a", 2, "e2"))
    runtime.control_strategy("resume")
    assert (
        runtime.consume_strategy_event(StrategyEventInputV1("consumer-a", "scope-a", 2, "e2"))[
            "checkpoint"
        ]
        == 2
    )


def test_pause_rejects_ems_admission_and_reset_requires_open_paths() -> None:
    async def scenario() -> None:
        runtime = EmsStrategyLoadPlatformV1()
        await runtime.start()
        try:
            runtime.control_ems("pause")
            with pytest.raises(EmsAdmissionPausedError):
                await runtime.submit_plan(EmsPlanInputV1("paused"))
            with pytest.raises(Exception, match="paused"):
                await runtime.reset_benchmark_state()
            runtime.control_ems("resume")
            assert (await runtime.reset_benchmark_state())["status"] == "RESET"
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_child_worker_failure_is_never_reported_as_ready() -> None:
    async def scenario() -> None:
        runtime = EmsStrategyLoadPlatformV1()
        await runtime.start()
        try:
            # A malformed routing snapshot is injected only after normal
            # startup to prove the HTTP/service contract is fail-closed.
            runtime._fresh_snapshot = lambda: None
            with pytest.raises(Exception, match="child preparation failed"):
                await runtime.submit_plan(EmsPlanInputV1("worker-failure", wait_for_child=True))
        finally:
            await runtime.stop()

    asyncio.run(scenario())


def test_http_contract_exposes_ems_strategy_and_fault_boundaries() -> None:
    app = create_app(enable_faults=True, fault_key="test-key")
    paths = {route.path for route in app.routes}
    assert {
        "/perf/v1/ems/plans",
        "/perf/v1/strategy/signals",
        "/perf/v1/strategy/events",
        "/perf/v1/ems/snapshot",
        "/perf/v1/strategy/snapshot",
        "/perf/v1/admin/{family}/{action}",
        "/perf/v1/metrics/hdr",
    }.issubset(paths)


def test_health_exposes_the_sidecar_open_file_limit_when_linux_provides_it() -> None:
    health = EmsStrategyLoadPlatformV1().health()
    limit = health["open_file_soft_limit"]
    assert limit is None or isinstance(limit, int)


def test_joint_w7_requires_explicit_oms_shards() -> None:
    assert _base_urls("http://127.0.0.1:18080,http://127.0.0.1:18081") == (
        "http://127.0.0.1:18080",
        "http://127.0.0.1:18081",
    )
    with pytest.raises(RuntimeError, match="oms-base-url"):
        _base_urls(" , ")


def test_hot_capacity_rates_are_strictly_increasing_positive_steps() -> None:
    assert _rates("2000,4000,8000") == (2000, 4000, 8000)
    with pytest.raises(Exception, match="strictly increasing"):
        _rates("2000,2000")
    with pytest.raises(Exception, match="positive"):
        _rates("0,1000")


def test_hot_capacity_summary_uses_the_k6_summary_export_schema(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_text(
        '{"metrics":{"http_reqs":{"count":60,"rate":2.0},'
        '"http_req_failed":{"passes":0,"fails":60,"value":0.0},'
        '"dropped_iterations":{"count":0,"rate":0.0}}}',
        encoding="utf-8",
    )
    assert _summary_metrics(path)["http_reqs"] == {"count": 60, "rate": 2.0}


def test_hot_capacity_rejects_a_crossed_k6_summary_threshold(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    path.write_text(
        '{"metrics":{"http_req_duration{workload:test}":'
        '{"thresholds":{"p(99)<40":true}},'
        '"http_req_failed":{"thresholds":{"rate==0":false}}}}',
        encoding="utf-8",
    )
    assert _summary_threshold_failures(path) == ["http_req_duration{workload:test}:p(99)<40"]


def test_joint_w7_accepts_only_taskset_cpu_lists() -> None:
    assert _cpu_list("150-159") == "150-159"
    assert _cpu_list(None) is None
    with pytest.raises(RuntimeError, match="ems-k6-cpu-list"):
        _cpu_list("150;159")


def test_joint_w7_requires_a_valid_loopback_k6_control_address() -> None:
    assert _k6_api_address("127.0.0.1:6567") == "127.0.0.1:6567"
    with pytest.raises(RuntimeError, match="ems-k6-api-address"):
        _k6_api_address("0.0.0.0:6567")
