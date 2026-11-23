"""Acceptance-level unit coverage for the isolated OMS HTTP load service."""

from __future__ import annotations

import asyncio
import json

import pytest
from tools.oms_load_platform.run_w7_parallel import (
    W7GateError,
    _assert_emt_499_report,
    _assert_emt_command_output,
)
from tools.oms_load_platform.server import _parser
from tools.oms_load_platform.service_v1 import (
    OmsFactInputV1,
    OmsLoadPlatformSettingsV1,
    OmsLoadPlatformV1,
    OmsProjectorPausedError,
    OmsSnapshotStaleError,
)


def _run(coroutine: object) -> object:
    return asyncio.run(coroutine)  # type: ignore[arg-type]


async def _runtime(*, seed_orders: int = 8) -> OmsLoadPlatformV1:
    runtime = OmsLoadPlatformV1(
        OmsLoadPlatformSettingsV1(seed_orders=seed_orders, snapshot_stale_after_ms=10_000)
    )
    await runtime.start()
    return runtime


def test_fact_ingest_projects_immutable_snapshot_and_hdr_metrics() -> None:
    async def exercise() -> None:
        runtime = await _runtime(seed_orders=0)
        try:
            receipt = await runtime.ingest(
                OmsFactInputV1(
                    scope_id="bench-scope-0",
                    execution_id="test-execution-1",
                    canonical_client_order_ref="test-client-ref-1",
                )
            )
            assert receipt.position == 1
            assert await runtime.wait_until_idle()
            assert runtime.order("test-execution-1")["state"] == "PRE_NATIVE_COMMITTED"  # type: ignore[index]
            snapshot = runtime.snapshot_summary(require_fresh=True)
            assert snapshot["watermarks"]["bench-scope-0"] == 1  # type: ignore[index]
            metrics = runtime.metrics.summary()
            assert metrics["format"] == "HDRHistogramV1"
            assert metrics["histograms_ns"]["fact_admit_to_projected_ns"]["count"] == 1  # type: ignore[index]
        finally:
            await runtime.stop()

    _run(exercise())


def test_replay_matches_live_snapshot() -> None:
    async def exercise() -> None:
        runtime = await _runtime(seed_orders=4)
        try:
            for index in range(12):
                await runtime.ingest(
                    OmsFactInputV1(
                        scope_id=f"bench-scope-{index % 4}",
                        execution_id=f"replay-execution-{index}",
                        canonical_client_order_ref=f"replay-client-ref-{index}",
                    )
                )
            result = await runtime.verify_replay()
            assert result["status"] == "PASS"
        finally:
            await runtime.stop()

    _run(exercise())


def test_pause_makes_snapshot_stale_and_rejects_new_fact_until_resumed() -> None:
    async def exercise() -> None:
        runtime = await _runtime(seed_orders=0)
        try:
            runtime.pause_projector("bench-scope-0")
            with pytest.raises(OmsSnapshotStaleError):
                runtime.snapshot_summary(require_fresh=True)
            with pytest.raises(OmsProjectorPausedError):
                await runtime.ingest(
                    OmsFactInputV1(
                        scope_id="bench-scope-0",
                        execution_id="paused-execution-1",
                        canonical_client_order_ref="paused-client-ref-1",
                    )
                )
            runtime.resume_projector("bench-scope-0")
            receipt = await runtime.ingest(
                OmsFactInputV1(
                    scope_id="bench-scope-0",
                    execution_id="resumed-execution-1",
                    canonical_client_order_ref="resumed-client-ref-1",
                )
            )
            assert receipt.position == 1
        finally:
            await runtime.stop()

    _run(exercise())


def test_benchmark_reset_restores_seeded_state_and_replay_boundary() -> None:
    async def exercise() -> None:
        runtime = await _runtime(seed_orders=4)
        try:
            await runtime.ingest(
                OmsFactInputV1(
                    scope_id="bench-scope-0",
                    execution_id="pre-reset-execution",
                    canonical_client_order_ref="pre-reset-client-ref",
                )
            )
            result = await runtime.reset_benchmark_state()
            assert result["status"] == "RESET"
            assert runtime.order("pre-reset-execution") is None
            assert runtime.order("seed-000000") is not None
            receipt = await runtime.ingest(
                OmsFactInputV1(
                    scope_id="bench-scope-0",
                    execution_id="post-reset-execution",
                    canonical_client_order_ref="post-reset-client-ref",
                )
            )
            assert receipt.position == 2
            assert (await runtime.verify_replay())["status"] == "PASS"
        finally:
            await runtime.stop()

    _run(exercise())


def test_w7_requires_complete_emt_499_lifecycle_evidence(tmp_path) -> None:
    report_path = tmp_path / "emt-499.json"
    payload = {
        "burst_width": 499,
        "complete": True,
        "outcomes": {
            "native_accepted": 499,
            "callback_completed_C7": 499,
            "callback_seen_downstream": 499,
            "callback_complete_C5_to_C7_pairs": 499,
            "callback_gaps": 0,
            "cleanup_cancel_attempted": 499,
            "cleanup_cancel_accepted": 499,
            "cleanup_terminal_callback_complete": True,
            "cleanup_terminal_inflight_remaining": 0,
        },
        "verification": {"full_acceptance_complete": True},
    }
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    assert _assert_emt_499_report(report_path)["native_accepted"] == 499
    report_path.write_text("{}", encoding="utf-8")
    with pytest.raises(W7GateError):
        _assert_emt_499_report(report_path)


def test_oms_server_uses_explicit_high_concurrency_listener_backlog() -> None:
    """W3/W7 must not lose valid clients during the k6 connection ramp."""

    args = _parser().parse_args([])

    assert args.backlog == 8_192


def test_w7_rejects_a_live_emt_command_with_a_mismatched_result_path(tmp_path) -> None:
    expected_report = tmp_path / "expected-emt-499.json"

    _assert_emt_command_output(("python", "--output", str(expected_report)), expected_report)

    with pytest.raises(W7GateError, match="does not match"):
        _assert_emt_command_output(
            ("python", "--output", str(tmp_path / "different-emt-499.json")),
            expected_report,
        )
