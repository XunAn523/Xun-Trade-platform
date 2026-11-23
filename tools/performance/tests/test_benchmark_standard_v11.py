from __future__ import annotations

from tools.performance.benchmark_standard_v11 import evaluate_preflight, validate_gate_evidence


def _profile(*, external_callback_required: bool) -> dict[str, object]:
    return {
        "schema": "BenchmarkProfileV11",
        "system_id": "test",
        "profile_id": "test-v11",
        "measurement_schema_version": "V1.1",
        "external_callback_required": external_callback_required,
        "g_boundaries": {f"G{index}": f"boundary-{index}" for index in range(8)},
        "topology": {"maximum_release_skew_ns": 10},
        "load_shape": {"shape": "test"},
        "data_distribution": {
            "seed": "1",
            "schema_version": "1",
            "payload_classes": "all",
            "uniform_key_contract": "uniform",
            "hot_key_contract": "hot",
            "idempotency_contract": "none",
            "invalid_input_contract": "none",
        },
        "durable_fence": {"G2": "test"},
        "acceptance": {"three": True},
    }


def _evidence(gate: str) -> dict[str, object]:
    return {
        "schema": "BenchmarkGateEvidenceV11",
        "profile_id": "test-v11",
        "gate": gate,
        "status": "PASS",
        "evidence": {"source": "test"},
    }


def test_v11_preflight_passes_only_with_complete_matching_evidence() -> None:
    result = evaluate_preflight(
        profile=_profile(external_callback_required=True),
        git_revision="a" * 40,
        git_clean=True,
        clock={
            "schema": "BenchmarkClockCalibrationV11",
            "profile_id": "test-v11",
            "status": "PASS",
            "monotonic_clock": "monotonic_ns",
            "maximum_observed_release_skew_ns": 10,
            "timestamp_read_overhead_ns": {"p50_ns": 1, "p100_ns": 2},
        },
        gate_evidence={
            gate: _evidence(gate)
            for gate in ("V2-GENERATOR", "V3-ENVIRONMENT", "V4-CALLBACK", "V5-OBSERVABILITY")
        },
    )
    assert result["status"] == "PASS"
    assert result["performance_conclusion_permitted"] is True


def test_v11_preflight_blocks_missing_callback_or_dirty_revision() -> None:
    result = evaluate_preflight(
        profile=_profile(external_callback_required=True),
        git_revision="a" * 40,
        git_clean=False,
        clock=None,
        gate_evidence={"V2-GENERATOR": _evidence("V2-GENERATOR")},
    )
    assert result["status"] == "BLOCKED"
    assert result["gates"]["V0-PROFILE"]["status"] == "BLOCKED"
    assert result["gates"]["V4-CALLBACK"]["status"] == "BLOCKED"
    assert not validate_gate_evidence(
        _evidence("V2-GENERATOR"), profile_id="other", gate="V2-GENERATOR"
    )
