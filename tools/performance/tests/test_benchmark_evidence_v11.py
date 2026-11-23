from __future__ import annotations

import json

import pytest
from tools.performance.benchmark_evidence_v11 import (
    BenchmarkEvidenceV11Error,
    emt_gate_evidence,
    platform_gate_evidence,
    write_platform_gate_evidence,
)


def _profile(*, callbacks: bool = False) -> dict[str, object]:
    return {
        "schema": "BenchmarkProfileV11",
        "system_id": "test",
        "profile_id": "test-profile-v11",
        "measurement_schema_version": "V1.1",
        "external_callback_required": callbacks,
        "g_boundaries": {f"G{number}": f"boundary-{number}" for number in range(8)},
        "topology": {"maximum_release_skew_ns": 1},
        "load_shape": {"warmup": "done"},
        "data_distribution": {
            "seed": "one",
            "schema_version": "1",
            "payload_classes": "typical",
            "uniform_key_contract": "uniform",
            "hot_key_contract": "hot",
            "idempotency_contract": "none",
            "invalid_input_contract": "none",
        },
        "durable_fence": {"G2": "commit"},
        "acceptance": {"three_independent_runs": True},
    }


def _report() -> dict[str, object]:
    return {
        "schema": "TradeCoreOmsLoadGateResultV1",
        "status": "PASS",
        "git_revision": "deadbeef",
        "generated_at_utc": "2026-09-12T00:00:00+00:00",
        "base_urls": ["http://127.0.0.1:18080"],
        "data_distribution": {"key_distribution": "uniform"},
        "workloads": [{"workload": "W3", "exit_code": 0, "summary": "w3.json"}],
    }


def test_platform_evidence_requires_retained_artifacts(tmp_path) -> None:
    report = _report()
    evidence = platform_gate_evidence(
        profile=_profile(), report=report, platform="OMS", output_dir=tmp_path
    )
    assert evidence["V2-GENERATOR"]["status"] == "BLOCKED"
    assert evidence["V5-OBSERVABILITY"]["status"] == "BLOCKED"
    (tmp_path / "w3.json").write_text("{}", encoding="utf-8")
    (tmp_path / "w3-hdr-shard-0.json").write_text("{}", encoding="utf-8")
    evidence = platform_gate_evidence(
        profile=_profile(), report=report, platform="OMS", output_dir=tmp_path
    )
    assert {entry["status"] for entry in evidence.values()} == {"PASS"}


def test_platform_evidence_rejects_callback_profile(tmp_path) -> None:
    with pytest.raises(BenchmarkEvidenceV11Error, match="no-callback"):
        platform_gate_evidence(
            profile=_profile(callbacks=True), report=_report(), platform="OMS", output_dir=tmp_path
        )


def test_writes_strict_gate_evidence(tmp_path) -> None:
    profile = _profile()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    (tmp_path / "w3.json").write_text("{}", encoding="utf-8")
    (tmp_path / "w3-hdr-shard-0.json").write_text("{}", encoding="utf-8")
    paths = write_platform_gate_evidence(
        profile_path=profile_path, report=_report(), platform="OMS", output_dir=tmp_path
    )
    payload = json.loads(paths["V2-GENERATOR"].read_text(encoding="utf-8"))
    assert payload["schema"] == "BenchmarkGateEvidenceV11"
    assert payload["status"] == "PASS"


def test_emt_evidence_requires_complete_callback_closure() -> None:
    report = {
        "schema_version": 3,
        "driver": "tradecore-mmap-emt-burst-c0-c7-process-isolated",
        "executed": True,
        "complete": True,
        "burst_width": 2,
        "payload_free_timing_samples_ns": [{"test_index": 0}, {"test_index": 1}],
        "execution_model": {
            "logical_simultaneous_signals": 2,
            "process_isolated_sessions": True,
            "attested_session_shard_count": 2,
            "worker_cpu_affinity": {},
        },
        "persistence_profile": {"name": "MmapJournalProfileV1"},
        "full_path": {"segments_ns": {"C5->C7": {"count": 2}}},
        "outcomes": {
            "native_accepted": 2,
            "callback_completed_C7": 2,
            "callback_seen_downstream": 2,
            "callback_complete_C5_to_C7_pairs": 2,
            "callback_gaps": 0,
            "cleanup_terminal_callback_complete": True,
            "cleanup_terminal_inflight_remaining": 0,
        },
        "verification": {"full_acceptance_complete": True},
    }
    evidence = emt_gate_evidence(profile=_profile(callbacks=True), report=report)
    assert {item["status"] for item in evidence.values()} == {"PASS"}
    report["outcomes"]["callback_gaps"] = 1
    evidence = emt_gate_evidence(profile=_profile(callbacks=True), report=report)
    assert evidence["V4-CALLBACK"]["status"] == "BLOCKED"
