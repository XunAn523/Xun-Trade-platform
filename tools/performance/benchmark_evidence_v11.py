"""Convert concrete benchmark-driver results into V1.1 gate evidence.

The V1.1 preflight intentionally does not know any OMS, EMS, or broker
implementation detail.  This module is the narrow adapter on the other side:
it turns a completed driver report plus its retained summary files into typed,
payload-free evidence.  It never invents a PASS -- an absent or failed driver
result remains ``BLOCKED``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from tools.performance.benchmark_standard_v11 import BenchmarkStandardV11Error, load_profile


class BenchmarkEvidenceV11Error(ValueError):
    """A concrete runner result cannot support the requested V1.1 gate."""


def _object(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BenchmarkEvidenceV11Error(f"{label} must be an object")
    return dict(value)


def _workload_result(report: Mapping[str, object]) -> list[dict[str, Any]]:
    raw = report.get("workloads")
    if not isinstance(raw, list) or not raw:
        raise BenchmarkEvidenceV11Error("driver report has no workloads")
    workloads = [_object(item, label="workload") for item in raw]
    if any(type(item.get("exit_code")) is not int for item in workloads):
        raise BenchmarkEvidenceV11Error("driver report workload exit_code is invalid")
    return workloads


def _gate(
    *, profile_id: str, gate: str, status: str, evidence: Mapping[str, object]
) -> dict[str, Any]:
    if status not in {"PASS", "BLOCKED"}:
        raise BenchmarkEvidenceV11Error("gate evidence status is invalid")
    return {
        "schema": "BenchmarkGateEvidenceV11",
        "profile_id": profile_id,
        "gate": gate,
        "status": status,
        "evidence": dict(evidence),
    }


def platform_gate_evidence(
    *,
    profile: Mapping[str, object],
    report: Mapping[str, object],
    platform: str,
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Build V2/V3/V5 evidence for a retained OMS or EMS/strategy driver run.

    ``V4-CALLBACK`` is deliberately absent: these sidecar profiles describe no
    venue callback boundary and must declare ``external_callback_required`` as
    false.  A caller trying to adapt an external-callback profile is rejected
    rather than silently creating an unusable preflight packet.
    """

    profile_id = profile.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id:
        raise BenchmarkEvidenceV11Error("benchmark profile_id is invalid")
    if profile.get("external_callback_required") is not False:
        raise BenchmarkEvidenceV11Error("platform adapter requires a no-callback profile")
    if platform not in {"OMS", "EMS_STRATEGY"}:
        raise BenchmarkEvidenceV11Error("platform is invalid")
    workloads = _workload_result(report)
    success = report.get("status") == "PASS" and all(item["exit_code"] == 0 for item in workloads)
    summaries = [str(item["summary"]) for item in workloads if isinstance(item.get("summary"), str)]
    summary_files = [name for name in summaries if (output_dir / name).is_file()]
    generator = _gate(
        profile_id=profile_id,
        gate="V2-GENERATOR",
        status="PASS" if success and len(summary_files) == len(workloads) else "BLOCKED",
        evidence={
            "platform": platform,
            "driver_schema": report.get("schema"),
            "git_revision": report.get("git_revision", "unknown"),
            "workload_count": len(workloads),
            "retained_k6_summaries": summary_files,
            "data_distribution": report.get("data_distribution"),
        },
    )
    base_urls = report.get("base_urls")
    environment_ready = isinstance(base_urls, list) and bool(base_urls)
    preflight = report.get("preflight")
    if platform == "EMS_STRATEGY":
        environment_ready = (
            environment_ready and isinstance(preflight, dict) and preflight.get("status") == "PASS"
        )
    environment = _gate(
        profile_id=profile_id,
        gate="V3-ENVIRONMENT",
        status="PASS" if success and environment_ready else "BLOCKED",
        evidence={
            "platform": platform,
            "base_urls": base_urls,
            "preflight": preflight,
            "generated_at_utc": report.get("generated_at_utc"),
        },
    )
    hdr_names: list[str] = []
    for item in workloads:
        hdr = item.get("hdr")
        if isinstance(hdr, list):
            hdr_names.extend(name for name in hdr if isinstance(name, str))
    hdr_names.extend(
        name
        for name in ("w3-hdr-shard-0.json",)
        if (output_dir / name).is_file() and name not in hdr_names
    )
    retained_hdr = [name for name in hdr_names if (output_dir / name).is_file()]
    observability = _gate(
        profile_id=profile_id,
        gate="V5-OBSERVABILITY",
        status="PASS" if success and bool(retained_hdr) else "BLOCKED",
        evidence={
            "platform": platform,
            "hdr_artifacts": retained_hdr,
            "driver_status": report.get("status"),
            "workload_exit_codes": {
                str(item.get("workload", item.get("gate", "unknown"))): item["exit_code"]
                for item in workloads
            },
        },
    )
    return {item["gate"]: item for item in (generator, environment, observability)}


def write_platform_gate_evidence(
    *, profile_path: Path, report: Mapping[str, object], platform: str, output_dir: Path
) -> dict[str, Path]:
    """Write individual V2/V3/V5 gate evidence alongside the driver result."""

    try:
        profile = load_profile(profile_path)
    except BenchmarkStandardV11Error as error:
        raise BenchmarkEvidenceV11Error(str(error)) from error
    evidence = platform_gate_evidence(
        profile=profile, report=report, platform=platform, output_dir=output_dir
    )
    result: dict[str, Path] = {}
    for gate, payload in evidence.items():
        filename = f"{gate.lower()}-evidence-v11.json"
        path = output_dir / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        result[gate] = path
    return result


def emt_gate_evidence(
    *, profile: Mapping[str, object], report: Mapping[str, object]
) -> dict[str, dict[str, Any]]:
    """Build retained V2--V5 evidence from one completed EMT C0--C7 report.

    It is intentionally a *post-run* adapter.  It proves what the burst
    actually retained, and can only be used as a prerequisite for a later run
    after the operator establishes that profile and environment have not
    changed.  It cannot retrospectively turn a previously ungated run into a
    preflight-approved result.
    """

    profile_id = profile.get("profile_id")
    if not isinstance(profile_id, str) or not profile_id:
        raise BenchmarkEvidenceV11Error("benchmark profile_id is invalid")
    if profile.get("external_callback_required") is not True:
        raise BenchmarkEvidenceV11Error("EMT adapter requires an external-callback profile")
    outcomes = _object(report.get("outcomes"), label="EMT outcomes")
    verification = _object(report.get("verification"), label="EMT verification")
    burst_width = report.get("burst_width")
    if type(burst_width) is not int or burst_width < 1:
        raise BenchmarkEvidenceV11Error("EMT burst_width is invalid")
    timing = report.get("payload_free_timing_samples_ns")
    timing_complete = isinstance(timing, list) and len(timing) == burst_width
    accepted = (
        report.get("executed") is True
        and report.get("complete") is True
        and verification.get("full_acceptance_complete") is True
        and outcomes.get("native_accepted") == burst_width
        and outcomes.get("callback_completed_C7") == burst_width
        and outcomes.get("callback_seen_downstream") == burst_width
        and outcomes.get("callback_complete_C5_to_C7_pairs") == burst_width
        and outcomes.get("callback_gaps") == 0
        and outcomes.get("cleanup_terminal_callback_complete") is True
        and outcomes.get("cleanup_terminal_inflight_remaining") == 0
        and timing_complete
    )
    execution_model = _object(report.get("execution_model"), label="EMT execution_model")
    persistence = _object(report.get("persistence_profile"), label="EMT persistence_profile")
    full_path = _object(report.get("full_path"), label="EMT full_path")
    common = {
        "phase": "post_run",
        "driver": report.get("driver"),
        "burst_width": burst_width,
        "report_schema_version": report.get("schema_version"),
        "profile_persistence": persistence.get("name"),
        "callback_persistence_profile": persistence.get("callback_persistence_profile"),
        "accepted": accepted,
    }
    generator = _gate(
        profile_id=profile_id,
        gate="V2-GENERATOR",
        status="PASS" if accepted else "BLOCKED",
        evidence={
            **common,
            "payload_free_timing_sample_count": len(timing) if isinstance(timing, list) else 0,
            "producer_start": execution_model.get("producer_start"),
            "logical_simultaneous_signals": execution_model.get("logical_simultaneous_signals"),
        },
    )
    environment = _gate(
        profile_id=profile_id,
        gate="V3-ENVIRONMENT",
        status="PASS" if accepted else "BLOCKED",
        evidence={
            **common,
            "process_isolated_sessions": execution_model.get("process_isolated_sessions"),
            "attested_session_shard_count": execution_model.get("attested_session_shard_count"),
            "worker_cpu_affinity": execution_model.get("worker_cpu_affinity"),
            "runtime_control": report.get("runtime_control"),
        },
    )
    callback = _gate(
        profile_id=profile_id,
        gate="V4-CALLBACK",
        status="PASS" if accepted else "BLOCKED",
        evidence={
            **common,
            "callback_completed_C7": outcomes.get("callback_completed_C7"),
            "callback_complete_C5_to_C7_pairs": outcomes.get("callback_complete_C5_to_C7_pairs"),
            "callback_gaps": outcomes.get("callback_gaps"),
            "callback_stage_coverage": outcomes.get("callback_stage_coverage"),
        },
    )
    observability = _gate(
        profile_id=profile_id,
        gate="V5-OBSERVABILITY",
        status="PASS" if accepted and bool(full_path.get("segments_ns")) else "BLOCKED",
        evidence={
            **common,
            "full_path_segment_count": len(full_path.get("segments_ns", {}))
            if isinstance(full_path.get("segments_ns"), dict)
            else 0,
            "batch_completion": full_path.get("batch_completion"),
            "runtime_attribution": report.get("runtime_attribution"),
        },
    )
    return {item["gate"]: item for item in (generator, environment, callback, observability)}


def write_emt_gate_evidence(
    *, profile_path: Path, report_path: Path, output_dir: Path
) -> dict[str, Path]:
    """Load one completed EMT report and write its V2--V5 post-run evidence."""

    try:
        profile = load_profile(profile_path)
    except BenchmarkStandardV11Error as error:
        raise BenchmarkEvidenceV11Error(str(error)) from error
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkEvidenceV11Error("cannot read EMT performance report") from error
    evidence = emt_gate_evidence(
        profile=profile, report=_object(report, label="EMT performance report")
    )
    result: dict[str, Path] = {}
    for gate, payload in evidence.items():
        path = output_dir / f"{gate.lower()}-evidence-v11.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        result[gate] = path
    return result
