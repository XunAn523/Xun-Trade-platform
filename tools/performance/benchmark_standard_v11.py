"""Executable, fail-closed V0--V5 evidence gate for BenchmarkStandardV1.1.

The gate is deliberately independent of OMS/EMS/adapter implementations.  It
does not generate load or side effects; it verifies that a concrete load
driver has supplied the prerequisite evidence required before a run may be
called a benchmark result.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from hashlib import sha256
from pathlib import Path
from typing import Any


class BenchmarkStandardV11Error(ValueError):
    """A V1.1 profile or gate evidence is incomplete or inconsistent."""


_PROFILE_REQUIRED = (
    "system_id",
    "profile_id",
    "measurement_schema_version",
    "g_boundaries",
    "topology",
    "load_shape",
    "data_distribution",
    "durable_fence",
    "acceptance",
    "external_callback_required",
)
_DATA_DISTRIBUTION_REQUIRED = (
    "seed",
    "schema_version",
    "payload_classes",
    "uniform_key_contract",
    "hot_key_contract",
    "idempotency_contract",
    "invalid_input_contract",
)
_GATES = ("V2-GENERATOR", "V3-ENVIRONMENT", "V4-CALLBACK", "V5-OBSERVABILITY")


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkStandardV11Error(f"{label} is unreadable") from error
    if not isinstance(raw, dict):
        raise BenchmarkStandardV11Error(f"{label} must be a JSON object")
    return raw


def load_profile(path: Path) -> dict[str, Any]:
    """Load one immutable V1.1 profile and validate its mandatory contract."""

    profile = _load_object(path, label="benchmark profile")
    missing = [field for field in _PROFILE_REQUIRED if field not in profile]
    if missing:
        raise BenchmarkStandardV11Error(f"benchmark profile is missing: {','.join(missing)}")
    if profile.get("schema") != "BenchmarkProfileV11":
        raise BenchmarkStandardV11Error("benchmark profile schema is invalid")
    for field in ("system_id", "profile_id", "measurement_schema_version"):
        if not isinstance(profile[field], str) or not profile[field]:
            raise BenchmarkStandardV11Error(f"benchmark profile {field} is invalid")
    if type(profile["external_callback_required"]) is not bool:
        raise BenchmarkStandardV11Error("benchmark profile external_callback_required is invalid")
    boundaries = profile["g_boundaries"]
    if not isinstance(boundaries, dict) or set(boundaries) != {f"G{index}" for index in range(8)}:
        raise BenchmarkStandardV11Error("benchmark profile must map every G0..G7 boundary")
    if any(not isinstance(value, str) or not value for value in boundaries.values()):
        raise BenchmarkStandardV11Error("benchmark profile G boundary value is invalid")
    distribution = profile["data_distribution"]
    if not isinstance(distribution, dict):
        raise BenchmarkStandardV11Error("benchmark profile data_distribution is invalid")
    missing_distribution = [
        field for field in _DATA_DISTRIBUTION_REQUIRED if field not in distribution
    ]
    if missing_distribution:
        raise BenchmarkStandardV11Error(
            "benchmark profile data_distribution is missing: " + ",".join(missing_distribution)
        )
    for field in ("topology", "load_shape", "durable_fence", "acceptance"):
        if not isinstance(profile[field], dict) or not profile[field]:
            raise BenchmarkStandardV11Error(f"benchmark profile {field} is invalid")
    return profile


def profile_sha256(profile: dict[str, Any]) -> str:
    """Return the canonical, payload-free profile identity digest."""

    return sha256(
        json.dumps(profile, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _git_clean(repo: Path) -> tuple[str, bool]:
    try:
        revision = subprocess.check_output(
            ("git", "rev-parse", "HEAD"), cwd=repo, text=True, stderr=subprocess.DEVNULL
        ).strip()
        dirty = subprocess.check_output(
            ("git", "status", "--porcelain"), cwd=repo, text=True, stderr=subprocess.DEVNULL
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown", False
    return revision, not dirty.strip()


def validate_clock_evidence(
    payload: dict[str, Any], *, profile_id: str, maximum_skew_ns: int
) -> bool:
    """Validate a measured common-release calibration without estimating it."""

    if payload.get("schema") != "BenchmarkClockCalibrationV11":
        return False
    if payload.get("profile_id") != profile_id or payload.get("status") != "PASS":
        return False
    if payload.get("monotonic_clock") != "monotonic_ns":
        return False
    skew = payload.get("maximum_observed_release_skew_ns")
    overhead = payload.get("timestamp_read_overhead_ns")
    return (
        type(skew) is int
        and 0 <= skew <= maximum_skew_ns
        and isinstance(overhead, dict)
        and type(overhead.get("p50_ns")) is int
        and type(overhead.get("p100_ns")) is int
        and 0 <= overhead["p50_ns"] <= overhead["p100_ns"]
    )


def validate_gate_evidence(payload: dict[str, Any], *, profile_id: str, gate: str) -> bool:
    """Require a driver-produced gate artifact rather than a local assumption."""

    return (
        payload.get("schema") == "BenchmarkGateEvidenceV11"
        and payload.get("profile_id") == profile_id
        and payload.get("gate") == gate
        and payload.get("status") == "PASS"
        and isinstance(payload.get("evidence"), dict)
        and bool(payload["evidence"])
    )


def evaluate_preflight(
    *,
    profile: dict[str, Any],
    git_revision: str,
    git_clean: bool,
    clock: dict[str, Any] | None,
    gate_evidence: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
    """Build one auditable PASS/BLOCKED V0--V5 decision."""

    profile_id = str(profile["profile_id"])
    maximum_skew_ns = profile["topology"].get("maximum_release_skew_ns")
    if type(maximum_skew_ns) is not int or maximum_skew_ns < 0:
        raise BenchmarkStandardV11Error("profile topology maximum_release_skew_ns is invalid")
    gates: dict[str, dict[str, Any]] = {
        "V0-PROFILE": {
            "status": "PASS" if git_clean and git_revision != "unknown" else "BLOCKED",
            "git_revision": git_revision,
            "git_clean": git_clean,
            "profile_sha256": profile_sha256(profile),
        },
        "V1-CLOCK": {
            "status": (
                "PASS"
                if clock is not None
                and validate_clock_evidence(
                    clock, profile_id=profile_id, maximum_skew_ns=maximum_skew_ns
                )
                else "BLOCKED"
            ),
            "maximum_release_skew_ns": maximum_skew_ns,
        },
    }
    for gate in _GATES:
        evidence = gate_evidence.get(gate)
        if gate == "V4-CALLBACK" and not profile["external_callback_required"]:
            gates[gate] = {"status": "N/A", "reason": "profile has no external callback boundary"}
        else:
            gates[gate] = {
                "status": (
                    "PASS"
                    if evidence is not None
                    and validate_gate_evidence(evidence, profile_id=profile_id, gate=gate)
                    else "BLOCKED"
                )
            }
    passed = all(gate["status"] in {"PASS", "N/A"} for gate in gates.values())
    return {
        "schema": "BenchmarkPreflightV11",
        "profile_id": profile_id,
        "measurement_schema_version": profile["measurement_schema_version"],
        "status": "PASS" if passed else "BLOCKED",
        "gates": gates,
        "performance_conclusion_permitted": passed,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run fail-closed BenchmarkStandardV1.1 V0--V5 gates"
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--clock", type=Path, required=True)
    parser.add_argument("--generator", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    parser.add_argument("--callback", type=Path)
    parser.add_argument("--observability", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        profile = load_profile(args.profile)
        repo = Path(__file__).resolve().parents[2]
        revision, clean = _git_clean(repo)
        clock = _load_object(args.clock, label="clock evidence")
        evidence = {
            "V2-GENERATOR": _load_object(args.generator, label="generator evidence"),
            "V3-ENVIRONMENT": _load_object(args.environment, label="environment evidence"),
            "V4-CALLBACK": (
                _load_object(args.callback, label="callback evidence")
                if args.callback is not None
                else None
            ),
            "V5-OBSERVABILITY": _load_object(args.observability, label="observability evidence"),
        }
        result = evaluate_preflight(
            profile=profile,
            git_revision=revision,
            git_clean=clean,
            clock=clock,
            gate_evidence=evidence,
        )
    except BenchmarkStandardV11Error as error:
        result = {
            "schema": "BenchmarkPreflightV11",
            "status": "BLOCKED",
            "performance_conclusion_permitted": False,
            "error": str(error),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
