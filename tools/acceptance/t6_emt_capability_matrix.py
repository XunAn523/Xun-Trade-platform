"""Convert payload-free EMT simulated-sweep outputs into a complete T6 matrix.

The matrix deliberately distinguishes a binding invocation or callback sighting
from a verified business terminal state.  It is safe to run on reports only:
no SDK import, login, query, mutation, account identifier or order payload is
needed to produce it.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from xuanling_tradecore.adapters.emt.trading_catalog_v1 import (
    TRADER_API_SPECS_V1,
    TRADER_SPI_SPECS_V1,
    known_linux_binding_blockers_v1,
)
from xuanling_tradecore.adapters.emt.trading_readiness_v1 import (
    EmtEvidenceStateV1,
    EmtEvidenceTargetV1,
)


class T6CapabilityMatrixError(ValueError):
    """One supplied vendor-sweep report cannot be used as evidence."""


def build_t6_capability_matrix(reports: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Return an exact 86/69 matrix from one or more vendor-sweep reports."""

    report_list = tuple(reports)
    if not report_list:
        raise T6CapabilityMatrixError("at least one vendor-sweep report is required")
    api_observations: dict[str, list[str]] = {}
    spi_observations: dict[str, list[str]] = {}
    profiles: list[str] = []
    for report in report_list:
        profile = _profile(report)
        profiles.append(profile)
        for row in _operation_rows(report):
            name = _required_text(row, "binding_method")
            latency = row.get("return_latency")
            callbacks = row.get("callbacks")
            if isinstance(latency, Mapping) and isinstance(latency.get("count"), int) and latency["count"] > 0:
                api_observations.setdefault(name, []).append(profile)
            if isinstance(callbacks, int) and callbacks > 0:
                spi_observations.setdefault(name, []).append(profile)
    blockers = set(known_linux_binding_blockers_v1())
    api = [
        _api_entry(spec.official_name, spec.binding_name, api_observations, blockers)
        for spec in TRADER_API_SPECS_V1
    ]
    spi = [
        _spi_entry(spec.official_name, spec.binding_name, spi_observations, blockers)
        for spec in TRADER_SPI_SPECS_V1
    ]
    return {
        "schema": "tradecore.emt.t6-capability-matrix.v1",
        "source_driver": "vendor-simulated-sweep",
        "profiles": sorted(set(profiles)),
        "api": api,
        "spi": spi,
        "summary": {
            "api_count": len(api),
            "spi_count": len(spi),
            "api_observed": sum(entry["state"] == EmtEvidenceStateV1.SUPPORTED_CONTRACT_ONLY.value for entry in api),
            "spi_observed": sum(entry["state"] == EmtEvidenceStateV1.SUPPORTED_CONTRACT_ONLY.value for entry in spi),
            "unsupported_binding": sum(entry["state"] == EmtEvidenceStateV1.UNSUPPORTED_BINDING.value for entry in api + spi),
            "not_terminally_verified": len(api) + len(spi),
        },
        "release_conclusion": "not_ready: sweep is invocation/callback evidence only; terminal semantics and recovery evidence remain required",
    }


def _api_entry(
    official_name: str,
    binding_name: str,
    observations: Mapping[str, list[str]],
    blockers: set[str],
) -> dict[str, object]:
    if official_name in blockers:
        state = EmtEvidenceStateV1.UNSUPPORTED_BINDING
    elif observations.get(binding_name):
        state = EmtEvidenceStateV1.SUPPORTED_CONTRACT_ONLY
    else:
        state = EmtEvidenceStateV1.BLOCKED_SEMANTICS
    return {
        "target": EmtEvidenceTargetV1.TRADER_API.value,
        "official_name": official_name,
        "binding_name": binding_name,
        "state": state.value,
        "observed_profiles": sorted(set(observations.get(binding_name, []))),
    }


def _spi_entry(
    official_name: str,
    binding_name: str,
    observations: Mapping[str, list[str]],
    blockers: set[str],
) -> dict[str, object]:
    if official_name in blockers:
        state = EmtEvidenceStateV1.UNSUPPORTED_BINDING
    elif observations.get(binding_name):
        state = EmtEvidenceStateV1.SUPPORTED_CONTRACT_ONLY
    else:
        state = EmtEvidenceStateV1.BLOCKED_SEMANTICS
    return {
        "target": EmtEvidenceTargetV1.TRADER_SPI.value,
        "official_name": official_name,
        "binding_name": binding_name,
        "state": state.value,
        "observed_profiles": sorted(set(observations.get(binding_name, []))),
    }


def _profile(report: Mapping[str, object]) -> str:
    profile = report.get("profile")
    if profile not in {"ordinary", "credit", "option"}:
        raise T6CapabilityMatrixError("vendor-sweep report profile is invalid")
    return profile


def _operation_rows(report: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    if report.get("driver") != "vendor-simulated-sweep":
        raise T6CapabilityMatrixError("report is not a vendor-simulated-sweep output")
    rows = report.get("operations")
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise T6CapabilityMatrixError("vendor-sweep operations are invalid")
    return tuple(rows)


def _required_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise T6CapabilityMatrixError(f"vendor-sweep operation {field} is invalid")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    reports: list[Mapping[str, object]] = []
    for path in args.input:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise T6CapabilityMatrixError(f"report must be an object: {path}")
        reports.append(raw)
    result = build_t6_capability_matrix(reports)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "summary": result["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
