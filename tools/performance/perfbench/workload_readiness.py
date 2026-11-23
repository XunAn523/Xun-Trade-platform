"""Readiness gate for the non-steady portion of the EMT performance matrix."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Mapping

from .matrix import all_scenarios, validate_runbook
from .workload_plan import nonsteady_scenarios, validate_workload_plan


class WorkloadReadinessError(ValueError):
    """Raised when inputs do not describe the current performance matrix."""


def _index_trace_report(report: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        raise WorkloadReadinessError("trace report scenarios must be a list")
    expected_keys = {scenario.key for scenario in all_scenarios()}
    indexed = {
        item.get("key"): item
        for item in scenarios
        if isinstance(item, Mapping) and isinstance(item.get("key"), str)
    }
    if len(indexed) != len(scenarios) or set(indexed) != expected_keys:
        raise WorkloadReadinessError(
            "trace report scenario keys must exactly match the current matrix"
        )
    return indexed


def _plan_readiness(
    workload_plan: Mapping[str, Any],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Any]]:
    try:
        validate_workload_plan(dict(workload_plan))
    except ValueError as exc:
        raise WorkloadReadinessError(str(exc)) from exc
    entries = workload_plan["scenarios"]
    indexed = {entry["key"]: entry for entry in entries}
    fields = ("fixture_id", "owner", "control_ref", "recovery_check")
    missing_by_field = {
        field: sum(not str(entry.get(field, "")).strip() for entry in entries) for field in fields
    }
    return indexed, {
        "name": "reviewed_nonsteady_controls",
        "ready": not any(missing_by_field.values()),
        "total_scenarios": len(entries),
        "missing_by_field": missing_by_field,
    }


def _runbook_alignment(
    workload_plan: Mapping[str, Mapping[str, Any]],
    runbook: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if runbook is None:
        return {
            "name": "runbook_ownership_alignment",
            "ready": False,
            "reason": "missing_runbook",
        }
    try:
        validate_runbook(dict(runbook))
    except ValueError as exc:
        raise WorkloadReadinessError(str(exc)) from exc
    runbook_entries = {entry["key"]: entry for entry in runbook["scenarios"]}
    mismatches = [
        key
        for key, entry in workload_plan.items()
        if any(
            runbook_entries[key].get(field) != entry.get(field) for field in ("fixture_id", "owner")
        )
    ]
    unowned = [
        key
        for key, entry in workload_plan.items()
        if any(
            not str(runbook_entries[key].get(field, "")).strip()
            for field in ("fixture_id", "owner")
        )
    ]
    return {
        "name": "runbook_ownership_alignment",
        "ready": not mismatches and not unowned,
        "mismatched_scenarios": len(mismatches),
        "examples": sorted(mismatches)[:10],
        "unowned_scenarios": len(unowned),
        "unowned_examples": sorted(unowned)[:10],
    }


def build_workload_readiness(
    trace_report: Mapping[str, Any],
    workload_plan: Mapping[str, Any],
    runbook: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return an auditable gate for every non-steady matrix scenario.

    A completed plan alone does not pass the gate: every non-steady scenario
    also needs real, successful C0--C7 trace evidence.  Conversely, observed
    traces do not bypass ownership, fixture-control, or recovery review.
    """

    trace_index = _index_trace_report(trace_report)
    plan_index, controls_gate = _plan_readiness(workload_plan)
    alignment_gate = _runbook_alignment(plan_index, runbook)
    scenarios = nonsteady_scenarios()
    status_counts = Counter(
        str(trace_index[scenario.key].get("evidence_status")) for scenario in scenarios
    )
    evidence_gate = {
        "name": "nonsteady_p0_evidence",
        "ready": status_counts.get("passed_p0", 0) == len(scenarios),
        "passed_scenarios": status_counts.get("passed_p0", 0),
        "total_scenarios": len(scenarios),
        "blocking_statuses": {
            status: count
            for status, count in sorted(status_counts.items())
            if status != "passed_p0"
        },
    }
    by_workload: dict[str, dict[str, Any]] = {}
    grouped: dict[str, list[Any]] = defaultdict(list)
    for scenario in scenarios:
        grouped[scenario.workload].append(scenario)
    for workload, grouped_scenarios in sorted(grouped.items()):
        scenario_keys = [scenario.key for scenario in grouped_scenarios]
        evidence = Counter(str(trace_index[key].get("evidence_status")) for key in scenario_keys)
        missing_controls = {
            field: sum(not str(plan_index[key].get(field, "")).strip() for key in scenario_keys)
            for field in ("fixture_id", "owner", "control_ref", "recovery_check")
        }
        by_workload[workload] = {
            "scenarios": len(scenario_keys),
            "passed_p0": evidence.get("passed_p0", 0),
            "blocking_evidence_statuses": {
                status: count for status, count in sorted(evidence.items()) if status != "passed_p0"
            },
            "missing_controls": missing_controls,
        }
    gates = (controls_gate, alignment_gate, evidence_gate)
    return {
        "schema_version": 1,
        "driver": "gateway-workload-readiness",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "measurement_scope": "non-steady workload controls and C0-C7 P0 evidence; not a production SLA declaration",
        "nonsteady_ready": all(gate["ready"] for gate in gates),
        "gates": list(gates),
        "by_workload": by_workload,
    }
