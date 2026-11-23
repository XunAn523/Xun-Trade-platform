"""Versioned performance-budget templates and strict acceptance evaluation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from .matrix import all_scenarios, scenario_index


class BaselineError(ValueError):
    """A budget file is incomplete or incompatible with the current matrix."""


def baseline_template() -> dict[str, Any]:
    """Create a non-approving template for every current matrix scenario."""

    return {
        "schema_version": 1,
        "purpose": "Approved EMT gateway performance budgets",
        "approval": {"approved_by": "", "approved_at": "", "change_reference": ""},
        "scenarios": [
            {
                "key": scenario.key,
                "max_p99_ns": {path: None for path in scenario.required_path_names},
            }
            for scenario in all_scenarios()
        ],
    }


def validate_baseline(baseline: Mapping[str, Any], *, require_approved: bool = False) -> None:
    if baseline.get("schema_version") != 1:
        raise BaselineError("baseline schema_version must be 1")
    approval = baseline.get("approval")
    if not isinstance(approval, Mapping):
        raise BaselineError("baseline approval must be an object")
    if require_approved and any(
        not isinstance(approval.get(field), str) or not approval[field]
        for field in ("approved_by", "approved_at", "change_reference")
    ):
        raise BaselineError("baseline lacks complete approval evidence")
    entries = baseline.get("scenarios")
    if not isinstance(entries, list):
        raise BaselineError("baseline scenarios must be a list")
    expected = scenario_index()
    keyed = {entry.get("key"): entry for entry in entries if isinstance(entry, Mapping)}
    if len(keyed) != len(entries) or set(keyed) != set(expected):
        raise BaselineError("baseline scenario keys must exactly match the current matrix")
    for key, scenario in expected.items():
        budgets = keyed[key].get("max_p99_ns")
        if not isinstance(budgets, Mapping) or set(budgets) != set(scenario.required_path_names):
            raise BaselineError(f"baseline paths must exactly match {key}")
        for path, value in budgets.items():
            if value is not None and (not isinstance(value, int) or value <= 0):
                raise BaselineError(f"baseline {key}:{path} must be a positive integer or null")


def evaluate_acceptance(report: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, Any]:
    """Reject missing approval, evidence, samples, failures and budget overruns."""

    validate_baseline(baseline, require_approved=True)
    report_scenarios = report.get("scenarios")
    if not isinstance(report_scenarios, list):
        raise BaselineError("gateway report scenarios must be a list")
    report_by_key = {
        item.get("key"): item for item in report_scenarios if isinstance(item, Mapping)
    }
    budget_by_key = {item["key"]: item for item in baseline["scenarios"]}
    decisions: list[dict[str, Any]] = []
    for scenario in all_scenarios():
        result = report_by_key.get(scenario.key)
        reasons: list[str] = []
        if not isinstance(result, Mapping):
            reasons.append("missing_report_scenario")
        else:
            if result.get("evidence_status") != "passed_p0":
                reasons.append(f"evidence_status={result.get('evidence_status')}")
            if result.get("successful_complete_traces", 0) < scenario.acceptance_minimum_successes:
                reasons.append("insufficient_acceptance_samples")
            if result.get("failed_or_rejected_traces", 0) != 0:
                reasons.append("failed_or_rejected_traces")
            latency = result.get("latency")
            budgets = budget_by_key[scenario.key]["max_p99_ns"]
            for path, maximum in budgets.items():
                summary = latency.get(path) if isinstance(latency, Mapping) else None
                observed = summary.get("p99_ns") if isinstance(summary, Mapping) else None
                if not isinstance(maximum, int):
                    reasons.append(f"unapproved_budget={path}")
                elif not isinstance(observed, int):
                    reasons.append(f"missing_p99={path}")
                elif observed > maximum:
                    reasons.append(f"p99_exceeds_budget={path}")
        decisions.append({"key": scenario.key, "accepted": not reasons, "reasons": reasons})
    return {
        "schema_version": 1,
        "driver": "gateway-acceptance-report",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "accepted": all(item["accepted"] for item in decisions),
        "accepted_scenarios": sum(item["accepted"] for item in decisions),
        "total_scenarios": len(decisions),
        "approval": dict(baseline["approval"]),
        "scenarios": decisions,
    }
