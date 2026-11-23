"""Non-bypassing release-readiness evaluation for gateway performance evidence."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any, Mapping

from .baseline import BaselineError, evaluate_acceptance, validate_baseline
from .matrix import all_scenarios


class ReleaseReadinessError(ValueError):
    """Raised when a trace report cannot be used for a release-readiness check."""


def build_release_readiness(
    report: Mapping[str, Any],
    baseline: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Explain every blocking release gate without weakening strict acceptance.

    Missing P0 evidence, missing approval and missing budgets remain blocking.
    Unlike the strict acceptance evaluator, this function emits a durable,
    payload-free explanation even when the release candidate is not ready.
    """

    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        raise ReleaseReadinessError("trace report scenarios must be a list")
    expected_keys = {scenario.key for scenario in all_scenarios()}
    indexed = {
        item.get("key"): item
        for item in scenarios
        if isinstance(item, Mapping) and isinstance(item.get("key"), str)
    }
    if len(indexed) != len(scenarios) or set(indexed) != expected_keys:
        raise ReleaseReadinessError(
            "trace report scenario keys must exactly match the current matrix"
        )

    evidence_statuses = Counter(str(item.get("evidence_status")) for item in scenarios)
    p0_passed = evidence_statuses.get("passed_p0", 0)
    p0_gate = {
        "name": "p0_evidence",
        "ready": p0_passed == len(scenarios),
        "passed_scenarios": p0_passed,
        "total_scenarios": len(scenarios),
        "blocking_statuses": {
            status: count
            for status, count in sorted(evidence_statuses.items())
            if status != "passed_p0"
        },
    }

    baseline_gate: dict[str, Any] = {"name": "approved_baseline", "ready": False}
    acceptance_gate: dict[str, Any] = {"name": "acceptance_budgets", "ready": False}
    if baseline is None:
        baseline_gate["reason"] = "missing_baseline"
        acceptance_gate["reason"] = "baseline_not_evaluable"
    else:
        try:
            validate_baseline(baseline, require_approved=True)
        except BaselineError as exc:
            baseline_gate["reason"] = f"invalid_or_unapproved_baseline={exc}"
            acceptance_gate["reason"] = "baseline_not_evaluable"
        else:
            baseline_gate["ready"] = True
            acceptance = evaluate_acceptance(report, baseline)
            rejected_reasons = Counter(
                reason
                for decision in acceptance["scenarios"]
                if not decision["accepted"]
                for reason in decision["reasons"]
            )
            acceptance_gate.update(
                {
                    "ready": bool(acceptance["accepted"]),
                    "accepted_scenarios": acceptance["accepted_scenarios"],
                    "total_scenarios": acceptance["total_scenarios"],
                    "blocking_reasons": dict(sorted(rejected_reasons.items())),
                }
            )

    gates = [p0_gate, baseline_gate, acceptance_gate]
    result = {
        "schema_version": 1,
        "driver": "gateway-release-readiness",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "measurement_scope": "release gate over C0-C7 trace evidence and approved budgets",
        "release_ready": all(gate["ready"] for gate in gates),
        "gates": gates,
    }
    provenance = report.get("trace_provenance")
    if isinstance(provenance, Mapping):
        result["trace_provenance"] = dict(provenance)
    return result
