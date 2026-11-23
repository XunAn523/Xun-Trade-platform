from __future__ import annotations

import unittest

from perfbench.baseline import (
    BaselineError,
    baseline_template,
    evaluate_acceptance,
    validate_baseline,
)
from perfbench.matrix import all_scenarios


class BaselineTests(unittest.TestCase):
    def test_template_cannot_be_used_until_budget_and_approval_are_completed(self) -> None:
        with self.assertRaises(BaselineError):
            validate_baseline(baseline_template(), require_approved=True)

    def test_complete_evidence_with_approved_budgets_passes_and_any_overrun_fails(self) -> None:
        baseline = baseline_template()
        baseline["approval"] = {
            "approved_by": "performance-owner",
            "approved_at": "2026-08-25T00:00:00Z",
            "change_reference": "TEST-1",
        }
        report_scenarios = []
        for scenario, budget in zip(all_scenarios(), baseline["scenarios"]):
            budget["max_p99_ns"] = {path: 100 for path in scenario.required_path_names}
            report_scenarios.append(
                {
                    "key": scenario.key,
                    "evidence_status": "passed_p0",
                    "successful_complete_traces": scenario.acceptance_minimum_successes,
                    "failed_or_rejected_traces": 0,
                    "latency": {path: {"p99_ns": 100} for path in scenario.required_path_names},
                }
            )
        accepted = evaluate_acceptance({"scenarios": report_scenarios}, baseline)
        self.assertTrue(accepted["accepted"])

        report_scenarios[0]["latency"][next(iter(report_scenarios[0]["latency"]))]["p99_ns"] = 101
        rejected = evaluate_acceptance({"scenarios": report_scenarios}, baseline)
        self.assertFalse(rejected["accepted"])
        self.assertTrue(rejected["scenarios"][0]["reasons"])
