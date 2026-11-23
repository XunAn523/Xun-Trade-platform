from __future__ import annotations

import unittest

from perfbench.matrix import all_scenarios, runbook_template
from perfbench.workload_plan import nonsteady_scenarios, workload_plan_template
from perfbench.workload_readiness import build_workload_readiness


def _complete_report() -> dict[str, object]:
    return {
        "scenarios": [
            {"key": scenario.key, "evidence_status": "passed_p0"} for scenario in all_scenarios()
        ],
    }


def _ready_inputs() -> tuple[dict[str, object], dict[str, object]]:
    runbook = runbook_template()
    plan = workload_plan_template()
    for entry in runbook["scenarios"]:
        entry["fixture_id"] = "reviewed-fixture"
        entry["owner"] = "performance-owner"
    for entry in plan["scenarios"]:
        entry["fixture_id"] = "reviewed-fixture"
        entry["owner"] = "performance-owner"
        entry["control_ref"] = "local-control-registry"
        entry["recovery_check"] = "local-reconciliation-check"
    return runbook, plan


class WorkloadReadinessTests(unittest.TestCase):
    def test_blank_plan_and_missing_runbook_are_explicit_blockers(self) -> None:
        readiness = build_workload_readiness(_complete_report(), workload_plan_template())
        self.assertFalse(readiness["nonsteady_ready"])
        self.assertEqual("missing_runbook", readiness["gates"][1]["reason"])
        self.assertEqual(
            len(nonsteady_scenarios()), readiness["gates"][0]["missing_by_field"]["fixture_id"]
        )

    def test_matching_controls_and_p0_evidence_are_ready(self) -> None:
        runbook, plan = _ready_inputs()
        readiness = build_workload_readiness(_complete_report(), plan, runbook)
        self.assertTrue(readiness["nonsteady_ready"])
        self.assertEqual(len(nonsteady_scenarios()), readiness["gates"][2]["passed_scenarios"])

    def test_one_pending_nonsteady_scenario_blocks_only_its_workload(self) -> None:
        runbook, plan = _ready_inputs()
        report = _complete_report()
        pending = next(
            scenario for scenario in all_scenarios() if scenario.workload == "pagination"
        )
        next(entry for entry in report["scenarios"] if entry["key"] == pending.key)[
            "evidence_status"
        ] = "pending"
        readiness = build_workload_readiness(report, plan, runbook)
        self.assertFalse(readiness["nonsteady_ready"])
        self.assertEqual(
            1, readiness["by_workload"]["pagination"]["blocking_evidence_statuses"]["pending"]
        )

    def test_matching_blank_runbook_is_not_considered_owned(self) -> None:
        runbook = runbook_template()
        readiness = build_workload_readiness(_complete_report(), workload_plan_template(), runbook)
        alignment = readiness["gates"][1]
        self.assertFalse(alignment["ready"])
        self.assertEqual(len(nonsteady_scenarios()), alignment["unowned_scenarios"])
