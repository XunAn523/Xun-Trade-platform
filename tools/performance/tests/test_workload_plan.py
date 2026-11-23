from __future__ import annotations

import unittest

from perfbench.matrix import MatrixError
from perfbench.workload_plan import (
    nonsteady_scenarios,
    validate_workload_plan,
    workload_plan_template,
)


class WorkloadPlanTests(unittest.TestCase):
    def test_template_has_exact_nonsteady_matrix_coverage(self) -> None:
        manifest = workload_plan_template()
        validate_workload_plan(manifest)
        self.assertEqual(len(nonsteady_scenarios()), len(manifest["scenarios"]))
        self.assertTrue(all(entry["workload"] != "steady" for entry in manifest["scenarios"]))

    def test_template_rejects_weakened_control_requirements(self) -> None:
        manifest = workload_plan_template()
        manifest["scenarios"][0]["control_requirements"] = []
        with self.assertRaises(MatrixError):
            validate_workload_plan(manifest)

    def test_execution_readiness_requires_reviewed_references(self) -> None:
        manifest = workload_plan_template()
        with self.assertRaises(MatrixError):
            validate_workload_plan(manifest, require_ready=True)
        for entry in manifest["scenarios"]:
            entry["fixture_id"] = "reviewed-fixture"
            entry["owner"] = "performance-owner"
            entry["control_ref"] = "local-control-registry"
            entry["recovery_check"] = "local-reconciliation-check"
        validate_workload_plan(manifest, require_ready=True)

    def test_template_can_initialize_a_non_sensitive_owner(self) -> None:
        manifest = workload_plan_template(owner="performance-owner")
        validate_workload_plan(manifest)
        self.assertTrue(
            all(entry["owner"] == "performance-owner" for entry in manifest["scenarios"])
        )
