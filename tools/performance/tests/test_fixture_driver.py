from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.fixture_driver import FixtureDriverError, FixtureDriverSettings, run_fixture_driver
from perfbench.matrix import runbook_template
from perfbench.workload_plan import workload_plan_template


class FixtureDriverTests(unittest.TestCase):
    def test_dry_run_requires_owned_fixture_evidence_but_never_starts_a_process(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest = runbook_template()
            for entry in manifest["scenarios"]:
                entry["fixture_id"] = "reviewed-fixture"
                entry["owner"] = "performance-owner"
            runbook = root / "runbook.json"
            runbook.write_text(json.dumps(manifest), encoding="utf-8")
            report = run_fixture_driver(
                FixtureDriverSettings(
                    runbook=runbook,
                    trace_output=root / "trace.jsonl",
                    command=(),
                )
            )
            self.assertFalse(report["executed"])
            self.assertEqual(len(manifest["scenarios"]), report["scenarios"])

    def test_unowned_scenarios_are_rejected_before_any_fixture_process(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            runbook = root / "runbook.json"
            runbook.write_text(json.dumps(runbook_template()), encoding="utf-8")
            with self.assertRaises(FixtureDriverError):
                run_fixture_driver(FixtureDriverSettings(runbook, root / "trace.jsonl", ()))

    def test_dry_run_validates_matching_nonsteady_workload_plan(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            runbook_manifest = runbook_template()
            for entry in runbook_manifest["scenarios"]:
                entry["fixture_id"] = "reviewed-fixture"
                entry["owner"] = "performance-owner"
            workload_manifest = workload_plan_template()
            for entry in workload_manifest["scenarios"]:
                entry["fixture_id"] = "reviewed-fixture"
                entry["owner"] = "performance-owner"
                entry["control_ref"] = "local-control-registry"
                entry["recovery_check"] = "local-reconciliation-check"
            runbook = root / "runbook.json"
            workload_plan = root / "workload-plan.json"
            runbook.write_text(json.dumps(runbook_manifest), encoding="utf-8")
            workload_plan.write_text(json.dumps(workload_manifest), encoding="utf-8")
            report = run_fixture_driver(
                FixtureDriverSettings(
                    runbook=runbook,
                    trace_output=root / "trace.jsonl",
                    command=(),
                    workload_plan=workload_plan,
                )
            )
            self.assertEqual("validated", report["nonsteady_workload_plan"])

    def test_external_execution_requires_a_completed_nonsteady_plan_before_a_process(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest = runbook_template()
            for entry in manifest["scenarios"]:
                entry["fixture_id"] = "reviewed-fixture"
                entry["owner"] = "performance-owner"
            runbook = root / "runbook.json"
            runbook.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(FixtureDriverError, "workload-plan"):
                run_fixture_driver(
                    FixtureDriverSettings(
                        runbook=runbook,
                        trace_output=root / "trace.jsonl",
                        command=(),
                        execute_simulated=True,
                    )
                )
