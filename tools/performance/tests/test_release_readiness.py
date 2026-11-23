from __future__ import annotations

import unittest

from perfbench.baseline import baseline_template
from perfbench.matrix import all_scenarios
from perfbench.release_readiness import build_release_readiness


def _complete_report() -> dict[str, object]:
    return {
        "scenarios": [
            {
                "key": scenario.key,
                "evidence_status": "passed_p0",
                "successful_complete_traces": scenario.acceptance_minimum_successes,
                "failed_or_rejected_traces": 0,
                "latency": {path: {"p99_ns": 100} for path in scenario.required_path_names},
            }
            for scenario in all_scenarios()
        ],
    }


def _approved_baseline() -> dict[str, object]:
    baseline = baseline_template()
    baseline["approval"] = {
        "approved_by": "performance-owner",
        "approved_at": "2026-08-26T00:00:00Z",
        "change_reference": "TEST-RELEASE-1",
    }
    for scenario, budget in zip(all_scenarios(), baseline["scenarios"]):
        budget["max_p99_ns"] = {path: 100 for path in scenario.required_path_names}
    return baseline


class ReleaseReadinessTests(unittest.TestCase):
    def test_missing_baseline_is_reported_without_bypassing_p0(self) -> None:
        readiness = build_release_readiness(_complete_report())
        self.assertFalse(readiness["release_ready"])
        self.assertTrue(readiness["gates"][0]["ready"])
        self.assertEqual("missing_baseline", readiness["gates"][1]["reason"])

    def test_complete_approved_evidence_is_release_ready(self) -> None:
        readiness = build_release_readiness(_complete_report(), _approved_baseline())
        self.assertTrue(readiness["release_ready"])
        self.assertTrue(all(gate["ready"] for gate in readiness["gates"]))

    def test_pending_evidence_remains_a_release_blocker(self) -> None:
        report = _complete_report()
        report["scenarios"][0]["evidence_status"] = "pending"
        readiness = build_release_readiness(report, _approved_baseline())
        self.assertFalse(readiness["release_ready"])
        self.assertEqual(1, readiness["gates"][0]["blocking_statuses"]["pending"])
        self.assertFalse(readiness["gates"][2]["ready"])

    def test_trace_provenance_is_preserved_in_the_gate_report(self) -> None:
        report = _complete_report()
        report["trace_provenance"] = {
            "included_batches": 2,
            "excluded_trace_files": ["invalid.traces.jsonl"],
        }
        readiness = build_release_readiness(report)
        self.assertEqual(report["trace_provenance"], readiness["trace_provenance"])
