from __future__ import annotations

import unittest

from perfbench.gateway_report import build_gateway_report
from perfbench.trace import GatewayTraceCollector


class GatewayReportTests(unittest.TestCase):
    def test_complete_trace_is_reported_but_catalog_remains_not_ready(self) -> None:
        collector = GatewayTraceCollector()
        for index in range(100):
            correlation_id = f"query-{index}"
            collector.begin(
                correlation_id,
                kind="api",
                interface="QueryAsset",
                profile="ordinary",
                workload="steady",
                timestamp_ns=index * 1000,
            )
            for offset, stage in enumerate(("C1", "C3", "C4", "C5", "C6", "C7"), start=1):
                collector.mark(correlation_id, stage, index * 1000 + offset * 10)
            collector.complete(correlation_id)
        collector.record_metric("callback_ring_depth", 3, 101_000)
        report = build_gateway_report(collector.export_events())
        result = next(
            item for item in report["scenarios"] if item["key"] == "api:QueryAsset:ordinary:steady"
        )
        self.assertEqual("passed_p0", result["evidence_status"])
        self.assertEqual(100, result["latency"]["C0_to_C7"]["count"])
        self.assertEqual(3, report["resource_metrics"]["callback_ring_depth"]["max"])
        self.assertFalse(report["summary"]["p0_ready"])

    def test_failed_trace_cannot_pass_even_when_paths_exist(self) -> None:
        events = [
            {
                "correlation_id": "failed",
                "kind": "api",
                "interface": "QueryAsset",
                "profile": "ordinary",
                "workload": "steady",
                "stage": "C0",
                "timestamp_ns": 1,
            },
            {
                "correlation_id": "failed",
                "kind": "api",
                "interface": "QueryAsset",
                "profile": "ordinary",
                "workload": "steady",
                "stage": "C1",
                "timestamp_ns": 2,
            },
            {
                "correlation_id": "failed",
                "kind": "api",
                "interface": "QueryAsset",
                "profile": "ordinary",
                "workload": "steady",
                "outcome": "timeout",
            },
        ]
        report = build_gateway_report(events)
        result = next(
            item for item in report["scenarios"] if item["key"] == "api:QueryAsset:ordinary:steady"
        )
        self.assertEqual("failed", result["evidence_status"])

    def test_controlled_injection_artifact_is_rejected_before_any_p0_aggregation(self) -> None:
        events = [
            {
                "event_type": "artifact_classification",
                "artifact_type": "controlled_gateway_injection",
                "counter_performance_eligible": False,
                "release_gate_eligible": False,
                "must_not_merge_with_emt_trace_evidence": True,
            }
        ]
        with self.assertRaisesRegex(ValueError, "ineligible"):
            build_gateway_report(events)
