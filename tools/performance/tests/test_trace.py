from __future__ import annotations

import unittest

from perfbench.trace import GatewayTraceCollector, TraceError, collector_from_events


class TraceTests(unittest.TestCase):
    def test_collects_a_complete_query_trace_without_business_payload(self) -> None:
        collector = GatewayTraceCollector()
        collector.begin(
            "run-1",
            kind="api",
            interface="QueryAsset",
            profile="ordinary",
            workload="steady",
            timestamp_ns=100,
        )
        for stage, timestamp in (
            ("C1", 110),
            ("C3", 120),
            ("C4", 125),
            ("C5", 130),
            ("C6", 140),
            ("C7", 150),
        ):
            collector.mark("run-1", stage, timestamp)
        collector.complete("run-1")
        events = collector.export_events()
        self.assertFalse(any("payload" in event for event in events))
        rebuilt = collector_from_events(events).records()
        self.assertEqual(1, len(rebuilt))
        self.assertEqual("ok", rebuilt[0].outcome)
        self.assertEqual(150, rebuilt[0].timestamps_ns["C7"])

    def test_records_only_allowlisted_health_metrics(self) -> None:
        collector = GatewayTraceCollector()
        collector.record_metric("callback_ring_depth", 7, 200)
        self.assertEqual(7, collector.metrics()[0].value)
        with self.assertRaises(TraceError):
            collector.record_metric("account_number", 1, 201)

    def test_rejects_non_monotonic_timestamps(self) -> None:
        collector = GatewayTraceCollector()
        collector.begin(
            "run-2",
            kind="api",
            interface="QueryAsset",
            profile="ordinary",
            workload="steady",
            timestamp_ns=100,
        )
        with self.assertRaises(TraceError):
            collector.mark("run-2", "C1", 99)

    def test_rejects_identity_mismatch_for_the_same_correlation_id(self) -> None:
        collector = GatewayTraceCollector()
        collector.begin(
            "run-3",
            kind="api",
            interface="QueryAsset",
            profile="ordinary",
            workload="steady",
            timestamp_ns=100,
        )
        with self.assertRaises(TraceError):
            collector.ingest(
                {
                    "correlation_id": "run-3",
                    "kind": "api",
                    "interface": "QueryFund",
                    "profile": "ordinary",
                    "workload": "steady",
                    "stage": "C1",
                    "timestamp_ns": 110,
                }
            )
