from __future__ import annotations

import sys
import unittest
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_ROOT = ROOT / "tools" / "performance"
if str(PERFORMANCE_ROOT) not in sys.path:
    sys.path.insert(0, str(PERFORMANCE_ROOT))

from emt_gateway.gateway import (  # noqa: E402
    CallbackPipeline,  # noqa: E402
    OrderGateway,
    OrderIntent,
)
from emt_gateway.journal import InMemoryJournal  # noqa: E402
from emt_gateway.query_registry import QueryCompletion, QueryRegistry  # noqa: E402
from perfbench.gateway_report import build_gateway_report  # noqa: E402
from perfbench.trace import GatewayTraceCollector  # noqa: E402


class NativeForTraceTest:
    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        return 301

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        return 302

    def get_last_error_code(self) -> int:
        return 0


class PerformanceIntegrationTests(unittest.TestCase):
    def test_p2_events_are_directly_consumable_by_the_gateway_report(self) -> None:
        collector = GatewayTraceCollector()
        gateway = OrderGateway(
            native=NativeForTraceTest(),
            command_journal=InMemoryJournal(),
            event_journal=InMemoryJournal(),
            trace=collector,
        )
        submitted = gateway.submit_order(
            OrderIntent(
                client_order_id="performance-order-1",
                payload_hash=sha256(b"performance-order-1").hexdigest(),
            )
        )
        gateway.callbacks.receive(
            "OnOrderEvent",
            profile="ordinary",
            workload="steady",
            order_emt_id=submitted.native_id,
        )
        delivery = gateway.callbacks.normalize_one()
        self.assertIsNotNone(delivery)
        gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery

        report = build_gateway_report(collector.export_events())
        api = next(
            item for item in report["scenarios"] if item["key"] == "api:InsertOrder:ordinary:steady"
        )
        callback = next(
            item
            for item in report["scenarios"]
            if item["key"] == "spi:OnOrderEvent:ordinary:steady"
        )
        self.assertEqual(1, api["latency"]["C0_to_C7"]["count"])
        self.assertEqual(1, callback["latency"]["C0_to_C7"]["count"])
        self.assertEqual("insufficient_samples", api["evidence_status"])
        self.assertFalse(report["summary"]["p0_ready"])

    def test_query_registry_events_are_directly_consumable_by_the_gateway_report(self) -> None:
        collector = GatewayTraceCollector()
        registry = QueryRegistry(trace=collector)
        pipeline = CallbackPipeline(
            journal=InMemoryJournal(),
            trace=collector,
            lookup_source=lambda record: None,
            capacity=4,
            lifecycle_observer=registry,
        )
        self.assertIsInstance(pipeline.native_fastspi_available, bool)
        handle = registry.open(
            api_name="QueryAsset",
            callback_name="OnQueryAsset",
            correlation_id="query-performance-correlation",
            profile="ordinary",
            workload="steady",
            completion=QueryCompletion.LAST,
        )
        registry.mark_native_started(handle.request_id)
        registry.mark_native_returned(handle.request_id, 0)
        pipeline.receive(
            "OnQueryAsset",
            profile="ordinary",
            workload="steady",
            request_id=handle.request_id,
            is_last=True,
        )
        delivery = pipeline.normalize_one()
        self.assertIsNotNone(delivery)
        pipeline.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery

        report = build_gateway_report(collector.export_events())
        query = next(
            item for item in report["scenarios"] if item["key"] == "api:QueryAsset:ordinary:steady"
        )
        self.assertEqual(1, query["latency"]["C0_to_C7"]["count"])
        self.assertEqual("insufficient_samples", query["evidence_status"])
