from __future__ import annotations

import unittest
from threading import Event, get_ident

from emt_gateway.operational_telemetry import (
    BoundedOperationalTelemetry,
    OperationalTelemetryEvent,
    OtlpGrpcMetricExporter,
    TelemetryError,
)


class _RecordingExporter:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[OperationalTelemetryEvent] = []
        self.thread_ids: list[int] = []
        self.fail = fail
        self.closed = False
        self.called = Event()

    def export(self, events: tuple[OperationalTelemetryEvent, ...]) -> None:
        self.thread_ids.append(get_ident())
        self.called.set()
        if self.fail:
            raise ConnectionError("collector unavailable")
        self.events.extend(events)

    def close(self) -> None:
        self.closed = True


class OperationalTelemetryEventTests(unittest.TestCase):
    def test_event_keeps_required_identifiers_but_hashes_account(self) -> None:
        event = OperationalTelemetryEvent.create(
            event_name="command.accepted",
            consumer_id="consumer-1",
            consumer_account_id="operator-7",
            strategy_id="strategy.alpha",
            account_id="1234567890",
            journal_lsn=42,
            command_id="command-1",
            execution_id="execution-1",
            order_client_id=99,
            session_state="READY",
            failure_category="native_timeout",
        )

        attributes = event.attributes()
        self.assertEqual("consumer-1", attributes["consumer.id"])
        self.assertEqual("99", attributes["order.client_id"])
        self.assertEqual("sha256:c775e7b757ede630", attributes["account.id_hash"])
        self.assertNotIn("1234567890", attributes.values())
        self.assertNotIn("account.id", attributes)

    def test_rejects_raw_or_unbounded_attribute_values(self) -> None:
        with self.assertRaisesRegex(TelemetryError, "event_name"):
            OperationalTelemetryEvent.create(event_name="Command Accepted")
        with self.assertRaisesRegex(TelemetryError, "strategy_id"):
            OperationalTelemetryEvent.create(
                event_name="command.accepted", strategy_id="strategy secret"
            )
        with self.assertRaisesRegex(TelemetryError, "account_id"):
            OperationalTelemetryEvent.create(event_name="command.accepted", account_id=" ")
        with self.assertRaisesRegex(TelemetryError, "account_id_hash"):
            OperationalTelemetryEvent(
                event_name="command.accepted",
                timestamp_ns=1,
                account_id_hash="1234567890",
            )


class BoundedOperationalTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = OperationalTelemetryEvent.create(
            event_name="callback.durable",
            command_id="command-1",
            journal_lsn=1,
        )

    def test_emit_is_bounded_and_never_calls_exporter_inline(self) -> None:
        exporter = _RecordingExporter()
        telemetry = BoundedOperationalTelemetry(exporter=exporter, capacity=1, batch_size=1)
        caller_thread = get_ident()

        self.assertTrue(telemetry.emit(self.event))
        self.assertFalse(telemetry.emit(self.event))
        self.assertFalse(exporter.called.is_set())
        self.assertEqual(1, telemetry.stats().dropped)

        telemetry.start()
        self.assertTrue(telemetry.wait_until_idle(1.0))
        telemetry.close()

        self.assertEqual([self.event], exporter.events)
        self.assertNotIn(caller_thread, exporter.thread_ids)
        self.assertTrue(exporter.closed)

    def test_unavailable_collector_drops_only_background_telemetry(self) -> None:
        exporter = _RecordingExporter(fail=True)
        telemetry = BoundedOperationalTelemetry(exporter=exporter, capacity=2, batch_size=2)
        telemetry.start()

        self.assertTrue(telemetry.emit(self.event))
        self.assertTrue(telemetry.wait_until_idle(1.0))
        stats = telemetry.stats()
        telemetry.close()

        self.assertEqual(1, stats.accepted)
        self.assertEqual(1, stats.dropped)
        self.assertEqual(0, stats.exported)
        self.assertEqual(1, stats.export_failed)
        self.assertTrue(exporter.closed)


class OtlpGrpcMetricExporterTests(unittest.TestCase):
    def test_constructs_without_connecting_to_the_collector(self) -> None:
        exporter = OtlpGrpcMetricExporter(
            endpoint="127.0.0.1:4317",
            export_interval_millis=60_000,
        )
        self.addCleanup(exporter.close)
