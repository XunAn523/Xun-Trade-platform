"""Contract tests for normalized EMT order-side callback ingress."""

from __future__ import annotations

import unittest
from threading import Thread
from time import monotonic, sleep

from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerEventIngress,
    EmtBrokerEventIngressError,
    EmtNativeBrokerEvent,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    CallbackGapEvidence,
    TradingDayEpoch,
)

DAY = TradingDayEpoch("2026-09-04")


class _Observer:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    ), manifest


def _event(*, event_kind: BrokerEventKind = BrokerEventKind.ORDER, **changes):
    values = {
        "event_kind": event_kind,
        "account_id": "account-1",
        "trading_day": DAY,
        "received_monotonic_ns": 101,
        "order_emt_id": 91,
        "order_client_id": 17,
        "order_cancel_emt_id": None,
        "order_status": 50,
        "filled_qty": 0,
        "leaves_qty": 100,
        "trade_key": "trade-1" if event_kind is BrokerEventKind.TRADE else None,
        "cancel_request_id": "cancel-1" if event_kind is BrokerEventKind.CANCEL_ERROR else None,
    }
    values.update(changes)
    return EmtNativeBrokerEvent(**values)


class EmtBrokerEventIngressTests(unittest.TestCase):
    def _ingress(self, *, capacity: int = 2):
        runtime, manifest = _mapping()
        mapping = EmtAdapterProfile(manifest).activate(runtime=runtime, activated_at_unix_s=0)
        ingress = EmtBrokerEventIngress(mapping, trading_day=DAY, capacity=capacity)
        observer = _Observer()
        ingress.attach_observer(observer)
        return ingress, observer

    def test_order_trade_and_cancel_events_are_normalized_without_vendor_payload(self) -> None:
        ingress, observer = self._ingress()
        ingress.offer(_event())
        ingress.offer(
            _event(
                event_kind=BrokerEventKind.TRADE,
                trade_key="trade-report-1",
                filled_qty=100,
                leaves_qty=0,
            )
        )
        self.assertEqual(ingress.drain(), 2)
        self.assertEqual(observer.events[0].venue_order_identity.client_order_ref, "17")
        self.assertEqual(observer.events[1].trade_key, "trade-report-1")
        self.assertTrue(observer.events[0].source_key.startswith("E1:"))

    def test_duplicate_is_idempotent_but_same_source_with_changed_summary_conflicts(self) -> None:
        ingress, _observer = self._ingress()
        event = _event(normalized_summary={"state": "working"})
        ingress.offer(event)
        ingress.offer(event)
        with self.assertRaisesRegex(EmtBrokerEventIngressError, "conflicts"):
            ingress.offer(_event(normalized_summary={"state": "changed"}))

    def test_full_ingress_reports_a_callback_gap(self) -> None:
        ingress, observer = self._ingress(capacity=1)
        ingress.offer(_event(order_emt_id=91))
        ingress.offer(_event(order_emt_id=92))
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual(observer.gaps[0].rejected_count, 1)

    def test_force_gap_retains_only_a_fixed_payload_free_reason(self) -> None:
        ingress, observer = self._ingress()
        ingress.force_gap(
            account_id="account-1",
            observed_monotonic_ns=102,
            reason="bridge_trade_identity",
        )
        self.assertEqual({"bridge_trade_identity": 1}, ingress.callback_gap_reason_counts)
        self.assertEqual(1, len(observer.gaps))
        with self.assertRaisesRegex(EmtBrokerEventIngressError, "reason"):
            ingress.force_gap(
                account_id="account-1",
                observed_monotonic_ns=103,
                reason="not ascii: 非",
            )

    def test_foreign_account_is_rejected_before_delivery(self) -> None:
        ingress, observer = self._ingress()
        with self.assertRaisesRegex(EmtBrokerEventIngressError, "another profile"):
            ingress.offer(_event(account_id="another-account"))
        self.assertEqual(observer.events, [])

    def test_blocking_drain_wakes_immediately_when_native_callback_is_enqueued(self) -> None:
        ingress, observer = self._ingress()

        def offer_callback() -> None:
            sleep(0.01)
            ingress.offer(_event())

        producer = Thread(target=offer_callback)
        producer.start()
        started = monotonic()
        self.assertEqual(1, ingress.drain(wait_timeout_seconds=1.0))
        elapsed = monotonic() - started
        producer.join(timeout=1.0)
        self.assertFalse(producer.is_alive())
        self.assertLess(elapsed, 0.2)
        self.assertEqual(1, len(observer.events))

    def test_blocking_drain_rejects_invalid_timeout(self) -> None:
        ingress, _observer = self._ingress()
        with self.assertRaisesRegex(EmtBrokerEventIngressError, "finite non-negative"):
            ingress.drain(wait_timeout_seconds=-0.001)


if __name__ == "__main__":
    unittest.main()
