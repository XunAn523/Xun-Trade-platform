"""M1 native-outcome observer tests for normalized V2 BrokerEvents."""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from emt_gateway.fastspi_v2_outcome import (
    FastSpiV2BrokerEventObserverMultiplexer,
    FastSpiV2NativeOutcomeError,
    FastSpiV2NativeOutcomeObserver,
    FastSpiV2OutcomeResolution,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    OmsOrderState,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import BrokerOrderObservationV1


@dataclass(frozen=True)
class _Binding:
    account_id: str = "account-1"


@dataclass(frozen=True)
class _Command:
    account_id: str = "account-1"


class _Journal:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def record_callback_observed(self, binding, command, **kwargs):
        self.calls.append((binding, command, kwargs))
        return object()


class _Observer:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[BrokerEvent] = []
        self._fail = fail

    def on_broker_event(self, event: BrokerEvent) -> None:
        if self._fail:
            raise RuntimeError("injected observer failure")
        self.events.append(event)


def _event(kind: BrokerEventKind = BrokerEventKind.ORDER) -> BrokerEvent:
    identity = VenueOrderIdentity(
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
        venue_order_id="venue-1",
    )
    return BrokerEvent(
        event_id="event-1",
        schema_version=1,
        source_key_schema_version=1,
        source_key="source-1",
        event_kind=kind,
        source_kind=BrokerSourceKind.CALLBACK,
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-02"),
        received_monotonic_ns=100,
        venue_order_identity=identity,
        trade_key="trade-1" if kind is BrokerEventKind.TRADE else None,
    )


def _resolution(event: BrokerEvent) -> FastSpiV2OutcomeResolution:
    return FastSpiV2OutcomeResolution(
        binding=_Binding(),
        command=_Command(),
        observation=BrokerOrderObservationV1(
            broker_event_id=event.event_id,
            source_key=event.source_key,
            order_state=OmsOrderState.PARTIALLY_FILLED,
            cumulative_filled_quantity=2,
            leaves_quantity=8,
            trade_keys=("trade-1",),
            venue_order_id="venue-1",
        ),
        transition_id="v2-transition-1",
    )


class FastSpiV2OutcomeTests(unittest.TestCase):
    def test_multiplexer_preserves_order_and_propagates_observer_failure(self) -> None:
        event = _event()
        first = _Observer()
        second = _Observer()
        FastSpiV2BrokerEventObserverMultiplexer(first, second).on_broker_event(event)
        self.assertEqual([event], first.events)
        self.assertEqual([event], second.events)
        with self.assertRaisesRegex(RuntimeError, "injected observer failure"):
            FastSpiV2BrokerEventObserverMultiplexer(_Observer(fail=True)).on_broker_event(event)

    def test_appends_only_a_proven_order_outcome(self) -> None:
        event = _event()
        journal = _Journal()
        observer = FastSpiV2NativeOutcomeObserver(
            journal=journal,
            resolve_outcome=lambda actual: _resolution(actual),
        )
        observer.on_broker_event(event)
        self.assertEqual(1, len(journal.calls))
        self.assertEqual("ORDER", journal.calls[0][2]["attributes"]["broker_event_kind"])

    def test_rejects_missing_proof_and_mismatched_trade_history(self) -> None:
        event = _event(BrokerEventKind.TRADE)
        missing = FastSpiV2NativeOutcomeObserver(
            journal=_Journal(), resolve_outcome=lambda actual: None
        )
        with self.assertRaisesRegex(FastSpiV2NativeOutcomeError, "no proven"):
            missing.on_broker_event(event)
        bad = _resolution(event)
        bad = FastSpiV2OutcomeResolution(
            binding=bad.binding,
            command=bad.command,
            observation=BrokerOrderObservationV1(
                broker_event_id=event.event_id,
                source_key=event.source_key,
                order_state=OmsOrderState.WORKING,
                cumulative_filled_quantity=0,
                leaves_quantity=10,
                trade_keys=(),
                venue_order_id="venue-1",
            ),
            transition_id=bad.transition_id,
        )
        observer = FastSpiV2NativeOutcomeObserver(
            journal=_Journal(), resolve_outcome=lambda actual: bad
        )
        with self.assertRaisesRegex(FastSpiV2NativeOutcomeError, "omits"):
            observer.on_broker_event(event)

    def test_ignores_non_order_state_events(self) -> None:
        journal = _Journal()
        observer = FastSpiV2NativeOutcomeObserver(
            journal=journal, resolve_outcome=lambda actual: None
        )
        observer.on_broker_event(
            BrokerEvent(
                event_id="query-1",
                schema_version=1,
                source_key_schema_version=1,
                source_key="query-source-1",
                event_kind=BrokerEventKind.QUERY,
                source_kind=BrokerSourceKind.CALLBACK,
                venue_id="EMT",
                adapter_instance_id="emt-sim-instance-1",
                account_id="account-1",
                trading_day=TradingDayEpoch("2026-09-02"),
                received_monotonic_ns=100,
            )
        )
        self.assertEqual([], journal.calls)


if __name__ == "__main__":
    unittest.main()
