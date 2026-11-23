"""Registered-route tests for fastspi V2 M1 native outcomes."""

from __future__ import annotations

import unittest
from hashlib import sha256

from emt_gateway.fastspi_v2_outcome import (
    FastSpiV2NativeOutcomeError,
    FastSpiV2NativeOutcomeObserver,
)
from emt_gateway.fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import BrokerOrderObservationV1, OmsFactKind, OmsJournalFact
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    MmapAdmissionEnvelopeV1,
    PreNativeRouteCommand,
)

from route_seed_fixture import recovery_seed


class _Journal:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def record_callback_observed(self, binding, command, **kwargs):
        self.calls.append(kwargs)
        return object()


def _binding() -> AccountJournalBinding:
    return AccountJournalBinding("account-1", "EMT", "xuanling:journal:account-1:", 1)


def _command() -> PreNativeRouteCommand:
    account_id = "account-1"
    execution_id = "execution-1"
    oms_order_id = "oms-order-1"
    intent_hash = sha256(b"intent-1").hexdigest()
    facts = tuple(
        OmsJournalFact(
            fact_id=f"fact-{kind.value}",
            kind=kind,
            account_id=account_id,
            oms_order_id=oms_order_id,
            execution_id=execution_id,
            attributes={"kind": kind.value},
        )
        for kind in (
            OmsFactKind.ORDER_ATTRIBUTED,
            OmsFactKind.RESERVATION_CREATED,
            OmsFactKind.CALL_STARTED,
        )
    )
    return PreNativeRouteCommand(
        account_id=account_id,
        command_id="command-1",
        execution_id=execution_id,
        oms_order_id=oms_order_id,
        client_order_ref="7001",
        route_token_digest=sha256(b"route-token-1").hexdigest(),
        binding_manifest_hash=sha256(b"binding-1").hexdigest(),
        intent_hash=intent_hash,
        decision_context_id="decision-1",
        native_start_deadline_ns=2_000_000,
        fact_envelope=MmapAdmissionEnvelopeV1.build(
            account_id=account_id,
            facts=facts,
            maximum_fact_count=128,
            maximum_encoded_bytes=1024 * 1024,
        ),
        recovery_seed=recovery_seed(
            account_id=account_id,
            oms_order_id=oms_order_id,
            execution_id=execution_id,
            decision_context_id="decision-1",
            intent_hash=intent_hash,
        ),
    )


def _identity(*, venue_order_id: str | None = "venue-1") -> VenueOrderIdentity:
    return VenueOrderIdentity(
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7001",
        client_order_ref="7001",
        venue_order_id=venue_order_id,
    )


def _event(
    kind: BrokerEventKind,
    *,
    source_key: str,
    leaves_qty: int | None = None,
    filled_qty: int | None = None,
    trade_key: str | None = None,
    identity: VenueOrderIdentity | None = None,
) -> BrokerEvent:
    return BrokerEvent(
        event_id=source_key,
        schema_version=1,
        source_key_schema_version=1,
        source_key=source_key,
        event_kind=kind,
        source_kind=BrokerSourceKind.CALLBACK,
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-02"),
        received_monotonic_ns=100,
        venue_order_identity=identity or _identity(),
        execution_id="execution-1",
        trade_key=trade_key,
        leaves_qty=leaves_qty,
        filled_qty=filled_qty,
        normalized_summary={"ledger_state": "LIVE"} if kind is BrokerEventKind.ORDER else {},
    )


class FastSpiV2RouteOutcomeDirectoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = FastSpiV2RouteOutcomeDirectory(
            config=FastSpiV2RouteOutcomeDirectoryConfig(
                account_id="account-1",
                adapter_instance_id="emt-sim-instance-1",
                adapter_client_scope="7001",
                maximum_routes=2,
            )
        )
        self.directory.register_pre_native_route(
            binding=_binding(),
            command=_command(),
            venue_order_identity=_identity(venue_order_id=None),
        )
        self.journal = _Journal()
        self.observer = FastSpiV2NativeOutcomeObserver(
            journal=self.journal,
            resolve_outcome=self.directory.resolve_outcome,
            outcome_committed=self.directory.record_outcome_committed,
        )

    def test_order_then_trade_commits_complete_registered_route_observations(self) -> None:
        working = _event(BrokerEventKind.ORDER, source_key="order-1", leaves_qty=100)
        self.observer.on_broker_event(working)
        trade = _event(
            BrokerEventKind.TRADE,
            source_key="trade-1",
            filled_qty=25,
            trade_key="trade-key-1",
        )
        self.observer.on_broker_event(trade)
        self.assertEqual(2, len(self.journal.calls))
        observation = self.journal.calls[-1]["observation"]
        self.assertIsInstance(observation, BrokerOrderObservationV1)
        assert isinstance(observation, BrokerOrderObservationV1)
        self.assertEqual(25, observation.cumulative_filled_quantity)
        self.assertEqual(75, observation.leaves_quantity)
        self.assertEqual(("trade-key-1",), observation.trade_keys)

    def test_rejects_unknown_or_out_of_order_callbacks_without_journal_write(self) -> None:
        with self.assertRaisesRegex(FastSpiV2NativeOutcomeError, "prior state"):
            self.observer.on_broker_event(
                _event(
                    BrokerEventKind.TRADE,
                    source_key="trade-first",
                    filled_qty=25,
                    trade_key="trade-key-1",
                )
            )
        with self.assertRaisesRegex(FastSpiV2NativeOutcomeError, "registered route"):
            self.observer.on_broker_event(
                _event(
                    BrokerEventKind.ORDER,
                    source_key="unknown-route",
                    leaves_qty=100,
                    identity=VenueOrderIdentity(
                        venue_id="EMT",
                        adapter_instance_id="emt-sim-instance-1",
                        adapter_client_scope="7001",
                        client_order_ref="other",
                    ),
                )
            )
        self.assertEqual([], self.journal.calls)

    def test_duplicate_source_reuses_exact_committed_outcome(self) -> None:
        event = _event(BrokerEventKind.ORDER, source_key="order-1", leaves_qty=100)
        self.observer.on_broker_event(event)
        self.observer.on_broker_event(event)
        self.assertEqual(2, len(self.journal.calls))
        self.assertEqual(
            self.journal.calls[0]["transition_id"], self.journal.calls[1]["transition_id"]
        )

    def test_first_venue_order_id_becomes_a_route_fence(self) -> None:
        self.observer.on_broker_event(
            _event(BrokerEventKind.ORDER, source_key="order-1", leaves_qty=100)
        )
        with self.assertRaisesRegex(FastSpiV2NativeOutcomeError, "does not match route"):
            self.observer.on_broker_event(
                _event(
                    BrokerEventKind.TRADE,
                    source_key="trade-other-venue",
                    filled_qty=25,
                    trade_key="trade-key-1",
                    identity=_identity(venue_order_id="another-venue-order"),
                )
            )
        self.assertEqual(1, len(self.journal.calls))


if __name__ == "__main__":
    unittest.main()
