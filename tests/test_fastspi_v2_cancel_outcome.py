"""V2 cancel-error callback persistence tests."""

from __future__ import annotations

import unittest
from hashlib import sha256

from emt_gateway.fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeError,
    FastSpiV2CancelOutcomeObserver,
)
from xuanling_tradecore.adapters import canonical_venue_order_target_hash
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import OmsFactKind, OmsJournalFact
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    CancelRouteCommand,
    MmapAdmissionEnvelopeV1,
)


class _Journal:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def record_rejected(self, binding, command, **kwargs):
        self.calls.append(kwargs)
        return object()


def _binding() -> AccountJournalBinding:
    return AccountJournalBinding("account-1", "EMT", "xuanling:journal:account-1:", 1)


def _target(*, venue_cancel_id: str | None = None) -> VenueOrderIdentity:
    return VenueOrderIdentity(
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7001",
        client_order_ref="7001",
        venue_order_id="venue-order-1",
        venue_cancel_id=venue_cancel_id,
    )


def _command() -> CancelRouteCommand:
    facts = tuple(
        OmsJournalFact(
            fact_id=f"fact-{kind.value}",
            kind=kind,
            account_id="account-1",
            oms_order_id="oms-order-1",
            execution_id="execution-1",
            attributes={"cancel_request_id": "cancel-1"},
        )
        for kind in (OmsFactKind.CANCEL_REQUESTED, OmsFactKind.CANCEL_STARTED)
    )
    return CancelRouteCommand(
        account_id="account-1",
        cancel_request_id="cancel-1",
        oms_order_id="oms-order-1",
        execution_id="execution-1",
        target_client_order_ref="7001",
        target_route_payload_hash=sha256(b"target-route").hexdigest(),
        venue_order_identity_hash=canonical_venue_order_target_hash(_target()),
        native_start_deadline_ns=2_000_000,
        fact_envelope=MmapAdmissionEnvelopeV1.build(
            account_id="account-1",
            facts=facts,
            maximum_fact_count=128,
            maximum_encoded_bytes=1024 * 1024,
        ),
    )


def _event(
    *, source_key: str = "cancel-error-1", identity: VenueOrderIdentity | None = None
) -> BrokerEvent:
    return BrokerEvent(
        event_id=source_key,
        schema_version=1,
        source_key_schema_version=1,
        source_key=source_key,
        event_kind=BrokerEventKind.CANCEL_ERROR,
        source_kind=BrokerSourceKind.CALLBACK,
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-02"),
        received_monotonic_ns=100,
        venue_order_identity=identity or _target(venue_cancel_id="venue-cancel-1"),
        execution_id="execution-1",
        cancel_request_id="cancel-1",
    )


class FastSpiV2CancelOutcomeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = _Journal()
        self.observer = FastSpiV2CancelOutcomeObserver(
            config=FastSpiV2CancelOutcomeDirectoryConfig(
                account_id="account-1",
                adapter_instance_id="emt-sim-instance-1",
                adapter_client_scope="7001",
            ),
            journal=self.journal,
        )
        self.observer.register_cancel_route(
            binding=_binding(), command=_command(), target_identity=_target()
        )

    def test_commits_registered_cancel_error_as_venue_rejected(self) -> None:
        self.observer.on_broker_event(_event())
        self.assertEqual("VENUE_REJECTED", self.journal.calls[0]["error_category"])

    def test_rejects_another_target_without_journal_write(self) -> None:
        with self.assertRaisesRegex(FastSpiV2CancelOutcomeError, "does not match"):
            self.observer.on_broker_event(
                _event(
                    identity=VenueOrderIdentity(
                        venue_id="EMT",
                        adapter_instance_id="emt-sim-instance-1",
                        adapter_client_scope="7001",
                        client_order_ref="7001",
                        venue_order_id="other-order",
                    )
                )
            )
        self.assertEqual([], self.journal.calls)

    def test_duplicate_source_reuses_the_same_transition_id(self) -> None:
        self.observer.on_broker_event(_event())
        self.observer.on_broker_event(_event())
        self.assertEqual(
            self.journal.calls[0]["transition_id"], self.journal.calls[1]["transition_id"]
        )


if __name__ == "__main__":
    unittest.main()
