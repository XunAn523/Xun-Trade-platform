"""Internal acceptance for the production-shaped QMT Adapter bridge boundary."""

from __future__ import annotations

import unittest
from dataclasses import dataclass, replace
from unittest.mock import patch

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerEventObserver,
    BrokerQuery,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.base import CancelExecutionInstruction, QueryHandle, RecoveryHandle
from xuanling_tradecore.adapters.qmt import (
    QmtAdapterProfile,
    QmtBridgeCancelReceipt,
    QmtBridgeDeliverySink,
    QmtBridgeError,
    QmtBridgeEventDelivery,
    QmtBridgeGapDelivery,
    QmtBridgeHealth,
    QmtBridgeNativeEvent,
    QmtBridgePort,
    QmtBridgeSubmitReceipt,
    QmtBrokerAdapterError,
    QmtCapabilityActivation,
    QmtCapabilityManifestV1,
    QmtMutationUnavailableError,
    load_builtin_qmt_fake_manifest,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    CallbackGapEvidence,
    CancelRequest,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    RecoveryPlan,
    TradingDayEpoch,
    VenueOrderIdentity,
)

DAY = TradingDayEpoch("2026-09-16")


class _Observer(BrokerEventObserver):
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


@dataclass
class _Bridge(QmtBridgePort):
    ready: bool = True

    def __post_init__(self) -> None:
        self.sink: QmtBridgeDeliverySink | None = None
        self.submits = []
        self.cancels = []
        self.acks: list[int] = []

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        self.sink = sink

    def close(self) -> None:
        self.sink = None

    def health(self) -> QmtBridgeHealth:
        return QmtBridgeHealth(self.ready, "READY" if self.ready else "NOT_READY")

    def submit(self, command):
        self.submits.append(command)
        return QmtBridgeSubmitReceipt(command.instruction.execution_id, 101)

    def cancel(
        self, request: CancelRequest, identity: VenueOrderIdentity
    ) -> QmtBridgeCancelReceipt:
        self.cancels.append((request, identity))
        return QmtBridgeCancelReceipt(request.cancel_request_id, 102, identity)

    def query(self, request: BrokerQuery) -> QueryHandle:
        return QueryHandle(request.request_id, "qmt-contract-fake-v1", "qmt-fake-v1")

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        return RecoveryHandle(plan.recovery_run_id, "qmt-contract-fake-v1", "qmt-fake-v1")

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        return BrokerRecoveryEvidence(
            evidence_id="evidence-1",
            recovery_run_id=handle.recovery_run_id,
            account_id="qmt-fake-account-1",
            venue_id="QMT",
            adapter_instance_id="qmt-fake-v1",
            trading_day=DAY,
            callback_replay_complete=False,
            orders_complete=False,
            trades_complete=False,
            working_orders_complete=False,
            risk_snapshot_complete=False,
            negative_confirmation_complete=False,
        )

    def acknowledge(self, delivery_id: int) -> None:
        self.acks.append(delivery_id)

    def deliver(self, delivery) -> None:
        assert self.sink is not None
        self.sink(delivery)


def _runtime(manifest):
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("qmt-fake-account-1", True, False, False),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )


def _verified_capability(mapping) -> QmtCapabilityActivation:  # type: ignore[no-untyped-def]
    ownership = mapping.manifest.scope_ownership
    binding = mapping.manifest.route_token_binding
    assert ownership is not None and binding is not None
    # The signature verification itself is covered by test_qmt_capability...;
    # this fixture represents the already-verified output handed to the bridge.
    return QmtCapabilityActivation(
        manifest=QmtCapabilityManifestV1(
            manifest_id="verified-edge-fixture",
            signer_id="fixture-signer",
            adapter_instance_id=ownership.adapter_instance_id,
            adapter_client_scope=ownership.adapter_client_scope,
            mapping_manifest_hash=mapping.manifest.manifest_hash,
            edge_instance_digest="a" * 64,
            qmt_build_digest="b" * 64,
            embedded_python_version="3.6",
            native_abi_digest="c" * 64,
            route_binding_digest=binding.manifest_hash,
            evidence_digest="d" * 64,
            valid_from_wall_ns=0,
            valid_until_wall_ns=None,
            capabilities=frozenset(
                {
                    "embedded_python_36",
                    "run_time",
                    "quick_trade_2",
                    "single_native_caller",
                    "native_mapping_interlocked",
                    "route_identity_echo",
                    "order_callback",
                    "query_complete",
                }
            ),
            signature="fixture-signature",
        )
    )


def _trace() -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="qmt-fake-account-1",
        strategy_id=None,
        strategy_order_id=None,
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.MANUAL,
    )


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="qmt-fake-account-1",
        trading_day=DAY,
        consumer_trace=_trace(),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )


class QmtBridgeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        manifest = load_builtin_qmt_fake_manifest()
        self.mapping = QmtAdapterProfile(manifest).activate(
            runtime=_runtime(manifest), activated_at_unix_s=0
        )
        self.bridge = _Bridge()
        self.capability = _verified_capability(self.mapping)
        self.adapter = QmtAdapterProfile.create_bridge_adapter(
            self.mapping,
            bridge=self.bridge,
            client_order_ref_factory=lambda _: "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
        )
        self.observer = _Observer()

    def test_disarmed_bridge_adapter_is_query_only(self) -> None:
        self.adapter.start(self.observer)
        self.assertFalse(self.adapter.capability_available)
        with self.assertRaises(QmtMutationUnavailableError):
            self.adapter.submit(_instruction())
        query = self.adapter.query(
            BrokerQuery("query-1", "qmt-fake-account-1", "QMT", "qmt-fake-v1", "orders", 10)
        )
        self.assertEqual(query.request_id, "query-1")
        self.assertEqual(self.adapter.snapshot().submit_attempts, 0)

    def test_mutation_cannot_be_enabled_without_verified_capability(self) -> None:
        with self.assertRaisesRegex(QmtMutationUnavailableError, "capability evidence"):
            QmtAdapterProfile.create_bridge_adapter(
                self.mapping,
                bridge=self.bridge,
                client_order_ref_factory=lambda _: "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
                mutation_enabled=True,
            )

    def test_enabled_adapter_maps_and_submits_only_to_edge(self) -> None:
        self.adapter = QmtAdapterProfile.create_bridge_adapter(
            self.mapping,
            bridge=self.bridge,
            client_order_ref_factory=lambda _: "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
            capability_activation=self.capability,
            mutation_enabled=True,
        )
        self.adapter.start(self.observer)
        accepted = self.adapter.submit(_instruction())
        self.assertEqual(accepted.execution_id, "execution-1")
        self.assertEqual(len(self.bridge.submits), 1)
        self.assertEqual(self.bridge.submits[0].mapped_order.side, 1)
        self.assertIsNone(accepted.venue_order_identity)

    def test_enabled_adapter_cancels_only_a_proven_scope_identity(self) -> None:
        self.adapter = QmtAdapterProfile.create_bridge_adapter(
            self.mapping,
            bridge=self.bridge,
            client_order_ref_factory=lambda _: "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
            capability_activation=self.capability,
            mutation_enabled=True,
        )
        self.adapter.start(self.observer)
        request = CancelRequest(
            cancel_request_id="cancel-1",
            oms_order_id="oms-1",
            execution_id="execution-1",
            account_id="qmt-fake-account-1",
            consumer_trace=_trace(),
            reason="operator request",
            requested_monotonic_ns=11,
        )
        identity = VenueOrderIdentity(
            venue_id="QMT",
            adapter_instance_id="qmt-fake-v1",
            adapter_client_scope="qmt-fake-scope-1",
            client_order_ref="Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
        )
        accepted = self.adapter.cancel(CancelExecutionInstruction(request, identity))
        self.assertEqual(accepted.cancel_request_id, "cancel-1")
        self.assertEqual(self.bridge.cancels, [(request, identity)])

    def test_recovery_handle_and_evidence_are_profile_fenced(self) -> None:
        self.adapter.start(self.observer)
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="qmt-fake-account-1",
            venue_id="QMT",
            adapter_instance_id="qmt-fake-v1",
            trading_day=DAY,
            journal_id="journal-1",
            expected_journal_lsn=0,
        )
        handle = self.adapter.begin_recovery(plan, self.observer)
        evidence = self.adapter.collect_recovery_evidence(handle)
        self.assertFalse(evidence.is_complete)
        self.assertEqual(self.adapter.snapshot().recovery_attempts, 1)

    def test_event_is_delivered_once_then_acknowledged(self) -> None:
        self.adapter.start(self.observer)
        event = QmtBridgeNativeEvent(
            external_event_id="event-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            account_id="qmt-fake-account-1",
            trading_day=DAY,
            received_monotonic_ns=12,
            execution_id="execution-1",
        )
        self.bridge.deliver(QmtBridgeEventDelivery(7, event))
        self.bridge.deliver(QmtBridgeEventDelivery(8, event))
        self.assertEqual(len(self.observer.events), 1)
        self.assertEqual(self.bridge.acks, [7, 8])
        self.assertEqual(self.adapter.snapshot().acknowledged_delivery_id, 8)

    def test_event_id_conflict_is_fail_closed_without_ack(self) -> None:
        self.adapter.start(self.observer)
        first = QmtBridgeNativeEvent(
            external_event_id="event-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            account_id="qmt-fake-account-1",
            trading_day=DAY,
            received_monotonic_ns=12,
        )
        second = QmtBridgeNativeEvent(
            external_event_id="event-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            account_id="qmt-fake-account-1",
            trading_day=DAY,
            received_monotonic_ns=13,
            execution_id="another-execution",
        )
        self.bridge.deliver(QmtBridgeEventDelivery(7, first))
        with self.assertRaises(QmtBrokerAdapterError):
            self.bridge.deliver(QmtBridgeEventDelivery(8, second))
        self.assertEqual(self.bridge.acks, [7])

    def test_gap_is_forwarded_before_edge_ack(self) -> None:
        self.adapter.start(self.observer)
        gap = CallbackGapEvidence("gap-1", "qmt-fake-account-1", "qmt-edge", 4, 4, 1, 20)
        self.bridge.deliver(QmtBridgeGapDelivery(9, gap))
        self.assertEqual(self.observer.gaps, [gap])
        self.assertEqual(self.bridge.acks, [9])

    def test_not_ready_edge_never_starts(self) -> None:
        self.bridge.ready = False
        with self.assertRaisesRegex(QmtBrokerAdapterError, "health probe"):
            self.adapter.start(self.observer)
        self.assertFalse(self.adapter.snapshot().started)

    def _start_enabled(self) -> None:
        self.adapter = QmtAdapterProfile.create_bridge_adapter(
            self.mapping,
            bridge=self.bridge,
            client_order_ref_factory=lambda _: "Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
            capability_activation=self.capability,
            mutation_enabled=True,
        )
        self.adapter.start(self.observer)

    @staticmethod
    def _event(event_id: str = "event-1") -> QmtBridgeNativeEvent:
        return QmtBridgeNativeEvent(
            external_event_id=event_id,
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            account_id="qmt-fake-account-1",
            trading_day=DAY,
            received_monotonic_ns=12,
        )

    def _assert_mutations_blocked(self) -> None:
        self.assertTrue(self.adapter.snapshot().mutation_blocked)
        with self.assertRaisesRegex(QmtMutationUnavailableError, "reconciliation"):
            self.adapter.submit(_instruction())
        self.assertEqual(self.bridge.submits, [])

    def test_sequence_gap_stops_new_orders_without_ack_or_projection(self) -> None:
        self._start_enabled()
        self.bridge.deliver(QmtBridgeEventDelivery(7, self._event()))
        with self.assertRaisesRegex(QmtBrokerAdapterError, "contiguous"):
            self.bridge.deliver(QmtBridgeEventDelivery(9, self._event("event-2")))
        self.assertEqual(self.bridge.acks, [7])
        self.assertEqual(len(self.observer.events), 1)
        self._assert_mutations_blocked()

    def test_delivery_regression_does_not_regress_cursor(self) -> None:
        self._start_enabled()
        self.bridge.deliver(QmtBridgeEventDelivery(7, self._event()))
        with self.assertRaisesRegex(QmtBrokerAdapterError, "contiguous"):
            self.bridge.deliver(QmtBridgeEventDelivery(6, self._event()))
        self.assertEqual(self.adapter.snapshot().acknowledged_delivery_id, 7)
        self._assert_mutations_blocked()

    def test_same_delivery_id_cannot_replace_event(self) -> None:
        self._start_enabled()
        self.bridge.deliver(QmtBridgeEventDelivery(7, self._event()))
        with self.assertRaisesRegex(QmtBrokerAdapterError, "delivery id"):
            self.bridge.deliver(QmtBridgeEventDelivery(7, self._event("another-event")))
        self.assertEqual(self.bridge.acks, [7])
        self._assert_mutations_blocked()

    def test_foreign_gap_is_not_forwarded_or_acknowledged(self) -> None:
        self._start_enabled()
        gap = CallbackGapEvidence("gap-1", "foreign-account", "qmt-edge", 4, 4, 1, 20)
        with self.assertRaisesRegex(QmtBrokerAdapterError, "another account"):
            self.bridge.deliver(QmtBridgeGapDelivery(7, gap))
        self.assertEqual(self.observer.gaps, [])
        self.assertEqual(self.bridge.acks, [])
        self._assert_mutations_blocked()

    def test_gap_blocks_mutations_across_restart_and_keeps_queries_available(self) -> None:
        self._start_enabled()
        gap = CallbackGapEvidence("gap-1", "qmt-fake-account-1", "qmt-edge", 4, 4, 1, 20)
        self.bridge.deliver(QmtBridgeGapDelivery(7, gap))
        self.adapter.stop()
        self.adapter.start(self.observer)
        self.bridge.deliver(QmtBridgeGapDelivery(7, gap))
        self.assertEqual(self.observer.gaps, [gap])
        self.assertEqual(self.bridge.acks, [7, 7])
        self._assert_mutations_blocked()
        handle = self.adapter.query(
            BrokerQuery("query-1", "qmt-fake-account-1", "QMT", "qmt-fake-v1", "orders", 10)
        )
        self.assertEqual(handle.request_id, "query-1")

    def test_ack_failure_replays_without_duplicate_projection_or_rearming(self) -> None:
        self._start_enabled()
        delivery = QmtBridgeEventDelivery(7, self._event())
        with patch.object(self.bridge, "acknowledge", side_effect=QmtBridgeError("ACK lost")):
            with self.assertRaises(QmtBridgeError):
                self.bridge.deliver(delivery)
        self.assertIsNone(self.adapter.snapshot().acknowledged_delivery_id)
        with self.assertRaisesRegex(QmtBrokerAdapterError, "unacknowledged"):
            self.bridge.deliver(QmtBridgeEventDelivery(8, self._event("event-2")))
        self.bridge.deliver(
            replace(delivery, event=replace(delivery.event, received_monotonic_ns=99))
        )
        self.assertEqual(len(self.observer.events), 1)
        self.assertEqual(self.bridge.acks, [7])
        self._assert_mutations_blocked()

    def test_observer_failure_is_not_acknowledged_or_remembered_as_committed(self) -> None:
        self._start_enabled()
        delivery = QmtBridgeEventDelivery(7, self._event())
        with patch.object(self.observer, "on_broker_event", side_effect=OSError("journal failed")):
            with self.assertRaises(OSError):
                self.bridge.deliver(delivery)
        self.assertEqual(self.bridge.acks, [])
        self.bridge.deliver(delivery)
        self.assertEqual(len(self.observer.events), 1)
        self.assertEqual(self.bridge.acks, [7])
        self._assert_mutations_blocked()

    def test_submit_transport_failure_blocks_a_second_attempt(self) -> None:
        self._start_enabled()
        with patch.object(self.bridge, "submit", side_effect=QmtBridgeError("connection lost")):
            with self.assertRaises(QmtBridgeError):
                self.adapter.submit(_instruction())
        self._assert_mutations_blocked()

    def test_event_conflict_latches_stop_new_order_gate(self) -> None:
        self._start_enabled()
        self.bridge.deliver(QmtBridgeEventDelivery(7, self._event()))
        with self.assertRaisesRegex(QmtBrokerAdapterError, "event id"):
            self.bridge.deliver(
                QmtBridgeEventDelivery(8, replace(self._event(), execution_id="changed"))
            )
        self._assert_mutations_blocked()


if __name__ == "__main__":
    unittest.main()
