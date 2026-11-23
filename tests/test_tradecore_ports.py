from __future__ import annotations

import ast
import unittest
from dataclasses import replace
from pathlib import Path
from typing import cast

from emt_gateway.gateway import OrderGateway
from xuanling_tradecore.adapters import (
    BrokerAdapter,
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    SubmitAccepted,
)
from xuanling_tradecore.compatibility import (
    LegacyCanonicalInstruction,
    LegacyInvocationContext,
    LegacyOrderGatewayBridge,
    LegacyOrderRequest,
)
from xuanling_tradecore.consumer import (
    ConsumerAccountOrderSnapshot,
    ConsumerAccountView,
    ConsumerEventPage,
    ConsumerGatewayPort,
    ConsumerOrderSignal,
    ConsumerSession,
    IntentReceipt,
)
from xuanling_tradecore.domain import (
    AccountLifecycle,
    AdmissionGate,
    AdmissionGateKind,
    AttributionBinding,
    BrokerCapabilities,
    BrokerEvent,
    BrokerRecoveryEvidence,
    CallbackGapEvidence,
    CancelRequest,
    ConsumerAccountBinding,
    ConsumerAccountRiskEvent,
    ConsumerApiVersion,
    ConsumerAuditTrail,
    ConsumerEventCursor,
    ConsumerEventKind,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
    ConsumerTrace,
    DomainValidationError,
    ErrorCategory,
    ExecutionInstruction,
    OrderOrigin,
    ReconciliationStatus,
    RecoveryPlan,
    TradingDayEpoch,
    VenueOrderIdentity,
)

ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = ROOT / "xuanling_tradecore"


def _trace(*, consumer_id: str = "consumer-1", account_id: str = "account-1") -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id=consumer_id,
        consumer_account_id="consumer-account-1",
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id="strategy-order-1",
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _capabilities() -> BrokerCapabilities:
    return BrokerCapabilities(
        capability_profile_id="fake-capability-v1",
        version=1,
        venue_id="FAKE",
        supports_cash=True,
        supports_margin_buy=False,
        supports_short_sell=False,
        supports_pagination=True,
        supports_public_stream_replay=True,
        echoes_client_order_ref=True,
        has_stable_venue_order_id=True,
        has_stable_trade_id=True,
        supports_complete_recovery_queries=True,
        supports_negative_confirmation=True,
        supports_paper_matching=True,
    )


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-order-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-08-31"),
        consumer_trace=_trace(),
        symbol="000010",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=163,
        limit_price_ticks=162,
    )


class _FakeSubscription:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeConsumerGateway:
    def __init__(self) -> None:
        self._session = ConsumerSession(
            session_id="session-1",
            consumer_id="consumer-1",
            api_version=ConsumerApiVersion(1, 0),
            binding_epoch=1,
        )
        self.last_signal: ConsumerOrderSignal | None = None

    def open_session(
        self,
        principal: ConsumerPrincipal,
        requested_api_version: ConsumerApiVersion,
    ) -> ConsumerSession:
        if principal.consumer_id != self._session.consumer_id:
            raise RuntimeError("unauthorized")
        if not self._session.api_version.is_compatible_with(requested_api_version):
            raise RuntimeError("incompatible")
        return self._session

    def list_accounts(self, session: ConsumerSession) -> tuple[ConsumerAccountView, ...]:
        if session != self._session:
            raise RuntimeError("unauthorized")
        return (
            ConsumerAccountView(
                consumer_account_id="consumer-account-1",
                account_display_name="仿真普通账户",
                venue_id="FAKE",
                capability_profile_id="fake-capability-v1",
                lifecycle=AccountLifecycle.DISARMED,
                reconciliation_status=ReconciliationStatus.REQUIRED,
                gates=(
                    AdmissionGate(
                        gate_id="reconciliation-1",
                        kind=AdmissionGateKind.RECONCILIATION,
                        version=1,
                        source_reference="startup",
                        first_observed_monotonic_ns=1,
                        last_observed_monotonic_ns=1,
                    ),
                ),
            ),
        )

    def submit_signal(self, session: ConsumerSession, signal: ConsumerOrderSignal) -> IntentReceipt:
        if session != self._session or signal.strategy_id != "strategy-1":
            raise RuntimeError("unauthorized")
        self.last_signal = signal
        return IntentReceipt(
            consumer_request_id=signal.consumer_request_id,
            accepted_event_id="event-1",
            oms_order_id="oms-order-1",
            execution_id="execution-1",
            journal_id="journal-1",
            journal_lsn=10,
            consumer_trace=_trace(),
        )

    def cancel_order(self, session: ConsumerSession, request: CancelRequest) -> IntentReceipt:
        if session != self._session:
            raise RuntimeError("unauthorized")
        return IntentReceipt(
            consumer_request_id=request.cancel_request_id,
            accepted_event_id="event-cancel-1",
            oms_order_id=request.oms_order_id,
            execution_id=request.execution_id,
            journal_id="journal-1",
            journal_lsn=11,
            consumer_trace=request.consumer_trace,
        )

    def get_order(self, session: ConsumerSession, oms_order_id: str) -> ConsumerAccountRiskEvent:
        if session != self._session or oms_order_id != "oms-order-1":
            raise RuntimeError("not found")
        return _risk_event()

    def get_order_audit_trail(
        self,
        session: ConsumerSession,
        oms_order_id: str,
    ) -> tuple[ConsumerAuditTrail, ...]:
        if session != self._session or oms_order_id != "oms-order-1":
            raise RuntimeError("not found")
        return (
            ConsumerAuditTrail(
                audit_id="audit-1",
                consumer_id="consumer-1",
                account_id="account-1",
                oms_order_id="oms-order-1",
                execution_id="execution-1",
                journal_id="journal-1",
                journal_lsn=10,
                action="CALL_STARTED",
            ),
        )

    def get_account_order_snapshot(
        self,
        session: ConsumerSession,
        consumer_account_id: str,
    ) -> ConsumerAccountOrderSnapshot:
        if session != self._session or consumer_account_id != "consumer-account-1":
            raise RuntimeError("unauthorized")
        cursor = _cursor()
        return ConsumerAccountOrderSnapshot(
            consumer_account_id=consumer_account_id,
            account_id="account-1",
            snapshot_lsn=cursor.journal_lsn,
            next_cursor=cursor,
            orders=(_risk_event(),),
        )

    def poll_events(
        self,
        session: ConsumerSession,
        cursor: ConsumerEventCursor,
        *,
        limit: int,
    ) -> ConsumerEventPage:
        if session != self._session or limit <= 0:
            raise RuntimeError("unauthorized")
        return ConsumerEventPage(
            events=(_risk_event(),),
            high_watermark_lsn=11,
            has_more=False,
            next_cursor=cursor,
        )

    def subscribe_order_events(
        self,
        session: ConsumerSession,
        cursor: ConsumerEventCursor,
    ) -> _FakeSubscription:
        if session != self._session:
            raise RuntimeError("unauthorized")
        return _FakeSubscription()


class _FakeObserver:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


class _FakeBrokerAdapter:
    adapter_id = "fake-adapter"
    adapter_instance_id = "fake-v1"
    capabilities = _capabilities()

    def __init__(self) -> None:
        self.observer: _FakeObserver | None = None
        self.submit_calls: list[ExecutionInstruction] = []
        self.cancel_calls: list[CancelExecutionInstruction] = []

    def start(self, observer: BrokerEventObserver) -> None:
        self.observer = cast(_FakeObserver, observer)

    def stop(self) -> None:
        return None

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        self.submit_calls.append(instruction)
        return SubmitAccepted(
            execution_id=instruction.execution_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=100,
        )

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        self.cancel_calls.append(instruction)
        return CancelAccepted(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=101,
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        return QueryHandle(
            request_id=request.request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
        )

    def begin_recovery(
        self,
        plan: RecoveryPlan,
        observer: BrokerEventObserver,
    ) -> RecoveryHandle:
        self.start(observer)
        return RecoveryHandle(
            recovery_run_id=plan.recovery_run_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
        )

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        return BrokerRecoveryEvidence(
            evidence_id="evidence-1",
            recovery_run_id=handle.recovery_run_id,
            account_id="account-1",
            venue_id="FAKE",
            adapter_instance_id=self.adapter_instance_id,
            trading_day=TradingDayEpoch("2026-08-31"),
            callback_replay_complete=True,
            orders_complete=True,
            trades_complete=True,
            working_orders_complete=True,
            risk_snapshot_complete=True,
            negative_confirmation_complete=True,
        )


class _FakeLegacyOms:
    def __init__(self) -> None:
        self.instructions: list[LegacyCanonicalInstruction] = []

    def accept_legacy_instruction(self, instruction: LegacyCanonicalInstruction) -> IntentReceipt:
        self.instructions.append(instruction)
        return IntentReceipt(
            consumer_request_id="legacy-request-1",
            accepted_event_id="event-1",
            oms_order_id="m0-legacy-order",
            execution_id="m0-legacy-execution",
            journal_id="journal-placeholder",
            journal_lsn=0,
            consumer_trace=_trace(),
        )


def _cursor() -> ConsumerEventCursor:
    return ConsumerEventCursor(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="account-1",
        journal_id="journal-1",
        stream_generation=1,
        journal_lsn=10,
        schema_version=1,
    )


def _risk_event(
    *, consumer_id: str = "consumer-1", journal_lsn: int = 10
) -> ConsumerAccountRiskEvent:
    return ConsumerAccountRiskEvent(
        consumer_event_id=f"event-{consumer_id}-{journal_lsn}",
        event_kind=ConsumerEventKind.ACCOUNT_RISK,
        schema_version=1,
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-08-31"),
        journal_id="journal-1",
        journal_lsn=journal_lsn,
        consumer_trace=_trace(consumer_id=consumer_id),
        risk_snapshot_id=None,
        error_category=ErrorCategory.RISK,
    )


class TradeCorePortTests(unittest.TestCase):
    def test_consumer_port_contract_has_no_consumer_id_in_strategy_signal(self) -> None:
        gateway = _FakeConsumerGateway()
        self.assertIsInstance(gateway, ConsumerGatewayPort)
        session = gateway.open_session(
            ConsumerPrincipal("consumer-1", "login-1", "张三"),
            ConsumerApiVersion(1, 0),
        )
        signal = ConsumerOrderSignal(
            consumer_request_id="request-1",
            consumer_account_id="consumer-account-1",
            strategy_id="strategy-1",
            strategy_order_id="strategy-order-1",
            trading_day=TradingDayEpoch("2026-08-31"),
            symbol="000010",
            market="SZ",
            side="BUY",
            price_type="LIMIT",
            business_type="CASH",
            quantity=100,
            protection_price_ticks=163,
        )
        receipt = gateway.submit_signal(session, signal)
        self.assertEqual("consumer-1", receipt.consumer_trace.consumer_id)
        self.assertEqual("仿真普通账户", gateway.list_accounts(session)[0].account_display_name)
        with self.assertRaises(TypeError):
            ConsumerOrderSignal(  # type: ignore[reportCallIssue] # Negative test proves payload identity cannot be injected.
                consumer_id="consumer-2", **signal.__dict__
            )

    def test_event_pages_are_account_and_consumer_scoped(self) -> None:
        cursor = _cursor()
        page = ConsumerEventPage(
            events=(_risk_event(journal_lsn=9), _risk_event(journal_lsn=10)),
            high_watermark_lsn=10,
            has_more=False,
            next_cursor=cursor,
        )
        self.assertEqual((9, 10), tuple(event.journal_lsn for event in page.events))
        with self.assertRaises(DomainValidationError):
            ConsumerEventPage(
                events=(_risk_event(consumer_id="consumer-2"),),
                high_watermark_lsn=10,
                has_more=False,
                next_cursor=cursor,
            )

    def test_adapter_contract_is_vendor_neutral_and_keeps_outcomes_in_observer(self) -> None:
        adapter = _FakeBrokerAdapter()
        observer = _FakeObserver()
        self.assertIsInstance(adapter, BrokerAdapter)
        self.assertIsInstance(observer, BrokerEventObserver)
        adapter.start(observer)
        result = adapter.submit(_instruction())
        self.assertEqual("execution-1", result.execution_id)
        self.assertEqual(1, len(adapter.submit_calls))
        self.assertEqual([], observer.events)

        identity = VenueOrderIdentity("FAKE", "fake-v1", "client-1", "7", "1001")
        cancel = CancelExecutionInstruction(
            cancel_request=CancelRequest(
                cancel_request_id="cancel-1",
                oms_order_id="oms-order-1",
                execution_id="execution-1",
                account_id="account-1",
                consumer_trace=_trace(),
                reason="strategy-stop",
                requested_monotonic_ns=101,
            ),
            venue_order_identity=identity,
        )
        self.assertEqual("cancel-1", adapter.cancel(cancel).cancel_request_id)
        self.assertEqual(
            "query-1",
            adapter.query(
                BrokerQuery("query-1", "account-1", "FAKE", "fake-v1", "orders", 50)
            ).request_id,
        )

    def test_legacy_bridge_preserves_binding_and_excludes_legacy_client_key_from_business_hash(
        self,
    ) -> None:
        account_binding = ConsumerAccountBinding(
            binding_id="account-binding-1",
            binding_version=1,
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            account_display_name="仿真普通账户",
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
        )
        strategy_binding = ConsumerStrategyBinding(
            binding_id="strategy-binding-1",
            binding_version=1,
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            consumer_account_binding_id="account-binding-1",
            strategy_id="strategy-1",
        )
        context = LegacyInvocationContext(
            account_binding=account_binding,
            strategy_binding=strategy_binding,
            attribution_binding=AttributionBinding(
                attribution_binding_id="attribution-1",
                consumer_account_binding_id="account-binding-1",
                consumer_strategy_binding_id="strategy-binding-1",
                consumer_id="consumer-1",
                consumer_account_id="consumer-account-1",
                strategy_id="strategy-1",
                binding_version=1,
            ),
            trading_day=TradingDayEpoch("2026-08-31"),
        )
        oms = _FakeLegacyOms()
        bridge = LegacyOrderGatewayBridge(
            legacy_gateway=cast(OrderGateway, object()),
            context=context,
            oms_port=oms,
        )
        first = bridge.canonicalize(_legacy_request("legacy-id-1"))
        retry = bridge.canonicalize(_legacy_request("legacy-id-2"))
        changed = bridge.canonicalize(replace(_legacy_request("legacy-id-3"), quantity=200))
        self.assertEqual(first.canonical_intent_hash, retry.canonical_intent_hash)
        self.assertEqual(first.instruction.oms_order_id, retry.instruction.oms_order_id)
        self.assertNotEqual(first.canonical_intent_hash, changed.canonical_intent_hash)
        self.assertEqual("consumer-1", first.instruction.consumer_trace.consumer_id)
        self.assertEqual("strategy-1", first.instruction.consumer_trace.strategy_id)
        self.assertEqual("legacy-id-1", first.legacy_client_order_id)
        self.assertFalse(hasattr(first.instruction, "legacy_client_order_id"))
        bridge.submit(_legacy_request("legacy-id-1"))
        self.assertEqual(1, len(oms.instructions))
        with self.assertRaises(DomainValidationError):
            LegacyInvocationContext(
                account_binding=account_binding,
                strategy_binding=replace(strategy_binding, strategy_id="strategy-2"),
                attribution_binding=context.attribution_binding,
                trading_day=TradingDayEpoch("2026-08-31"),
            )

    def test_new_consumer_and_adapter_layers_never_import_vendor_or_legacy_packages(self) -> None:
        # This boundary prevents Consumer and adapter ports from importing a
        # broker SDK, a legacy gateway, or a database driver directly.  The
        # stdlib ``sqlite3`` module is intentionally not included: QMT Edge's
        # deployment-only durable inbox/outbox is a reviewed SQLite WAL store,
        # not a broker/vendor dependency.  Its production-version gate lives
        # in ``adapters.qmt.edge_runtime``.
        forbidden_roots = {"emt_gateway", "vnemttrader", "qmt", "psycopg"}
        for relative_root in ("consumer", "adapters"):
            for source in (CORE_ROOT / relative_root).rglob("*.py"):
                tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imported_names = tuple(alias.name for alias in node.names)
                    elif isinstance(node, ast.ImportFrom) and node.module is not None:
                        imported_names = (node.module,)
                    else:
                        continue
                    for imported_name in imported_names:
                        self.assertNotIn(
                            imported_name.split(".", maxsplit=1)[0],
                            forbidden_roots,
                            f"{source.relative_to(ROOT)} imports a forbidden dependency",
                        )


def _legacy_request(legacy_client_order_id: str) -> LegacyOrderRequest:
    return LegacyOrderRequest(
        legacy_client_order_id=legacy_client_order_id,
        strategy_order_id="strategy-order-1",
        symbol="000010",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=163,
        limit_price_ticks=162,
    )


if __name__ == "__main__":
    unittest.main()
