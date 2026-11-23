"""Shared complete OMS recovery seed fixture for route persistence tests."""

from __future__ import annotations

from hashlib import sha256

from xuanling_tradecore.domain import (
    ConsumerTrace,
    DecisionContext,
    ExecutionInstruction,
    OmsOrder,
    OmsOrderState,
    OrderOrigin,
    TradingDayEpoch,
)
from xuanling_tradecore.oms import (
    OmsRecoverySeedV1,
    OrderBookOrder,
    Reservation,
    ReservationRequirement,
    ReservationStatus,
)


def recovery_seed(
    *,
    account_id: str,
    oms_order_id: str,
    execution_id: str,
    decision_context_id: str,
    intent_hash: str,
) -> OmsRecoverySeedV1:
    day = TradingDayEpoch("2026-09-02")
    trace = ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id=f"consumer-{account_id}",
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id=f"strategy-order-{account_id}",
        attribution_binding_id=f"attribution-{account_id}",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )
    requirement = ReservationRequirement(symbol="SZ.000001", cash_minor=100_000, currency="CNY")
    risk_snapshot_id = f"risk-{account_id}"
    context = DecisionContext(
        decision_context_id=decision_context_id,
        account_id=account_id,
        trading_day=day,
        risk_snapshot_id=risk_snapshot_id,
        risk_rule_bundle_hash=sha256(f"risk-rules:{account_id}".encode()).hexdigest(),
        protection_price_ticks=1_000,
        fee_model_version="fee-v1",
        margin_model_version="margin-v1",
        reservation_model_version="reservation-v1",
        decided_monotonic_ns=100,
    )
    return OmsRecoverySeedV1.build(
        OrderBookOrder(
            order=OmsOrder(
                oms_order_id=oms_order_id,
                account_id=account_id,
                trading_day=day,
                consumer_trace=trace,
                intent_hash=intent_hash,
                order_origin=OrderOrigin.STRATEGY,
                state=OmsOrderState.ACCEPTED,
                created_monotonic_ns=100,
            ),
            execution=ExecutionInstruction(
                execution_id=execution_id,
                oms_order_id=oms_order_id,
                account_id=account_id,
                trading_day=day,
                consumer_trace=trace,
                symbol="SZ.000001",
                market="SZ",
                side="BUY",
                price_type="LIMIT",
                business_type="CASH",
                quantity=100,
                protection_price_ticks=1_000,
                limit_price_ticks=999,
                execution_parameters={"algo": "DIRECT"},
            ),
            decision_context=context,
            reservation=Reservation(
                reservation_id=f"reservation-{account_id}",
                oms_order_id=oms_order_id,
                account_id=account_id,
                trading_day=day,
                risk_snapshot_id=risk_snapshot_id,
                decision_context_id=decision_context_id,
                original_requirement=requirement,
                active_requirement=requirement,
                status=ReservationStatus.ACTIVE,
                reservation_revision=0,
                created_monotonic_ns=100,
            ),
        )
    )
