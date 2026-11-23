"""M1-01 contract tests for the vendor-neutral OMS order-book core."""

from __future__ import annotations

import ast
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from xuanling_tradecore.domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerStrategyBinding,
    ConsumerTrace,
    DecisionContext,
    OmsOrderState,
    OrderOrigin,
    RiskSnapshot,
    TradingDayEpoch,
)
from xuanling_tradecore.oms import (
    InMemoryOmsFactJournal,
    IntentAdmissionOutcome,
    OmsFactKind,
    OmsOrderBookError,
    OmsOrderIntent,
    OrderBook,
    OrderBookStateError,
    Reservation,
    ReservationBook,
    ReservationRequirement,
    ReservationRiskError,
    ReservationStatus,
    intent_hash_for,
)

ROOT = Path(__file__).resolve().parents[1]
SHA256 = "a" * 64


class _SequentialIds:
    def __init__(self) -> None:
        self._value = 0

    def __call__(self) -> str:
        self._value += 1
        return f"test-id-{self._value}"


class _FailingJournal:
    def append_batch(self, facts: object) -> object:
        del facts
        raise OSError("injected Journal failure")


def _bindings(
    *, active: bool = True
) -> tuple[
    ConsumerAccountBinding,
    ConsumerStrategyBinding,
    AttributionBinding,
]:
    account = ConsumerAccountBinding(
        binding_id="account-binding-1",
        binding_version=7,
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="broker-account-1",
        account_display_name="模拟账户一",
        venue_id="PAPER",
        adapter_instance_id="paper-1",
        active=active,
    )
    strategy = ConsumerStrategyBinding(
        binding_id="strategy-binding-1",
        binding_version=7,
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        consumer_account_binding_id=account.binding_id,
        strategy_id="strategy-1",
        active=active,
    )
    attribution = AttributionBinding(
        attribution_binding_id="attribution-1",
        consumer_account_binding_id=account.binding_id,
        consumer_strategy_binding_id=strategy.binding_id,
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        strategy_id="strategy-1",
        binding_version=7,
    )
    return account, strategy, attribution


def _intent(
    *,
    strategy_order_id: str = "strategy-order-1",
    quantity: int = 100,
    execution_parameters: dict[str, str] | None = None,
) -> OmsOrderIntent:
    return OmsOrderIntent(
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="broker-account-1",
            strategy_id="strategy-1",
            strategy_order_id=strategy_order_id,
            attribution_binding_id="attribution-1",
            binding_version=7,
            order_origin=OrderOrigin.STRATEGY,
        ),
        trading_day=TradingDayEpoch("2026-08-31"),
        symbol="SZ.000010",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=quantity,
        protection_price_ticks=163,
        limit_price_ticks=161,
        execution_parameters=execution_parameters or {"algo": "DIRECT"},
    )


def _risk_snapshot(*, expires_monotonic_ns: int = 1_000, cash_minor: int = 10_000) -> RiskSnapshot:
    return RiskSnapshot(
        risk_snapshot_id="risk-1",
        account_id="broker-account-1",
        trading_day=TradingDayEpoch("2026-08-31"),
        rule_bundle_hash=SHA256,
        captured_monotonic_ns=10,
        expires_monotonic_ns=expires_monotonic_ns,
        available_cash_minor=cash_minor,
        available_margin_minor=5_000,
        sellable_quantities={"SZ.000010": 10_000},
        borrowable_quantities={"SZ.000010": 5_000},
    )


def _decision_context(*, decided_monotonic_ns: int = 11) -> DecisionContext:
    return DecisionContext(
        decision_context_id="decision-1",
        account_id="broker-account-1",
        trading_day=TradingDayEpoch("2026-08-31"),
        risk_snapshot_id="risk-1",
        risk_rule_bundle_hash=SHA256,
        protection_price_ticks=163,
        fee_model_version="fee-v1",
        margin_model_version="margin-v1",
        reservation_model_version="reservation-v1",
        decided_monotonic_ns=decided_monotonic_ns,
    )


def _requirement(*, cash_minor: int = 1_000) -> ReservationRequirement:
    return ReservationRequirement(symbol="SZ.000010", cash_minor=cash_minor)


class TradeCoreOmsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = InMemoryOmsFactJournal("oms-journal-1")
        self.book = OrderBook(journal=self.journal, id_factory=_SequentialIds())
        self.account, self.strategy, self.attribution = _bindings()

    def _admit(
        self,
        *,
        intent: OmsOrderIntent | None = None,
        risk_snapshot: RiskSnapshot | None = None,
        requirement: ReservationRequirement | None = None,
    ):
        return self.book.admit(
            intent=intent or _intent(),
            account_binding=self.account,
            strategy_binding=self.strategy,
            attribution_binding=self.attribution,
            risk_snapshot=risk_snapshot or _risk_snapshot(),
            decision_context=_decision_context(),
            reservation_requirement=requirement or _requirement(),
            now_monotonic_ns=100,
        )

    def test_equal_intent_replays_without_second_reservation_or_journal_batch(self) -> None:
        accepted = self._admit()
        replayed = self._admit(
            intent=_intent(execution_parameters={"algo": "DIRECT", "route": "default"})
        )
        # A changed execution parameter is a business change and therefore conflicts.
        self.assertEqual(IntentAdmissionOutcome.ACCEPTED, accepted.outcome)
        self.assertEqual(IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT, replayed.outcome)
        exact_replay = self._admit()
        self.assertEqual(IntentAdmissionOutcome.IDEMPOTENT, exact_replay.outcome)
        self.assertEqual(accepted.order, exact_replay.order)
        self.assertEqual(accepted.order.reservation, exact_replay.order.reservation)
        self.assertEqual(3, len(self.journal.records))
        self.assertEqual(1, len(self.book.reservations("broker-account-1")))

    def test_canonical_hash_is_stable_for_mapping_order_and_excludes_transport_data(self) -> None:
        first = _intent(execution_parameters={"algo": "DIRECT", "route": "default"})
        second = _intent(execution_parameters={"route": "default", "algo": "DIRECT"})
        self.assertEqual(intent_hash_for(first), intent_hash_for(second))
        self.assertRegex(intent_hash_for(first), r"[0-9a-f]{64}\Z")

    def test_changed_business_intent_records_conflict_without_mutating_original_order(self) -> None:
        accepted = self._admit()
        conflict = self._admit(intent=_intent(quantity=101))
        self.assertEqual(IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT, conflict.outcome)
        self.assertEqual(accepted.order, conflict.order)
        self.assertEqual(1, len(self.book.reservations("broker-account-1")))
        self.assertEqual(
            [
                OmsFactKind.ORDER_ATTRIBUTED,
                OmsFactKind.RESERVATION_CREATED,
                OmsFactKind.IDEMPOTENCY_CONFLICT,
            ],
            [record.kind for record in self.journal.records],
        )

    def test_binding_staleness_risk_failure_and_journal_failure_have_no_order_or_reservation_side_effect(
        self,
    ) -> None:
        inactive_account, inactive_strategy, inactive_attribution = _bindings(active=False)
        with self.assertRaisesRegex(OmsOrderBookError, "inactive"):
            self.book.admit(
                intent=_intent(),
                account_binding=inactive_account,
                strategy_binding=inactive_strategy,
                attribution_binding=inactive_attribution,
                risk_snapshot=_risk_snapshot(),
                decision_context=_decision_context(),
                reservation_requirement=_requirement(),
                now_monotonic_ns=100,
            )
        with self.assertRaisesRegex(ReservationRiskError, "stale"):
            self._admit(risk_snapshot=_risk_snapshot(expires_monotonic_ns=100))
        self.assertEqual((), self.journal.records)
        self.assertEqual((), self.book.reservations("broker-account-1"))

        failing_book = OrderBook(journal=_FailingJournal(), id_factory=_SequentialIds())
        with self.assertRaises(OSError):
            failing_book.admit(
                intent=_intent(),
                account_binding=self.account,
                strategy_binding=self.strategy,
                attribution_binding=self.attribution,
                risk_snapshot=_risk_snapshot(),
                decision_context=_decision_context(),
                reservation_requirement=_requirement(),
                now_monotonic_ns=100,
            )
        self.assertEqual((), failing_book.reservations("broker-account-1"))
        self.assertEqual((), failing_book.events("broker-account-1"))

    def test_account_serialization_prevents_check_then_reserve_race(self) -> None:
        book = OrderBook(journal=InMemoryOmsFactJournal(), id_factory=_SequentialIds())
        start = threading.Barrier(2)
        outcomes: list[str] = []

        def submit(strategy_order_id: str) -> None:
            start.wait()
            try:
                book.admit(
                    intent=_intent(strategy_order_id=strategy_order_id),
                    account_binding=self.account,
                    strategy_binding=self.strategy,
                    attribution_binding=self.attribution,
                    risk_snapshot=_risk_snapshot(cash_minor=1_000),
                    decision_context=_decision_context(),
                    reservation_requirement=_requirement(cash_minor=600),
                    now_monotonic_ns=100,
                )
                outcomes.append("accepted")
            except ReservationRiskError:
                outcomes.append("risk")

        first = threading.Thread(target=submit, args=("strategy-order-1",))
        second = threading.Thread(target=submit, args=("strategy-order-2",))
        first.start()
        second.start()
        first.join()
        second.join()
        self.assertCountEqual(["accepted", "risk"], outcomes)
        self.assertEqual(1, len(book.reservations("broker-account-1")))

    def test_reservation_totals_follow_committed_create_adjust_and_release(self) -> None:
        book = ReservationBook()
        risk = RiskSnapshot(
            risk_snapshot_id="risk-totals",
            account_id="broker-account-1",
            trading_day=TradingDayEpoch("2026-08-31"),
            rule_bundle_hash=SHA256,
            captured_monotonic_ns=10,
            expires_monotonic_ns=1_000,
            available_cash_minor=100,
            available_margin_minor=80,
            sellable_quantities={"SZ.000010": 9, "SH.600000": 7},
            borrowable_quantities={"SZ.000010": 5, "SH.600000": 4},
        )

        def reservation(order_id: str, requirement: ReservationRequirement) -> Reservation:
            return Reservation(
                reservation_id=f"reservation-{order_id}",
                oms_order_id=order_id,
                account_id="broker-account-1",
                trading_day=TradingDayEpoch("2026-08-31"),
                risk_snapshot_id="risk-totals",
                decision_context_id="decision-totals",
                original_requirement=requirement,
                active_requirement=requirement,
                status=ReservationStatus.ACTIVE,
                reservation_revision=0,
                created_monotonic_ns=100,
            )

        first = reservation(
            "order-1",
            ReservationRequirement("SZ.000010", 40, 20, 5, 2),
        )
        second = reservation(
            "order-2",
            ReservationRequirement("SH.600000", 30, 10, 4, 1),
        )
        book.create(first)
        book.create(second)
        book.create(first)  # An idempotent replay must not double-count.
        with self.assertRaisesRegex(ReservationRiskError, "cash"):
            book.ensure_available(ReservationRequirement("SZ.000010", cash_minor=31), risk)
        with self.assertRaisesRegex(ReservationRiskError, "sellable"):
            book.ensure_available(ReservationRequirement("SZ.000010", sellable_quantity=5), risk)
        with self.assertRaisesRegex(ReservationRiskError, "borrowable"):
            book.ensure_available(ReservationRequirement("SH.600000", borrowable_quantity=4), risk)

        reduced = replace(
            first,
            active_requirement=ReservationRequirement("SZ.000010", 20, 10, 2, 1),
            reservation_revision=1,
        )
        book.replace_committed(reduced)
        book.ensure_available(ReservationRequirement("SZ.000010", 50, 50, 7, 4), risk)
        with self.assertRaisesRegex(ReservationRiskError, "cash"):
            book.ensure_available(ReservationRequirement("SH.600000", cash_minor=51), risk)
        released = replace(
            second,
            active_requirement=ReservationRequirement.zero_for("SH.600000"),
            status=ReservationStatus.RELEASED,
            reservation_revision=1,
        )
        book.replace_committed(released)
        book.ensure_available(ReservationRequirement("SH.600000", 80, 70, 7, 4), risk)
        with self.assertRaisesRegex(OmsOrderBookError, "immutable identity"):
            book.replace_committed(replace(reduced, reservation_id="wrong", reservation_revision=2))
        book.ensure_available(ReservationRequirement("SZ.000010", cash_minor=80), risk)

    def test_state_machine_trade_dedup_and_reservation_adjustment_are_journal_first(self) -> None:
        accepted = self._admit(requirement=_requirement(cash_minor=1_000))
        order_id = accepted.order.order.oms_order_id
        working = self.book.mark_working(oms_order_id=order_id)
        self.assertIsNotNone(working)
        trade = self.book.apply_trade(oms_order_id=order_id, trade_key="SZ:trade-1", quantity=25)
        self.assertIsNotNone(trade)
        order = self.book.order(order_id)
        self.assertEqual(OmsOrderState.PARTIALLY_FILLED, order.order.state)
        self.assertEqual(25, order.filled_quantity)
        self.assertEqual(750, order.reservation.active_requirement.cash_minor)
        self.assertIsNone(
            self.book.apply_trade(oms_order_id=order_id, trade_key="SZ:trade-1", quantity=25)
        )
        canceled = self.book.cancel(oms_order_id=order_id)
        self.assertIsNotNone(canceled)
        terminal = self.book.order(order_id)
        self.assertEqual(OmsOrderState.CANCELED, terminal.order.state)
        self.assertEqual(ReservationStatus.RELEASED, terminal.reservation.status)
        self.assertTrue(terminal.reservation.active_requirement.is_empty)
        with self.assertRaises(OrderBookStateError):
            self.book.apply_trade(oms_order_id=order_id, trade_key="SZ:trade-2", quantity=1)
        self.assertEqual(
            [
                OmsFactKind.ORDER_ATTRIBUTED,
                OmsFactKind.RESERVATION_CREATED,
                OmsFactKind.ORDER_STATE_CHANGED,
                OmsFactKind.BROKER_TRADE_APPLIED,
                OmsFactKind.RESERVATION_ADJUSTED,
                OmsFactKind.ORDER_STATE_CHANGED,
                OmsFactKind.RESERVATION_RELEASED,
            ],
            [record.kind for record in self.journal.records],
        )

    def test_oms_core_does_not_import_vendor_persistence_or_legacy_gateway_layers(self) -> None:
        forbidden = {"emt_gateway", "vnemttrader", "psycopg", "sqlite3", "redis"}
        for source in (ROOT / "xuanling_tradecore" / "oms").rglob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    names = (node.module,)
                else:
                    continue
                self.assertFalse(
                    {name.split(".", 1)[0] for name in names} & forbidden,
                    f"{source.relative_to(ROOT)} imports a forbidden runtime layer",
                )


if __name__ == "__main__":
    unittest.main()
