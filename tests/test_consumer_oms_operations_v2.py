from __future__ import annotations

import unittest
from dataclasses import replace

from xuanling_tradecore.consumer import (
    ConsumerGatewayError,
    ConsumerOmsAdmissionContextV2,
    ConsumerOrderSignal,
    ConsumerSession,
    OrderBookConsumerOperationsV2,
)
from xuanling_tradecore.consumer.request_trace_v2 import request_trace_scope
from xuanling_tradecore.domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerApiVersion,
    ConsumerStrategyBinding,
    DecisionContext,
    OmsOrderState,
    RiskSnapshot,
    TradingDayEpoch,
)
from xuanling_tradecore.oms import InMemoryOmsFactJournal, OrderBook, ReservationRequirement

_SHA = "a" * 64


class _Ids:
    def __init__(self) -> None:
        self._next = 0

    def __call__(self) -> str:
        self._next += 1
        return f"id-{self._next}"


class ConsumerOmsOperationsV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.account = ConsumerAccountBinding(
            "account-bind", 1, "consumer-1", "ca-1", "account-1", "sim", "PAPER", "paper-1"
        )
        self.strategy = ConsumerStrategyBinding(
            "strategy-bind", 1, "consumer-1", "ca-1", "account-bind", "strategy-1"
        )
        self.attribution = AttributionBinding(
            "attribution-1", "account-bind", "strategy-bind", "consumer-1", "ca-1", "strategy-1", 1
        )
        self.operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=InMemoryOmsFactJournal("journal-1"), id_factory=_Ids()),
            admission_context_for=lambda *_: self._context(),
            now_monotonic_ns=lambda: 20,
        )
        self.session = ConsumerSession("session-1", "consumer-1", ConsumerApiVersion(2, 0), 1, 2)

    def _context(self) -> ConsumerOmsAdmissionContextV2:
        day = TradingDayEpoch("2026-09-03")
        risk = RiskSnapshot("risk-1", "account-1", day, _SHA, 1, 100, 10_000, 0)
        decision = DecisionContext(
            "decision-1", "account-1", day, "risk-1", _SHA, 10, "fee", "margin", "reserve", 2
        )
        return ConsumerOmsAdmissionContextV2(
            self.strategy,
            self.attribution,
            risk,
            decision,
            ReservationRequirement("000001", cash_minor=100),
        )

    @staticmethod
    def _signal(request_id: str = "request-1") -> ConsumerOrderSignal:
        return ConsumerOrderSignal(
            request_id,
            "ca-1",
            "strategy-1",
            "strategy-order-1",
            TradingDayEpoch("2026-09-03"),
            "000001",
            "SZ",
            "BUY",
            "LIMIT",
            "CASH",
            10,
            10,
        )

    def test_submit_cancel_read_and_audit_are_derived_from_orderbook_journal_facts(self) -> None:
        receipt = self.operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=self._signal(),
        )
        self.assertEqual("account-1", receipt.consumer_trace.account_id)
        order = self.operations.get_order(
            account_binding=self.account, oms_order_id=receipt.oms_order_id
        )
        self.assertEqual(receipt.execution_id, order.execution_id)
        # EMS moves a successfully dispatched order to WORKING before its normal cancel lane.
        self.operations._order_book.mark_working(  # type: ignore[reportPrivateUsage]
            oms_order_id=receipt.oms_order_id
        )
        cancelled = self.operations.cancel_order(
            session=self.session,
            account_binding=self.account,
            consumer_request_id="cancel-1",
            oms_order_id=receipt.oms_order_id,
            reason="test",
        )
        self.assertNotEqual(receipt.accepted_event_id, cancelled.accepted_event_id)
        self.assertEqual(
            3,
            len(
                self.operations.get_order_audit_trail(
                    account_binding=self.account, oms_order_id=receipt.oms_order_id
                )
            ),
        )
        self.assertEqual(
            OmsOrderState.CANCEL_PENDING,
            self.operations._order_book.order(cancelled.oms_order_id).order.state,  # type: ignore[reportPrivateUsage]
        )

    def test_submit_trace_has_ordered_non_sensitive_admission_boundaries(self) -> None:
        record = {}
        with request_trace_scope(record):
            self.operations.submit_signal(
                session=self.session, account_binding=self.account,
                strategy_binding_id="strategy-bind", signal=self._signal(),
            )
        self.assertEqual(set(record), {
            "submit_context_started_ns", "submit_admission_started_ns",
            "submit_admission_finished_ns",
        })
        self.assertLessEqual(record["submit_context_started_ns"], record["submit_admission_started_ns"])
        self.assertLessEqual(record["submit_admission_started_ns"], record["submit_admission_finished_ns"])

    def test_local_feature_flag_rejects_before_context_or_oms_and_requires_bundle_evidence(
        self,
    ) -> None:
        calls: list[str] = []
        blocked = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=InMemoryOmsFactJournal("journal-2"), id_factory=_Ids()),
            admission_context_for=lambda *_: calls.append("context") or self._context(),
            feature_flag_enabled=lambda *_: False,
            now_monotonic_ns=lambda: 20,
        )
        with self.assertRaisesRegex(ConsumerGatewayError, "feature flag"):
            blocked.submit_signal(
                session=self.session,
                account_binding=self.account,
                strategy_binding_id="strategy-bind",
                signal=self._signal(),
            )
        self.assertEqual([], calls)
        context = self._context()
        bundled = replace(
            context,
            decision_context=replace(
                context.decision_context, decision_bundle_hash="b" * 64, decision_bundle_epoch=1
            ),
        )
        allowed = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=InMemoryOmsFactJournal("journal-3"), id_factory=_Ids()),
            admission_context_for=lambda *_: bundled,
            feature_flag_enabled=lambda account_id, flag: (
                account_id == "account-1" and flag == "cash"
            ),
            now_monotonic_ns=lambda: 20,
        )
        self.assertEqual(
            "request-1",
            allowed.submit_signal(
                session=self.session,
                account_binding=self.account,
                strategy_binding_id="strategy-bind",
                signal=self._signal(),
            ).consumer_request_id,
        )

    def test_committed_execution_wake_is_once_and_never_changes_journal_acceptance(self) -> None:
        notified: list[str] = []
        operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=InMemoryOmsFactJournal("journal-4"), id_factory=_Ids()),
            admission_context_for=lambda *_: self._context(),
            now_monotonic_ns=lambda: 20,
            on_execution_committed=lambda order: notified.append(order.execution.execution_id),
        )
        first = operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=self._signal(),
        )
        repeated = operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=self._signal("request-2"),
        )
        self.assertEqual(first.oms_order_id, repeated.oms_order_id)
        self.assertEqual([first.execution_id], notified)

    def test_cancel_remains_pending_until_callback_and_wakes_ems_once(self) -> None:
        execution_wakes: list[str] = []
        cancel_wakes: list[tuple[str, str]] = []
        operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=InMemoryOmsFactJournal("journal-5"), id_factory=_Ids()),
            admission_context_for=lambda *_: self._context(),
            now_monotonic_ns=lambda: 20,
            on_execution_committed=lambda order: execution_wakes.append(
                order.execution.execution_id
            ),
            cancel_admission_allowed=lambda _order: True,
            on_cancel_committed=lambda order, request_id, _reason: cancel_wakes.append(
                (order.execution.execution_id, request_id)
            ),
        )
        receipt = operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=self._signal(),
        )
        operations._order_book.mark_working(  # type: ignore[reportPrivateUsage]
            oms_order_id=receipt.oms_order_id
        )
        operations.cancel_order(
            session=self.session,
            account_binding=self.account,
            consumer_request_id="cancel-1",
            oms_order_id=receipt.oms_order_id,
            reason="strategy cleanup",
        )
        self.assertEqual([receipt.execution_id], execution_wakes)
        self.assertEqual([(receipt.execution_id, "cancel-1")], cancel_wakes)
        facts = operations._order_book.journal.records  # type: ignore[attr-defined,reportPrivateUsage]
        cancel_fact, transition_fact = facts[-2:]
        self.assertEqual("CANCEL_REQUESTED", cancel_fact.kind.value)
        self.assertEqual("cancel-1", cancel_fact.attributes["cancel_request_id"])
        self.assertEqual("strategy cleanup", cancel_fact.attributes["reason"])
        self.assertEqual(cancel_fact.attributes, transition_fact.attributes)
        self.assertEqual(
            OmsOrderState.CANCEL_PENDING,
            operations._order_book.order(receipt.oms_order_id).order.state,  # type: ignore[reportPrivateUsage]
        )

    def test_cancel_evidence_wakeup_failure_fences_after_committed_fact(self) -> None:
        evidence = []
        fences = []

        def wake(order, event, request_id, reason):
            evidence.append((order, event, request_id, reason))
            raise RuntimeError("worker unavailable")

        journal = InMemoryOmsFactJournal("journal-cancel-evidence")
        operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=journal, id_factory=_Ids()),
            admission_context_for=lambda *_: self._context(),
            now_monotonic_ns=lambda: 20,
            cancel_admission_allowed=lambda _order: True,
            on_cancel_commit_evidence=wake,
            on_cancel_wakeup_failed=lambda: fences.append("closed"),
        )
        receipt = operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=self._signal(),
        )
        result = operations.cancel_order(
            session=self.session,
            account_binding=self.account,
            consumer_request_id="cancel-evidence-1",
            oms_order_id=receipt.oms_order_id,
            reason="test cleanup",
        )
        self.assertEqual(["closed"], fences)
        self.assertEqual(1, len(evidence))
        self.assertEqual(evidence[0][1].journal_lsn, result.journal_lsn)
        self.assertEqual(journal.records[-2].attributes["cancel_request_id"], "cancel-evidence-1")
        self.assertEqual(
            operations._order_book.order(receipt.oms_order_id).order.state,  # type: ignore[reportPrivateUsage]
            OmsOrderState.CANCEL_PENDING,
        )

    def test_absent_event_observer_does_not_build_discarded_projection(self):
        from unittest.mock import patch

        with patch.object(
            self.operations, "_project_order", side_effect=AssertionError("unused projection")
        ):
            receipt = self.operations.submit_signal(
                session=self.session,
                account_binding=self.account,
                strategy_binding_id="strategy-bind",
                signal=self._signal(),
            )
            self.operations._order_book.mark_working(oms_order_id=receipt.oms_order_id)
            self.operations.cancel_order(
                session=self.session,
                account_binding=self.account,
                consumer_request_id="cancel",
                oms_order_id=receipt.oms_order_id,
                reason="test",
            )
        observed = []
        self.operations._on_order_event_committed = observed.append
        self.operations.submit_signal(
            session=self.session,
            account_binding=self.account,
            strategy_binding_id="strategy-bind",
            signal=replace(self._signal(), strategy_order_id="next"),
        )
        self.assertEqual(1, len(observed))


if __name__ == "__main__":
    unittest.main()
