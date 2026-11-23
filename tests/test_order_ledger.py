from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from emt_gateway.order_ledger import (
    AsyncPostgresOrderLedger,
    BookHealth,
    LedgerEventKind,
    OrderLedgerError,
    OrderState,
    PostgresOrderLedger,
)
from tests.postgres_support import HAS_TEST_POSTGRES, PostgresTestDatabase

_ACCOUNT = "ordinary-1"
_DAY = "20260827"
_ORDER = "gateway-order-1"


@unittest.skipUnless(
    HAS_TEST_POSTGRES, "set EMT_GATEWAY_TEST_POSTGRES_DSN to run PostgreSQL ledger tests"
)
class PostgresOrderLedgerTests(unittest.TestCase):
    def test_book_requires_reconciliation_before_new_intent(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                with self.assertRaisesRegex(OrderLedgerError, "UNINITIALIZED"):
                    self._record_intent(ledger)
                ledger.begin_reconciliation(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    event_id="reconcile-start",
                    reason="startup",
                )
                self.assertEqual(
                    BookHealth.RECONCILING,
                    ledger.book_status(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                    ).health,
                )
                ledger.complete_reconciliation(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    event_id="reconcile-done",
                )
                self._record_intent(ledger)
                self.assertEqual(OrderState.INTENT_DURABLE, self._order(ledger).state)

    def test_trade_deduplication_and_projection_rebuild_preserve_continuity(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                self._ready(ledger)
                self._record_intent(ledger)
                ledger.record_call_started(
                    event_id="call-started",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                )
                ledger.record_sdk_accepted(
                    event_id="sdk-accepted",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                )
                ledger.record_order_update(
                    event_id="order-live",
                    source_key="order:101:live",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    state=OrderState.LIVE,
                    leaves_qty=100,
                )
                first_trade = ledger.record_trade(
                    event_id="trade-1",
                    execution_key="SZ:execution-1",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    market="SZ",
                    qty=40,
                    price="1.6100",
                )
                duplicate = ledger.record_trade(
                    event_id="trade-1",
                    execution_key="SZ:execution-1",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    market="SZ",
                    qty=40,
                    price="1.6100",
                )
                self.assertEqual(first_trade.lsn, duplicate.lsn)
                self.assertEqual(
                    (40, 60, OrderState.PART_FILLED),
                    (
                        self._order(ledger).filled_qty,
                        self._order(ledger).leaves_qty,
                        self._order(ledger).state,
                    ),
                )
                cancellable = ledger.resolve_cancellable_order(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    client_order_id="client-order-1",
                )
                self.assertEqual(101, cancellable.order_emt_id)
                ledger.rebuild_projection()
                rebuilt = self._order(ledger)
                self.assertEqual(
                    (40, 60, OrderState.PART_FILLED),
                    (
                        rebuilt.filled_qty,
                        rebuilt.leaves_qty,
                        rebuilt.state,
                    ),
                )

            with PostgresOrderLedger(database.dsn, schema=database.schema) as reopened:
                recovered = reopened.order_by_emt_id(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    order_emt_id=101,
                )
                self.assertIsNotNone(recovered)
                self.assertEqual(OrderState.PART_FILLED, recovered.state)  # type: ignore[union-attr]  # recovery loop guarantees record
                self.assertEqual(40, recovered.filled_qty)  # type: ignore[union-attr]  # recovery loop guarantees record

    def test_callback_that_races_a_late_sdk_return_keeps_the_newer_exchange_state(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                self._ready(ledger)
                self._record_intent(ledger)
                ledger.record_call_started(
                    event_id="call-started",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                )
                ledger.record_order_update(
                    event_id="early-order-event",
                    source_key="order:101:live",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    state=OrderState.LIVE,
                    leaves_qty=100,
                )
                ledger.record_sdk_accepted(
                    event_id="late-sdk-return",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                )
                self.assertEqual(OrderState.LIVE, self._order(ledger).state)
                self.assertEqual(101, self._order(ledger).order_emt_id)

    def test_trade_after_canceled_callback_is_accounted_for_despite_cross_thread_delivery(
        self,
    ) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                self._ready(ledger)
                self._record_intent(ledger)
                ledger.record_sdk_accepted(
                    event_id="sdk-accepted",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                )
                ledger.record_order_update(
                    event_id="order-canceled",
                    source_key="order:101:canceled",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    state=OrderState.CANCELED,
                    leaves_qty=100,
                )
                ledger.record_trade(
                    event_id="late-trade",
                    execution_key="SZ:execution-late",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    market="SZ",
                    qty=10,
                    price="1.6100",
                )
                self.assertEqual(OrderState.CANCELED, self._order(ledger).state)
                self.assertEqual(10, self._order(ledger).filled_qty)
                self.assertEqual(90, self._order(ledger).leaves_qty)
                status = ledger.book_status(account_id=_ACCOUNT, trading_day=_DAY)
                self.assertEqual(BookHealth.READY, status.health)
                with self.assertRaisesRegex(OrderLedgerError, "CANCELED"):
                    ledger.resolve_cancellable_order(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        client_order_id="client-order-1",
                    )

    def test_event_id_cannot_be_reused_for_a_different_order_fact(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                self._ready(ledger)
                self._record_intent(ledger)
                ledger.record_sdk_accepted(
                    event_id="sdk-accepted",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                )
                ledger.record_trade(
                    event_id="trade-1",
                    execution_key="SZ:execution-1",
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    gateway_order_id=_ORDER,
                    order_emt_id=101,
                    market="SZ",
                    qty=10,
                    price="1.6100",
                )
                with self.assertRaisesRegex(OrderLedgerError, "conflicting"):
                    ledger.record_trade(
                        event_id="trade-1",
                        execution_key="SZ:execution-1",
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        gateway_order_id=_ORDER,
                        order_emt_id=101,
                        market="SZ",
                        qty=11,
                        price="1.6100",
                    )

    def test_event_hashes_form_a_per_account_trading_day_chain(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                start = ledger.begin_reconciliation(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    event_id="reconcile-start",
                    reason="startup",
                )
                done = ledger.complete_reconciliation(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    event_id="reconcile-done",
                )
                self.assertEqual(start.event_hash, done.previous_hash)
                self.assertEqual(64, len(done.event_hash))

    def test_postgres_row_lock_serializes_concurrent_scope_appends(self) -> None:
        with PostgresTestDatabase() as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:

                def append_fact(index: int) -> int:
                    with PostgresOrderLedger(
                        database.dsn,
                        schema=database.schema,
                        initialize_schema=False,
                    ) as writer:
                        event = writer.append(
                            kind=LedgerEventKind.RECONCILIATION_STARTED,
                            event_id=f"concurrent-{index}",
                            account_id=_ACCOUNT,
                            trading_day=_DAY,
                            gateway_order_id="__book__",
                            payload={"reason": f"concurrent-{index}"},
                        )
                        return event.lsn

                with ThreadPoolExecutor(max_workers=8) as executor:
                    lsns = list(executor.map(append_fact, range(32)))
                self.assertEqual(32, len(set(lsns)))
                events = ledger.events(account_id=_ACCOUNT, trading_day=_DAY)
                self.assertEqual(32, len(events))
                for prior, current in zip(events, events[1:]):
                    self.assertEqual(prior.event_hash, current.previous_hash)

    def test_async_projector_persists_facts_in_background(self) -> None:
        with PostgresTestDatabase() as database:
            projector = AsyncPostgresOrderLedger(database.dsn, schema=database.schema)
            try:
                self.assertTrue(
                    projector.begin_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="async-reconcile-start",
                        reason="startup",
                    )
                )
                self.assertTrue(
                    projector.complete_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="async-reconcile-done",
                    )
                )
                self.assertTrue(
                    projector.record_intent(
                        event_id="async-intent",
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        gateway_order_id=_ORDER,
                        strategy_id="strategy-a",
                        strategy_order_id="strategy-order-1",
                        client_order_id="client-order-1",
                        ticker="000010",
                        market="SZ",
                        side="BUY",
                        order_qty=100,
                        limit_price="1.6100",
                    )
                )
                self.assertTrue(
                    projector.record_sdk_accepted(
                        event_id="async-sdk-accepted",
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        gateway_order_id=_ORDER,
                        order_emt_id=101,
                    )
                )
                self.assertTrue(projector.wait_until_idle(5.0))
                with PostgresOrderLedger(database.dsn, schema=database.schema) as reader:
                    projected = reader.order_by_client_id(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        client_order_id="client-order-1",
                    )
                    self.assertIsNotNone(projected)
                    self.assertEqual(101, projected.order_emt_id)  # type: ignore[union-attr]  # projection polling guarantees record
                    self.assertEqual(OrderState.SDK_ACCEPTED, projected.state)  # type: ignore[union-attr]  # projection polling guarantees record
            finally:
                projector.close()

    def _ready(self, ledger: PostgresOrderLedger) -> None:
        ledger.begin_reconciliation(
            account_id=_ACCOUNT,
            trading_day=_DAY,
            event_id="reconcile-start",
            reason="startup",
        )
        ledger.complete_reconciliation(
            account_id=_ACCOUNT,
            trading_day=_DAY,
            event_id="reconcile-done",
        )

    def _record_intent(self, ledger: PostgresOrderLedger) -> None:
        ledger.record_intent(
            event_id="intent",
            account_id=_ACCOUNT,
            trading_day=_DAY,
            gateway_order_id=_ORDER,
            strategy_id="strategy-a",
            strategy_order_id="strategy-order-1",
            client_order_id="client-order-1",
            ticker="000010",
            market="SZ",
            side="BUY",
            order_qty=100,
            limit_price="1.6100",
        )

    def _order(self, ledger: PostgresOrderLedger):
        order = ledger.order(account_id=_ACCOUNT, trading_day=_DAY, gateway_order_id=_ORDER)
        self.assertIsNotNone(order)
        return order


class AsyncPostgresOrderLedgerFailureTests(unittest.TestCase):
    def test_connection_failure_is_background_only(self) -> None:
        projector = AsyncPostgresOrderLedger(
            "postgresql://127.0.0.1:1/emt_gateway_unavailable",
            reconnect_delay_seconds=0.01,
            max_reconnect_delay_seconds=0.05,
            connect_timeout_seconds=1,
        )
        try:
            self.assertTrue(
                projector.begin_reconciliation(
                    account_id=_ACCOUNT,
                    trading_day=_DAY,
                    event_id="unavailable-start",
                    reason="startup",
                )
            )
            for _ in range(150):
                if projector.status().failures:
                    break
                sleep(0.01)
            status = projector.status()
            self.assertGreaterEqual(status.failures, 1)
            self.assertEqual(0, status.dropped)
        finally:
            projector.close()
