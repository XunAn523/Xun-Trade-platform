from __future__ import annotations

import unittest
from dataclasses import dataclass
from hashlib import sha256
from time import sleep

from emt_gateway.gateway import (
    CancelIntent,
    CommandState,
    OrderGateway,
    OrderIntent,
    UnknownOrderError,
)
from emt_gateway.journal import InMemoryJournal
from emt_gateway.order_ledger import (
    AsyncPostgresOrderLedger,
    BookHealth,
    LedgerCallbackFact,
    LedgerCallbackKind,
    LedgerOrderIntent,
    OrderState,
    PostgresOrderLedger,
)
from tests.postgres_support import HAS_TEST_POSTGRES, PostgresTestDatabase


def _digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


class TraceSpy:
    """Payload-free implementation of the public performance trace protocol."""

    def __init__(self) -> None:
        self.traces: dict[str, dict[str, object]] = {}
        self.metrics: list[tuple[str, int]] = []

    def begin(self, correlation_id: str, **identity: object) -> None:
        self.traces[correlation_id] = {"identity": identity, "stages": ["C0"], "outcome": None}

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None:
        self.traces[correlation_id]["stages"].append(stage)  # type: ignore[index]  # heterogeneous test trace

    def complete(self, correlation_id: str, outcome: str = "ok", reason: str | None = None) -> None:
        self.traces[correlation_id]["outcome"] = outcome

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None:
        self.metrics.append((metric, value))


@dataclass
class FakeNative:
    order_return: int = 101
    cancel_return: int = 201
    error_code: int = 0
    raise_on_order: bool = False

    def __post_init__(self) -> None:
        self.orders: list[tuple[str, str]] = []
        self.cancels: list[tuple[str, int]] = []

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        self.orders.append((command_id, client_order_id))
        if self.raise_on_order:
            raise RuntimeError("native process boundary failed")
        return self.order_return

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        self.cancels.append((command_id, order_emt_id))
        return self.cancel_return

    def get_last_error_code(self) -> int:
        return self.error_code


class FailingJournal:
    def append(self, **kwargs: object) -> object:
        raise OSError("durability unavailable")


class OrderGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.trace = TraceSpy()
        self.native = FakeNative()
        self.command_journal = InMemoryJournal()
        self.event_journal = InMemoryJournal()
        self.gateway = OrderGateway(
            native=self.native,
            command_journal=self.command_journal,
            event_journal=self.event_journal,
            trace=self.trace,
            callback_capacity=2,
        )

    def test_order_is_durable_before_native_and_same_id_never_replays(self) -> None:
        intent = OrderIntent("strategy-order-1", _digest("order-1"))
        first = self.gateway.submit_order(intent)
        second = self.gateway.submit_order(intent)

        self.assertEqual(CommandState.NATIVE_ACCEPTED, first.state)
        self.assertEqual(first, second)
        self.assertEqual(1, len(self.native.orders))
        self.assertEqual(
            ["CALL_STARTED", "NATIVE_ACCEPTED"],
            [record.kind for record in self.command_journal.records()],
        )
        self.assertEqual(
            ["C0", "C1", "C2", "C3", "C4"], self.trace.traces[first.correlation_id]["stages"]
        )

    def test_callback_creates_c5_to_c7_for_order_and_spi_paths(self) -> None:
        submitted = self.gateway.submit_order(OrderIntent("strategy-order-2", _digest("order-2")))
        callback_id = self.gateway.callbacks.receive(
            "OnOrderEvent",
            profile="ordinary",
            workload="steady",
            order_emt_id=submitted.native_id,
        )
        self.assertIsNotNone(callback_id)
        delivery = self.gateway.callbacks.normalize_one()
        self.assertIsNotNone(delivery)
        self.gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery

        self.assertEqual(
            ["C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"],
            self.trace.traces[submitted.correlation_id]["stages"],
        )
        self.assertEqual("ok", self.trace.traces[submitted.correlation_id]["outcome"])
        self.assertEqual(["C0", "C5", "C6", "C7"], self.trace.traces[callback_id]["stages"])
        self.assertEqual("ok", self.trace.traces[callback_id]["outcome"])
        self.assertEqual(
            ["NATIVE_CALLBACK"], [record.kind for record in self.event_journal.records()]
        )

    def test_duplicate_order_notifications_keep_one_command_c5_to_c7_path(self) -> None:
        submitted = self.gateway.submit_order(
            OrderIntent("strategy-order-duplicate", _digest("order-duplicate"))
        )
        for _ in range(2):
            self.gateway.callbacks.receive(
                "OnOrderEvent",
                profile="ordinary",
                workload="steady",
                order_emt_id=submitted.native_id,
            )
            delivery = self.gateway.callbacks.normalize_one()
            self.assertIsNotNone(delivery)
            self.gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery

        self.assertEqual(
            ["C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"],
            self.trace.traces[submitted.correlation_id]["stages"],
        )
        self.assertEqual("ok", self.trace.traces[submitted.correlation_id]["outcome"])
        self.assertEqual(2, len(self.event_journal.records()))

    def test_call_started_exception_is_unknown_and_same_id_is_not_called_again(self) -> None:
        self.native.raise_on_order = True
        intent = OrderIntent("strategy-order-3", _digest("order-3"))
        first = self.gateway.submit_order(intent)
        second = self.gateway.submit_order(intent)

        self.assertEqual(CommandState.SUBMIT_UNKNOWN, first.state)
        self.assertEqual(first, second)
        self.assertEqual(1, len(self.native.orders))
        self.assertEqual(
            ["CALL_STARTED", "SUBMIT_UNKNOWN"],
            [record.kind for record in self.command_journal.records()],
        )

    def test_cancel_requires_native_mapping_and_uses_its_own_callback_correlation(self) -> None:
        with self.assertRaises(UnknownOrderError):
            self.gateway.cancel_order(CancelIntent("cancel-1", "unknown"))

        submitted = self.gateway.submit_order(OrderIntent("strategy-order-4", _digest("order-4")))
        cancelled = self.gateway.cancel_order(CancelIntent("cancel-2", "strategy-order-4"))
        self.assertEqual(CommandState.NATIVE_ACCEPTED, cancelled.state)
        self.assertEqual([(cancelled.command_id, submitted.native_id)], self.native.cancels)

        self.gateway.callbacks.receive(
            "OnOrderEvent",
            profile="ordinary",
            workload="steady",
            order_emt_id=submitted.native_id,
            operation_native_id=cancelled.native_id,
        )
        delivery = self.gateway.callbacks.normalize_one()
        self.gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery
        self.assertEqual("ok", self.trace.traces[cancelled.correlation_id]["outcome"])

    @unittest.skipUnless(
        HAS_TEST_POSTGRES, "set EMT_GATEWAY_TEST_POSTGRES_DSN to run PostgreSQL ledger tests"
    )
    def test_async_projection_persists_mapping_but_restart_never_uses_pg_for_cancel(self) -> None:
        with PostgresTestDatabase() as database:
            ledger = AsyncPostgresOrderLedger(database.dsn, schema=database.schema)
            try:
                gateway = OrderGateway(
                    native=self.native,
                    command_journal=self.command_journal,
                    event_journal=self.event_journal,
                    trace=self.trace,
                    callback_capacity=2,
                    order_ledger=ledger,
                    ledger_account_id="ordinary-1",
                    ledger_trading_day="20260827",
                )
                order = gateway.submit_order(
                    OrderIntent(
                        "strategy-order-ledger",
                        _digest("order-ledger"),
                        ledger_intent=LedgerOrderIntent(
                            account_id="ordinary-1",
                            trading_day="20260827",
                            strategy_id="strategy-a",
                            strategy_order_id="strategy-order-ledger",
                            ticker="000010",
                            market="SZ",
                            side="BUY",
                            order_qty=100,
                            limit_price="1.6100",
                        ),
                    )
                )
                self.assertEqual(CommandState.NATIVE_ACCEPTED, order.state)
                self.assertTrue(ledger.wait_until_idle(5.0))
                with PostgresOrderLedger(database.dsn, schema=database.schema) as reader:
                    persisted = reader.order_by_client_id(
                        account_id="ordinary-1",
                        trading_day="20260827",
                        client_order_id="strategy-order-ledger",
                    )
                    self.assertIsNotNone(persisted)
                    self.assertEqual(order.native_id, persisted.order_emt_id)  # type: ignore[union-attr]  # persistence polling guarantees record
                    self.assertEqual(OrderState.SDK_ACCEPTED, persisted.state)  # type: ignore[union-attr]  # persistence polling guarantees record

                restarted = OrderGateway(
                    native=self.native,
                    command_journal=InMemoryJournal(),
                    event_journal=InMemoryJournal(),
                    trace=TraceSpy(),
                    callback_capacity=2,
                    order_ledger=ledger,
                    ledger_account_id="ordinary-1",
                    ledger_trading_day="20260827",
                )
                with self.assertRaises(UnknownOrderError):
                    restarted.cancel_order(
                        CancelIntent(
                            "strategy-cancel-ledger",
                            "strategy-order-ledger",
                            ledger_account_id="ordinary-1",
                            ledger_trading_day="20260827",
                        )
                    )
                self.assertEqual([], self.native.cancels)
            finally:
                ledger.close()

    @unittest.skipUnless(
        HAS_TEST_POSTGRES, "set EMT_GATEWAY_TEST_POSTGRES_DSN to run PostgreSQL ledger tests"
    )
    def test_durable_emt_callbacks_project_order_and_trade_and_dedupe_replay(self) -> None:
        with PostgresTestDatabase() as database:
            ledger = AsyncPostgresOrderLedger(database.dsn, schema=database.schema)
            try:
                gateway = OrderGateway(
                    native=self.native,
                    command_journal=self.command_journal,
                    event_journal=self.event_journal,
                    trace=self.trace,
                    callback_capacity=8,
                    order_ledger=ledger,
                    ledger_account_id="ordinary-1",
                    ledger_trading_day="20260827",
                )
                submitted = gateway.submit_order(
                    OrderIntent(
                        "strategy-order-callback-ledger",
                        _digest("callback-ledger"),
                        ledger_intent=LedgerOrderIntent(
                            account_id="ordinary-1",
                            trading_day="20260827",
                            strategy_id="strategy-a",
                            strategy_order_id="strategy-order-callback-ledger",
                            ticker="000010",
                            market="SZ",
                            side="BUY",
                            order_qty=100,
                            limit_price="1.6100",
                        ),
                    )
                )
                self.assertEqual(CommandState.NATIVE_ACCEPTED, submitted.state)
                order_fact = LedgerCallbackFact(
                    kind=LedgerCallbackKind.ORDER_UPDATE,
                    source_key="order:101:4:20260827100000000:0:100:-",
                    order_emt_id=101,
                    client_order_id="strategy-order-callback-ledger",
                    state=OrderState.LIVE,
                    leaves_qty=100,
                )
                trade_fact = LedgerCallbackFact(
                    kind=LedgerCallbackKind.TRADE,
                    source_key="trade:SZ:report:99",
                    order_emt_id=101,
                    client_order_id="strategy-order-callback-ledger",
                    market="SZ",
                    execution_key="report:99",
                    qty=100,
                    price="1.6100",
                )
                for fact in (order_fact, trade_fact, trade_fact):
                    self.assertIsNotNone(
                        gateway.callbacks.receive(
                            "OnOrderEvent"
                            if fact.kind is LedgerCallbackKind.ORDER_UPDATE
                            else "OnTradeEvent",
                            profile="ordinary",
                            workload="steady",
                            order_emt_id=101,
                            ledger_fact=fact,
                        )
                    )
                    delivery = gateway.callbacks.normalize_one()
                    self.assertIsNotNone(delivery)
                    gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery

                self.assertTrue(ledger.wait_until_idle(5.0))
                with PostgresOrderLedger(database.dsn, schema=database.schema) as reader:
                    projected = reader.order_by_client_id(
                        account_id="ordinary-1",
                        trading_day="20260827",
                        client_order_id="strategy-order-callback-ledger",
                    )
                    self.assertIsNotNone(projected)
                    self.assertEqual(OrderState.FILLED, projected.state)  # type: ignore[union-attr]  # projection polling guarantees record
                    self.assertEqual(100, projected.filled_qty)  # type: ignore[union-attr]  # projection polling guarantees record
                    self.assertEqual(0, projected.leaves_qty)  # type: ignore[union-attr]  # projection polling guarantees record
                    trades = [
                        event
                        for event in reader.events(account_id="ordinary-1", trading_day="20260827")
                        if event.kind.value == "TRADE"
                    ]
                    self.assertEqual(1, len(trades))
            finally:
                ledger.close()

    @unittest.skipUnless(
        HAS_TEST_POSTGRES, "set EMT_GATEWAY_TEST_POSTGRES_DSN to run PostgreSQL ledger tests"
    )
    def test_unknown_emt_callback_requires_reconciliation_without_guessing_an_order(self) -> None:
        with PostgresTestDatabase() as database:
            ledger = AsyncPostgresOrderLedger(database.dsn, schema=database.schema)
            try:
                gateway = OrderGateway(
                    native=self.native,
                    command_journal=self.command_journal,
                    event_journal=self.event_journal,
                    trace=self.trace,
                    callback_capacity=2,
                    order_ledger=ledger,
                    ledger_account_id="ordinary-1",
                    ledger_trading_day="20260827",
                )
                fact = LedgerCallbackFact(
                    kind=LedgerCallbackKind.ORDER_UPDATE,
                    source_key="order:999:4:20260827100100000:0:100:-",
                    order_emt_id=999,
                    state=OrderState.LIVE,
                    leaves_qty=100,
                )
                gateway.callbacks.receive(
                    "OnOrderEvent",
                    profile="ordinary",
                    workload="steady",
                    order_emt_id=999,
                    ledger_fact=fact,
                )
                delivery = gateway.callbacks.normalize_one()
                self.assertIsNotNone(delivery)
                self.assertTrue(ledger.wait_until_idle(5.0))
                with PostgresOrderLedger(database.dsn, schema=database.schema) as reader:
                    self.assertEqual(
                        BookHealth.RECONCILE_REQUIRED,
                        reader.book_status(account_id="ordinary-1", trading_day="20260827").health,
                    )
            finally:
                ledger.close()

    def test_unavailable_postgres_does_not_block_command_or_callback_ack(self) -> None:
        """No PG connection, transaction, or retry is allowed on C0--C7."""

        ledger = AsyncPostgresOrderLedger(
            "postgresql://127.0.0.1:1/emt_gateway_unavailable",
            reconnect_delay_seconds=0.01,
            max_reconnect_delay_seconds=0.05,
            connect_timeout_seconds=1,
        )
        try:
            gateway = OrderGateway(
                native=self.native,
                command_journal=self.command_journal,
                event_journal=self.event_journal,
                trace=self.trace,
                order_ledger=ledger,
                ledger_account_id="ordinary-1",
                ledger_trading_day="20260827",
            )
            submitted = gateway.submit_order(
                OrderIntent(
                    "strategy-order-pg-down",
                    _digest("pg-down"),
                    ledger_intent=LedgerOrderIntent(
                        account_id="ordinary-1",
                        trading_day="20260827",
                        strategy_id="strategy-a",
                        strategy_order_id="strategy-order-pg-down",
                        ticker="000010",
                        market="SZ",
                        side="BUY",
                        order_qty=100,
                        limit_price="1.6100",
                    ),
                )
            )
            self.assertEqual(CommandState.NATIVE_ACCEPTED, submitted.state)
            callback_id = gateway.callbacks.receive(
                "OnOrderEvent",
                profile="ordinary",
                workload="steady",
                order_emt_id=submitted.native_id,
                ledger_fact=LedgerCallbackFact(
                    kind=LedgerCallbackKind.ORDER_UPDATE,
                    source_key="order:101:pg-down",
                    order_emt_id=101,
                    state=OrderState.LIVE,
                    leaves_qty=100,
                ),
            )
            delivery = gateway.callbacks.normalize_one()
            self.assertIsNotNone(callback_id)
            self.assertIsNotNone(delivery)
            gateway.callbacks.ack(delivery.delivery_id)  # type: ignore[union-attr]  # fixture produces a callback delivery
            self.assertEqual("ok", self.trace.traces[submitted.correlation_id]["outcome"])
            self.assertEqual("ok", self.trace.traces[callback_id]["outcome"])

            # Connection refusal happens on the background worker only. Give
            # it a bounded scheduling window, then prove the failure is merely
            # observable rather than propagated into either gateway path.
            for _ in range(150):
                if ledger.status().failures:
                    break
                sleep(0.01)
            self.assertGreaterEqual(ledger.status().failures, 1)
        finally:
            ledger.close()

    def test_native_zero_return_is_a_recorded_local_rejection(self) -> None:
        self.native.order_return = 0
        self.native.error_code = 211008
        result = self.gateway.submit_order(OrderIntent("strategy-order-5", _digest("order-5")))

        self.assertEqual(CommandState.LOCAL_SEND_REJECTED, result.state)
        self.assertEqual(211008, result.native_error_code)
        self.assertEqual("rejected", self.trace.traces[result.correlation_id]["outcome"])

    def test_admission_rejection_never_reaches_the_journal_or_native_boundary(self) -> None:
        gateway = OrderGateway(
            native=self.native,
            command_journal=self.command_journal,
            event_journal=self.event_journal,
            trace=self.trace,
            admit=lambda interface, payload_hash: (_ for _ in ()).throw(
                ValueError("risk rejected")
            ),
        )
        result = gateway.submit_order(OrderIntent("strategy-order-6", _digest("order-6")))

        self.assertEqual(CommandState.REJECTED_LOCAL, result.state)
        self.assertEqual([], self.native.orders)
        self.assertEqual((), self.command_journal.records())

    def test_journal_failure_fails_closed_before_the_native_boundary(self) -> None:
        gateway = OrderGateway(
            native=self.native,
            command_journal=FailingJournal(),  # type: ignore[arg-type]  # failure double intentionally implements partial journal
            event_journal=self.event_journal,
            trace=self.trace,
        )
        result = gateway.submit_order(OrderIntent("strategy-order-7", _digest("order-7")))

        self.assertEqual(CommandState.REJECTED_LOCAL, result.state)
        self.assertEqual([], self.native.orders)
        self.assertEqual("failed", self.trace.traces[result.correlation_id]["outcome"])

    def test_callback_overflow_disarms_instead_of_silent_drop(self) -> None:
        gateway = OrderGateway(
            native=self.native,
            command_journal=InMemoryJournal(),
            event_journal=InMemoryJournal(),
            trace=self.trace,
            callback_capacity=1,
        )
        self.assertIsNotNone(
            gateway.callbacks.receive("OnError", profile="ordinary", workload="reconnect")
        )
        self.assertIsNone(
            gateway.callbacks.receive("OnError", profile="ordinary", workload="reconnect")
        )
        self.assertTrue(gateway.callbacks.disarmed)
        self.assertTrue(gateway.callbacks.callback_gap_pending)
        self.assertTrue(gateway.callbacks.wait_for_callback_gap(0))
        self.assertFalse(gateway.callbacks.acknowledge_callback_gap_persisted())
        self.assertIn(("dropped_callbacks", 1), self.trace.metrics)

        result = gateway.submit_order(OrderIntent("blocked-after-gap", _digest("blocked")))
        self.assertEqual(CommandState.REJECTED_LOCAL, result.state)
        self.assertEqual([], self.native.orders)

    def test_callback_batch_preserves_order_and_each_delivery_can_be_acked(self) -> None:
        gateway = OrderGateway(
            native=self.native,
            command_journal=self.command_journal,
            event_journal=self.event_journal,
            trace=self.trace,
            callback_capacity=4,
        )
        submitted = gateway.submit_order(OrderIntent("strategy-order-batch", _digest("batch")))
        for _ in range(3):
            self.assertIsNotNone(
                gateway.callbacks.receive(
                    "OnOrderEvent",
                    profile="ordinary",
                    workload="steady",
                    order_emt_id=submitted.native_id,
                )
            )
        deliveries = gateway.callbacks.normalize_many(64)
        self.assertEqual(3, len(deliveries))
        self.assertEqual([1, 2, 3], [item.event_lsn for item in deliveries])
        for delivery in deliveries:
            gateway.callbacks.ack(delivery.delivery_id)
        self.assertEqual(3, len(self.event_journal.records()))
