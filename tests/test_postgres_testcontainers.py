"""Docker-backed PostgreSQL contracts for the asynchronous read-model path.

This module is deliberately opt-in outside CI.  It creates an isolated
PostgreSQL container and generated schemas only; it never loads the EMT SDK
or sends an order.  Enable it with ``EMT_GATEWAY_TESTCONTAINERS=1``.
"""

from __future__ import annotations

import os
import unittest
from time import monotonic, sleep

import psycopg
from emt_gateway.order_ledger import (
    AsyncPostgresOrderLedger,
    BookHealth,
    OrderState,
    PostgresOrderLedger,
)
from tests.postgres_support import PostgresTestDatabase

try:
    from testcontainers.postgres import PostgresContainer
except ImportError:
    _HAS_TESTCONTAINERS = False
else:
    _HAS_TESTCONTAINERS = True


_ENABLED = os.environ.get("EMT_GATEWAY_TESTCONTAINERS", "").strip() == "1"
_ACCOUNT = "ordinary-1"
_DAY = "20260829"
_ORDER = "container-order-1"


@unittest.skipUnless(
    _ENABLED and _HAS_TESTCONTAINERS,
    "set EMT_GATEWAY_TESTCONTAINERS=1 with testcontainers[postgres] installed",
)
class PostgresTestcontainersContracts(unittest.TestCase):
    """Exercise current schema initialization and recovery against real PostgreSQL."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._container = PostgresContainer(
            "postgres:16-alpine",
            username="emt_gateway_test",
            password="emt_gateway_test",
            dbname="emt_gateway_test",
        )
        cls._container.start()
        # Testcontainers exposes the SQLAlchemy psycopg2 URL form, while the
        # gateway intentionally uses psycopg 3 directly.
        cls._dsn = cls._container.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://", 1
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._container.stop()

    def test_current_schema_initialization_and_async_projection(self) -> None:
        with PostgresTestDatabase(self._dsn) as database:
            with PostgresOrderLedger(database.dsn, schema=database.schema) as ledger:
                with psycopg.connect(database.dsn) as connection:
                    rows = connection.execute(
                        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s",
                        (database.schema,),
                    ).fetchall()
                self.assertSetEqual(
                    {row[0] for row in rows},
                    {"book_status", "ledger_events", "ledger_heads", "orders", "trades"},
                )
                self.assertIsNone(
                    ledger.order_by_client_id(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        client_order_id="container-client-1",
                    )
                )

            projector = AsyncPostgresOrderLedger(
                f"{database.dsn}?application_name=emt_gateway_test_async_projector",
                schema=database.schema,
                reconnect_delay_seconds=0.010,
                max_reconnect_delay_seconds=0.050,
            )
            try:
                self.assertTrue(
                    projector.begin_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="container-reconcile-start",
                        reason="startup",
                    )
                )
                self.assertTrue(
                    projector.complete_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="container-reconcile-done",
                    )
                )
                self.assertTrue(
                    projector.record_intent(
                        event_id="container-intent",
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        gateway_order_id=_ORDER,
                        strategy_id="strategy-a",
                        strategy_order_id="strategy-order-1",
                        client_order_id="container-client-1",
                        ticker="000010",
                        market="SZ",
                        side="BUY",
                        order_qty=100,
                        limit_price="1.6100",
                    )
                )
                self.assertTrue(
                    projector.record_sdk_accepted(
                        event_id="container-sdk-accepted",
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
                        client_order_id="container-client-1",
                    )
                self.assertIsNotNone(projected)
                self.assertEqual(OrderState.SDK_ACCEPTED, projected.state)  # type: ignore[union-attr]  # polling condition guarantees projection
                self.assertEqual(101, projected.order_emt_id)  # type: ignore[union-attr]  # polling condition guarantees projection
            finally:
                projector.close()

    def test_connection_termination_is_retried_in_the_background_worker(self) -> None:
        with PostgresTestDatabase(self._dsn) as database:
            projector = AsyncPostgresOrderLedger(
                f"{database.dsn}?application_name=emt_gateway_test_async_projector",
                schema=database.schema,
                reconnect_delay_seconds=0.010,
                max_reconnect_delay_seconds=0.050,
            )
            try:
                self.assertTrue(
                    projector.begin_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="outage-reconcile-start",
                        reason="startup",
                    )
                )
                self.assertTrue(projector.wait_until_idle(5.0))

                with psycopg.connect(database.dsn, autocommit=True) as control:
                    control_pid = control.execute("SELECT pg_backend_pid()").fetchone()[0]
                    target = control.execute(
                        """
                        SELECT pid
                        FROM pg_stat_activity
                        WHERE application_name = %s AND pid <> %s
                        """,
                        ("emt_gateway_test_async_projector", control_pid),
                    ).fetchone()
                    self.assertIsNotNone(target)
                    terminated = control.execute(
                        "SELECT pg_terminate_backend(%s)", (target[0],)
                    ).fetchone()[0]
                self.assertTrue(terminated)

                # Queue admission remains immediate; only the worker observes
                # the broken connection, retries it, and projects the fact.
                self.assertTrue(
                    projector.complete_reconciliation(
                        account_id=_ACCOUNT,
                        trading_day=_DAY,
                        event_id="outage-reconcile-done",
                    )
                )
                self.assertTrue(_wait_for(lambda: projector.status().failures >= 1, 5.0))
                self.assertTrue(projector.wait_until_idle(5.0))
                self.assertGreaterEqual(projector.status().persisted, 2)

                with PostgresOrderLedger(database.dsn, schema=database.schema) as reader:
                    status = reader.book_status(account_id=_ACCOUNT, trading_day=_DAY)
                self.assertEqual(BookHealth.READY, status.health)
            finally:
                projector.close()


def _wait_for(predicate: object, timeout_seconds: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(0.010)
    return bool(predicate())
