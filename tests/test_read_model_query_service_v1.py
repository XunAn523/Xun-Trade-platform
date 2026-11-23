from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest import TestCase
from uuid import UUID

from xuanling_tradecore.read_model import (
    PostgresReadModelQueryServiceV1,
    ReadModelConsumerScopeV1,
    ReadModelOrderCursorV1,
    ReadModelQueryErrorV1,
)

_CONSUMER_ID = UUID("00000000-0000-0000-0000-000000000061")
_ORDER_A = UUID("00000000-0000-0000-0000-000000000062")
_ORDER_B = UUID("00000000-0000-0000-0000-000000000063")
_STRATEGY_ID = UUID("00000000-0000-0000-0000-000000000064")
_NOW = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Connection:
    def __init__(self):
        self.sql = []
        self.parameters = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback

    def transaction(self):
        return nullcontext()

    def execute(self, sql, parameters=None):
        self.sql.append(sql)
        self.parameters.append(parameters)
        if "FROM xuanling_tradecore_rm.v_orders" in sql:
            return _Cursor(
                [
                    (
                        _ORDER_A,
                        "account-a",
                        _STRATEGY_ID,
                        "strategy-order-a",
                        "SH",
                        "instrument-a",
                        "BUY",
                        10,
                        0,
                        10,
                        "WORKING",
                        1,
                        _NOW,
                        _NOW,
                    ),
                    (
                        _ORDER_B,
                        "account-a",
                        _STRATEGY_ID,
                        "strategy-order-b",
                        "SH",
                        "instrument-b",
                        "BUY",
                        10,
                        0,
                        10,
                        "WORKING",
                        1,
                        _NOW - timedelta(seconds=1),
                        _NOW,
                    ),
                ]
            )
        if "min(projected_at)" in sql:
            return _Cursor([(1, _NOW)])
        return _Cursor([])


class ReadModelQueryServiceV1Tests(TestCase):
    def test_order_query_is_consumer_scoped_keyset_pagination(self):
        connection = _Connection()
        service = PostgresReadModelQueryServiceV1(
            dsn="postgresql://query",
            stale_after=timedelta(seconds=5),
            connect=lambda _dsn: connection,
            now=lambda: _NOW,
            query_role="tradecore_query",
        )
        page = service.list_orders(
            scope=ReadModelConsumerScopeV1(_CONSUMER_ID), account_id="account-a", limit=1
        )
        self.assertEqual((_ORDER_A,), tuple(row.oms_order_id for row in page.rows))
        self.assertEqual(
            ReadModelOrderCursorV1(_NOW, _ORDER_A),
            page.next_cursor,
        )
        self.assertTrue(any("SET LOCAL ROLE tradecore_query" in sql for sql in connection.sql))
        self.assertIn(str(_CONSUMER_ID), connection.parameters[1])
        self.assertFalse(any("INSERT" in sql or "UPDATE" in sql for sql in connection.sql))

    def test_watermark_is_stale_only_as_a_query_freshness_signal(self):
        connection = _Connection()
        service = PostgresReadModelQueryServiceV1(
            dsn="postgresql://query",
            stale_after=timedelta(seconds=5),
            connect=lambda _dsn: connection,
            now=lambda: _NOW + timedelta(seconds=6),
        )
        watermark = service.watermark(scope=ReadModelConsumerScopeV1(_CONSUMER_ID))
        self.assertEqual(1, watermark.account_count)
        self.assertTrue(watermark.stale)

    def test_page_limit_is_bounded_before_any_database_call(self):
        with self.assertRaisesRegex(ReadModelQueryErrorV1, "page limit"):
            PostgresReadModelQueryServiceV1(
                dsn="postgresql://query", stale_after=timedelta(seconds=1)
            ).list_orders(scope=ReadModelConsumerScopeV1(_CONSUMER_ID), limit=501)


if __name__ == "__main__":
    import unittest

    unittest.main()
