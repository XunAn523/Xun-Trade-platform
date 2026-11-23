"""Read-only, consumer-scoped PostgreSQL query service for F3."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol, cast
from uuid import UUID


class ReadModelQueryErrorV1(RuntimeError):
    """A consumer query cannot be served from the non-authoritative read model."""


class _CursorV1(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchone(self) -> tuple[object, ...] | None: ...


class _ConnectionV1(Protocol):
    def __enter__(self) -> _ConnectionV1: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> _CursorV1: ...

    def transaction(self) -> AbstractContextManager[object]: ...


QueryConnectionFactoryV1 = Callable[[str], _ConnectionV1]


def _default_connect(dsn: str) -> _ConnectionV1:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - runtime dependency is declared.
        raise ReadModelQueryErrorV1("psycopg is unavailable for F3 query") from exc
    return cast(_ConnectionV1, psycopg.connect(dsn))


@dataclass(frozen=True)
class ReadModelConsumerScopeV1:
    """The RLS identity required for every query transaction."""

    consumer_id: UUID


@dataclass(frozen=True)
class ReadModelOrderCursorV1:
    """Opaque-to-transport keyset state; never an execution command."""

    created_at: datetime
    oms_order_id: UUID

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None:
            raise ReadModelQueryErrorV1("read-model order cursor requires an aware timestamp")


@dataclass(frozen=True)
class ReadModelOrderRowV1:
    oms_order_id: UUID
    account_id: str
    strategy_id: UUID
    strategy_order_id: str
    market: str
    instrument_id: str
    side: str
    order_qty: object
    filled_qty: object
    leaves_qty: object
    status: str
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ReadModelOrderPageV1:
    rows: tuple[ReadModelOrderRowV1, ...]
    next_cursor: ReadModelOrderCursorV1 | None


@dataclass(frozen=True)
class ReadModelWatermarkV1:
    """Read freshness; false freshness never becomes an execution approval."""

    account_count: int
    oldest_projected_at: datetime | None
    stale: bool


class PostgresReadModelQueryServiceV1:
    """Perform RLS-bound keyset queries; it has no submit/cancel method."""

    def __init__(
        self,
        *,
        dsn: str,
        stale_after: timedelta,
        connect: QueryConnectionFactoryV1 = _default_connect,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        query_role: str | None = None,
    ) -> None:
        if not dsn.strip():
            raise ReadModelQueryErrorV1("read-model query DSN is invalid")
        if stale_after <= timedelta(0):
            raise ReadModelQueryErrorV1("read-model stale threshold is invalid")
        if query_role is not None and (
            not query_role.replace("_", "").isalnum() or len(query_role) > 63
        ):
            raise ReadModelQueryErrorV1("read-model query role is invalid")
        self._dsn = dsn
        self._stale_after = stale_after
        self._connect = connect
        self._now = now
        self._query_role = query_role

    def list_orders(
        self,
        *,
        scope: ReadModelConsumerScopeV1,
        account_id: str | None = None,
        after: ReadModelOrderCursorV1 | None = None,
        limit: int = 100,
    ) -> ReadModelOrderPageV1:
        """Return the next stable, descending ``created_at, oms_order_id`` page."""

        if type(limit) is not int or not 1 <= limit <= 500:
            raise ReadModelQueryErrorV1("read-model order page limit is invalid")
        if account_id is not None and (not account_id or len(account_id) > 256):
            raise ReadModelQueryErrorV1("read-model account filter is invalid")
        cursor_time = (
            after.created_at if after is not None else datetime.max.replace(tzinfo=timezone.utc)
        )
        cursor_id = (
            after.oms_order_id
            if after is not None
            else UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        )
        try:
            with self._connect(self._dsn) as connection:
                with connection.transaction():
                    _set_consumer_scope(connection, scope, self._query_role)
                    rows = connection.execute(
                        "SELECT oms_order_id, account_id, strategy_id, strategy_order_id, market, "
                        "instrument_id, side, order_qty, filled_qty, leaves_qty, status, revision, "
                        "created_at, updated_at "
                        "FROM xuanling_tradecore_rm.v_orders "
                        "WHERE (%s::text IS NULL OR account_id = %s) "
                        "AND (created_at, oms_order_id) < (%s, %s) "
                        "ORDER BY created_at DESC, oms_order_id DESC LIMIT %s",
                        (account_id, account_id, cursor_time, cursor_id, limit + 1),
                    ).fetchall()
        except ReadModelQueryErrorV1:
            raise
        except Exception as exc:
            raise ReadModelQueryErrorV1("read-model order query failed") from exc
        mapped = tuple(_order_row(row) for row in rows[:limit])
        next_cursor = None
        if len(rows) > limit and mapped:
            last = mapped[-1]
            next_cursor = ReadModelOrderCursorV1(last.created_at, last.oms_order_id)
        return ReadModelOrderPageV1(mapped, next_cursor)

    def watermark(self, *, scope: ReadModelConsumerScopeV1) -> ReadModelWatermarkV1:
        """Return query-read freshness based on RLS-visible account projections."""

        try:
            with self._connect(self._dsn) as connection:
                with connection.transaction():
                    _set_consumer_scope(connection, scope, self._query_role)
                    row = connection.execute(
                        "SELECT count(*), min(projected_at) FROM xuanling_tradecore_rm.v_accounts"
                    ).fetchone()
        except Exception as exc:
            raise ReadModelQueryErrorV1("read-model watermark query failed") from exc
        if row is None:
            raise ReadModelQueryErrorV1("read-model watermark query returned no aggregate")
        count, oldest = int(cast(int, row[0])), cast(datetime | None, row[1])
        stale = count == 0 or oldest is None or self._now() - oldest > self._stale_after
        return ReadModelWatermarkV1(count, oldest, stale)


def _set_consumer_scope(
    connection: _ConnectionV1, scope: ReadModelConsumerScopeV1, query_role: str | None
) -> None:
    if query_role is not None:
        connection.execute(f"SET LOCAL ROLE {query_role}")
    connection.execute("SELECT set_config('app.consumer_id', %s, true)", (str(scope.consumer_id),))


def _order_row(row: tuple[object, ...]) -> ReadModelOrderRowV1:
    return ReadModelOrderRowV1(
        oms_order_id=cast(UUID, row[0]),
        account_id=str(row[1]),
        strategy_id=cast(UUID, row[2]),
        strategy_order_id=str(row[3]),
        market=str(row[4]),
        instrument_id=str(row[5]),
        side=str(row[6]),
        order_qty=row[7],
        filled_qty=row[8],
        leaves_qty=row[9],
        status=str(row[10]),
        revision=int(cast(int, row[11])),
        created_at=cast(datetime, row[12]),
        updated_at=cast(datetime, row[13]),
    )
