"""A small, durable PostgreSQL order ledger; deliberately not a full OMS.

The ledger keeps an immutable event stream and a rebuildable PostgreSQL
projection for one EMT account/trading-day scope.  It owns local continuity only: command
identity, EMT id mapping, observed order states and de-duplicated executions.
It does *not* route orders, match orders, calculate positions, infer an
exchange state, or automatically retry an ambiguous native submission.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from queue import Empty, Full, Queue
from threading import Condition, Event, RLock, Thread
from time import monotonic, perf_counter_ns
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row


class OrderLedgerError(RuntimeError):
    """The local order ledger cannot safely accept or project an event."""


class BookHealth(str, Enum):
    """Whether the local book may be used as an EMT-state cache."""

    UNINITIALIZED = "UNINITIALIZED"
    RECONCILING = "RECONCILING"
    READY = "READY"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    DEGRADED = "DEGRADED"


class OrderState(str, Enum):
    """Normalized local state; EMT remains the external state authority."""

    INTENT_DURABLE = "INTENT_DURABLE"
    CALL_STARTED = "CALL_STARTED"
    SDK_ACCEPTED = "SDK_ACCEPTED"
    LIVE = "LIVE"
    PART_FILLED = "PART_FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


class LedgerEventKind(str, Enum):
    """The narrow set of facts required to rebuild the lightweight book."""

    ORDER_INTENT = "ORDER_INTENT"
    CALL_STARTED = "CALL_STARTED"
    SDK_ACCEPTED = "SDK_ACCEPTED"
    SDK_REJECTED = "SDK_REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    ORDER_UPDATE = "ORDER_UPDATE"
    TRADE = "TRADE"
    RECONCILIATION_STARTED = "RECONCILIATION_STARTED"
    RECONCILIATION_COMPLETED = "RECONCILIATION_COMPLETED"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


class LedgerCallbackKind(str, Enum):
    """The two EMT callback facts which are useful to the local book."""

    ORDER_UPDATE = "ORDER_UPDATE"
    TRADE = "TRADE"


_TERMINAL_STATES = frozenset({OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED})


@dataclass(frozen=True)
class LedgerEvent:
    lsn: int
    event_id: str
    kind: LedgerEventKind
    account_id: str
    trading_day: str
    gateway_order_id: str
    source_key: str | None
    payload: dict[str, Any]
    observed_ns: int
    previous_hash: str
    event_hash: str


@dataclass(frozen=True)
class OrderView:
    account_id: str
    trading_day: str
    gateway_order_id: str
    strategy_id: str | None
    strategy_order_id: str | None
    client_order_id: str | None
    order_emt_id: int | None
    cancel_emt_id: int | None
    ticker: str | None
    market: str | None
    side: str | None
    order_qty: int | None
    limit_price: str | None
    filled_qty: int
    leaves_qty: int | None
    state: OrderState
    last_lsn: int


@dataclass(frozen=True)
class BookStatus:
    account_id: str
    trading_day: str
    health: BookHealth
    last_lsn: int
    reconciliation_generation: int
    reason: str | None


@dataclass(frozen=True)
class LedgerOrderIntent:
    """Business fields needed to persist one gateway-owned EMT order."""

    account_id: str
    trading_day: str
    strategy_id: str
    strategy_order_id: str
    ticker: str
    market: str
    side: str
    order_qty: int
    limit_price: str


@dataclass(frozen=True)
class LedgerCallbackFact:
    """Small, normalized callback data copied at the SPI boundary.

    It intentionally excludes vendor dictionaries, error strings, account
    credentials, and symbol text.  ``source_key`` is a stable EMT-side key so
    a callback replay becomes an idempotent ledger append rather than another
    local fill.  Only the fields applicable to ``kind`` are populated.
    """

    kind: LedgerCallbackKind
    source_key: str
    order_emt_id: int
    client_order_id: str | None = None
    state: OrderState | None = None
    leaves_qty: int | None = None
    cancel_emt_id: int | None = None
    market: str | None = None
    execution_key: str | None = None
    qty: int | None = None
    price: str | None = None


@dataclass(frozen=True)
class AsyncLedgerStatus:
    """Best-effort projection health; never a gateway trading gate."""

    queued: int
    inflight: int
    persisted: int
    dropped: int
    failures: int
    last_error: str | None


@dataclass(frozen=True)
class _LedgerWorkItem:
    operation: str
    kwargs: dict[str, Any]


class PostgresOrderLedger:
    """Immutable event ledger plus rebuildable PostgreSQL projection.

    ``ledger_events`` is application-immutable and is written together with
    the materialized ``orders`` / ``trades`` rows in one PostgreSQL
    transaction.  A row in ``ledger_heads`` is locked with ``FOR UPDATE`` for
    every account/trading-day append, serializing the hash chain without a
    process-local lock.  A failed projection write rolls the whole transaction
    back; :meth:`rebuild_projection` derives the view exclusively from the
    immutable event sequence.

    The DSN must point to a dedicated database or schema owned by the gateway
    role.  TLS, role password, and endpoint are deployment secrets; they are
    never persisted in ledger events or application logs.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "emt_gateway",
        initialize_schema: bool = True,
        connect_timeout_seconds: int | None = None,
    ) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise OrderLedgerError("PostgreSQL DSN must be a non-blank string")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise OrderLedgerError("PostgreSQL schema must be a simple SQL identifier")
        if connect_timeout_seconds is not None and connect_timeout_seconds <= 0:
            raise OrderLedgerError("connect_timeout_seconds must be positive or None")
        self.schema = schema
        self._lock = RLock()
        self._closed = False
        try:
            connection_kwargs: dict[str, Any] = {"autocommit": True, "row_factory": dict_row}
            if connect_timeout_seconds is not None:
                connection_kwargs["connect_timeout"] = connect_timeout_seconds
            self._connection = psycopg.connect(dsn, **connection_kwargs)
            if initialize_schema:
                self._create_schema()
            else:
                self._connection.execute(f'SET search_path TO "{self.schema}"')
        except (psycopg.Error, OrderLedgerError) as exc:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            if isinstance(exc, OrderLedgerError):
                raise
            raise OrderLedgerError("cannot open PostgreSQL order ledger") from exc

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._connection.close()
            finally:
                self._closed = True

    def __enter__(self) -> PostgresOrderLedger:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def begin_reconciliation(
        self,
        *,
        account_id: str,
        trading_day: str,
        event_id: str,
        reason: str,
    ) -> LedgerEvent:
        return self.append(
            kind=LedgerEventKind.RECONCILIATION_STARTED,
            event_id=event_id,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id="__book__",
            payload={"reason": _required_text(reason, "reason")},
        )

    def complete_reconciliation(
        self,
        *,
        account_id: str,
        trading_day: str,
        event_id: str,
    ) -> LedgerEvent:
        return self.append(
            kind=LedgerEventKind.RECONCILIATION_COMPLETED,
            event_id=event_id,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id="__book__",
            payload={},
        )

    def require_reconciliation(
        self,
        *,
        account_id: str,
        trading_day: str,
        event_id: str,
        reason: str,
        gateway_order_id: str = "__book__",
    ) -> LedgerEvent:
        return self.append(
            kind=LedgerEventKind.RECONCILE_REQUIRED,
            event_id=event_id,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id=gateway_order_id,
            payload={"reason": _required_text(reason, "reason")},
        )

    def record_intent(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        strategy_id: str,
        strategy_order_id: str,
        client_order_id: str,
        ticker: str,
        market: str,
        side: str,
        order_qty: int,
        limit_price: str,
        projection_only: bool = False,
    ) -> LedgerEvent:
        """Persist an immutable order intent.

        The direct ledger API retains its historical ready-state guard. The
        asynchronous projector may set ``projection_only`` because it is fed
        by an already-durable command journal and is never an authorization
        source for a native EMT side effect.
        """

        if not projection_only:
            self.require_ready_for_new_orders(account_id=account_id, trading_day=trading_day)
        if not isinstance(order_qty, int) or order_qty <= 0:
            raise OrderLedgerError("order_qty must be a positive integer")
        return self.append(
            kind=LedgerEventKind.ORDER_INTENT,
            event_id=event_id,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id=gateway_order_id,
            payload={
                "strategy_id": _required_text(strategy_id, "strategy_id"),
                "strategy_order_id": _required_text(strategy_order_id, "strategy_order_id"),
                "client_order_id": _required_text(client_order_id, "client_order_id"),
                "ticker": _required_text(ticker, "ticker"),
                "market": _required_text(market, "market"),
                "side": _required_text(side, "side"),
                "order_qty": order_qty,
                "limit_price": _required_text(limit_price, "limit_price"),
            },
        )

    def record_call_started(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
    ) -> LedgerEvent:
        return self._order_event(
            LedgerEventKind.CALL_STARTED, event_id, account_id, trading_day, gateway_order_id, {}
        )

    def record_sdk_accepted(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        order_emt_id: int,
    ) -> LedgerEvent:
        if not isinstance(order_emt_id, int) or order_emt_id <= 0:
            raise OrderLedgerError("order_emt_id must be a positive integer")
        return self._order_event(
            LedgerEventKind.SDK_ACCEPTED,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {"order_emt_id": order_emt_id},
        )

    def record_sdk_rejected(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        reason: str,
    ) -> LedgerEvent:
        return self._order_event(
            LedgerEventKind.SDK_REJECTED,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {"reason": _required_text(reason, "reason")},
        )

    def record_submit_unknown(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        reason: str,
    ) -> LedgerEvent:
        return self._order_event(
            LedgerEventKind.SUBMIT_UNKNOWN,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {"reason": _required_text(reason, "reason")},
        )

    def record_cancel_requested(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
    ) -> LedgerEvent:
        return self._order_event(
            LedgerEventKind.CANCEL_REQUESTED,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {},
        )

    def record_cancel_accepted(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        cancel_emt_id: int,
    ) -> LedgerEvent:
        if not isinstance(cancel_emt_id, int) or cancel_emt_id <= 0:
            raise OrderLedgerError("cancel_emt_id must be a positive integer")
        return self._order_event(
            LedgerEventKind.CANCEL_ACCEPTED,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {"cancel_emt_id": cancel_emt_id},
        )

    def record_cancel_rejected(
        self,
        *,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        reason: str,
    ) -> LedgerEvent:
        return self._order_event(
            LedgerEventKind.CANCEL_REJECTED,
            event_id,
            account_id,
            trading_day,
            gateway_order_id,
            {"reason": _required_text(reason, "reason")},
        )

    def record_order_update(
        self,
        *,
        event_id: str,
        source_key: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        order_emt_id: int,
        state: OrderState,
        leaves_qty: int | None = None,
        cancel_emt_id: int | None = None,
    ) -> LedgerEvent:
        """Record one normalized EMT order callback after its durable ingress."""

        if not isinstance(order_emt_id, int) or order_emt_id <= 0:
            raise OrderLedgerError("order_emt_id must be a positive integer")
        if leaves_qty is not None and (not isinstance(leaves_qty, int) or leaves_qty < 0):
            raise OrderLedgerError("leaves_qty must be a non-negative integer or None")
        if cancel_emt_id is not None and (not isinstance(cancel_emt_id, int) or cancel_emt_id <= 0):
            raise OrderLedgerError("cancel_emt_id must be a positive integer or None")
        if state not in {
            OrderState.LIVE,
            OrderState.FILLED,
            OrderState.CANCELED,
            OrderState.REJECTED,
        }:
            raise OrderLedgerError(
                "order callback state must be LIVE, FILLED, CANCELED, or REJECTED"
            )
        return self.append(
            kind=LedgerEventKind.ORDER_UPDATE,
            event_id=event_id,
            source_key=_required_text(source_key, "source_key"),
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id=gateway_order_id,
            payload={
                "order_emt_id": order_emt_id,
                "state": state.value,
                "leaves_qty": leaves_qty,
                "cancel_emt_id": cancel_emt_id,
            },
        )

    def record_trade(
        self,
        *,
        event_id: str,
        execution_key: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        order_emt_id: int,
        market: str,
        qty: int,
        price: str,
    ) -> LedgerEvent:
        """Record one normalized trade event using an EMT execution dedupe key."""

        if not isinstance(order_emt_id, int) or order_emt_id <= 0:
            raise OrderLedgerError("order_emt_id must be a positive integer")
        if not isinstance(qty, int) or qty <= 0:
            raise OrderLedgerError("trade qty must be a positive integer")
        execution_key = _required_text(execution_key, "execution_key")
        market = _required_text(market, "market")
        return self.append(
            kind=LedgerEventKind.TRADE,
            event_id=event_id,
            source_key=execution_key,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id=gateway_order_id,
            payload={
                "order_emt_id": order_emt_id,
                "execution_key": execution_key,
                "market": market,
                "qty": qty,
                "price": _required_text(price, "price"),
            },
        )

    def record_callback_fact(
        self,
        *,
        account_id: str,
        trading_day: str,
        fact: LedgerCallbackFact,
    ) -> None:
        """Project one already-durable callback fact in the background.

        This is deliberately a projection concern: it may require reads and
        PostgreSQL transactions, and must therefore only run in the async
        projector worker, never in the EMT SPI / gateway path.
        """

        order = self.order_by_emt_id(
            account_id=account_id,
            trading_day=trading_day,
            order_emt_id=fact.order_emt_id,
        )
        if order is None and fact.client_order_id is not None:
            order = self.order_by_client_id(
                account_id=account_id,
                trading_day=trading_day,
                client_order_id=fact.client_order_id,
            )
        event_prefix = f"callback:{account_id}:{trading_day}:{fact.kind.value}:{fact.source_key}"
        if order is None:
            self.require_reconciliation(
                event_id=f"{event_prefix}:unowned",
                account_id=account_id,
                trading_day=trading_day,
                reason="callback_for_unknown_emt_order",
            )
            return
        if order.order_emt_id is not None and order.order_emt_id != fact.order_emt_id:
            self.require_reconciliation(
                event_id=f"{event_prefix}:mapping-conflict",
                account_id=account_id,
                trading_day=trading_day,
                gateway_order_id=order.gateway_order_id,
                reason="callback_emt_order_id_conflicts_with_local_mapping",
            )
            return
        if fact.kind is LedgerCallbackKind.ORDER_UPDATE:
            if fact.state is None:
                raise OrderLedgerError("order callback fact is missing its normalized state")
            self.record_order_update(
                event_id=event_prefix,
                source_key=fact.source_key,
                account_id=account_id,
                trading_day=trading_day,
                gateway_order_id=order.gateway_order_id,
                order_emt_id=fact.order_emt_id,
                state=fact.state,
                leaves_qty=fact.leaves_qty,
                cancel_emt_id=fact.cancel_emt_id,
            )
            return
        if fact.kind is LedgerCallbackKind.TRADE:
            if (
                fact.market is None
                or fact.execution_key is None
                or fact.qty is None
                or fact.price is None
            ):
                raise OrderLedgerError("trade callback fact is incomplete")
            self.record_trade(
                event_id=event_prefix,
                execution_key=fact.execution_key,
                account_id=account_id,
                trading_day=trading_day,
                gateway_order_id=order.gateway_order_id,
                order_emt_id=fact.order_emt_id,
                market=fact.market,
                qty=fact.qty,
                price=fact.price,
            )
            return
        raise OrderLedgerError(f"unsupported callback fact kind: {fact.kind.value}")

    def append(
        self,
        *,
        kind: LedgerEventKind,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        payload: Mapping[str, Any],
        source_key: str | None = None,
        observed_ns: int | None = None,
    ) -> LedgerEvent:
        """Atomically append an immutable fact and apply its local projection."""

        if not isinstance(kind, LedgerEventKind):
            raise OrderLedgerError("kind must be a LedgerEventKind")
        event_id = _required_text(event_id, "event_id")
        account_id = _required_text(account_id, "account_id")
        trading_day = _required_text(trading_day, "trading_day")
        gateway_order_id = _required_text(gateway_order_id, "gateway_order_id")
        if source_key is not None:
            source_key = _required_text(source_key, "source_key")
        try:
            payload_json = json.dumps(
                dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise OrderLedgerError("event payload must be a JSON object") from exc
        timestamp = perf_counter_ns() if observed_ns is None else observed_ns
        if not isinstance(timestamp, int) or timestamp < 0:
            raise OrderLedgerError("observed_ns must be a non-negative integer")

        with self._lock:
            self._ensure_open()
            try:
                with self._connection.transaction():
                    # A local override keeps the acknowledgement boundary
                    # durable even if a broadly scoped PostgreSQL setting was
                    # changed to asynchronous commit for analytics traffic.
                    self._execute("SET LOCAL synchronous_commit = on")
                    # A database row lock, rather than a host-local mutex,
                    # serializes writers for this scope and makes the hash
                    # chain correct across process restarts and instances.
                    self._execute(
                        """
                        INSERT INTO ledger_heads(account_id, trading_day, event_hash)
                        VALUES (?, ?, ?)
                        ON CONFLICT(account_id, trading_day) DO NOTHING
                        """,
                        (account_id, trading_day, "0" * 64),
                    )
                    head = self._execute(
                        """
                        SELECT event_hash FROM ledger_heads
                        WHERE account_id = ? AND trading_day = ? FOR UPDATE
                        """,
                        (account_id, trading_day),
                    ).fetchone()
                    assert head is not None
                    existing = self._execute(
                        "SELECT * FROM ledger_events WHERE event_id = ?", (event_id,)
                    ).fetchone()
                    if existing is not None:
                        event = _event_from_row(existing)
                        if (
                            event.kind is not kind
                            or event.account_id != account_id
                            or event.trading_day != trading_day
                            or event.gateway_order_id != gateway_order_id
                            or event.source_key != source_key
                            or _canonical_json(event.payload) != payload_json
                        ):
                            raise OrderLedgerError(
                                "event_id was reused with conflicting order fact"
                            )
                        return event

                    previous_hash = str(head["event_hash"])
                    event_hash = _event_hash(
                        previous_hash=previous_hash,
                        event_id=event_id,
                        kind=kind,
                        account_id=account_id,
                        trading_day=trading_day,
                        gateway_order_id=gateway_order_id,
                        source_key=source_key,
                        payload_json=payload_json,
                        observed_ns=timestamp,
                    )
                    row = self._execute(
                        """
                        INSERT INTO ledger_events(
                            event_id, kind, account_id, trading_day, gateway_order_id,
                            source_key, payload_json, observed_ns, previous_hash, event_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        RETURNING *
                        """,
                        (
                            event_id,
                            kind.value,
                            account_id,
                            trading_day,
                            gateway_order_id,
                            source_key,
                            payload_json,
                            timestamp,
                            previous_hash,
                            event_hash,
                        ),
                    ).fetchone()
                    assert row is not None
                    event = _event_from_row(row)
                    self._apply_event(event)
                    self._execute(
                        """
                        UPDATE ledger_heads SET event_hash = ?
                        WHERE account_id = ? AND trading_day = ?
                        """,
                        (event_hash, account_id, trading_day),
                    )
                    return event
            except psycopg.Error as exc:
                raise OrderLedgerError("order ledger durable transaction failed") from exc

    def order(
        self,
        *,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
    ) -> OrderView | None:
        with self._lock:
            self._ensure_open()
            row = self._execute(
                """
                SELECT * FROM orders
                WHERE account_id = ? AND trading_day = ? AND gateway_order_id = ?
                """,
                (account_id, trading_day, gateway_order_id),
            ).fetchone()
            return None if row is None else _order_from_row(row)

    def order_by_emt_id(
        self,
        *,
        account_id: str,
        trading_day: str,
        order_emt_id: int,
    ) -> OrderView | None:
        with self._lock:
            self._ensure_open()
            row = self._execute(
                """
                SELECT * FROM orders
                WHERE account_id = ? AND trading_day = ? AND order_emt_id = ?
                """,
                (account_id, trading_day, order_emt_id),
            ).fetchone()
            return None if row is None else _order_from_row(row)

    def order_by_client_id(
        self,
        *,
        account_id: str,
        trading_day: str,
        client_order_id: str,
    ) -> OrderView | None:
        with self._lock:
            self._ensure_open()
            row = self._execute(
                """
                SELECT * FROM orders
                WHERE account_id = ? AND trading_day = ? AND client_order_id = ?
                """,
                (account_id, trading_day, _required_text(client_order_id, "client_order_id")),
            ).fetchone()
            return None if row is None else _order_from_row(row)

    def resolve_cancellable_order(
        self,
        *,
        account_id: str,
        trading_day: str,
        client_order_id: str,
    ) -> OrderView:
        """Resolve a locally proven EMT id without a per-order EMT query.

        A stale/reconciling book deliberately fails closed.  The caller can
        perform the controlled reconciliation workflow, but cannot guess an
        EMT id or turn a local cache miss into an unrestricted cancel request.
        """

        self.require_ready_for_new_orders(account_id=account_id, trading_day=trading_day)
        order = self.order_by_client_id(
            account_id=account_id,
            trading_day=trading_day,
            client_order_id=client_order_id,
        )
        if order is None:
            raise OrderLedgerError("client_order_id is absent from the local order ledger")
        if order.order_emt_id is None:
            raise OrderLedgerError("order has no proven EMT order id")
        if order.state in _TERMINAL_STATES or order.state in {
            OrderState.SUBMIT_UNKNOWN,
            OrderState.RECONCILE_REQUIRED,
        }:
            raise OrderLedgerError(f"order state {order.state.value} is not cancellable")
        return order

    def orders(self, *, account_id: str, trading_day: str) -> tuple[OrderView, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._execute(
                """
                SELECT * FROM orders WHERE account_id = ? AND trading_day = ?
                ORDER BY last_lsn, gateway_order_id
                """,
                (account_id, trading_day),
            ).fetchall()
            return tuple(_order_from_row(row) for row in rows)

    def book_status(self, *, account_id: str, trading_day: str) -> BookStatus:
        with self._lock:
            self._ensure_open()
            row = self._execute(
                "SELECT * FROM book_status WHERE account_id = ? AND trading_day = ?",
                (account_id, trading_day),
            ).fetchone()
            if row is None:
                return BookStatus(account_id, trading_day, BookHealth.UNINITIALIZED, 0, 0, None)
            return BookStatus(
                account_id=str(row["account_id"]),
                trading_day=str(row["trading_day"]),
                health=BookHealth(str(row["health"])),
                last_lsn=int(row["last_lsn"]),
                reconciliation_generation=int(row["reconciliation_generation"]),
                reason=str(row["reason"]) if row["reason"] is not None else None,
            )

    def require_ready_for_new_orders(self, *, account_id: str, trading_day: str) -> None:
        status = self.book_status(account_id=account_id, trading_day=trading_day)
        if status.health is not BookHealth.READY:
            raise OrderLedgerError(
                f"order book is {status.health.value}; reconciliation must complete before new orders"
            )

    def events(self, *, account_id: str, trading_day: str) -> tuple[LedgerEvent, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._execute(
                """
                SELECT * FROM ledger_events
                WHERE account_id = ? AND trading_day = ? ORDER BY lsn
                """,
                (account_id, trading_day),
            ).fetchall()
            return tuple(_event_from_row(row) for row in rows)

    def rebuild_projection(self) -> None:
        """Recreate materialized orders/trades/status only from immutable events."""

        with self._lock:
            self._ensure_open()
            try:
                with self._connection.transaction():
                    self._execute("SET LOCAL synchronous_commit = on")
                    self._execute("DELETE FROM trades")
                    self._execute("DELETE FROM orders")
                    self._execute("DELETE FROM book_status")
                    rows = self._execute("SELECT * FROM ledger_events ORDER BY lsn").fetchall()
                    for row in rows:
                        self._apply_event(_event_from_row(row))
            except psycopg.Error as exc:
                raise OrderLedgerError("order book projection rebuild failed") from exc

    def _order_event(
        self,
        kind: LedgerEventKind,
        event_id: str,
        account_id: str,
        trading_day: str,
        gateway_order_id: str,
        payload: Mapping[str, Any],
    ) -> LedgerEvent:
        return self.append(
            kind=kind,
            event_id=event_id,
            account_id=account_id,
            trading_day=trading_day,
            gateway_order_id=gateway_order_id,
            payload=payload,
        )

    def _apply_event(self, event: LedgerEvent) -> None:
        if event.kind is LedgerEventKind.RECONCILIATION_STARTED:
            prior = self.book_status(account_id=event.account_id, trading_day=event.trading_day)
            self._upsert_book_status(
                event,
                health=BookHealth.RECONCILING,
                generation=prior.reconciliation_generation + 1,
                reason=str(event.payload["reason"]),
            )
            return
        if event.kind is LedgerEventKind.RECONCILIATION_COMPLETED:
            prior = self.book_status(account_id=event.account_id, trading_day=event.trading_day)
            self._upsert_book_status(
                event,
                health=BookHealth.READY,
                generation=prior.reconciliation_generation,
                reason=None,
            )
            return
        if event.kind is LedgerEventKind.RECONCILE_REQUIRED:
            reason = str(event.payload["reason"])
            self._upsert_book_status(
                event,
                health=BookHealth.RECONCILE_REQUIRED,
                generation=self.book_status(
                    account_id=event.account_id, trading_day=event.trading_day
                ).reconciliation_generation,
                reason=reason,
            )
            if event.gateway_order_id != "__book__":
                self._update_order_state(event, OrderState.RECONCILE_REQUIRED)
            return
        if event.kind is LedgerEventKind.ORDER_INTENT:
            self._insert_intent(event)
            return
        if event.kind is LedgerEventKind.CALL_STARTED:
            self._update_order_state(event, OrderState.CALL_STARTED)
            return
        if event.kind is LedgerEventKind.SDK_ACCEPTED:
            self._accept_sdk(event)
            return
        if event.kind is LedgerEventKind.SDK_REJECTED:
            self._update_order_state(event, OrderState.REJECTED)
            return
        if event.kind is LedgerEventKind.SUBMIT_UNKNOWN:
            self._update_order_state(event, OrderState.SUBMIT_UNKNOWN)
            self._upsert_book_status(
                event,
                health=BookHealth.RECONCILE_REQUIRED,
                generation=self.book_status(
                    account_id=event.account_id, trading_day=event.trading_day
                ).reconciliation_generation,
                reason=str(event.payload["reason"]),
            )
            return
        if event.kind is LedgerEventKind.CANCEL_REQUESTED:
            self._update_order_state(event, OrderState.CANCEL_PENDING)
            return
        if event.kind is LedgerEventKind.CANCEL_ACCEPTED:
            self._update_order_fields(event, cancel_emt_id=event.payload.get("cancel_emt_id"))
            return
        if event.kind is LedgerEventKind.CANCEL_REJECTED:
            self._update_order_state(event, OrderState.LIVE)
            return
        if event.kind is LedgerEventKind.ORDER_UPDATE:
            self._apply_order_update(event)
            return
        if event.kind is LedgerEventKind.TRADE:
            self._apply_trade(event)
            return
        raise OrderLedgerError(f"unhandled ledger event kind: {event.kind.value}")

    def _insert_intent(self, event: LedgerEvent) -> None:
        payload = event.payload
        try:
            self._execute(
                """
                INSERT INTO orders(
                    account_id, trading_day, gateway_order_id, strategy_id,
                    strategy_order_id, client_order_id, ticker, market, side,
                    order_qty, limit_price, filled_qty, leaves_qty, state, last_lsn
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    event.account_id,
                    event.trading_day,
                    event.gateway_order_id,
                    payload["strategy_id"],
                    payload["strategy_order_id"],
                    payload["client_order_id"],
                    payload["ticker"],
                    payload["market"],
                    payload["side"],
                    payload["order_qty"],
                    payload["limit_price"],
                    payload["order_qty"],
                    OrderState.INTENT_DURABLE.value,
                    event.lsn,
                ),
            )
        except psycopg.IntegrityError as exc:
            raise OrderLedgerError("duplicate gateway, client, or strategy order identity") from exc

    def _accept_sdk(self, event: LedgerEvent) -> None:
        order_emt_id = event.payload.get("order_emt_id")
        if not isinstance(order_emt_id, int) or order_emt_id <= 0:
            raise OrderLedgerError("SDK_ACCEPTED requires a positive order_emt_id")
        current = self._required_order(event)
        existing_emt_id = current["order_emt_id"]
        if existing_emt_id is not None and int(existing_emt_id) != order_emt_id:
            self._mark_order_reconcile_required(
                event, "sdk_accepted_emt_id_conflicts_with_callback"
            )
            return
        if existing_emt_id is None:
            self._update_order_fields(event, order_emt_id=order_emt_id)
        # The native binding may invoke a callback on another thread before
        # InsertOrder returns.  A late SDK return confirms the mapping but must
        # never overwrite a newer LIVE / terminal exchange observation.
        if OrderState(str(current["state"])) in {
            OrderState.INTENT_DURABLE,
            OrderState.CALL_STARTED,
            OrderState.SDK_ACCEPTED,
        }:
            self._update_order_state(event, OrderState.SDK_ACCEPTED)

    def _apply_order_update(self, event: LedgerEvent) -> None:
        payload = event.payload
        incoming = OrderState(str(payload["state"]))
        self._update_order_fields(
            event,
            order_emt_id=payload.get("order_emt_id"),
            cancel_emt_id=payload.get("cancel_emt_id"),
            leaves_qty=payload.get("leaves_qty"),
        )
        current = self._required_order(event)
        current_state = OrderState(str(current["state"]))
        if current_state in _TERMINAL_STATES and incoming is not current_state:
            self._mark_order_reconcile_required(event, "conflicting_terminal_order_callback")
            return
        filled_qty = int(current["filled_qty"])
        if incoming is OrderState.LIVE and filled_qty > 0:
            incoming = OrderState.PART_FILLED
        self._update_order_state(event, incoming)

    def _apply_trade(self, event: LedgerEvent) -> None:
        payload = event.payload
        order = self._required_order(event)
        order_emt_id = payload.get("order_emt_id")
        if order["order_emt_id"] is not None and int(order["order_emt_id"]) != order_emt_id:
            self._mark_order_reconcile_required(event, "trade_order_emt_id_mismatch")
            return
        if order["order_emt_id"] is None:
            # A callback can race the return from InsertOrder.  Its EMT id is
            # still a durable vendor fact, so establish the mapping here; the
            # later SDK_ACCEPTED append is idempotent only when it agrees.
            self._update_order_fields(event, order_emt_id=order_emt_id)
            order = self._required_order(event)
        try:
            cursor = self._execute(
                """
                INSERT INTO trades(
                    account_id, trading_day, market, execution_key, gateway_order_id,
                    order_emt_id, qty, price, event_lsn
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, trading_day, market, execution_key) DO NOTHING
                """,
                (
                    event.account_id,
                    event.trading_day,
                    payload["market"],
                    payload["execution_key"],
                    event.gateway_order_id,
                    order_emt_id,
                    payload["qty"],
                    payload["price"],
                    event.lsn,
                ),
            )
        except psycopg.IntegrityError as exc:
            raise OrderLedgerError(
                "trade event conflicts with an existing execution identity"
            ) from exc
        if cursor.rowcount == 0:
            return
        filled_qty = int(order["filled_qty"]) + int(payload["qty"])
        order_qty = int(order["order_qty"]) if order["order_qty"] is not None else None
        if order_qty is not None and filled_qty > order_qty:
            self._mark_order_reconcile_required(event, "filled_qty_exceeds_order_qty")
            return
        leaves_qty = None if order_qty is None else order_qty - filled_qty
        prior_state = OrderState(str(order["state"]))
        if prior_state is OrderState.REJECTED:
            self._mark_order_reconcile_required(event, "trade_after_terminal_order_state")
            return
        # EMT documents that order and trade callbacks may arrive on separate
        # threads.  Therefore a final FILLED / CANCELED order event can be
        # observed before its last trade callback.  Keep the already-terminal
        # state (which safely blocks a later cancel) while still accounting for
        # the deduplicated trade.  Treating this ordinary cross-thread ordering
        # as a contradiction would unnecessarily stop the whole account.
        if prior_state in {OrderState.FILLED, OrderState.CANCELED}:
            next_state = prior_state
        else:
            next_state = OrderState.FILLED if leaves_qty == 0 else OrderState.PART_FILLED
        self._execute(
            """
            UPDATE orders SET filled_qty = ?, leaves_qty = ?, state = ?, last_lsn = ?
            WHERE account_id = ? AND trading_day = ? AND gateway_order_id = ?
            """,
            (
                filled_qty,
                leaves_qty,
                next_state.value,
                event.lsn,
                event.account_id,
                event.trading_day,
                event.gateway_order_id,
            ),
        )

    def _update_order_state(self, event: LedgerEvent, state: OrderState) -> None:
        self._required_order(event)
        self._execute(
            """
            UPDATE orders SET state = ?, last_lsn = ?
            WHERE account_id = ? AND trading_day = ? AND gateway_order_id = ?
            """,
            (state.value, event.lsn, event.account_id, event.trading_day, event.gateway_order_id),
        )

    def _update_order_fields(
        self,
        event: LedgerEvent,
        *,
        order_emt_id: object = None,
        cancel_emt_id: object = None,
        leaves_qty: object = None,
    ) -> None:
        self._required_order(event)
        assignments: list[str] = ["last_lsn = ?"]
        values: list[Any] = [event.lsn]
        if order_emt_id is not None:
            assignments.append("order_emt_id = ?")
            values.append(order_emt_id)
        if cancel_emt_id is not None:
            assignments.append("cancel_emt_id = ?")
            values.append(cancel_emt_id)
        if leaves_qty is not None:
            assignments.append("leaves_qty = ?")
            values.append(leaves_qty)
        values.extend((event.account_id, event.trading_day, event.gateway_order_id))
        try:
            self._execute(
                f"UPDATE orders SET {', '.join(assignments)} "
                "WHERE account_id = ? AND trading_day = ? AND gateway_order_id = ?",
                tuple(values),
            )
        except psycopg.IntegrityError as exc:
            raise OrderLedgerError("EMT id is already mapped to another local order") from exc

    def _mark_order_reconcile_required(self, event: LedgerEvent, reason: str) -> None:
        self._update_order_state(event, OrderState.RECONCILE_REQUIRED)
        self._upsert_book_status(
            event,
            health=BookHealth.RECONCILE_REQUIRED,
            generation=self.book_status(
                account_id=event.account_id, trading_day=event.trading_day
            ).reconciliation_generation,
            reason=reason,
        )

    def _required_order(self, event: LedgerEvent) -> Mapping[str, Any]:
        row = self._execute(
            """
            SELECT * FROM orders
            WHERE account_id = ? AND trading_day = ? AND gateway_order_id = ?
            """,
            (event.account_id, event.trading_day, event.gateway_order_id),
        ).fetchone()
        if row is None:
            raise OrderLedgerError("order event references an unknown gateway_order_id")
        return row

    def _upsert_book_status(
        self,
        event: LedgerEvent,
        *,
        health: BookHealth,
        generation: int,
        reason: str | None,
    ) -> None:
        self._execute(
            """
            INSERT INTO book_status(
                account_id, trading_day, health, last_lsn, reconciliation_generation, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, trading_day) DO UPDATE SET
                health = excluded.health,
                last_lsn = excluded.last_lsn,
                reconciliation_generation = excluded.reconciliation_generation,
                reason = excluded.reason
            """,
            (event.account_id, event.trading_day, health.value, event.lsn, generation, reason),
        )

    def _create_schema(self) -> None:
        quoted_schema = f'"{self.schema}"'
        try:
            with self._connection.transaction():
                self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
                self._connection.execute(f"SET LOCAL search_path TO {quoted_schema}")
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ledger_heads(
                        account_id TEXT NOT NULL,
                        trading_day TEXT NOT NULL,
                        event_hash TEXT NOT NULL,
                        PRIMARY KEY(account_id, trading_day)
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ledger_events(
                        lsn BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE,
                        kind TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        trading_day TEXT NOT NULL,
                        gateway_order_id TEXT NOT NULL,
                        source_key TEXT,
                        payload_json TEXT NOT NULL,
                        observed_ns BIGINT NOT NULL,
                        previous_hash TEXT NOT NULL,
                        event_hash TEXT NOT NULL UNIQUE
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS ledger_events_scope_lsn
                    ON ledger_events(account_id, trading_day, lsn)
                    """
                )
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orders(
                        account_id TEXT NOT NULL,
                        trading_day TEXT NOT NULL,
                        gateway_order_id TEXT NOT NULL,
                        strategy_id TEXT,
                        strategy_order_id TEXT,
                        client_order_id TEXT,
                        order_emt_id BIGINT,
                        cancel_emt_id BIGINT,
                        ticker TEXT,
                        market TEXT,
                        side TEXT,
                        order_qty BIGINT,
                        limit_price TEXT,
                        filled_qty BIGINT NOT NULL,
                        leaves_qty BIGINT,
                        state TEXT NOT NULL,
                        last_lsn BIGINT NOT NULL,
                        PRIMARY KEY(account_id, trading_day, gateway_order_id),
                        UNIQUE(account_id, trading_day, client_order_id),
                        UNIQUE(account_id, trading_day, order_emt_id)
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades(
                        account_id TEXT NOT NULL,
                        trading_day TEXT NOT NULL,
                        market TEXT NOT NULL,
                        execution_key TEXT NOT NULL,
                        gateway_order_id TEXT NOT NULL,
                        order_emt_id BIGINT NOT NULL,
                        qty BIGINT NOT NULL,
                        price TEXT NOT NULL,
                        event_lsn BIGINT NOT NULL,
                        PRIMARY KEY(account_id, trading_day, market, execution_key)
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS book_status(
                        account_id TEXT NOT NULL,
                        trading_day TEXT NOT NULL,
                        health TEXT NOT NULL,
                        last_lsn BIGINT NOT NULL,
                        reconciliation_generation BIGINT NOT NULL,
                        reason TEXT,
                        PRIMARY KEY(account_id, trading_day)
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE OR REPLACE FUNCTION forbid_ledger_events_mutation()
                    RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE EXCEPTION 'ledger_events are immutable';
                    END;
                    $$
                    """
                )
                self._connection.execute(
                    "DROP TRIGGER IF EXISTS ledger_events_no_mutation ON ledger_events"
                )
                self._connection.execute(
                    """
                    CREATE TRIGGER ledger_events_no_mutation
                    BEFORE UPDATE OR DELETE ON ledger_events
                    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_events_mutation()
                    """
                )
            # ``SET LOCAL`` above intentionally ends with the schema creation
            # transaction.  Keep the same search path for every later query on
            # this connection; the validated identifier prevents injection.
            self._connection.execute(f"SET search_path TO {quoted_schema}")
        except psycopg.Error as exc:
            raise OrderLedgerError("cannot initialize PostgreSQL order ledger schema") from exc

    def _ensure_open(self) -> None:
        if self._closed:
            raise OrderLedgerError("order ledger is closed")

    def _execute(self, statement: str, params: tuple[Any, ...] = ()) -> Any:
        """Execute SQL using the module's historical qmark call sites.

        PostgreSQL/psycopg uses ``%s`` bind placeholders.  Centralizing this
        translation keeps all values parameterized while making the storage
        migration auditable: no business value is interpolated into SQL.
        """

        return self._connection.execute(statement.replace("?", "%s"), params)


class AsyncPostgresOrderLedger:
    """Non-blocking PostgreSQL projection writer.

    The gateway only performs a bounded ``put_nowait`` of small, normalized
    facts.  This object owns all PostgreSQL connections, reads, writes,
    retries and backoff on a daemon worker.  A full queue or a database outage
    is observable through :meth:`status`, but never changes a gateway command
    result, callback ACK, or native EMT call.

    Its data is an acceleration/read-model copy.  The existing command/event
    journals remain the synchronous source of truth and recovery authority.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "emt_gateway",
        capacity: int = 65_536,
        reconnect_delay_seconds: float = 0.050,
        max_reconnect_delay_seconds: float = 5.0,
        connect_timeout_seconds: int = 1,
    ) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("PostgreSQL DSN must be a non-blank string")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise ValueError("PostgreSQL schema must be a simple SQL identifier")
        if capacity <= 0:
            raise ValueError("async ledger capacity must be positive")
        if reconnect_delay_seconds <= 0 or max_reconnect_delay_seconds < reconnect_delay_seconds:
            raise ValueError("invalid async ledger reconnect delays")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        self._dsn = dsn
        self._schema = schema
        self._queue: Queue[_LedgerWorkItem] = Queue(maxsize=capacity)
        self._stop = Event()
        self._condition = Condition()
        self._queued = 0
        self._inflight = 0
        self._persisted = 0
        self._dropped = 0
        self._failures = 0
        self._last_error: str | None = None
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self._connect_timeout_seconds = connect_timeout_seconds
        self._worker = Thread(target=self._run, name="emt-pg-ledger", daemon=True)
        self._worker.start()

    def close(self, *, join_seconds: float = 1.0) -> None:
        """Request worker shutdown; never called from a trading hot path."""

        if join_seconds < 0:
            raise ValueError("join_seconds must be non-negative")
        self._stop.set()
        self._worker.join(join_seconds)

    def status(self) -> AsyncLedgerStatus:
        with self._condition:
            return AsyncLedgerStatus(
                queued=self._queued,
                inflight=self._inflight,
                persisted=self._persisted,
                dropped=self._dropped,
                failures=self._failures,
                last_error=self._last_error,
            )

    def wait_until_idle(self, timeout_seconds: float) -> bool:
        """Control/test helper; it is intentionally not used by the gateway."""

        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        deadline = monotonic() + timeout_seconds
        with self._condition:
            while self._queued or self._inflight:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def begin_reconciliation(self, **kwargs: Any) -> bool:
        return self._enqueue("begin_reconciliation", kwargs)

    def complete_reconciliation(self, **kwargs: Any) -> bool:
        return self._enqueue("complete_reconciliation", kwargs)

    def require_reconciliation(self, **kwargs: Any) -> bool:
        return self._enqueue("require_reconciliation", kwargs)

    def record_intent(self, **kwargs: Any) -> bool:
        return self._enqueue("record_intent", kwargs)

    def record_call_started(self, **kwargs: Any) -> bool:
        return self._enqueue("record_call_started", kwargs)

    def record_sdk_accepted(self, **kwargs: Any) -> bool:
        return self._enqueue("record_sdk_accepted", kwargs)

    def record_sdk_rejected(self, **kwargs: Any) -> bool:
        return self._enqueue("record_sdk_rejected", kwargs)

    def record_submit_unknown(self, **kwargs: Any) -> bool:
        return self._enqueue("record_submit_unknown", kwargs)

    def record_cancel_requested(self, **kwargs: Any) -> bool:
        return self._enqueue("record_cancel_requested", kwargs)

    def record_cancel_accepted(self, **kwargs: Any) -> bool:
        return self._enqueue("record_cancel_accepted", kwargs)

    def record_cancel_rejected(self, **kwargs: Any) -> bool:
        return self._enqueue("record_cancel_rejected", kwargs)

    def record_callback_fact(self, **kwargs: Any) -> bool:
        return self._enqueue("record_callback_fact", kwargs)

    def _enqueue(self, operation: str, kwargs: Mapping[str, Any]) -> bool:
        """The sole gateway-facing method: bounded, immediate and no I/O."""

        # Publish the counter before the queue item.  The worker may run as
        # soon as ``put_nowait`` returns, so incrementing afterwards could
        # transiently make the in-flight accounting negative.
        with self._condition:
            self._queued += 1
        try:
            self._queue.put_nowait(_LedgerWorkItem(operation, dict(kwargs)))
        except Full:
            with self._condition:
                self._queued -= 1
                self._dropped += 1
                self._last_error = "queue_full"
                self._condition.notify_all()
            return False
        except Exception:
            # A projection defect cannot be permitted to affect an EMT command
            # or callback path.  Keep only a class name, never arbitrary input
            # or a DSN/error string in the observable status.
            with self._condition:
                self._queued -= 1
                self._dropped += 1
                self._last_error = "enqueue_failure"
                self._condition.notify_all()
            return False
        with self._condition:
            self._condition.notify_all()
        return True

    def _run(self) -> None:
        ledger: PostgresOrderLedger | None = None
        item: _LedgerWorkItem | None = None
        delay = self._reconnect_delay_seconds
        try:
            while not self._stop.is_set():
                if item is None:
                    try:
                        item = self._queue.get(timeout=0.050)
                    except Empty:
                        continue
                    with self._condition:
                        self._queued -= 1
                        self._inflight += 1
                        self._condition.notify_all()
                try:
                    if ledger is None:
                        ledger = PostgresOrderLedger(
                            self._dsn,
                            schema=self._schema,
                            connect_timeout_seconds=self._connect_timeout_seconds,
                        )
                    operation = getattr(ledger, item.operation)
                    kwargs = item.kwargs
                    if item.operation == "record_intent":
                        # A command journal record has already authorized the
                        # native boundary. PG book health is only diagnostic
                        # here and cannot retroactively reject that command.
                        kwargs = {**kwargs, "projection_only": True}
                    operation(**kwargs)
                except Exception as exc:
                    if ledger is not None:
                        ledger.close()
                        ledger = None
                    with self._condition:
                        self._failures += 1
                        self._last_error = type(exc).__name__
                        self._condition.notify_all()
                    # Only this background worker backs off.  The caller that
                    # enqueued the fact has already continued its gateway path.
                    self._stop.wait(delay)
                    delay = min(delay * 2, self._max_reconnect_delay_seconds)
                    continue
                with self._condition:
                    self._inflight -= 1
                    self._persisted += 1
                    self._last_error = None
                    self._condition.notify_all()
                item = None
                delay = self._reconnect_delay_seconds
        finally:
            if ledger is not None:
                ledger.close()
            if item is not None:
                with self._condition:
                    self._inflight -= 1
                    self._condition.notify_all()


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OrderLedgerError(f"{name} must be a non-blank string")
    return value


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _event_hash(
    *,
    previous_hash: str,
    event_id: str,
    kind: LedgerEventKind,
    account_id: str,
    trading_day: str,
    gateway_order_id: str,
    source_key: str | None,
    payload_json: str,
    observed_ns: int,
) -> str:
    material = _canonical_json(
        {
            "previous_hash": previous_hash,
            "event_id": event_id,
            "kind": kind.value,
            "account_id": account_id,
            "trading_day": trading_day,
            "gateway_order_id": gateway_order_id,
            "source_key": source_key,
            "payload": json.loads(payload_json),
            "observed_ns": observed_ns,
        }
    )
    return sha256(material.encode("utf-8")).hexdigest()


def _event_from_row(row: Mapping[str, Any]) -> LedgerEvent:
    return LedgerEvent(
        lsn=int(row["lsn"]),
        event_id=str(row["event_id"]),
        kind=LedgerEventKind(str(row["kind"])),
        account_id=str(row["account_id"]),
        trading_day=str(row["trading_day"]),
        gateway_order_id=str(row["gateway_order_id"]),
        source_key=str(row["source_key"]) if row["source_key"] is not None else None,
        payload=json.loads(str(row["payload_json"])),
        observed_ns=int(row["observed_ns"]),
        previous_hash=str(row["previous_hash"]),
        event_hash=str(row["event_hash"]),
    )


def _order_from_row(row: Mapping[str, Any]) -> OrderView:
    return OrderView(
        account_id=str(row["account_id"]),
        trading_day=str(row["trading_day"]),
        gateway_order_id=str(row["gateway_order_id"]),
        strategy_id=str(row["strategy_id"]) if row["strategy_id"] is not None else None,
        strategy_order_id=str(row["strategy_order_id"])
        if row["strategy_order_id"] is not None
        else None,
        client_order_id=str(row["client_order_id"]) if row["client_order_id"] is not None else None,
        order_emt_id=int(row["order_emt_id"]) if row["order_emt_id"] is not None else None,
        cancel_emt_id=int(row["cancel_emt_id"]) if row["cancel_emt_id"] is not None else None,
        ticker=str(row["ticker"]) if row["ticker"] is not None else None,
        market=str(row["market"]) if row["market"] is not None else None,
        side=str(row["side"]) if row["side"] is not None else None,
        order_qty=int(row["order_qty"]) if row["order_qty"] is not None else None,
        limit_price=str(row["limit_price"]) if row["limit_price"] is not None else None,
        filled_qty=int(row["filled_qty"]),
        leaves_qty=int(row["leaves_qty"]) if row["leaves_qty"] is not None else None,
        state=OrderState(str(row["state"])),
        last_lsn=int(row["last_lsn"]),
    )
