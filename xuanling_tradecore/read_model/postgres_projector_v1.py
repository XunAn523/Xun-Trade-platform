"""PostgreSQL implementation of F3's Journal-first projection boundary.

This module is deliberately import-safe: opening a connection is an explicit
``apply`` action performed by a background projector only.  It contains no
Adapter calls, queue admission, broker routing or execution authority.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, cast
from uuid import UUID

from .projection_v1 import (
    APPEND_TABLES,
    PRIMARY_KEYS,
    SNAPSHOT_SCOPE_KEYS,
    SNAPSHOT_TABLES,
    ReadModelCheckpointV1,
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
    ReadModelRowMutationV1,
    ReadModelSnapshotReplacementV1,
    ReadModelTableV1,
)


class PostgresReadModelProjectionErrorV1(ReadModelProjectionErrorV1):
    """The PostgreSQL read model could not prove a safe projection commit."""


class _CursorV1(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...


class _ConnectionV1(Protocol):
    def __enter__(self) -> _ConnectionV1: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

    def execute(self, sql: str, parameters: tuple[object, ...] | None = None) -> _CursorV1: ...

    def transaction(self) -> AbstractContextManager[object]: ...


ConnectionFactoryV1 = Callable[[str], _ConnectionV1]


@dataclass(frozen=True)
class ReadModelProjectionCommitV1:
    """Payload-free result from a single committed or idempotent replay batch."""

    checkpoint: ReadModelCheckpointV1
    inserted_or_updated_rows: int
    replayed: bool = False


_SCHEMA = "xuanling_tradecore_rm"
_CHECKPOINT_TABLE = "projection_checkpoints"
_TABLE_COLUMNS: Mapping[ReadModelTableV1, tuple[str, ...]] = {
    ReadModelTableV1.ACCOUNTS: (
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "consumer_id",
        "account_display_name",
        "venue_account_masked",
        "trading_day",
        "trading_day_epoch",
        "lifecycle",
        "reconciliation_status",
        "reconciliation_reason_code",
        "active_gate_count",
        "can_submit",
        "submit_block_reason_code",
        "last_recovery_run_id",
        "capability_hash",
        "revision",
        "source_fact_hash",
        "updated_at",
        "projected_at",
    ),
    ReadModelTableV1.STRATEGIES: (
        "strategy_id",
        "consumer_id",
        "strategy_code",
        "display_name",
        "strategy_kind",
        "status",
        "revision",
        "source_fact_hash",
        "updated_at",
        "projected_at",
    ),
    ReadModelTableV1.ORDERS: (
        "oms_order_id",
        "consumer_id",
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "strategy_id",
        "strategy_order_id",
        "trader_id",
        "origin",
        "trading_day",
        "trading_day_epoch",
        "market",
        "instrument_id",
        "side",
        "position_effect",
        "order_type",
        "time_in_force",
        "currency",
        "order_qty",
        "limit_price",
        "filled_qty",
        "leaves_qty",
        "avg_fill_price",
        "status",
        "risk_result",
        "risk_reason_code",
        "decision_context_hash",
        "business_remark",
        "intent_hash",
        "revision",
        "source_fact_hash",
        "created_at",
        "updated_at",
        "terminal_at",
        "projected_at",
    ),
    ReadModelTableV1.ORDER_EXECUTIONS: (
        "execution_id",
        "oms_order_id",
        "child_no",
        "parent_execution_id",
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "trading_day",
        "trading_day_epoch",
        "execution_qty",
        "execution_price",
        "filled_qty",
        "execution_status",
        "dispatch_phase",
        "native_outcome",
        "adapter_client_scope_digest",
        "client_order_ref_kind",
        "client_order_ref",
        "client_order_ref_digest",
        "venue_order_id",
        "venue_order_id_digest",
        "broker_system_id",
        "broker_strategy_id",
        "broker_trader_id",
        "broker_custom_ref",
        "broker_custom_ref_chars",
        "broker_custom_ref_bytes",
        "broker_custom_ref_max_chars",
        "broker_custom_ref_max_bytes",
        "broker_custom_ref_encoding",
        "broker_custom_ref_normalization_version",
        "broker_custom_field_manifest_hash",
        "revision",
        "source_fact_hash",
        "created_at",
        "updated_at",
        "projected_at",
    ),
    ReadModelTableV1.ORDER_ACTIONS: (
        "action_id",
        "oms_order_id",
        "execution_id",
        "trading_day",
        "action_type",
        "caller_action_key",
        "action_status",
        "dispatch_phase",
        "reason_code",
        "operator_id",
        "venue_action_id",
        "error_code",
        "error_message",
        "requested_at",
        "completed_at",
        "revision",
        "source_fact_hash",
        "projected_at",
    ),
    ReadModelTableV1.TRADES: (
        "trade_id",
        "oms_order_id",
        "execution_id",
        "consumer_id",
        "strategy_id",
        "trader_id",
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "trading_day",
        "trading_day_epoch",
        "trade_key",
        "venue_trade_id",
        "venue_trade_id_digest",
        "market",
        "instrument_id",
        "side",
        "position_effect",
        "fill_qty",
        "fill_price",
        "fill_amount",
        "fee_amount",
        "currency",
        "trade_at",
        "received_at",
        "revision",
        "source_fact_hash",
        "projected_at",
    ),
    ReadModelTableV1.ASSETS: (
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "trading_day",
        "trading_day_epoch",
        "snapshot_generation",
        "currency",
        "total_asset",
        "net_asset",
        "cash_balance",
        "available_cash",
        "frozen_cash",
        "market_value",
        "margin_used",
        "total_debt",
        "credit_limit",
        "collateral_ratio",
        "realized_pnl",
        "unrealized_pnl",
        "total_fee",
        "as_of_at",
        "revision",
        "source_fact_hash",
        "updated_at",
        "projected_at",
    ),
    ReadModelTableV1.POSITIONS: (
        "account_id",
        "venue_id",
        "adapter_instance_id",
        "trading_day",
        "trading_day_epoch",
        "snapshot_generation",
        "market",
        "instrument_id",
        "direction",
        "total_qty",
        "available_qty",
        "frozen_qty",
        "today_qty",
        "yesterday_qty",
        "avg_open_price",
        "cost_price",
        "last_price",
        "market_value",
        "realized_pnl",
        "unrealized_pnl",
        "as_of_at",
        "revision",
        "source_fact_hash",
        "updated_at",
        "projected_at",
    ),
}


def _default_connect(dsn: str) -> _ConnectionV1:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - runtime dependency is declared.
        raise PostgresReadModelProjectionErrorV1(
            "psycopg is unavailable for F3 projection"
        ) from exc
    return cast(_ConnectionV1, psycopg.connect(dsn))


class PostgresReadModelProjectionStoreV1:
    """Commit a validated batch and its checkpoint in one PostgreSQL transaction."""

    def __init__(self, *, dsn: str, connect: ConnectionFactoryV1 = _default_connect) -> None:
        if not dsn.strip():
            raise PostgresReadModelProjectionErrorV1("read-model projector DSN is invalid")
        self._dsn = dsn
        self._connect = connect

    def apply(self, batch: ReadModelProjectionBatchV1) -> ReadModelProjectionCommitV1:
        """Apply a contiguous, reader-validated fact batch without execution side effects."""

        try:
            with self._connect(self._dsn) as connection:
                with connection.transaction():
                    replayed = self._guard_checkpoint(connection, batch)
                    if replayed:
                        return ReadModelProjectionCommitV1(batch.checkpoint, 0, replayed=True)
                    changed = sum(
                        self._apply_mutation(connection, item) for item in batch.mutations
                    )
                    changed += sum(
                        self._replace_snapshot(connection, item) for item in batch.snapshots
                    )
                    self._write_checkpoint(connection, batch.checkpoint)
                    return ReadModelProjectionCommitV1(batch.checkpoint, changed)
        except ReadModelProjectionErrorV1:
            raise
        except Exception as exc:
            raise PostgresReadModelProjectionErrorV1(
                "read-model projection transaction failed"
            ) from exc

    @staticmethod
    def _guard_checkpoint(connection: _ConnectionV1, batch: ReadModelProjectionBatchV1) -> bool:
        key = batch.checkpoint.key
        row = connection.execute(
            f"SELECT journal_id, stream_generation, redis_ms, redis_seq, fact_ordinal, source_fact_hash "
            f"FROM {_SCHEMA}.{_CHECKPOINT_TABLE} "
            "WHERE projector_name = %s AND shard_id = %s AND account_scope = %s FOR UPDATE",
            (key.projector_name, key.shard_id, key.account_scope),
        ).fetchone()
        if row is None:
            if batch.expected_checkpoint is not None:
                raise PostgresReadModelProjectionErrorV1(
                    "read-model checkpoint predecessor differs"
                )
            return False
        current_position = ReadModelPositionV1(
            journal_id=cast(UUID, row[0]),
            stream_generation=int(cast(int, row[1])),
            redis_ms=int(cast(int, row[2])),
            redis_seq=int(cast(int, row[3])),
            fact_ordinal=int(cast(int, row[4])),
        )
        current_hash = str(row[5])
        if current_position == batch.checkpoint.position:
            if current_hash == batch.checkpoint.source_fact_hash:
                return True
            raise PostgresReadModelProjectionErrorV1(
                "read-model checkpoint position has a conflicting source hash"
            )
        if current_position > batch.checkpoint.position:
            raise PostgresReadModelProjectionErrorV1("read-model checkpoint moves backward")
        if current_position != batch.expected_checkpoint:
            raise PostgresReadModelProjectionErrorV1("read-model checkpoint predecessor differs")
        return False

    @staticmethod
    def _apply_mutation(
        connection: _ConnectionV1, mutation: ReadModelRowMutationV1, *, snapshot_row: bool = False
    ) -> int:
        if mutation.table in SNAPSHOT_TABLES and not snapshot_row:
            raise PostgresReadModelProjectionErrorV1("snapshot rows require atomic replacement")
        columns = _columns_for(mutation.table, mutation.values)
        parameters = tuple(mutation.values[column] for column in columns)
        primary_keys = PRIMARY_KEYS[mutation.table]
        assignments = tuple(column for column in columns if column not in primary_keys)
        if mutation.table in APPEND_TABLES:
            return _insert_append_only(connection, mutation.table, columns, parameters, mutation)
        update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in assignments)
        sql = (
            f"INSERT INTO {_SCHEMA}.{mutation.table.value} ({', '.join(columns)}) "
            f"VALUES ({', '.join('%s' for _ in columns)}) "
            f"ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {update_sql} "
            f"WHERE {_SCHEMA}.{mutation.table.value}.revision < EXCLUDED.revision "
            "RETURNING revision, source_fact_hash"
        )
        updated = connection.execute(sql, parameters).fetchone()
        if updated is not None:
            return 1
        return _validate_existing_identity(connection, mutation)

    @staticmethod
    def _replace_snapshot(
        connection: _ConnectionV1, replacement: ReadModelSnapshotReplacementV1
    ) -> int:
        scope_columns = SNAPSHOT_SCOPE_KEYS[replacement.table]
        scope_values = tuple(replacement.scope[column] for column in scope_columns)
        rows = connection.execute(
            f"SELECT snapshot_generation FROM {_SCHEMA}.{replacement.table.value} "
            f"WHERE {' AND '.join(f'{column} = %s' for column in scope_columns)} FOR UPDATE",
            scope_values,
        ).fetchall()
        current_generation = max((int(cast(int, row[0])) for row in rows), default=0)
        if current_generation >= replacement.snapshot_generation:
            raise PostgresReadModelProjectionErrorV1("read-model snapshot generation is not newer")
        changed = sum(
            PostgresReadModelProjectionStoreV1._apply_mutation(connection, item, snapshot_row=True)
            for item in replacement.rows
        )
        predicates = " AND ".join(f"{column} = %s" for column in scope_columns)
        connection.execute(
            f"DELETE FROM {_SCHEMA}.{replacement.table.value} "
            f"WHERE {predicates} AND snapshot_generation <> %s",
            (*scope_values, replacement.snapshot_generation),
        )
        return changed

    @staticmethod
    def _write_checkpoint(connection: _ConnectionV1, checkpoint: ReadModelCheckpointV1) -> None:
        position = checkpoint.position
        key = checkpoint.key
        connection.execute(
            f"INSERT INTO {_SCHEMA}.{_CHECKPOINT_TABLE} ("
            "projector_name, shard_id, account_scope, projector_version, journal_id, stream_generation, "
            "redis_ms, redis_seq, fact_ordinal, source_fact_hash, status, lag_records, lag_milliseconds, "
            "last_error_code, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp()) "
            "ON CONFLICT (projector_name, shard_id, account_scope) DO UPDATE SET "
            "projector_version = EXCLUDED.projector_version, journal_id = EXCLUDED.journal_id, "
            "stream_generation = EXCLUDED.stream_generation, redis_ms = EXCLUDED.redis_ms, "
            "redis_seq = EXCLUDED.redis_seq, fact_ordinal = EXCLUDED.fact_ordinal, "
            "source_fact_hash = EXCLUDED.source_fact_hash, status = EXCLUDED.status, "
            "lag_records = EXCLUDED.lag_records, lag_milliseconds = EXCLUDED.lag_milliseconds, "
            "last_error_code = EXCLUDED.last_error_code, updated_at = EXCLUDED.updated_at",
            (
                key.projector_name,
                key.shard_id,
                key.account_scope,
                checkpoint.projector_version,
                position.journal_id,
                position.stream_generation,
                position.redis_ms,
                position.redis_seq,
                position.fact_ordinal,
                checkpoint.source_fact_hash,
                checkpoint.status,
                checkpoint.lag_records,
                checkpoint.lag_milliseconds,
                checkpoint.last_error_code,
            ),
        )


def _columns_for(table: ReadModelTableV1, values: Mapping[str, object]) -> tuple[str, ...]:
    allowed = _TABLE_COLUMNS[table]
    unknown = set(values).difference(allowed)
    if unknown:
        raise PostgresReadModelProjectionErrorV1("read-model row has an unknown table column")
    return tuple(column for column in allowed if column in values)


def _insert_append_only(
    connection: _ConnectionV1,
    table: ReadModelTableV1,
    columns: tuple[str, ...],
    parameters: tuple[object, ...],
    mutation: ReadModelRowMutationV1,
) -> int:
    primary_keys = PRIMARY_KEYS[table]
    inserted = connection.execute(
        f"INSERT INTO {_SCHEMA}.{table.value} ({', '.join(columns)}) "
        f"VALUES ({', '.join('%s' for _ in columns)}) "
        f"ON CONFLICT ({', '.join(primary_keys)}) DO NOTHING RETURNING source_fact_hash",
        parameters,
    ).fetchone()
    if inserted is not None:
        return 1
    return _validate_existing_identity(connection, mutation)


def _validate_existing_identity(connection: _ConnectionV1, mutation: ReadModelRowMutationV1) -> int:
    primary_keys = PRIMARY_KEYS[mutation.table]
    row = connection.execute(
        f"SELECT revision, source_fact_hash FROM {_SCHEMA}.{mutation.table.value} "
        f"WHERE {' AND '.join(f'{column} = %s' for column in primary_keys)}",
        tuple(mutation.values[column] for column in primary_keys),
    ).fetchone()
    if row is None:
        raise PostgresReadModelProjectionErrorV1("read-model row conflict has no primary-key row")
    current_revision, current_hash = int(cast(int, row[0])), str(row[1])
    incoming_revision = cast(int, mutation.values["revision"])
    incoming_hash = cast(str, mutation.values["source_fact_hash"])
    if current_revision == incoming_revision and current_hash != incoming_hash:
        raise PostgresReadModelProjectionErrorV1(
            "read-model same revision has conflicting source hash"
        )
    return 0
