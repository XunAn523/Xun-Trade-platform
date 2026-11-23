"""Journal-first, transactional F3 read-model projection contracts.

The contract intentionally accepts only already-committed facts supplied by a
reader.  It cannot call an Adapter, resolve a route, enqueue an execution, or
change an account admission gate.  Both the deterministic in-memory store and
the PostgreSQL implementation use the same revision/checkpoint semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Mapping, cast
from uuid import UUID


class ReadModelProjectionErrorV1(RuntimeError):
    """A fact cannot advance the non-authoritative F3 read model safely."""


class ReadModelTableV1(str, Enum):
    ACCOUNTS = "accounts"
    STRATEGIES = "strategies"
    ORDERS = "orders"
    ORDER_EXECUTIONS = "order_executions"
    ORDER_ACTIONS = "order_actions"
    TRADES = "trades"
    ASSETS = "assets"
    POSITIONS = "positions"


STATE_TABLES = frozenset(
    {
        ReadModelTableV1.ACCOUNTS,
        ReadModelTableV1.STRATEGIES,
        ReadModelTableV1.ORDERS,
        ReadModelTableV1.ORDER_EXECUTIONS,
    }
)
APPEND_TABLES = frozenset({ReadModelTableV1.ORDER_ACTIONS, ReadModelTableV1.TRADES})
SNAPSHOT_TABLES = frozenset({ReadModelTableV1.ASSETS, ReadModelTableV1.POSITIONS})
PRIMARY_KEYS: Mapping[ReadModelTableV1, tuple[str, ...]] = MappingProxyType(
    {
        ReadModelTableV1.ACCOUNTS: ("account_id", "venue_id", "adapter_instance_id"),
        ReadModelTableV1.STRATEGIES: ("strategy_id",),
        ReadModelTableV1.ORDERS: ("oms_order_id",),
        ReadModelTableV1.ORDER_EXECUTIONS: ("execution_id",),
        ReadModelTableV1.ORDER_ACTIONS: ("action_id",),
        ReadModelTableV1.TRADES: ("trade_id",),
        ReadModelTableV1.ASSETS: (
            "account_id",
            "venue_id",
            "adapter_instance_id",
            "trading_day_epoch",
            "currency",
        ),
        ReadModelTableV1.POSITIONS: (
            "account_id",
            "venue_id",
            "adapter_instance_id",
            "trading_day_epoch",
            "market",
            "instrument_id",
            "direction",
        ),
    }
)
SNAPSHOT_SCOPE_KEYS: Mapping[ReadModelTableV1, tuple[str, ...]] = MappingProxyType(
    {
        ReadModelTableV1.ASSETS: (
            "account_id",
            "venue_id",
            "adapter_instance_id",
            "trading_day_epoch",
        ),
        ReadModelTableV1.POSITIONS: (
            "account_id",
            "venue_id",
            "adapter_instance_id",
            "trading_day_epoch",
        ),
    }
)


def _text(value: object, label: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ReadModelProjectionErrorV1(f"{label} is invalid")
    return value


def _sha256(value: object, label: str) -> str:
    value = _text(value, label, maximum=64)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ReadModelProjectionErrorV1(f"{label} must be a lower-case SHA-256")
    return value


def _freeze_values(values: Mapping[str, object], label: str) -> Mapping[str, object]:
    frozen = dict(values)
    if not frozen:
        raise ReadModelProjectionErrorV1(f"{label} is empty")
    for key, value in frozen.items():
        _text(key, f"{label} column", maximum=64)
        # PostgreSQL ``numeric`` values are intentionally Decimal at this
        # boundary.  Accepting binary float would make replay hashes and
        # monetary values platform-dependent.
        if not isinstance(value, (str, int, bool, Decimal, UUID, date, datetime, type(None))):
            raise ReadModelProjectionErrorV1(f"{label} contains a non-SQL scalar")
    return MappingProxyType(frozen)


@dataclass(frozen=True, order=True)
class ReadModelPositionV1:
    """One numeric Journal position after a reader has proven continuity."""

    journal_id: UUID
    stream_generation: int
    redis_ms: int
    redis_seq: int
    fact_ordinal: int

    def __post_init__(self) -> None:
        if (
            self.stream_generation < 0
            or self.redis_ms < 0
            or self.redis_seq < 0
            or self.fact_ordinal < 0
        ):
            raise ReadModelProjectionErrorV1("read-model Journal position is invalid")


@dataclass(frozen=True)
class ReadModelCheckpointKeyV1:
    projector_name: str
    shard_id: str
    account_scope: str

    def __post_init__(self) -> None:
        _text(self.projector_name, "read-model projector name", maximum=128)
        _text(self.shard_id, "read-model shard id", maximum=64)
        _text(self.account_scope, "read-model account scope")


@dataclass(frozen=True)
class ReadModelCheckpointV1:
    """The only checkpoint state that can advance after an entire batch commits."""

    key: ReadModelCheckpointKeyV1
    projector_version: str
    position: ReadModelPositionV1
    source_fact_hash: str
    status: str = "RUNNING"
    lag_records: int = 0
    lag_milliseconds: int = 0
    last_error_code: str | None = None

    def __post_init__(self) -> None:
        _text(self.projector_version, "read-model projector version", maximum=64)
        _sha256(self.source_fact_hash, "read-model checkpoint source hash")
        if self.status not in {"RUNNING", "PAUSED", "ERROR", "REBUILDING"}:
            raise ReadModelProjectionErrorV1("read-model checkpoint status is invalid")
        if self.lag_records < 0 or self.lag_milliseconds < 0:
            raise ReadModelProjectionErrorV1("read-model checkpoint lag is invalid")
        if self.last_error_code is not None:
            _text(self.last_error_code, "read-model checkpoint error code", maximum=64)


@dataclass(frozen=True)
class ReadModelRowMutationV1:
    """One validated row operation with no executable SQL supplied by callers."""

    table: ReadModelTableV1
    values: Mapping[str, object]

    def __post_init__(self) -> None:
        values = _freeze_values(self.values, "read-model row values")
        required = (*PRIMARY_KEYS[self.table], "revision", "source_fact_hash")
        missing = [name for name in required if name not in values]
        if missing:
            raise ReadModelProjectionErrorV1(
                "read-model row misses a required identity or revision"
            )
        revision = values["revision"]
        if type(revision) is not int or revision < 1:
            raise ReadModelProjectionErrorV1("read-model row revision is invalid")
        _sha256(values["source_fact_hash"], "read-model row source hash")
        object.__setattr__(self, "values", values)

    @property
    def primary_key(self) -> tuple[object, ...]:
        return tuple(self.values[name] for name in PRIMARY_KEYS[self.table])


@dataclass(frozen=True)
class ReadModelSnapshotReplacementV1:
    """A complete account snapshot to atomically replace one asset/position scope."""

    table: ReadModelTableV1
    scope: Mapping[str, object]
    snapshot_generation: int
    rows: tuple[ReadModelRowMutationV1, ...]

    def __post_init__(self) -> None:
        if self.table not in SNAPSHOT_TABLES:
            raise ReadModelProjectionErrorV1("read-model snapshot table is invalid")
        scope = _freeze_values(self.scope, "read-model snapshot scope")
        required = SNAPSHOT_SCOPE_KEYS[self.table]
        if set(scope) != set(required):
            raise ReadModelProjectionErrorV1("read-model snapshot scope has an unexpected shape")
        scope = MappingProxyType({name: scope[name] for name in required})
        if type(self.snapshot_generation) is not int or self.snapshot_generation < 1:
            raise ReadModelProjectionErrorV1("read-model snapshot generation is invalid")
        rows = tuple(self.rows)
        primary_keys: set[tuple[object, ...]] = set()
        for row in rows:
            if row.table is not self.table:
                raise ReadModelProjectionErrorV1("read-model snapshot contains another table")
            if any(row.values[name] != scope[name] for name in required):
                raise ReadModelProjectionErrorV1(
                    "read-model snapshot row escapes its account scope"
                )
            if row.values.get("snapshot_generation") != self.snapshot_generation:
                raise ReadModelProjectionErrorV1("read-model snapshot row has another generation")
            if row.primary_key in primary_keys:
                raise ReadModelProjectionErrorV1("read-model snapshot repeats a primary key")
            primary_keys.add(row.primary_key)
        object.__setattr__(self, "scope", scope)
        object.__setattr__(self, "rows", rows)


@dataclass(frozen=True)
class ReadModelProjectionBatchV1:
    """One all-or-nothing projector transaction after Journal validation."""

    expected_checkpoint: ReadModelPositionV1 | None
    checkpoint: ReadModelCheckpointV1
    mutations: tuple[ReadModelRowMutationV1, ...] = ()
    snapshots: tuple[ReadModelSnapshotReplacementV1, ...] = ()

    def __post_init__(self) -> None:
        mutations = tuple(self.mutations)
        snapshots = tuple(self.snapshots)
        if not mutations and not snapshots:
            raise ReadModelProjectionErrorV1("read-model projection batch is empty")
        if (
            self.expected_checkpoint is not None
            and self.checkpoint.position <= self.expected_checkpoint
        ):
            raise ReadModelProjectionErrorV1("read-model checkpoint moves backward")
        seen: set[tuple[ReadModelTableV1, tuple[object, ...]]] = set()
        for mutation in mutations:
            if mutation.table in SNAPSHOT_TABLES:
                raise ReadModelProjectionErrorV1("snapshot rows require an atomic replacement")
            key = (mutation.table, mutation.primary_key)
            if key in seen:
                raise ReadModelProjectionErrorV1("read-model batch repeats a row mutation")
            seen.add(key)
        seen_snapshot_scopes: set[tuple[ReadModelTableV1, tuple[object, ...]]] = set()
        for snapshot in snapshots:
            scope_key = (
                snapshot.table,
                tuple(snapshot.scope[name] for name in SNAPSHOT_SCOPE_KEYS[snapshot.table]),
            )
            if scope_key in seen_snapshot_scopes:
                raise ReadModelProjectionErrorV1("read-model batch repeats a snapshot replacement")
            seen_snapshot_scopes.add(scope_key)
        object.__setattr__(self, "mutations", mutations)
        object.__setattr__(self, "snapshots", snapshots)

    @property
    def batch_hash(self) -> str:
        """A bounded identity used only for projector diagnostics and tests."""

        payload = (
            self.checkpoint.key.projector_name,
            self.checkpoint.key.shard_id,
            self.checkpoint.key.account_scope,
            str(self.checkpoint.position),
            self.checkpoint.source_fact_hash,
            *(
                f"{item.table.value}:{item.primary_key}:{item.values['source_fact_hash']}"
                for item in self.mutations
            ),
        )
        return sha256("|".join(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReadModelProjectionSnapshotV1:
    """Test/health snapshot; it is never an execution authorization source."""

    checkpoint: ReadModelCheckpointV1 | None
    rows: Mapping[ReadModelTableV1, Mapping[tuple[object, ...], Mapping[str, object]]]


class InMemoryReadModelProjectionStoreV1:
    """Deterministic transaction model for projector and recovery contract tests."""

    def __init__(self) -> None:
        self._checkpoints: dict[ReadModelCheckpointKeyV1, ReadModelCheckpointV1] = {}
        self._rows: dict[ReadModelTableV1, dict[tuple[object, ...], Mapping[str, object]]] = {
            table: {} for table in ReadModelTableV1
        }

    def apply(self, batch: ReadModelProjectionBatchV1) -> None:
        """Commit every row replacement and the checkpoint, or modify nothing."""

        current = self._checkpoints.get(batch.checkpoint.key)
        if current is not None and current.position == batch.checkpoint.position:
            if current.source_fact_hash != batch.checkpoint.source_fact_hash:
                raise ReadModelProjectionErrorV1(
                    "read-model checkpoint position has a conflicting source hash"
                )
            return
        current_position = current.position if current is not None else None
        if current_position is not None and current_position > batch.checkpoint.position:
            raise ReadModelProjectionErrorV1("read-model checkpoint moves backward")
        if current_position != batch.expected_checkpoint:
            raise ReadModelProjectionErrorV1("read-model checkpoint predecessor differs")
        rows = {table: dict(values) for table, values in self._rows.items()}
        for mutation in batch.mutations:
            self._apply_mutation(rows, mutation)
        for replacement in batch.snapshots:
            self._apply_snapshot(rows, replacement)
        self._rows = rows
        self._checkpoints[batch.checkpoint.key] = batch.checkpoint

    def snapshot(self, key: ReadModelCheckpointKeyV1) -> ReadModelProjectionSnapshotV1:
        return ReadModelProjectionSnapshotV1(
            checkpoint=self._checkpoints.get(key),
            rows=MappingProxyType(
                {table: MappingProxyType(dict(values)) for table, values in self._rows.items()}
            ),
        )

    @staticmethod
    def _apply_mutation(
        rows: dict[ReadModelTableV1, dict[tuple[object, ...], Mapping[str, object]]],
        mutation: ReadModelRowMutationV1,
    ) -> None:
        table_rows = rows[mutation.table]
        current = table_rows.get(mutation.primary_key)
        if current is None:
            table_rows[mutation.primary_key] = mutation.values
            return
        current_revision = current["revision"]
        assert type(current_revision) is int
        incoming_revision = mutation.values["revision"]
        assert type(incoming_revision) is int
        if incoming_revision < current_revision:
            return
        if incoming_revision == current_revision:
            if current["source_fact_hash"] != mutation.values["source_fact_hash"]:
                raise ReadModelProjectionErrorV1(
                    "read-model same revision has conflicting source hash"
                )
            return
        table_rows[mutation.primary_key] = mutation.values

    @staticmethod
    def _apply_snapshot(
        rows: dict[ReadModelTableV1, dict[tuple[object, ...], Mapping[str, object]]],
        replacement: ReadModelSnapshotReplacementV1,
    ) -> None:
        table_rows = rows[replacement.table]
        scope_keys = SNAPSHOT_SCOPE_KEYS[replacement.table]
        current_generation = max(
            (
                int(cast(int, row["snapshot_generation"]))
                for row in table_rows.values()
                if all(row[name] == replacement.scope[name] for name in scope_keys)
            ),
            default=0,
        )
        if current_generation >= replacement.snapshot_generation:
            raise ReadModelProjectionErrorV1("read-model snapshot generation is not newer")
        stale = [
            key
            for key, row in table_rows.items()
            if all(row[name] == replacement.scope[name] for name in scope_keys)
            and row["snapshot_generation"] != replacement.snapshot_generation
        ]
        for key in stale:
            del table_rows[key]
        for mutation in replacement.rows:
            InMemoryReadModelProjectionStoreV1._apply_mutation(rows, mutation)
