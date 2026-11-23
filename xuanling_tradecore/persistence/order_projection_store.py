"""Transactional incremental read index; never a trading durability fence.

SQLite DELETE/FULL runs only on cold workers. Each complete Journal batch commits
latest state, events, facts and its physical resume cursor together. Historical
orders stay on disk; updates load one bounded order, not full account history.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import get_ident
from typing import Callable, Generator

import msgspec

from ..domain import ConsumerEventPositionV2, ConsumerOrderEvent, RedisStreamIdV1
from ..oms.journal_audit import JournalAuditAnchor, JournalOrderAuditPlan
from ..oms.journal_tail import JournalTailCursor, VerifiedJournalBatch, journal_batch_fingerprint
from ..oms.order_projection import TransactionOrderProjection, reduce_order_projection
from ..oms.verified_seed_cache import AdmissionSeedTransaction, VerifiedAdmissionSeedCache
from .frozen_consumer_snapshot import build_frozen_consumer_snapshot
from .runtime_files import ExclusiveRuntimeLease, open_private, require_private_runtime_file
from .stage_timing import StageTiming, TimedSqliteConnection


class OrderProjectionError(RuntimeError):
    pass


class OrderProjectionStale(OrderProjectionError):
    pass


class _Meta(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    cursor: JournalTailCursor
    orders: int = 0
    events: int = 0
    facts: int = 0
    last_digest: str = ""
    schema_version: int = 1


@dataclass(frozen=True)
class ProjectionEventPage:
    events: tuple[ConsumerOrderEvent, ...]
    after_lsn: int
    watermark: int
    has_more: bool


_EVENT = msgspec.json.Decoder(ConsumerOrderEvent)
_SCHEMA = (
    "CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK(id=1), body BLOB NOT NULL CHECK(length(body)<=4096))",
    "CREATE TABLE orders (oms_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL UNIQUE, account TEXT NOT NULL, consumer TEXT NOT NULL, ca TEXT NOT NULL, strategy TEXT NOT NULL, ref TEXT NOT NULL, checkpoint BLOB NOT NULL CHECK(length(checkpoint)<=262144), checkpoint_hash BLOB NOT NULL CHECK(length(checkpoint_hash)=32), latest BLOB NOT NULL CHECK(length(latest)<=65536), latest_hash BLOB NOT NULL CHECK(length(latest_hash)=32), UNIQUE(account,strategy,ref))",
    "CREATE INDEX order_scope ON orders(consumer,ca,account,oms_id)",
    "CREATE TABLE events (lsn INTEGER PRIMARY KEY, oms_id TEXT NOT NULL, consumer TEXT NOT NULL, ca TEXT NOT NULL, account TEXT NOT NULL, body BLOB NOT NULL CHECK(length(body)<=65536), digest BLOB NOT NULL CHECK(length(digest)=32))",
    "CREATE INDEX event_scope ON events(consumer,ca,account,lsn)",
    "CREATE TABLE batches (first_lsn INTEGER PRIMARY KEY, final_lsn INTEGER NOT NULL UNIQUE, batch_id INTEGER NOT NULL UNIQUE, digest TEXT NOT NULL, anchor BLOB NOT NULL CHECK(length(anchor)<=4096), anchor_hash BLOB NOT NULL CHECK(length(anchor_hash)=32))",
    "CREATE TABLE facts (lsn INTEGER PRIMARY KEY, oms_id TEXT NOT NULL, body BLOB NOT NULL CHECK(length(body)<=16384), digest BLOB NOT NULL CHECK(length(digest)=32))",
    "CREATE INDEX order_facts ON facts(oms_id,lsn)",
    "CREATE TABLE snapshot_scopes (consumer TEXT NOT NULL, ca TEXT NOT NULL, account TEXT NOT NULL, orders INTEGER NOT NULL, PRIMARY KEY(consumer,ca,account)) WITHOUT ROWID",
    "CREATE TABLE snapshot_ids (oms_id TEXT PRIMARY KEY, consumer TEXT NOT NULL, ca TEXT NOT NULL, account TEXT NOT NULL, ordinal INTEGER NOT NULL, UNIQUE(consumer,ca,account,ordinal))",
    "CREATE TABLE snapshot_versions (consumer TEXT NOT NULL, ca TEXT NOT NULL, account TEXT NOT NULL, ordinal INTEGER NOT NULL, lsn INTEGER NOT NULL, body BLOB NOT NULL CHECK(length(body)<=65536), digest BLOB NOT NULL CHECK(length(digest)=32), PRIMARY KEY(consumer,ca,account,ordinal,lsn)) WITHOUT ROWID",
)


_EVENT_PAGE_SQL = "SELECT lsn,CASE WHEN length(body)<=65536 THEN body END,digest FROM events WHERE consumer=? AND ca=? AND account=? AND lsn>? AND lsn<=? ORDER BY lsn LIMIT ?"
_ORDER_WRITE_SQL = "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(oms_id) DO UPDATE SET checkpoint=excluded.checkpoint,checkpoint_hash=excluded.checkpoint_hash,latest=excluded.latest,latest_hash=excluded.latest_hash"
_FACT_WRITE_SQL = "INSERT INTO facts VALUES(?,?,?,?)"
_EVENT_WRITE_SQL = "INSERT INTO events VALUES(?,?,?,?,?,?,?)"
_VERSION_WRITE_SQL = "INSERT INTO snapshot_versions VALUES(?,?,?,?,?,?,?)"


class OrderProjectionStore:
    """One owner thread per connection; readonly peers run on read lanes.

    Missing DB requires explicit initialization; detected corruption is not absence.
    This is a rebuildable read index, not execution or recovery authority.
    """

    def __init__(
        self,
        root: Path,
        *,
        journal_id: str,
        profile_hash: str,
        initialize: bool = False,
        read_only: bool = False,
        maximum_database_bytes: int = 8 * 1024**3,
        maximum_order_events: int = 64,
        stage_observer: Callable[[dict[str, object]], None] | None = None,
        optimize_transactions: bool = False,
        commit_only_diagnostics: bool = False,
        reuse_verified_admission_seeds: bool = False,
    ) -> None:
        if (
            not journal_id
            or len(journal_id) > 128
            or len(profile_hash) != 64
            or any(c not in "0123456789abcdef" for c in profile_hash)
            or type(maximum_database_bytes) is not int
            or not 1024**2 <= maximum_database_bytes <= 1024**4
            or type(maximum_order_events) is not int
            or not 1 <= maximum_order_events <= 1024
            or (initialize and read_only)
            or type(initialize) is not bool
            or type(read_only) is not bool
            or type(optimize_transactions) is not bool
            or type(commit_only_diagnostics) is not bool
            or type(reuse_verified_admission_seeds) is not bool
            or (reuse_verified_admission_seeds and not optimize_transactions)
            or (commit_only_diagnostics and stage_observer is None)
            or (optimize_transactions and read_only)
            or (stage_observer is not None and (not callable(stage_observer) or read_only))
        ):
            raise OrderProjectionError("projection profile/budget is invalid")
        if initialize:
            root.mkdir(mode=0o700, parents=False, exist_ok=True)
        if (
            root.is_symlink()
            or not root.is_dir()
            or (
                os.name == "posix"
                and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
            )
        ):
            raise OrderProjectionError("projection root is missing/unsafe")
        if initialize and any(root.iterdir()):
            raise OrderProjectionError("projection initialization requires an empty directory")
        self._owner = get_ident()
        self._scope = (journal_id, profile_hash)
        self._maximum_order_events = maximum_order_events
        self._read_only, self._closed, self._failed = read_only, False, False
        self._grouped_write = False
        self._optimize_transactions = optimize_transactions
        self._seed_context = (
            journal_id,
            profile_hash,
            sha256(str(root.resolve()).encode()).hexdigest(),
        )
        self._seed_cache = (
            VerifiedAdmissionSeedCache(self._seed_context)
            if reuse_verified_admission_seeds
            else None
        )
        self._seed_transaction: AdmissionSeedTransaction | None = None
        self._group_meta: _Meta | None = None
        self._group_checkpoints: dict[str, bytes] = {}
        self._group_reducers: dict[str, TransactionOrderProjection] = {}
        self._group_snapshot_scopes: dict[tuple[str, str, str], int] = {}
        self._group_snapshot_ids: dict[str, tuple[tuple[str, str, str], int]] = {}
        self._group_order_rows: dict[str, tuple[str | bytes | None, ...]] = {}
        self._group_fact_rows: list[tuple[int, str | None, bytes, bytes]] = []
        self._group_event_rows: list[tuple[str | int | bytes, ...]] = []
        self._group_version_rows: list[tuple[str | int | bytes, ...]] = []
        self._stage_observer = stage_observer
        self._commit_only_diagnostics = commit_only_diagnostics
        self._stage: StageTiming | None = None
        self._stage_batches = self._stage_facts = self._stage_first = self._stage_final = 0
        self._lease = None if read_only else ExclusiveRuntimeLease(root / "projection-owner.lock")
        self.path = root / "orders.sqlite"
        db: sqlite3.Connection | None = None
        try:
            if initialize:
                os.close(open_private(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
            require_private_runtime_file(self.path)
            if self.path.stat().st_size > maximum_database_bytes:
                raise OrderProjectionError(
                    "existing projection exceeds the configured database budget"
                )
            uri = self.path.resolve().as_uri() + ("?mode=ro" if read_only else "?mode=rw")
            db = sqlite3.connect(
                uri,
                uri=True,
                timeout=5,
                factory=TimedSqliteConnection
                if stage_observer is not None and not commit_only_diagnostics
                else sqlite3.Connection,
            )
            self._db = db
            db.execute("PRAGMA cache_size=-8192")
            if read_only:
                db.execute("PRAGMA query_only=ON")
            else:
                if db.execute("PRAGMA journal_mode=DELETE").fetchone() != ("delete",):
                    raise OrderProjectionError("projection journal mode is not DELETE")
                db.execute("PRAGMA synchronous=FULL")
                page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
                page_limit = maximum_database_bytes // page_size
                actual_limit = db.execute(f"PRAGMA max_page_count={page_limit}").fetchone()[0]
                if actual_limit > page_limit:
                    raise OrderProjectionError("projection page budget could not be enforced")
            if initialize:
                with db:
                    for statement in _SCHEMA:
                        db.execute(statement)
                    db.execute(
                        "INSERT INTO meta VALUES(1,?)",
                        (msgspec.json.encode(_Meta(JournalTailCursor(journal_id, profile_hash))),),
                    )
                    db.execute("PRAGMA user_version=3")
            if db.execute("PRAGMA user_version").fetchone() != (3,) or {
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            } != {
                "meta",
                "orders",
                "events",
                "batches",
                "facts",
                "snapshot_scopes",
                "snapshot_ids",
                "snapshot_versions",
            }:
                raise OrderProjectionError("projection schema is missing/unsupported")
            if tuple(row[1] for row in db.execute("PRAGMA table_info(batches)")) != (
                "first_lsn",
                "final_lsn",
                "batch_id",
                "digest",
                "anchor",
                "anchor_hash",
            ):
                raise OrderProjectionError("projection audit schema requires an explicit rebuild")
            if tuple(row[1] for row in db.execute("PRAGMA table_info(events)")) != (
                "lsn",
                "oms_id",
                "consumer",
                "ca",
                "account",
                "body",
                "digest",
            ):
                raise OrderProjectionError("projection event schema requires an explicit rebuild")
            if not read_only and db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise OrderProjectionError("projection database integrity failed")
            self._meta()
        except BaseException:
            if db is not None:
                db.close()
            if self._lease is not None:
                self._lease.close()
            raise

    def _require_open(self) -> None:
        if get_ident() != self._owner or self._closed or self._failed:
            raise OrderProjectionError(
                "projection is closed/failed or used outside its owner thread"
            )

    def _meta(self) -> _Meta:
        if self._grouped_write and self._optimize_transactions and self._group_meta is not None:
            return self._group_meta
        row = self._db.execute(
            "SELECT CASE WHEN length(m.body)<=4096 THEN m.body END,"
            "b.first_lsn,b.final_lsn,b.batch_id,b.digest FROM meta AS m "
            "LEFT JOIN (SELECT first_lsn,final_lsn,batch_id,digest FROM batches "
            "ORDER BY first_lsn DESC LIMIT 1) AS b ON 1=1 WHERE m.id=1"
        ).fetchone()
        if row is None or type(row[0]) is not bytes or len(row[0]) > 4096:
            raise OrderProjectionError("projection watermark is missing/oversized")
        meta = msgspec.json.decode(row[0], type=_Meta)
        c = meta.cursor
        if (
            meta.schema_version != 1
            or (c.journal_id, c.profile_hash) != self._scope
            or c.next_lsn < 1
            or c.next_batch_id < 1
            or c.segment < 1
            or c.frame < 0
            or min(meta.orders, meta.events, meta.facts) < 0
            or meta.facts != c.next_lsn - 1
        ):
            raise OrderProjectionError("projection watermark scope/count is invalid")
        # Constructor/cursor checks may run outside an explicit read txn. Both
        # halves MUST come from one SQL statement snapshot: two SELECTs could
        # straddle a valid writer commit and falsely report corrupt coverage.
        last = None if row[1] is None else row[2:5]
        if (c.next_lsn == 1 and last is not None) or (
            c.next_lsn > 1
            and (last is None or last != (c.next_lsn - 1, c.next_batch_id - 1, meta.last_digest))
        ):
            raise OrderProjectionError("projection watermark is not a complete batch boundary")
        return meta

    @property
    def cursor(self) -> JournalTailCursor:
        self._require_open()
        return self._meta().cursor

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Generator[None, None, None]:
        self._require_open()
        if write and self._read_only:
            raise OrderProjectionError("readonly projection cannot apply facts")
        if self._grouped_write:
            if not write:
                raise OrderProjectionError("read transaction cannot nest in a grouped write")
            # Only apply_many owns this flag. Any member failure propagates to
            # its outer transaction; an individual batch cannot commit a prefix.
            yield
            return
        stage = (
            StageTiming("projector", self._stage_observer)
            if write and self._stage_observer is not None
            else None
        )
        committed = False
        if stage is not None:
            self._stage_batches = self._stage_facts = self._stage_first = self._stage_final = 0
            if not self._commit_only_diagnostics:
                self._stage = stage
                assert isinstance(self._db, TimedSqliteConnection)
                self._db.stage_timing = stage
        try:
            self._db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield
                if self._commit_only_diagnostics and stage is not None:
                    stage.call("sql_commit", self._db.commit)
                else:
                    self._db.commit()
                committed = True
            except BaseException:
                if self._commit_only_diagnostics and stage is not None:
                    stage.call("sql_rollback", self._db.rollback)
                else:
                    self._db.rollback()
                raise
        finally:
            if stage is not None:
                self._stage = None
                if not self._commit_only_diagnostics:
                    assert isinstance(self._db, TimedSqliteConnection)
                    self._db.stage_timing = None
                stage.finish(
                    {
                        "committed": committed,
                        "attempted_batches": self._stage_batches,
                        "committed_batches": self._stage_batches if committed else 0,
                        "committed_facts": self._stage_facts if committed else 0,
                        "first_lsn": self._stage_first,
                        "final_lsn": self._stage_final,
                        "commit_only": self._commit_only_diagnostics,
                        "seed_cache_hits_total": self._seed_cache.hits if self._seed_cache else 0,
                        "seed_cache_misses_total": self._seed_cache.misses
                        if self._seed_cache
                        else 0,
                        "seed_cache_entries_before_publish": self._seed_cache.entries
                        if self._seed_cache
                        else 0,
                        "seed_cache_bytes_before_publish": self._seed_cache.encoded_bytes
                        if self._seed_cache
                        else 0,
                    }
                )

    @staticmethod
    def _checked(blob: object, digest: object) -> bytes:
        if type(blob) is not bytes or type(digest) is not bytes or sha256(blob).digest() != digest:
            raise OrderProjectionError("projection row checksum is invalid")
        return blob

    def _write_meta(self, meta: _Meta) -> None:
        if self._grouped_write and self._optimize_transactions:
            self._group_meta = meta
            return
        self._db.execute("UPDATE meta SET body=? WHERE id=1", (self._encode(meta),))

    def _encode(self, value: object) -> bytes:
        return (
            msgspec.json.encode(value)
            if self._stage is None
            else self._stage.call("encoding", msgspec.json.encode, value)
        )

    def _digest(self, value: bytes) -> bytes:
        if self._stage is None:
            return sha256(value).digest()
        return self._stage.call("hashing", lambda: sha256(value).digest())

    def apply_many(self, batches: tuple[VerifiedJournalBatch, ...]) -> tuple[bool, ...]:
        """Amortize FULL commit over already available complete source batches.

        No coalescing delay, partial source batch, weakened durability or writer
        notification is introduced. All rows and the final cursor commit once.
        A bad later member rolls back the whole group, not just that member.
        """
        self._require_open()
        if (
            type(batches) is not tuple
            or not 1 <= len(batches) <= 256
            or sum(len(batch.records) for batch in batches) > 512
            or self._grouped_write
        ):
            raise OrderProjectionError("projection transaction group exceeds its budget")
        try:
            if self._seed_cache is not None:
                self._seed_transaction = self._seed_cache.begin(self._seed_context)
            with self._transaction(write=True):
                self._grouped_write = True
                try:
                    results = tuple(self.apply(batch) for batch in batches)
                    if self._group_order_rows:
                        self._db.executemany(_ORDER_WRITE_SQL, self._group_order_rows.values())
                    if self._group_fact_rows:
                        self._db.executemany(_FACT_WRITE_SQL, self._group_fact_rows)
                    if self._group_event_rows:
                        self._db.executemany(_EVENT_WRITE_SQL, self._group_event_rows)
                    if self._group_version_rows:
                        self._db.executemany(_VERSION_WRITE_SQL, self._group_version_rows)
                    for scope, count in self._group_snapshot_scopes.items():
                        self._db.execute(
                            "INSERT INTO snapshot_scopes VALUES(?,?,?,?) ON CONFLICT(consumer,ca,account) DO UPDATE SET orders=excluded.orders",
                            (*scope, count),
                        )
                    if self._optimize_transactions and self._group_meta is not None:
                        self._db.execute(
                            "UPDATE meta SET body=? WHERE id=1", (self._encode(self._group_meta),)
                        )
                finally:
                    self._grouped_write = False
                    self._group_meta = None
                    self._group_checkpoints.clear()
                    self._group_reducers.clear()
                    self._group_snapshot_scopes.clear()
                    self._group_snapshot_ids.clear()
                    self._group_order_rows.clear()
                    self._group_fact_rows.clear()
                    self._group_event_rows.clear()
                    self._group_version_rows.clear()
            # The context above has returned from the REAL SQLite FULL commit.
            # No mutable state is retained and a rollback publishes no proofs.
            if self._seed_transaction is not None:
                self._seed_transaction.publish_after_full_commit()
            return results
        except BaseException:
            self._failed = True
            raise
        finally:
            if self._seed_transaction is not None:
                self._seed_transaction.abort()
                self._seed_transaction = None

    def apply(self, batch: VerifiedJournalBatch) -> bool:
        """Accept physically verified tail output; commit rows+cursor atomically."""
        self._require_open()
        try:
            with self._transaction(write=True):
                meta = self._meta()
                before, after, rows = batch.before, batch.after, batch.records
                if (
                    not rows
                    or len(rows) > 256
                    or len(batch.sha256) != 64
                    or any(c not in "0123456789abcdef" for c in batch.sha256)
                    or (before.journal_id, before.profile_hash) != self._scope
                    or (after.journal_id, after.profile_hash) != self._scope
                    or after.next_lsn != before.next_lsn + len(rows)
                    or after.next_batch_id != before.next_batch_id + 1
                    or after.segment != before.segment
                    or after.frame != before.frame + len(rows)
                    or batch.sha256
                    != (
                        journal_batch_fingerprint(rows)
                        if self._stage is None
                        else self._stage.call("batch_validation", journal_batch_fingerprint, rows)
                    )
                ):
                    raise OrderProjectionError("projection batch scope/range is invalid")
                for ordinal, row in enumerate(rows):
                    if (
                        row.lsn != before.next_lsn + ordinal
                        or row.batch_id != before.next_batch_id
                        or row.ordinal != ordinal
                        or row.batch_size != len(rows)
                    ):
                        raise OrderProjectionError("projection received an incomplete batch")
                old = self._db.execute(
                    "SELECT final_lsn,batch_id,digest FROM batches WHERE first_lsn=?",
                    (before.next_lsn,),
                ).fetchone()
                if old is not None:
                    if old != (after.next_lsn - 1, before.next_batch_id, batch.sha256):
                        raise OrderProjectionError("projection duplicate batch changed")
                    return False
                if (
                    before.next_lsn != meta.cursor.next_lsn
                    or before.next_batch_id != meta.cursor.next_batch_id
                    or (before.segment == meta.cursor.segment and before.frame != meta.cursor.frame)
                    or before.segment not in {meta.cursor.segment, meta.cursor.segment + 1}
                    or (before.segment != meta.cursor.segment and before.frame != 0)
                ):
                    raise OrderProjectionError("projection suffix has a gap/regression")
                first = rows[0].fact
                reuse = self._optimize_transactions and self._grouped_write
                previous = self._group_checkpoints.get(first.oms_order_id or "") if reuse else None
                new_order = False
                if previous is None:
                    old = self._db.execute(
                        "SELECT CASE WHEN length(checkpoint)<=262144 THEN checkpoint END,checkpoint_hash FROM orders WHERE oms_id=?",
                        (first.oms_order_id,),
                    ).fetchone()
                    previous = None if old is None else self._checked(old[0], old[1])
                    new_order = old is None
                if reuse:
                    key = first.oms_order_id or ""
                    reducer = self._group_reducers.get(key)
                    if reducer is None:
                        reducer = TransactionOrderProjection(
                            previous=previous,
                            maximum_order_events=self._maximum_order_events,
                            seed_transaction=self._seed_transaction,
                        )
                        self._group_reducers[key] = reducer
                    reduce = reducer.reduce
                    facts = tuple(row.fact for row in rows)
                    result = (
                        reduce(facts, batch.commit)
                        if self._stage is None
                        else self._stage.call("reducer", reduce, facts, batch.commit)
                    )
                elif self._stage is None:
                    result = reduce_order_projection(
                        tuple(row.fact for row in rows),
                        batch.commit,
                        previous=previous,
                        maximum_order_events=self._maximum_order_events,
                    )
                else:
                    result = self._stage.call(
                        "reducer",
                        reduce_order_projection,
                        tuple(row.fact for row in rows),
                        batch.commit,
                        previous=previous,
                        maximum_order_events=self._maximum_order_events,
                    )
                event = result.latest
                trace = event.consumer_trace
                encoded = self._encode(event)
                encoded_hash = self._digest(encoded) if self._optimize_transactions else None
                order_row = (
                    event.oms_order_id,
                    event.execution_id,
                    event.account_id,
                    trace.consumer_id,
                    trace.consumer_account_id,
                    trace.strategy_id or "",
                    trace.strategy_order_id or "",
                    result.checkpoint,
                    self._digest(result.checkpoint),
                    encoded,
                    self._digest(encoded) if encoded_hash is None else encoded_hash,
                )
                if reuse:
                    self._group_order_rows[event.oms_order_id] = order_row
                else:
                    self._db.execute(_ORDER_WRITE_SQL, order_row)
                if result.event is not None:
                    self._snapshot_event(event, encoded, new_order=new_order, digest=encoded_hash)
                    event_row = (
                        event.journal_lsn,
                        event.oms_order_id,
                        trace.consumer_id,
                        trace.consumer_account_id,
                        event.account_id,
                        encoded,
                        self._digest(encoded) if encoded_hash is None else encoded_hash,
                    )
                    if reuse:
                        self._group_event_rows.append(event_row)
                    else:
                        self._db.execute(_EVENT_WRITE_SQL, event_row)
                fact_rows: list[tuple[int, str | None, bytes, bytes]] = []
                for row in rows:
                    fact = row.fact
                    body = self._encode(
                        {
                            "fact_id": fact.fact_id,
                            "kind": fact.kind.value,
                            "account_id": fact.account_id,
                            "oms_order_id": fact.oms_order_id,
                            "execution_id": fact.execution_id,
                            "attributes": dict(fact.attributes),
                            "recovery_payload": fact.recovery_payload,
                        }
                    )
                    values = (row.lsn, fact.oms_order_id, body, self._digest(body))
                    if reuse:
                        self._group_fact_rows.append(values)
                    elif self._optimize_transactions:
                        fact_rows.append(values)
                    else:
                        self._db.execute(_FACT_WRITE_SQL, values)
                if self._optimize_transactions and not reuse:
                    self._db.executemany(_FACT_WRITE_SQL, fact_rows)
                anchor = self._encode(before)
                self._db.execute(
                    "INSERT INTO batches VALUES(?,?,?,?,?,?)",
                    (
                        before.next_lsn,
                        after.next_lsn - 1,
                        before.next_batch_id,
                        batch.sha256,
                        anchor,
                        self._digest(anchor),
                    ),
                )
                self._write_meta(
                    _Meta(
                        after,
                        meta.orders + new_order,
                        meta.events + (result.event is not None),
                        meta.facts + len(rows),
                        batch.sha256,
                    )
                )
                if reuse:
                    self._group_checkpoints[event.oms_order_id] = result.checkpoint
                if self._stage is not None or self._commit_only_diagnostics:
                    self._stage_batches += 1
                    self._stage_facts += len(rows)
                    self._stage_first = self._stage_first or before.next_lsn
                    self._stage_final = after.next_lsn - 1
            return True
        except Exception:
            self._failed = True
            raise

    def _scope_event(
        self, encoded: bytes, scope: tuple[str, str, str], watermark: int
    ) -> ConsumerOrderEvent:
        event = _EVENT.decode(encoded)
        if (
            (
                event.consumer_trace.consumer_id,
                event.consumer_trace.consumer_account_id,
                event.account_id,
            )
            != scope
            or event.journal_id != self._scope[0]
            or event.journal_lsn > watermark
            or event.consumer_trace.account_id != scope[2]
        ):
            raise OrderProjectionError("projection row scope/watermark mismatch")
        return event

    def _snapshot_event(
        self,
        event: ConsumerOrderEvent,
        encoded: bytes,
        *,
        new_order: bool,
        digest: bytes | None = None,
    ) -> None:
        trace = event.consumer_trace
        scope = (trace.consumer_id, trace.consumer_account_id, event.account_id)
        reuse = self._grouped_write and self._optimize_transactions
        if new_order:
            if reuse:
                count = self._group_snapshot_scopes.get(scope)
                if count is None:
                    row = self._db.execute(
                        "SELECT orders FROM snapshot_scopes WHERE consumer=? AND ca=? AND account=?",
                        scope,
                    ).fetchone()
                    count = 0 if row is None else row[0]
                    if type(count) is not int or count < 0:
                        raise OrderProjectionError("snapshot membership counter is invalid")
                ordinal = count + 1
                self._group_snapshot_scopes[scope] = ordinal
            else:
                self._db.execute(
                    "INSERT INTO snapshot_scopes VALUES(?,?,?,1) ON CONFLICT(consumer,ca,account) DO UPDATE SET orders=orders+1",
                    scope,
                )
                ordinal = self._db.execute(
                    "SELECT orders FROM snapshot_scopes WHERE consumer=? AND ca=? AND account=?",
                    scope,
                ).fetchone()[0]
            self._db.execute(
                "INSERT INTO snapshot_ids VALUES(?,?,?,?,?)", (event.oms_order_id, *scope, ordinal)
            )
        else:
            cached = self._group_snapshot_ids.get(event.oms_order_id) if reuse else None
            if cached is not None:
                if cached[0] != scope:
                    raise OrderProjectionError("snapshot cached identity changed scope")
                ordinal = cached[1]
            else:
                row = self._db.execute(
                    "SELECT ordinal FROM snapshot_ids WHERE oms_id=? AND consumer=? AND ca=? AND account=?",
                    (event.oms_order_id, *scope),
                ).fetchone()
                if row is None:
                    raise OrderProjectionError("snapshot identity coverage is missing")
                ordinal = row[0]
        if reuse:
            self._group_snapshot_ids[event.oms_order_id] = (scope, ordinal)
        version_row = (
            *scope,
            ordinal,
            event.journal_lsn,
            encoded,
            self._digest(encoded) if digest is None else digest,
        )
        if reuse:
            self._group_version_rows.append(version_row)
        else:
            self._db.execute(_VERSION_WRITE_SQL, version_row)

    def snapshot_descriptor(self, scope: tuple[str, str, str]) -> tuple[int, int]:
        """O(1) fixed membership cut, not a whole-history copy or pinned SQL txn."""
        with self._transaction():
            meta = self._meta()
            watermark = meta.facts
            row = self._db.execute(
                "SELECT orders FROM snapshot_scopes WHERE consumer=? AND ca=? AND account=?", scope
            ).fetchone()
            count = 0 if row is None else row[0]
            if type(count) is not int or not 0 <= count <= meta.orders:
                raise OrderProjectionError("snapshot membership counter is invalid")
            edge = self._db.execute(
                "SELECT ordinal FROM snapshot_ids WHERE consumer=? AND ca=? AND account=? ORDER BY ordinal DESC LIMIT 1",
                scope,
            ).fetchone()
            if (edge is None and count) or (edge is not None and edge != (count,)):
                raise OrderProjectionError("snapshot membership counter lost coverage")
            return watermark, count

    def snapshot_page(
        self, scope: tuple[str, str, str], *, watermark: int, count: int, start: int, end: int
    ) -> tuple[ConsumerOrderEvent, ...]:
        """Immutable version rows shared by all pins; at most 200 identities read.

        Ordering is stable scope-local admission ordinal. No OFFSET scan, full
        Python tuple or live-only omission; terminal orders remain queryable.
        """
        if (
            any(type(v) is not int for v in (watermark, count, start, end))
            or watermark < 0
            or not 0 <= start <= end <= count
            or end - start > 200
        ):
            raise OrderProjectionError("snapshot page budget is invalid")
        with self._transaction():
            if self._meta().facts < watermark:
                raise OrderProjectionStale("snapshot source regressed below its pin")
            rows = self._db.execute(
                "SELECT v.ordinal,CASE WHEN length(v.body)<=65536 THEN v.body END,v.digest FROM snapshot_versions v WHERE v.consumer=? AND v.ca=? AND v.account=? AND v.ordinal>? AND v.ordinal<=? AND v.lsn=(SELECT max(p.lsn) FROM snapshot_versions p WHERE p.consumer=v.consumer AND p.ca=v.ca AND p.account=v.account AND p.ordinal=v.ordinal AND p.lsn<=?) ORDER BY v.ordinal",
                (*scope, start, end, watermark),
            )
            events: list[ConsumerOrderEvent] = []
            used = 0
            for ordinal, encoded, checksum in rows:
                if ordinal != start + len(events) + 1:
                    raise OrderProjectionError("snapshot page has a membership gap")
                body = self._checked(encoded, checksum)
                used += len(body)
                if used > 1024**2:
                    raise OrderProjectionError("snapshot page exceeds decoded byte budget")
                events.append(self._scope_event(body, scope, watermark))
            if len(events) != end - start:
                raise OrderProjectionError("snapshot page coverage is incomplete")
            return tuple(events)

    def lookup(
        self, scope: tuple[str, str, str], order_id: str, *, minimum_watermark: int = 0
    ) -> ConsumerOrderEvent | None:
        with self._transaction():
            watermark = self._meta().facts
            if watermark < minimum_watermark:
                raise OrderProjectionStale("projection has not reached the required watermark")
            row = self._db.execute(
                "SELECT CASE WHEN length(latest)<=65536 THEN latest END,latest_hash FROM orders WHERE consumer=? AND ca=? AND account=? AND oms_id=?",
                (*scope, order_id),
            ).fetchone()
            if row is None:
                return None
            event = self._scope_event(self._checked(row[0], row[1]), scope, watermark)
            if event.oms_order_id != order_id:
                raise OrderProjectionError("projection row identity mismatch")
            return event

    def events_after(
        self,
        scope: tuple[str, str, str],
        after_lsn: int,
        *,
        limit: int = 100,
        through_lsn: int | None = None,
        minimum_watermark: int = 0,
    ) -> ProjectionEventPage:
        if (
            type(after_lsn) is not int
            or after_lsn < 0
            or type(limit) is not int
            or not 1 <= limit <= 200
            or type(minimum_watermark) is not int
            or minimum_watermark < 0
            or (
                through_lsn is not None
                and (type(through_lsn) is not int or through_lsn < after_lsn)
            )
        ):
            raise OrderProjectionError("projection event page capacity is invalid")
        with self._transaction():
            watermark = self._meta().facts
            if max(after_lsn, minimum_watermark, through_lsn or 0) > watermark:
                raise OrderProjectionStale("projection is behind the requested event position")
            high = watermark if through_lsn is None else through_lsn
            rows = self._db.execute(
                _EVENT_PAGE_SQL,
                (*scope, after_lsn, high, limit + 1),
            )
            collected: list[ConsumerOrderEvent] = []
            used = 0
            has_more = False
            for row in rows:
                body = self._checked(row[1], row[2])
                if len(collected) == limit or used + len(body) > 262144:
                    has_more = True
                    break
                event = self._scope_event(body, scope, watermark)
                if event.journal_lsn != row[0]:
                    raise OrderProjectionError("projection event position mismatch")
                collected.append(event)
                used += len(body)
            events = tuple(collected)
            return ProjectionEventPage(
                events,
                events[-1].journal_lsn if events else after_lsn,
                high,
                has_more,
            )

    def partition_events(
        self,
        scope: tuple[str, str, str],
        *,
        partition_count: int,
        minimum_watermark: int = 0,
        after_lsn: int = 0,
        through_lsn: int | None = None,
    ) -> tuple[tuple[int, int, int], ...]:
        """Cold-lane range planning: one SQL snapshot, at most 64 bounds.

        Counts/offsets may scan scoped disk indexes, never a Python history list.
        This maintenance read is not an O(1) hot-path or throughput claim.
        """
        if (
            type(partition_count) is not int
            or not 1 <= partition_count <= 64
            or type(minimum_watermark) is not int
            or minimum_watermark < 0
            or type(after_lsn) is not int
            or after_lsn < 0
            or (through_lsn is not None and (type(through_lsn) is not int or through_lsn < 0))
        ):
            raise OrderProjectionError("projection partition capacity is invalid")
        with self._transaction():
            watermark = self._meta().facts
            if watermark < max(minimum_watermark, after_lsn):
                raise OrderProjectionStale("projection is behind the requested watermark")
            high_cut = watermark if through_lsn is None else min(watermark, through_lsn)
            if high_cut < after_lsn:
                raise OrderProjectionStale("source fence precedes the signed predecessor")
            count = self._db.execute(
                "SELECT count(*) FROM events WHERE consumer=? AND ca=? AND account=? AND lsn>? AND lsn<=?",
                (*scope, after_lsn, high_cut),
            ).fetchone()[0]
            size = min(partition_count, count)
            result: list[tuple[int, int, int]] = []
            after = after_lsn
            for index in range(size):
                start, end = count * index // size, count * (index + 1) // size
                high = self._db.execute(
                    "SELECT lsn FROM events WHERE consumer=? AND ca=? AND account=? AND lsn>? AND lsn<=? ORDER BY lsn LIMIT 1 OFFSET ?",
                    (*scope, after_lsn, high_cut, end - 1),
                ).fetchone()[0]
                if high <= after:
                    raise OrderProjectionError("projection partition order is invalid")
                result.append((after, high, end - start))
                after = high
            return tuple(result)

    def freeze_snapshot(self, root: Path, scope: tuple[str, str, str]) -> int:
        """Stream sorted rows under one read transaction, not a full-history tuple."""
        with self._transaction():
            watermark = self._meta().facts
            rows = self._db.execute(
                "SELECT CASE WHEN length(latest)<=65536 THEN latest END,latest_hash FROM orders WHERE consumer=? AND ca=? AND account=? ORDER BY oms_id",
                scope,
            )
            orders = (
                self._scope_event(self._checked(row[0], row[1]), scope, watermark) for row in rows
            )
            build_frozen_consumer_snapshot(
                root=root,
                scope=(scope[0], scope[1], self._scope[0]),
                account_id=scope[2],
                position=ConsumerEventPositionV2(RedisStreamIdV1(watermark, 0), 0),
                orders=orders,
            )
            return watermark

    def order_audit_plan(
        self,
        scope: tuple[str, str, str],
        oms_order_id: str,
        *,
        minimum_watermark: int,
        maximum_batches: int = 128,
        maximum_facts: int = 512,
    ) -> JournalOrderAuditPlan | None:
        """Bounded, non-authoritative physical hints, copied in one SQL snapshot."""
        if (
            type(minimum_watermark) is not int
            or minimum_watermark < 0
            or type(maximum_batches) is not int
            or not 1 <= maximum_batches <= 256
            or type(maximum_facts) is not int
            or not 1 <= maximum_facts <= 4096
        ):
            raise OrderProjectionError("projection audit capacity is invalid")
        with self._transaction():
            if self._meta().facts < minimum_watermark:
                raise OrderProjectionStale("projection is behind the audit watermark")
            row = self._db.execute(
                "SELECT CASE WHEN length(checkpoint)<=262144 THEN checkpoint END,checkpoint_hash FROM orders WHERE consumer=? AND ca=? AND account=? AND oms_id=?",
                (*scope, oms_order_id),
            ).fetchone()
            if row is None:
                return None
            checkpoint = self._checked(row[0], row[1])
            rows = self._db.execute(
                "SELECT b.first_lsn,b.final_lsn,b.batch_id,b.digest,CASE WHEN length(b.anchor)<=4096 THEN b.anchor END,b.anchor_hash FROM facts f JOIN batches b ON b.first_lsn=f.lsn WHERE f.oms_id=? ORDER BY f.lsn LIMIT ?",
                (oms_order_id, maximum_batches + 1),
            )
            anchors: list[JournalAuditAnchor] = []
            total = 0
            for first, final, batch_id, digest, encoded, checksum in rows:
                total += final - first + 1
                if len(anchors) >= maximum_batches or total > maximum_facts:
                    raise OrderProjectionError("order audit exceeds its bounded contract")
                before = msgspec.json.decode(
                    self._checked(encoded, checksum), type=JournalTailCursor
                )
                if (
                    (before.journal_id, before.profile_hash) != self._scope
                    or before.next_lsn != first
                    or before.next_batch_id != batch_id
                    or final < first
                ):
                    raise OrderProjectionError("order audit anchor is invalid")
                anchors.append(JournalAuditAnchor(before, final, digest))
            return JournalOrderAuditPlan(checkpoint, tuple(anchors))

    def recovery_counts(self) -> tuple[int, int, int]:
        """Startup/maintenance only: verify persisted counts, never a hot health call."""
        with self._transaction():
            meta = self._meta()
            counts = tuple(
                self._db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("facts", "orders", "events")
            )
            if counts != (meta.facts, meta.orders, meta.events):
                raise OrderProjectionError("recovery projection coverage count mismatch")
            return meta.facts, meta.orders, meta.events

    def recovery_order_page(
        self, after_order_id: str, *, watermark: int, limit: int = 32
    ) -> tuple[tuple[str, bytes], ...]:
        """Bounded keyset iteration at one quiescent startup waterline."""
        if (
            type(after_order_id) is not str
            or len(after_order_id) > 128
            or type(watermark) is not int
            or watermark < 0
            or type(limit) is not int
            or not 1 <= limit <= 32
        ):
            raise OrderProjectionError("recovery page capacity is invalid")
        with self._transaction():
            if self._meta().facts != watermark:
                raise OrderProjectionError("recovery projection watermark changed")
            rows = self._db.execute(
                "SELECT oms_id,CASE WHEN length(checkpoint)<=262144 THEN checkpoint END,checkpoint_hash FROM orders WHERE oms_id>? ORDER BY oms_id LIMIT ?",
                (after_order_id, limit),
            )
            result: list[tuple[str, bytes]] = []
            used = 0
            for order_id, encoded, digest in rows:
                body = self._checked(encoded, digest)
                if used + len(body) > 1024**2:
                    break
                result.append((order_id, body))
                used += len(body)
            return tuple(result)

    def close(self) -> None:
        if not self._closed:
            if get_ident() != self._owner:
                raise OrderProjectionError("projection close requires its owner thread")
            self._db.close()
            if self._seed_cache is not None:
                self._seed_cache.close()
            self._closed = True
            if self._lease is not None:
                self._lease.close()
