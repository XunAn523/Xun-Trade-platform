"""Bounded, off-lane exact intent storage and generation-checked lookup results.

SQLite is a derived cold index, never the admission fence. Its single owning
thread uses DELETE journaling/FULL sync, not WAL and not a trading-thread call.
An archive receipt is published only after commit and Bloom publication. The
caller must retain the hot order until that receipt, then recheck the receipt
generation, binding epoch and hot tail under its account lock before eviction
or admission. This module does not itself authorize eviction or new orders.
"""

from __future__ import annotations

import hmac
import os
import secrets
import sqlite3
from array import array
from concurrent.futures import Future
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from time import monotonic_ns
from typing import Callable, cast

import msgspec

from ..oms.cold_intent import (
    ColdArchiveReceipt,
    ColdIntentIndexBusy,
    ColdIntentIndexError,
    ColdIntentKey,
    ColdIntentResolution,
)
from ..oms.order_book import OmsOrderBookError, OrderBook, OrderBookOrder
from ..oms.recovery import OmsRecoverySeedV1
from ..oms.recovery_checkpoint import decode_checkpoint
from .runtime_files import (
    ExclusiveRuntimeLease,
    open_private,
    require_private_runtime_file,
)


@dataclass(frozen=True)
class ColdIntentIndexStats:
    generation: int
    records: int
    queued: int
    pending_lookups: int
    filter_bytes: int
    failed: bool
    filter_occupancy_ppm: int
    archive_batches: int = 0
    validation_ns: int = 0
    sql_commit_ns: int = 0
    max_validation_ns: int = 0
    max_sql_commit_ns: int = 0
    lookup_epoch_bytes: int = 0
    stale_lookup_proofs: int = 0


class ColdArchiveOutcomeUnknown(ColdIntentIndexError):
    """A remote FULL commit may exist: no subsequent negative proofs."""


@dataclass(frozen=True)
class ColdCommittedBatch:
    keys: tuple[ColdIntentKey, ...]
    digests: tuple[bytes, ...]
    generation: int
    records: int
    validation_ns: int
    sql_commit_ns: int


def commit_cold_rows(
    db: sqlite3.Connection, rows: tuple[tuple[ColdIntentKey, bytes, bytes], ...]
) -> tuple[int, int]:
    """Same append-only FULL transaction for thread or process SQL owner."""
    if len({key for key, _, _ in rows}) != len(rows):
        raise ColdIntentIndexError("cold archive repeats a key")
    added = 0
    with db:
        for key, payload, digest in rows:
            old = db.execute(
                "SELECT payload,digest FROM intents WHERE key=?", (key.encoded(),)
            ).fetchone()
            if old is not None:
                if old != (payload, digest):
                    raise ColdIntentIndexError("cold archive would overwrite an exact result")
            else:
                db.execute("INSERT INTO intents VALUES (?,?,?)", (key.encoded(), payload, digest))
                added += 1
        db.execute(
            "UPDATE meta SET generation=generation+1, records=records+? WHERE id=1", (added,)
        )
    return cast(
        tuple[int, int], db.execute("SELECT generation,records FROM meta WHERE id=1").fetchone()
    )


@dataclass(frozen=True)
class _Archive:
    checkpoints: tuple[bytes, ...]
    future: Future[ColdArchiveReceipt]


class _NoWrites:
    def append_batch(self, facts: object) -> object:
        raise ColdIntentIndexError("cold checkpoint validation cannot write facts")


class _TerminalCheckpointVerifier(OrderBook):
    """Keep only this restore's canonical admission, never a cross-call cache.

    The original restore still checks every mutable order/event/reservation
    invariant. Its decoder already reconstructs and compares the entire seed;
    deriving the key from that result must not repeat the same decode afterward.
    A failed restore never returns the captured identity to the SQL owner.
    """

    def __init__(self) -> None:
        from ..oms.order_book import OmsFactJournal

        super().__init__(journal=cast(OmsFactJournal, _NoWrites()), persist_recovery_seed=True)
        self.admission: OrderBookOrder | None = None

    def _decode_recovery_admission(self, encoded: bytes) -> OmsRecoverySeedV1:
        if self.admission is not None:
            raise ColdIntentIndexError("cold verifier is not single-order")
        seed = super()._decode_recovery_admission(encoded)
        self.admission = seed.order
        return seed


def validate_terminal_checkpoint(encoded: bytes, journal_id: str) -> ColdIntentKey:
    """Pure exact recovery validation, optionally run in a bounded child."""
    state = decode_checkpoint(encoded, maximum_bytes=256 * 1024)
    if state.journal_id != journal_id or len(state.orders) != 1:
        raise ColdIntentIndexError("cold checkpoint scope/order count is invalid")
    row = state.orders[0]
    if row.state not in {"FILLED", "CANCELED", "REJECTED"}:
        raise ColdIntentIndexError("unresolved/active orders cannot retire")
    verifier = _TerminalCheckpointVerifier()
    verifier.restore_recovery_checkpoint(encoded, expected_journal_id=journal_id)
    initial = verifier.admission
    if initial is None:
        raise ColdIntentIndexError("cold checkpoint has no verified admission")
    trace = initial.order.consumer_trace
    return ColdIntentKey(
        initial.order.account_id, trace.strategy_id or "", trace.strategy_order_id or ""
    )


class ColdIntentIndex:
    """A bounded worker mailbox; no SQLite access in probe/validate/caller threads."""

    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        capacity: int = 256,
        filter_bytes: int = 64 * 1024**2,
        maximum_batch: int = 128,
        initialize: bool = False,
        maximum_database_bytes: int = 8 * 1024**3,
        checkpoint_validator: Callable[[tuple[bytes, ...]], tuple[ColdIntentKey, ...]]
        | None = None,
        archive_committer: Callable[[tuple[bytes, ...]], ColdCommittedBatch] | None = None,
        archive_close: Callable[[], None] | None = None,
        maximum_transaction_archives: int = 1,
        maximum_transaction_checkpoints: int = 512,
        scoped_lookup_epochs: bool = False,
    ) -> None:
        if (
            not journal_id
            or len(journal_id) > 128
            or type(capacity) is not int
            or capacity < 1
            or type(filter_bytes) is not int
            or not 64 <= filter_bytes <= 64 * 1024**2
            or type(maximum_batch) is not int
            or not 1 <= maximum_batch <= 512
            or type(maximum_database_bytes) is not int
            or not 1024**2 <= maximum_database_bytes <= 1024**4
            or (archive_committer is not None and archive_close is None)
            or type(maximum_transaction_archives) is not int
            or not 1 <= maximum_transaction_archives <= 8
            or type(maximum_transaction_checkpoints) is not int
            or not maximum_batch <= maximum_transaction_checkpoints <= 512
            or type(scoped_lookup_epochs) is not bool
        ):
            raise ColdIntentIndexError("cold index scope/capacity is invalid")
        if not initialize and not root.is_dir():
            raise ColdIntentIndexError("cold index requires explicit initialization/rebuild")
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
        if root.is_symlink() or (
            os.name == "posix"
            and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        ):
            raise ColdIntentIndexError("cold index directory is unsafe")
        if initialize and any(root.iterdir()):
            raise ColdIntentIndexError("cold index initialization requires an empty directory")
        self._lease = ExclusiveRuntimeLease(root / "cold-owner.lock")
        self._archive_close = archive_close
        self._backend_drained = False
        self._checkpoint_validator = checkpoint_validator
        self._archive_committer = archive_committer
        self._transaction_archives = maximum_transaction_archives
        self._transaction_checkpoints = maximum_transaction_checkpoints
        self._path = root / "intents.sqlite"
        try:
            self._created = initialize
            if self._created:
                os.close(open_private(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
            require_private_runtime_file(self._path)
            if self._path.stat().st_size > maximum_database_bytes:
                raise ColdIntentIndexError("cold index exceeds its database budget")
            self._maximum_database_bytes = maximum_database_bytes
            self._journal_id = journal_id
            self._capacity, self._maximum_batch = capacity, maximum_batch
            self._filter = bytearray(filter_bytes)
            self._filter_set_bits = 0
            self._lookup_epochs = array("Q", [0]) * 65_536 if scoped_lookup_epochs else None
            if self._lookup_epochs is not None and self._lookup_epochs.itemsize != 8:
                raise ColdIntentIndexError("lookup epoch representation must be uint64")
            self._stale_lookup_proofs = 0
            self._lock = Lock()
            self._queue: Queue[ColdIntentKey | _Archive | None] = Queue(capacity)
            self._pending: dict[ColdIntentKey, list[Future[ColdIntentResolution]]] = {}
            self._pending_count = self._records = self._generation = 0
            self._archive_batches = self._validation_ns = self._sql_commit_ns = 0
            self._max_validation_ns = self._max_sql_commit_ns = 0
            self._nonce = secrets.token_bytes(32)
            self._failed = self._closed = False
            self._stop = Event()
            self._ready: Future[None] = Future()
            self._thread = Thread(target=self._run, name="oms-cold-index", daemon=True)
            self._thread.start()
            self._ready.result(timeout=30)
        except BaseException:
            # Startup errors are propagated by the worker after its connection closes.
            if hasattr(self, "_thread"):
                self._stop.set()
                try:
                    self._queue.put_nowait(None)
                except Full:
                    pass
                self._thread.join(timeout=30)
                if self._thread.is_alive():
                    raise ColdIntentIndexError("cold index startup did not stop")
            self._finish_close()
            raise

    def _bits(self, key: ColdIntentKey) -> tuple[int, ...]:
        digest = sha256(key.encoded()).digest()
        return tuple(
            int.from_bytes(digest[i : i + 8], "little") % (8 * len(self._filter))
            for i in range(0, 32, 8)
        )

    def _add_filter(self, key: ColdIntentKey) -> None:
        for bit in self._bits(key):
            mask = 1 << (bit % 8)
            if not self._filter[bit // 8] & mask:
                self._filter[bit // 8] |= mask
                self._filter_set_bits += 1

    def _require_open(self) -> None:
        if self._failed or self._closed:
            raise ColdIntentIndexError("cold index is failed/closed; absence is unknown")

    def probe(self, key: ColdIntentKey) -> ColdIntentResolution | None:
        """Return a memory-only negative result, or require asynchronous exact lookup."""
        bits = self._bits(key)
        with self._lock:
            self._require_open()
            if any(not self._filter[b // 8] & (1 << (b % 8)) for b in bits):
                return self._resolution(key, None, stripe=bits[0] % 65_536)
            return None

    def validate(self, result: ColdIntentResolution | ColdArchiveReceipt) -> bool:
        """Revalidate ownership and either legacy global or scoped invalidation.

        Archive receipts prove immutable, committed positive rows: later
        append-only archives cannot invalidate those rows. They remain valid
        for this owner only, never across restart or future/unpublished epochs.
        """
        with self._lock:
            self._require_open()
            if isinstance(result, ColdIntentResolution):
                if self._lookup_epochs is None:
                    if result.lookup_epoch is not None or result.generation != self._generation:
                        self._stale_lookup_proofs += 1
                        return False
                    expected = self._resolution(result.key, result.checkpoint).owner_nonce
                else:
                    stripe = self._bits(result.key)[0] % 65_536
                    if (
                        type(result.generation) is not int
                        or not 0 <= result.generation <= self._generation
                        or type(result.lookup_epoch) is not int
                        or result.lookup_epoch != self._lookup_epochs[stripe]
                    ):
                        self._stale_lookup_proofs += 1
                        return False
                    expected = self._scoped_token(
                        result.key,
                        result.checkpoint,
                        result.generation,
                        result.lookup_epoch,
                        stripe,
                    )
            else:
                if (
                    type(result.generation) is not int
                    or not 1 <= result.generation <= self._generation
                ):
                    return False
                expected = self._archive_token(result.generation, result.keys, result.digests)
            return hmac.compare_digest(result.owner_nonce, expected)

    def _resolution(
        self, key: ColdIntentKey, payload: bytes | None, *, stripe: int | None = None
    ) -> ColdIntentResolution:
        if self._lookup_epochs is not None:
            if stripe is None:
                stripe = self._bits(key)[0] % 65_536
            epoch = self._lookup_epochs[stripe]
            return ColdIntentResolution(
                key,
                self._generation,
                self._scoped_token(key, payload, self._generation, epoch, stripe),
                payload,
                epoch,
            )
        token = hmac.digest(
            self._nonce,
            msgspec.json.encode(
                (
                    "lookup",
                    self._generation,
                    key.encoded(),
                    None if payload is None else sha256(payload).digest(),
                )
            ),
            "sha256",
        )
        return ColdIntentResolution(key, self._generation, token, payload)

    def _scoped_token(
        self, key: ColdIntentKey, payload: bytes | None, generation: int, epoch: int, stripe: int
    ) -> bytes:
        return hmac.digest(
            self._nonce,
            msgspec.json.encode(
                (
                    "lookup-stripe-v1",
                    self._journal_id,
                    len(self._filter),
                    65_536,
                    stripe,
                    generation,
                    epoch,
                    key.encoded(),
                    None if payload is None else sha256(payload).digest(),
                )
            ),
            "sha256",
        )

    def _archive_token(
        self, generation: int, keys: tuple[ColdIntentKey, ...], digests: tuple[bytes, ...]
    ) -> bytes:
        return hmac.digest(
            self._nonce,
            msgspec.json.encode(("archive", generation, tuple(k.encoded() for k in keys), digests)),
            "sha256",
        )

    def resolve(self, key: ColdIntentKey) -> Future[ColdIntentResolution]:
        future: Future[ColdIntentResolution] = Future()
        future.set_running_or_notify_cancel()
        with self._lock:
            self._require_open()
            if self._pending_count >= self._capacity:
                raise ColdIntentIndexBusy("cold lookup waiter capacity is full")
            if key in self._pending:
                self._pending[key].append(future)
            else:
                try:
                    self._queue.put_nowait(key)
                except Full as exc:
                    raise ColdIntentIndexBusy("cold worker queue is full") from exc
                self._pending[key] = [future]
            self._pending_count += 1
        return future

    def archive(self, checkpoints: tuple[bytes, ...]) -> Future[ColdArchiveReceipt]:
        """Schedule validation/commit; the caller must not retire before its result."""
        if (
            not checkpoints
            or len(checkpoints) > self._maximum_batch
            or any(type(p) is not bytes or not 0 < len(p) <= 256 * 1024 for p in checkpoints)
        ):
            raise ColdIntentIndexError("cold archive batch capacity is invalid")
        future: Future[ColdArchiveReceipt] = Future()
        future.set_running_or_notify_cancel()
        with self._lock:
            self._require_open()
            try:
                self._queue.put_nowait(_Archive(checkpoints, future))
            except Full as exc:
                raise ColdIntentIndexBusy("cold worker queue is full") from exc
        return future

    def _validate_checkpoint(self, encoded: bytes) -> ColdIntentKey:
        try:
            return self._validated_checkpoint_key(encoded)
        except OmsOrderBookError as exc:
            raise ColdIntentIndexError("cold archive checkpoint validation failed") from exc

    def _validated_checkpoint_key(self, encoded: bytes) -> ColdIntentKey:
        if self._checkpoint_validator is None:
            return validate_terminal_checkpoint(encoded, self._journal_id)
        return self._checkpoint_validator((encoded,))[0]

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(str(self._path), timeout=5)
        try:
            if not self._created and db.execute("PRAGMA user_version").fetchone() != (1,):
                raise ColdIntentIndexError("cold index format version is invalid")
            if not self._created and {
                r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            } != {"meta", "intents"}:
                raise ColdIntentIndexError("cold index schema is incomplete")
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA cache_size=-8192")
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            page_limit = self._maximum_database_bytes // page_size
            if db.execute(f"PRAGMA max_page_count={page_limit}").fetchone()[0] > page_limit:
                raise ColdIntentIndexError("cold database page budget could not be enforced")
            if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise ColdIntentIndexError("cold index integrity check failed")
            db.execute(
                "CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK(id=1), journal TEXT NOT NULL, generation INTEGER NOT NULL, records INTEGER NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS intents (key BLOB PRIMARY KEY, payload BLOB NOT NULL, digest BLOB NOT NULL) WITHOUT ROWID"
            )
            db.execute("INSERT OR IGNORE INTO meta VALUES (1, ?, 0, 0)", (self._journal_id,))
            if self._created:
                db.execute("PRAGMA user_version=1")
            db.commit()
            journal, generation, count = db.execute(
                "SELECT journal,generation,records FROM meta WHERE id=1"
            ).fetchone()
            if (
                journal != self._journal_id
                or type(generation) is not int
                or generation < 0
                or type(count) is not int
                or count < 0
                or count != db.execute("SELECT count(*) FROM intents").fetchone()[0]
            ):
                raise ColdIntentIndexError("cold index identity/count mismatch")
            for key_bytes, payload, digest in db.execute("SELECT key,payload,digest FROM intents"):
                if self._stop.is_set():
                    raise ColdIntentIndexError("cold index startup stopped")
                key = self._validate_checkpoint(payload)
                if key.encoded() != key_bytes or sha256(payload).digest() != digest:
                    raise ColdIntentIndexError("cold index row checksum/identity mismatch")
                self._add_filter(key)
            self._generation, self._records = generation, count
            return db
        except BaseException:
            db.close()
            raise

    def _write(self, db: sqlite3.Connection, work: _Archive) -> ColdArchiveReceipt:
        started = monotonic_ns()
        remote = (
            self._archive_committer(work.checkpoints)
            if self._archive_committer is not None
            else None
        )
        keys = (
            remote.keys
            if remote is not None
            else (
                tuple(self._validate_checkpoint(p) for p in work.checkpoints)
                if self._checkpoint_validator is None
                else self._checkpoint_validator(work.checkpoints)
            )
        )
        if len(keys) != len(work.checkpoints):
            raise ColdArchiveOutcomeUnknown("cold validator returned incomplete coverage")
        rows = [(key, p, sha256(p).digest()) for key, p in zip(keys, work.checkpoints, strict=True)]
        if len({k for k, _, _ in rows}) != len(rows):
            if remote is not None:
                raise ColdArchiveOutcomeUnknown("remote cold commit repeated a key")
            raise ColdIntentIndexError("cold archive repeats a key")
        validated = monotonic_ns()
        # One owning worker serializes writes. Stage only bounded publication
        # metadata; no epoch is advanced until the FULL commit has returned.
        epochs: tuple[tuple[int, int], ...] = ()
        if self._lookup_epochs is not None:
            with self._lock:
                stripes = {self._bits(key)[0] % 65_536 for key in keys}
                if any(self._lookup_epochs[s] == 2**64 - 1 for s in stripes):
                    self._failed = True
                    raise ColdArchiveOutcomeUnknown("lookup invalidation epoch exhausted")
                epochs = tuple((s, self._lookup_epochs[s] + 1) for s in stripes)
        if remote is None:
            generation, records = commit_cold_rows(db, tuple(rows))
        else:
            generation, records = remote.generation, remote.records
            if (
                remote.digests != tuple(d for _, _, d in rows)
                or generation != self._generation + 1
                or not self._records <= records <= self._records + len(rows)
            ):
                raise ColdArchiveOutcomeUnknown("remote cold commit coverage mismatch")
        # A caller can only retire after this memory publication and Future result.
        committed = monotonic_ns()
        with self._lock:
            self._archive_batches += 1
            validation_ns = validated - started if remote is None else remote.validation_ns
            commit_ns = committed - validated if remote is None else remote.sql_commit_ns
            self._validation_ns += validation_ns
            self._sql_commit_ns += commit_ns
            self._max_validation_ns = max(self._max_validation_ns, validation_ns)
            self._max_sql_commit_ns = max(self._max_sql_commit_ns, commit_ns)
            try:
                if self._lookup_epochs is not None:
                    for stripe, epoch in epochs:
                        self._lookup_epochs[stripe] = epoch
                for key, _, _ in rows:
                    self._add_filter(key)
                self._generation, self._records = generation, records
            except Exception as exc:
                # SQL may already contain this whole group. No old absence
                # proof may be accepted while the publication is uncertain.
                self._failed = True
                raise ColdArchiveOutcomeUnknown("cold FULL publication failed") from exc
            keys, digests = tuple(k for k, _, _ in rows), tuple(d for _, _, d in rows)
            return ColdArchiveReceipt(
                self._generation,
                self._archive_token(self._generation, keys, digests),
                keys,
                digests,
            )

    def _lookup(self, db: sqlite3.Connection, key: ColdIntentKey) -> ColdIntentResolution:
        row = db.execute(
            "SELECT payload,digest FROM intents WHERE key=?", (key.encoded(),)
        ).fetchone()
        payload = None
        if row is not None:
            payload, digest = row
            if sha256(payload).digest() != digest or self._validate_checkpoint(payload) != key:
                raise ColdIntentIndexError("cold lookup result is corrupt")
        with self._lock:
            return self._resolution(key, payload)

    def _finish_lookup(self, key: ColdIntentKey, result: ColdIntentResolution | Exception) -> None:
        with self._lock:
            futures = self._pending.pop(key)
            self._pending_count -= len(futures)
        for future in futures:
            if not future.cancelled():
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)

    def _run(self) -> None:
        db: sqlite3.Connection | None = None
        work: ColdIntentKey | _Archive | None = None
        carry: list[ColdIntentKey | _Archive | None] = []
        group: list[_Archive] = []
        try:
            db = self._connect()
            self._ready.set_result(None)
            while True:
                work = carry.pop() if carry else self._queue.get()
                if work is None:
                    break
                if isinstance(work, _Archive):
                    group = [work]
                    checkpoint_count = len(work.checkpoints)
                    hashes = {sha256(p).digest() for p in work.checkpoints}
                    while len(group) < self._transaction_archives:
                        try:
                            following = self._queue.get_nowait()
                        except Empty:
                            break
                        following_hashes: set[bytes] = (
                            {sha256(p).digest() for p in following.checkpoints}
                            if isinstance(following, _Archive)
                            else set()
                        )
                        if (
                            not isinstance(following, _Archive)
                            or checkpoint_count + len(following.checkpoints)
                            > self._transaction_checkpoints
                            or hashes & following_hashes
                        ):
                            carry.append(following)
                            break
                        hashes.update(following_hashes)
                        checkpoint_count += len(following.checkpoints)
                        group.append(following)
                    try:
                        result = self._write(
                            db,
                            _Archive(
                                tuple(p for item in group for p in item.checkpoints), Future()
                            ),
                        )
                    except ColdIntentIndexError as exc:
                        if isinstance(exc, ColdArchiveOutcomeUnknown):
                            raise
                        # Invalid archive input rolled back; no publication/retirement.
                        for item in group:
                            item.future.set_exception(exc)
                    else:
                        offset = 0
                        for item in group:
                            end = offset + len(item.checkpoints)
                            keys, digests = result.keys[offset:end], result.digests[offset:end]
                            item.future.set_result(
                                ColdArchiveReceipt(
                                    result.generation,
                                    self._archive_token(result.generation, keys, digests),
                                    keys,
                                    digests,
                                )
                            )
                            offset = end
                    group = []
                else:
                    self._finish_lookup(work, self._lookup(db, work))
                work = None
        except Exception:
            error = ColdIntentIndexError("cold index worker failed; absence is unknown")
            with self._lock:
                self._failed = True
            if not self._ready.done():
                self._ready.set_exception(error)
            if isinstance(work, _Archive) and not work.future.done():
                work.future.set_exception(error)
            for item in [*group, *(value for value in carry if isinstance(value, _Archive))]:
                if not item.future.done():
                    item.future.set_exception(error)
            with self._lock:
                keys = tuple(self._pending)
            for key in keys:
                self._finish_lookup(key, error)
            while True:
                try:
                    pending = self._queue.get_nowait()
                except Empty:
                    break
                if isinstance(pending, _Archive) and not pending.future.done():
                    pending.future.set_exception(error)
        finally:
            if db is not None:
                db.close()
            try:
                self._finish_close()
            except BaseException:
                with self._lock:
                    self._failed = True

    def _finish_close(self) -> None:
        if not self._backend_drained:
            if self._archive_close is not None:
                self._archive_close()
            self._lease.close()
            self._backend_drained = True

    def stats(self) -> ColdIntentIndexStats:
        with self._lock:
            return ColdIntentIndexStats(
                self._generation,
                self._records,
                self._queue.qsize(),
                self._pending_count,
                len(self._filter),
                self._failed,
                self._filter_set_bits * 1_000_000 // (8 * len(self._filter)),
                self._archive_batches,
                self._validation_ns,
                self._sql_commit_ns,
                self._max_validation_ns,
                self._max_sql_commit_ns,
                len(self._lookup_epochs) * self._lookup_epochs.itemsize
                if self._lookup_epochs is not None
                else 0,
                self._stale_lookup_proofs,
            )

    def close(self) -> None:
        with self._lock:
            if self._closed and not self._thread.is_alive() and self._backend_drained:
                return
            self._closed = True
        if self._thread.is_alive():
            try:
                self._queue.put(None, timeout=10)
            except Full as exc:
                raise ColdIntentIndexError("cold index drain is blocked") from exc
            self._thread.join(timeout=30)
            if self._thread.is_alive():
                raise ColdIntentIndexError("cold index worker did not drain")
        self._finish_close()
        self._lease.close()
