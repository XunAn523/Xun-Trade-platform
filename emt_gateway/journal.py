"""Durable journal implementations used by command and callback paths.

``FileJournal`` deliberately persists command identity and a SHA-256 payload
digest, never an account credential or business payload.  It provides strict
write-through semantics: :meth:`append` returns only after the framed record
has been flushed with :func:`os.fsync`.  The gateway therefore fails closed
before an EMT call when the journal cannot prove durability.

The in-memory implementation remains a deterministic test double only.
"""

from __future__ import annotations

import os
import re
import struct
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from threading import Condition, Event, Lock, Thread
from time import perf_counter_ns, sleep
from typing import Protocol, Sequence

from xuntrade_omsems.compute import crc32c

from .locking import JournalLease, LeaseError


class JournalFailure(RuntimeError):
    """A journal could not provide the durable evidence required by the caller."""


@dataclass(frozen=True)
class JournalRecord:
    lsn: int
    kind: str
    command_id: str | None
    correlation_id: str
    payload_hash: str
    monotonic_ns: int


@dataclass(frozen=True)
class JournalAppend:
    """One identity-only record submitted to a durable journal."""

    kind: str
    command_id: str | None
    correlation_id: str
    payload_hash: str


@dataclass(frozen=True)
class JournalAppendTiming:
    """Breakdown of one completed durable journal batch.

    All values are measured in the caller that completed the batch.  The
    ``flush_ns`` value includes the platform durability primitive
    (``FlushFileBuffers`` on Windows through :func:`os.fsync`); it is kept
    separate from record construction so a storage tail cannot be mistaken for
    Python execution time.
    """

    entry_count: int
    validation_and_encoding_ns: int
    lock_wait_ns: int
    frame_encoding_ns: int
    write_ns: int
    flush_ns: int
    total_ns: int


class DurableJournal(Protocol):
    """A record is returned only after its journal-specific durable commit."""

    def append(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord: ...


class BatchDurableJournal(DurableJournal, Protocol):
    """Optional extension: all returned records share one durable flush."""

    def append_batch(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]: ...


_FILE_MAGIC = b"EMTJ"
_COMMIT_MAGIC = b"ECMT"
_FORMAT_VERSION = 1
_HEADER = struct.Struct("!4sBQQHHH32s")
_COMMIT = struct.Struct("!4sQI")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")


class FileJournal:
    """Append-only, recoverable, strict-durability binary journal.

    Every frame is ``header | UTF-8 identity fields | CRC32C commit marker``.
    On open, only the contiguous CRC-verified prefix is retained; an incomplete
    or corrupt tail is truncated while holding the journal instance lock.
    This class records hashes rather than full commands, so it is an audit and
    ambiguity-evidence journal, not yet a command-replay store.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_record_bytes: int = 4096,
        exclusive: bool = True,
        timing_history_capacity: int = 0,
    ) -> None:
        if max_record_bytes < 1 or max_record_bytes > 0xFFFF * 3:
            raise ValueError("max_record_bytes must fit the binary frame")
        if not 0 <= timing_history_capacity <= 100_000:
            raise ValueError("timing_history_capacity must be in 0..100000")
        self.path = Path(path)
        self._max_record_bytes = max_record_bytes
        self._lock = Lock()
        self._records: list[JournalRecord] = []
        self._next_lsn = 1
        self._closed = False
        self._failed = False
        self._last_flush_ns: int | None = None
        self._last_append_timing: JournalAppendTiming | None = None
        self._timing_history: deque[JournalAppendTiming] = deque(
            maxlen=timing_history_capacity or None
        )
        self._timing_history_enabled = timing_history_capacity > 0
        self._lease = (
            JournalLease(self.path.with_name(f"{self.path.name}.lock")) if exclusive else None
        )

        try:
            if self._lease is not None:
                self._lease.acquire()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # Journal writes are already explicitly followed by ``os.fsync``.
            # Keeping the handle unbuffered removes a second Python-level
            # buffer copy and lets each batch be emitted as one contiguous OS
            # write. Recovery still uses the same seekable handle.
            self._file = self.path.open("a+b", buffering=0)
            with self._lock:
                self._recover_locked()
        except (OSError, LeaseError) as exc:
            opened_file = getattr(self, "_file", None)
            if opened_file is not None:
                try:
                    opened_file.close()
                except OSError:
                    pass
            if self._lease is not None:
                self._lease.close()
            raise JournalFailure(f"cannot open or recover journal {self.path}") from exc

    @property
    def last_flush_ns(self) -> int | None:
        """Elapsed time of the most recent successful write-through append."""

        with self._lock:
            return self._last_flush_ns

    @property
    def last_append_timing(self) -> JournalAppendTiming | None:
        """Return the latest completed append breakdown, if any.

        This is diagnostic-only. A timing is published only after the
        corresponding flush succeeds and cannot weaken durability semantics.
        """

        with self._lock:
            return self._last_append_timing

    def timing_history(self) -> tuple[JournalAppendTiming, ...]:
        """Return bounded diagnostic samples captured at durable commits.

        History is disabled by default so production paths allocate no
        unbounded telemetry. Benchmark drivers may explicitly request a
        bounded history to distinguish storage flush time from queueing.
        """

        with self._lock:
            return tuple(self._timing_history)

    def append(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord:
        return self.append_batch((JournalAppend(kind, command_id, correlation_id, payload_hash),))[
            0
        ]

    def append_batch(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]:
        """Append identity-only frames then issue one write-through flush.

        The records become visible only after the shared ``fsync`` succeeds.
        If a write fails, recovery retains only the verified prefix and this
        journal instance fails closed just as it does for a single append.
        """

        if not entries:
            return ()
        operation_started_ns = perf_counter_ns()
        encoded = [
            (
                *self._validate_fields(
                    kind=entry.kind,
                    command_id=entry.command_id,
                    correlation_id=entry.correlation_id,
                    payload_hash=entry.payload_hash,
                ),
                entry,
            )
            for entry in entries
        ]
        prepared_ns = perf_counter_ns()
        lock_requested_ns = prepared_ns
        with self._lock:
            self._ensure_writable_locked()
            lock_acquired_ns = perf_counter_ns()
            records = tuple(
                JournalRecord(
                    lsn=self._next_lsn + index,
                    kind=entry.kind,
                    command_id=entry.command_id,
                    correlation_id=entry.correlation_id,
                    payload_hash=entry.payload_hash,
                    monotonic_ns=perf_counter_ns(),
                )
                for index, (*_, entry) in enumerate(encoded)
            )
            frames = tuple(
                self._encode_frame(
                    record, kind_bytes, command_id_bytes, correlation_id_bytes, digest
                )
                for record, (kind_bytes, command_id_bytes, correlation_id_bytes, digest, _) in zip(
                    records, encoded
                )
            )
            frame_encoded_ns = perf_counter_ns()
            # A single, contiguous append avoids repeated Python-to-kernel
            # transitions for the small framed records in a group. The file is
            # opened in append mode, so no seek-to-EOF syscall is required on
            # the hot path.
            payload = b"".join(frames)
            try:
                write_started_ns = perf_counter_ns()
                self._write_all_locked(payload)
                write_completed_ns = perf_counter_ns()
                self._file.flush()
                # Python maps this to the platform synchronous file flush
                # primitive (FlushFileBuffers on Windows; fsync on Linux).
                os.fsync(self._file.fileno())
            except OSError as exc:
                # A possibly partial frame is intentionally not reused in this
                # process.  A restart will retain only the verified prefix.
                self._failed = True
                raise JournalFailure(
                    "journal write-through failed; gateway must remain closed"
                ) from exc

            completed_ns = perf_counter_ns()
            write_ns = write_completed_ns - write_started_ns
            flush_ns = completed_ns - write_completed_ns
            self._last_flush_ns = write_ns + flush_ns
            self._last_append_timing = JournalAppendTiming(
                entry_count=len(records),
                validation_and_encoding_ns=prepared_ns - operation_started_ns,
                lock_wait_ns=lock_acquired_ns - lock_requested_ns,
                frame_encoding_ns=frame_encoded_ns - lock_acquired_ns,
                write_ns=write_ns,
                flush_ns=flush_ns,
                total_ns=completed_ns - operation_started_ns,
            )
            if self._timing_history_enabled:
                self._timing_history.append(self._last_append_timing)
            self._records.extend(records)
            self._next_lsn += len(records)
            return records

    def _write_all_locked(self, payload: bytes) -> None:
        """Write a complete encoded batch while holding the journal lock."""

        offset = 0
        while offset < len(payload):
            written = self._file.write(payload[offset:])
            if written is None:
                # ``RawIOBase`` may return ``None`` only for a non-blocking
                # stream. A journal must never claim durability from it.
                raise OSError("journal write would block")
            if written <= 0:
                raise OSError(f"short journal write: {written}/{len(payload) - offset} bytes")
            offset += written

    def records(self) -> tuple[JournalRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._file.close()
            finally:
                self._closed = True
                if self._lease is not None:
                    self._lease.close()

    def __enter__(self) -> FileJournal:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _recover_locked(self) -> None:
        self._file.seek(0)
        valid_end = 0
        expected_lsn = 1
        recovered: list[JournalRecord] = []

        while True:
            header = self._file.read(_HEADER.size)
            if not header:
                break
            if len(header) != _HEADER.size:
                break
            try:
                (
                    magic,
                    version,
                    lsn,
                    monotonic_ns,
                    kind_size,
                    command_id_size,
                    correlation_id_size,
                    digest,
                ) = _HEADER.unpack(header)
            except struct.error:
                break
            body_size = kind_size + command_id_size + correlation_id_size
            if (
                magic != _FILE_MAGIC
                or version != _FORMAT_VERSION
                or lsn != expected_lsn
                or not kind_size
                or not correlation_id_size
                or body_size > self._max_record_bytes
            ):
                break
            body = self._file.read(body_size)
            commit = self._file.read(_COMMIT.size)
            if len(body) != body_size or len(commit) != _COMMIT.size:
                break
            try:
                commit_magic, commit_lsn, commit_crc32c = _COMMIT.unpack(commit)
            except struct.error:
                break
            if (
                commit_magic != _COMMIT_MAGIC
                or commit_lsn != lsn
                or commit_crc32c != crc32c(header + body)
            ):
                break
            try:
                record = self._decode_record(
                    lsn=lsn,
                    monotonic_ns=monotonic_ns,
                    digest=digest,
                    body=body,
                    kind_size=kind_size,
                    command_id_size=command_id_size,
                    correlation_id_size=correlation_id_size,
                )
            except (UnicodeDecodeError, JournalFailure):
                break
            recovered.append(record)
            valid_end = self._file.tell()
            expected_lsn += 1

        self._file.seek(0, os.SEEK_END)
        if self._file.tell() != valid_end:
            self._file.seek(valid_end)
            self._file.truncate()
            self._file.flush()
            os.fsync(self._file.fileno())
        self._file.seek(0, os.SEEK_END)
        self._records = recovered
        self._next_lsn = expected_lsn

    def _ensure_writable_locked(self) -> None:
        if self._closed:
            raise JournalFailure("journal is closed")
        if self._failed:
            raise JournalFailure("journal previously failed; restart and recover before retrying")

    def _validate_fields(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> tuple[bytes, bytes, bytes, bytes]:
        if not isinstance(kind, str) or not kind:
            raise JournalFailure("journal kind is required")
        if command_id is not None and (not isinstance(command_id, str) or not command_id):
            raise JournalFailure("command_id must be a non-empty string or None")
        if not isinstance(correlation_id, str) or not correlation_id:
            raise JournalFailure("correlation_id is required")
        if not isinstance(payload_hash, str) or not _SHA256_HEX.fullmatch(payload_hash):
            raise JournalFailure("payload_hash must be a lowercase SHA-256 hexadecimal digest")
        try:
            kind_bytes = kind.encode("utf-8")
            command_id_bytes = b"" if command_id is None else command_id.encode("utf-8")
            correlation_id_bytes = correlation_id.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise JournalFailure("journal identity fields must be UTF-8 encodable") from exc
        body_size = len(kind_bytes) + len(command_id_bytes) + len(correlation_id_bytes)
        if (
            not kind_bytes
            or not correlation_id_bytes
            or len(kind_bytes) > 0xFFFF
            or len(command_id_bytes) > 0xFFFF
            or len(correlation_id_bytes) > 0xFFFF
            or body_size > self._max_record_bytes
        ):
            raise JournalFailure("journal identity fields exceed frame limits")
        return kind_bytes, command_id_bytes, correlation_id_bytes, bytes.fromhex(payload_hash)

    def _encode_frame(
        self,
        record: JournalRecord,
        kind: bytes,
        command_id: bytes,
        correlation_id: bytes,
        digest: bytes,
    ) -> bytes:
        header = _HEADER.pack(
            _FILE_MAGIC,
            _FORMAT_VERSION,
            record.lsn,
            record.monotonic_ns,
            len(kind),
            len(command_id),
            len(correlation_id),
            digest,
        )
        body = kind + command_id + correlation_id
        return header + body + _COMMIT.pack(_COMMIT_MAGIC, record.lsn, crc32c(header + body))

    def _decode_record(
        self,
        *,
        lsn: int,
        monotonic_ns: int,
        digest: bytes,
        body: bytes,
        kind_size: int,
        command_id_size: int,
        correlation_id_size: int,
    ) -> JournalRecord:
        kind_end = kind_size
        command_id_end = kind_end + command_id_size
        correlation_id_end = command_id_end + correlation_id_size
        kind = body[:kind_end].decode("utf-8")
        command_id = body[kind_end:command_id_end].decode("utf-8") or None
        correlation_id = body[command_id_end:correlation_id_end].decode("utf-8")
        payload_hash = digest.hex()
        self._validate_fields(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
        return JournalRecord(
            lsn=lsn,
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
            monotonic_ns=monotonic_ns,
        )


class InMemoryJournal:
    """Thread-safe test double; never use it as a production durability claim."""

    def __init__(self) -> None:
        self._records: list[JournalRecord] = []
        self._lock = Lock()

    def append(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord:
        return self.append_batch((JournalAppend(kind, command_id, correlation_id, payload_hash),))[
            0
        ]

    def append_batch(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]:
        if not entries:
            return ()
        if any(
            not entry.kind or not entry.correlation_id or not entry.payload_hash
            for entry in entries
        ):
            raise JournalFailure("journal record identity fields are required")
        with self._lock:
            records = tuple(
                JournalRecord(
                    lsn=len(self._records) + index + 1,
                    kind=entry.kind,
                    command_id=entry.command_id,
                    correlation_id=entry.correlation_id,
                    payload_hash=entry.payload_hash,
                    monotonic_ns=perf_counter_ns(),
                )
                for index, entry in enumerate(entries)
            )
            self._records.extend(records)
            return records

    def records(self) -> tuple[JournalRecord, ...]:
        with self._lock:
            return tuple(self._records)


@dataclass
class _PendingAppend:
    entry: JournalAppend
    ready: Event
    record: JournalRecord | None = None
    error: BaseException | None = None


class GroupCommitJournal:
    """Bounded concurrent append adapter with durable group commit.

    Every caller still blocks until its own record belongs to a completed
    durable flush.  Grouping therefore changes only the number of flushes, not
    the durable-before-native or durable-before-ACK contract.  A crash after a
    returned record retains the entire committed group; a crash before it
    returns retains no unacknowledged durability claim.
    """

    def __init__(
        self,
        journal: DurableJournal,
        *,
        max_batch_size: int = 64,
        max_wait_seconds: float = 0.001,
        direct_when_idle: bool = False,
        priority_kinds: Sequence[str] = (),
    ) -> None:
        if not 1 <= max_batch_size <= 1024:
            raise ValueError("max_batch_size must be in 1..1024")
        if not 0 <= max_wait_seconds <= 0.010:
            raise ValueError("max_wait_seconds must be in [0, 0.010]")
        if any(not isinstance(kind, str) or not kind for kind in priority_kinds):
            raise ValueError("priority_kinds must contain non-empty strings")
        self._journal = journal
        self._max_batch_size = max_batch_size
        self._max_wait_seconds = max_wait_seconds
        self._direct_when_idle = direct_when_idle
        self._priority_kinds = frozenset(priority_kinds)
        self._pending: list[_PendingAppend] = []
        self._condition = Condition(Lock())
        self._closed = False
        self._worker_failure: BaseException | None = None
        self._direct_inflight = False
        self._direct_idle = Event()
        self._direct_idle.set()
        self._worker = Thread(target=self._run, name="emt-journal-group-commit", daemon=True)
        self._worker.start()

    @property
    def last_flush_ns(self) -> int | None:
        return getattr(self._journal, "last_flush_ns", None)

    @property
    def last_append_timing(self) -> JournalAppendTiming | None:
        return getattr(self._journal, "last_append_timing", None)

    def append(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord:
        return self.append_batch((JournalAppend(kind, command_id, correlation_id, payload_hash),))[
            0
        ]

    def append_batch(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]:
        if not entries:
            return ()
        if len(entries) == 1 and self._direct_when_idle:
            direct = self._append_direct_when_idle(entries)
            if direct is not None:
                return direct
        pending = [_PendingAppend(entry, Event()) for entry in entries]
        with self._condition:
            if self._closed:
                raise JournalFailure("group-commit journal is closed")
            if self._worker_failure is not None:
                raise JournalFailure("group-commit journal worker failed") from self._worker_failure
            self._pending.extend(pending)
            self._condition.notify()
        for item in pending:
            item.ready.wait()
            if item.error is not None:
                raise JournalFailure("group-commit journal append failed") from item.error
        records = tuple(item.record for item in pending)
        assert all(record is not None for record in records)
        return tuple(record for record in records if record is not None)

    def _append_direct_when_idle(
        self,
        entries: Sequence[JournalAppend],
    ) -> tuple[JournalRecord, ...] | None:
        """Avoid the worker hand-off when a low-rate journal is uncontended.

        The direct branch is still a synchronous append to the same delegate,
        so it has exactly the same durability contract.  A concurrently
        arriving writer falls back to the bounded batching worker instead.
        """

        with self._condition:
            if self._closed:
                raise JournalFailure("group-commit journal is closed")
            if self._worker_failure is not None:
                raise JournalFailure("group-commit journal worker failed") from self._worker_failure
            if self._pending or self._direct_inflight:
                return None
            self._direct_inflight = True
            self._direct_idle.clear()
        try:
            return self._append_delegate(entries)
        finally:
            with self._condition:
                self._direct_inflight = False
                self._direct_idle.set()
                self._condition.notify_all()

    def records(self) -> tuple[JournalRecord, ...]:
        records = getattr(self._journal, "records", None)
        if not callable(records):
            raise JournalFailure("wrapped journal does not expose records")
        return tuple(records())

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()
        self._worker.join(timeout=2.0)
        if self._worker.is_alive():
            raise JournalFailure("group-commit journal worker did not stop")
        if not self._direct_idle.wait(timeout=2.0):
            raise JournalFailure("group-commit journal direct append did not stop")

    def _run(self) -> None:
        try:
            while True:
                with self._condition:
                    while not self._pending and not self._closed:
                        self._condition.wait()
                    if not self._pending and self._closed:
                        return
                    deadline_ns = perf_counter_ns() + int(self._max_wait_seconds * 1_000_000_000)
                    while (
                        self._max_wait_seconds > 0
                        and len(self._pending) < self._max_batch_size
                        and not self._closed
                    ):
                        remaining_ns = deadline_ns - perf_counter_ns()
                        if remaining_ns <= 0:
                            break
                        if remaining_ns <= 2_000_000:
                            # Windows' timed condition wait is normally bound
                            # to the ~15.6 ms scheduler tick.  A one-millisecond
                            # group-commit bound must not silently become a
                            # 15 ms admission stall, so briefly release the
                            # mutex and yield instead.  Producers can enqueue
                            # while the mutex is released and every caller
                            # still waits for the following durable flush.
                            self._condition.release()
                            try:
                                sleep(0)
                            finally:
                                self._condition.acquire()
                            continue
                        self._condition.wait(remaining_ns / 1_000_000_000)
                    batch = self._take_batch_locked()
                self._commit(batch)
        except BaseException as exc:
            with self._condition:
                self._worker_failure = exc
                stranded = self._pending
                self._pending = []
            for item in stranded:
                item.error = exc
                item.ready.set()

    def _commit(self, pending: list[_PendingAppend]) -> None:
        try:
            records = self._append_delegate(tuple(item.entry for item in pending))
            if len(records) != len(pending):
                raise JournalFailure("group-commit journal returned an incomplete batch")
        except BaseException as exc:
            for item in pending:
                item.error = exc
                item.ready.set()
            return
        for item, record in zip(pending, records):
            item.record = record
            item.ready.set()

    def _take_batch_locked(self) -> list[_PendingAppend]:
        """Take one bounded group, prioritising pre-native evidence safely.

        ``CALL_STARTED`` is the only record which authorizes crossing the
        native side-effect boundary. It may overtake a durable outcome record
        for a *different* command, but never an earlier record for the same
        command (that outcome cannot exist before its own CALL_STARTED).
        Reserve one quarter of a mixed group for ordinary records so outcome
        evidence cannot starve during sustained command admission.
        """

        if not self._priority_kinds:
            batch = self._pending[: self._max_batch_size]
        else:
            priority = [item for item in self._pending if item.entry.kind in self._priority_kinds]
            ordinary = [
                item for item in self._pending if item.entry.kind not in self._priority_kinds
            ]
            if priority and ordinary:
                priority_limit = max(1, (self._max_batch_size * 3) // 4)
                batch = priority[:priority_limit]
                batch.extend(ordinary[: self._max_batch_size - len(batch)])
            else:
                batch = self._pending[: self._max_batch_size]
        selected = {id(item) for item in batch}
        self._pending = [item for item in self._pending if id(item) not in selected]
        return batch

    def _append_delegate(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]:
        append_batch = getattr(self._journal, "append_batch", None)
        if callable(append_batch):
            return tuple(append_batch(entries))
        return tuple(
            self._journal.append(
                kind=entry.kind,
                command_id=entry.command_id,
                correlation_id=entry.correlation_id,
                payload_hash=entry.payload_hash,
            )
            for entry in entries
        )
