"""Asynchronous SSD archive for facts already committed to the mmap Journal.

The archive never participates in the proof required to cross a native
side-effect boundary. It receives identity-only copies after a committed mmap
frame and flushes them from a dedicated worker. A full or failed archive is
observable, but it never delays an EMT call or callback delivery.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Protocol

from .journal import JournalAppend
from .segmented_archive import SegmentedArchiveStore


class JournalArchiveSink(Protocol):
    """A non-blocking best-effort replica of already durable Journal facts."""

    def offer(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> bool:
        """Queue an identity-only archive record without waiting for SSD I/O."""

        ...


@dataclass(frozen=True)
class JournalArchiveSnapshot:
    """Observable archive progress; it is never an admission or READY proof."""

    accepting: bool
    queued: int
    offered: int
    rejected: int
    persisted: int
    failed: int
    last_error: str | None
    sealed_segments: int = 0
    retained_record_objects: int = 0
    pruned_segments: int = 0


class AsyncFileJournalArchive(JournalArchiveSink):
    """Persist identity-only mmap-Journal replicas from one bounded worker.

    ``offer`` is intentionally a ``put_nowait`` operation.  If the bounded
    in-process hint queue is full, the method returns ``False``; callers must
    expose that condition to their archive/replay supervisor, but must not
    delay an otherwise proven native call or broker callback. The committed
    mmap frame remains the process-crash fence for this profile.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        capacity: int = 16_384,
        maximum_batch_size: int = 128,
        maximum_segment_records: int = 4096,
        maximum_segment_bytes: int = 16 * 1024 * 1024,
        maximum_segments: int | None = None,
    ) -> None:
        if capacity <= 0 or maximum_batch_size <= 0:
            raise ValueError("archive capacity and batch size must be positive")
        self._queue: Queue[JournalAppend] = Queue(maxsize=capacity)
        self._journal = SegmentedArchiveStore(
            path,
            maximum_segment_records=maximum_segment_records,
            maximum_segment_bytes=maximum_segment_bytes,
            maximum_segments=maximum_segments,
        )
        self._maximum_batch_size = maximum_batch_size
        self._stop = Event()
        self._lock = Lock()
        self._close_lock = Lock()
        self._closed = False
        self._accepting = True
        self._offered = 0
        self._rejected = 0
        self._persisted = 0
        self._failed = 0
        self._last_error: str | None = None
        self._worker = Thread(target=self._run, name="emt-journal-archive", daemon=True)
        try:
            self._worker.start()
        except BaseException:
            self._journal.close()
            raise

    def offer(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> bool:
        entry = JournalAppend(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
        with self._lock:
            if not self._accepting:
                self._rejected += 1
                return False
            self._offered += 1
            try:
                self._queue.put_nowait(entry)
            except Full:
                self._rejected += 1
                return False
            return True

    def snapshot(self) -> JournalArchiveSnapshot:
        with self._lock:
            storage = self._journal.snapshot()
            return JournalArchiveSnapshot(
                accepting=self._accepting,
                queued=self._queue.qsize(),
                offered=self._offered,
                rejected=self._rejected,
                persisted=self._persisted,
                failed=self._failed,
                last_error=self._last_error,
                sealed_segments=storage.sealed_segments,
                retained_record_objects=storage.retained_record_objects,
                pruned_segments=storage.pruned_segments,
            )

    def close(self, *, timeout_seconds: float = 5.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("archive close timeout must be positive")
        with self._close_lock:
            if self._closed:
                return
            with self._lock:
                self._accepting = False
            self._stop.set()
            self._worker.join(timeout=timeout_seconds)
            if self._worker.is_alive():
                # Keep the journal open under the live writer. A later close may retry.
                raise TimeoutError("archive worker did not stop; storage remains owned")
            self._journal.close()
            self._closed = True

    def __enter__(self) -> AsyncFileJournalArchive:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _run(self) -> None:
        while True:
            try:
                first = self._queue.get(timeout=0.1)
            except Empty:
                if self._stop.is_set():
                    return
                continue
            assert isinstance(first, JournalAppend)
            batch = [first]
            while len(batch) < self._maximum_batch_size:
                try:
                    candidate = self._queue.get_nowait()
                except Empty:
                    break
                assert isinstance(candidate, JournalAppend)
                batch.append(candidate)
            try:
                self._journal.append_batch(batch)
            except BaseException as exc:
                with self._lock:
                    self._failed += len(batch)
                    self._last_error = type(exc).__name__
                    self._accepting = False
                    self._stop.set()
            else:
                with self._lock:
                    self._persisted += len(batch)
