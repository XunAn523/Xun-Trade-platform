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

from .journal import FileJournal, JournalAppend


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
    ) -> None:
        if capacity <= 0 or maximum_batch_size <= 0:
            raise ValueError("archive capacity and batch size must be positive")
        self._queue: Queue[JournalAppend | object] = Queue(maxsize=capacity)
        self._journal = FileJournal(path)
        self._maximum_batch_size = maximum_batch_size
        self._stop = Event()
        self._sentinel = object()
        self._lock = Lock()
        self._accepting = True
        self._offered = 0
        self._rejected = 0
        self._persisted = 0
        self._failed = 0
        self._last_error: str | None = None
        self._worker = Thread(target=self._run, name="emt-journal-archive", daemon=True)
        self._worker.start()

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
            with self._lock:
                self._rejected += 1
            return False
        return True

    def snapshot(self) -> JournalArchiveSnapshot:
        with self._lock:
            return JournalArchiveSnapshot(
                accepting=self._accepting,
                queued=self._queue.qsize(),
                offered=self._offered,
                rejected=self._rejected,
                persisted=self._persisted,
                failed=self._failed,
                last_error=self._last_error,
            )

    def close(self, *, timeout_seconds: float = 5.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("archive close timeout must be positive")
        with self._lock:
            if not self._accepting:
                return
            self._accepting = False
        self._stop.set()
        while True:
            try:
                self._queue.put(self._sentinel, timeout=timeout_seconds)
                break
            except Full:
                if not self._worker.is_alive():
                    break
        self._worker.join(timeout=timeout_seconds)
        self._journal.close()

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
            if first is self._sentinel:
                return
            assert isinstance(first, JournalAppend)
            batch = [first]
            stop_after_batch = False
            while len(batch) < self._maximum_batch_size:
                try:
                    candidate = self._queue.get_nowait()
                except Empty:
                    break
                if candidate is self._sentinel:
                    stop_after_batch = True
                    break
                assert isinstance(candidate, JournalAppend)
                batch.append(candidate)
            try:
                self._journal.append_batch(batch)
            except BaseException as exc:
                with self._lock:
                    self._failed += len(batch)
                    self._last_error = type(exc).__name__
            else:
                with self._lock:
                    self._persisted += len(batch)
            if stop_after_batch:
                return
