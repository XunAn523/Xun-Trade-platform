"""Bounded read execution; sockets and response delivery remain reactor-owned."""

from __future__ import annotations

import socket
from contextvars import Context, copy_context
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import monotonic
from typing import Callable


@dataclass(frozen=True)
class DeferredReadResultV2:
    token: int
    response: bytes | None
    error: Exception | None
    trace: dict[str, object] | None


@dataclass(frozen=True)
class _Read:
    token: int
    run: Callable[[], bytes]
    context: Context
    trace: dict[str, object] | None


class BoundedDeferredReadsV2:
    """Capacity includes queued, running AND undelivered results.

    Workers never touch client sockets or service session lifecycle. No task
    cancellation/retry is inferred from disconnects. Shutdown drains accepted
    reads; a stuck worker is an explicit failure, not a successful drain.
    """

    def __init__(self, *, workers: int, capacity: int, maximum_response_bytes: int) -> None:
        if (
            any(
                type(value) is not int or value < 1
                for value in (workers, capacity, maximum_response_bytes)
            )
            or workers > capacity
            or capacity > 1024
            or workers > 16
        ):
            raise ValueError("invalid bounded read limits")
        self._capacity = capacity
        self._maximum_response_bytes = maximum_response_bytes
        self._jobs: Queue[_Read] = Queue(capacity)
        self._done: Queue[DeferredReadResultV2] = Queue(capacity)
        self._lock = Lock()
        self._stop = Event()
        self._outstanding = 0
        self._started = False
        self._closed = False
        self.reader, self._writer = socket.socketpair()
        self.reader.setblocking(False)
        self._writer.setblocking(False)
        self._threads = tuple(
            Thread(target=self._work, name=f"consumer-cold-read-{i}", daemon=True)
            for i in range(workers)
        )

    @property
    def outstanding(self) -> int:
        with self._lock:
            return self._outstanding

    @property
    def has_completed(self) -> bool:
        return not self._done.empty()

    def start(self) -> None:
        with self._lock:
            if self._started or self._closed:
                raise RuntimeError("deferred reads cannot be restarted")
            self._started = True
        for thread in self._threads:
            thread.start()

    def offer(
        self, token: int, run: Callable[[], bytes], trace: dict[str, object] | None = None
    ) -> bool:
        with self._lock:
            if not self._started or self._closed or self._outstanding >= self._capacity:
                return False
            self._outstanding += 1
            self._jobs.put_nowait(_Read(token, run, copy_context(), trace))
            return True

    def take(self) -> DeferredReadResultV2 | None:
        try:
            result = self._done.get_nowait()
        except Empty:
            return None
        with self._lock:
            self._outstanding -= 1
        return result

    def clear_notification(self) -> None:
        try:
            while self.reader.recv(4096):
                pass
        except BlockingIOError:
            pass
        except OSError:
            if not self._closed:
                raise

    def close(self, *, timeout: float = 5.0) -> None:
        with self._lock:
            self._closed = True
            self._stop.set()
        deadline = monotonic() + timeout
        for thread in self._threads:
            if thread.is_alive():
                thread.join(max(0.0, deadline - monotonic()))
        if any(thread.is_alive() for thread in self._threads):
            raise RuntimeError("deferred read drain timed out")
        self.reader.close()
        self._writer.close()

    def _work(self) -> None:
        while True:
            try:
                task = self._jobs.get(timeout=0.05)
            except Empty:
                if self._stop.is_set():
                    return
                continue
            response = None
            error = None
            try:
                response = task.context.run(task.run)
                if len(response) > self._maximum_response_bytes:
                    raise ValueError("deferred read response exceeds its bounded frame")
            except BaseException as exc:
                response = None
                # A thread-local SystemExit/KeyboardInterrupt must not silently
                # abandon its token and hang the reactor's shutdown barrier.
                error = exc if isinstance(exc, Exception) else RuntimeError("deferred read aborted")
            self._done.put_nowait(DeferredReadResultV2(task.token, response, error, task.trace))
            self._jobs.task_done()
            try:
                self._writer.send(b"r")
            except BlockingIOError:
                # An existing notification already makes the selector readable.
                pass
