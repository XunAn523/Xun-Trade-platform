"""Bounded asynchronous lookup notifications, never background OMS admission."""

from __future__ import annotations

import socket
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Condition
from time import monotonic
from typing import Generator

from ..oms.cold_intent import ColdIntentKey, ColdIntentResolution, ColdIntentResolver
from .base import ConsumerGatewayError


class ConsumerColdLookupRequiredV2(ConsumerGatewayError):
    """Raised strictly before a new fact/hold; transport may suspend this frame."""

    def __init__(self, key: ColdIntentKey, resolver: ColdIntentResolver) -> None:
        super().__init__("cold admission lookup required")
        self.key, self.resolver = key, resolver


class ConsumerAdmissionCapacityV2(ConsumerGatewayError):
    """Bounded OMS live state cannot accept another new intent."""


@dataclass(frozen=True)
class ColdAdmissionRequestV2:
    key: ColdIntentKey
    resolver: ColdIntentResolver


@dataclass(frozen=True)
class ColdAdmissionResumeV2:
    required: ColdAdmissionRequestV2
    resolution: ColdIntentResolution


_RESUME: ContextVar[ColdAdmissionResumeV2 | None] = ContextVar(
    "cold_admission_resume", default=None
)


def admission_resolution(resolver: ColdIntentResolver) -> ColdIntentResolution | None:
    resume = _RESUME.get()
    return (
        resume.resolution if resume is not None and resume.required.resolver is resolver else None
    )


@contextmanager
def cold_admission_scope(resume: ColdAdmissionResumeV2 | None) -> Generator[None, None, None]:
    token = _RESUME.set(resume)
    try:
        yield
    finally:
        _RESUME.reset(token)


@dataclass(frozen=True)
class ColdAdmissionCompletionV2:
    token: int
    required: ColdAdmissionRequestV2
    resolution: ColdIntentResolution | None
    error: Exception | None


class BoundedColdAdmissionsV2:
    """Slots remain reserved through the WRITE-ready queue, until explicit ack.

    Future callbacks only enqueue immutable lookup results and notify a socket.
    They must never authorize, reserve funds, append facts or dispatch an Adapter.
    """

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 1024:
            raise ValueError("invalid cold admission capacity")
        self._capacity = capacity
        self._done: Queue[ColdAdmissionCompletionV2] = Queue(capacity)
        self._condition = Condition()
        self._tokens: set[int] = set()
        self._pending = 0
        self._closed = False
        self.reader, self._writer = socket.socketpair()
        self.reader.setblocking(False)
        self._writer.setblocking(False)

    @property
    def outstanding(self) -> int:
        with self._condition:
            return len(self._tokens)

    @property
    def has_completed(self) -> bool:
        return not self._done.empty()

    def offer(self, token: int, required: ConsumerColdLookupRequiredV2) -> bool:
        # Never retain a raised exception/traceback across suspension: it owns
        # stack frames (potentially the entire reactor state), not just a key.
        request = ColdAdmissionRequestV2(required.key, required.resolver)
        with self._condition:
            if self._closed or len(self._tokens) >= self._capacity:
                return False
            if token in self._tokens:
                raise ValueError("duplicate cold admission token")
            self._tokens.add(token)
            self._pending += 1
        try:
            future = request.resolver.resolve(request.key)
        except BaseException:
            with self._condition:
                self._tokens.remove(token)
                self._pending -= 1
                self._condition.notify_all()
            raise
        future.add_done_callback(lambda done: self._complete(token, request, done))
        return True

    def _complete(
        self,
        token: int,
        required: ColdAdmissionRequestV2,
        future: Future[ColdIntentResolution],
    ) -> None:
        result = None
        error = None
        try:
            # add_done_callback guarantees completion: this never waits for I/O.
            result = future.result()
            if result.key != required.key or (
                result.checkpoint is not None and len(result.checkpoint) > 256 * 1024
            ):
                raise ValueError("cold lookup result scope/capacity mismatch")
        except BaseException:
            result = None
            # Preserve failure, not private exception text or its storage stack.
            error = RuntimeError("cold lookup evidence unavailable")
        self._done.put_nowait(ColdAdmissionCompletionV2(token, required, result, error))
        try:
            self._writer.send(b"c")
        except BlockingIOError:
            pass
        finally:
            with self._condition:
                self._pending -= 1
                self._condition.notify_all()

    def take(self) -> ColdAdmissionCompletionV2 | None:
        try:
            return self._done.get_nowait()
        except Empty:
            return None

    def release(self, token: int) -> None:
        with self._condition:
            self._tokens.remove(token)

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
        deadline = monotonic() + timeout
        with self._condition:
            self._closed = True
            while self._pending:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise RuntimeError("cold lookup drain timed out")
                self._condition.wait(remaining)
        self.reader.close()
        self._writer.close()
