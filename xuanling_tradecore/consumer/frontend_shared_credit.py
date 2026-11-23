"""Spawn-safe global credit for fixed multi-frontend qualification workers.

This is not a recovery mechanism.  A worker death while holding credit fences
the complete candidate; credits are never reclaimed to continue trading after
an unverified process failure.  Normal release happens only after the original
request leaves that frontend's delivery lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, MutableSequence, cast

_MAX_WORKERS = 64
_MAX_CREDIT = 128


class FrontendSharedCreditError(RuntimeError):
    """The shared frontend budget is invalid, fenced, or not drained."""


@dataclass(frozen=True)
class FrontendSharedCreditSnapshotV1:
    capacity: int
    worker_count: int
    held: int
    held_by_worker: tuple[int, ...]
    fenced: bool
    restricted_held: int
    restricted_held_by_worker: tuple[int, ...]


class FrontendSharedCreditV1:
    """One hard total across spawned frontend processes.

    ``context`` must be the exact multiprocessing spawn context used to create
    the workers.  Its synchronization objects are inherited only within that
    process tree; there is no name, network endpoint, or attach-after-crash
    path.
    """

    def __init__(self, context: object, *, capacity: int, worker_count: int) -> None:
        if (
            type(capacity) is not int
            or not 1 <= capacity <= _MAX_CREDIT
            or type(worker_count) is not int
            or not 1 <= worker_count <= _MAX_WORKERS
        ):
            raise FrontendSharedCreditError("invalid shared frontend credit dimensions")
        semaphore_factory = getattr(context, "BoundedSemaphore", None)
        lock_factory = getattr(context, "Lock", None)
        array_factory = getattr(context, "Array", None)
        if (
            not callable(semaphore_factory)
            or not callable(lock_factory)
            or not callable(array_factory)
        ):
            raise FrontendSharedCreditError("multiprocessing context lacks shared primitives")
        self._capacity = capacity
        self._worker_count = worker_count
        self._semaphore: Any = semaphore_factory(capacity)
        self._lock: Any = lock_factory()
        # [fenced, global-held, restricted-held, worker-held..., restricted-worker-held...].
        # The optional restricted class partitions existing capacity for a
        # caller-defined lane; it is not another semaphore or more credit.  The
        # explicit lock owns all multiword transitions; RawArray avoids a
        # second implicit lock.
        self._state = cast(
            MutableSequence[int],
            array_factory("Q", [0] * (worker_count * 2 + 3), lock=False),
        )

    def _worker(self, worker_id: int) -> tuple[int, int]:
        if type(worker_id) is not int or not 0 <= worker_id < self._worker_count:
            raise FrontendSharedCreditError("frontend worker outside fixed topology")
        return worker_id + 3, worker_id + 3 + self._worker_count

    def try_acquire(self, worker_id: int, *, restricted_limit: int | None = None) -> bool:
        index, restricted_index = self._worker(worker_id)
        if restricted_limit is not None and (
            type(restricted_limit) is not int
            or not 1 <= restricted_limit <= self._capacity
        ):
            raise FrontendSharedCreditError("invalid shared frontend restricted limit")
        # Do not wait on capacity.  The frontend keeps the request in its
        # existing bounded fair queue and applies the reviewed backpressure.
        if self._state[0] or not self._semaphore.acquire(False):
            return False
        acquired = True
        try:
            with self._lock:
                if self._state[0]:
                    return False
                held = int(self._state[1])
                if restricted_limit is not None and int(self._state[2]) >= restricted_limit:
                    return False
                self._state[1] = held + 1
                self._state[index] = int(self._state[index]) + 1
                if restricted_limit is not None:
                    self._state[2] = int(self._state[2]) + 1
                    self._state[restricted_index] = int(self._state[restricted_index]) + 1
                acquired = False
                return True
        finally:
            if acquired:
                self._semaphore.release()

    def release(self, worker_id: int, *, restricted: bool = False) -> None:
        index, restricted_index = self._worker(worker_id)
        with self._lock:
            worker_held, total = int(self._state[index]), int(self._state[1])
            if worker_held <= 0 or total <= 0:
                self._state[0] = 1
                raise FrontendSharedCreditError("frontend released unowned shared credit")
            if restricted and (
                int(self._state[2]) <= 0 or int(self._state[restricted_index]) <= 0
            ):
                self._state[0] = 1
                raise FrontendSharedCreditError(
                    "frontend released unowned restricted shared credit"
                )
            self._state[index] = worker_held - 1
            self._state[1] = total - 1
            if restricted:
                self._state[2] = int(self._state[2]) - 1
                self._state[restricted_index] = int(self._state[restricted_index]) - 1
        try:
            self._semaphore.release()
        except BaseException as exc:
            self.fence()
            raise FrontendSharedCreditError("shared frontend semaphore release failed") from exc

    def fence(self) -> None:
        with self._lock:
            self._state[0] = 1

    @property
    def fenced(self) -> bool:
        return bool(self._state[0])

    @property
    def capacity(self) -> int:
        """Immutable configured capacity used to size adjacent bounded stages."""

        return self._capacity

    @property
    def maybe_available(self) -> bool:
        """Lock-free scheduling hint; ``try_acquire`` remains authoritative."""

        return not self._state[0] and int(self._state[1]) < self._capacity

    def maybe_restricted_available(self, restricted_limit: int) -> bool:
        """Lock-free hint for a lane capped within the existing capacity."""

        if type(restricted_limit) is not int or not 1 <= restricted_limit <= self._capacity:
            raise FrontendSharedCreditError("invalid shared frontend restricted limit")
        return not self._state[0] and int(self._state[2]) < restricted_limit

    def snapshot(self) -> FrontendSharedCreditSnapshotV1:
        with self._lock:
            by_worker = tuple(int(self._state[index + 3]) for index in range(self._worker_count))
            restricted_by_worker = tuple(
                int(self._state[index + 3 + self._worker_count])
                for index in range(self._worker_count)
            )
            held = int(self._state[1])
            restricted_held = int(self._state[2])
            if (
                held != sum(by_worker)
                or restricted_held != sum(restricted_by_worker)
                or not 0 <= restricted_held <= held <= self._capacity
                or any(
                    restricted > total
                    for restricted, total in zip(restricted_by_worker, by_worker, strict=True)
                )
            ):
                self._state[0] = 1
                raise FrontendSharedCreditError("shared frontend credit accounting diverged")
            return FrontendSharedCreditSnapshotV1(
                self._capacity,
                self._worker_count,
                held,
                by_worker,
                bool(self._state[0]),
                restricted_held,
                restricted_by_worker,
            )

    def require_worker_drained(self, worker_id: int) -> None:
        index, restricted_index = self._worker(worker_id)
        with self._lock:
            if self._state[0] or self._state[index] or self._state[restricted_index]:
                raise FrontendSharedCreditError("frontend worker credit is fenced/not drained")

    def require_all_drained(self) -> None:
        snapshot = self.snapshot()
        if snapshot.fenced or snapshot.held or any(snapshot.held_by_worker):
            raise FrontendSharedCreditError("shared frontend credit is fenced/not drained")
