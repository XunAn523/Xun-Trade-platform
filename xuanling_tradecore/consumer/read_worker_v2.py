"""Single-flight process isolation for large immutable Consumer snapshot builds."""

from __future__ import annotations

import gc
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from multiprocessing import get_context
from threading import Lock

from ..domain import ConsumerAccountBinding, ConsumerEventPositionV2, ConsumerOrderEvent
from .request_trace_v2 import mark_request_stage, mark_request_value
from .snapshot_index_v2 import (
    ConsumerPagedSnapshotErrorV2,
    ConsumerSnapshotVersionV2,
)
from .stream_registry_v2 import CursorStreamRefV2


@dataclass(frozen=True)
class ConsumerReadWorkerStatsV2:
    published_events: int
    ready_events: int
    build_inflight: bool
    build_submissions: int
    build_failures: int
    parent_gc_compactions: int
    parent_gc_frozen_objects: int
    parent_automatic_gc_disabled: bool


def _build_snapshot_version_v2(
    chunks: tuple[tuple[ConsumerOrderEvent, ...], ...],
    position: ConsumerEventPositionV2,
) -> ConsumerSnapshotVersionV2:
    """Run only in the isolated worker process; no writer state is mutated."""

    latest: dict[str, ConsumerOrderEvent] = {}
    for chunk in chunks:
        for event in chunk:
            latest[event.oms_order_id] = event
    orders = tuple(sorted(latest.values(), key=lambda item: item.oms_order_id))
    return ConsumerSnapshotVersionV2(position, orders)


class ConsumerSnapshotReadWorkerV2:
    """Publish O(1) updates and construct at most one full version off-process."""

    def __init__(
        self,
        *,
        chunk_size: int = 1_024,
        disable_parent_automatic_gc_on_start: bool = False,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("snapshot read worker chunk size must be positive")
        self._chunk_size = chunk_size
        self._lock = Lock()
        self._frozen: list[tuple[ConsumerOrderEvent, ...]] = []
        self._open: list[ConsumerOrderEvent] = []
        self._published_position: ConsumerEventPositionV2 | None = None
        self._scope: tuple[str, str, str] | None = None
        self._scope_invalid = False
        self._published_events = 0
        self._ready_events = -1
        self._ready: ConsumerSnapshotVersionV2 | None = None
        self._future: Future[ConsumerSnapshotVersionV2] | None = None
        self._future_events = 0
        self._executor: ProcessPoolExecutor | None = None
        self._started = False
        self._submissions = 0
        self._failures = 0
        self._disable_parent_automatic_gc_on_start = (
            disable_parent_automatic_gc_on_start
        )
        self._parent_gc_compactions = 0
        self._parent_gc_frozen_objects = 0
        self._parent_gc_was_enabled: bool | None = None

    def start(self) -> None:
        with self._lock:
            if self._started:
                raise RuntimeError("snapshot read worker is already started")
            parent_gc_was_enabled: bool | None = None
            if self._disable_parent_automatic_gc_on_start:
                # This mode is reserved for the short-lived, no-Adapter UDS
                # benchmark service.  Disable cyclic GC before the large
                # immutable graph is published so no request is ever selected
                # to collect/freeze hundreds of thousands of objects.  Normal
                # reference counting remains active and close() restores the
                # process-wide setting it observed here.
                parent_gc_was_enabled = gc.isenabled()
                self._parent_gc_was_enabled = parent_gc_was_enabled
                gc.disable()
            try:
                self._executor = _new_executor()
            except Exception:
                self._parent_gc_was_enabled = None
                if parent_gc_was_enabled:
                    gc.enable()
                raise
            self._started = True

    def close(self) -> None:
        with self._lock:
            executor = self._executor
            self._executor = None
            self._started = False
            future = self._future
            self._future = None
            parent_gc_was_enabled = self._parent_gc_was_enabled
            self._parent_gc_was_enabled = None
        if future is not None:
            future.cancel()
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        if parent_gc_was_enabled:
            gc.enable()

    def publish(self, event: ConsumerOrderEvent) -> None:
        """Append one already-durable event; never wait for the read process."""

        with self._lock:
            scope = (
                event.consumer_trace.consumer_id,
                event.consumer_trace.consumer_account_id,
                event.journal_id,
            )
            if self._scope is None:
                self._scope = scope
            elif self._scope != scope:
                self._scope_invalid = True
            self._open.append(event)
            self._published_events += 1
            self._published_position = event_position_v2(event)
            if len(self._open) >= self._chunk_size:
                self._frozen.append(tuple(self._open))
                self._open.clear()

    def capture_snapshot_version(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerSnapshotVersionV2:
        """Return a current published version or start one and fail fast for retry."""

        mark_request_stage("snapshot_ipc_enqueue_started_ns")
        with self._lock:
            self._harvest_locked()
            if self._scope_invalid or (
                self._scope is not None
                and self._scope
                != (binding.consumer_id, binding.consumer_account_id, stream.journal_id)
            ):
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_NOT_READY", "snapshot read worker scope is invalid"
                )
            if (
                self._ready is not None
                and self._ready_events == self._published_events
            ):
                mark_request_value("snapshot_worker_ready_events", self._ready_events)
                mark_request_stage("snapshot_ipc_result_observed_ns")
                return self._ready
            if not self._started or self._executor is None:
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_NOT_READY", "snapshot read worker is not running", retryable=True
                )
            if self._future is None:
                position = self._published_position
                if position is None:
                    # The empty view is already bounded and needs no process work.
                    from ..domain import RedisStreamIdV1

                    position = ConsumerEventPositionV2(RedisStreamIdV1(0, 0), 0)
                chunks = tuple(self._frozen) + (tuple(self._open),)
                self._future_events = self._published_events
                self._submit_build_locked(chunks, position)
                mark_request_stage("snapshot_ipc_enqueued_ns")
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot version is building", retryable=True
            )

    def stats(self) -> ConsumerReadWorkerStatsV2:
        with self._lock:
            self._harvest_locked()
            return ConsumerReadWorkerStatsV2(
                self._published_events,
                self._ready_events,
                self._future is not None,
                self._submissions,
                self._failures,
                self._parent_gc_compactions,
                self._parent_gc_frozen_objects,
                self._parent_gc_was_enabled is not None and not gc.isenabled(),
            )

    def _harvest_locked(self) -> None:
        future = self._future
        if future is None or not future.done():
            return
        self._future = None
        try:
            self._ready = future.result()
            self._ready_events = self._future_events
        except Exception as exc:
            self._ready = None
            self._ready_events = -1
            self._failures += 1
            if isinstance(exc, BrokenProcessPool) and self._started:
                self._replace_executor_locked()

    def _submit_build_locked(
        self,
        chunks: tuple[tuple[ConsumerOrderEvent, ...], ...],
        position: ConsumerEventPositionV2,
    ) -> None:
        for _attempt in range(2):
            executor = self._executor
            if not self._started or executor is None:
                break
            try:
                self._future = executor.submit(_build_snapshot_version_v2, chunks, position)
            except BrokenProcessPool:
                self._failures += 1
                self._replace_executor_locked()
                continue
            self._submissions += 1
            return
        raise ConsumerPagedSnapshotErrorV2(
            "SNAPSHOT_NOT_READY",
            "snapshot read worker is restarting",
            retryable=True,
        )

    def _replace_executor_locked(self) -> None:
        old_executor = self._executor
        self._executor = _new_executor() if self._started else None
        if old_executor is not None:
            old_executor.shutdown(wait=False, cancel_futures=True)


def event_position_v2(event: ConsumerOrderEvent) -> ConsumerEventPositionV2:
    from ..domain import RedisStreamIdV1

    return ConsumerEventPositionV2(RedisStreamIdV1(event.journal_lsn, 0), 0)


def _new_executor() -> ProcessPoolExecutor:
    # Never fork a live UDS reactor or its trace/overload threads.
    return ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn"))
