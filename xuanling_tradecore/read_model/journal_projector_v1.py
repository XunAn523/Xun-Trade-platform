"""Validated Journal-to-read-model handoff, intentionally outside C0--C7."""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock, Thread
from time import monotonic_ns
from typing import Callable, Iterable, Protocol

from ..oms.sidecar_v1 import OmsSidecarFactReaderV1, OmsSidecarFactV1
from .postgres_projector_v1 import ReadModelProjectionCommitV1
from .projection_v1 import (
    ReadModelPositionV1,
    ReadModelProjectionBatchV1,
    ReadModelProjectionErrorV1,
)


class JournalFirstProjectionErrorV1(ReadModelProjectionErrorV1):
    """A read-model worker was offered facts without a proven Journal boundary."""


@dataclass(frozen=True)
class CommittedReadModelFactV1:
    """One source fact whose fact reader has already proved continuity.

    ``sidecar_fact`` is deliberately the existing compact F1 fact contract;
    the separate position and hash establish the durable replay identity used
    by F3.  A domain mapper decides which non-authoritative read rows are safe
    to project.  No mapper can use this object to call an Adapter.
    """

    sidecar_fact: OmsSidecarFactV1
    position: ReadModelPositionV1
    source_fact_hash: str

    def __post_init__(self) -> None:
        if self.source_fact_hash != self.sidecar_fact.fingerprint:
            raise JournalFirstProjectionErrorV1(
                "read-model fact hash does not match its committed source fact"
            )


class CommittedReadModelFactMapperV1(Protocol):
    """Translate verified facts to a transaction-shaped, non-execution batch."""

    def map_committed_facts(
        self,
        *,
        facts: tuple[CommittedReadModelFactV1, ...],
        expected_checkpoint: ReadModelPositionV1 | None,
    ) -> ReadModelProjectionBatchV1: ...


class ReadModelProjectionStoreV1(Protocol):
    """The transactional persistence seam used by the F3 background worker."""

    def apply(self, batch: ReadModelProjectionBatchV1) -> ReadModelProjectionCommitV1 | None: ...


class JournalFirstReadModelProjectorV1:
    """Project only validated fact suffixes; never emit or authorize an action."""

    def __init__(
        self,
        *,
        fact_reader: OmsSidecarFactReaderV1,
        mapper: CommittedReadModelFactMapperV1,
        store: ReadModelProjectionStoreV1,
        expected_checkpoint: ReadModelPositionV1 | None = None,
    ) -> None:
        self._fact_reader = fact_reader
        self._mapper = mapper
        self._store = store
        self._checkpoint = expected_checkpoint

    @property
    def checkpoint(self) -> ReadModelPositionV1 | None:
        """Return the last successfully committed F3 checkpoint position."""

        return self._checkpoint

    def project(
        self, facts: Iterable[CommittedReadModelFactV1]
    ) -> ReadModelProjectionCommitV1 | None:
        """Validate a contiguous suffix, then commit its rows and checkpoint together."""

        source = tuple(facts)
        if not source:
            return None
        reader_watermark = self._fact_reader.watermark
        try:
            committed_sidecar = self._fact_reader.read_committed(
                item.sidecar_fact for item in source
            )
            if len(committed_sidecar) != len(source):
                raise JournalFirstProjectionErrorV1(
                    "read-model projector received a duplicate or uncommitted fact boundary"
                )
            for prior, current in zip(source, source[1:]):
                if current.position <= prior.position:
                    raise JournalFirstProjectionErrorV1(
                        "read-model Journal position is not strictly increasing"
                    )
            batch = self._mapper.map_committed_facts(
                facts=source,
                expected_checkpoint=self._checkpoint,
            )
            if batch.expected_checkpoint != self._checkpoint:
                raise JournalFirstProjectionErrorV1(
                    "mapper changed the verified checkpoint predecessor"
                )
            if batch.checkpoint.position != source[-1].position:
                raise JournalFirstProjectionErrorV1(
                    "mapper checkpoint does not end at the fact suffix"
                )
            if batch.checkpoint.source_fact_hash != source[-1].source_fact_hash:
                raise JournalFirstProjectionErrorV1(
                    "mapper checkpoint hash differs from source fact"
                )
            commit = self._store.apply(batch)
        except Exception:
            self._fact_reader.rewind_to(reader_watermark)
            raise
        self._checkpoint = batch.checkpoint.position
        return commit


@dataclass(frozen=True)
class ReadModelProjectorHealthV1:
    """Payload-free health information suitable for F3 stale/lag reporting."""

    queued_batches: int
    accepted_batches: int
    rejected_batches: int
    completed_batches: int
    last_error_code: str | None
    last_completion_monotonic_ns: int | None


class BoundedReadModelProjectorQueueV1:
    """A bounded background handoff whose full queue never blocks execution paths.

    ``offer`` uses only an in-process lock and list append/pop.  The caller is
    required to treat ``False`` as observability lag: it must not retry inline,
    wait for PostgreSQL, or change an EMT admission decision.
    """

    def __init__(
        self,
        *,
        projector: JournalFirstReadModelProjectorV1,
        capacity: int,
        on_health_change: Callable[[ReadModelProjectorHealthV1], None] | None = None,
    ) -> None:
        if type(capacity) is not int or capacity < 1:
            raise JournalFirstProjectionErrorV1("read-model queue capacity is invalid")
        self._projector = projector
        self._capacity = capacity
        self._on_health_change = on_health_change
        self._lock = RLock()
        self._pending: list[tuple[CommittedReadModelFactV1, ...]] = []
        self._accepted = 0
        self._rejected = 0
        self._completed = 0
        self._last_error_code: str | None = None
        self._last_completion_monotonic_ns: int | None = None

    def offer(self, facts: Iterable[CommittedReadModelFactV1]) -> bool:
        """Offer a batch without any database wait; return false on bounded lag."""

        frozen = tuple(facts)
        if not frozen:
            raise JournalFirstProjectionErrorV1("read-model queue batch is empty")
        with self._lock:
            if len(self._pending) >= self._capacity:
                self._rejected += 1
                self._publish_health_locked()
                return False
            self._pending.append(frozen)
            self._accepted += 1
            self._publish_health_locked()
            return True

    def drain_once(self) -> bool:
        """Run one queued batch in the caller's background worker context."""

        with self._lock:
            if not self._pending:
                return False
            batch = self._pending.pop(0)
            self._publish_health_locked()
        try:
            self._projector.project(batch)
        except Exception as exc:
            with self._lock:
                self._pending.insert(0, batch)
                self._last_error_code = exc.__class__.__name__
                self._publish_health_locked()
            raise
        with self._lock:
            self._completed += 1
            self._last_error_code = None
            self._last_completion_monotonic_ns = monotonic_ns()
            self._publish_health_locked()
        return True

    def start_one_shot_background_drain(self) -> Thread:
        """Start one background drain thread; offered work remains out of C0--C7."""

        worker = Thread(target=self.drain_once, name="tradecore-f3-projector", daemon=True)
        worker.start()
        return worker

    def health(self) -> ReadModelProjectorHealthV1:
        with self._lock:
            return self._health_locked()

    def _health_locked(self) -> ReadModelProjectorHealthV1:
        return ReadModelProjectorHealthV1(
            queued_batches=len(self._pending),
            accepted_batches=self._accepted,
            rejected_batches=self._rejected,
            completed_batches=self._completed,
            last_error_code=self._last_error_code,
            last_completion_monotonic_ns=self._last_completion_monotonic_ns,
        )

    def _publish_health_locked(self) -> None:
        if self._on_health_change is not None:
            self._on_health_change(self._health_locked())
