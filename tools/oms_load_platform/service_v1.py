"""Async, out-of-path OMS fact ingestion and immutable snapshot service.

This module is a performance and acceptance harness around the existing OMS
sidecar contracts.  It never imports an EMT adapter, SDK, dispatcher or mmap
writer.  It is deliberately started as a separate process for W0--W7.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass, field
from hashlib import sha256
from time import monotonic_ns
from typing import Any, Mapping

from hdrh.histogram import HdrHistogram
from xuanling_tradecore.oms.sidecar_v1 import (
    ImmutableOmsSnapshotStoreV1,
    OmsSidecarError,
    OmsSidecarFactKind,
    OmsSidecarFactReaderV1,
    OmsSidecarFactV1,
    OmsSidecarOrderState,
    OmsSidecarSnapshotV1,
    OmsSnapshotQueryServiceV1,
    OrderStateProjectorV1,
)


class OmsLoadPlatformError(RuntimeError):
    """A load-platform request cannot be accepted safely."""


class OmsProjectorPausedError(OmsLoadPlatformError):
    """The target projector was intentionally paused for W6."""


class OmsIngressOverloadedError(OmsLoadPlatformError):
    """A bounded scope queue is full; callers must retry with backoff."""


class OmsSnapshotStaleError(OmsLoadPlatformError):
    """A caller explicitly required a fresh immutable snapshot."""


@dataclass(frozen=True)
class OmsLoadPlatformSettingsV1:
    """Bounded configuration for the standalone performance service."""

    scope_count: int = 4
    queue_capacity: int = 8_192
    projector_batch_size: int = 128
    snapshot_publish_interval_ms: int = 200
    snapshot_stale_after_ms: int = 250
    acknowledgement_timeout_ms: int = 1_000
    replay_history_capacity: int = 250_000
    seed_orders: int = 1_024

    def __post_init__(self) -> None:
        if not 1 <= self.scope_count <= 64:
            raise ValueError("scope_count must be in [1, 64]")
        if not 32 <= self.queue_capacity <= 1_000_000:
            raise ValueError("queue_capacity must be in [32, 1_000_000]")
        if not 1 <= self.projector_batch_size <= 4_096:
            raise ValueError("projector_batch_size must be in [1, 4096]")
        if not 1 <= self.snapshot_publish_interval_ms <= 1_000:
            raise ValueError("snapshot_publish_interval_ms must be in [1, 1000]")
        if not 1 <= self.snapshot_stale_after_ms <= 60_000:
            raise ValueError("snapshot_stale_after_ms must be in [1, 60000]")
        if not 10 <= self.acknowledgement_timeout_ms <= 60_000:
            raise ValueError("acknowledgement_timeout_ms must be in [10, 60000]")
        if not 1_024 <= self.replay_history_capacity <= 2_000_000:
            raise ValueError("replay_history_capacity must be in [1024, 2000000]")
        if not 0 <= self.seed_orders <= 100_000:
            raise ValueError("seed_orders must be in [0, 100000]")


@dataclass(frozen=True)
class OmsFactInputV1:
    """Sanitized HTTP fact payload; the service assigns Journal positions."""

    scope_id: str
    execution_id: str
    canonical_client_order_ref: str
    kind: OmsSidecarFactKind = OmsSidecarFactKind.PRE_NATIVE_COMMITTED
    order_state: OmsSidecarOrderState = OmsSidecarOrderState.PRE_NATIVE_COMMITTED
    external_order_id: str | None = None
    state_version: int = 1
    cumulative_quantity: int | None = None
    leaves_quantity: int | None = None


@dataclass(frozen=True)
class OmsFactReceiptV1:
    """Acknowledgement only after the actor has applied the committed fact."""

    scope_id: str
    position: int
    snapshot_generation: int
    admitted_monotonic_ns: int
    projected_monotonic_ns: int


@dataclass
class _QueuedFact:
    fact: OmsSidecarFactV1
    admitted_monotonic_ns: int
    receipt: asyncio.Future[OmsFactReceiptV1]


@dataclass
class _ScopeActor:
    scope_id: str
    reader: OmsSidecarFactReaderV1
    projector: OrderStateProjectorV1
    queue: asyncio.Queue[_QueuedFact]
    replay_history: deque[OmsSidecarFactV1]
    resume_event: asyncio.Event = field(default_factory=asyncio.Event)
    next_position: int = 1
    worker: asyncio.Task[None] | None = None
    last_projected_monotonic_ns: int = 0
    processing: int = 0


class HdrMetricsV1:
    """Bounded-significant-figure HDR latency metrics for every HTTP contract."""

    _HIGHEST_TRACKABLE_NS = 60_000_000_000

    def __init__(self) -> None:
        self._histograms: dict[str, HdrHistogram] = {}
        self._counters: dict[str, int] = {}
        self._started_monotonic_ns = monotonic_ns()

    def increment(self, name: str, amount: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + amount

    def record(self, name: str, elapsed_ns: int) -> None:
        histogram = self._histograms.get(name)
        if histogram is None:
            histogram = HdrHistogram(1, self._HIGHEST_TRACKABLE_NS, 3)
            self._histograms[name] = histogram
        histogram.record_value(max(1, min(elapsed_ns, self._HIGHEST_TRACKABLE_NS)))

    def reset(self) -> None:
        self._histograms.clear()
        self._counters.clear()
        self._started_monotonic_ns = monotonic_ns()

    def summary(self) -> dict[str, object]:
        return {
            "format": "HDRHistogramV1",
            "started_monotonic_ns": self._started_monotonic_ns,
            "counters": dict(sorted(self._counters.items())),
            "histograms_ns": {
                name: self._histogram_summary(histogram)
                for name, histogram in sorted(self._histograms.items())
            },
        }

    @staticmethod
    def _histogram_summary(histogram: HdrHistogram) -> dict[str, int | float]:
        return {
            "count": histogram.total_count,
            "min_ns": histogram.get_min_value(),
            "p50_ns": histogram.get_value_at_percentile(50.0),
            "p95_ns": histogram.get_value_at_percentile(95.0),
            "p99_ns": histogram.get_value_at_percentile(99.0),
            "p999_ns": histogram.get_value_at_percentile(99.9),
            "max_ns": histogram.get_max_value(),
            "mean_ns": round(histogram.get_mean_value(), 3),
        }

    def prometheus(self) -> str:
        """Produce dependency-free scrape text; histograms remain at the HDR endpoint."""

        lines = ["# TYPE tradecore_oms_platform_counter_total counter"]
        for name, value in sorted(self._counters.items()):
            lines.append(f"tradecore_oms_platform_counter_total{{name={name!r}}} {value}")
        lines.append("# TYPE tradecore_oms_platform_hdr_percentile_ns gauge")
        for name, histogram in sorted(self._histograms.items()):
            for percentile, value in (
                ("p50", histogram.get_value_at_percentile(50.0)),
                ("p95", histogram.get_value_at_percentile(95.0)),
                ("p99", histogram.get_value_at_percentile(99.0)),
                ("p999", histogram.get_value_at_percentile(99.9)),
                ("max", histogram.get_max_value()),
            ):
                lines.append(
                    "tradecore_oms_platform_hdr_percentile_ns"
                    f"{{name={name!r},percentile={percentile!r}}} {value}"
                )
        return "\n".join(lines) + "\n"


class OmsLoadPlatformV1:
    """Actor-backed test facade over committed-fact/projector/snapshot contracts.

    The facade intentionally assigns monotonically increasing synthetic
    positions.  It is not a replacement for the actual mmap Journal reader:
    W0--W6 measure the OMS sidecar's admission/project/query surfaces, while
    W7 compares an independently run EMT burst with and without this process.
    """

    def __init__(self, settings: OmsLoadPlatformSettingsV1 | None = None) -> None:
        self.settings = settings or OmsLoadPlatformSettingsV1()
        self.metrics = HdrMetricsV1()
        self._actors: dict[str, _ScopeActor] = {}
        self._store = ImmutableOmsSnapshotStoreV1(history_capacity=16)
        self._query = OmsSnapshotQueryServiceV1(self._store)
        self._scope_snapshots: dict[str, OmsSidecarSnapshotV1] = {}
        self._global_generation = 0
        self._last_snapshot_publish_monotonic_ns = monotonic_ns()
        self._snapshot_dirty = False
        self._dirty_scope_ids: set[str] = set()
        self._snapshot_dirty_event = asyncio.Event()
        self._snapshot_publish_lock = asyncio.Lock()
        self._admission_enabled = asyncio.Event()
        self._admission_enabled.set()
        self._publisher: asyncio.Task[None] | None = None
        self._stopped = False
        self._started = False

    @property
    def scope_ids(self) -> tuple[str, ...]:
        return tuple(self._actors)

    async def start(self) -> None:
        if self._started:
            return
        for index in range(self.settings.scope_count):
            scope_id = f"bench-scope-{index}"
            event = asyncio.Event()
            event.set()
            self._actors[scope_id] = _ScopeActor(
                scope_id=scope_id,
                reader=OmsSidecarFactReaderV1(
                    scope_id=scope_id, activation_epoch=1, route_version=1
                ),
                projector=OrderStateProjectorV1(),
                queue=asyncio.Queue(maxsize=self.settings.queue_capacity),
                replay_history=deque(maxlen=self.settings.replay_history_capacity),
                resume_event=event,
            )
        self._seed_snapshot_orders()
        self._publish_global_snapshot()
        for actor in self._actors.values():
            actor.worker = asyncio.create_task(self._run_actor(actor), name=actor.scope_id)
        self._publisher = asyncio.create_task(self._run_publisher(), name="oms-snapshot-publisher")
        self._started = True

    async def stop(self) -> None:
        self._stopped = True
        for actor in self._actors.values():
            if actor.worker is not None:
                actor.worker.cancel()
        workers = [actor.worker for actor in self._actors.values() if actor.worker is not None]
        if self._publisher is not None:
            self._publisher.cancel()
            workers.append(self._publisher)
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    async def ingest(self, item: OmsFactInputV1) -> OmsFactReceiptV1:
        if not self._started or self._stopped:
            raise OmsLoadPlatformError("OMS load platform is not running")
        if not self._admission_enabled.is_set():
            self.metrics.increment("fact_rejected_benchmark_reset_total")
            raise OmsLoadPlatformError("benchmark state reset is in progress")
        actor = self._actors.get(item.scope_id)
        if actor is None:
            raise OmsLoadPlatformError("unknown benchmark scope")
        if not actor.resume_event.is_set():
            self.metrics.increment("fact_rejected_projector_paused_total")
            raise OmsProjectorPausedError("projector is paused")
        if actor.queue.full():
            self.metrics.increment("fact_rejected_overloaded_total")
            raise OmsIngressOverloadedError("bounded projector queue is full")
        admitted = monotonic_ns()
        position = actor.next_position
        actor.next_position += 1
        fact = OmsSidecarFactV1(
            scope_id=item.scope_id,
            position=position,
            schema_version=1,
            activation_epoch=1,
            route_version=1,
            kind=item.kind,
            execution_id=item.execution_id,
            canonical_client_order_ref=item.canonical_client_order_ref,
            payload_hash=_payload_hash(item),
            observed_monotonic_ns=admitted,
            external_order_id=item.external_order_id,
            order_state=item.order_state,
            state_version=item.state_version,
            cumulative_quantity=item.cumulative_quantity,
            leaves_quantity=item.leaves_quantity,
        )
        loop = asyncio.get_running_loop()
        receipt: asyncio.Future[OmsFactReceiptV1] = loop.create_future()
        actor.queue.put_nowait(_QueuedFact(fact, admitted, receipt))
        self.metrics.increment("fact_admitted_total")
        try:
            result = await asyncio.wait_for(
                asyncio.shield(receipt), self.settings.acknowledgement_timeout_ms / 1_000
            )
        except asyncio.TimeoutError as error:
            self.metrics.increment("fact_ack_timeout_total")
            raise OmsLoadPlatformError("projector acknowledgement timed out") from error
        self.metrics.record("fact_admit_to_projected_ns", result.projected_monotonic_ns - admitted)
        return result

    def order(self, execution_id: str) -> Mapping[str, object] | None:
        started = monotonic_ns()
        order = self._query.order_by_execution_id(execution_id)
        self.metrics.increment("snapshot_order_query_total")
        self.metrics.record("snapshot_order_query_ns", monotonic_ns() - started)
        if order is None:
            return None
        return {
            "execution_id": order.execution_id,
            "scope_id": order.scope_id,
            "state": order.state.value,
            "state_version": order.state_version,
            "applied_position": order.applied_position,
            "external_order_id": order.external_order_id,
            "cumulative_quantity": order.cumulative_quantity,
            "leaves_quantity": order.leaves_quantity,
            "reconciliation_required": order.reconciliation_required,
            "snapshot_generation": self._query.snapshot().generation,
        }

    def snapshot_summary(self, *, require_fresh: bool = False) -> dict[str, object]:
        started = monotonic_ns()
        stale = self.snapshot_is_stale()
        if require_fresh and stale:
            self.metrics.increment("snapshot_rejected_stale_total")
            raise OmsSnapshotStaleError("snapshot is stale")
        snapshot = self._query.snapshot()
        self.metrics.increment("snapshot_summary_query_total")
        self.metrics.record("snapshot_summary_query_ns", monotonic_ns() - started)
        return {
            "generation": snapshot.generation,
            "as_of_monotonic_ns": snapshot.as_of_monotonic_ns,
            "watermarks": dict(snapshot.per_scope_applied_watermark),
            "order_count": len(snapshot.orders_by_execution_id),
            "trade_count": len(snapshot.trades_by_key),
            "authoritative": snapshot.is_authoritative,
            "stale": stale,
            "snapshot_age_ms": self.snapshot_age_ms(),
        }

    def health(self) -> dict[str, object]:
        started = monotonic_ns()
        snapshot = self._query.snapshot()
        result = {
            "status": "STALE" if self.snapshot_is_stale() else "READY",
            "snapshot_generation": snapshot.generation,
            "snapshot_age_ms": self.snapshot_age_ms(),
            "watermarks": dict(snapshot.per_scope_applied_watermark),
            "actors": {
                scope_id: {
                    "status": "CURRENT" if actor.resume_event.is_set() else "PAUSED",
                    "queue_depth": actor.queue.qsize(),
                    "processing": actor.processing,
                    "last_projected_monotonic_ns": actor.last_projected_monotonic_ns,
                }
                for scope_id, actor in self._actors.items()
            },
        }
        self.metrics.increment("watermark_health_query_total")
        self.metrics.record("watermark_health_query_ns", monotonic_ns() - started)
        return result

    def reset_metrics(self) -> None:
        self.metrics.reset()

    def pause_projector(self, scope_id: str) -> None:
        actor = self._actor(scope_id)
        actor.resume_event.clear()
        self.metrics.increment("projector_pause_total")

    def resume_projector(self, scope_id: str) -> None:
        actor = self._actor(scope_id)
        actor.resume_event.set()
        self.metrics.increment("projector_resume_total")

    async def wait_until_idle(self, timeout_ms: int = 5_000) -> bool:
        deadline = monotonic_ns() + timeout_ms * 1_000_000
        while monotonic_ns() < deadline:
            if all(
                actor.queue.empty() and actor.processing == 0 for actor in self._actors.values()
            ):
                await self._flush_snapshot()
                return True
            await asyncio.sleep(0.001)
        return False

    async def verify_replay(self) -> dict[str, object]:
        if not await self.wait_until_idle():
            raise OmsLoadPlatformError("cannot replay while a projector is busy")
        started = monotonic_ns()
        rebuilt: dict[str, OmsSidecarSnapshotV1] = {}
        for scope_id, actor in self._actors.items():
            reader = OmsSidecarFactReaderV1(scope_id=scope_id, activation_epoch=1, route_version=1)
            projector = OrderStateProjectorV1()
            committed = reader.read_committed(tuple(actor.replay_history))
            rebuilt[scope_id] = projector.apply(committed)
        actual = self._query.snapshot()
        expected = self._combine_snapshots(rebuilt, self._global_generation + 1)
        duration = monotonic_ns() - started
        matches = _snapshot_digest(actual) == _snapshot_digest(expected)
        self.metrics.increment("replay_verification_total")
        self.metrics.increment(
            "replay_verification_pass_total" if matches else "replay_verification_fail_total"
        )
        self.metrics.record("replay_verification_ns", duration)
        return {
            "status": "PASS" if matches else "FAIL",
            "actual_digest": _snapshot_digest(actual),
            "replayed_digest": _snapshot_digest(expected),
            "duration_ns": duration,
            "watermarks": dict(actual.per_scope_applied_watermark),
        }

    async def reset_benchmark_state(self) -> dict[str, object]:
        """Restore synthetic seed state between independent k6 workloads.

        This is intentionally a harness-only operation behind the fault key.
        It closes admission first, waits for every accepted fact to be
        projected, and only then replaces reader/projector state.  It never
        exists on the production OMS sidecar path.
        """

        if any(not actor.resume_event.is_set() for actor in self._actors.values()):
            raise OmsLoadPlatformError("cannot reset while a projector is paused")
        self._admission_enabled.clear()
        try:
            if not await self.wait_until_idle():
                raise OmsLoadPlatformError("cannot reset while a projector is busy")
            async with self._snapshot_publish_lock:
                for actor in self._actors.values():
                    actor.reader = OmsSidecarFactReaderV1(
                        scope_id=actor.scope_id, activation_epoch=1, route_version=1
                    )
                    actor.projector = OrderStateProjectorV1()
                    actor.replay_history.clear()
                    actor.next_position = 1
                    actor.last_projected_monotonic_ns = 0
                self._scope_snapshots.clear()
                self._snapshot_dirty = False
                self._dirty_scope_ids.clear()
                self._snapshot_dirty_event.clear()
                self._seed_snapshot_orders()
                self._publish_global_snapshot()
                self.metrics.increment("benchmark_state_reset_total")
                return {
                    "status": "RESET",
                    "generation": self._query.snapshot().generation,
                    "scope_count": len(self._actors),
                }
        finally:
            self._admission_enabled.set()

    def snapshot_age_ms(self) -> float:
        return round((monotonic_ns() - self._last_snapshot_publish_monotonic_ns) / 1_000_000, 3)

    def snapshot_is_stale(self) -> bool:
        if any(not actor.resume_event.is_set() for actor in self._actors.values()):
            return True
        has_pending = any(
            actor.processing or not actor.queue.empty() for actor in self._actors.values()
        )
        return has_pending and self.snapshot_age_ms() > self.settings.snapshot_stale_after_ms

    def _actor(self, scope_id: str) -> _ScopeActor:
        try:
            return self._actors[scope_id]
        except KeyError as error:
            raise OmsLoadPlatformError("unknown benchmark scope") from error

    async def _run_actor(self, actor: _ScopeActor) -> None:
        try:
            while True:
                queued = await actor.queue.get()
                await actor.resume_event.wait()
                batch = [queued]
                while len(batch) < self.settings.projector_batch_size:
                    try:
                        batch.append(actor.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                actor.processing = len(batch)
                try:
                    facts = actor.reader.read_committed(item.fact for item in batch)
                    actor.projector.apply_unpublished(facts)
                    actor.replay_history.extend(facts)
                    actor.last_projected_monotonic_ns = monotonic_ns()
                    self._snapshot_dirty = True
                    self._dirty_scope_ids.add(actor.scope_id)
                    self._snapshot_dirty_event.set()
                    for item in batch:
                        if not item.receipt.done():
                            item.receipt.set_result(
                                OmsFactReceiptV1(
                                    scope_id=actor.scope_id,
                                    position=item.fact.position,
                                    snapshot_generation=self._query.snapshot().generation,
                                    admitted_monotonic_ns=item.admitted_monotonic_ns,
                                    projected_monotonic_ns=actor.last_projected_monotonic_ns,
                                )
                            )
                except (OmsSidecarError, OmsLoadPlatformError) as error:
                    self.metrics.increment("fact_projector_error_total")
                    for item in batch:
                        if not item.receipt.done():
                            item.receipt.set_exception(OmsLoadPlatformError(str(error)))
                finally:
                    actor.processing = 0
                    for _ in batch:
                        actor.queue.task_done()
        except asyncio.CancelledError:
            raise

    async def _run_publisher(self) -> None:
        try:
            while True:
                await self._snapshot_dirty_event.wait()
                await asyncio.sleep(self.settings.snapshot_publish_interval_ms / 1_000)
                await self._flush_snapshot()
        except asyncio.CancelledError:
            raise

    def _seed_snapshot_orders(self) -> None:
        for index in range(self.settings.seed_orders):
            scope_id = f"bench-scope-{index % self.settings.scope_count}"
            actor = self._actors[scope_id]
            position = actor.next_position
            actor.next_position += 1
            execution_id = f"seed-{index:06d}"
            fact = OmsSidecarFactV1(
                scope_id=scope_id,
                position=position,
                schema_version=1,
                activation_epoch=1,
                route_version=1,
                kind=OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                execution_id=execution_id,
                canonical_client_order_ref=f"seed-ref-{index:06d}",
                payload_hash=sha256(execution_id.encode("ascii")).hexdigest(),
                observed_monotonic_ns=monotonic_ns(),
                order_state=OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
                state_version=1,
            )
            committed = actor.reader.read_committed((fact,))
            self._scope_snapshots[scope_id] = actor.projector.apply(committed)
            actor.replay_history.extend(committed)
            actor.last_projected_monotonic_ns = fact.observed_monotonic_ns

    def _publish_global_snapshot(self) -> None:
        self._global_generation += 1
        combined = self._combine_snapshots(self._scope_snapshots, self._global_generation)
        self._store.publish(combined)
        self._last_snapshot_publish_monotonic_ns = monotonic_ns()

    async def _flush_snapshot(self) -> None:
        async with self._snapshot_publish_lock:
            if not self._snapshot_dirty:
                return
            started = monotonic_ns()
            dirty_scope_ids = tuple(self._dirty_scope_ids)
            self._snapshot_dirty = False
            self._snapshot_dirty_event.clear()
            self._dirty_scope_ids.clear()
            prepared = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        self._actors[scope_id].projector.snapshot,
                        as_of_monotonic_ns=self._actors[scope_id].last_projected_monotonic_ns,
                    )
                    for scope_id in dirty_scope_ids
                )
            )
            self._scope_snapshots.update(zip(dirty_scope_ids, prepared))
            self._publish_global_snapshot()
            self.metrics.record("snapshot_materialize_publish_ns", monotonic_ns() - started)

    def _combine_snapshots(
        self, snapshots: Mapping[str, OmsSidecarSnapshotV1], generation: int
    ) -> OmsSidecarSnapshotV1:
        execution: dict[str, object] = {}
        client: dict[str, object] = {}
        external: dict[str, object] = {}
        trades: dict[str, object] = {}
        watermarks: dict[str, int] = {}
        reconcile: set[str] = set()
        health: dict[str, str] = {}
        as_of = 0
        for scope_id, actor in self._actors.items():
            snapshot = snapshots.get(scope_id)
            if snapshot is None:
                watermarks[scope_id] = actor.reader.watermark
                health[scope_id] = "CURRENT" if actor.resume_event.is_set() else "PAUSED"
                continue
            execution.update(snapshot.orders_by_execution_id)
            client.update(snapshot.orders_by_client_order_ref)
            external.update(snapshot.orders_by_external_order_id)
            trades.update(snapshot.trades_by_key)
            watermarks.update(snapshot.per_scope_applied_watermark)
            reconcile.update(snapshot.reconciliation_required_execution_ids)
            health[scope_id] = "CURRENT" if actor.resume_event.is_set() else "PAUSED"
            as_of = max(as_of, snapshot.as_of_monotonic_ns)
        health["reconciliation"] = "REQUIRED" if reconcile else "VALID"
        return OmsSidecarSnapshotV1(
            generation=generation,
            as_of_monotonic_ns=as_of,
            per_scope_applied_watermark=watermarks,
            orders_by_execution_id=execution,
            orders_by_client_order_ref=client,
            orders_by_external_order_id=external,
            trades_by_key=trades,
            health=health,
            reconciliation_required_execution_ids=frozenset(reconcile),
        )


def _payload_hash(item: OmsFactInputV1) -> str:
    payload = {
        "scope_id": item.scope_id,
        "execution_id": item.execution_id,
        "canonical_client_order_ref": item.canonical_client_order_ref,
        "kind": item.kind.value,
        "order_state": item.order_state.value,
        "external_order_id": item.external_order_id,
        "state_version": item.state_version,
        "cumulative_quantity": item.cumulative_quantity,
        "leaves_quantity": item.leaves_quantity,
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )
    ).hexdigest()


def _snapshot_digest(snapshot: OmsSidecarSnapshotV1) -> str:
    payload: dict[str, Any] = {
        "watermarks": dict(sorted(snapshot.per_scope_applied_watermark.items())),
        "orders": {
            execution_id: {
                "scope_id": order.scope_id,
                "state": order.state.value,
                "state_version": order.state_version,
                "position": order.applied_position,
                "external_order_id": order.external_order_id,
                "cumulative_quantity": order.cumulative_quantity,
                "leaves_quantity": order.leaves_quantity,
                "reconciliation_required": order.reconciliation_required,
            }
            for execution_id, order in sorted(snapshot.orders_by_execution_id.items())
        },
        "trades": sorted(snapshot.trades_by_key),
        "reconciliation_required": sorted(snapshot.reconciliation_required_execution_ids),
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )
    ).hexdigest()
