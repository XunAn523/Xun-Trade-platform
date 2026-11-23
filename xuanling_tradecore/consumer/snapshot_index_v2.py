"""Immutable, bounded, binding-scoped order snapshot pagination."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Mapping as RuntimeMapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields, is_dataclass, replace
from threading import Lock
from time import time_ns
from typing import Callable, Generator, Iterable, Mapping, Protocol, cast

from ..domain import ConsumerAccountBinding, ConsumerEventPositionV2, ConsumerOrderEvent
from ..domain._validation import require_positive
from .base import ConsumerGatewayError, ConsumerSession
from .payload_v2 import canonical_payload_bytes, order_event_payload
from .snapshot_cursor_v2 import (
    SnapshotCursorClaimsV2,
    SnapshotCursorCodecV2,
    SnapshotCursorErrorV2,
)
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2

_CACHED_ONLY: ContextVar[bool] = ContextVar("consumer_cached_snapshot_only", default=False)


@contextmanager
def cached_snapshot_only() -> Generator[None, None, None]:
    """Reviewed reactor read: no cold source, pin expiry cleanup or fallback IO."""
    token = _CACHED_ONLY.set(True)
    try:
        yield
    finally:
        _CACHED_ONLY.reset(token)


class ConsumerPagedSnapshotErrorV2(ConsumerGatewayError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code, self.retryable = code, retryable


@dataclass(frozen=True)
class ConsumerSnapshotVersionV2:
    snapshot_position: ConsumerEventPositionV2
    orders: tuple[ConsumerOrderEvent, ...]
    projection_lag: int = 0
    stale: bool = False
    order_source: ConsumerSnapshotOrderSourceV2 | None = None
    retained_bytes: int = 0


class ConsumerSnapshotOrderSourceV2(Protocol):
    scope: tuple[str, str, str]
    order_count: int
    account_id: str
    snapshot_position: ConsumerEventPositionV2
    retained_bytes: int

    def read_orders(self, start: int, end: int) -> tuple[ConsumerOrderEvent, ...]: ...

    def close(self) -> None: ...


class ConsumerSnapshotVersionReaderV2(Protocol):
    def capture_snapshot_version(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerSnapshotVersionV2: ...


@dataclass(frozen=True)
class ConsumerOrderSnapshotPlanV2:
    snapshot_id: str
    consumer_account_id: str
    journal_id: str
    stream_generation: int
    snapshot_position: ConsumerEventPositionV2
    order_count: int
    page_size: int
    page_cursor: str
    created_at_wall_ns: int
    expires_at_wall_ns: int
    projection_lag: int
    stale: bool


@dataclass(frozen=True)
class ConsumerOrderSnapshotPageV2:
    snapshot_id: str
    consumer_account_id: str
    journal_id: str
    stream_generation: int
    snapshot_position: ConsumerEventPositionV2
    orders: tuple[ConsumerOrderEvent, ...]
    order_payloads: tuple[Mapping[str, object], ...]
    response_payload_bytes: bytes
    page_count: int
    has_more: bool
    next_page_cursor: str | None
    # Internal policy carried to the dispatcher, never part of the wire schema.
    payload_budget_bytes: int = 262_144


@dataclass(frozen=True)
class _CachedSnapshotPageV2:
    orders: tuple[ConsumerOrderEvent, ...]
    order_payloads: tuple[Mapping[str, object], ...]
    has_more: bool
    next_cursor: str | None
    payload_size: int
    response_payload_bytes: bytes
    retained_bytes: int = 0


def _page_heap_bytes(value: object, seen: set[int] | None = None) -> int:
    """Charge a bounded page graph, conservatively counting sharing across pages twice."""
    if seen is None:
        seen = set()
    if id(value) in seen:
        return 0
    seen.add(id(value))
    result = sys.getsizeof(value)
    if is_dataclass(value) and not isinstance(value, type):
        result += sys.getsizeof(getattr(value, "__dict__", {}))
        result += sum(_page_heap_bytes(getattr(value, f.name), seen) for f in fields(value))
    elif isinstance(value, RuntimeMapping):
        result += sum(
            _page_heap_bytes(k, seen) + _page_heap_bytes(v, seen)
            for k, v in cast(Mapping[object, object], value).items()
        )
    elif isinstance(value, (tuple, list, frozenset)):
        result += sum(_page_heap_bytes(v, seen) for v in cast(Iterable[object], value))
    return result


def _new_page_cache() -> dict[int, _CachedSnapshotPageV2]:
    return {}


@dataclass(frozen=True)
class _PublishedSnapshotV2:
    plan: ConsumerOrderSnapshotPlanV2
    orders: tuple[ConsumerOrderEvent, ...]
    source: ConsumerSnapshotOrderSourceV2 | None = None
    retained_bytes: int = 0
    pages: dict[int, _CachedSnapshotPageV2] = field(default_factory=_new_page_cache, compare=False)


class ConsumerSnapshotIndexV2:
    """Keep a small set of immutable snapshot versions and signed page capabilities."""

    def __init__(
        self,
        *,
        registry_for: Callable[[ConsumerAccountBinding], CursorStreamRegistryV2],
        reader: ConsumerSnapshotVersionReaderV2,
        cursor_codec: SnapshotCursorCodecV2,
        ttl_ns: int = 120_000_000_000,
        maximum_active_snapshots: int = 8,
        maximum_page_size: int = 200,
        page_payload_budget_bytes: int = 262_144,
        wall_clock_ns: Callable[[], int] = time_ns,
        maximum_retained_bytes: int | None = None,
        maximum_page_cache_bytes: int | None = None,
        cursor_codec_for: Callable[[], SnapshotCursorCodecV2] | None = None,
    ) -> None:
        for value, name in (
            (ttl_ns, "snapshot ttl"),
            (maximum_active_snapshots, "maximum active snapshots"),
            (maximum_page_size, "maximum page size"),
            (page_payload_budget_bytes, "page payload budget"),
        ):
            require_positive(value, name)
        self._registry_for, self._reader, self._codec = registry_for, reader, cursor_codec
        self._codec_for = cursor_codec_for or (lambda: self._codec)
        self._ttl_ns, self._maximum_active = ttl_ns, maximum_active_snapshots
        self._maximum_page_size, self._page_budget = maximum_page_size, page_payload_budget_bytes
        self._wall_clock_ns = wall_clock_ns
        self._lock = Lock()
        self._snapshots: dict[str, _PublishedSnapshotV2] = {}
        for limit in (maximum_retained_bytes, maximum_page_cache_bytes):
            if limit is not None and (type(limit) is not int or limit < 1):
                raise ValueError("snapshot byte budget must be a positive integer")
        self._maximum_retained_bytes = maximum_retained_bytes
        self._maximum_page_cache_bytes = maximum_page_cache_bytes
        self._retained_bytes = self._page_cache_bytes = 0
        self._closed = False

    def create(
        self,
        *,
        session: ConsumerSession,
        binding: ConsumerAccountBinding,
        page_size: int,
    ) -> ConsumerOrderSnapshotPlanV2:
        if type(page_size) is not int or not 1 <= page_size <= self._maximum_page_size:
            raise ConsumerPagedSnapshotErrorV2(
                "SCHEMA_VALIDATION_FAILED", "snapshot page size is outside the configured bound"
            )
        now = self._wall_clock_ns()
        with self._lock:
            retired = self._expire_locked(now)
            full = self._closed or len(self._snapshots) >= self._maximum_active
        self._close_sources(retired)
        if full:
            raise ConsumerPagedSnapshotErrorV2(
                "RATE_LIMITED", "snapshot index is closed/full", retryable=True
            )
        stream = self._registry(binding).current
        try:
            version = self._reader.capture_snapshot_version(binding=binding, stream=stream)
        except ConsumerPagedSnapshotErrorV2:
            raise
        except Exception as exc:
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot projection is not ready", retryable=True
            ) from exc
        try:
            return self._publish_version(
                version=version,
                now=now,
                stream=stream,
                binding=binding,
                session=session,
                page_size=page_size,
            )
        except BaseException:
            if version.order_source is not None:
                version.order_source.close()
            raise

    def _publish_version(
        self,
        *,
        version: ConsumerSnapshotVersionV2,
        now: int,
        stream: CursorStreamRefV2,
        binding: ConsumerAccountBinding,
        session: ConsumerSession,
        page_size: int,
    ) -> ConsumerOrderSnapshotPlanV2:
        source = version.order_source
        if source is not None and (
            version.orders
            or source.scope != (binding.consumer_id, binding.consumer_account_id, stream.journal_id)
            or source.account_id != binding.account_id
            or source.snapshot_position != version.snapshot_position
            or type(source.order_count) is not int
            or source.order_count < 0
        ):
            source.close()
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot source scope is invalid"
            )
        orders = (
            ()
            if source is not None
            else tuple(sorted(version.orders, key=lambda item: item.oms_order_id))
        )
        retained = source.retained_bytes if source is not None else version.retained_bytes
        if (
            type(retained) is not int
            or retained < 0
            or (self._maximum_retained_bytes is not None and retained == 0)
        ):
            if source is not None:
                source.close()
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot pin cost is unavailable"
            )
        if any(
            order.consumer_trace.consumer_id != binding.consumer_id
            or order.account_id != binding.account_id
            or order.consumer_trace.consumer_account_id != binding.consumer_account_id
            or order.journal_id != stream.journal_id
            for order in orders
        ):
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot projection scope is invalid"
            )
        snapshot_id = f"snp-{uuid.uuid4().hex}"
        expires = now + self._ttl_ns
        cursor = self._issue(
            session=session,
            binding=binding,
            stream=stream,
            snapshot_id=snapshot_id,
            position=version.snapshot_position,
            next_index=0,
            page_size=page_size,
            issued_at=now,
            expires_at=expires,
        )
        plan = ConsumerOrderSnapshotPlanV2(
            snapshot_id,
            binding.consumer_account_id,
            stream.journal_id,
            stream.stream_generation,
            version.snapshot_position,
            source.order_count if source is not None else len(orders),
            page_size,
            cursor,
            now,
            expires,
            version.projection_lag,
            version.stale,
        )
        with self._lock:
            retired = self._expire_locked(now)
            full = (
                self._closed
                or len(self._snapshots) >= self._maximum_active
                or (
                    self._maximum_retained_bytes is not None
                    and self._retained_bytes + retained > self._maximum_retained_bytes
                )
            )
            if not full:
                self._snapshots[snapshot_id] = _PublishedSnapshotV2(plan, orders, source, retained)
                self._retained_bytes += retained
        self._close_sources(retired)
        if full:
            # create() owns cleanup of the unpublished version, outside this lock.
            raise ConsumerPagedSnapshotErrorV2(
                "RATE_LIMITED", "active snapshot capacity is full", retryable=True
            )
        return plan

    def validate_page_delivery(
        self, *, session: ConsumerSession, binding: ConsumerAccountBinding, page_cursor: str
    ) -> None:
        """Memory-only final fence; never expire/close a cold source on the reactor."""
        self._validated_delivery_claims(session=session, binding=binding, page_cursor=page_cursor)

    def _validated_delivery_claims(
        self, *, session: ConsumerSession, binding: ConsumerAccountBinding, page_cursor: str
    ) -> SnapshotCursorClaimsV2:
        try:
            claims = self._codec_for().verify(page_cursor, evaluated_wall_ns=self._wall_clock_ns())
        except SnapshotCursorErrorV2 as exc:
            raise ConsumerPagedSnapshotErrorV2(
                exc.code, "snapshot cursor cannot be delivered"
            ) from exc
        if (
            claims.consumer_id != session.consumer_id
            or claims.consumer_account_id != binding.consumer_account_id
            or claims.authorization_epoch != session.binding_epoch
        ):
            raise ConsumerPagedSnapshotErrorV2("CURSOR_INVALID", "snapshot cursor scope changed")
        current = self._registry(binding).current
        if (
            claims.journal_id != current.journal_id
            or claims.stream_generation != current.stream_generation
        ):
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_GENERATION_MISMATCH", "snapshot generation changed"
            )
        with self._lock:
            published = self._snapshots.get(claims.snapshot_id)
            if self._closed or published is None:
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_EXPIRED", "snapshot pin is no longer live"
                )
        return claims

    def cached_page_ready(
        self, *, session: ConsumerSession, binding: ConsumerAccountBinding, page_cursor: str
    ) -> bool:
        # An optimization hint, NEVER an authorization or delivery proof.
        try:
            claims = self._validated_delivery_claims(
                session=session, binding=binding, page_cursor=page_cursor
            )
        except ConsumerPagedSnapshotErrorV2:
            return False
        with self._lock:
            published = self._snapshots.get(claims.snapshot_id)
            return published is not None and claims.next_index in published.pages

    def read_page(
        self,
        *,
        session: ConsumerSession,
        binding: ConsumerAccountBinding,
        page_cursor: str,
    ) -> ConsumerOrderSnapshotPageV2:
        now = self._wall_clock_ns()
        try:
            claims = self._codec_for().verify(page_cursor, evaluated_wall_ns=now)
        except SnapshotCursorErrorV2 as exc:
            detail = (
                "snapshot version has expired"
                if exc.code == "SNAPSHOT_EXPIRED"
                else "snapshot cursor cannot be used"
            )
            raise ConsumerPagedSnapshotErrorV2(exc.code, detail) from exc
        if (
            claims.consumer_id != session.consumer_id
            or claims.consumer_account_id != binding.consumer_account_id
            or claims.authorization_epoch != session.binding_epoch
        ):
            raise ConsumerPagedSnapshotErrorV2(
                "CURSOR_INVALID", "snapshot cursor scope does not match"
            )
        current = self._registry(binding).current
        if (
            claims.journal_id != current.journal_id
            or claims.stream_generation != current.stream_generation
        ):
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_GENERATION_MISMATCH", "snapshot cursor is from another generation"
            )
        memory_only = _CACHED_ONLY.get()
        with self._lock:
            retired = () if memory_only else self._expire_locked(now)
            published = self._snapshots.get(claims.snapshot_id)
        self._close_sources(retired)
        if published is None:
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_EXPIRED", "snapshot version is no longer available"
            )
        plan = published.plan
        if (
            plan.journal_id != claims.journal_id
            or plan.stream_generation != claims.stream_generation
            or plan.snapshot_position.to_external_id() != claims.snapshot_position
            or plan.page_size != claims.page_size
        ):
            raise ConsumerPagedSnapshotErrorV2("CURSOR_INVALID", "snapshot cursor changed version")
        start = claims.next_index
        if start > plan.order_count:
            raise ConsumerPagedSnapshotErrorV2("CURSOR_INVALID", "snapshot cursor exceeds version")
        end = min(start + claims.page_size, plan.order_count)
        with self._lock:
            cached_page = published.pages.get(start)
        if cached_page is None:
            if memory_only:
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_NOT_READY", "snapshot page is not cached", retryable=True
                )
            has_more = end < plan.order_count
            next_cursor = None
            if has_more:
                next_cursor = self._issue(
                    session=session,
                    binding=binding,
                    stream=current,
                    snapshot_id=plan.snapshot_id,
                    position=plan.snapshot_position,
                    next_index=end,
                    page_size=plan.page_size,
                    issued_at=plan.created_at_wall_ns,
                    expires_at=plan.expires_at_wall_ns,
                )
            try:
                orders = (
                    published.orders[start:end]
                    if published.source is None
                    else published.source.read_orders(start, end)
                )
            except (OSError, ValueError) as exc:
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_NOT_READY", "frozen page cannot be verified"
                ) from exc
            if len(orders) != end - start or any(
                order.consumer_trace.consumer_id != binding.consumer_id
                or order.account_id != binding.account_id
                or order.consumer_trace.consumer_account_id != binding.consumer_account_id
                or order.journal_id != current.journal_id
                for order in orders
            ):
                raise ConsumerPagedSnapshotErrorV2(
                    "SNAPSHOT_NOT_READY", "frozen page scope/count is invalid"
                )
            order_payloads = tuple(order_event_payload(item) for item in orders)
            response_payload = {
                "snapshot_id": plan.snapshot_id,
                "consumer_account_id": plan.consumer_account_id,
                "journal_id": plan.journal_id,
                "stream_generation": plan.stream_generation,
                "snapshot_position": plan.snapshot_position.to_external_id(),
                "orders": list(order_payloads),
                "count": len(orders),
                "has_more": has_more,
                "next_page_cursor": next_cursor,
                "replay_cursor": None,
            }
            candidate = _CachedSnapshotPageV2(
                orders,
                order_payloads,
                has_more,
                next_cursor,
                len(canonical_payload_bytes({"orders": list(order_payloads)})),
                canonical_payload_bytes(response_payload),
            )
            if candidate.payload_size > self._page_budget:
                raise ConsumerPagedSnapshotErrorV2(
                    "FRAME_TOO_LARGE", "snapshot page exceeds its encoded byte budget"
                )
            cache_bytes = (
                _page_heap_bytes(candidate) + 128
                if self._maximum_page_cache_bytes is not None
                else len(candidate.response_payload_bytes) + candidate.payload_size
            )
            candidate = replace(candidate, retained_bytes=cache_bytes)
            with self._lock:
                if self._snapshots.get(plan.snapshot_id) is published and (
                    self._maximum_page_cache_bytes is None
                    or self._page_cache_bytes + cache_bytes <= self._maximum_page_cache_bytes
                ):
                    existed = start in published.pages
                    cached_page = published.pages.setdefault(start, candidate)
                    if not existed:
                        self._page_cache_bytes += cache_bytes
                else:
                    cached_page = candidate
        if cached_page.payload_size > self._page_budget:
            raise ConsumerPagedSnapshotErrorV2(
                "FRAME_TOO_LARGE", "snapshot page exceeds its encoded byte budget"
            )
        return ConsumerOrderSnapshotPageV2(
            plan.snapshot_id,
            plan.consumer_account_id,
            plan.journal_id,
            plan.stream_generation,
            plan.snapshot_position,
            cached_page.orders,
            cached_page.order_payloads,
            cached_page.response_payload_bytes,
            len(cached_page.orders),
            cached_page.has_more,
            cached_page.next_cursor,
            self._page_budget,
        )

    def _registry(self, binding: ConsumerAccountBinding) -> CursorStreamRegistryV2:
        try:
            return self._registry_for(binding)
        except Exception as exc:
            raise ConsumerPagedSnapshotErrorV2(
                "SNAPSHOT_NOT_READY", "snapshot stream registry is unavailable"
            ) from exc

    def _issue(
        self,
        *,
        session: ConsumerSession,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        snapshot_id: str,
        position: ConsumerEventPositionV2,
        next_index: int,
        page_size: int,
        issued_at: int,
        expires_at: int,
    ) -> str:
        codec = self._codec_for()
        return codec.issue(
            SnapshotCursorClaimsV2(
                session.consumer_id,
                binding.consumer_account_id,
                session.binding_epoch,
                snapshot_id,
                stream.journal_id,
                stream.stream_generation,
                position.to_external_id(),
                next_index,
                page_size,
                issued_at,
                expires_at,
                codec.key_id,
            )
        )

    def _expire_locked(self, now: int) -> tuple[ConsumerSnapshotOrderSourceV2, ...]:
        retired: list[ConsumerSnapshotOrderSourceV2] = []
        for snapshot_id, published in tuple(self._snapshots.items()):
            if now >= published.plan.expires_at_wall_ns:
                del self._snapshots[snapshot_id]
                self._retained_bytes -= published.retained_bytes
                self._page_cache_bytes -= sum(p.retained_bytes for p in published.pages.values())
                if published.source is not None:
                    retired.append(published.source)
        return tuple(retired)

    @staticmethod
    def _close_sources(sources: tuple[ConsumerSnapshotOrderSourceV2, ...]) -> None:
        # close may wait for a page's file lock / in-progress read. Never hold
        # the shared index lock needed by the reactor's memory-only fence.
        for source in sources:
            source.close()

    def retained_storage_bytes(self) -> tuple[int, int]:
        with self._lock:
            retired = self._expire_locked(self._wall_clock_ns())
            result = self._retained_bytes, self._page_cache_bytes
        self._close_sources(retired)
        return result

    def close(self) -> None:
        with self._lock:
            self._closed = True
            retired = tuple(
                published.source
                for published in self._snapshots.values()
                if published.source is not None
            )
            self._snapshots.clear()
            self._retained_bytes = self._page_cache_bytes = 0
        self._close_sources(retired)
