"""Request-id ownership, callback completion and C0--C7 query trace joining."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from time import perf_counter_ns

from .gateway import CallbackRecord
from .telemetry import NullPerformanceTrace, PerformanceTracePort


class QueryRegistryError(RuntimeError):
    """A query cannot safely acquire or complete an EMT request id."""


class QueryCompletion(str, Enum):
    LAST = "LAST"
    SINGLE = "SINGLE"


@dataclass(frozen=True)
class QueryHandle:
    request_id: int
    correlation_id: str
    api_name: str
    callback_name: str
    profile: str
    workload: str


@dataclass(frozen=True)
class QuerySemanticObservation:
    """Payload-free facts needed to validate a query consumer's semantics."""

    correlation_id: str
    api_name: str
    callback_count: int
    item_count: int
    terminal_seen: bool
    terminal_flag_states: str
    query_reference: int | None


@dataclass
class _Query:
    handle: QueryHandle
    completion: QueryCompletion
    deadline_ns: int
    c5_recorded: bool = False
    c6_recorded: bool = False
    terminal_seen: bool = False
    terminal_flag_mask: int = 0
    callback_count: int = 0
    item_count: int = 0
    query_reference: int | None = None


class QueryRegistry:
    """Never reuses an active or cooling signed-int request id."""

    def __init__(
        self,
        *,
        trace: PerformanceTracePort | None = None,
        cooldown_ns: int = 30_000_000_000,
        minimum_request_id: int = 1,
        maximum_request_id: int = 2_147_483_647,
    ) -> None:
        if cooldown_ns < 0 or not 1 <= minimum_request_id <= maximum_request_id <= 2_147_483_647:
            raise ValueError("invalid request-id range or cooldown")
        self._trace = trace or NullPerformanceTrace()
        self._cooldown_ns = cooldown_ns
        self._minimum = minimum_request_id
        self._maximum = maximum_request_id
        self._next = minimum_request_id
        self._active: dict[int, _Query] = {}
        self._completed: dict[str, QuerySemanticObservation] = {}
        self._completed_order: deque[str] = deque()
        self._cooling: deque[tuple[int, int]] = deque()
        self._cooling_ids: set[int] = set()
        self._lock = Lock()

    def open(
        self,
        *,
        api_name: str,
        callback_name: str,
        correlation_id: str,
        profile: str,
        workload: str,
        completion: QueryCompletion,
        timeout_ns: int = 8_000_000_000,
    ) -> QueryHandle:
        if (
            not api_name.startswith("Query")
            or not callback_name.startswith("OnQuery")
            or not correlation_id
        ):
            raise QueryRegistryError("query api, callback and correlation_id are required")
        if timeout_ns <= 0:
            raise QueryRegistryError("query timeout_ns must be positive")
        now = perf_counter_ns()
        with self._lock:
            self._release_cooling_locked(now)
            request_id = self._allocate_locked()
            handle = QueryHandle(
                request_id, correlation_id, api_name, callback_name, profile, workload
            )
            self._active[request_id] = _Query(
                handle=handle, completion=completion, deadline_ns=now + timeout_ns
            )
        self._trace.begin(
            correlation_id,
            kind="api",
            interface=api_name,
            profile=profile,
            workload=workload,
            timestamp_ns=now,
        )
        self._trace.mark(correlation_id, "C1")
        return handle

    def mark_native_started(self, request_id: int) -> None:
        self._trace.mark(self._get_active(request_id).handle.correlation_id, "C3")

    def mark_native_returned(self, request_id: int, return_code: int) -> None:
        query = self._get_active(request_id)
        self._trace.mark(query.handle.correlation_id, "C4")
        if return_code != 0:
            # The numeric return code is bounded diagnostic metadata.  It
            # allows a fixture to distinguish local binding/counter rejection
            # from a missing callback without retaining native error text or
            # any business payload.
            self._trace.complete(
                query.handle.correlation_id, "rejected", f"native_return_{return_code}"
            )
            self._retire(request_id)

    def on_enqueued(self, record: CallbackRecord) -> None:
        with self._lock:
            query = self._matching_query_locked(record)
            if query is None:
                return
            mark_c5 = not query.c5_recorded
            if mark_c5:
                query.c5_recorded = True
            query.callback_count += 1
            query.terminal_flag_mask |= _terminal_flag_mask(record.is_last)
            if record.query_item_present:
                query.item_count += 1
            if record.query_reference is not None:
                query.query_reference = record.query_reference
            if self._is_terminal(query, record):
                query.terminal_seen = True
            correlation_id = query.handle.correlation_id
        if mark_c5:
            self._trace.mark(correlation_id, "C5", record.ring_enqueue_ns)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        with self._lock:
            query = self._matching_query_locked(record)
            if query is None or not self._is_terminal(query, record) or query.c6_recorded:
                return
            query.c6_recorded = True
            correlation_id = query.handle.correlation_id
        self._trace.mark(correlation_id, "C6", durable_ns)

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        with self._lock:
            query = self._matching_query_locked(record)
            if query is None or not query.terminal_seen or not self._is_terminal(query, record):
                return
            correlation_id = query.handle.correlation_id
            self._retire_locked(query.handle.request_id, acknowledged_ns)
        self._trace.mark(correlation_id, "C7", acknowledged_ns)
        self._trace.complete(correlation_id, "ok")

    def expire(self, now_ns: int | None = None, *, force: bool = False) -> tuple[int, ...]:
        """Timeout expired requests, or retire all active ones during session teardown."""

        now = perf_counter_ns() if now_ns is None else now_ns
        with self._lock:
            active = tuple(
                query for query in self._active.values() if force or query.deadline_ns <= now
            )
            for query in active:
                self._retire_locked(query.handle.request_id, now)
            self._release_cooling_locked(now)
        for query in active:
            self._trace.complete(
                query.handle.correlation_id, "timeout", "query_timeout_or_session_end"
            )
        return tuple(query.handle.request_id for query in active)

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def semantic_observation(self, correlation_id: str) -> QuerySemanticObservation | None:
        """Return a bounded payload-free observation after a query completes."""

        with self._lock:
            return self._completed.get(correlation_id)

    def _matching_query_locked(self, record: CallbackRecord) -> _Query | None:
        if record.request_id is None:
            # A legacy binding may omit request_id even though current EMT
            # 2.27 documentation includes it for streaming query callbacks.
            # Associate such a record only when it is the sole active request
            # for that callback.  It still cannot complete a LAST query until
            # an unambiguous ``is_last=True`` is observed.
            matches = [
                query
                for query in self._active.values()
                if query.handle.callback_name == record.callback_name
            ]
            return matches[0] if len(matches) == 1 else None
        query = self._active.get(record.request_id)
        if query is None or query.handle.callback_name != record.callback_name:
            return None
        return query

    def _get_active(self, request_id: int) -> _Query:
        with self._lock:
            try:
                return self._active[request_id]
            except KeyError as exc:
                raise QueryRegistryError(f"unknown or retired request_id: {request_id}") from exc

    def _is_terminal(self, query: _Query, record: CallbackRecord) -> bool:
        return query.completion is QueryCompletion.SINGLE or record.is_last is True

    def _retire(self, request_id: int, now_ns: int | None = None) -> None:
        now = perf_counter_ns() if now_ns is None else now_ns
        with self._lock:
            self._retire_locked(request_id, now)

    def _retire_locked(self, request_id: int, now_ns: int) -> None:
        query = self._active.get(request_id)
        if query is None:
            return
        self._active.pop(request_id)
        observation = QuerySemanticObservation(
            correlation_id=query.handle.correlation_id,
            api_name=query.handle.api_name,
            callback_count=query.callback_count,
            item_count=query.item_count,
            terminal_seen=query.terminal_seen,
            terminal_flag_states=_terminal_flag_states(query.terminal_flag_mask),
            query_reference=query.query_reference,
        )
        self._completed[observation.correlation_id] = observation
        self._completed_order.append(observation.correlation_id)
        while len(self._completed_order) > 1_024:
            self._completed.pop(self._completed_order.popleft(), None)
        self._cooling.append((now_ns + self._cooldown_ns, request_id))
        self._cooling_ids.add(request_id)

    def _allocate_locked(self) -> int:
        possible = self._maximum - self._minimum + 1
        for _ in range(possible):
            candidate = self._next
            self._next = self._minimum if candidate == self._maximum else candidate + 1
            if candidate not in self._active and candidate not in self._cooling_ids:
                return candidate
        raise QueryRegistryError("all signed request ids are active or cooling")

    def _release_cooling_locked(self, now_ns: int) -> None:
        while self._cooling and self._cooling[0][0] <= now_ns:
            _, request_id = self._cooling.popleft()
            self._cooling_ids.discard(request_id)


def _terminal_flag_mask(value: bool | None) -> int:
    """Encode only the three payload-free terminal-flag states."""

    return 0b001 if value is None else 0b010 if value is False else 0b100


def _terminal_flag_states(mask: int) -> str:
    """Render a bounded, stable terminal-flag observation for acceptance evidence."""

    states = tuple(
        label for bit, label in ((0b001, "absent"), (0b010, "false"), (0b100, "true")) if mask & bit
    )
    return ",".join(states) if states else "none"
