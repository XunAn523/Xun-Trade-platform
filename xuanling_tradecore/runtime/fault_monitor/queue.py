"""A bounded, non-blocking fault queue with a conservative loss latch."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock

from ...domain._validation import DomainValidationError, require_positive
from .model import OperationalFaultSeverityV1, OperationalFaultV1


@dataclass(frozen=True)
class MonitoringGapLatchV1:
    """A loss summary.  It is evidence of missing diagnostics, never a clear."""

    first_sequence: int
    last_sequence: int
    dropped_count: int
    severity_bitmap: int

    def __post_init__(self) -> None:
        require_positive(self.dropped_count, "monitoring gap dropped count")
        if self.first_sequence > self.last_sequence:
            raise DomainValidationError("monitoring gap sequence range is reversed")


class OperationalFaultQueueV1:
    """Keep CRITICAL reserve capacity and never wait on monitoring consumers."""

    def __init__(self, *, capacity: int, critical_reserve: int) -> None:
        require_positive(capacity, "operational fault queue capacity")
        if not 0 < critical_reserve < capacity:
            raise DomainValidationError("operational fault critical reserve is invalid")
        self._capacity, self._critical_reserve = capacity, critical_reserve
        self._items: deque[OperationalFaultV1] = deque()
        self._gap: MonitoringGapLatchV1 | None = None
        self._lock = Lock()

    def try_publish(self, fault: OperationalFaultV1) -> bool:
        """Publish without blocking; record a loss latch when admission is refused."""

        with self._lock:
            normal_limit = self._capacity - self._critical_reserve
            accepts = len(self._items) < self._capacity and (
                fault.severity is OperationalFaultSeverityV1.CRITICAL
                or len(self._items) < normal_limit
            )
            if accepts:
                self._items.append(fault)
                return True
            self._record_drop(fault)
            return False

    def drain(self, *, limit: int) -> tuple[OperationalFaultV1, ...]:
        require_positive(limit, "operational fault drain limit")
        with self._lock:
            return tuple(self._items.popleft() for _ in range(min(limit, len(self._items))))

    def monitoring_gap(self) -> MonitoringGapLatchV1 | None:
        with self._lock:
            return self._gap

    def acknowledge_gap_exported(self, latch: MonitoringGapLatchV1) -> None:
        """Clear only the exact gap that an independent exporter persisted."""

        with self._lock:
            if self._gap != latch:
                raise DomainValidationError("monitoring gap acknowledgement is stale")
            self._gap = None

    def _record_drop(self, fault: OperationalFaultV1) -> None:
        bit = 1 << int(fault.severity)
        if self._gap is None:
            self._gap = MonitoringGapLatchV1(fault.sequence, fault.sequence, 1, bit)
            return
        prior = self._gap
        self._gap = MonitoringGapLatchV1(
            min(prior.first_sequence, fault.sequence),
            max(prior.last_sequence, fault.sequence),
            prior.dropped_count + 1,
            prior.severity_bitmap | bit,
        )
