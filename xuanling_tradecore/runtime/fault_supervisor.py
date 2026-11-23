"""Independent, non-trading supervisor for exported monitoring-gap evidence."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable

from ..domain._validation import require_nonnegative
from .fault_monitor.gap_page import RuntimeFaultGapPageV1


@dataclass(frozen=True)
class RuntimeFaultGapEvidenceV1:
    process_epoch: int
    first_sequence: int
    last_sequence: int
    dropped_count: int
    severity_bitmap: int
    observed_monotonic_ns: int

    def __post_init__(self) -> None:
        for value, name in (
            (self.process_epoch, "fault evidence process epoch"),
            (self.first_sequence, "fault evidence first sequence"),
            (self.last_sequence, "fault evidence last sequence"),
            (self.dropped_count, "fault evidence dropped count"),
            (self.severity_bitmap, "fault evidence severity bitmap"),
            (self.observed_monotonic_ns, "fault evidence observed time"),
        ):
            require_nonnegative(value, name)


class RuntimeFaultSupervisorV1:
    """Export each active gap once; it never modifies trading state or clears a latch."""

    def __init__(
        self,
        *,
        read_page: Callable[[], bytes],
        persist_evidence: Callable[[RuntimeFaultGapEvidenceV1], None],
        persist_heartbeat: Callable[[int], None] | None = None,
        clock_ns: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._read_page, self._persist_evidence, self._clock_ns = (
            read_page,
            persist_evidence,
            clock_ns,
        )
        self._persist_heartbeat = persist_heartbeat
        self._last_exported: tuple[int, int, int, int, int] | None = None
        self._last_heartbeat_ns = 0

    def poll_once(self) -> RuntimeFaultGapEvidenceV1 | None:
        now = self._clock_ns()
        self._last_heartbeat_ns = now
        if self._persist_heartbeat is not None:
            self._persist_heartbeat(now)
        page = RuntimeFaultGapPageV1.decode(self._read_page())
        if not page.active:
            return None
        identity = (
            page.process_epoch,
            page.first_sequence,
            page.last_sequence,
            page.dropped_count,
            page.severity_bitmap,
        )
        if identity == self._last_exported:
            return None
        evidence = RuntimeFaultGapEvidenceV1(*identity, observed_monotonic_ns=now)
        self._persist_evidence(evidence)
        self._last_exported = identity
        return evidence

    @property
    def last_heartbeat_monotonic_ns(self) -> int:
        return self._last_heartbeat_ns
