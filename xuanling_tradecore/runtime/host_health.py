"""Read-only host-health worker with one-way fail-closed protection."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from time import monotonic_ns

from ..domain._validation import DomainValidationError, require_nonnegative, require_text


class HostHealthImpactV1(str, Enum):
    INTEGRITY = "INTEGRITY"
    CAPACITY = "CAPACITY"


@dataclass(frozen=True)
class HostHealthObservationV1:
    """A bounded result from a non-hot-path host or dependency probe."""

    probe: str
    healthy: bool
    impact: HostHealthImpactV1
    scope: str
    source_reference: str

    def __post_init__(self) -> None:
        require_text(self.probe, "host-health probe", max_length=80)
        require_text(self.scope, "host-health scope", max_length=160)
        require_text(self.source_reference, "host-health source reference", max_length=256)


@dataclass(frozen=True)
class HostHealthReportV1:
    observed_monotonic_ns: int
    observations: tuple[HostHealthObservationV1, ...]

    def __post_init__(self) -> None:
        require_nonnegative(self.observed_monotonic_ns, "host-health observation time")
        if not self.observations:
            raise DomainValidationError("host-health report requires at least one observation")

    @property
    def healthy(self) -> bool:
        return all(observation.healthy for observation in self.observations)


class HostHealthWorkerV1:
    """Apply only protective actions; successful probes never clear an existing gate."""

    def __init__(
        self,
        *,
        read_observations: Callable[[], Sequence[HostHealthObservationV1]],
        disarm_integrity: Callable[[str, str], None],
        stop_new_work: Callable[[str, str], None],
        clock_ns: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._read_observations = read_observations
        self._disarm_integrity = disarm_integrity
        self._stop_new_work = stop_new_work
        self._clock_ns = clock_ns
        self._last_report: HostHealthReportV1 | None = None

    @property
    def last_report(self) -> HostHealthReportV1 | None:
        return self._last_report

    def probe_once(self) -> HostHealthReportV1:
        """Read probes outside admission locks and apply only observed failures."""

        try:
            observations = tuple(self._read_observations())
        except Exception as exc:
            # A failed health reader cannot be interpreted as healthy.  Keep
            # the failure boundary explicit and protect the whole runtime;
            # callers may still observe the original error for diagnostics.
            self._disarm_integrity("runtime", f"host-health-reader:{type(exc).__name__}")
            raise
        report = HostHealthReportV1(self._clock_ns(), observations)
        for observation in report.observations:
            if observation.healthy:
                continue
            if observation.impact is HostHealthImpactV1.INTEGRITY:
                self._disarm_integrity(observation.scope, observation.source_reference)
            else:
                self._stop_new_work(observation.scope, observation.source_reference)
        self._last_report = report
        return report
