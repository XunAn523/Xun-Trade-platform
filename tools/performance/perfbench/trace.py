"""Correlation-safe C0--C7 timestamp collection for a gateway integration.

Only identifiers, interface metadata, monotonic timestamps, and an outcome are
accepted.  Business payloads and credentials cannot be recorded by this API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from time import perf_counter_ns
from typing import Any, Iterable, Mapping

from .matrix import scenario_index

STAGES = ("C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7")
_STAGE_NUMBER = {stage: index for index, stage in enumerate(STAGES)}
VALID_OUTCOMES = frozenset({"ok", "failed", "timeout", "rejected", "unsupported"})
METRICS = frozenset(
    {
        "callback_ring_depth",
        "worker_queue_depth",
        "journal_queue_depth",
        "journal_durable_lag_ns",
        "process_cpu_millicores",
        "process_rss_bytes",
        "gc_collections",
        "dropped_callbacks",
        "rate_limited",
        "native_error_count",
    }
)


class TraceError(ValueError):
    """Raised for unsafe, malformed, or causally invalid trace input."""


@dataclass
class TraceRecord:
    correlation_id: str
    kind: str
    interface: str
    profile: str
    workload: str
    timestamps_ns: dict[str, int] = field(default_factory=dict)
    outcome: str | None = None
    reason: str | None = None
    _lock: Lock = field(default_factory=Lock, init=False, repr=False, compare=False)

    @property
    def scenario_key(self) -> str:
        return f"{self.kind}:{self.interface}:{self.profile}:{self.workload}"

    def stamp(self, stage: str, timestamp_ns: int) -> None:
        with self._lock:
            if stage not in _STAGE_NUMBER:
                raise TraceError(f"unknown stage: {stage}")
            if not isinstance(timestamp_ns, int) or timestamp_ns < 0:
                raise TraceError("timestamp_ns must be a non-negative integer")
            if stage in self.timestamps_ns:
                raise TraceError(f"duplicate timestamp for {self.correlation_id}:{stage}")
            for recorded_stage, recorded_timestamp in self.timestamps_ns.items():
                if (
                    _STAGE_NUMBER[recorded_stage] < _STAGE_NUMBER[stage]
                    and recorded_timestamp > timestamp_ns
                ):
                    raise TraceError(f"non-monotonic stage timestamp for {self.correlation_id}")
                if (
                    _STAGE_NUMBER[recorded_stage] > _STAGE_NUMBER[stage]
                    and recorded_timestamp < timestamp_ns
                ):
                    raise TraceError(f"out-of-order stage timestamp for {self.correlation_id}")
            self.timestamps_ns[stage] = timestamp_ns

    def complete(self, outcome: str, reason: str | None = None) -> None:
        with self._lock:
            if outcome not in VALID_OUTCOMES:
                raise TraceError(f"unknown outcome: {outcome}")
            self.outcome = outcome
            self.reason = reason

    def as_events(self) -> list[dict[str, Any]]:
        """Return payload-free JSONL-ready event objects in timestamp order."""

        with self._lock:
            events = [
                {
                    "correlation_id": self.correlation_id,
                    "kind": self.kind,
                    "interface": self.interface,
                    "profile": self.profile,
                    "workload": self.workload,
                    "stage": stage,
                    "timestamp_ns": timestamp,
                }
                for stage, timestamp in sorted(
                    self.timestamps_ns.items(), key=lambda item: _STAGE_NUMBER[item[0]]
                )
            ]
            if self.outcome is not None:
                events.append(
                    {
                        "correlation_id": self.correlation_id,
                        "kind": self.kind,
                        "interface": self.interface,
                        "profile": self.profile,
                        "workload": self.workload,
                        "outcome": self.outcome,
                        "reason": self.reason or "",
                    }
                )
            return events

    def snapshot(self) -> TraceRecord:
        """Return a detached, coherent copy for report construction."""

        with self._lock:
            return TraceRecord(
                correlation_id=self.correlation_id,
                kind=self.kind,
                interface=self.interface,
                profile=self.profile,
                workload=self.workload,
                timestamps_ns=dict(self.timestamps_ns),
                outcome=self.outcome,
                reason=self.reason,
            )


@dataclass(frozen=True)
class MetricSample:
    """A bounded gateway-health measurement captured beside the trace stream."""

    metric: str
    value: int
    timestamp_ns: int

    def as_event(self) -> dict[str, Any]:
        return {
            "event_type": "metric",
            "metric": self.metric,
            "value": self.value,
            "timestamp_ns": self.timestamp_ns,
        }


class GatewayTraceCollector:
    """Thread-safe collector to be instantiated once in the gateway process."""

    def __init__(self) -> None:
        self._records: dict[str, TraceRecord] = {}
        self._metrics: list[MetricSample] = []
        self._lock = Lock()

    def begin(
        self,
        correlation_id: str,
        *,
        kind: str,
        interface: str,
        profile: str,
        workload: str,
        timestamp_ns: int | None = None,
    ) -> None:
        key = f"{kind}:{interface}:{profile}:{workload}"
        if key not in scenario_index():
            raise TraceError(f"unknown matrix scenario: {key}")
        if not correlation_id:
            raise TraceError("correlation_id is required")
        with self._lock:
            if correlation_id in self._records:
                raise TraceError(f"duplicate correlation_id: {correlation_id}")
            record = TraceRecord(correlation_id, kind, interface, profile, workload)
            record.stamp("C0", perf_counter_ns() if timestamp_ns is None else timestamp_ns)
            self._records[correlation_id] = record

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None:
        record = self._record(correlation_id)
        record.stamp(stage, perf_counter_ns() if timestamp_ns is None else timestamp_ns)

    def complete(self, correlation_id: str, outcome: str = "ok", reason: str | None = None) -> None:
        self._record(correlation_id).complete(outcome, reason)

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None:
        """Record a bounded health metric; arbitrary labels and payloads are rejected."""

        if metric not in METRICS:
            raise TraceError(f"unknown metric: {metric}")
        if not isinstance(value, int) or value < 0:
            raise TraceError("metric value must be a non-negative integer")
        timestamp = perf_counter_ns() if timestamp_ns is None else timestamp_ns
        if not isinstance(timestamp, int) or timestamp < 0:
            raise TraceError("timestamp_ns must be a non-negative integer")
        with self._lock:
            self._metrics.append(MetricSample(metric, value, timestamp))

    def ingest(self, event: Mapping[str, Any]) -> None:
        """Ingest one JSONL event emitted by another gateway process."""

        if event.get("event_type") == "metric":
            self.record_metric(
                str(event.get("metric")),
                event.get("value"),
                event.get("timestamp_ns"),
            )
            return
        required = ("correlation_id", "kind", "interface", "profile", "workload")
        if any(not isinstance(event.get(field), str) or not event[field] for field in required):
            raise TraceError("trace event is missing string identity fields")
        correlation_id = str(event["correlation_id"])
        with self._lock:
            if correlation_id not in self._records:
                key = f"{event['kind']}:{event['interface']}:{event['profile']}:{event['workload']}"
                if key not in scenario_index():
                    raise TraceError(f"unknown matrix scenario: {key}")
                self._records[correlation_id] = TraceRecord(
                    correlation_id=correlation_id,
                    kind=str(event["kind"]),
                    interface=str(event["interface"]),
                    profile=str(event["profile"]),
                    workload=str(event["workload"]),
                )
            record = self._records[correlation_id]
            for field in required[1:]:
                if getattr(record, field) != event[field]:
                    raise TraceError(f"conflicting identity for correlation_id: {correlation_id}")
        if "stage" in event:
            record.stamp(str(event["stage"]), event.get("timestamp_ns"))
        if "outcome" in event:
            record.complete(str(event["outcome"]), str(event.get("reason") or "") or None)

    def records(self) -> tuple[TraceRecord, ...]:
        with self._lock:
            records = tuple(self._records.values())
        return tuple(record.snapshot() for record in records)

    def metrics(self) -> tuple[MetricSample, ...]:
        with self._lock:
            return tuple(self._metrics)

    def export_events(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                *[event for record in self._records.values() for event in record.as_events()],
                *[sample.as_event() for sample in self._metrics],
            ]

    def _record(self, correlation_id: str) -> TraceRecord:
        with self._lock:
            try:
                return self._records[correlation_id]
            except KeyError as exc:
                raise TraceError(f"unknown correlation_id: {correlation_id}") from exc


def collector_from_events(events: Iterable[Mapping[str, Any]]) -> GatewayTraceCollector:
    collector = GatewayTraceCollector()
    for event in events:
        collector.ingest(event)
    return collector
