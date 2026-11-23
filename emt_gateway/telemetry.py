"""Payload-free C0--C7 trace protocol shared with the performance harness."""

from __future__ import annotations

from typing import Protocol


class PerformanceTracePort(Protocol):
    """Compatible with ``perfbench.trace.GatewayTraceCollector`` by duck typing."""

    def begin(
        self,
        correlation_id: str,
        *,
        kind: str,
        interface: str,
        profile: str,
        workload: str,
        timestamp_ns: int | None = None,
    ) -> None: ...

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None: ...

    def complete(
        self, correlation_id: str, outcome: str = "ok", reason: str | None = None
    ) -> None: ...

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None: ...


class NullPerformanceTrace:
    """Production-safe default when a performance run is not enabled."""

    def begin(self, correlation_id: str, **_: object) -> None:
        return None

    def mark(self, correlation_id: str, stage: str, timestamp_ns: int | None = None) -> None:
        return None

    def complete(self, correlation_id: str, outcome: str = "ok", reason: str | None = None) -> None:
        return None

    def record_metric(self, metric: str, value: int, timestamp_ns: int | None = None) -> None:
        return None
