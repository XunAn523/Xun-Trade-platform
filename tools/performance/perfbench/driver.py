"""Driver contract; real EMT access is deliberately injected, never hard-coded."""

from __future__ import annotations

from time import perf_counter_ns
from typing import Protocol

from .catalog import InterfaceSpec


class BenchmarkDriver(Protocol):
    name: str

    def invoke(self, spec: InterfaceSpec, workload: str) -> dict[str, int]:
        """Return non-negative stage durations in nanoseconds for one scenario."""


class SyntheticGatewayDriver:
    """Deterministic CPU-only driver for CI and harness validation.

    It does not call EMT or model venue latency.  Its only legitimate use is
    measuring test/harness overhead and proving full catalog coverage.
    """

    name = "synthetic"

    def invoke(self, spec: InterfaceSpec, workload: str) -> dict[str, int]:
        started = perf_counter_ns()
        accumulator = 0
        rounds = 4 + ((len(spec.name) + len(workload)) % 12)
        for index in range(rounds):
            accumulator ^= (index + 1) * (len(spec.name) + len(workload))
        finished = perf_counter_ns()
        # Keep the calculation observable without contaminating the timing API.
        if accumulator < 0:  # pragma: no cover - impossible, but preserves use.
            raise RuntimeError("synthetic accumulator underflow")
        return {"driver_ns": finished - started}
