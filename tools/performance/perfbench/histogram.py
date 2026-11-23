"""Exact percentile aggregation for bounded benchmark samples."""

from __future__ import annotations

from math import ceil


class ExactHistogram:
    """Keeps all benchmark samples so results remain auditable and mergeable."""

    def __init__(self) -> None:
        self._samples: list[int] = []

    def record(self, value_ns: int) -> None:
        if value_ns < 0:
            raise ValueError("latency cannot be negative")
        self._samples.append(value_ns)

    @property
    def count(self) -> int:
        return len(self._samples)

    def percentile(self, quantile: float) -> int:
        if not 0 < quantile <= 1:
            raise ValueError("quantile must be in (0, 1]")
        if not self._samples:
            raise ValueError("cannot summarize an empty histogram")
        ordered = sorted(self._samples)
        return ordered[ceil(quantile * len(ordered)) - 1]

    def summary(self) -> dict[str, int]:
        if not self._samples:
            raise ValueError("cannot summarize an empty histogram")
        return {
            "count": len(self._samples),
            "min_ns": min(self._samples),
            "p50_ns": self.percentile(0.50),
            "p95_ns": self.percentile(0.95),
            "p99_ns": self.percentile(0.99),
            "p999_ns": self.percentile(0.999),
            "max_ns": max(self._samples),
        }
