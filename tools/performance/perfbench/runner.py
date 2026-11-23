"""Full-catalog benchmark execution and report validation."""

from __future__ import annotations

import platform
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from .catalog import TRADER_API, TRADER_SPI, InterfaceSpec, all_specs
from .driver import BenchmarkDriver
from .histogram import ExactHistogram


class CoverageError(RuntimeError):
    pass


def _scenario_key(spec: InterfaceSpec, workload: str) -> str:
    return f"{spec.kind}:{spec.name}:{workload}"


def run(driver: BenchmarkDriver, *, iterations: int) -> dict[str, Any]:
    if iterations <= 0:
        raise ValueError("iterations must be positive")

    scenarios: list[dict[str, Any]] = []
    covered_api: set[str] = set()
    covered_spi: set[str] = set()
    for spec in all_specs():
        target = covered_api if spec.kind == "api" else covered_spi
        target.add(spec.name)
        for workload in spec.workloads:
            histogram = ExactHistogram()
            for _ in range(iterations):
                samples = driver.invoke(spec, workload)
                if not samples:
                    raise CoverageError(
                        f"driver returned no timings for {_scenario_key(spec, workload)}"
                    )
                for value in samples.values():
                    histogram.record(value)
            scenarios.append(
                {
                    "key": _scenario_key(spec, workload),
                    "interface": asdict(spec),
                    "latency": histogram.summary(),
                }
            )

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "driver": driver.name,
        "environment": {
            "python": sys.version,
            "implementation": sys.implementation.name,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "catalog": {"trader_api": len(TRADER_API), "trader_spi": len(TRADER_SPI)},
        "coverage": {"api": sorted(covered_api), "spi": sorted(covered_spi)},
        "scenarios": scenarios,
    }
    assert_full_coverage(report)
    return report


def assert_full_coverage(report: dict[str, Any]) -> None:
    coverage = report.get("coverage", {})
    actual_api = set(coverage.get("api", []))
    actual_spi = set(coverage.get("spi", []))
    missing_api = set(TRADER_API) - actual_api
    missing_spi = set(TRADER_SPI) - actual_spi
    extra_api = actual_api - set(TRADER_API)
    extra_spi = actual_spi - set(TRADER_SPI)
    if missing_api or missing_spi or extra_api or extra_spi:
        raise CoverageError(
            "catalog mismatch: "
            f"missing_api={sorted(missing_api)} missing_spi={sorted(missing_spi)} "
            f"extra_api={sorted(extra_api)} extra_spi={sorted(extra_spi)}"
        )
