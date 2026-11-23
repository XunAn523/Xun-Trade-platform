"""Payload-free Linux scheduling attribution outside a C0--C7 window."""

from __future__ import annotations

import os
import platform
import time
from dataclasses import dataclass
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover
    resource = None  # type: ignore[assignment]


@dataclass(frozen=True)
class _Snapshot:
    monotonic_ns: int
    process_cpu_ns: int | None
    context_switches: dict[str, int] | None
    page_faults: dict[str, int] | None
    migrations: int | None
    irq_total: int | None


class LinuxRuntimeTelemetry:
    """Collect host counters without invoking any service or hot-path I/O."""

    def capture(self) -> _Snapshot:
        return _Snapshot(
            monotonic_ns=time.monotonic_ns(),
            process_cpu_ns=_process_cpu_ns(),
            context_switches=_context_switches(),
            page_faults=_page_faults(),
            migrations=_migrations(),
            irq_total=_irq_total(),
        )

    @staticmethod
    def report(before: _Snapshot, after: _Snapshot) -> dict[str, Any]:
        return {
            "capture": "linux_counter_delta_outside_C0_to_C7",
            "window_ns": max(0, after.monotonic_ns - before.monotonic_ns),
            "linux": {
                "available": platform.system() == "Linux",
                "process_cpu_ns_delta": _int_delta(after.process_cpu_ns, before.process_cpu_ns),
                "process_context_switches_delta": _mapping_delta(
                    after.context_switches, before.context_switches
                ),
                "process_page_faults_delta": _mapping_delta(after.page_faults, before.page_faults),
                "process_migrations_delta": _int_delta(after.migrations, before.migrations),
                "host_irq_total_delta": _int_delta(after.irq_total, before.irq_total),
            },
            "payload_free": True,
        }


def _process_cpu_ns() -> int | None:
    if platform.system() != "Linux":
        return None
    try:
        clocks = os.times()
        return int((clocks.user + clocks.system) * 1_000_000_000)
    except OSError:
        return None


def _context_switches() -> dict[str, int] | None:
    if platform.system() != "Linux":
        return None
    try:
        values = {}
        for line in open("/proc/self/status", encoding="ascii"):
            key, separator, value = line.partition(":")
            if separator and key in {"voluntary_ctxt_switches", "nonvoluntary_ctxt_switches"}:
                values[key] = int(value.strip())
        return values if len(values) == 2 else None
    except (OSError, ValueError):
        return None


def _page_faults() -> dict[str, int] | None:
    if platform.system() != "Linux" or resource is None:
        return None
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {"minor": int(usage.ru_minflt), "major": int(usage.ru_majflt)}
    except (OSError, ValueError):
        return None


def _migrations() -> int | None:
    if platform.system() != "Linux":
        return None
    try:
        for line in open("/proc/self/sched", encoding="ascii"):
            key, separator, value = line.partition(":")
            if separator and key.strip() in {"nr_migrations", "se.nr_migrations"}:
                return int(value.strip())
    except (OSError, ValueError):
        return None
    return None


def _irq_total() -> int | None:
    if platform.system() != "Linux":
        return None
    try:
        total = 0
        with open("/proc/interrupts", encoding="ascii", errors="ignore") as handle:
            for line in handle:
                total += sum(
                    int(value) for value in line.replace(":", " ").split() if value.isdigit()
                )
        return total
    except (OSError, ValueError):
        return None


def _int_delta(after: int | None, before: int | None) -> int | None:
    return after - before if after is not None and before is not None and after >= before else None


def _mapping_delta(
    after: dict[str, int] | None, before: dict[str, int] | None
) -> dict[str, int] | None:
    if after is None or before is None or set(after) != set(before):
        return None
    return {key: max(0, after[key] - before[key]) for key in sorted(after)}
