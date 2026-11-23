"""Explicit resource contract for benchmark and persistent Consumer runtimes."""

from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from typing import Mapping


class ConsumerRuntimeProfile(str, Enum):
    BENCHMARK = "BENCHMARK"
    LONG_RUNNING = "LONG_RUNNING"


class ConsumerRuntimePolicyError(ValueError):
    """A runtime budget or lifecycle capability is not safe to enable."""


_GIB = 1_024**3
REQUIRED_LONG_RUNNING_CAPABILITIES = frozenset(
    {
        "segmented_journal",
        "state_recovery",
        "bounded_projection",
        "persistent_cursors",
        "trace_rotation",
        "runtime_maintenance",
    }
)


@dataclass(frozen=True)
class ConsumerLongRunningPolicy:
    segment_frames: int = 65_536
    spare_segments: int = 2
    maximum_active_orders: int = 50_000
    replay_cache_events: int = 16_384
    cold_filter_bytes: int = 64 * 1_024**2
    snapshot_budget_bytes: int = 768 * 1_024**2
    journal_disk_budget_bytes: int = 32 * _GIB
    disk_reserve_bytes: int = 4 * _GIB
    memory_target_bytes: int = 9 * _GIB // 2
    memory_guard_bytes: int = 51 * _GIB // 10
    memory_limit_bytes: int = 6 * _GIB
    trace_segment_bytes: int = 128 * 1_024**2
    trace_disk_budget_bytes: int = 16 * _GIB

    def __post_init__(self) -> None:
        if any(
            type(getattr(self, item.name)) is not int or getattr(self, item.name) <= 0
            for item in fields(self)
        ):
            raise ConsumerRuntimePolicyError("runtime budgets must be positive integers")
        if not 2 <= self.segment_frames <= 1_000_000 or not 2 <= self.spare_segments <= 8:
            raise ConsumerRuntimePolicyError("segment/spare capacity is outside supported bounds")
        if self.segment_frames % 2:
            raise ConsumerRuntimePolicyError("segment capacity must fit complete two-fact intents")
        if not 64 <= self.cold_filter_bytes <= 64 * 1_024**2:
            raise ConsumerRuntimePolicyError("cold negative filter exceeds its bounded memory budget")
        if not self.memory_target_bytes < self.memory_guard_bytes < self.memory_limit_bytes:
            raise ConsumerRuntimePolicyError("memory target/guard/limit must be strictly ordered")
        prepared_bytes = (self.spare_segments + 1) * (4_096 + self.segment_frames * 4_096)
        if self.journal_disk_budget_bytes < prepared_bytes + self.disk_reserve_bytes:
            raise ConsumerRuntimePolicyError("journal disk budget cannot hold prepared segments")
        if self.trace_disk_budget_bytes < self.trace_segment_bytes * 2:
            raise ConsumerRuntimePolicyError("trace budget must hold two complete segments")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> ConsumerLongRunningPolicy:
        allowed = {item.name for item in fields(cls)}
        if not set(raw) <= allowed:
            raise ConsumerRuntimePolicyError("unknown long-running budget field")
        defaults = cls()
        values: dict[str, int] = {}
        for name in allowed:
            value = raw.get(name, getattr(defaults, name))
            if type(value) is not int:
                raise ConsumerRuntimePolicyError("runtime budgets must be integer values")
            values[name] = value
        return cls(**values)


def require_long_running_capabilities(implemented: frozenset[str]) -> None:
    missing = REQUIRED_LONG_RUNNING_CAPABILITIES - implemented
    if missing:
        raise ConsumerRuntimePolicyError(
            "LONG_RUNNING implementation incomplete: " + ",".join(sorted(missing))
        )


def fact_capacity_budget(
    *,
    submit_tps: int,
    facts_per_submit: int,
    duration_seconds: int,
    frame_bytes: int = 4_096,
) -> dict[str, int]:
    values = (submit_tps, facts_per_submit, duration_seconds, frame_bytes)
    if any(type(value) is not int or value <= 0 for value in values):
        raise ConsumerRuntimePolicyError("capacity inputs must be positive integers")
    fact_rate = submit_tps * facts_per_submit
    return {
        "fact_rate": fact_rate,
        "required_frames": fact_rate * duration_seconds,
        "raw_bytes_per_second": fact_rate * frame_bytes,
        "raw_bytes": fact_rate * frame_bytes * duration_seconds,
    }
