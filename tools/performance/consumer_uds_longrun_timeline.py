"""Exact open-loop MIX timeline, independent of transport and server management.

Each yielded tick is a planned release, not evidence that any call completed.
The consumer runner must retain release lateness/errors and stream its evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

TICK_NS = 10_000_000
CYCLE_TICKS = 60_000
OPERATIONS = ("SubmitSignal", "ReadOrderSnapshotPage")


@dataclass(frozen=True)
class PlannedConsumerTick:
    cycle: int
    phase: str
    planned_release_ns: int
    phase_started_ns: int
    phase_ends_ns: int
    groups: tuple[tuple[str, int], ...]
    bursts: tuple[tuple[str, int], ...]


_PHASES = (
    (0, 6000, "IDLE", 0),
    (6000, 8000, "LOW_1", 1),
    (8000, 10000, "LOW_10", 10),
    (10000, 12000, "LOW_20", 20),
    (12000, 15000, "RAMP", -1),
    (15000, 27000, "HIGH", 2000),
    (27000, 30000, "STEP_LOW", 20),
    (30000, 33000, "STEP_HIGH", 2000),
    (33000, 36000, "STEP_LOW", 20),
    (36000, 39000, "STEP_HIGH", 2000),
    (39000, 45000, "FAIR", 2000),
    (45000, 51000, "BURST_MIX", 2000),
    (51000, 60000, "RECOVER", 20),
)


def _fixed_count(tick: int, rate: int) -> int:
    return ((tick + 1) * rate) // 100 - (tick * rate) // 100


def _ramp_cumulative(tick: int) -> int:
    # t=tick/100 seconds: floor(20t + 33t**2), without floating-point drift.
    return (2000 * tick + 33 * tick * tick) // 10_000


def _ramp_low_counts() -> tuple[int, ...]:
    # Never allocate a negative H count when the rounded total has an empty
    # tick. Carry a due L release to the next available planned total slot.
    assigned = 0
    result = []
    for tick in range(3000):
        total = _ramp_cumulative(tick + 1) - _ramp_cumulative(tick)
        count = min(total, ((tick + 1) * 20) // 100 - assigned)
        result.append(count)
        assigned += count
    assert assigned == 600
    return tuple(result)


_RAMP_LOW_COUNTS = _ramp_low_counts()


class ConsumerMixedTimeline:
    def __init__(self, *, cycles: int, start_ns: int) -> None:
        if (
            type(cycles) is not int
            or not 1 <= cycles <= 432
            or type(start_ns) is not int
            or start_ns < 0
        ):
            raise ValueError("MIX timeline horizon/start is invalid")
        self.cycles, self.start_ns = cycles, start_ns

    @property
    def duration_ns(self) -> int:
        return self.cycles * CYCLE_TICKS * TICK_NS

    def at(self, absolute_tick: int) -> PlannedConsumerTick:
        if type(absolute_tick) is not int or not 0 <= absolute_tick < self.cycles * CYCLE_TICKS:
            raise ValueError("planned tick is outside the frozen measurement horizon")
        cycle, tick = divmod(absolute_tick, CYCLE_TICKS)
        for start, end, phase, rate in _PHASES:
            if start <= tick < end:
                break
        else:
            raise AssertionError("timeline contains an uncovered tick")
        offset = tick - start
        groups: list[tuple[str, int]] = []
        if phase == "RAMP":
            total = _ramp_cumulative(offset + 1) - _ramp_cumulative(offset)
            low = _RAMP_LOW_COUNTS[offset]
            if low:
                groups.append(("L", low))
            if total - low:
                groups.append(("H", total - low))
        elif phase == "FAIR":
            for n in range(20):
                count = _fixed_count(offset, 10)
                if count:
                    groups.append((f"F{n:02d}", count))
            groups.append(("H", 18))
        elif rate == 2000:
            low = _fixed_count(offset, 20)
            if low:
                groups.append(("L", low))
            groups.append(("H", 20 - low))
        elif rate:
            count = _fixed_count(offset, rate)
            if count:
                groups.append(("L", count))
        bursts = ()
        if tick == 45000:
            bursts = ((OPERATIONS[0], 2000),)
        elif tick == 47000:
            bursts = ((OPERATIONS[1], 2000),)
        elif tick == 49000:
            bursts = ((OPERATIONS[0], 1000), (OPERATIONS[1], 1000))
        base = self.start_ns + cycle * CYCLE_TICKS * TICK_NS
        return PlannedConsumerTick(
            cycle,
            phase,
            base + tick * TICK_NS,
            base + start * TICK_NS,
            base + end * TICK_NS,
            tuple(groups),
            bursts,
        )

    def ticks(self) -> Iterator[PlannedConsumerTick]:
        for n in range(self.cycles * CYCLE_TICKS):
            yield self.at(n)

    def expected_totals(self) -> dict[str, int]:
        return {
            "cycles": self.cycles,
            "duration_seconds": self.cycles * 600,
            "submit": 636920 * self.cycles,
            "snapshot": 636920 * self.cycles,
            "burst_barriers": 3 * self.cycles,
            "burst_requests": 6000 * self.cycles,
        }
