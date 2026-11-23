from __future__ import annotations

from collections import Counter

import pytest
from consumer_uds_longrun_timeline import CYCLE_TICKS, TICK_NS, ConsumerMixedTimeline


def test_one_cycle_has_exact_offered_counts_and_non_overlapping_phases() -> None:
    timeline = ConsumerMixedTimeline(cycles=1, start_ns=100)
    phases = Counter()
    groups = Counter()
    burst = Counter()
    last = -1
    for tick in timeline.ticks():
        assert tick.planned_release_ns > last
        assert tick.phase_started_ns <= tick.planned_release_ns < tick.phase_ends_ns
        last = tick.planned_release_ns
        for group, count in tick.groups:
            assert count > 0
            groups[group] += count
            phases[tick.phase] += count
        for operation, count in tick.bursts:
            burst[operation] += count
    assert phases == {
        "LOW_1": 20,
        "LOW_10": 200,
        "LOW_20": 400,
        "RAMP": 30300,
        "HIGH": 240000,
        "STEP_LOW": 1200,
        "STEP_HIGH": 120000,
        "FAIR": 120000,
        "BURST_MIX": 120000,
        "RECOVER": 1800,
    }
    assert burst == {"SubmitSignal": 3000, "ReadOrderSnapshotPage": 3000}
    assert sum(phases.values()) + burst["SubmitSignal"] == 636920
    assert sum(v for k, v in groups.items() if k.startswith("F")) == 12000


def test_background_is_not_paused_during_any_burst() -> None:
    timeline = ConsumerMixedTimeline(cycles=1, start_ns=100)
    for n in (45000, 47000, 49000):
        tick = timeline.at(n)
        assert sum(count for _, count in tick.groups) == 20
        assert sum(count for _, count in tick.bursts) == 2000
    combined = timeline.at(49000)
    assert len(combined.bursts) == 2
    assert combined.bursts[0][1] == combined.bursts[1][1] == 1000


def test_low_high_split_has_exact_1980_high_and_20_low_per_second() -> None:
    timeline = ConsumerMixedTimeline(cycles=1, start_ns=0)
    counts = Counter()
    for n in range(15000, 15100):
        counts.update(dict(timeline.at(n).groups))
    assert counts == {"L": 20, "H": 1980}
    assert sum(sum(v for _, v in timeline.at(n).groups) for n in range(6000, 8000)) == 20


def test_twelve_hours_are_72_cycles_without_clock_drift_or_an_extra_tick() -> None:
    timeline = ConsumerMixedTimeline(cycles=72, start_ns=123456789)
    assert timeline.expected_totals() == {
        "cycles": 72,
        "duration_seconds": 43200,
        "submit": 45858240,
        "snapshot": 45858240,
        "burst_barriers": 216,
        "burst_requests": 432000,
    }
    last = timeline.at(72 * CYCLE_TICKS - 1)
    assert last.planned_release_ns + TICK_NS == timeline.start_ns + timeline.duration_ns
    assert timeline.at(CYCLE_TICKS).phase == "IDLE"
    assert timeline.at(CYCLE_TICKS).cycle == 1
    with pytest.raises(ValueError):
        timeline.at(72 * CYCLE_TICKS)


@pytest.mark.parametrize("cycles,start", [(0, 0), (True, 0), (433, 0), (1, -1), (1, False)])
def test_invalid_horizon_is_rejected(cycles, start) -> None:
    with pytest.raises(ValueError):
        ConsumerMixedTimeline(cycles=cycles, start_ns=start)
