from __future__ import annotations

import gc
from dataclasses import replace
from threading import Event, get_ident
from types import SimpleNamespace

import pytest
from xuanling_tradecore.consumer.runtime_health import (
    RuntimeAdmissionGuard,
    RuntimeAdmissionUnavailable,
    RuntimeGcObserver,
    RuntimeHealthState,
    RuntimeResourceSample,
    RuntimeResourceSampler,
    sample_cgroup_resources,
)
from xuanling_tradecore.consumer.runtime_profile import ConsumerLongRunningPolicy


def setup():
    policy = ConsumerLongRunningPolicy()
    now = [100]
    guard = RuntimeAdmissionGuard(
        policy=policy, disk_reserve_bytes=64 * 1024**3, stale_after_ns=10, clock=lambda: now[0]
    )
    sample = RuntimeResourceSample(100, 1024**3, 6 * 1024**3, 1024**4, 0, 0, 0, True)
    return policy, now, guard, sample


def test_cached_gate_stale_hysteresis_and_terminal_close() -> None:
    policy, now, guard, sample = setup()
    with pytest.raises(RuntimeAdmissionUnavailable):
        guard.require_admission()
    guard.observe(sample)
    guard.require_admission()
    now[0] = 111
    with pytest.raises(RuntimeAdmissionUnavailable):
        guard.require_admission()
    guard.observe(replace(sample, sampled_ns=111, memory_bytes=policy.memory_guard_bytes))
    assert guard.state() is RuntimeHealthState.PROTECTING
    now[0] = 112
    guard.observe(replace(sample, sampled_ns=112, memory_bytes=policy.memory_target_bytes))
    assert guard.state() is RuntimeHealthState.PROTECTING
    now[0] = 113
    guard.observe(replace(sample, sampled_ns=113))
    guard.require_admission()
    assert guard.allow_optional_maintenance()
    guard.close()
    guard.observe(replace(sample, sampled_ns=114))
    assert guard.state() is RuntimeHealthState.CLOSED


def test_guard_excludes_only_bounded_reclaimable_file_cache() -> None:
    policy, now, guard, sample = setup()
    total = policy.memory_guard_bytes + 512 * 1024**2
    reclaimable = 1024**3
    guard.observe(
        replace(
            sample,
            memory_bytes=total,
            reclaimable_file_bytes=reclaimable,
        )
    )
    assert guard.state() is RuntimeHealthState.HEALTHY
    assert guard.evidence()["memory_current_bytes"] == total
    assert guard.evidence()["admission_memory_bytes"] == total - reclaimable

    now[0] = 101
    guard.observe(
        replace(
            sample,
            sampled_ns=101,
            memory_bytes=total,
            reclaimable_file_bytes=128 * 1024**2,
        )
    )
    assert guard.state() is RuntimeHealthState.PROTECTING


def test_cgroup_sampler_subtracts_only_clean_inactive_file_cache(tmp_path, monkeypatch) -> None:
    cgroup = tmp_path / "cgroup"
    data = tmp_path / "data"
    cgroup.mkdir()
    data.mkdir()
    (cgroup / "memory.current").write_text("1000\n")
    (cgroup / "memory.max").write_text(str(6 * 1024**3))
    (cgroup / "memory.events").write_text("max 0\noom 0\noom_kill 0\n")
    (cgroup / "memory.stat").write_text(
        "file 800\ninactive_file 700\nfile_dirty 100\nfile_writeback 50\nshmem 200\n"
    )
    monkeypatch.setattr(gc, "isenabled", lambda: True)
    monkeypatch.setattr(
        "xuanling_tradecore.consumer.runtime_health.os.statvfs",
        lambda _path: SimpleNamespace(f_bavail=1024**3, f_frsize=4096),
        raising=False,
    )
    sample = sample_cgroup_resources(cgroup_root=cgroup, data_root=data)
    assert sample.memory_bytes == 1000
    assert sample.reclaimable_file_bytes == 450
    assert sample.admission_memory_bytes == 550


def test_cgroup_sampler_fails_closed_without_working_set_counters(tmp_path) -> None:
    cgroup = tmp_path / "cgroup"
    data = tmp_path / "data"
    cgroup.mkdir()
    data.mkdir()
    (cgroup / "memory.current").write_text("1000\n")
    (cgroup / "memory.max").write_text(str(6 * 1024**3))
    (cgroup / "memory.events").write_text("max 0\noom 0\noom_kill 0\n")
    (cgroup / "memory.stat").write_text("file 800\n")
    with pytest.raises(ValueError, match="working-set fields"):
        sample_cgroup_resources(cgroup_root=cgroup, data_root=data)


@pytest.mark.parametrize(
    "field,value",
    [
        ("oom_events", 1),
        ("memory_max_events", 1),
        ("automatic_gc_enabled", False),
        ("memory_limit_bytes", 8 * 1024**3),
        ("sampled_ns", 99),
    ],
)
def test_invalid_observation_is_sticky_failure(field, value) -> None:
    _, now, guard, sample = setup()
    guard.observe(sample)
    now[0] = 101
    bad = (
        replace(sample, sampled_ns=101, **{field: value})
        if field != "sampled_ns"
        else replace(sample, sampled_ns=value)
    )
    guard.observe(bad)
    assert guard.state() is RuntimeHealthState.FAILED
    now[0] = 102
    guard.observe(replace(sample, sampled_ns=102))
    with pytest.raises(RuntimeAdmissionUnavailable):
        guard.require_admission()


def test_sampler_io_is_off_caller_and_failure_closes_admission() -> None:
    _, _, guard, _ = setup()
    called = Event()
    threads = []

    def sample():
        threads.append(get_ident())
        called.set()
        raise OSError("injected")

    sampler = RuntimeResourceSampler(guard=guard, sample=sample, interval_seconds=0.01)
    sampler.start()
    try:
        assert called.wait(5)
        sampler._thread.join(5)
        assert threads == [sampler._thread.ident] and threads[0] != get_ident()
        assert guard.state() is RuntimeHealthState.FAILED
    finally:
        sampler.close()


def test_gc_observer_is_bounded_and_preserves_gc_configuration(monkeypatch) -> None:
    monkeypatch.setattr(gc, "callbacks", [])
    now = [1]
    observer = RuntimeGcObserver(capacity=2, clock=lambda: now[0])
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    observer.start()
    try:
        for n in range(5):
            observer._observe("start", {"generation": 0})
            now[0] += 10
            observer._observe("stop", {"generation": 0})
        assert (
            observer.count == 5
            and observer.total_pause_ns == 50
            and observer.maximum_pause_ns == 10
        )
        assert len(observer.recent()) == 2
        evidence = observer.pause_evidence()
        assert evidence["intervals"] == ((4, 31, 41, 0), (5, 41, 51, 0))
        assert evidence["omitted_before_sequence"] == 3
        assert evidence["full_history_evidence"] is False
        assert observer.recent() == ((41, 0, 10), (51, 0, 10))
    finally:
        observer.close()
    assert gc.isenabled() == enabled and gc.get_threshold() == thresholds
    assert observer._callback not in gc.callbacks


def test_gc_observer_freezes_started_graph_once_without_disabling_automatic_gc(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gc, "callbacks", [])
    calls: list[str] = []
    monkeypatch.setattr(gc, "collect", lambda: calls.append("collect") or 37)
    monkeypatch.setattr(gc, "freeze", lambda: calls.append("freeze"))
    monkeypatch.setattr(gc, "get_freeze_count", lambda: 1234)
    observer = RuntimeGcObserver(clock=lambda: 9876)

    with pytest.raises(RuntimeError, match="started enabled observer"):
        observer.freeze_stable_graph()
    observer.start()
    try:
        observer.freeze_stable_graph()
        assert calls == ["collect", "freeze"]
        assert observer.stable_graph_frozen
        assert observer.prefreeze_collected == 37
        assert observer.frozen_objects == 1234
        assert observer.frozen_at_ns == 9876
        assert gc.isenabled()
        with pytest.raises(RuntimeError, match="started enabled observer"):
            observer.freeze_stable_graph()
    finally:
        observer.close()


def test_gc_compatibility_view_does_not_iterate_live_ring_during_callback(monkeypatch):
    now = [1]
    observer = RuntimeGcObserver(capacity=2, clock=lambda: now[0])
    for _ in range(2):
        observer._observe("start", {"generation": 0})
        now[0] += 10
        observer._observe("stop", {"generation": 0})
    triggered = [False]

    def callback_during_allocation(*values):
        if not triggered[0]:
            triggered[0] = True
            observer._observe("start", {"generation": 1})
            now[0] += 5
            observer._observe("stop", {"generation": 1})
        return max(*values)

    monkeypatch.setattr(
        "xuanling_tradecore.consumer.runtime_health.max", callback_during_allocation, raising=False
    )
    assert observer.recent() == ((11, 0, 10), (21, 0, 10))
    assert observer.count == 3
