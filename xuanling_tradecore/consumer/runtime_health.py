"""Cached admission pressure gates; metrics IO runs only on the sampler thread."""

from __future__ import annotations

import gc
import os
from collections import deque
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic_ns
from typing import Callable

from .runtime_profile import ConsumerLongRunningPolicy


class RuntimeAdmissionUnavailable(ValueError):
    pass


class RuntimeHealthState(str, Enum):
    RECOVERING = "RECOVERING"
    HEALTHY = "HEALTHY"
    PROTECTING = "PROTECTING"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class RuntimeResourceSample:
    sampled_ns: int
    memory_bytes: int
    memory_limit_bytes: int
    disk_free_bytes: int
    memory_max_events: int
    oom_events: int
    oom_kill_events: int
    automatic_gc_enabled: bool
    reclaimable_file_bytes: int = 0

    @property
    def admission_memory_bytes(self) -> int:
        """Conservative cgroup working set used by the admission guard.

        ``memory.current`` includes clean inactive file cache.  That cache is
        reclaimable by the kernel before ``memory.max`` needs to kill a task;
        treating it as anonymous/non-reclaimable memory makes a persistent
        Journal or SQLite index eventually reject every new order even while
        the cgroup still has several GiB of safe working-set headroom.

        The sampler only classifies the smaller of ``inactive_file`` and
        clean, non-shmem file pages as reclaimable.  Active file pages, dirty
        and writeback pages, anonymous memory and kernel memory remain charged.
        ``memory.events`` changes are still a sticky hard failure.
        """

        return self.memory_bytes - self.reclaimable_file_bytes


class RuntimeAdmissionGuard:
    def __init__(
        self,
        *,
        policy: ConsumerLongRunningPolicy,
        disk_reserve_bytes: int,
        stale_after_ns: int = 3_000_000_000,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        if (
            type(disk_reserve_bytes) is not int
            or disk_reserve_bytes < policy.disk_reserve_bytes
            or type(stale_after_ns) is not int
            or stale_after_ns < 1
        ):
            raise ValueError("runtime guard budget is invalid")
        self._policy, self._reserve, self._stale, self._clock = (
            policy,
            disk_reserve_bytes,
            stale_after_ns,
            clock,
        )
        self._lock = Lock()
        self._state = RuntimeHealthState.RECOVERING
        self._last: RuntimeResourceSample | None = None
        self._events: tuple[int, int, int] | None = None

    def observe(self, sample: RuntimeResourceSample) -> None:
        with self._lock:
            if self._state in {RuntimeHealthState.FAILED, RuntimeHealthState.CLOSED}:
                return
            numbers = (
                sample.sampled_ns,
                sample.memory_bytes,
                sample.memory_limit_bytes,
                sample.disk_free_bytes,
                sample.memory_max_events,
                sample.oom_events,
                sample.oom_kill_events,
                sample.reclaimable_file_bytes,
            )
            if (
                any(type(v) is not int or v < 0 for v in numbers)
                or sample.reclaimable_file_bytes > sample.memory_bytes
                or not self._policy.memory_guard_bytes
                < sample.memory_limit_bytes
                <= self._policy.memory_limit_bytes
                or not sample.automatic_gc_enabled
                or sample.sampled_ns > self._clock()
                or (self._last is not None and sample.sampled_ns <= self._last.sampled_ns)
            ):
                self._state = RuntimeHealthState.FAILED
                return
            events = (sample.memory_max_events, sample.oom_events, sample.oom_kill_events)
            if self._events is None:
                self._events = events
            elif events != self._events:
                self._state = RuntimeHealthState.FAILED
                return
            self._last = sample
            protecting = (
                sample.admission_memory_bytes >= self._policy.memory_guard_bytes
                or sample.disk_free_bytes <= self._reserve
            )
            if protecting:
                self._state = RuntimeHealthState.PROTECTING
            elif self._state is RuntimeHealthState.PROTECTING:
                spare = (self._policy.spare_segments + 1) * (
                    4096 + self._policy.segment_frames * 4096
                )
                if (
                    sample.admission_memory_bytes < self._policy.memory_target_bytes
                    and sample.disk_free_bytes > self._reserve + spare
                ):
                    self._state = RuntimeHealthState.HEALTHY
            else:
                self._state = RuntimeHealthState.HEALTHY

    def fail(self) -> None:
        with self._lock:
            if self._state is not RuntimeHealthState.CLOSED:
                self._state = RuntimeHealthState.FAILED

    def state(self) -> RuntimeHealthState:
        with self._lock:
            if self._state is RuntimeHealthState.HEALTHY and (
                self._last is None or self._clock() - self._last.sampled_ns > self._stale
            ):
                return RuntimeHealthState.PROTECTING
            return self._state

    def require_admission(self) -> None:
        state = self.state()
        if state is not RuntimeHealthState.HEALTHY:
            raise RuntimeAdmissionUnavailable(
                f"runtime does not permit new admission: {state.value}"
            )

    def allow_optional_maintenance(self) -> bool:
        with self._lock:
            return (
                self._state is RuntimeHealthState.HEALTHY
                and self._last is not None
                and self._clock() - self._last.sampled_ns <= self._stale
                and self._last.admission_memory_bytes < self._policy.memory_target_bytes
            )

    def evidence(self) -> dict[str, int | str | bool | None]:
        """Return a bounded non-secret snapshot for off-path status evidence."""

        with self._lock:
            sample = self._last
            state = self._state
            stale = sample is None or self._clock() - sample.sampled_ns > self._stale
            return {
                "state": (
                    RuntimeHealthState.PROTECTING.value
                    if state is RuntimeHealthState.HEALTHY and stale
                    else state.value
                ),
                "sampled_ns": None if sample is None else sample.sampled_ns,
                "memory_current_bytes": None if sample is None else sample.memory_bytes,
                "reclaimable_file_bytes": (
                    None if sample is None else sample.reclaimable_file_bytes
                ),
                "admission_memory_bytes": (
                    None if sample is None else sample.admission_memory_bytes
                ),
                "memory_limit_bytes": None if sample is None else sample.memory_limit_bytes,
                "disk_free_bytes": None if sample is None else sample.disk_free_bytes,
                "memory_max_events": None if sample is None else sample.memory_max_events,
                "oom_events": None if sample is None else sample.oom_events,
                "oom_kill_events": None if sample is None else sample.oom_kill_events,
                "automatic_gc_enabled": (
                    None if sample is None else sample.automatic_gc_enabled
                ),
                "sample_stale": stale,
            }

    def close(self) -> None:
        with self._lock:
            self._state = RuntimeHealthState.CLOSED


def sample_cgroup_resources(*, cgroup_root: Path, data_root: Path) -> RuntimeResourceSample:
    current = int((cgroup_root / "memory.current").read_text())
    limit = int(
        (cgroup_root / "memory.max").read_text()
    )  # 'max' intentionally fails the finite-budget gate
    events = dict(line.split() for line in (cgroup_root / "memory.events").read_text().splitlines())
    memory = dict(line.split() for line in (cgroup_root / "memory.stat").read_text().splitlines())
    required = {"file", "inactive_file", "file_dirty", "file_writeback", "shmem"}
    if not required <= memory.keys():
        raise ValueError("cgroup memory.stat lacks required working-set fields")
    values = {name: int(memory[name]) for name in required}
    if any(value < 0 for value in values.values()):
        raise ValueError("cgroup memory.stat contains a negative counter")
    clean_file = max(
        0,
        values["file"]
        - values["file_dirty"]
        - values["file_writeback"]
        - values["shmem"],
    )
    reclaimable_file = min(current, values["inactive_file"], clean_file)
    disk = os.statvfs(data_root)
    return RuntimeResourceSample(
        monotonic_ns(),
        current,
        limit,
        disk.f_bavail * disk.f_frsize,
        int(events["max"]),
        int(events["oom"]),
        int(events["oom_kill"]),
        gc.isenabled(),
        reclaimable_file,
    )


class RuntimeResourceSampler:
    def __init__(
        self,
        *,
        guard: RuntimeAdmissionGuard,
        sample: Callable[[], RuntimeResourceSample],
        interval_seconds: float = 1.0,
    ) -> None:
        if not 0.01 <= interval_seconds <= 10:
            raise ValueError("resource sampling interval is invalid")
        self._guard, self._sample, self._interval = guard, sample, interval_seconds
        self._stop = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("runtime sampler already started")
        self._thread = Thread(target=self._run, name="consumer-resource-sampler", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._guard.observe(self._sample())
            except Exception:
                self._guard.fail()
                return
            self._stop.wait(self._interval)

    def close(self) -> None:
        self._stop.set()
        self._guard.close()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError("runtime sampler did not stop")


class RuntimeGcObserver:
    """Bounded pause evidence; never changes thresholds or calls gc.collect()."""

    def __init__(self, capacity: int = 1024, clock: Callable[[], int] = monotonic_ns) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("GC evidence capacity is invalid")
        self._clock = clock
        self._starts: dict[int, int] = {}
        self._recent: deque[tuple[int, int, int, int]] = deque(maxlen=capacity)
        self.count = self.total_pause_ns = self.maximum_pause_ns = 0
        self.stable_graph_frozen = False
        self.prefreeze_collected = 0
        self.frozen_objects = 0
        self.frozen_at_ns = 0
        self._started = False
        self._callback = self._observe

    def _observe(self, phase: str, info: dict[str, int]) -> None:
        generation = info["generation"]
        now = self._clock()
        if phase == "start":
            self._starts[generation] = now
        elif phase == "stop":
            start = self._starts.pop(generation, None)
            if start is not None:
                duration = max(0, now - start)
                self.count += 1
                self.total_pause_ns += duration
                self.maximum_pause_ns = max(self.maximum_pause_ns, duration)
                self._recent.append((self.count, start, now, generation))

    def start(self) -> None:
        if self._started or not gc.isenabled():
            raise RuntimeError("long runtime requires enabled automatic GC and one observer")
        gc.callbacks.append(self._callback)
        self._started = True

    def freeze_stable_graph(self) -> None:
        """Compact and freeze the fully-started long-lived object graph once.

        Automatic collection stays enabled for allocations made after READY.
        This only removes the stable startup graph from later generation scans;
        it neither changes collection thresholds nor runs on a request path.
        """

        if not self._started or self.stable_graph_frozen or not gc.isenabled():
            raise RuntimeError("stable GC graph freeze requires one started enabled observer")
        self.prefreeze_collected = gc.collect()
        gc.freeze()
        self.frozen_objects = gc.get_freeze_count()
        self.frozen_at_ns = self._clock()
        self.stable_graph_frozen = True

    def recent(self) -> tuple[tuple[int, int, int], ...]:
        # Building compatibility tuples can itself trigger GC callbacks. Take
        # the C-level immutable snapshot before allocating per-record objects.
        rows = tuple(self._recent)
        return tuple((end, generation, max(0, end - start)) for _, start, end, generation in rows)

    def pause_evidence(self) -> dict[str, object]:
        """Bounded numbered intervals for off-path scheduling attribution.

        This is a recent ring, not lossless long-run storage. Consumers must
        retain successive samples and detect missing sequence numbers; a final
        status file alone cannot prove all GC windows over twelve hours.
        """
        rows = tuple(self._recent)
        return {
            "schema": "gc-pause-interval-v1",
            "fields": ("sequence", "start_monotonic_ns", "end_monotonic_ns", "generation"),
            "intervals": rows,
            "retained_intervals": len(rows),
            "omitted_before_sequence": rows[0][0] - 1 if rows else 0,
            "full_history_evidence": False,
        }

    def close(self) -> None:
        if self._started:
            gc.callbacks.remove(self._callback)
            self._started = False
            self._starts.clear()
