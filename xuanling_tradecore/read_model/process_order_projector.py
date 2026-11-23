"""Isolate verified projection CPU from the socket/reducer GIL.

Spawned child owns SQL projection only, never the Journal writer/lease. One
bounded synchronous control channel supplies trusted fences and resolved paths;
facts still come from readonly mmap and undergo the existing physical checks.
No facts, history or arbitrary operations are sent through IPC. All descendants
inherit the enclosing service cgroup; this is not an external unbudgeted worker.
"""

from __future__ import annotations

import multiprocessing as mp
import os
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event, Lock, RLock, Thread
from time import monotonic_ns, sleep
from typing import Any, Callable, cast

from ..oms.segmented_fact_journal import SegmentedJournalReadBound, SegmentedOmsFactJournal
from ..persistence.order_projection_store import OrderProjectionError
from ..persistence.stage_timing import StageTiming
from .incremental_order_projector import IncrementalOrderProjector, IncrementalProjectionHealth


class _RemoteSource:
    def __init__(
        self,
        channel: Connection,
        journal_id: str,
        profile_hash: str,
        stage_observer: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.channel, self.journal_id, self.profile_hash = channel, journal_id, profile_hash
        self._lock = RLock()
        self._bound: SegmentedJournalReadBound | None = None
        self._sampled = 0
        self._stage_observer = stage_observer

    def call(self, kind: str, value: object = None) -> object:
        def invoke():
            with self._lock:
                self.channel.send((kind, value))
                if not self.channel.poll(2):
                    raise OrderProjectionError("projection source control timed out")
                ok, result = cast(tuple[bool, object], self.channel.recv())
                if not ok:
                    raise OrderProjectionError("projection source control failed")
                return result

        if kind == "HEALTH" or self._stage_observer is None:
            return invoke()
        stage = StageTiming("projection-source", self._stage_observer)
        completed = False
        try:
            result = stage.call("source_control", invoke)
            completed = True
            return result
        finally:
            stage.finish({"control_completed": completed, "bound": kind == "BOUND"})

    def read_bound(self) -> SegmentedJournalReadBound:
        now = monotonic_ns()
        if self._bound is None or now - self._sampled >= 5_000_000:
            bound = self.call("BOUND")
            if not isinstance(bound, SegmentedJournalReadBound) or (
                bound.journal_id != self.journal_id or bound.profile_hash != self.profile_hash
            ):
                raise OrderProjectionError("invalid child source fence")
            self._bound = bound
            self._sampled = now
        return self._bound

    def segment_read_path(self, index: int) -> Path:
        path = self.call("PATH", index)
        if not isinstance(path, str):
            raise OrderProjectionError("invalid child source path")
        return Path(path)


def _child(
    channel: Connection,
    journal_id: str,
    profile_hash: str,
    root: Path,
    budget: int,
    cpu: int | None,
    maximum_transaction_batches: int,
    stage_trace_path: Path | None,
    stage_trace_budget: int,
    optimize_transactions: bool,
    commit_only_diagnostics: bool,
    reuse_verified_admission_seeds: bool,
    read_service_pause_ns: int,
    read_service_signal: Any | None,
) -> None:
    if cpu is not None:
        os.sched_setaffinity(0, {cpu})
    stage_trace = None
    if stage_trace_path is not None:
        from ..consumer.request_trace_v2 import BoundedRequestTraceWriterV2

        stage_trace = BoundedRequestTraceWriterV2(
            stage_trace_path,
            journal_id=journal_id,
            capacity=4096,
            max_bytes=stage_trace_budget,
            rotation_bytes=16 * 1024**2,
            record_schema="consumer-stage-trace-v1",
        )
    observer = stage_trace.submit if stage_trace is not None else None
    source = _RemoteSource(
        channel, journal_id, profile_hash, None if commit_only_diagnostics else observer
    )
    projector = IncrementalOrderProjector(
        journal=source,
        root=root,
        maximum_database_bytes=budget,
        maximum_transaction_batches=maximum_transaction_batches,
        stage_observer=observer,
        optimize_transactions=optimize_transactions,
        commit_only_diagnostics=commit_only_diagnostics,
        reuse_verified_admission_seeds=reuse_verified_admission_seeds,
        read_service_pause_ns=read_service_pause_ns,
        read_service_signal=read_service_signal,
    )
    try:
        projector.start()
        while True:
            health = projector.health()
            command = source.call("HEALTH", health)
            if not isinstance(command, tuple):
                raise OrderProjectionError("invalid child lifecycle command")
            flags = cast(tuple[object, ...], command)
            if len(flags) != 2 or any(type(flag) is not bool for flag in flags):
                raise OrderProjectionError("invalid child lifecycle command")
            stopping, paused = cast(tuple[bool, bool], flags)
            if stopping:
                break
            if paused:
                projector.pause()
            else:
                projector.resume()
            if health.state == "FAILED":
                raise OrderProjectionError("child projection failed")
            sleep(0.01)
    finally:
        try:
            projector.close()
            source.call("HEALTH", projector.health())
        finally:
            channel.close()
            if stage_trace is not None:
                stage_trace.close()
                if not stage_trace.coverage_verified:
                    raise OrderProjectionError("projection stage evidence incomplete")


class ProcessOrderProjector:
    """Same cached health/pause/drain surface, explicit opt-in candidate only."""

    def __init__(
        self,
        *,
        journal: SegmentedOmsFactJournal,
        root: Path,
        maximum_database_bytes: int,
        cpu: int | None = None,
        maximum_transaction_batches: int = 16,
        stage_trace_path: Path | None = None,
        stage_trace_budget: int = 1024**3,
        optimize_transactions: bool = False,
        commit_only_diagnostics: bool = False,
        reuse_verified_admission_seeds: bool = False,
        read_service_pause_ns: int = 0,
        read_service_signal: Any | None = None,
    ) -> None:
        if cpu is not None and (type(cpu) is not int or cpu < 0):
            raise OrderProjectionError("invalid projection owner CPU")
        if (
            type(maximum_transaction_batches) is not int
            or not 1 <= maximum_transaction_batches <= 256
            or type(optimize_transactions) is not bool
            or type(commit_only_diagnostics) is not bool
            or type(reuse_verified_admission_seeds) is not bool
            or type(read_service_pause_ns) is not int
            or not 0 <= read_service_pause_ns <= 10_000_000
            or (reuse_verified_admission_seeds and not optimize_transactions)
            or (commit_only_diagnostics and stage_trace_path is None)
        ):
            raise OrderProjectionError("invalid projection transaction batch budget")
        if type(stage_trace_budget) is not int or not 65536 <= stage_trace_budget <= 16 * 1024**3:
            raise OrderProjectionError("invalid projection stage trace budget")
        self._journal = journal
        context = mp.get_context("spawn")
        self._parent, child = context.Pipe()
        self._child_channel = child
        # Never share a semaphore-backed Event with a killable child: dying
        # while holding its internal lock could deadlock the parent forever.
        # The bounded request/reply channel carries these lifecycle flags.
        self._stop, self._pause = Event(), Event()
        self._broker_stop, self._ready, self._lock = Event(), Event(), Lock()
        self._health = IncrementalProjectionHealth("NEW", 0, 0, 0, monotonic_ns())
        self._started = self._closed = False
        self._process = context.Process(
            target=_child,
            args=(
                child,
                journal.journal_id,
                journal.profile_hash,
                root,
                maximum_database_bytes,
                cpu,
                maximum_transaction_batches,
                stage_trace_path,
                stage_trace_budget,
                optimize_transactions,
                commit_only_diagnostics,
                reuse_verified_admission_seeds,
                read_service_pause_ns,
                read_service_signal,
            ),
            name="consumer-projection-owner",
        )
        self._broker = Thread(target=self._serve, name="projection-control", daemon=True)

    def _fail(self) -> None:
        with self._lock:
            old = self._health
            self._health = IncrementalProjectionHealth(
                "FAILED",
                old.projected_lsn,
                old.observed_committed_lsn,
                old.completed_batches,
                monotonic_ns(),
                "PROJECTION_PROCESS_FAILED",
            )
        self._ready.set()

    def _serve(self) -> None:
        try:
            while not self._broker_stop.is_set():
                if not self._parent.poll(0.01):
                    if not self._process.is_alive():
                        if not self._stop.is_set() or self._process.exitcode != 0:
                            self._fail()
                        break
                    continue
                kind, value = cast(tuple[str, object], self._parent.recv())
                result: object
                if kind == "BOUND" and value is None:
                    result = self._journal.read_bound()
                elif kind == "PATH" and type(value) is int and value > 0:
                    result = str(self._journal.segment_read_path(value))
                elif kind == "HEALTH" and isinstance(value, IncrementalProjectionHealth):
                    with self._lock:
                        if self._health.state != "FAILED":
                            if value.projected_lsn < self._health.projected_lsn:
                                raise OrderProjectionError("child watermark regressed")
                            self._health = value
                    if value.state in {"RUNNING", "FAILED"}:
                        self._ready.set()
                    result = (self._stop.is_set(), self._pause.is_set())
                else:
                    raise OrderProjectionError("invalid child control operation")
                self._parent.send((True, result))
        except (EOFError, BrokenPipeError, OSError):
            if not self._stop.is_set():
                self._fail()
        except BaseException:
            self._fail()
            try:
                self._parent.send((False, None))
            except OSError:
                pass

    def start(self) -> None:
        if self._started or self._closed:
            raise OrderProjectionError("projection process cannot restart")
        self._started = True
        self._process.start()
        self._child_channel.close()
        self._broker.start()
        if not self._ready.wait(30) or self.health().state != "RUNNING":
            self.close()
            raise OrderProjectionError("projection process startup failed")

    def health(self) -> IncrementalProjectionHealth:
        with self._lock:
            health = self._health
        if (
            health.state in {"RUNNING", "PAUSED"}
            and monotonic_ns() - health.sampled_monotonic_ns > 2_000_000_000
        ):
            self._fail()
            with self._lock:
                health = self._health
        return health

    def pause(self) -> None:
        self._pause.set()

    def resume(self) -> None:
        self._pause.clear()

    def drain(self, *, final_lsn: int, timeout_seconds: float = 30) -> IncrementalProjectionHealth:
        """Bounded final source fence AFTER admission and every writer stopped.

        This is not a throughput/lag repair and never resumes a paused owner.
        A changing source, failed child, stale health or deadline invalidates
        final closure. The caller must still close and verify the child exit.
        """
        if (
            type(final_lsn) is not int
            or final_lsn < 0
            or type(timeout_seconds) not in (int, float)
            or not 0 < timeout_seconds <= 30
            or not self._started
        ):
            raise OrderProjectionError("invalid final projection drain")
        deadline = monotonic_ns() + int(timeout_seconds * 1_000_000_000)
        while True:
            if monotonic_ns() >= deadline:
                raise OrderProjectionError("final projection drain exceeded its deadline")
            if self._journal.read_bound().committed_lsn != final_lsn:
                raise OrderProjectionError("final projection source is not quiescent")
            health = self.health()
            expected_state = "STOPPED" if self._closed else "RUNNING"
            if (
                health.state != expected_state
                or health.error_code is not None
                or health.observed_committed_lsn > final_lsn
            ):
                raise OrderProjectionError("final projection owner is unavailable")
            if health.projected_lsn == health.observed_committed_lsn == final_lsn:
                return health
            if self._closed or health.projected_lsn > final_lsn:
                raise OrderProjectionError("final projection drain did not reach its fence")
            sleep(0.01)

    def close(self) -> None:
        if self._closed:
            return
        self._stop.set()
        forced = False
        if self._process.pid is not None:
            self._process.join(7)
            if self._process.is_alive():
                forced = True
                self._process.terminate()
                self._process.join(2)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(2)
        self._broker_stop.set()
        if self._broker.ident is not None:
            self._broker.join(2)
        self._parent.close()
        self._child_channel.close()
        self._closed = not self._process.is_alive() and not self._broker.is_alive()
        if forced or self._broker.is_alive():
            self._fail()
            raise OrderProjectionError("projection process required forced drain")

    @property
    def shutdown_verified(self) -> bool:
        health = self.health()
        return (
            self._closed
            and self._process.exitcode == 0
            and health.state == "STOPPED"
            and health.error_code is None
        )
