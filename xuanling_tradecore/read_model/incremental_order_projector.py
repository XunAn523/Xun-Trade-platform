"""Opt-in side-lane runtime for the bounded Journal tail and read index.

No synchronous publish hook is added to the writer. Pause/failure is read-model
lag, never authorization to dispatch, delete facts or grant trading READY.
"""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic_ns, sleep
from typing import Any, Callable

from ..oms.journal_tail import JournalTailSource, SegmentedJournalTail
from ..persistence.order_projection_store import OrderProjectionError, OrderProjectionStore
from ..persistence.stage_timing import StageTiming
from .projection_groups import VerifiedProjectionGroups


@dataclass(frozen=True)
class IncrementalProjectionHealth:
    state: str
    projected_lsn: int
    observed_committed_lsn: int
    completed_batches: int
    sampled_monotonic_ns: int
    error_code: str | None = None

    @property
    def lag_facts(self) -> int:
        return max(0, self.observed_committed_lsn - self.projected_lsn)


class IncrementalOrderProjector:
    def __init__(
        self,
        *,
        journal: JournalTailSource,
        root: Path,
        initialize: bool = False,
        poll_interval_seconds: float = 0.01,
        maximum_database_bytes: int = 8 * 1024**3,
        maximum_order_events: int = 64,
        maximum_batch_facts: int = 128,
        maximum_transaction_batches: int = 16,
        stage_observer: Callable[[dict[str, object]], None] | None = None,
        optimize_transactions: bool = False,
        commit_only_diagnostics: bool = False,
        reuse_verified_admission_seeds: bool = False,
        read_service_pause_ns: int = 0,
        read_service_signal: Any | None = None,
    ) -> None:
        if (
            type(poll_interval_seconds) not in (int, float)
            or not 0.001 <= poll_interval_seconds <= 1
        ):
            raise ValueError("invalid projection poll interval")
        if (
            type(maximum_transaction_batches) is not int
            or not 1 <= maximum_transaction_batches <= 256
            or type(optimize_transactions) is not bool
            or type(commit_only_diagnostics) is not bool
            or type(reuse_verified_admission_seeds) is not bool
            or type(read_service_pause_ns) is not int
            or not 0 <= read_service_pause_ns <= 10_000_000
            or (reuse_verified_admission_seeds and not optimize_transactions)
            or (commit_only_diagnostics and stage_observer is None)
        ):
            raise ValueError("invalid projection transaction batch budget")
        self._journal, self._root, self._initialize = journal, root, initialize
        self._interval, self._db_budget = poll_interval_seconds, maximum_database_bytes
        self._order_events, self._batch_facts = maximum_order_events, maximum_batch_facts
        self._transaction_batches = maximum_transaction_batches
        self._stage_observer = stage_observer
        self._optimize_transactions = optimize_transactions
        self._commit_only_diagnostics = commit_only_diagnostics
        self._reuse_verified_admission_seeds = reuse_verified_admission_seeds
        self._read_service_pause_seconds = read_service_pause_ns / 1_000_000_000
        self._read_service_signal = read_service_signal
        self._stop, self._pause = Event(), Event()
        self._lock = Lock()
        self._health = IncrementalProjectionHealth("NEW", 0, 0, 0, monotonic_ns())
        self._ready: Future[None] = Future()
        self._started = self._closed = False
        self._thread = Thread(target=self._run, name="oms-incremental-projector", daemon=True)

    def start(self) -> None:
        if self._started or self._closed:
            raise OrderProjectionError("incremental projector cannot restart")
        self._started = True
        self._thread.start()
        try:
            self._ready.result(timeout=30)
        except BaseException:
            self.close()
            raise

    def health(self) -> IncrementalProjectionHealth:
        """Cached only: no disk, SQL or writer-lock acquisition by the caller."""
        with self._lock:
            return self._health

    def pause(self) -> None:
        self._pause.set()

    def resume(self) -> None:
        self._pause.clear()

    def _publish(self, state: str, position: int, batches: int, error: str | None = None) -> None:
        observed = self._journal.read_bound().committed_lsn
        with self._lock:
            if self._health.state == "FAILED":
                return
            self._health = IncrementalProjectionHealth(
                state, position, observed, batches, monotonic_ns(), error
            )

    def _run(self) -> None:
        store = tail = None
        position = batches = 0
        try:
            store = OrderProjectionStore(
                self._root,
                journal_id=self._journal.journal_id,
                profile_hash=self._journal.profile_hash,
                initialize=self._initialize,
                maximum_database_bytes=self._db_budget,
                maximum_order_events=self._order_events,
                stage_observer=self._stage_observer,
                optimize_transactions=self._optimize_transactions,
                commit_only_diagnostics=self._commit_only_diagnostics,
                reuse_verified_admission_seeds=self._reuse_verified_admission_seeds,
            )
            position = store.cursor.next_lsn - 1
            tail = SegmentedJournalTail(
                self._journal,
                cursor=store.cursor,
                maximum_batch_facts=self._batch_facts,
                reuse_segment_views=self._optimize_transactions,
            )
            # Prove the physical resume anchor before publishing startup success.
            batch = tail.read_next()
            if batch is not None:
                store.apply(batch)
                position = batch.after.next_lsn - 1
                batches += 1
            self._publish("RUNNING", position, batches)
            self._ready.set_result(None)
            groups = VerifiedProjectionGroups(
                tail,
                self._transaction_batches,
                self._stop.is_set,
                freeze_source_bound=self._optimize_transactions,
            )
            while not self._stop.is_set():
                if self._pause.is_set():
                    self._publish("PAUSED", position, batches)
                    self._stop.wait(self._interval)
                    continue
                stage = (
                    StageTiming("projector", self._stage_observer)
                    if self._stage_observer is not None and not self._commit_only_diagnostics
                    else None
                )
                group = (
                    groups.read_next()
                    if stage is None
                    else stage.call("tail_validation", groups.read_next)
                )
                if not group:
                    if stage is not None:
                        stage.finish({"verified_batches": 0, "verified_facts": 0})
                    self._publish("RUNNING", position, batches)
                    self._stop.wait(self._interval)
                    continue
                facts = sum(len(batch.records) for batch in group)
                if stage is not None:
                    stage.finish(
                        {
                            "verified_batches": len(group),
                            "verified_facts": facts,
                            "first_lsn": group[0].before.next_lsn,
                            "final_lsn": group[-1].after.next_lsn - 1,
                        }
                    )
                store.apply_many(group)
                position = group[-1].after.next_lsn - 1
                batches += len(group)
                self._publish("RUNNING", position, batches)
                if self._read_service_pause_seconds and (
                    self._read_service_signal is None or self._read_service_signal.value
                ):
                    # DELETE/FULL releases the derived projection lock at commit.
                    # Leave a fixed, opt-in acquisition window before the writer
                    # starts its next transaction so bounded readonly peers cannot
                    # be starved by a continuously ready projector.
                    sleep(self._read_service_pause_seconds)
            self._publish("STOPPED", position, batches)
        except BaseException:
            # Do not retain private storage exceptions or their stack frames.
            self._fail(position, batches, "PROJECTION_FAILED")
            if not self._ready.done():
                self._ready.set_exception(OrderProjectionError("incremental projection failed"))
        finally:
            for resource in (tail, store):
                if resource is not None:
                    try:
                        resource.close()
                    except BaseException:
                        self._fail(position, batches, "PROJECTION_CLOSE_FAILED")

    def _fail(self, position: int, batches: int, code: str) -> None:
        with self._lock:
            if self._health.state != "FAILED":
                self._health = IncrementalProjectionHealth(
                    "FAILED",
                    position,
                    self._health.observed_committed_lsn,
                    batches,
                    monotonic_ns(),
                    code,
                )

    def close(self) -> None:
        self._closed = True
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                previous = self.health()
                self._fail(
                    previous.projected_lsn, previous.completed_batches, "PROJECTION_STOP_TIMEOUT"
                )
                raise OrderProjectionError("incremental projection drain timed out")
