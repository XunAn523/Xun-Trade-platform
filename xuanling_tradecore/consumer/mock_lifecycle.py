"""Explicit no-Adapter qualification lifecycle, never a broker implementation.

One maintenance owner observes the bounded durable live book, not a lossy
post-commit notification queue. Every state change uses the real OMS reducer
and Journal. Retirement follows a validated, committed cold archive receipt.
"""

from __future__ import annotations

import os
import sys
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Event, Lock, Thread
from time import monotonic_ns, sleep, thread_time_ns

from ..domain import OmsOrderState
from ..oms import MmapOmsFactJournalFull
from ..oms.cold_intent import ColdArchiveReceipt
from ..oms.retained_order_book import RetainedOrderBook, TerminalArchiveCandidate
from ..persistence.cold_intent_index import ColdIntentIndex

MOCK_PRODUCER_MODEL = "NO_ADAPTER_ACCEPT_TERMINAL_TWO_FACTS"


class MockLifecycleError(RuntimeError):
    pass


def _lower_mock_thread_priority(increment: int) -> None:
    if not increment:
        return
    if sys.platform != "linux" or not hasattr(os, "setpriority"):
        raise MockLifecycleError("mock thread nice requires Linux")
    # Linux nice is per native thread; never increase priority and never
    # adjust another process or the owner submitter.
    old_nice = os.getpriority(os.PRIO_PROCESS, 0)
    target_nice = min(19, old_nice + increment)
    os.setpriority(os.PRIO_PROCESS, 0, target_nice)
    if os.getpriority(os.PRIO_PROCESS, 0) != target_nice:
        raise MockLifecycleError("mock thread nice was not applied")


class _TerminalDeadline(MockLifecycleError):
    def __init__(self, elapsed: int):
        super().__init__("mock terminal deadline exceeded")
        self.elapsed = elapsed


@dataclass(frozen=True)
class MockLifecycleHealth:
    state: str
    retired: int
    filled: int
    rejected: int
    max_terminal_ns: int
    sampled_ns: int
    error_code: str | None = None
    native_calls: int = 0
    pending_archives: int = 0
    peak_pending_archives: int = 0
    terminal_work_ns: int = 0
    retire_work_ns: int = 0
    archive_wait_ns: int = 0
    max_archive_wait_ns: int = 0


def mock_sequence(reference: str) -> int:
    """Frozen generator contract: LR-<nonnegative decimal sequence>."""
    if (
        not reference.startswith("LR-")
        or not reference[3:]
        or len(reference) > 24
        or any(c not in "0123456789" for c in reference[3:])
    ):
        raise MockLifecycleError("mock lifecycle requires an explicit LR sequence")
    return int(reference[3:])


class MockOrderLifecycle:
    def __init__(
        self,
        *,
        book: RetainedOrderBook,
        cold: ColdIntentIndex,
        batch_size: int = 32,
        interval_seconds: float = 0.001,
        terminal_deadline_ns: int = 1_000_000_000,
        maximum_pending_archives: int = 4,
        yield_every: int = 0,
        timing_diagnostics: bool = False,
        nice_increment: int = 0,
    ) -> None:
        if (
            type(batch_size) is not int
            or not 1 <= batch_size <= 128
            or type(interval_seconds) not in (float, int)
            or not 0.001 <= interval_seconds <= 1
            or type(terminal_deadline_ns) is not int
            or terminal_deadline_ns <= 0
            or type(maximum_pending_archives) is not int
            or not 1 <= maximum_pending_archives <= 8
            or type(yield_every) is not int
            or not 0 <= yield_every <= 128
            or type(timing_diagnostics) is not bool
            or type(nice_increment) is not int
            or not 0 <= nice_increment <= 5
        ):
            raise MockLifecycleError("mock lifecycle work budget is invalid")
        self._book, self._cold = book, cold
        self._batch_size, self._interval = batch_size, interval_seconds
        self._deadline = terminal_deadline_ns
        self._archive_limit = maximum_pending_archives
        self._yield_every = yield_every
        self._timing_diagnostics = timing_diagnostics
        self._nice_increment = nice_increment
        self._timing_intervals: deque[tuple[str, int, int, int]] = deque(maxlen=4096)
        self._timing_count = 0
        self._stop, self._lock = Event(), Lock()
        self._health = MockLifecycleHealth("NEW", 0, 0, 0, 0, monotonic_ns())
        self._thread = Thread(target=self._run, name="consumer-mock-lifecycle", daemon=True)
        self._started = self._closed = False

    def health(self) -> MockLifecycleHealth:
        with self._lock:
            return self._health

    def timing_evidence(self) -> dict[str, object] | None:
        """Post-stop bounded, identifier-free batch timing, never a hot-path fence."""
        if not self._timing_diagnostics or not self._closed or self._thread.is_alive():
            return None
        return {
            "schema": "mock-batch-timing-v1",
            "fields": ["kind", "start_ns", "finish_ns", "thread_cpu_ns"],
            "intervals": list(self._timing_intervals),
            "total": self._timing_count,
            "complete": self._timing_count == len(self._timing_intervals),
        }

    def _record_timing(self, kind: str, started: int, cpu_started: int) -> None:
        if self._timing_diagnostics:
            self._timing_intervals.append(
                (kind, started, monotonic_ns(), thread_time_ns() - cpu_started)
            )
            self._timing_count += 1

    def start(self) -> None:
        if self._started or self._closed:
            raise MockLifecycleError("mock lifecycle cannot restart")
        self._started = True
        self._thread.start()

    def _publish(self, **changes: object) -> None:
        from dataclasses import replace

        with self._lock:
            if self._health.state != "FAILED":
                self._health = replace(self._health, sampled_ns=monotonic_ns(), **changes)

    def _terminal(self, order_id: str) -> tuple[int, int, int]:
        order = self._book.order(order_id)
        state = order.order.state
        if state in {OmsOrderState.FILLED, OmsOrderState.REJECTED, OmsOrderState.CANCELED}:
            return 0, 0, 0
        if state is not OmsOrderState.ACCEPTED:
            # In particular SUBMIT_UNKNOWN is NEVER converted to a mock fill.
            raise MockLifecycleError("mock refuses unresolved or non-profile live state")
        sequence = mock_sequence(order.order.consumer_trace.strategy_order_id or "")
        reject = sequence % 5 == 4
        if reject:
            self._book.reject(oms_order_id=order_id)
        else:
            self._book.apply_trade(
                oms_order_id=order_id,
                trade_key=f"mock-fill-{sequence}",
                quantity=order.leaves_quantity,
            )
        elapsed = monotonic_ns() - order.decision_context.decided_monotonic_ns
        if elapsed < 0 or elapsed > self._deadline:
            raise _TerminalDeadline(elapsed)
        return int(not reject), int(reject), elapsed

    def _run(self) -> None:
        filled = rejected = retired = peak = 0
        pending: deque[
            tuple[tuple[TerminalArchiveCandidate, ...], Future[ColdArchiveReceipt], int]
        ] = deque()
        identities: set[str] = set()
        pending_peak = 0
        terminal_work = retire_work = archive_wait = max_archive_wait = 0
        try:
            _lower_mock_thread_priority(self._nice_increment)
            self._publish(state="RUNNING")
            while not self._stop.is_set() or pending:
                while pending and pending[0][1].done():
                    archived, future, enqueued = pending.popleft()
                    wait_ns = monotonic_ns() - enqueued
                    archive_wait += wait_ns
                    max_archive_wait = max(max_archive_wait, wait_ns)
                    retire_started = monotonic_ns()
                    retire_cpu = thread_time_ns() if self._timing_diagnostics else 0
                    receipt = future.result()
                    self._book.retire_terminals(archived, receipt)
                    for candidate in archived:
                        identities.remove(candidate.current.order.oms_order_id)
                    retired += len(archived)
                    retire_work += monotonic_ns() - retire_started
                    self._record_timing("retire", retire_started, retire_cpu)
                if pending and monotonic_ns() - pending[0][2] > 5_000_000_000:
                    raise MockLifecycleError("mock archive deadline exceeded")
                self._publish(
                    retired=retired,
                    filled=filled,
                    rejected=rejected,
                    max_terminal_ns=peak,
                    pending_archives=len(pending),
                    peak_pending_archives=pending_peak,
                    terminal_work_ns=terminal_work,
                    retire_work_ns=retire_work,
                    archive_wait_ns=archive_wait,
                    max_archive_wait_ns=max_archive_wait,
                )
                if self._stop.is_set() or len(pending) >= self._archive_limit:
                    sleep(self._interval)
                    continue
                ids = self._book.maintenance_order_ids(
                    limit=self._batch_size, exclude=frozenset(identities)
                )
                if not ids:
                    self._stop.wait(self._interval)
                    continue
                candidates: list[TerminalArchiveCandidate] = []
                terminal_started = monotonic_ns()
                terminal_cpu = thread_time_ns() if self._timing_diagnostics else 0
                for index, order_id in enumerate(ids, 1):
                    # Only pre-commit segment capacity may be retried. An
                    # unknown outcome or storage error poisons this owner.
                    while True:
                        try:
                            f, r, elapsed = self._terminal(order_id)
                            break
                        except MmapOmsFactJournalFull:
                            if self._stop.wait(self._interval):
                                raise MockLifecycleError(
                                    "mock stopped before capacity retry"
                                ) from None
                    filled += f
                    rejected += r
                    peak = max(peak, elapsed)
                    candidates.append(self._book.prepare_terminal_archive(order_id))
                    # Optional candidate-only fairness experiment; all terminal
                    # facts and archive receipts remain mandatory and ordered.
                    if self._yield_every and index % self._yield_every == 0:
                        sleep(0)
                immutable = tuple(candidates)
                terminal_work += monotonic_ns() - terminal_started
                self._record_timing("terminal", terminal_started, terminal_cpu)
                future = self._cold.archive(tuple(c.encode() for c in immutable))
                identities.update(c.current.order.oms_order_id for c in immutable)
                pending.append((immutable, future, monotonic_ns()))
                pending_peak = max(pending_peak, len(pending))
                # No wait for a batch to fill, no unbounded future list, and no
                # hot retirement until the exact durable receipt is available.
                self._publish(
                    retired=retired,
                    filled=filled,
                    rejected=rejected,
                    max_terminal_ns=peak,
                    pending_archives=len(pending),
                    peak_pending_archives=pending_peak,
                    terminal_work_ns=terminal_work,
                    retire_work_ns=retire_work,
                    archive_wait_ns=archive_wait,
                    max_archive_wait_ns=max_archive_wait,
                )
            self._publish(state="STOPPED")
        except _TerminalDeadline as error:
            self._publish(
                state="FAILED",
                error_code="MOCK_TERMINAL_DEADLINE",
                max_terminal_ns=max(peak, error.elapsed),
            )
        except BaseException:
            # No source exception, identity, private path or traceback retained.
            self._publish(state="FAILED", error_code="MOCK_LIFECYCLE_FAILED")

    def close(self) -> None:
        self._closed = True
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join(timeout=6)
            if self._thread.is_alive():
                self._publish(state="FAILED", error_code="MOCK_STOP_TIMEOUT")
                raise MockLifecycleError("mock lifecycle did not stop")
