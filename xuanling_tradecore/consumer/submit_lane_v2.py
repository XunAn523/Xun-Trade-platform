"""Bounded single-owner WRITE execution, not a GIL/process isolation claim.

No client socket is passed to the owner. A ticket can prevent queued work from
starting; once started, disconnect/timeout never means rollback or permission to
retry. Capacity includes queued, running AND uncollected completions.
"""

from __future__ import annotations

import socket
from contextvars import Context, copy_context
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import monotonic_ns
from typing import Callable, cast

from .cold_admission_v2 import ColdAdmissionRequestV2, ConsumerColdLookupRequiredV2


class SubmissionPrecheckFailedV2(Exception):
    """Only the listener's schema precheck may raise this BEFORE its handler."""


class SubmissionTicketV2:
    def __init__(self) -> None:
        self._lock = Lock()
        self._phase = "QUEUED"

    def cancel_before_start(self) -> bool:
        with self._lock:
            if self._phase != "QUEUED":
                return False
            self._phase = "CANCELED"
            return True

    def begin(self, *, stopped: bool, deadline_ns: int) -> bool:
        with self._lock:
            if self._phase != "QUEUED":
                return False
            if stopped or monotonic_ns() >= deadline_ns:
                self._phase = "CANCELED"
                return False
            self._phase = "RUNNING"
            return True


@dataclass(frozen=True)
class SubmissionResultV2:
    token: int
    response: bytes | None
    cold: ColdAdmissionRequestV2 | None
    outcome_unknown: bool
    started: bool
    trace: dict[str, object] | None
    precheck_failed: bool = False


@dataclass(frozen=True)
class _Submission:
    token: int
    ticket: SubmissionTicketV2
    deadline_ns: int
    run: Callable[[], bytes]
    context: Context
    trace: dict[str, object] | None


class BoundedSubmitLaneV2:
    def __init__(self, *, capacity: int, maximum_response_bytes: int, stopped: Event) -> None:
        if (
            type(capacity) is not int
            or not 1 <= capacity <= 128
            or type(maximum_response_bytes) is not int
            or maximum_response_bytes < 1
        ):
            raise ValueError("invalid submit execution limits")
        self._capacity, self._maximum_response_bytes = capacity, maximum_response_bytes
        self._stopped, self._closed = stopped, Event()
        self._notifier_failed = Event()
        self._outcome_unknown = Event()
        self._lock = Lock()
        self._tokens: set[int] = set()
        self._started = False
        self._jobs: Queue[_Submission] = Queue(capacity)
        self._done: Queue[SubmissionResultV2] = Queue(capacity)
        self.reader, self._writer = socket.socketpair()
        self.reader.setblocking(False)
        self._writer.setblocking(False)
        self._thread = Thread(target=self._work, name="consumer-submit-owner", daemon=True)

    @property
    def outstanding(self) -> int:
        with self._lock:
            return len(self._tokens)

    @property
    def full(self) -> bool:
        return self.outstanding >= self._capacity

    @property
    def failed(self) -> bool:
        return self._outcome_unknown.is_set() or self._notifier_failed.is_set()

    @property
    def has_completed(self) -> bool:
        return not self._done.empty()

    def start(self) -> None:
        with self._lock:
            if self._started or self._closed.is_set():
                raise RuntimeError("submit owner cannot restart")
            self._started = True
        self._thread.start()

    def offer(
        self,
        token: int,
        run: Callable[[], bytes],
        *,
        deadline_ns: int,
        trace: dict[str, object] | None = None,
    ) -> SubmissionTicketV2 | None:
        if type(token) is not int or token < 1 or type(deadline_ns) is not int or deadline_ns < 1:
            raise ValueError("invalid submission token/deadline")
        with self._lock:
            if not self._started or self._closed.is_set() or self._stopped.is_set():
                return None
            if token in self._tokens:
                raise ValueError("duplicate submission token")
            if len(self._tokens) >= self._capacity:
                return None
            ticket = SubmissionTicketV2()
            self._tokens.add(token)
            self._jobs.put_nowait(
                _Submission(token, ticket, deadline_ns, run, copy_context(), trace)
            )
            return ticket

    def take(self) -> SubmissionResultV2 | None:
        try:
            result = self._done.get_nowait()
        except Empty:
            return None
        with self._lock:
            self._tokens.remove(result.token)
        return result

    def clear_notification(self) -> None:
        try:
            while self.reader.recv(4096):
                pass
        except BlockingIOError:
            pass
        except OSError:
            if not self._closed.is_set():
                raise

    def close(self, *, timeout: float = 5.0) -> None:
        if type(timeout) not in (int, float) or not 0 < timeout <= 60:
            raise ValueError("submit owner drain requires a bounded timeout")
        self._closed.set()
        if self._started:
            self._thread.join(timeout)
            if self._thread.is_alive():
                raise RuntimeError("submit execution drain timed out")
        self.reader.close()
        self._writer.close()
        if self._outcome_unknown.is_set():
            raise RuntimeError("submit execution outcome unknown")
        if self._notifier_failed.is_set():
            raise RuntimeError("submit execution notification failed")

    def _work(self) -> None:
        while True:
            try:
                task = self._jobs.get(timeout=0.05)
            except Empty:
                if self._closed.is_set():
                    return
                continue
            started = task.ticket.begin(
                stopped=self._closed.is_set() or self._stopped.is_set(),
                deadline_ns=task.deadline_ns,
            )
            response = None
            cold = None
            unknown = False
            precheck_failed = False
            observed_ns = monotonic_ns()
            if task.trace is not None:
                task.trace.setdefault(
                    "submit_owner_started_ns" if started else "submit_not_started_ns", observed_ns
                )
                task.trace["submit_last_owner_started_ns"] = observed_ns
                task.trace["submit_execution_started"] = started
            if started:
                try:
                    response = task.context.run(task.run)
                    if type(response) is not bytes or len(response) > self._maximum_response_bytes:
                        raise ValueError("invalid submit response")
                except ConsumerColdLookupRequiredV2 as required:
                    # Copy ONLY the pre-commit lookup port/key, never a raised
                    # exception/traceback with references to request/owner state.
                    cold = ColdAdmissionRequestV2(required.key, required.resolver)
                except SubmissionPrecheckFailedV2:
                    precheck_failed = True
                except BaseException:
                    # The fact may already be committed. No fabricated retryable
                    # negative receipt, automatic resubmission or traceback leak.
                    response = None
                    unknown = True
                    self._outcome_unknown.set()
                    self._closed.set()  # queued/new writes must not proceed on an uncertain owner
            if task.trace is not None:
                finished_ns = monotonic_ns()
                task.trace["submit_owner_finished_ns"] = finished_ns
                task.trace["submit_precheck_failed"] = precheck_failed
                timings = task.trace.get("submit_attempt_timings", [])
                if isinstance(timings, list) and len(cast(list[object], timings)) < 4:
                    # New bounded list: never mutate the reactor's shallow copy.
                    task.trace["submit_attempt_timings"] = [
                        *timings,
                        {
                            "enqueued_ns": task.trace.get("submit_last_enqueued_ns"),
                            "owner_observed_ns": observed_ns,
                            "owner_finished_ns": finished_ns,
                            "started": started,
                        },
                    ]
                else:
                    task.trace["submit_timing_coverage_failed"] = True
            self._done.put_nowait(
                SubmissionResultV2(
                    task.token, response, cold, unknown, started, task.trace, precheck_failed
                )
            )
            self._jobs.task_done()
            # Drop closures/context before idle; they may retain a large runtime.
            del task
            del response, cold
            try:
                self._writer.send(b"s")
            except BlockingIOError:
                pass  # an existing byte already makes the selector readable
            except OSError:
                self._notifier_failed.set()
                self._closed.set()
