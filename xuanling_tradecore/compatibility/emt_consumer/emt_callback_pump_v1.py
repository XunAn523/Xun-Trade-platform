"""Managed off-SDK-thread callback drain for a single EMT session.

An orderly stop is not a broker replay certificate. In particular, callbacks
may race vendor logout after the pump stops; reopening requires reconciliation.
"""

from __future__ import annotations

from threading import Event, Lock, Thread

from ...ems import ExecutionSessionRuntime
from .emt_submit_composition_v1 import ManagedEmtConsumerSubmitV1


class EmtCallbackPumpErrorV1(RuntimeError):
    """The controlled callback drain is unavailable or cannot be stopped."""


class ManagedEmtCallbackPumpV1:
    def __init__(
        self,
        *,
        runtime: ExecutionSessionRuntime,
        failure_target: ManagedEmtConsumerSubmitV1,
        poll_seconds: float = 0.05,
        shutdown_timeout_seconds: float = 5.0,
    ) -> None:
        if (
            failure_target.worker.runtime is not runtime
            or not 0 < poll_seconds <= 0.25
            or not 0 < shutdown_timeout_seconds <= 60
        ):
            raise ValueError("EMT callback timing must be bounded")
        self._runtime = runtime
        self._failure_target = failure_target
        self._poll = poll_seconds
        self._timeout = shutdown_timeout_seconds
        self._stop = Event()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._failed = False

    @property
    def runtime(self) -> ExecutionSessionRuntime:
        return self._runtime

    @property
    def failure_target(self) -> ManagedEmtConsumerSubmitV1:
        return self._failure_target

    def healthy(self) -> bool:
        with self._lock:
            return (
                not self._failed
                and not self._stop.is_set()
                and self._thread is not None
                and self._thread.is_alive()
            )

    def start(self) -> None:
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise EmtCallbackPumpErrorV1("EMT callback pump cannot restart")
            self._thread = Thread(target=self._run, name="consumer-emt-callback-pump")
            self._thread.start()

    def close(self) -> None:
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None:
            thread.join(self._timeout)
            if thread.is_alive():
                raise EmtCallbackPumpErrorV1("EMT callback pump did not stop; retain session")

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                self._runtime.drain_broker_events(wait_timeout_seconds=self._poll)
        except BaseException:
            with self._lock:
                self._failed = True
            self._stop.set()
            try:
                self._failure_target.on_execution_wakeup_failed()
            except BaseException:
                # The failed health latch remains closed even when the
                # caller's additional fence cannot be confirmed.
                pass
