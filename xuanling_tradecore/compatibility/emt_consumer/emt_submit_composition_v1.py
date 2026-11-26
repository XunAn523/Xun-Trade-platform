"""Opt-in Consumer OMS to bounded EMT submit-worker composition.

This module supplies no broker credentials, risk data, recovery certificate,
or production startup. The caller must construct those separately and keep
the Consumer disarmed unless every required gate is genuinely satisfied.
"""

from __future__ import annotations

from threading import Event, Lock, Thread
from typing import Callable, Protocol

from ...adapters import BrokerEventObserver
from ...consumer.base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerOrderSignal,
    ConsumerSession,
)
from ...consumer.gateway_v2 import ConsumerGatewayConfigurationError
from ...consumer.oms_operations_v2 import (
    ConsumerOmsAdmissionContextV2,
    OrderBookConsumerOperationsV2,
)
from ...domain import ConsumerAccountBinding, ConsumerOrderEvent, RiskSnapshot
from ...ems import ExecutionSessionRuntime
from ...oms import OmsJournalCommit, OrderBook, OrderBookOrder
from .emt_submit_worker_v1 import EmtConsumerSubmitWorkerErrorV1, EmtConsumerSubmitWorkerV1


class ConsumerEmtRuntimeGatePortV1(Protocol):
    def admission_permitted(self) -> bool: ...
    def require_account(self, account_id: str) -> None: ...
    def risk_snapshot(self) -> RiskSnapshot: ...


class ManagedEmtExecutionSessionV1:
    """Manage the exact worker session without granting trading readiness.

    The runtime's supervisor is responsible for ownership and signed profile
    checks; the observer is responsible for the actual callback pipeline.
    Neither starting a session nor opening a socket is a recovery certificate.
    A failed or closed component cannot be restarted with consumed credentials.
    """

    def __init__(
        self,
        *,
        runtime: ExecutionSessionRuntime,
        observer: BrokerEventObserver,
        now_wall_ns: Callable[[], int],
    ) -> None:
        self._runtime = runtime
        self._observer = observer
        self._now_wall_ns = now_wall_ns
        self._lock = Lock()
        self._state = "new"

    @property
    def runtime(self) -> ExecutionSessionRuntime:
        return self._runtime

    def start(self) -> None:
        with self._lock:
            if self._state != "new":
                raise EmtConsumerSubmitWorkerErrorV1("EMT managed session cannot be restarted")
            self._state = "starting"
            try:
                self._runtime.start(self._observer, evaluated_wall_ns=self._now_wall_ns())
            except BaseException:
                self._state = "failed"
                raise
            self._state = "running"

    def close(self) -> None:
        with self._lock:
            if self._state in ("new", "closed"):
                self._state = "closed"
                return
            if self._state != "running":
                raise EmtConsumerSubmitWorkerErrorV1("EMT managed session state is uncertain")
            self._state = "closing"
            try:
                self._runtime.stop()
            except BaseException:
                self._state = "failed"
                raise
            self._state = "closed"


class ManagedEmtConsumerSubmitV1:
    """Single managed worker owner; failed/unclean shutdown needs reconciliation."""

    def __init__(self, worker: EmtConsumerSubmitWorkerV1, *, shutdown_timeout_seconds: float = 5.0):
        if not 0 < shutdown_timeout_seconds <= 60:
            raise ValueError("EMT worker shutdown timeout must be bounded")
        self._worker = worker
        self._timeout = shutdown_timeout_seconds
        self._wake = Event()
        self._stop = Event()
        self._lock = Lock()
        self._thread: Thread | None = None
        self._failed = False

    @property
    def worker(self) -> EmtConsumerSubmitWorkerV1:
        return self._worker

    @property
    def failed(self) -> bool:
        with self._lock:
            return self._failed

    def admission_permitted(self) -> bool:
        with self._lock:
            thread = self._thread
            running = thread is not None and thread.is_alive() and not self._failed
        return running and not self._stop.is_set() and self._worker.admission_permitted()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None or self._stop.is_set():
                raise EmtConsumerSubmitWorkerErrorV1("EMT managed worker cannot be restarted")
            self._thread = Thread(target=self._run, name="consumer-emt-submit-owner")
            self._thread.start()

    def on_execution_committed(self, order: OrderBookOrder, commit: OmsJournalCommit) -> None:
        if self._stop.is_set():
            raise EmtConsumerSubmitWorkerErrorV1("EMT managed worker is stopping")
        self._worker.on_execution_committed(order, commit)
        self._wake.set()

    def on_execution_wakeup_failed(self) -> None:
        self._stop.set()
        self._wake.set()
        self._worker.on_execution_wakeup_failed()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        try:
            self._worker.fence_for_shutdown()
        finally:
            with self._lock:
                thread = self._thread
            if thread is not None:
                thread.join(self._timeout)
                if thread.is_alive():
                    raise EmtConsumerSubmitWorkerErrorV1(
                        "EMT submit owner did not stop; do not release its session or Journal"
                    )

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                self._wake.wait()
                self._wake.clear()
                while not self._stop.is_set() and self._worker.run_one() is not None:
                    pass
        except BaseException:
            with self._lock:
                self._failed = True
            self._stop.set()
            try:
                self._worker.fence_for_shutdown()
            except Exception:
                # Keep the failed state visible; close() will retry/report an
                # unconfirmed fence rather than declaring a clean shutdown.
                pass


def build_consumer_emt_submit_operations_v1(
    *,
    order_book: OrderBook,
    managed_worker: ManagedEmtConsumerSubmitV1,
    admission_context_for: Callable[
        [ConsumerAccountBinding, str, ConsumerOrderSignal], ConsumerOmsAdmissionContextV2
    ],
    account_ready: Callable[[ConsumerAccountBinding], bool],
    snapshot_for: Callable[
        [ConsumerSession, ConsumerAccountBinding, str | None], ConsumerAccountOrderSnapshotV2
    ]
    | None = None,
    on_order_event_committed: Callable[[ConsumerOrderEvent], None] | None = None,
    runtime_gate: ConsumerEmtRuntimeGatePortV1 | None = None,
    now_monotonic_ns: Callable[[], int],
) -> OrderBookConsumerOperationsV2:
    """Wire the exact recovery-enabled OMS to the same worker's Journal.

    This submit-only composition explicitly rejects cancels. It does not
    install an EMT account binding, callback projection or recovery policy.
    """

    preparer = managed_worker.worker.preparer
    if (
        order_book.journal is not preparer.oms_journal
        or not order_book.persists_recovery_seed
        or (runtime_gate is not None and preparer.admission_authorizer != runtime_gate.admission_permitted)
    ):
        raise EmtConsumerSubmitWorkerErrorV1("Consumer OMS and EMT worker Journal do not match")

    def gated_context(
        binding: ConsumerAccountBinding, strategy_binding_id: str, signal: ConsumerOrderSignal
    ) -> ConsumerOmsAdmissionContextV2:
        try:
            allowed = (
                binding == preparer.consumer_binding
                and managed_worker.admission_permitted()
                and account_ready(binding) is True
                and (runtime_gate is None or runtime_gate.admission_permitted())
            )
        except Exception as exc:
            raise ConsumerGatewayConfigurationError("EMT submit admission check failed") from exc
        if not allowed:
            raise ConsumerGatewayConfigurationError("EMT submit admission is closed")
        context = admission_context_for(binding, strategy_binding_id, signal)
        if runtime_gate is not None and context.risk_snapshot != runtime_gate.risk_snapshot():
            raise ConsumerGatewayConfigurationError("EMT admission risk snapshot does not match runtime gate")
        return context

    return OrderBookConsumerOperationsV2(
        order_book=order_book,
        admission_context_for=gated_context,
        now_monotonic_ns=now_monotonic_ns,
        snapshot_for=snapshot_for,
        on_order_event_committed=on_order_event_committed,
        on_execution_commit_evidence=managed_worker.on_execution_committed,
        on_execution_wakeup_failed=managed_worker.on_execution_wakeup_failed,
        cancel_admission_allowed=lambda _order: False,
    )
