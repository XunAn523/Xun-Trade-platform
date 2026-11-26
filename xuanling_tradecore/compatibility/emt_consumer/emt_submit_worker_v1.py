"""Bounded Consumer-to-EMS submit owner for an explicitly composed EMT lane.

This worker does not create a session, authorize startup, replay old orders,
or make a service READY. Its caller must provide a recovered, fenced runtime
and a durable disposition sink. It never automatically retries an execution.
"""

from __future__ import annotations

from collections import deque
from contextlib import AbstractContextManager
from dataclasses import replace
from threading import Lock
from typing import Callable, Protocol

from emt_gateway.tradecore_mmap_journal import EmtNativeRouteResolver

from ...consumer.execution_runtime_v1 import (
    ConsumerEmsExecutionBridgeV1,
    ConsumerExecutionDispatchRecordV1,
    ConsumerExecutionDispatchStateV1,
)
from ...domain import ExecutionInstruction
from ...ems import ExecutionSessionRuntime, GuardMarketSnapshot, KillSwitchSnapshot
from ...oms import OmsJournalCommit, OrderBookOrder
from .emt_submit_route_preparer_v1 import EmtConsumerSubmitRoutePreparerV1


class EmtConsumerSubmitWorkerErrorV1(RuntimeError):
    """The bounded worker cannot safely accept or dispatch more orders."""


class EmtNativeLaneV1(Protocol):
    @property
    def route_resolver(self) -> EmtNativeRouteResolver: ...

    def mmap_native_lane(self, lane_id: str) -> AbstractContextManager[None]: ...


class EmtConsumerSubmitWorkerV1:
    """One owner for committed admissions, route preparation and EMS dispatch.

    ``record_disposition`` must commit its record before returning. A missing
    wake-up, unclean restart, full history, dispatch failure or sink failure
    fences admission and requires reconciliation. In-memory history is not
    proof of broker recovery or sufficient for reopening this worker.
    """

    def __init__(
        self,
        *,
        preparer: EmtConsumerSubmitRoutePreparerV1,
        runtime: ExecutionSessionRuntime,
        native_journal: EmtNativeLaneV1,
        lane_id: str,
        market_for_execution: Callable[[ExecutionInstruction], GuardMarketSnapshot],
        kill_switches_for_account: Callable[[str], KillSwitchSnapshot],
        serialized_size_for_execution: Callable[[ExecutionInstruction], int],
        record_disposition: Callable[[ConsumerExecutionDispatchRecordV1], None],
        now_monotonic_ns: Callable[[], int],
        max_pending: int,
        max_seen: int,
        before_dispatch: Callable[[], None] | None = None,
    ) -> None:
        if (
            runtime.binding != preparer.execution_binding
            or native_journal.route_resolver is not preparer.resolver
            or not lane_id
            or type(max_pending) is not int
            or type(max_seen) is not int
            or not 1 <= max_pending <= max_seen <= 1_000_000
        ):
            raise EmtConsumerSubmitWorkerErrorV1("EMT submit worker composition is inconsistent")
        self._preparer = preparer
        self._runtime = runtime
        self._native_journal = native_journal
        self._lane_id = lane_id
        self._record_disposition = record_disposition
        self._max_pending = max_pending
        self._max_seen = max_seen
        self._pending: deque[tuple[OrderBookOrder, OmsJournalCommit]] = deque()
        self._seen: dict[str, tuple[OrderBookOrder, OmsJournalCommit]] = {}
        self._lock = Lock()
        self._owner_lock = Lock()
        self._failed = False
        self._fence_confirmed = False
        self._before_dispatch = before_dispatch
        self._bridge = ConsumerEmsExecutionBridgeV1(
            runtime_for_binding=lambda _: runtime,
            binding_for_order=lambda _: preparer.consumer_binding,
            market_for_execution=market_for_execution,
            kill_switches_for_account=kill_switches_for_account,
            serialized_size_for_execution=serialized_size_for_execution,
            serialized_size_for_cancel=lambda _: 1,
            now_monotonic_ns=now_monotonic_ns,
        )

    @property
    def preparer(self) -> EmtConsumerSubmitRoutePreparerV1:
        return self._preparer

    @property
    def runtime(self) -> ExecutionSessionRuntime:
        """Return the exact session owned by this worker's native lane."""

        return self._runtime

    @property
    def native_journal(self) -> EmtNativeLaneV1:
        return self._native_journal

    def admission_permitted(self) -> bool:
        """Fail closed before OMS admission when the local worker is unavailable."""

        with self._lock:
            available = (
                not self._failed
                and len(self._pending) < self._max_pending
                and len(self._seen) < self._max_seen
            )
        return available and self._preparer.admission_permitted()

    def on_execution_committed(self, order: OrderBookOrder, commit: OmsJournalCommit) -> None:
        """Post-commit wake-up only; this method never calls the SDK."""

        try:
            with self._lock:
                if self._failed:
                    raise EmtConsumerSubmitWorkerErrorV1("EMT submit worker is latched")
                execution_id = order.execution.execution_id
                existing = self._seen.get(execution_id)
                if existing is not None:
                    if existing != (order, commit):
                        raise EmtConsumerSubmitWorkerErrorV1(
                            "committed execution identity conflicts"
                        )
                    return
                if len(self._pending) >= self._max_pending or len(self._seen) >= self._max_seen:
                    raise EmtConsumerSubmitWorkerErrorV1("EMT submit worker capacity is exhausted")
                self._seen[execution_id] = (order, commit)
                self._pending.append((order, commit))
        except Exception as exc:
            self._fence("consumer-emt-submit-wakeup-failed")
            raise EmtConsumerSubmitWorkerErrorV1(
                "EMT submit worker requires reconciliation"
            ) from exc

    def on_execution_wakeup_failed(self) -> None:
        """Fence the runtime if the OMS cannot deliver a post-commit wake-up."""

        self._fence("consumer-emt-submit-wakeup-lost")

    def fence_for_shutdown(self) -> None:
        """Stop new admissions before the owner exits; never drain by guessing."""

        self._fence("consumer-emt-submit-shutdown")

    def run_one(self) -> ConsumerExecutionDispatchRecordV1 | None:
        """Run at most one queued execution on the sole EMS/native owner."""

        with self._owner_lock:
            with self._lock:
                if self._failed:
                    raise EmtConsumerSubmitWorkerErrorV1("EMT submit worker is latched")
                if not self._pending:
                    return None
                order, commit = self._pending.popleft()
            try:
                if self._before_dispatch is not None:
                    self._before_dispatch()
                route = self._preparer.prepare(order, commit)
                with self._lock:
                    if self._failed:
                        raise EmtConsumerSubmitWorkerErrorV1("EMT submit worker was fenced")
                if not self._bridge.enqueue_committed_execution(order):
                    raise EmtConsumerSubmitWorkerErrorV1("EMS execution was already registered")
                with self._native_journal.mmap_native_lane(self._lane_id):
                    record = self._bridge.dispatch_next()
                if record is None or record.execution_id != order.execution.execution_id:
                    raise EmtConsumerSubmitWorkerErrorV1("EMS returned another execution")
                if record.state is ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED:
                    identity = self._bridge.accepted_identity_for(order.execution.execution_id)
                    expected = route.venue_order_identity
                    if (
                        identity is None
                        or identity.venue_id != expected.venue_id
                        or identity.adapter_instance_id != expected.adapter_instance_id
                        or identity.adapter_client_scope != expected.adapter_client_scope
                        or identity.client_order_ref != expected.client_order_ref
                        or not identity.venue_order_id
                    ):
                        self._record_disposition(
                            replace(
                                record,
                                state=ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION,
                                result_code="NATIVE_ACCEPTANCE_IDENTITY_MISMATCH",
                            )
                        )
                        raise EmtConsumerSubmitWorkerErrorV1(
                            "native acceptance identity is unproven"
                        )
                # Native success has its own PRE_NATIVE and outcome Journal;
                # this sink makes the worker disposition explicit as well.
                self._record_disposition(record)
                if record.state is not ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED:
                    raise EmtConsumerSubmitWorkerErrorV1("EMS did not confirm native submission")
                return record
            except Exception as exc:
                self._fence("consumer-emt-submit-dispatch-failed")
                raise EmtConsumerSubmitWorkerErrorV1(
                    "EMT submit worker requires reconciliation"
                ) from exc

    def _fence(self, reason: str) -> None:
        with self._lock:
            already_failed = self._failed
            self._failed = True
            confirmed = self._fence_confirmed
        if already_failed:
            if not confirmed:
                raise EmtConsumerSubmitWorkerErrorV1("EMT admission closure is unconfirmed")
            return
        if not already_failed:
            try:
                self._runtime.close_admission(reason=reason)
            except Exception as exc:
                raise EmtConsumerSubmitWorkerErrorV1(
                    "EMT submit failure and admission closure is unconfirmed"
                ) from exc
            with self._lock:
                self._fence_confirmed = True
