"""Journal-first Consumer-to-EMS execution bridge.

Consumer admission only writes OMS facts.  This bridge is the separate worker
side which receives a post-commit wake-up (or a Journal replay), resolves the
already-authorized binding to one exact EMS runtime, and asks that runtime to
perform at most one native dispatch.  It never owns credentials, never looks
up the control plane, and never retries an uncertain native invocation.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from time import monotonic_ns
from types import MappingProxyType
from typing import Callable, Mapping

from ..adapters import CancelExecutionInstruction, SubmitAccepted
from ..domain import CancelRequest, ConsumerAccountBinding, ExecutionInstruction, VenueOrderIdentity
from ..domain._validation import require_nonnegative, require_positive, require_text
from ..ems import (
    ExecutionDispatchOutcome,
    ExecutionSessionRuntime,
    GuardMarketSnapshot,
    KillSwitchSnapshot,
)
from ..oms import OrderBookOrder


class ConsumerExecutionRuntimeErrorV1(RuntimeError):
    """A committed Consumer command cannot be safely dispatched."""


class ConsumerExecutionDispatchStateV1(str, Enum):
    """Persistable-equivalent local disposition; no state asserts venue terminality."""

    QUEUED = "QUEUED"
    GUARD_REJECTED = "GUARD_REJECTED"
    SUBMIT_ACCEPTED = "SUBMIT_ACCEPTED"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"
    EXPIRED_PRE_NATIVE = "EXPIRED_PRE_NATIVE"
    BLOCKED_DISARMED = "BLOCKED_DISARMED"
    UNKNOWN_REQUIRES_RECONCILIATION = "UNKNOWN_REQUIRES_RECONCILIATION"


@dataclass(frozen=True)
class ConsumerExecutionDispatchRecordV1:
    """Payload-free dispatch evidence for one execution or cancellation request."""

    command_id: str
    execution_id: str
    binding_id: str
    binding_generation: int
    state: ConsumerExecutionDispatchStateV1
    result_code: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.command_id, "consumer dispatch command ID"),
            (self.execution_id, "consumer dispatch execution ID"),
            (self.binding_id, "consumer dispatch binding ID"),
            (self.result_code, "consumer dispatch result code"),
        ):
            require_text(value, label, max_length=256)
        require_positive(self.binding_generation, "consumer dispatch binding generation")


@dataclass(frozen=True)
class ConsumerExecutionRuntimeSnapshotV1:
    """Bounded bridge health used by Consumer/EMS readiness projection."""

    pending_new_orders: int
    pending_cancels: int
    records: Mapping[str, ConsumerExecutionDispatchRecordV1]

    def __post_init__(self) -> None:
        require_nonnegative(self.pending_new_orders, "consumer bridge pending new orders")
        require_nonnegative(self.pending_cancels, "consumer bridge pending cancels")
        object.__setattr__(self, "records", MappingProxyType(dict(self.records)))


@dataclass(frozen=True)
class _QueuedNewOrderV1:
    binding: ConsumerAccountBinding
    order: OrderBookOrder


@dataclass(frozen=True)
class _QueuedCancelV1:
    binding: ConsumerAccountBinding
    order: OrderBookOrder
    cancel_request: CancelRequest
    venue_order_identity: VenueOrderIdentity


class ConsumerEmsExecutionBridgeV1:
    """One worker-owned bridge from committed Consumer facts to EMS runtimes.

    New orders and cancels are intentionally not dispatched from Consumer's
    UDS request thread.  A call to :meth:`dispatch_next` must be owned by one
    EMS worker.  The short worker lock protects against accidental concurrent
    callers; it is not part of Consumer admission or the SDK hot lane.
    """

    def __init__(
        self,
        *,
        runtime_for_binding: Callable[[ConsumerAccountBinding], ExecutionSessionRuntime],
        binding_for_order: Callable[[OrderBookOrder], ConsumerAccountBinding],
        market_for_execution: Callable[[ExecutionInstruction], GuardMarketSnapshot],
        kill_switches_for_account: Callable[[str], KillSwitchSnapshot],
        serialized_size_for_execution: Callable[[ExecutionInstruction], int],
        serialized_size_for_cancel: Callable[[CancelRequest], int],
        now_monotonic_ns: Callable[[], int] = monotonic_ns,
    ) -> None:
        self._runtime_for_binding = runtime_for_binding
        self._binding_for_order = binding_for_order
        self._market_for_execution = market_for_execution
        self._kill_switches_for_account = kill_switches_for_account
        self._serialized_size_for_execution = serialized_size_for_execution
        self._serialized_size_for_cancel = serialized_size_for_cancel
        self._now_monotonic_ns = now_monotonic_ns
        self._new_orders: deque[_QueuedNewOrderV1] = deque()
        self._cancels: deque[_QueuedCancelV1] = deque()
        self._records: dict[str, ConsumerExecutionDispatchRecordV1] = {}
        self._bindings_by_command: dict[str, ConsumerAccountBinding] = {}
        self._venue_identities: dict[str, VenueOrderIdentity] = {}
        self._state_lock = Lock()
        self._worker_lock = Lock()

    def enqueue_committed_execution(self, order: OrderBookOrder) -> bool:
        """Register one committed execution exactly once without Adapter I/O.

        A duplicate wake-up is expected after recovery and is ignored.  A
        restart must call this method again for every replayed unterminal
        Journal fact; this in-memory queue is deliberately not recovery
        authority.
        """

        binding = self._require_order_binding(self._binding_for_order(order), order)
        execution_id = order.execution.execution_id
        with self._state_lock:
            if execution_id in self._records:
                return False
            self._records[execution_id] = self._record(
                command_id=execution_id,
                execution_id=execution_id,
                binding=binding,
                state=ConsumerExecutionDispatchStateV1.QUEUED,
                result_code="OMS_FACT_COMMITTED",
            )
            self._bindings_by_command[execution_id] = binding
            self._new_orders.append(_QueuedNewOrderV1(binding=binding, order=order))
            return True

    def can_request_cancel(self, order: OrderBookOrder) -> bool:
        """Return whether this exact execution has a proven native identity."""

        binding = self._require_order_binding(self._binding_for_order(order), order)
        with self._state_lock:
            identity = self._venue_identities.get(order.execution.execution_id)
        return identity is not None and self._identity_matches_binding(identity, binding)

    def accepted_identity_for(self, execution_id: str) -> VenueOrderIdentity | None:
        """Return the accepted identity for an exact locally dispatched execution."""

        with self._state_lock:
            return self._venue_identities.get(execution_id)

    def enqueue_committed_cancel(
        self,
        order: OrderBookOrder,
        *,
        cancel_request_id: str,
        reason: str,
    ) -> bool:
        """Queue an already-committed cancel only when its native identity is proven."""

        binding = self._require_order_binding(self._binding_for_order(order), order)
        canonical_request_id = require_text(
            cancel_request_id, "consumer cancel request ID", max_length=128
        )
        canonical_reason = require_text(reason, "consumer cancel reason", max_length=256)
        with self._state_lock:
            if canonical_request_id in self._records:
                return False
            identity = self._venue_identities.get(order.execution.execution_id)
            if identity is None or not self._identity_matches_binding(identity, binding):
                raise ConsumerExecutionRuntimeErrorV1(
                    "consumer cancel lacks a proven owned venue order identity"
                )
            request = CancelRequest(
                cancel_request_id=canonical_request_id,
                oms_order_id=order.order.oms_order_id,
                execution_id=order.execution.execution_id,
                account_id=order.order.account_id,
                consumer_trace=order.order.consumer_trace,
                reason=canonical_reason,
                requested_monotonic_ns=self._now_monotonic_ns(),
            )
            self._records[canonical_request_id] = self._record(
                command_id=canonical_request_id,
                execution_id=order.execution.execution_id,
                binding=binding,
                state=ConsumerExecutionDispatchStateV1.QUEUED,
                result_code="OMS_CANCEL_FACT_COMMITTED",
            )
            self._bindings_by_command[canonical_request_id] = binding
            self._cancels.append(
                _QueuedCancelV1(
                    binding=binding,
                    order=order,
                    cancel_request=request,
                    venue_order_identity=identity,
                )
            )
            return True

    def dispatch_next(self) -> ConsumerExecutionDispatchRecordV1 | None:
        """Dispatch one queued command; an exception after admission is never retried."""

        with self._worker_lock:
            queued_cancel = self._pop_cancel()
            if queued_cancel is not None:
                return self._dispatch_cancel(queued_cancel)
            queued_new = self._pop_new_order()
            if queued_new is not None:
                return self._dispatch_new_order(queued_new)
            return None

    def release_after_terminal(self, *, execution_id: str) -> bool:
        """Release the native in-flight credit only after a durable terminal fact."""

        with self._state_lock:
            record = self._records.get(execution_id)
            if (
                record is None
                or record.state is not ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED
            ):
                return False
            binding = self._bindings_by_command.get(execution_id)
            if binding is None:
                raise ConsumerExecutionRuntimeErrorV1(
                    "Consumer bridge lost the execution binding evidence"
                )
        runtime = self._validated_runtime(binding)
        return runtime.release_new_order_inflight_after_terminal(execution_id)

    def snapshot(self) -> ConsumerExecutionRuntimeSnapshotV1:
        with self._state_lock:
            return ConsumerExecutionRuntimeSnapshotV1(
                pending_new_orders=len(self._new_orders),
                pending_cancels=len(self._cancels),
                records=dict(self._records),
            )

    def _dispatch_new_order(self, queued: _QueuedNewOrderV1) -> ConsumerExecutionDispatchRecordV1:
        instruction = queued.order.execution
        try:
            runtime = self._validated_runtime(queued.binding)
        except ConsumerExecutionRuntimeErrorV1:
            return self._replace_record(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION,
                result_code="BINDING_RUNTIME_MISMATCH",
            )
        try:
            admitted = runtime.admit_submit(
                instruction,
                serialized_size_bytes=self._require_size(
                    self._serialized_size_for_execution(instruction), "new order"
                ),
                market=self._market_for_execution(instruction),
                kill_switches=self._kill_switches_for_account(instruction.account_id),
                admitted_monotonic_ns=self._now_monotonic_ns(),
            )
        except Exception:
            return self._unknown(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="EMS_ADMISSION_EXCEPTION",
            )
        if not admitted.decision.accepted or admitted.authorized_result is None:
            return self._replace_record(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.GUARD_REJECTED,
                result_code=admitted.decision.code.value,
            )
        try:
            dispatched = runtime.dispatch_once(checked_monotonic_ns=self._now_monotonic_ns())
        except Exception:
            return self._unknown(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="NATIVE_DISPATCH_EXCEPTION",
            )
        if dispatched is None or dispatched.scheduled.command != instruction:
            return self._unknown(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="NATIVE_DISPATCH_OWNERSHIP_MISMATCH",
            )
        if dispatched.outcome is ExecutionDispatchOutcome.SUBMIT_ACCEPTED:
            accepted = dispatched.submit_accepted
            if accepted is None or accepted.execution_id != instruction.execution_id:
                return self._unknown(
                    command_id=instruction.execution_id,
                    execution_id=instruction.execution_id,
                    binding=queued.binding,
                    runtime=runtime,
                    result_code="NATIVE_ACCEPTANCE_MISMATCH",
                )
            try:
                self._remember_identity(accepted, queued.binding)
            except ConsumerExecutionRuntimeErrorV1:
                return self._unknown(
                    command_id=instruction.execution_id,
                    execution_id=instruction.execution_id,
                    binding=queued.binding,
                    runtime=runtime,
                    result_code="NATIVE_IDENTITY_OUTSIDE_BINDING",
                )
            return self._replace_record(
                command_id=instruction.execution_id,
                execution_id=instruction.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED,
                result_code="NATIVE_SUBMIT_ACCEPTED",
            )
        return self._from_non_native_outcome(
            command_id=instruction.execution_id,
            execution_id=instruction.execution_id,
            binding=queued.binding,
            runtime=runtime,
            outcome=dispatched.outcome,
        )

    def _dispatch_cancel(self, queued: _QueuedCancelV1) -> ConsumerExecutionDispatchRecordV1:
        request = queued.cancel_request
        try:
            runtime = self._validated_runtime(queued.binding)
        except ConsumerExecutionRuntimeErrorV1:
            return self._replace_record(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION,
                result_code="BINDING_RUNTIME_MISMATCH",
            )
        try:
            admitted = runtime.admit_cancel(
                CancelExecutionInstruction(request, queued.venue_order_identity),
                serialized_size_bytes=self._require_size(
                    self._serialized_size_for_cancel(request), "cancel"
                ),
                admitted_monotonic_ns=self._now_monotonic_ns(),
            )
        except Exception:
            return self._unknown(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="EMS_CANCEL_ADMISSION_EXCEPTION",
            )
        if not getattr(admitted, "accepted", False):
            code = getattr(getattr(admitted, "code", None), "value", "INGRESS_REJECTED")
            return self._replace_record(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.GUARD_REJECTED,
                result_code=code,
            )
        try:
            dispatched = runtime.dispatch_once(checked_monotonic_ns=self._now_monotonic_ns())
        except Exception:
            return self._unknown(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="NATIVE_CANCEL_EXCEPTION",
            )
        if dispatched is None or dispatched.scheduled.command != request:
            return self._unknown(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                runtime=runtime,
                result_code="NATIVE_CANCEL_OWNERSHIP_MISMATCH",
            )
        if dispatched.outcome is ExecutionDispatchOutcome.CANCEL_ACCEPTED:
            accepted = dispatched.cancel_accepted
            if accepted is None or accepted.cancel_request_id != request.cancel_request_id:
                return self._unknown(
                    command_id=request.cancel_request_id,
                    execution_id=request.execution_id,
                    binding=queued.binding,
                    runtime=runtime,
                    result_code="NATIVE_CANCEL_ACCEPTANCE_MISMATCH",
                )
            return self._replace_record(
                command_id=request.cancel_request_id,
                execution_id=request.execution_id,
                binding=queued.binding,
                state=ConsumerExecutionDispatchStateV1.CANCEL_ACCEPTED,
                result_code="NATIVE_CANCEL_ACCEPTED",
            )
        return self._from_non_native_outcome(
            command_id=request.cancel_request_id,
            execution_id=request.execution_id,
            binding=queued.binding,
            runtime=runtime,
            outcome=dispatched.outcome,
        )

    def _from_non_native_outcome(
        self,
        *,
        command_id: str,
        execution_id: str,
        binding: ConsumerAccountBinding,
        runtime: ExecutionSessionRuntime,
        outcome: ExecutionDispatchOutcome,
    ) -> ConsumerExecutionDispatchRecordV1:
        if outcome is ExecutionDispatchOutcome.EXPIRED_PRE_NATIVE:
            return self._replace_record(
                command_id=command_id,
                execution_id=execution_id,
                binding=binding,
                state=ConsumerExecutionDispatchStateV1.EXPIRED_PRE_NATIVE,
                result_code=outcome.value,
            )
        if outcome is ExecutionDispatchOutcome.BLOCKED_DISARMED:
            return self._replace_record(
                command_id=command_id,
                execution_id=execution_id,
                binding=binding,
                state=ConsumerExecutionDispatchStateV1.BLOCKED_DISARMED,
                result_code=outcome.value,
            )
        return self._unknown(
            command_id=command_id,
            execution_id=execution_id,
            binding=binding,
            runtime=runtime,
            result_code="NATIVE_DISPATCH_OUTCOME_UNSUPPORTED",
        )

    def _unknown(
        self,
        *,
        command_id: str,
        execution_id: str,
        binding: ConsumerAccountBinding,
        runtime: ExecutionSessionRuntime,
        result_code: str,
    ) -> ConsumerExecutionDispatchRecordV1:
        runtime.close_admission(reason="consumer-ems-uncertain-native-dispatch")
        return self._replace_record(
            command_id=command_id,
            execution_id=execution_id,
            binding=binding,
            state=ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION,
            result_code=result_code,
        )

    def _remember_identity(self, accepted: SubmitAccepted, binding: ConsumerAccountBinding) -> None:
        identity = accepted.venue_order_identity
        if identity is None:
            return
        if not self._identity_matches_binding(identity, binding):
            raise ConsumerExecutionRuntimeErrorV1(
                "adapter returned a native identity outside the Consumer binding"
            )
        with self._state_lock:
            self._venue_identities[accepted.execution_id] = identity

    def _validated_runtime(self, binding: ConsumerAccountBinding) -> ExecutionSessionRuntime:
        runtime = self._runtime_for_binding(binding)
        route = runtime.binding
        if (
            route.binding_id != binding.binding_id
            or route.binding_version != binding.binding_version
            or route.account_id != binding.account_id
            or route.venue_id != binding.venue_id
            or route.adapter_instance_id != binding.adapter_instance_id
        ):
            runtime.close_admission(reason="consumer-binding-runtime-mismatch")
            raise ConsumerExecutionRuntimeErrorV1(
                "Consumer binding resolves to another EMS runtime generation"
            )
        return runtime

    @staticmethod
    def _identity_matches_binding(
        identity: VenueOrderIdentity, binding: ConsumerAccountBinding
    ) -> bool:
        return (
            identity.venue_id == binding.venue_id
            and identity.adapter_instance_id == binding.adapter_instance_id
        )

    @staticmethod
    def _require_order_binding(
        binding: ConsumerAccountBinding, order: OrderBookOrder
    ) -> ConsumerAccountBinding:
        trace = order.order.consumer_trace
        if (
            binding.account_id != order.order.account_id
            or binding.consumer_id != trace.consumer_id
            or binding.consumer_account_id != trace.consumer_account_id
            or not binding.active
        ):
            raise ConsumerExecutionRuntimeErrorV1(
                "Consumer EMS bridge resolved an order outside its active binding"
            )
        return binding

    @staticmethod
    def _require_size(value: int, command_kind: str) -> int:
        return require_positive(value, f"consumer bridge {command_kind} serialized size")

    def _pop_new_order(self) -> _QueuedNewOrderV1 | None:
        with self._state_lock:
            return self._new_orders.popleft() if self._new_orders else None

    def _pop_cancel(self) -> _QueuedCancelV1 | None:
        with self._state_lock:
            return self._cancels.popleft() if self._cancels else None

    def _replace_record(
        self,
        *,
        command_id: str,
        execution_id: str,
        binding: ConsumerAccountBinding,
        state: ConsumerExecutionDispatchStateV1,
        result_code: str,
    ) -> ConsumerExecutionDispatchRecordV1:
        record = self._record(
            command_id=command_id,
            execution_id=execution_id,
            binding=binding,
            state=state,
            result_code=result_code,
        )
        with self._state_lock:
            self._records[command_id] = record
        return record

    @staticmethod
    def _record(
        *,
        command_id: str,
        execution_id: str,
        binding: ConsumerAccountBinding,
        state: ConsumerExecutionDispatchStateV1,
        result_code: str,
    ) -> ConsumerExecutionDispatchRecordV1:
        return ConsumerExecutionDispatchRecordV1(
            command_id=command_id,
            execution_id=execution_id,
            binding_id=binding.binding_id,
            binding_generation=binding.binding_version,
            state=state,
            result_code=result_code,
        )
