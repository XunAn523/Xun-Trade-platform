"""One-session EMS composition for a verified BrokerAdapter.

The runtime makes the order of operations explicit and testable:

``EMS Guard -> static account route -> bounded scheduler -> one dispatch caller
-> BrokerAdapter``.

It deliberately does not arm an account.  A running session remains
``DISARMED`` and this class only provides the composition seam used by a
separately approved controlled-mutation harness.  Callback gaps and
host-health failures latch the composition closed before another native call
can start; they never retry or re-route an instruction.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Callable

from ..adapters import (
    BrokerAdapter,
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    SubmitAccepted,
)
from ..domain import (
    BrokerEvent,
    CallbackGapEvidence,
    CancelRequest,
    ExecutionInstruction,
    VenueOrderIdentity,
)
from ..domain._validation import require_nonnegative, require_text
from .guard import (
    ExecutionGuardAdmissionGate,
    GuardedAdmissionResult,
    GuardMarketSnapshot,
    KillSwitchSnapshot,
)
from .routing import AccountExecutionBinding, AccountExecutionRouter
from .scheduler import (
    ExecutionScheduler,
    IngressAdmission,
    IngressDecision,
    IngressLane,
    ScheduledDispatch,
)
from .session import SessionSupervisor


class ExecutionSessionRuntimeError(RuntimeError):
    """A session composition cannot safely perform another native side effect."""


class ExecutionDispatchOutcome(str, Enum):
    """The local disposition of one dequeued, already-admitted command."""

    SUBMIT_ACCEPTED = "SUBMIT_ACCEPTED"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"
    EXPIRED_PRE_NATIVE = "EXPIRED_PRE_NATIVE"
    BLOCKED_DISARMED = "BLOCKED_DISARMED"


@dataclass(frozen=True)
class ExecutionDispatchResult:
    """One dispatch disposition without any native SDK object or payload."""

    scheduled: ScheduledDispatch
    outcome: ExecutionDispatchOutcome
    submit_accepted: SubmitAccepted | None = None
    cancel_accepted: CancelAccepted | None = None

    def __post_init__(self) -> None:
        if self.outcome is ExecutionDispatchOutcome.SUBMIT_ACCEPTED:
            if self.submit_accepted is None or self.cancel_accepted is not None:
                raise ExecutionSessionRuntimeError("submit dispatch result is inconsistent")
        elif self.outcome is ExecutionDispatchOutcome.CANCEL_ACCEPTED:
            if self.cancel_accepted is None or self.submit_accepted is not None:
                raise ExecutionSessionRuntimeError("cancel dispatch result is inconsistent")
        elif self.submit_accepted is not None or self.cancel_accepted is not None:
            raise ExecutionSessionRuntimeError(
                "non-native dispatch result cannot contain acceptance"
            )


@dataclass(frozen=True)
class ExecutionSessionRuntimeSnapshot:
    """Payload-free runtime state for host-health and acceptance evidence."""

    started: bool
    admission_closed: bool
    admission_closed_reason: str | None
    callback_gap_count: int
    scheduler_depths: dict[str, int]


class _ObserverFence(BrokerEventObserver):
    """Forward facts while latching new native work after a callback gap."""

    def __init__(
        self,
        *,
        downstream: BrokerEventObserver,
        close_admission: Callable[[str], None],
    ) -> None:
        self._downstream = downstream
        self._close_admission = close_admission

    def on_broker_event(self, event: BrokerEvent) -> None:
        self._downstream.on_broker_event(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self._close_admission("callback-gap")
        self._downstream.on_callback_gap(evidence)


class ExecutionSessionRuntime:
    """Compose one exact account/scope route with one native dispatch caller.

    The caller is responsible for running :meth:`dispatch_once` from exactly
    one worker for this object.  A second concurrent caller is serialized by
    the runtime lock, preserving the vendor rule that one SDK session has one
    native caller.  `submit` and `cancel` only enqueue an already-authorized
    command; neither calls the Adapter inline.
    """

    def __init__(
        self,
        *,
        binding: AccountExecutionBinding,
        router: AccountExecutionRouter,
        scheduler: ExecutionScheduler,
        supervisor: SessionSupervisor,
        guard_admission: ExecutionGuardAdmissionGate,
    ) -> None:
        configured = router.plan.binding_for(binding.account_id)
        if configured != binding:
            raise ExecutionSessionRuntimeError("runtime binding is not the static route binding")
        if scheduler.account_id != binding.account_id:
            raise ExecutionSessionRuntimeError("runtime scheduler belongs to another account")
        self._binding = binding
        self._router = router
        self._scheduler = scheduler
        self._supervisor = supervisor
        self._guard_admission = guard_admission
        self._adapter: BrokerAdapter | None = None
        self._observer_fence: _ObserverFence | None = None
        self._cancel_targets: dict[str, VenueOrderIdentity] = {}
        self._admission_closed_reason: str | None = None
        self._callback_gap_count = 0
        self._lock = RLock()
        self._dispatch_lock = RLock()

    def start(self, observer: BrokerEventObserver, *, evaluated_wall_ns: int) -> None:
        """Start only after the supervisor verifies ownership and profile proof."""

        require_nonnegative(evaluated_wall_ns, "execution runtime evaluated wall time")
        with self._lock:
            if self._adapter is not None:
                raise ExecutionSessionRuntimeError("execution session runtime is already started")
            fenced_observer = _ObserverFence(
                downstream=observer,
                close_admission=self._close_from_callback_gap,
            )
            try:
                self._supervisor.start(fenced_observer, evaluated_wall_ns=evaluated_wall_ns)
                adapter = self._supervisor.require_started_adapter()
                self._require_adapter_scope(adapter)
            except BaseException:
                try:
                    self._supervisor.stop_adapter()
                except BaseException:
                    pass
                raise
            self._adapter = adapter
            self._observer_fence = fenced_observer

    @property
    def binding(self) -> AccountExecutionBinding:
        """Expose the immutable static route used to construct this runtime."""

        return self._binding

    def stop(self) -> None:
        """Stop the adapter; ownership-fence release remains the outer concern."""

        with self._dispatch_lock:
            with self._lock:
                if self._adapter is None:
                    return
                try:
                    self._supervisor.stop_adapter()
                finally:
                    self._adapter = None
                    self._observer_fence = None
                    self._cancel_targets.clear()

    def close_admission(self, *, reason: str) -> None:
        """Latch all future native work closed after a host-health failure."""

        self._close(reason)

    def on_host_health_integrity(self, scope: str, source_reference: str) -> None:
        """Match ``HostHealthWorkerV1.disarm_integrity`` without retaining diagnostics.

        The host-health worker owns the detailed evidence.  The execution
        runtime only validates that a bounded observation exists and latches
        its own future SDK calls closed.
        """

        require_text(scope, "execution host-health integrity scope", max_length=160)
        require_text(source_reference, "execution host-health integrity source", max_length=256)
        self._close("host-health-integrity")

    def on_host_health_capacity(self, scope: str, source_reference: str) -> None:
        """Match ``HostHealthWorkerV1.stop_new_work`` for this native session."""

        require_text(scope, "execution host-health capacity scope", max_length=160)
        require_text(source_reference, "execution host-health capacity source", max_length=256)
        self._close("host-health-capacity")

    def admit_submit(
        self,
        instruction: ExecutionInstruction,
        *,
        serialized_size_bytes: int,
        market: GuardMarketSnapshot,
        kill_switches: KillSwitchSnapshot,
        admitted_monotonic_ns: int,
    ) -> GuardedAdmissionResult[IngressDecision]:
        """Guard and queue a new order; this method never invokes the Adapter."""

        self._require_open_started()
        lane = self._router.route_execution(instruction)
        self._require_lane(lane.binding, lane.scheduler)
        request_id = instruction.consumer_trace.strategy_order_id
        if request_id is None:
            raise ExecutionSessionRuntimeError("execution runtime requires a strategy_order_id")

        return self._guard_admission.admit(
            instruction,
            market=market,
            kill_switches=kill_switches,
            evaluated_monotonic_ns=admitted_monotonic_ns,
            on_authorized=lambda authorized, _decision: self._scheduler.admit_and_commit_new_order(
                request_id=request_id,
                serialized_size_bytes=serialized_size_bytes,
                admitted_monotonic_ns=admitted_monotonic_ns,
                instruction=authorized,
                checked_monotonic_ns=admitted_monotonic_ns,
            ),
        )

    def admit_cancel(
        self,
        instruction: CancelExecutionInstruction,
        *,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
    ) -> IngressDecision:
        """Queue one exact-scope cancellation without invoking the Adapter."""

        self._require_open_started()
        lane = self._router.route_cancel(
            instruction.cancel_request,
            instruction.venue_order_identity,
        )
        self._require_lane(lane.binding, lane.scheduler)
        decision = self._scheduler.admit_cancel(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            serialized_size_bytes=serialized_size_bytes,
            admitted_monotonic_ns=admitted_monotonic_ns,
        )
        if isinstance(decision, IngressAdmission):
            try:
                self._scheduler.commit_cancel(
                    decision,
                    instruction.cancel_request,
                    checked_monotonic_ns=admitted_monotonic_ns,
                )
            except BaseException:
                self._scheduler.abandon(decision)
                raise
            with self._lock:
                self._cancel_targets[decision.request_id] = instruction.venue_order_identity
        return decision

    def query(self, request: BrokerQuery) -> QueryHandle:
        """Issue a bounded read-only query; queries remain available while closed."""

        adapter = self._require_started_adapter()
        if (
            request.account_id != self._binding.account_id
            or request.venue_id != self._binding.venue_id
            or request.adapter_instance_id != self._binding.adapter_instance_id
        ):
            raise ExecutionSessionRuntimeError("query is outside this static execution route")
        return adapter.query(request)

    def drain_broker_events(self, *, wait_timeout_seconds: float = 0.0) -> int:
        """Drain a bounded vendor callback batch through the fenced observer chain.

        ``BrokerAdapter`` intentionally has no mandatory polling method: an
        event-driven host may receive callbacks on a dedicated worker instead.
        The EMT controlled adapter provides this bounded drain surface, though,
        and a controlled acceptance harness must use the same runtime fence as
        mutation dispatch rather than reaching behind it to the Adapter.
        """

        if wait_timeout_seconds < 0:
            raise ValueError("broker event drain timeout must be nonnegative")
        adapter = self._require_started_adapter()
        drain = getattr(adapter, "drain_broker_events", None)
        if not callable(drain):
            raise ExecutionSessionRuntimeError(
                "started Adapter does not provide a bounded broker event drain"
            )
        drained = drain(wait_timeout_seconds=wait_timeout_seconds)
        if type(drained) is not int or drained < 0:
            raise ExecutionSessionRuntimeError(
                "Adapter returned an invalid broker event drain count"
            )
        return drained

    def dispatch_once(self, *, checked_monotonic_ns: int) -> ExecutionDispatchResult | None:
        """Run at most one queued command through the sole native caller."""

        require_nonnegative(checked_monotonic_ns, "execution dispatch check time")
        with self._dispatch_lock:
            scheduled = self._scheduler.dequeue(checked_monotonic_ns=checked_monotonic_ns)
            if scheduled is None:
                return None
            if scheduled.expired_before_native:
                self._discard_cancel_target(scheduled)
                return ExecutionDispatchResult(
                    scheduled=scheduled,
                    outcome=ExecutionDispatchOutcome.EXPIRED_PRE_NATIVE,
                )
            if self._admission_is_closed():
                self._discard_pre_native(scheduled)
                return ExecutionDispatchResult(
                    scheduled=scheduled,
                    outcome=ExecutionDispatchOutcome.BLOCKED_DISARMED,
                )
            adapter = self._require_started_adapter()
            if scheduled.lane is IngressLane.NEW_ORDER:
                assert isinstance(scheduled.command, ExecutionInstruction)
                accepted = adapter.submit(scheduled.command)
                return ExecutionDispatchResult(
                    scheduled=scheduled,
                    outcome=ExecutionDispatchOutcome.SUBMIT_ACCEPTED,
                    submit_accepted=accepted,
                )
            assert isinstance(scheduled.command, CancelRequest)
            target = self._pop_cancel_target(scheduled.admission.request_id)
            accepted = adapter.cancel(
                CancelExecutionInstruction(
                    cancel_request=scheduled.command,
                    venue_order_identity=target,
                )
            )
            return ExecutionDispatchResult(
                scheduled=scheduled,
                outcome=ExecutionDispatchOutcome.CANCEL_ACCEPTED,
                cancel_accepted=accepted,
            )

    def release_new_order_inflight_after_terminal(self, execution_id: str) -> bool:
        """Release one scheduler credit only after a durable terminal callback."""

        return self._scheduler.release_new_order_inflight(execution_id)

    def snapshot(self) -> ExecutionSessionRuntimeSnapshot:
        with self._lock:
            scheduler = self._scheduler.snapshot()
            return ExecutionSessionRuntimeSnapshot(
                started=self._adapter is not None,
                admission_closed=self._admission_closed_reason is not None,
                admission_closed_reason=self._admission_closed_reason,
                callback_gap_count=self._callback_gap_count,
                scheduler_depths=dict(scheduler.depths),
            )

    def _close_from_callback_gap(self, _reason: str) -> None:
        with self._lock:
            self._callback_gap_count += 1
        self._close("callback-gap")

    def _close(self, reason: str) -> None:
        canonical_reason = require_text(reason, "execution admission close reason", max_length=128)
        with self._lock:
            if self._admission_closed_reason is None:
                self._admission_closed_reason = canonical_reason

    def _require_open_started(self) -> None:
        self._require_started_adapter()
        if self._admission_is_closed():
            raise ExecutionSessionRuntimeError("execution session admission is closed")

    def _require_started_adapter(self) -> BrokerAdapter:
        with self._lock:
            adapter = self._adapter
        if adapter is None:
            raise ExecutionSessionRuntimeError("execution session runtime is not started")
        return adapter

    def _admission_is_closed(self) -> bool:
        with self._lock:
            return self._admission_closed_reason is not None

    def _require_adapter_scope(self, adapter: BrokerAdapter) -> None:
        if (
            adapter.adapter_instance_id != self._binding.adapter_instance_id
            or adapter.capabilities.venue_id != self._binding.venue_id
        ):
            raise ExecutionSessionRuntimeError(
                "started Adapter does not match static execution route"
            )

    def _require_lane(
        self,
        binding: AccountExecutionBinding,
        scheduler: ExecutionScheduler,
    ) -> None:
        if binding != self._binding or scheduler is not self._scheduler:
            raise ExecutionSessionRuntimeError("static route resolved to another execution session")

    def _discard_pre_native(self, scheduled: ScheduledDispatch) -> None:
        self._discard_cancel_target(scheduled)
        if scheduled.lane is IngressLane.NEW_ORDER:
            assert isinstance(scheduled.command, ExecutionInstruction)
            self._scheduler.release_new_order_inflight(scheduled.command.execution_id)

    def _discard_cancel_target(self, scheduled: ScheduledDispatch) -> None:
        if scheduled.lane is IngressLane.CANCEL:
            self._pop_cancel_target(scheduled.admission.request_id)

    def _pop_cancel_target(self, cancel_request_id: str) -> VenueOrderIdentity:
        with self._lock:
            try:
                return self._cancel_targets.pop(cancel_request_id)
            except KeyError as exc:
                raise ExecutionSessionRuntimeError(
                    "queued cancellation lacks its exact native identity"
                ) from exc
