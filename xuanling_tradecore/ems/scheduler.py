"""Bounded per-account ingress reservation and native-dispatch scheduling."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Condition, Lock
from types import MappingProxyType
from typing import Mapping, cast

from ..domain import (
    CancelDeadlinePolicy,
    CancelRequest,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
)
from ..domain._validation import require_positive, require_sha256, require_text


class SchedulerStateError(RuntimeError):
    """An ingress ticket or scheduled command violates scheduler state."""


class SchedulerAdmissionExpiredError(SchedulerStateError):
    """A reserved command expired while it was still provably PRE_NATIVE."""


class IngressLane(str, Enum):
    NEW_ORDER = "NEW_ORDER"
    CANCEL = "CANCEL"


class IngressRejectionCode(str, Enum):
    INGRESS_OVERLOADED = "INGRESS_OVERLOADED"
    INGRESS_MESSAGE_TOO_LARGE = "INGRESS_MESSAGE_TOO_LARGE"
    INGRESS_DUPLICATE_ACTIVE = "INGRESS_DUPLICATE_ACTIVE"


@dataclass(frozen=True)
class ExecutionSchedulerPolicy:
    """Versioned queue, byte-bound and native-inflight policy.

    ``maximum_inflight_new_orders`` is independent from pre-native queue
    capacity.  A credit is claimed at new-order admission and retained until
    the caller proves the SDK was never called, or a durably processed terminal
    callback releases it.  This keeps excess raw signals out of the native
    lane instead of converting them into venue-side pressure.
    """

    policy_id: str
    version: int
    policy_hash: str
    new_order_capacity: int
    cancel_capacity: int
    new_order_high_watermark: int
    cancel_high_watermark: int
    max_new_order_bytes: int
    max_cancel_bytes: int
    maximum_inflight_new_orders: int | None = None

    def __post_init__(self) -> None:
        require_text(self.policy_id, "scheduler policy_id")
        require_positive(self.version, "scheduler policy version")
        require_sha256(self.policy_hash, "scheduler policy_hash")
        for field_name, value in (
            ("new_order_capacity", self.new_order_capacity),
            ("cancel_capacity", self.cancel_capacity),
            ("new_order_high_watermark", self.new_order_high_watermark),
            ("cancel_high_watermark", self.cancel_high_watermark),
            ("max_new_order_bytes", self.max_new_order_bytes),
            ("max_cancel_bytes", self.max_cancel_bytes),
        ):
            require_positive(value, f"scheduler {field_name}")
        if self.new_order_high_watermark > self.new_order_capacity:
            raise ValueError("new-order high watermark exceeds capacity")
        if self.cancel_high_watermark > self.cancel_capacity:
            raise ValueError("cancel high watermark exceeds capacity")
        if self.maximum_inflight_new_orders is not None:
            require_positive(
                self.maximum_inflight_new_orders,
                "scheduler maximum_inflight_new_orders",
            )


@dataclass(frozen=True)
class IngressAdmission:
    """A bounded slot reserved before OMS identity, freeze or CALL_STARTED."""

    lane: IngressLane
    request_id: str
    account_id: str
    process_epoch_id: str
    admitted_monotonic_ns: int
    native_start_deadline_ns: int
    serialized_size_bytes: int
    scheduler_policy_id: str
    scheduler_policy_version: int
    scheduler_policy_hash: str
    deadline_policy_id: str
    deadline_policy_version: int
    deadline_policy_hash: str


@dataclass(frozen=True)
class IngressRejection:
    """Structured zero-admission result; new-order rejects never have an OMS ID."""

    lane: IngressLane
    request_id: str
    account_id: str
    code: IngressRejectionCode
    retryable: bool
    oms_order_id: None = None


IngressDecision = IngressAdmission | IngressRejection


@dataclass(frozen=True)
class ScheduledDispatch:
    """One dequeued command and its PRE_NATIVE deadline disposition."""

    lane: IngressLane
    admission: IngressAdmission
    command: ExecutionInstruction | CancelRequest
    dequeued_monotonic_ns: int
    queue_wait_ns: int
    expired_before_native: bool


@dataclass(frozen=True)
class ExecutionSchedulerSnapshot:
    """Bounded operational evidence for watermarks, budgets and rejections."""

    account_id: str
    process_epoch_id: str
    scheduler_policy_id: str
    scheduler_policy_version: int
    scheduler_policy_hash: str
    execution_deadline_policy_hash: str
    cancel_deadline_policy_hash: str
    execution_native_start_budget_ns: int
    cancel_native_start_budget_ns: int
    new_order_high_watermark_reached: bool
    cancel_high_watermark_reached: bool
    capacities: Mapping[str, int]
    high_watermarks: Mapping[str, int]
    depths: Mapping[str, int]
    max_observed_depths: Mapping[str, int]
    counters: Mapping[str, int]


@dataclass(frozen=True)
class _QueuedCommand:
    admission: IngressAdmission
    command: ExecutionInstruction | CancelRequest


def _frozen_int_mapping(values: Mapping[str, int]) -> Mapping[str, int]:
    return cast(Mapping[str, int], MappingProxyType(dict(values)))


class ExecutionScheduler:
    """Two isolated bounded lanes; qualified cancels always dequeue first."""

    def __init__(
        self,
        *,
        account_id: str,
        process_epoch_id: str,
        policy: ExecutionSchedulerPolicy,
        execution_deadline_policy: ExecutionDeadlinePolicy,
        cancel_deadline_policy: CancelDeadlinePolicy,
    ) -> None:
        self._account_id = require_text(account_id, "scheduler account_id")
        self._process_epoch_id = require_text(
            process_epoch_id, "scheduler process_epoch_id", max_length=128
        )
        self._policy = policy
        self._execution_deadline_policy = execution_deadline_policy
        self._cancel_deadline_policy = cancel_deadline_policy
        self._new_reservations: dict[str, IngressAdmission] = {}
        self._cancel_reservations: dict[str, IngressAdmission] = {}
        self._new_queue: deque[_QueuedCommand] = deque()
        self._cancel_queue: deque[_QueuedCommand] = deque()
        self._active_new_ids: set[str] = set()
        self._active_cancel_ids: set[str] = set()
        # The request map covers a reservation before it has an execution ID;
        # the execution map supports idempotent terminal callback release.
        self._new_inflight_by_request: dict[str, str | None] = {}
        self._new_inflight_by_execution: dict[str, str] = {}
        self._max_new_depth = 0
        self._max_cancel_depth = 0
        self._max_new_inflight = 0
        self._counters: dict[str, int] = {
            "new_order_accepted": 0,
            "cancel_accepted": 0,
            "new_order_overloaded": 0,
            "cancel_overloaded": 0,
            "new_order_too_large": 0,
            "cancel_too_large": 0,
            "new_order_duplicate": 0,
            "cancel_duplicate": 0,
            "new_order_abandoned": 0,
            "cancel_abandoned": 0,
            "new_order_expired_pre_native": 0,
            "cancel_expired_pre_native": 0,
            "new_order_dequeued": 0,
            "cancel_dequeued": 0,
            "new_order_inflight_limited": 0,
            "new_order_inflight_released": 0,
            "dispatch_wake_notifications": 0,
        }
        self._lock = Lock()
        self._dispatch_ready = Condition(self._lock)

    @property
    def account_id(self) -> str:
        return self._account_id

    def admit_new_order(
        self,
        *,
        request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
    ) -> IngressDecision:
        return self._admit(
            lane=IngressLane.NEW_ORDER,
            request_id=request_id,
            serialized_size_bytes=serialized_size_bytes,
            admitted_monotonic_ns=admitted_monotonic_ns,
        )

    def admit_and_commit_new_order(
        self,
        *,
        request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
        instruction: ExecutionInstruction,
        checked_monotonic_ns: int,
    ) -> IngressDecision:
        """Atomically reserve and queue one ready new-order command.

        The two-phase ``admit_new_order``/``commit_new_order`` API remains for
        callers that must reserve a slot before they can build the immutable
        execution instruction.  A hot ingress path that already has that
        instruction should use this method instead: it avoids a second lock
        acquisition and never exposes an intermediate reservation to another
        thread.
        """

        canonical_request_id, capacity, maximum_bytes, deadline_policy = (
            self._validated_admission_inputs(
                lane=IngressLane.NEW_ORDER,
                request_id=request_id,
                serialized_size_bytes=serialized_size_bytes,
                admitted_monotonic_ns=admitted_monotonic_ns,
            )
        )
        if instruction.consumer_trace.strategy_order_id != canonical_request_id:
            raise SchedulerStateError("strategy_order_id does not match the ingress request")
        if instruction.account_id != self._account_id:
            raise SchedulerStateError("scheduled command belongs to another account")
        if checked_monotonic_ns < admitted_monotonic_ns:
            raise ValueError("scheduler check time precedes admission")

        with self._lock:
            self._require_execution_id_available_locked(instruction.execution_id)
            admission = self._reserve_locked(
                lane=IngressLane.NEW_ORDER,
                canonical_request_id=canonical_request_id,
                serialized_size_bytes=serialized_size_bytes,
                admitted_monotonic_ns=admitted_monotonic_ns,
                capacity=capacity,
                maximum_bytes=maximum_bytes,
                deadline_policy=deadline_policy,
            )
            if isinstance(admission, IngressRejection):
                return admission
            if checked_monotonic_ns > admission.native_start_deadline_ns:
                del self._new_reservations[admission.request_id]
                self._active_new_ids.remove(admission.request_id)
                self._release_new_inflight_by_request_locked(admission.request_id)
                self._increment(IngressLane.NEW_ORDER, "expired_pre_native")
                raise SchedulerAdmissionExpiredError(
                    "command expired while still an unqueued PRE_NATIVE reservation"
                )
            del self._new_reservations[admission.request_id]
            self._bind_new_inflight_execution_locked(admission.request_id, instruction.execution_id)
            self._enqueue_and_notify_if_idle_locked(
                self._new_queue,
                _QueuedCommand(admission=admission, command=instruction),
            )
            return admission

    def admit_cancel(
        self,
        *,
        cancel_request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
    ) -> IngressDecision:
        return self._admit(
            lane=IngressLane.CANCEL,
            request_id=cancel_request_id,
            serialized_size_bytes=serialized_size_bytes,
            admitted_monotonic_ns=admitted_monotonic_ns,
        )

    def _admit(
        self,
        *,
        lane: IngressLane,
        request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
    ) -> IngressDecision:
        canonical_request_id, capacity, maximum_bytes, deadline_policy = (
            self._validated_admission_inputs(
                lane=lane,
                request_id=request_id,
                serialized_size_bytes=serialized_size_bytes,
                admitted_monotonic_ns=admitted_monotonic_ns,
            )
        )

        with self._lock:
            return self._reserve_locked(
                lane=lane,
                canonical_request_id=canonical_request_id,
                serialized_size_bytes=serialized_size_bytes,
                admitted_monotonic_ns=admitted_monotonic_ns,
                capacity=capacity,
                maximum_bytes=maximum_bytes,
                deadline_policy=deadline_policy,
            )

    def _validated_admission_inputs(
        self,
        *,
        lane: IngressLane,
        request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
    ) -> tuple[str, int, int, ExecutionDeadlinePolicy | CancelDeadlinePolicy]:
        canonical_request_id = require_text(request_id, "scheduler request_id", max_length=128)
        require_positive(serialized_size_bytes, "scheduler serialized_size_bytes")
        if admitted_monotonic_ns < 0:
            raise ValueError("scheduler admitted_monotonic_ns must be nonnegative")
        if lane is IngressLane.NEW_ORDER:
            return (
                canonical_request_id,
                self._policy.new_order_capacity,
                self._policy.max_new_order_bytes,
                self._execution_deadline_policy,
            )
        return (
            canonical_request_id,
            self._policy.cancel_capacity,
            self._policy.max_cancel_bytes,
            self._cancel_deadline_policy,
        )

    def _reserve_locked(
        self,
        *,
        lane: IngressLane,
        canonical_request_id: str,
        serialized_size_bytes: int,
        admitted_monotonic_ns: int,
        capacity: int,
        maximum_bytes: int,
        deadline_policy: ExecutionDeadlinePolicy | CancelDeadlinePolicy,
    ) -> IngressDecision:
        reservations, queue, active_ids = self._lane_state(lane)
        if canonical_request_id in active_ids:
            self._increment(lane, "duplicate")
            return IngressRejection(
                lane=lane,
                request_id=canonical_request_id,
                account_id=self._account_id,
                code=IngressRejectionCode.INGRESS_DUPLICATE_ACTIVE,
                retryable=False,
            )
        if serialized_size_bytes > maximum_bytes:
            self._increment(lane, "too_large")
            return IngressRejection(
                lane=lane,
                request_id=canonical_request_id,
                account_id=self._account_id,
                code=IngressRejectionCode.INGRESS_MESSAGE_TOO_LARGE,
                retryable=False,
            )
        if (
            lane is IngressLane.NEW_ORDER
            and self._policy.maximum_inflight_new_orders is not None
            and len(self._new_inflight_by_request) >= self._policy.maximum_inflight_new_orders
        ):
            self._increment(lane, "inflight_limited")
            return IngressRejection(
                lane=lane,
                request_id=canonical_request_id,
                account_id=self._account_id,
                code=IngressRejectionCode.INGRESS_OVERLOADED,
                retryable=True,
            )
        if len(reservations) + len(queue) >= capacity:
            self._increment(lane, "overloaded")
            return IngressRejection(
                lane=lane,
                request_id=canonical_request_id,
                account_id=self._account_id,
                code=IngressRejectionCode.INGRESS_OVERLOADED,
                retryable=True,
            )
        admission = IngressAdmission(
            lane=lane,
            request_id=canonical_request_id,
            account_id=self._account_id,
            process_epoch_id=self._process_epoch_id,
            admitted_monotonic_ns=admitted_monotonic_ns,
            native_start_deadline_ns=deadline_policy.deadline_for(admitted_monotonic_ns),
            serialized_size_bytes=serialized_size_bytes,
            scheduler_policy_id=self._policy.policy_id,
            scheduler_policy_version=self._policy.version,
            scheduler_policy_hash=self._policy.policy_hash,
            deadline_policy_id=deadline_policy.policy_id,
            deadline_policy_version=deadline_policy.version,
            deadline_policy_hash=deadline_policy.policy_hash,
        )
        reservations[canonical_request_id] = admission
        active_ids.add(canonical_request_id)
        if lane is IngressLane.NEW_ORDER:
            self._new_inflight_by_request[canonical_request_id] = None
            self._max_new_inflight = max(
                self._max_new_inflight,
                len(self._new_inflight_by_request),
            )
        self._increment(lane, "accepted")
        self._observe_depth(lane, len(reservations) + len(queue))
        return admission

    def commit_new_order(
        self,
        admission: IngressAdmission,
        instruction: ExecutionInstruction,
        *,
        checked_monotonic_ns: int,
    ) -> None:
        if admission.lane is not IngressLane.NEW_ORDER:
            raise SchedulerStateError("a cancel reservation cannot schedule a new order")
        if instruction.consumer_trace.strategy_order_id != admission.request_id:
            raise SchedulerStateError("strategy_order_id does not match its ingress reservation")
        self._commit(
            admission,
            instruction,
            checked_monotonic_ns=checked_monotonic_ns,
        )

    def commit_cancel(
        self,
        admission: IngressAdmission,
        request: CancelRequest,
        *,
        checked_monotonic_ns: int,
    ) -> None:
        if admission.lane is not IngressLane.CANCEL:
            raise SchedulerStateError("a new-order reservation cannot schedule a cancel")
        if request.cancel_request_id != admission.request_id:
            raise SchedulerStateError("cancel_request_id does not match its ingress reservation")
        if request.requested_monotonic_ns > admission.admitted_monotonic_ns:
            raise SchedulerStateError("cancel admission precedes the request timestamp")
        self._commit(admission, request, checked_monotonic_ns=checked_monotonic_ns)

    def _commit(
        self,
        admission: IngressAdmission,
        command: ExecutionInstruction | CancelRequest,
        *,
        checked_monotonic_ns: int,
    ) -> None:
        if checked_monotonic_ns < admission.admitted_monotonic_ns:
            raise ValueError("scheduler check time precedes admission")
        if command.account_id != self._account_id or admission.account_id != self._account_id:
            raise SchedulerStateError("scheduled command belongs to another account")
        with self._lock:
            reservations, queue, active_ids = self._lane_state(admission.lane)
            if (
                admission.process_epoch_id != self._process_epoch_id
                or reservations.get(admission.request_id) != admission
            ):
                raise SchedulerStateError(
                    "ingress reservation is foreign, stale or already consumed"
                )
            if checked_monotonic_ns > admission.native_start_deadline_ns:
                del reservations[admission.request_id]
                active_ids.remove(admission.request_id)
                if admission.lane is IngressLane.NEW_ORDER:
                    self._release_new_inflight_by_request_locked(admission.request_id)
                self._increment(admission.lane, "expired_pre_native")
                raise SchedulerAdmissionExpiredError(
                    "command expired while still an unqueued PRE_NATIVE reservation"
                )
            if admission.lane is IngressLane.NEW_ORDER:
                assert isinstance(command, ExecutionInstruction)
                self._require_execution_id_available_locked(command.execution_id)
            del reservations[admission.request_id]
            if admission.lane is IngressLane.NEW_ORDER:
                self._bind_new_inflight_execution_locked(
                    admission.request_id,
                    command.execution_id,
                )
            self._enqueue_and_notify_if_idle_locked(
                queue,
                _QueuedCommand(admission=admission, command=command),
            )

    def abandon(self, admission: IngressAdmission) -> None:
        """Return an unused pre-OMS/Guard reservation without creating facts."""

        with self._lock:
            reservations, _, active_ids = self._lane_state(admission.lane)
            if reservations.get(admission.request_id) != admission:
                raise SchedulerStateError(
                    "ingress reservation is foreign, stale or already consumed"
                )
            del reservations[admission.request_id]
            active_ids.remove(admission.request_id)
            if admission.lane is IngressLane.NEW_ORDER:
                self._release_new_inflight_by_request_locked(admission.request_id)
            self._increment(admission.lane, "abandoned")

    def release_new_order_inflight(self, execution_id: str) -> bool:
        """Release one credit after no SDK call or a durable terminal fact.

        Duplicate terminal callbacks are expected and therefore return
        ``False`` without changing state.  A merely accepted SDK call or a
        working callback must not release a credit.
        """

        canonical_execution_id = require_text(
            execution_id,
            "scheduler execution_id",
            max_length=256,
        )
        with self._lock:
            request_id = self._new_inflight_by_execution.get(canonical_execution_id)
            if request_id is None:
                return False
            self._release_new_inflight_by_request_locked(request_id)
            return True

    def dequeue(self, *, checked_monotonic_ns: int) -> ScheduledDispatch | None:
        """Prefer cancel and report expiry; never invokes Journal or Adapter code."""

        if checked_monotonic_ns < 0:
            raise ValueError("scheduler check time must be nonnegative")
        with self._lock:
            if self._cancel_queue:
                lane = IngressLane.CANCEL
                queue = self._cancel_queue
                active_ids = self._active_cancel_ids
            elif self._new_queue:
                lane = IngressLane.NEW_ORDER
                queue = self._new_queue
                active_ids = self._active_new_ids
            else:
                return None
            queued = queue[0]
            if checked_monotonic_ns < queued.admission.admitted_monotonic_ns:
                raise SchedulerStateError("scheduler monotonic clock moved backwards")
            queue.popleft()
            active_ids.remove(queued.admission.request_id)
            expired = checked_monotonic_ns > queued.admission.native_start_deadline_ns
            self._increment(lane, "dequeued")
            if expired:
                if lane is IngressLane.NEW_ORDER:
                    self._release_new_inflight_by_request_locked(queued.admission.request_id)
                self._increment(lane, "expired_pre_native")
            return ScheduledDispatch(
                lane=lane,
                admission=queued.admission,
                command=queued.command,
                dequeued_monotonic_ns=checked_monotonic_ns,
                queue_wait_ns=checked_monotonic_ns - queued.admission.admitted_monotonic_ns,
                expired_before_native=expired,
            )

    def wait_for_dispatch(self, *, timeout_seconds: float | None = None) -> bool:
        """Wait until either lane has work, without polling the hot path.

        This is an optional consumer-side primitive.  It neither dequeues a
        command nor changes priority: callers still use :meth:`dequeue` with
        a freshly observed monotonic timestamp after a ``True`` result.
        """

        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("scheduler dispatch wait timeout must be nonnegative")
        with self._lock:
            if self._cancel_queue or self._new_queue:
                return True
            self._dispatch_ready.wait(timeout_seconds)
            return bool(self._cancel_queue or self._new_queue)

    def snapshot(self) -> ExecutionSchedulerSnapshot:
        with self._lock:
            new_depth = len(self._new_reservations) + len(self._new_queue)
            cancel_depth = len(self._cancel_reservations) + len(self._cancel_queue)
            return ExecutionSchedulerSnapshot(
                account_id=self._account_id,
                process_epoch_id=self._process_epoch_id,
                scheduler_policy_id=self._policy.policy_id,
                scheduler_policy_version=self._policy.version,
                scheduler_policy_hash=self._policy.policy_hash,
                execution_deadline_policy_hash=self._execution_deadline_policy.policy_hash,
                cancel_deadline_policy_hash=self._cancel_deadline_policy.policy_hash,
                execution_native_start_budget_ns=(
                    self._execution_deadline_policy.native_start_budget_ns
                ),
                cancel_native_start_budget_ns=self._cancel_deadline_policy.native_start_budget_ns,
                new_order_high_watermark_reached=(
                    new_depth >= self._policy.new_order_high_watermark
                ),
                cancel_high_watermark_reached=(cancel_depth >= self._policy.cancel_high_watermark),
                capacities=_frozen_int_mapping(
                    {
                        "new_order": self._policy.new_order_capacity,
                        "cancel": self._policy.cancel_capacity,
                        "new_order_inflight": self._policy.maximum_inflight_new_orders or 0,
                        "max_new_order_bytes": self._policy.max_new_order_bytes,
                        "max_cancel_bytes": self._policy.max_cancel_bytes,
                    }
                ),
                high_watermarks=_frozen_int_mapping(
                    {
                        "new_order": self._policy.new_order_high_watermark,
                        "cancel": self._policy.cancel_high_watermark,
                    }
                ),
                depths=_frozen_int_mapping(
                    {
                        "new_order": new_depth,
                        "cancel": cancel_depth,
                        "new_order_reservations": len(self._new_reservations),
                        "cancel_reservations": len(self._cancel_reservations),
                        "new_order_queued": len(self._new_queue),
                        "cancel_queued": len(self._cancel_queue),
                        "new_order_inflight": len(self._new_inflight_by_request),
                    }
                ),
                max_observed_depths=_frozen_int_mapping(
                    {
                        "new_order": self._max_new_depth,
                        "cancel": self._max_cancel_depth,
                        "new_order_inflight": self._max_new_inflight,
                    }
                ),
                counters=_frozen_int_mapping(self._counters),
            )

    def _lane_state(
        self, lane: IngressLane
    ) -> tuple[dict[str, IngressAdmission], deque[_QueuedCommand], set[str]]:
        if lane is IngressLane.NEW_ORDER:
            return self._new_reservations, self._new_queue, self._active_new_ids
        return self._cancel_reservations, self._cancel_queue, self._active_cancel_ids

    def _enqueue_and_notify_if_idle_locked(
        self, queue: deque[_QueuedCommand], queued: _QueuedCommand
    ) -> None:
        """Wake the sole native consumer only on an empty-to-nonempty edge.

        The scheduler has one dispatch consumer per session.  Once either
        queue holds work that consumer cannot be sleeping in
        :meth:`wait_for_dispatch`, so another condition notification adds no
        progress signal.  Coalescing the notification preserves the existing
        cancel-first dequeue rule while avoiding one Python Condition notify
        for every member of a burst.

        The caller holds ``_lock``.  Checking both queues before appending is
        safe because ``Condition.wait`` releases that same lock atomically;
        it therefore cannot miss the only wakeup transition.
        """

        idle = not self._cancel_queue and not self._new_queue
        queue.append(queued)
        if idle:
            self._dispatch_ready.notify()
            self._counters["dispatch_wake_notifications"] += 1

    def _require_execution_id_available_locked(self, execution_id: str) -> None:
        if execution_id in self._new_inflight_by_execution:
            raise SchedulerStateError("execution already holds a native-inflight credit")

    def _bind_new_inflight_execution_locked(self, request_id: str, execution_id: str) -> None:
        if request_id not in self._new_inflight_by_request:
            raise SchedulerStateError("new-order admission has no native-inflight credit")
        self._require_execution_id_available_locked(execution_id)
        self._new_inflight_by_request[request_id] = execution_id
        self._new_inflight_by_execution[execution_id] = request_id

    def _release_new_inflight_by_request_locked(self, request_id: str) -> bool:
        if request_id not in self._new_inflight_by_request:
            return False
        execution_id = self._new_inflight_by_request.pop(request_id)
        if execution_id is not None:
            self._new_inflight_by_execution.pop(execution_id, None)
        self._increment(IngressLane.NEW_ORDER, "inflight_released")
        return True

    def _increment(self, lane: IngressLane, suffix: str) -> None:
        key = f"{'new_order' if lane is IngressLane.NEW_ORDER else 'cancel'}_{suffix}"
        self._counters[key] += 1

    def _observe_depth(self, lane: IngressLane, depth: int) -> None:
        if lane is IngressLane.NEW_ORDER:
            self._max_new_depth = max(self._max_new_depth, depth)
        else:
            self._max_cancel_depth = max(self._max_cancel_depth, depth)
