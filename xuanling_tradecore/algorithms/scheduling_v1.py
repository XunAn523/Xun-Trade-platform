"""Pure F6 execution-algorithm planning and lifecycle control.

The module intentionally has no Adapter, scheduler, network, clock, or market
data dependency.  Its output is immutable ``ExecutionInstruction`` values
that must subsequently pass ordinary OMS/EMS admission and Journal-first
dispatch.  A paused or failed algorithm can therefore never bypass the EMT
execution fences.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from threading import RLock
from typing import Mapping, Protocol

from ..domain import ExecutionInstruction
from ..domain._validation import require_nonnegative, require_positive, require_text
from .planning import (
    AlgorithmChildInstruction,
    AlgorithmPlan,
    ExecutionAlgorithm,
    ExecutionAlgorithmError,
)

_ALGORITHM_NOT_BEFORE_NS = "algorithm_not_before_monotonic_ns"


def _require_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExecutionAlgorithmError(f"{field_name} must be an integer")
    return value


@dataclass(frozen=True)
class TargetPositionSpecV1:
    """Explicit current/target position delta for a side-fixed parent intent."""

    algorithm_id: str
    algorithm_version: int
    current_position: int
    target_position: int
    max_child_quantity: int

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "target-position algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "target-position algorithm_version")
        _require_integer(self.current_position, "target-position current_position")
        _require_integer(self.target_position, "target-position target_position")
        require_positive(self.max_child_quantity, "target-position max_child_quantity")


class TargetPositionAlgorithmV1:
    """Turn one explicit target delta into bounded, immutable child intents."""

    def __init__(self, spec: TargetPositionSpecV1) -> None:
        self._spec = spec

    @property
    def spec(self) -> TargetPositionSpecV1:
        return self._spec

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        delta = self._spec.target_position - self._spec.current_position
        if delta == 0:
            raise ExecutionAlgorithmError("target-position delta is zero")
        expected_side = "BUY" if delta > 0 else "SELL"
        if parent.side != expected_side or parent.quantity != abs(delta):
            raise ExecutionAlgorithmError(
                "target-position parent side or quantity does not match target delta"
            )
        quantities = _max_quantity_slices(parent.quantity, self._spec.max_child_quantity)
        return _make_plan(
            parent,
            algorithm_id=self._spec.algorithm_id,
            algorithm_version=self._spec.algorithm_version,
            quantities=quantities,
            schedule_ns=(0,) * len(quantities),
            extra_parameters={
                "algorithm_target_position": str(self._spec.target_position),
                "algorithm_current_position": str(self._spec.current_position),
            },
        )


@dataclass(frozen=True)
class TwapSpecV1:
    """A fixed monotonic release schedule for equally sized TWAP slices."""

    algorithm_id: str
    algorithm_version: int
    slice_count: int
    first_release_monotonic_ns: int
    interval_ns: int

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "TWAP algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "TWAP algorithm_version")
        require_positive(self.slice_count, "TWAP slice_count")
        require_nonnegative(self.first_release_monotonic_ns, "TWAP first_release_monotonic_ns")
        require_nonnegative(self.interval_ns, "TWAP interval_ns")


class TwapAlgorithmV1:
    """Plan equal-quantity slices; release times are inputs, never sleeps."""

    def __init__(self, spec: TwapSpecV1) -> None:
        self._spec = spec

    @property
    def spec(self) -> TwapSpecV1:
        return self._spec

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        if parent.quantity < self._spec.slice_count:
            raise ExecutionAlgorithmError("TWAP slice_count exceeds parent quantity")
        quantities = _allocate_weighted(parent.quantity, (1,) * self._spec.slice_count)
        schedule = tuple(
            self._spec.first_release_monotonic_ns + index * self._spec.interval_ns
            for index in range(self._spec.slice_count)
        )
        return _make_plan(
            parent,
            algorithm_id=self._spec.algorithm_id,
            algorithm_version=self._spec.algorithm_version,
            quantities=quantities,
            schedule_ns=schedule,
            extra_parameters={},
        )


@dataclass(frozen=True)
class VolumeScheduleBucketV1:
    """One externally supplied, versioned volume-curve bucket."""

    bucket_id: str
    release_monotonic_ns: int
    weight: int

    def __post_init__(self) -> None:
        require_text(self.bucket_id, "volume schedule bucket_id", max_length=128)
        require_nonnegative(self.release_monotonic_ns, "volume schedule release_monotonic_ns")
        require_positive(self.weight, "volume schedule weight")


@dataclass(frozen=True)
class VwapSpecV1:
    """Versioned externally supplied VWAP weights; no market-data client is embedded."""

    algorithm_id: str
    algorithm_version: int
    buckets: tuple[VolumeScheduleBucketV1, ...]

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "VWAP algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "VWAP algorithm_version")
        if not self.buckets:
            raise ExecutionAlgorithmError("VWAP requires volume schedule buckets")
        _require_unique_bucket_ids(self.buckets, "VWAP")


class VwapAlgorithmV1:
    """Allocate the parent quantity across immutable supplied VWAP buckets."""

    def __init__(self, spec: VwapSpecV1) -> None:
        self._spec = spec

    @property
    def spec(self) -> VwapSpecV1:
        return self._spec

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        if parent.quantity < len(self._spec.buckets):
            raise ExecutionAlgorithmError("VWAP bucket count exceeds parent quantity")
        quantities = _allocate_weighted(
            parent.quantity, tuple(bucket.weight for bucket in self._spec.buckets)
        )
        return _make_plan(
            parent,
            algorithm_id=self._spec.algorithm_id,
            algorithm_version=self._spec.algorithm_version,
            quantities=quantities,
            schedule_ns=tuple(bucket.release_monotonic_ns for bucket in self._spec.buckets),
            extra_parameters={
                "algorithm_schedule_kind": "VWAP",
            },
            schedule_bucket_ids=tuple(bucket.bucket_id for bucket in self._spec.buckets),
        )


@dataclass(frozen=True)
class PovScheduleBucketV1:
    """An externally measured expected-volume bucket used by POV planning."""

    bucket_id: str
    release_monotonic_ns: int
    expected_market_volume: int

    def __post_init__(self) -> None:
        require_text(self.bucket_id, "POV schedule bucket_id", max_length=128)
        require_nonnegative(self.release_monotonic_ns, "POV schedule release_monotonic_ns")
        require_positive(self.expected_market_volume, "POV expected_market_volume")


@dataclass(frozen=True)
class PovSpecV1:
    """A maximum participation plan based on supplied expected market volumes."""

    algorithm_id: str
    algorithm_version: int
    max_participation_bps: int
    buckets: tuple[PovScheduleBucketV1, ...]

    def __post_init__(self) -> None:
        require_text(self.algorithm_id, "POV algorithm_id", max_length=128)
        require_positive(self.algorithm_version, "POV algorithm_version")
        if not 1 <= self.max_participation_bps <= 10_000:
            raise ExecutionAlgorithmError("POV max_participation_bps must be in 1..10000")
        if not self.buckets:
            raise ExecutionAlgorithmError("POV requires expected-volume buckets")
        _require_unique_bucket_ids(self.buckets, "POV")


class PovAlgorithmV1:
    """Bound a parent quantity by explicit participation capacity per bucket."""

    def __init__(self, spec: PovSpecV1) -> None:
        self._spec = spec

    @property
    def spec(self) -> PovSpecV1:
        return self._spec

    def plan(self, parent: ExecutionInstruction) -> AlgorithmPlan:
        capacities = tuple(
            bucket.expected_market_volume * self._spec.max_participation_bps // 10_000
            for bucket in self._spec.buckets
        )
        if sum(capacities) < parent.quantity:
            raise ExecutionAlgorithmError(
                "POV expected-volume capacity cannot cover parent quantity"
            )
        allocated = _allocate_capped(parent.quantity, capacities)
        planned = tuple(
            (bucket, quantity)
            for bucket, quantity in zip(self._spec.buckets, allocated)
            if quantity > 0
        )
        return _make_plan(
            parent,
            algorithm_id=self._spec.algorithm_id,
            algorithm_version=self._spec.algorithm_version,
            quantities=tuple(quantity for _bucket, quantity in planned),
            schedule_ns=tuple(bucket.release_monotonic_ns for bucket, _quantity in planned),
            extra_parameters={
                "algorithm_schedule_kind": "POV",
                "algorithm_max_participation_bps": str(self._spec.max_participation_bps),
            },
            schedule_bucket_ids=tuple(bucket.bucket_id for bucket, _quantity in planned),
        )


class ExecutionAlgorithmSelectorV1:
    """Choose only a registered pure planner; unknown selection fails closed."""

    def __init__(self, algorithms: Mapping[str, ExecutionAlgorithm]) -> None:
        if not algorithms:
            raise ExecutionAlgorithmError("execution algorithm selector requires algorithms")
        selected: dict[str, ExecutionAlgorithm] = {}
        for algorithm_id, algorithm in algorithms.items():
            require_text(algorithm_id, "execution algorithm selector algorithm_id", max_length=128)
            if algorithm_id in selected:
                raise ExecutionAlgorithmError("execution algorithm selector repeats algorithm_id")
            selected[algorithm_id] = algorithm
        self._algorithms = selected

    def select(self, algorithm_id: str) -> ExecutionAlgorithm:
        try:
            return self._algorithms[algorithm_id]
        except KeyError as exc:
            raise ExecutionAlgorithmError("execution algorithm is not registered") from exc


class AlgorithmRunStateV1(str, Enum):
    """The algorithm control state, separate from individual child outcomes."""

    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AlgorithmChildStateV1(str, Enum):
    """Local lifecycle of one child before and after normal EMS admission."""

    PENDING = "PENDING"
    RELEASED = "RELEASED"
    ACCEPTED = "ACCEPTED"
    TERMINAL = "TERMINAL"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class AlgorithmAuditFactV1:
    """Payload-free audit fact for plan lifecycle and child lineage."""

    plan_id: str
    sequence: int
    event_kind: str
    observed_monotonic_ns: int
    child_execution_id: str | None
    detail_code: str

    def __post_init__(self) -> None:
        require_text(self.plan_id, "algorithm audit plan_id", max_length=256)
        require_positive(self.sequence, "algorithm audit sequence")
        require_text(self.event_kind, "algorithm audit event_kind", max_length=128)
        require_nonnegative(self.observed_monotonic_ns, "algorithm audit observed_monotonic_ns")
        if self.child_execution_id is not None:
            require_text(
                self.child_execution_id, "algorithm audit child_execution_id", max_length=256
            )
        require_text(self.detail_code, "algorithm audit detail_code", max_length=128)


class AlgorithmAuditPortV1(Protocol):
    """Durable fact append boundary owned by the caller's normal OMS Journal."""

    def append_algorithm_audit(self, fact: AlgorithmAuditFactV1) -> None:
        """Persist the fact before exposing the corresponding controller state."""


@dataclass(frozen=True)
class AlgorithmRunSnapshotV1:
    """Read-only controller state; no BrokerAdapter state is inferred."""

    plan_id: str
    state: AlgorithmRunStateV1
    child_states: Mapping[str, AlgorithmChildStateV1]
    audit_sequence: int


class AlgorithmRunControllerV1:
    """Pause/resume/release children without dispatching an Adapter.

    A rejected child moves the run to ``FAILED`` and stops releases.  A
    terminal event only completes the run after every child has reached an
    explicit terminal state.  The controller does not invent replacement
    child IDs or retry a rejected instruction.
    """

    def __init__(
        self,
        *,
        plan_id: str,
        plan: AlgorithmPlan,
        audit_port: AlgorithmAuditPortV1,
        created_monotonic_ns: int,
    ) -> None:
        require_text(plan_id, "algorithm run plan_id", max_length=256)
        require_nonnegative(created_monotonic_ns, "algorithm run created_monotonic_ns")
        self._plan_id = plan_id
        self._plan = plan
        self._audit_port = audit_port
        self._lock = RLock()
        self._state = AlgorithmRunStateV1.PLANNED
        self._child_states = {
            child.instruction.execution_id: AlgorithmChildStateV1.PENDING for child in plan.children
        }
        self._audit_sequence = 0
        self._append_fact(
            event_kind="PLAN_CREATED",
            observed_monotonic_ns=created_monotonic_ns,
            child_execution_id=None,
            detail_code="PLANNED",
        )

    @property
    def plan(self) -> AlgorithmPlan:
        return self._plan

    def snapshot(self) -> AlgorithmRunSnapshotV1:
        with self._lock:
            return AlgorithmRunSnapshotV1(
                plan_id=self._plan_id,
                state=self._state,
                child_states=dict(self._child_states),
                audit_sequence=self._audit_sequence,
            )

    def start(self, *, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_run(
                expected=(AlgorithmRunStateV1.PLANNED,),
                next_state=AlgorithmRunStateV1.RUNNING,
                event_kind="PLAN_STARTED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code="RUNNING",
            )

    def pause(self, *, reason_code: str, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_run(
                expected=(AlgorithmRunStateV1.RUNNING,),
                next_state=AlgorithmRunStateV1.PAUSED,
                event_kind="PLAN_PAUSED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code=require_text(reason_code, "algorithm pause reason", max_length=128),
            )

    def resume(self, *, reason_code: str, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_run(
                expected=(AlgorithmRunStateV1.PAUSED,),
                next_state=AlgorithmRunStateV1.RUNNING,
                event_kind="PLAN_RESUMED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code=require_text(reason_code, "algorithm resume reason", max_length=128),
            )

    def cancel(self, *, reason_code: str, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_run(
                expected=(
                    AlgorithmRunStateV1.PLANNED,
                    AlgorithmRunStateV1.RUNNING,
                    AlgorithmRunStateV1.PAUSED,
                ),
                next_state=AlgorithmRunStateV1.CANCELLED,
                event_kind="PLAN_CANCELLED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code=require_text(reason_code, "algorithm cancel reason", max_length=128),
            )

    def release_due(self, *, observed_monotonic_ns: int) -> tuple[AlgorithmChildInstruction, ...]:
        """Expose due children for ordinary OMS admission, never direct dispatch."""

        require_nonnegative(observed_monotonic_ns, "algorithm release observed_monotonic_ns")
        with self._lock:
            if self._state is not AlgorithmRunStateV1.RUNNING:
                return ()
            released: list[AlgorithmChildInstruction] = []
            for child in self._plan.children:
                execution_id = child.instruction.execution_id
                if self._child_states[execution_id] is not AlgorithmChildStateV1.PENDING:
                    continue
                if _not_before_ns(child.instruction) > observed_monotonic_ns:
                    continue
                self._append_fact(
                    event_kind="CHILD_RELEASED",
                    observed_monotonic_ns=observed_monotonic_ns,
                    child_execution_id=execution_id,
                    detail_code="PENDING_TO_RELEASED",
                )
                self._child_states[execution_id] = AlgorithmChildStateV1.RELEASED
                released.append(child)
            return tuple(released)

    def mark_child_accepted(self, *, execution_id: str, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_child(
                execution_id=execution_id,
                expected=AlgorithmChildStateV1.RELEASED,
                next_state=AlgorithmChildStateV1.ACCEPTED,
                event_kind="CHILD_ACCEPTED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code="RELEASED_TO_ACCEPTED",
            )

    def mark_child_terminal(self, *, execution_id: str, observed_monotonic_ns: int) -> None:
        with self._lock:
            self._transition_child(
                execution_id=execution_id,
                expected=AlgorithmChildStateV1.ACCEPTED,
                next_state=AlgorithmChildStateV1.TERMINAL,
                event_kind="CHILD_TERMINAL",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code="ACCEPTED_TO_TERMINAL",
            )
            if all(
                state is AlgorithmChildStateV1.TERMINAL for state in self._child_states.values()
            ):
                self._transition_run(
                    expected=(AlgorithmRunStateV1.RUNNING, AlgorithmRunStateV1.PAUSED),
                    next_state=AlgorithmRunStateV1.COMPLETED,
                    event_kind="PLAN_COMPLETED",
                    observed_monotonic_ns=observed_monotonic_ns,
                    detail_code="ALL_CHILDREN_TERMINAL",
                )

    def mark_child_rejected(
        self, *, execution_id: str, reason_code: str, observed_monotonic_ns: int
    ) -> None:
        with self._lock:
            self._transition_child(
                execution_id=execution_id,
                expected=AlgorithmChildStateV1.RELEASED,
                next_state=AlgorithmChildStateV1.REJECTED,
                event_kind="CHILD_REJECTED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code=require_text(
                    reason_code, "algorithm child rejection reason", max_length=128
                ),
            )
            self._transition_run(
                expected=(AlgorithmRunStateV1.RUNNING, AlgorithmRunStateV1.PAUSED),
                next_state=AlgorithmRunStateV1.FAILED,
                event_kind="PLAN_FAILED",
                observed_monotonic_ns=observed_monotonic_ns,
                detail_code="CHILD_REJECTED",
            )

    def _transition_run(
        self,
        *,
        expected: tuple[AlgorithmRunStateV1, ...],
        next_state: AlgorithmRunStateV1,
        event_kind: str,
        observed_monotonic_ns: int,
        detail_code: str,
    ) -> None:
        require_nonnegative(observed_monotonic_ns, "algorithm run observed_monotonic_ns")
        if self._state not in expected:
            raise ExecutionAlgorithmError("algorithm run state transition is invalid")
        self._append_fact(
            event_kind=event_kind,
            observed_monotonic_ns=observed_monotonic_ns,
            child_execution_id=None,
            detail_code=detail_code,
        )
        self._state = next_state

    def _transition_child(
        self,
        *,
        execution_id: str,
        expected: AlgorithmChildStateV1,
        next_state: AlgorithmChildStateV1,
        event_kind: str,
        observed_monotonic_ns: int,
        detail_code: str,
    ) -> None:
        require_text(execution_id, "algorithm child execution_id", max_length=256)
        require_nonnegative(observed_monotonic_ns, "algorithm child observed_monotonic_ns")
        try:
            current = self._child_states[execution_id]
        except KeyError as exc:
            raise ExecutionAlgorithmError("algorithm child does not belong to this plan") from exc
        if current is not expected:
            raise ExecutionAlgorithmError("algorithm child state transition is invalid")
        self._append_fact(
            event_kind=event_kind,
            observed_monotonic_ns=observed_monotonic_ns,
            child_execution_id=execution_id,
            detail_code=detail_code,
        )
        self._child_states[execution_id] = next_state

    def _append_fact(
        self,
        *,
        event_kind: str,
        observed_monotonic_ns: int,
        child_execution_id: str | None,
        detail_code: str,
    ) -> None:
        fact = AlgorithmAuditFactV1(
            plan_id=self._plan_id,
            sequence=self._audit_sequence + 1,
            event_kind=event_kind,
            observed_monotonic_ns=observed_monotonic_ns,
            child_execution_id=child_execution_id,
            detail_code=detail_code,
        )
        self._audit_port.append_algorithm_audit(fact)
        self._audit_sequence = fact.sequence


def _max_quantity_slices(quantity: int, maximum: int) -> tuple[int, ...]:
    remaining = quantity
    result: list[int] = []
    while remaining:
        child_quantity = min(remaining, maximum)
        result.append(child_quantity)
        remaining -= child_quantity
    return tuple(result)


def _allocate_weighted(total: int, weights: tuple[int, ...]) -> tuple[int, ...]:
    if not weights or total < len(weights):
        raise ExecutionAlgorithmError("weighted execution plan lacks positive child allocation")
    weight_total = sum(weights)
    base = [total * weight // weight_total for weight in weights]
    remainder = total - sum(base)
    ranked = sorted(
        range(len(weights)),
        key=lambda index: (-(total * weights[index] % weight_total), index),
    )
    for index in ranked[:remainder]:
        base[index] += 1
    if any(quantity <= 0 for quantity in base):
        raise ExecutionAlgorithmError("weighted execution plan has an empty child")
    return tuple(base)


def _allocate_capped(total: int, capacities: tuple[int, ...]) -> tuple[int, ...]:
    if total <= 0 or not capacities or sum(capacities) < total:
        raise ExecutionAlgorithmError("capped execution plan has insufficient capacity")
    remaining = total
    result: list[int] = []
    for capacity in capacities:
        allocated = min(capacity, remaining)
        result.append(allocated)
        remaining -= allocated
    if remaining != 0:
        raise ExecutionAlgorithmError("capped execution plan cannot preserve parent quantity")
    return tuple(result)


def _make_plan(
    parent: ExecutionInstruction,
    *,
    algorithm_id: str,
    algorithm_version: int,
    quantities: tuple[int, ...],
    schedule_ns: tuple[int, ...],
    extra_parameters: Mapping[str, str],
    schedule_bucket_ids: tuple[str, ...] | None = None,
) -> AlgorithmPlan:
    if not quantities or len(quantities) != len(schedule_ns) or sum(quantities) != parent.quantity:
        raise ExecutionAlgorithmError("algorithm plan cannot preserve parent quantity")
    if schedule_bucket_ids is not None and len(schedule_bucket_ids) != len(quantities):
        raise ExecutionAlgorithmError("algorithm schedule bucket count is invalid")
    children: list[AlgorithmChildInstruction] = []
    for child_index, (quantity, not_before_ns) in enumerate(zip(quantities, schedule_ns), start=1):
        require_positive(quantity, "algorithm child quantity")
        require_nonnegative(not_before_ns, "algorithm child not_before_monotonic_ns")
        child_execution_id = f"{parent.execution_id}:child:{child_index}"
        if len(child_execution_id) > 256:
            raise ExecutionAlgorithmError("algorithm child execution_id exceeds 256 characters")
        parameters = dict(parent.execution_parameters)
        parameters.update(
            {
                "algorithm_id": algorithm_id,
                "algorithm_version": str(algorithm_version),
                "algorithm_parent_execution_id": parent.execution_id,
                "algorithm_child_index": str(child_index),
                _ALGORITHM_NOT_BEFORE_NS: str(not_before_ns),
            }
        )
        parameters.update(extra_parameters)
        if schedule_bucket_ids is not None:
            parameters["algorithm_schedule_bucket_id"] = schedule_bucket_ids[child_index - 1]
        child = replace(
            parent,
            execution_id=child_execution_id,
            quantity=quantity,
            execution_parameters=parameters,
        )
        children.append(AlgorithmChildInstruction(parent.execution_id, child_index, child))
    return AlgorithmPlan(
        algorithm_id=algorithm_id,
        algorithm_version=algorithm_version,
        parent_instruction=parent,
        children=tuple(children),
    )


def _not_before_ns(instruction: ExecutionInstruction) -> int:
    value = instruction.execution_parameters.get(_ALGORITHM_NOT_BEFORE_NS, "0")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ExecutionAlgorithmError("algorithm child schedule value is invalid") from exc
    return require_nonnegative(parsed, "algorithm child schedule value")


def _require_unique_bucket_ids(
    buckets: tuple[VolumeScheduleBucketV1, ...] | tuple[PovScheduleBucketV1, ...],
    algorithm_name: str,
) -> None:
    identifiers = tuple(bucket.bucket_id for bucket in buckets)
    if len(identifiers) != len(set(identifiers)):
        raise ExecutionAlgorithmError(f"{algorithm_name} schedule repeats bucket_id")
