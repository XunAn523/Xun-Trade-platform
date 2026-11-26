"""Bounded OMS-snapshot driven EMS planning that never dispatches an adapter.

The control plane produces immutable child instructions for later normal OMS
admission.  It deliberately has no ``submit``, ``cancel`` or SDK dependency:
all generated instructions still cross the existing account fence, risk gate,
Journal-first boundary and native route verification when a caller chooses to
release them.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from types import MappingProxyType
from typing import Mapping

from ..algorithms import AlgorithmPlan, ExecutionAlgorithm
from ..domain import ExecutionInstruction
from ..oms.sidecar_v1 import OmsSidecarSnapshotV1
from .session_sharding import AccountSessionShardRouter


class EmsControlPlaneError(RuntimeError):
    """A plan cannot safely be generated, retained or routed."""


class EmsPlanAdmissionOutcome(str, Enum):
    """Explicit bounded outcomes for future instruction generation only."""

    ACCEPTED = "ACCEPTED"
    SNAPSHOT_STALE = "EMS_SNAPSHOT_STALE"
    RECONCILIATION_REQUIRED = "EMS_RECONCILIATION_REQUIRED"
    PENDING_FULL = "EMS_PENDING_FULL"
    DUPLICATE = "EMS_DUPLICATE_PLAN"


@dataclass(frozen=True)
class EmsRoutedChildInstructionV1:
    """A frozen child instruction plus its selected, attested execution scope."""

    instruction: ExecutionInstruction
    shard_id: str
    adapter_client_scope: str
    ownership_scope_hash: str
    route_version: int


@dataclass(frozen=True)
class EmsChildOrderPlanV1:
    """A bounded plan that is not yet a dispatch record or authorization."""

    plan_id: str
    source_snapshot_generation: int
    source_scope_watermarks: Mapping[str, int]
    risk_policy_hash: str
    created_monotonic_ns: int
    expires_monotonic_ns: int
    algorithm_plan: AlgorithmPlan
    routed_children: tuple[EmsRoutedChildInstructionV1, ...]

    def __post_init__(self) -> None:
        if not self.plan_id:
            raise EmsControlPlaneError("EMS plan id is invalid")
        if self.source_snapshot_generation < 0 or self.created_monotonic_ns < 0:
            raise EmsControlPlaneError("EMS plan snapshot generation or creation time is invalid")
        if self.expires_monotonic_ns <= self.created_monotonic_ns:
            raise EmsControlPlaneError("EMS plan expiry is invalid")
        if len(self.risk_policy_hash) != 64 or any(
            char not in "0123456789abcdef" for char in self.risk_policy_hash
        ):
            raise EmsControlPlaneError("EMS plan risk policy hash is invalid")
        watermarks = dict(self.source_scope_watermarks)
        if not watermarks or any(
            not scope or position < 0 for scope, position in watermarks.items()
        ):
            raise EmsControlPlaneError("EMS plan source watermark is invalid")
        children = tuple(self.routed_children)
        expected_ids = tuple(
            child.instruction.execution_id for child in self.algorithm_plan.children
        )
        actual_ids = tuple(child.instruction.execution_id for child in children)
        if actual_ids != expected_ids or len(actual_ids) != len(set(actual_ids)):
            raise EmsControlPlaneError(
                "EMS routed children do not match the immutable algorithm plan"
            )
        if any(not child.shard_id or not child.adapter_client_scope for child in children):
            raise EmsControlPlaneError("EMS routed child scope is invalid")
        if any(len(child.ownership_scope_hash) != 64 for child in children):
            raise EmsControlPlaneError("EMS routed child ownership hash is invalid")
        object.__setattr__(self, "source_scope_watermarks", MappingProxyType(watermarks))
        object.__setattr__(self, "routed_children", children)

    def is_expired_at(self, monotonic_ns: int) -> bool:
        if monotonic_ns < 0:
            raise EmsControlPlaneError("EMS plan comparison time is invalid")
        return monotonic_ns >= self.expires_monotonic_ns


@dataclass(frozen=True)
class EmsPlanAdmissionV1:
    """The result of generating a future EMS plan; no side effect has occurred."""

    outcome: EmsPlanAdmissionOutcome
    plan: EmsChildOrderPlanV1 | None

    def __post_init__(self) -> None:
        if self.outcome is EmsPlanAdmissionOutcome.ACCEPTED and self.plan is None:
            raise EmsControlPlaneError("accepted EMS plan admission requires a plan")
        if self.outcome is not EmsPlanAdmissionOutcome.ACCEPTED and self.plan is not None:
            raise EmsControlPlaneError("rejected EMS plan admission cannot expose a plan")


class EmsPendingPlanBufferV1:
    """A bounded, control-plane-only buffer that never holds an adapter lock."""

    def __init__(self, *, capacity: int) -> None:
        if not 1 <= capacity <= 10_000:
            raise EmsControlPlaneError("EMS pending plan capacity is invalid")
        self._capacity = capacity
        self._lock = Lock()
        self._plans: deque[EmsChildOrderPlanV1] = deque()
        self._plan_ids: set[str] = set()

    def offer(self, plan: EmsChildOrderPlanV1) -> EmsPlanAdmissionOutcome:
        with self._lock:
            if plan.plan_id in self._plan_ids:
                return EmsPlanAdmissionOutcome.DUPLICATE
            if len(self._plans) >= self._capacity:
                return EmsPlanAdmissionOutcome.PENDING_FULL
            self._plans.append(plan)
            self._plan_ids.add(plan.plan_id)
            return EmsPlanAdmissionOutcome.ACCEPTED

    def drain_next(self, *, monotonic_ns: int) -> EmsChildOrderPlanV1 | None:
        """Return one unexpired plan for the caller to submit through normal OMS."""

        with self._lock:
            while self._plans:
                plan = self._plans.popleft()
                self._plan_ids.remove(plan.plan_id)
                if not plan.is_expired_at(monotonic_ns):
                    return plan
            return None

    def depth(self) -> int:
        with self._lock:
            return len(self._plans)


class EmsControlPlaneV1:
    """Derive and route future child instructions from a verified OMS snapshot."""

    def __init__(
        self,
        *,
        session_router: AccountSessionShardRouter,
        pending_plans: EmsPendingPlanBufferV1,
        maximum_snapshot_lag_positions: int = 0,
        routing_workers: int = 1,
    ) -> None:
        if maximum_snapshot_lag_positions < 0:
            raise EmsControlPlaneError("EMS maximum snapshot lag is invalid")
        if type(routing_workers) is not int or not 1 <= routing_workers <= 32:
            raise EmsControlPlaneError("EMS routing workers must be in [1, 32]")
        self._router = session_router
        self._pending_plans = pending_plans
        self._maximum_snapshot_lag_positions = maximum_snapshot_lag_positions
        self._routing_workers = routing_workers

    def create_plan(
        self,
        *,
        plan_id: str,
        parent_instruction: ExecutionInstruction,
        algorithm: ExecutionAlgorithm,
        snapshot: OmsSidecarSnapshotV1,
        required_scope_watermarks: Mapping[str, int],
        risk_policy_hash: str,
        created_monotonic_ns: int,
        expires_monotonic_ns: int,
    ) -> EmsPlanAdmissionV1:
        """Plan only when the immutable snapshot is fresh and conflict-free."""

        if not snapshot.is_authoritative:
            return EmsPlanAdmissionV1(EmsPlanAdmissionOutcome.RECONCILIATION_REQUIRED, None)
        if not _watermarks_satisfy(
            snapshot.per_scope_applied_watermark,
            required_scope_watermarks,
            self._maximum_snapshot_lag_positions,
        ):
            return EmsPlanAdmissionV1(EmsPlanAdmissionOutcome.SNAPSHOT_STALE, None)
        algorithm_plan = algorithm.plan(parent_instruction)
        instructions = tuple(child.instruction for child in algorithm_plan.children)
        # Routing is computation only: no child has been admitted or dispatched.
        lanes = self._router.route_executions(instructions, workers=self._routing_workers)
        routed: list[EmsRoutedChildInstructionV1] = []
        for child, lane in zip(algorithm_plan.children, lanes, strict=True):
            binding = lane.binding
            routed.append(
                EmsRoutedChildInstructionV1(
                    instruction=child.instruction,
                    shard_id=binding.shard_id,
                    adapter_client_scope=binding.adapter_client_scope,
                    ownership_scope_hash=binding.ownership_scope_hash,
                    route_version=binding.journal_routing_epoch,
                )
            )
        plan = EmsChildOrderPlanV1(
            plan_id=plan_id,
            source_snapshot_generation=snapshot.generation,
            source_scope_watermarks=snapshot.per_scope_applied_watermark,
            risk_policy_hash=risk_policy_hash,
            created_monotonic_ns=created_monotonic_ns,
            expires_monotonic_ns=expires_monotonic_ns,
            algorithm_plan=algorithm_plan,
            routed_children=tuple(routed),
        )
        outcome = self._pending_plans.offer(plan)
        return EmsPlanAdmissionV1(
            outcome, plan if outcome is EmsPlanAdmissionOutcome.ACCEPTED else None
        )


def _watermarks_satisfy(
    actual: Mapping[str, int], required: Mapping[str, int], maximum_lag: int
) -> bool:
    if not required:
        return False
    for scope, required_position in required.items():
        if not scope or required_position < 0:
            return False
        if actual.get(scope, -1) + maximum_lag < required_position:
            return False
    return True
