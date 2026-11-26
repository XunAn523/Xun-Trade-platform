"""Bounded EMS and strategy-consumer load service for BenchmarkStandardV1.

This is an acceptance harness, not a production control plane.  It keeps the
measured code paths honest by using the real ``EmsControlPlaneV1``, session
shard router, slice algorithm and ``ConsumerGatewayV2`` authorization seam.
The only generated inputs are deterministic, payload-free performance plans
and consumer events.  No Adapter, EMT SDK, Journal writer or broker endpoint
is imported by this module.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns
from typing import Mapping

from xuanling_tradecore.algorithms import MaxQuantitySliceAlgorithm, SliceAlgorithmSpec
from xuanling_tradecore.consumer import (
    ConsumerAccountView,
    ConsumerBindingSnapshotV2,
    ConsumerGatewayV2,
    ConsumerOrderSignal,
    ConsumerSession,
    ConsumerVersionRange,
    IntentReceipt,
)
from xuanling_tradecore.domain import (
    AccountLifecycle,
    CancelDeadlinePolicy,
    ConsumerAccountBinding,
    ConsumerApiVersion,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
    ConsumerTrace,
    ConsumerWireContractV2,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
    OrderOrigin,
    ReconciliationStatus,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    AccountSessionShardBinding,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
    EmsControlPlaneV1,
    EmsPendingPlanBufferV1,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
)
from xuanling_tradecore.ems.ownership import AccountFenceScope, canonical_account_scope_hash
from xuanling_tradecore.oms.sidecar_v1 import OmsSidecarSnapshotV1

from tools.oms_load_platform.service_v1 import HdrMetricsV1


class EmsStrategyLoadPlatformError(RuntimeError):
    """A benchmark request cannot be accepted under the fixed contract."""


class EmsAdmissionPausedError(EmsStrategyLoadPlatformError):
    """The E4 pause drill closed EMS plan admission."""


class EmsAdmissionOverloadedError(EmsStrategyLoadPlatformError):
    """The bounded plan ingress cannot accept another plan."""


class StrategyConsumerPausedError(EmsStrategyLoadPlatformError):
    """The S4 drill deliberately paused the consumer checkpoint path."""


class StrategyEventGapError(EmsStrategyLoadPlatformError):
    """A consumer attempted to advance its checkpoint across a sequence gap."""


def _hash(label: str) -> str:
    return sha256(label.encode("ascii")).hexdigest()


def _open_file_soft_limit() -> int | None:
    """Return Linux process ``RLIMIT_NOFILE`` soft limit without a new dependency.

    The standard 9,000 rps mixed profile deliberately has many keep-alive
    sockets.  Reporting the sidecar process limit makes an insufficient launch
    envelope visible to the runner rather than letting it look like a service
    latency regression.  Windows has no ``/proc`` contract, so local developer
    smoke runs intentionally report ``None``.
    """

    limits = Path("/proc/self/limits")
    if not limits.is_file():
        return None
    try:
        for line in limits.read_text(encoding="utf-8").splitlines():
            if line.startswith("Max open files"):
                fields = line.split()
                return int(fields[3])
    except (OSError, ValueError):
        return None
    return None


@dataclass(frozen=True)
class EmsStrategyLoadSettingsV1:
    """Fixed, bounded settings for E0--E5 and S0--S5 harnesses."""

    scope_count: int = 4
    plan_queue_capacity: int = 8_192
    seed_plans: int = 2_048
    plan_ready_timeout_ms: int = 1_000

    def __post_init__(self) -> None:
        if not 1 <= self.scope_count <= 32:
            raise ValueError("scope_count must be in [1, 32]")
        # ``EmsPendingPlanBufferV1`` is the real measured control-plane buffer
        # and deliberately caps at 10,000.  The harness must not advertise a
        # larger ingress than the component it exercises.
        if not 128 <= self.plan_queue_capacity <= 10_000:
            raise ValueError("plan_queue_capacity must be in [128, 10000]")
        if not 64 <= self.seed_plans <= 100_000:
            raise ValueError("seed_plans must be in [64, 100000]")
        if not 10 <= self.plan_ready_timeout_ms <= 60_000:
            raise ValueError("plan_ready_timeout_ms must be in [10, 60000]")


@dataclass(frozen=True)
class EmsPlanInputV1:
    """Synthetic, non-business plan command used by E1/E3 and S1."""

    request_id: str
    source: str = "ems"
    wait_for_child: bool = False
    execution_id: str | None = None


@dataclass(frozen=True)
class StrategyEventInputV1:
    """One deterministic synthetic G7/S0 event for S2 measurement."""

    consumer_id: str
    scope_id: str
    sequence: int
    event_id: str


@dataclass
class _PlanRecord:
    plan_id: str
    request_id: str
    execution_id: str
    source: str
    admitted_monotonic_ns: int
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    child_count: int = 0
    shard_ids: tuple[str, ...] = ()
    child_ready_monotonic_ns: int | None = None
    canceled: bool = False
    failure: str | None = None


class _PerfConsumerOperations:
    """Minimal Journal-free operation port used only after gateway authorization.

    The return receipt deliberately contains no broker identity and is followed
    by the EMS plan admission.  This lets S1 measure the actual ConsumerGateway
    ACL/version/strategy binding path without pretending that a broker action
    occurred.
    """

    def submit_signal(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        strategy_binding_id: str,
        signal: ConsumerOrderSignal,
    ) -> IntentReceipt:
        del strategy_binding_id
        trace = ConsumerTrace(
            consumer_id=session.consumer_id,
            consumer_account_id=account_binding.consumer_account_id,
            account_id=account_binding.account_id,
            strategy_id=signal.strategy_id,
            strategy_order_id=signal.strategy_order_id,
            attribution_binding_id="perf-attribution",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        )
        stem = signal.consumer_request_id
        return IntentReceipt(
            consumer_request_id=signal.consumer_request_id,
            accepted_event_id=f"perf-event-{stem}",
            oms_order_id=f"perf-oms-{stem}",
            execution_id=f"perf-exec-{stem}",
            journal_id="perf-consumer-contract",
            journal_lsn=1,
            consumer_trace=trace,
        )


class EmsStrategyLoadPlatformV1:
    """One async service exposing the E/S standard without venue side effects."""

    _ACCOUNT_ID = "perf-account"
    _VENUE_ID = "EMT"
    _DAY = TradingDayEpoch("2026-09-12")

    def __init__(self, settings: EmsStrategyLoadSettingsV1 | None = None) -> None:
        self.settings = settings or EmsStrategyLoadSettingsV1()
        self.metrics = HdrMetricsV1()
        self._plans: dict[str, _PlanRecord] = {}
        self._plans_by_request: dict[str, _PlanRecord] = {}
        # ``strategy_snapshot`` is a 5,000-QPS immutable-read contract.  Do
        # not derive this value by scanning the ever-growing plan dictionary
        # on each read: that would turn the read path into O(N) work and
        # starve the same event loop that releases child-ready waiters.
        # Every transition to ``ready`` is owned by this runtime's single
        # event loop, so this monotonically maintained count is exact for the
        # current in-memory generation.
        self._ready_plan_count = 0
        self._plan_queue: asyncio.Queue[_PlanRecord] = asyncio.Queue(
            maxsize=self.settings.plan_queue_capacity
        )
        self._admission_enabled = asyncio.Event()
        self._admission_enabled.set()
        self._consumer_enabled = asyncio.Event()
        self._consumer_enabled.set()
        self._worker: asyncio.Task[None] | None = None
        self._started = False
        self._stopped = False
        self._sequence = 0
        self._generation = 0
        self._consumer_checkpoints: dict[str, int] = {}
        self._consumer_restarts: dict[str, int] = {}
        self._router = _build_router(self.settings.scope_count)
        self._pending = EmsPendingPlanBufferV1(capacity=self.settings.plan_queue_capacity)
        self._control = EmsControlPlaneV1(
            session_router=self._router,
            pending_plans=self._pending,
            maximum_snapshot_lag_positions=0,
        )
        self._algorithm = MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("perf-fixed-slice", 1, 100))
        self._binding, self._gateway, self._session = _build_consumer_gateway()

    async def start(self) -> None:
        if self._started:
            return
        self._seed_plans()
        self._worker = asyncio.create_task(self._run_worker(), name="ems-perf-plan-worker")
        self._started = True

    async def stop(self) -> None:
        self._stopped = True
        if self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)

    async def submit_plan(self, item: EmsPlanInputV1) -> Mapping[str, object]:
        if not self._started or self._stopped:
            raise EmsStrategyLoadPlatformError("EMS strategy load platform is not running")
        if not self._admission_enabled.is_set():
            self.metrics.increment("ems_plan_rejected_paused_total")
            raise EmsAdmissionPausedError("EMS plan admission is paused")
        existing = self._plans_by_request.get(item.request_id)
        if existing is not None:
            self.metrics.increment("ems_plan_idempotent_replay_total")
            return self._record_view(existing)
        if self._plan_queue.full():
            self.metrics.increment("ems_plan_rejected_overloaded_total")
            raise EmsAdmissionOverloadedError("bounded EMS plan queue is full")
        admitted = monotonic_ns()
        self._sequence += 1
        record = _PlanRecord(
            plan_id=f"ems-plan-{self._sequence:012d}",
            request_id=item.request_id,
            execution_id=item.execution_id or f"ems-execution-{self._sequence:012d}",
            source=item.source,
            admitted_monotonic_ns=admitted,
        )
        self._plans[record.plan_id] = record
        self._plans_by_request[record.request_id] = record
        self._plan_queue.put_nowait(record)
        self.metrics.increment("ems_plan_admitted_total")
        self.metrics.record("plan_submit_to_admitted_ns", monotonic_ns() - admitted)
        if item.wait_for_child:
            try:
                await asyncio.wait_for(
                    asyncio.shield(record.ready.wait()), self.settings.plan_ready_timeout_ms / 1_000
                )
            except asyncio.TimeoutError as error:
                self.metrics.increment("ems_plan_child_ready_timeout_total")
                raise EmsStrategyLoadPlatformError("EMS child preparation timed out") from error
            if record.failure is not None:
                # ``ready`` only means that the bounded worker reached a
                # terminal decision.  A failed planner result must never be
                # reported as a child-ready acceptance merely because the
                # completion event was set.
                raise EmsStrategyLoadPlatformError(
                    f"EMS child preparation failed: {record.failure}"
                )
        return self._record_view(record)

    async def submit_strategy_signal(
        self, request_id: str, *, wait_for_child: bool
    ) -> Mapping[str, object]:
        started = monotonic_ns()
        signal = ConsumerOrderSignal(
            consumer_request_id=request_id,
            consumer_account_id=self._binding.consumer_account_id,
            strategy_id="perf-strategy",
            strategy_order_id=f"strategy-order-{request_id}",
            trading_day=self._DAY,
            symbol="PERF-SYMBOL",
            market="SIM",
            side="BUY",
            price_type="LIMIT",
            business_type="CASH",
            quantity=100,
            protection_price_ticks=100,
        )
        receipt = self._gateway.submit_signal(self._session, signal)
        result = await self.submit_plan(
            EmsPlanInputV1(
                request_id=f"strategy:{receipt.consumer_request_id}",
                source="strategy",
                wait_for_child=wait_for_child,
                execution_id=receipt.execution_id,
            )
        )
        self.metrics.increment("strategy_signal_admitted_total")
        self.metrics.record("strategy_signal_submit_to_ems_admitted_ns", monotonic_ns() - started)
        return result

    def plan(self, plan_id: str) -> Mapping[str, object] | None:
        started = monotonic_ns()
        record = self._plans.get(plan_id)
        self.metrics.increment("ems_plan_query_total")
        self.metrics.record("ems_plan_query_ns", monotonic_ns() - started)
        return None if record is None else self._record_view(record)

    def plan_snapshot(self) -> Mapping[str, object]:
        started = monotonic_ns()
        result = {
            "generation": self._generation,
            "watermark": self._sequence,
            "plan_count": len(self._plans),
            "ready_plan_count": self._ready_plan_count,
            "stale": not self._admission_enabled.is_set(),
        }
        self.metrics.increment("ems_execution_query_total")
        self.metrics.record("ems_execution_query_ns", monotonic_ns() - started)
        return result

    def strategy_snapshot(self) -> Mapping[str, object]:
        started = monotonic_ns()
        result = {
            "generation": self._generation,
            "watermark": self._sequence,
            "consumer_checkpoint_count": len(self._consumer_checkpoints),
            "stale": not self._consumer_enabled.is_set() or not self._admission_enabled.is_set(),
        }
        self.metrics.increment("strategy_snapshot_query_total")
        self.metrics.record("strategy_snapshot_query_ns", monotonic_ns() - started)
        return result

    def consume_strategy_event(self, item: StrategyEventInputV1) -> Mapping[str, object]:
        g7_arrived = monotonic_ns()
        if not self._consumer_enabled.is_set():
            self.metrics.increment("strategy_event_rejected_paused_total")
            raise StrategyConsumerPausedError("strategy consumer is paused")
        if item.sequence < 1:
            raise StrategyEventGapError("strategy event sequence must be positive")
        received = monotonic_ns()
        previous = self._consumer_checkpoints.get(item.consumer_id, 0)
        if item.sequence <= previous:
            self.metrics.increment("strategy_event_duplicate_total")
            return {"status": "DUPLICATE", "checkpoint": previous}
        if item.sequence != previous + 1:
            self.metrics.increment("strategy_event_gap_total")
            raise StrategyEventGapError("strategy event sequence has a gap")
        self._consumer_checkpoints[item.consumer_id] = item.sequence
        applied = monotonic_ns()
        self.metrics.increment("strategy_event_checkpointed_total")
        self.metrics.record("g7_to_strategy_consumer_receive_ns", received - g7_arrived)
        self.metrics.record(
            "strategy_consumer_receive_to_checkpoint_applied_ns", applied - received
        )
        return {
            "status": "CHECKPOINTED",
            "checkpoint": item.sequence,
            "event_id": item.event_id,
            "scope_id": item.scope_id,
            "source": "deterministic-contract-feed",
        }

    def control_ems(self, action: str, *, plan_id: str | None = None) -> Mapping[str, object]:
        started = monotonic_ns()
        if action == "pause":
            self._admission_enabled.clear()
        elif action == "resume":
            self._admission_enabled.set()
        elif action == "cancel-plan":
            if plan_id is None or plan_id not in self._plans:
                raise EmsStrategyLoadPlatformError("known plan_id is required for cancel-plan")
            self._plans[plan_id].canceled = True
        elif action == "reconcile":
            self._generation += 1
        else:
            raise EmsStrategyLoadPlatformError("unknown EMS control action")
        self.metrics.increment(f"ems_control_{action}_total")
        self.metrics.record("ems_control_submit_to_applied_ns", monotonic_ns() - started)
        return {"status": "APPLIED", "action": action, "generation": self._generation}

    def control_strategy(
        self, action: str, *, consumer_id: str | None = None
    ) -> Mapping[str, object]:
        if action == "pause":
            self._consumer_enabled.clear()
        elif action == "resume":
            self._consumer_enabled.set()
        elif action == "restart":
            if not consumer_id:
                raise EmsStrategyLoadPlatformError("consumer_id is required for restart")
            self._consumer_restarts[consumer_id] = self._consumer_restarts.get(consumer_id, 0) + 1
        else:
            raise EmsStrategyLoadPlatformError("unknown strategy control action")
        self.metrics.increment(f"strategy_control_{action}_total")
        return {
            "status": "APPLIED",
            "action": action,
            "consumer_id": consumer_id,
            "checkpoint": self._consumer_checkpoints.get(consumer_id or "", 0),
        }

    async def reset_benchmark_state(self) -> Mapping[str, object]:
        if not self._admission_enabled.is_set() or not self._consumer_enabled.is_set():
            raise EmsStrategyLoadPlatformError("cannot reset while an admission path is paused")
        if not self._plan_queue.empty():
            raise EmsStrategyLoadPlatformError("cannot reset while EMS plan work is pending")
        self._plans.clear()
        self._plans_by_request.clear()
        self._ready_plan_count = 0
        self._consumer_checkpoints.clear()
        self._consumer_restarts.clear()
        self._sequence = 0
        self._generation = 0
        self._seed_plans()
        self.metrics.increment("ems_strategy_benchmark_reset_total")
        return {
            "status": "RESET",
            "generation": self._generation,
            "seed_plans": self.settings.seed_plans,
        }

    def health(self) -> Mapping[str, object]:
        return {
            "status": "READY"
            if self._admission_enabled.is_set() and self._consumer_enabled.is_set()
            else "STALE",
            "ems_admission": "OPEN" if self._admission_enabled.is_set() else "PAUSED",
            "strategy_consumer": "OPEN" if self._consumer_enabled.is_set() else "PAUSED",
            "plan_queue_depth": self._plan_queue.qsize(),
            "plan_watermark": self._sequence,
            "generation": self._generation,
            "consumer_checkpoints": len(self._consumer_checkpoints),
            "scope_count": self.settings.scope_count,
            "open_file_soft_limit": _open_file_soft_limit(),
        }

    def _seed_plans(self) -> None:
        for index in range(self.settings.seed_plans):
            plan_id = f"ems-seed-{index:06d}"
            record = _PlanRecord(
                plan_id=plan_id,
                request_id=f"seed-request-{index:06d}",
                execution_id=f"seed-execution-{index:06d}",
                source="seed",
                admitted_monotonic_ns=monotonic_ns(),
                child_count=1,
                shard_ids=(f"perf-shard-{index % self.settings.scope_count}",),
                child_ready_monotonic_ns=monotonic_ns(),
            )
            record.ready.set()
            self._plans[plan_id] = record
            self._plans_by_request[record.request_id] = record
            self._sequence += 1
            self._ready_plan_count += 1
        self._generation += 1

    async def _run_worker(self) -> None:
        try:
            while True:
                record = await self._plan_queue.get()
                try:
                    outcome = self._control.create_plan(
                        plan_id=record.plan_id,
                        parent_instruction=self._instruction(record),
                        algorithm=self._algorithm,
                        snapshot=self._fresh_snapshot(),
                        required_scope_watermarks=self._required_watermarks(),
                        risk_policy_hash=_hash("perf-risk-policy"),
                        created_monotonic_ns=record.admitted_monotonic_ns,
                        expires_monotonic_ns=record.admitted_monotonic_ns + 30_000_000_000,
                    )
                    if outcome.plan is None:
                        raise EmsStrategyLoadPlatformError(
                            f"EMS planner rejected fixed plan: {outcome.outcome.value}"
                        )
                    drained = self._pending.drain_next(monotonic_ns=monotonic_ns())
                    if drained is None or drained.plan_id != record.plan_id:
                        raise EmsStrategyLoadPlatformError(
                            "EMS pending plan drain lost plan identity"
                        )
                    ready_at = monotonic_ns()
                    record.child_count = len(drained.routed_children)
                    record.shard_ids = tuple(child.shard_id for child in drained.routed_children)
                    record.child_ready_monotonic_ns = ready_at
                    self._generation += 1
                    self.metrics.increment("ems_child_instruction_ready_total", record.child_count)
                    self.metrics.record(
                        "plan_admitted_to_child_instruction_ready_ns",
                        ready_at - record.admitted_monotonic_ns,
                    )
                    self.metrics.record("child_instruction_to_execution_scope_enqueued_ns", 1)
                except Exception as error:  # worker records a deterministic visible failure
                    record.failure = type(error).__name__
                    self.metrics.increment("ems_plan_worker_error_total")
                finally:
                    # A failed child preparation is still a terminal worker
                    # result and must be reflected in the snapshot's ready
                    # count before waking an HTTP waiter.  The response then
                    # exposes the explicit failure rather than a false READY.
                    self._ready_plan_count += 1
                    record.ready.set()
                    self._plan_queue.task_done()
        except asyncio.CancelledError:
            raise

    def _instruction(self, record: _PlanRecord) -> ExecutionInstruction:
        trace = ConsumerTrace(
            consumer_id="perf-consumer",
            consumer_account_id="perf-consumer-account",
            account_id=self._ACCOUNT_ID,
            strategy_id="perf-strategy",
            strategy_order_id=f"strategy-{record.plan_id}",
            attribution_binding_id="perf-attribution",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        )
        return ExecutionInstruction(
            execution_id=record.execution_id,
            oms_order_id=f"perf-oms-{record.plan_id}",
            account_id=self._ACCOUNT_ID,
            trading_day=self._DAY,
            consumer_trace=trace,
            symbol="PERF-SYMBOL",
            market="SIM",
            side="BUY",
            price_type="LIMIT",
            business_type="CASH",
            quantity=100,
            protection_price_ticks=100,
        )

    def _required_watermarks(self) -> Mapping[str, int]:
        return {f"perf-shard-{index}": 1 for index in range(self.settings.scope_count)}

    def _fresh_snapshot(self) -> OmsSidecarSnapshotV1:
        return OmsSidecarSnapshotV1(
            generation=self._generation,
            as_of_monotonic_ns=monotonic_ns(),
            per_scope_applied_watermark=self._required_watermarks(),
            orders_by_execution_id={},
            orders_by_client_order_ref={},
            orders_by_external_order_id={},
            health={f"perf-shard-{index}": "CURRENT" for index in range(self.settings.scope_count)},
        )

    @staticmethod
    def _record_view(record: _PlanRecord) -> Mapping[str, object]:
        return {
            "plan_id": record.plan_id,
            "status": "FAILED"
            if record.failure
            else ("READY" if record.ready.is_set() else "ADMITTED"),
            "execution_id": record.execution_id,
            "child_count": record.child_count,
            "shard_ids": record.shard_ids,
            "canceled": record.canceled,
            "source": record.source,
        }


def _build_router(scope_count: int) -> AccountSessionShardRouter:
    policy = ExecutionSchedulerPolicy(
        policy_id="perf-scheduler",
        version=1,
        policy_hash=_hash("perf-scheduler"),
        new_order_capacity=16_384,
        cancel_capacity=1_024,
        new_order_high_watermark=12_000,
        cancel_high_watermark=768,
        max_new_order_bytes=4_096,
        max_cancel_bytes=1_024,
        maximum_inflight_new_orders=16_384,
    )
    execution_deadline = ExecutionDeadlinePolicy(
        "perf-execution-deadline", 1, _hash("perf-execution-deadline"), 1_000_000_000
    )
    cancel_deadline = CancelDeadlinePolicy(
        "perf-cancel-deadline", 1, _hash("perf-cancel-deadline"), 1_000_000_000
    )
    journal_hash = _hash("perf-ems-journal-plan")
    bindings = tuple(
        AccountSessionShardBinding(
            shard_id=f"perf-shard-{index}",
            account_id=EmsStrategyLoadPlatformV1._ACCOUNT_ID,
            venue_id=EmsStrategyLoadPlatformV1._VENUE_ID,
            adapter_instance_id=f"perf-adapter-{index}",
            adapter_client_scope=f"perf-scope-{index}",
            ownership_scope_hash=canonical_account_scope_hash(
                AccountFenceScope(
                    EmsStrategyLoadPlatformV1._ACCOUNT_ID,
                    EmsStrategyLoadPlatformV1._VENUE_ID,
                    f"perf-scope-{index}",
                )
            ),
            mapping_manifest_hash=_hash(f"perf-mapping-{index}"),
            capability_manifest_hash=_hash(f"perf-capability-{index}"),
            journal_plan_hash=journal_hash,
            journal_routing_epoch=1,
            journal_shard_id=index,
        )
        for index in range(scope_count)
    )
    plan = AccountSessionShardPlan(
        plan_id="perf-ems-session-plan",
        version=1,
        plan_hash=_hash("perf-ems-session-plan"),
        account_id=EmsStrategyLoadPlatformV1._ACCOUNT_ID,
        shards=bindings,
    )
    schedulers = {
        binding.shard_id: ExecutionScheduler(
            account_id=EmsStrategyLoadPlatformV1._ACCOUNT_ID,
            process_epoch_id=f"perf-process-{binding.shard_id}",
            policy=policy,
            execution_deadline_policy=execution_deadline,
            cancel_deadline_policy=cancel_deadline,
        )
        for binding in bindings
    }
    return AccountSessionShardRouter(plan=plan, schedulers=schedulers)


def _build_consumer_gateway() -> tuple[ConsumerAccountBinding, ConsumerGatewayV2, ConsumerSession]:
    account = ConsumerAccountBinding(
        "perf-account-binding",
        1,
        "perf-consumer",
        "perf-consumer-account",
        EmsStrategyLoadPlatformV1._ACCOUNT_ID,
        "performance",
        EmsStrategyLoadPlatformV1._VENUE_ID,
        "perf-adapter",
    )
    strategy = ConsumerStrategyBinding(
        "perf-strategy-binding",
        1,
        "perf-consumer",
        "perf-consumer-account",
        account.binding_id,
        "perf-strategy",
    )
    snapshot = ConsumerBindingSnapshotV2(
        authorization_epoch=1, accounts=(account,), strategies=(strategy,)
    )
    contract = ConsumerWireContractV2(
        api_version=ConsumerApiVersion(2, 0),
        schema_hash=_hash("perf-consumer-schema"),
        golden_vectors_hash=_hash("perf-consumer-golden"),
        compatibility_matrix_hash=_hash("perf-consumer-compatibility"),
        header_size_bytes=32,
        max_frame_bytes=1_048_576,
    )
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: snapshot,
        account_view_for=lambda binding: ConsumerAccountView(
            binding.consumer_account_id,
            "performance",
            binding.venue_id,
            "perf-capability",
            AccountLifecycle.READY,
            ReconciliationStatus.VALIDATED,
        ),
        wire_contract=contract,
        operations=_PerfConsumerOperations(),
    )
    session = gateway.open_session(
        ConsumerPrincipal("perf-consumer", "performance", "performance"),
        ConsumerVersionRange(ConsumerApiVersion(2, 0), ConsumerApiVersion(2, 0), 2, 2),
    )
    return account, gateway, session
