"""Tests for F2's Guard -> static route -> scheduler -> Adapter composition."""

from __future__ import annotations

import unittest

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RuntimeVenueProfile,
    SubmitAccepted,
)
from xuanling_tradecore.adapters.emt import EmtAdapterProfile, load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.domain import (
    BrokerEvent,
    CallbackGapEvidence,
    CancelDeadlinePolicy,
    CancelRequest,
    ConsumerTrace,
    ExecutionDeadlinePolicy,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountExecutionRoutePlan,
    AccountExecutionRouter,
    AccountFenceScope,
    ExecutionDispatchOutcome,
    ExecutionGuard,
    ExecutionGuardAdmissionGate,
    ExecutionGuardPolicy,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    ExecutionSessionRuntime,
    ExecutionSessionRuntimeError,
    GuardMarketSnapshot,
    IngressAdmission,
    KillSwitchSnapshot,
    OwnershipFenceProof,
    SessionSupervisor,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
DAY = TradingDayEpoch("2026-09-09")


class _Observer:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


class _Audit:
    def __init__(self) -> None:
        self.rejections: list[str] = []

    def record_execution_rejected(self, instruction, decision) -> None:
        self.rejections.append(f"{instruction.execution_id}:{decision.code.value}")


class _Fence:
    def __init__(self, scope_hash: str) -> None:
        self.scope_hash = scope_hash

    def require_held(self) -> OwnershipFenceProof:
        return OwnershipFenceProof(
            scope_hash=self.scope_hash,
            instance_id="tradecore-single-host-1",
            owner_pid=1,
            acquired_monotonic_ns=1,
        )


class _Adapter:
    def __init__(self, mapping) -> None:
        self._mapping = mapping
        self.observer = None
        self.started = False
        self.submit_calls: list[ExecutionInstruction] = []
        self.cancel_calls: list[CancelExecutionInstruction] = []
        self.query_calls: list[BrokerQuery] = []
        self.drain_calls: list[float] = []

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        return "emt-sim-instance-1"

    @property
    def capabilities(self):
        return self._mapping.runtime.capabilities

    def start(self, observer) -> None:
        self.observer = observer
        self.started = True

    def stop(self) -> None:
        self.started = False

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        self.submit_calls.append(instruction)
        return SubmitAccepted(
            execution_id=instruction.execution_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=10,
            venue_order_identity=_identity(client_order_ref="17", venue_order_id="91"),
        )

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        self.cancel_calls.append(instruction)
        return CancelAccepted(
            cancel_request_id=instruction.cancel_request.cancel_request_id,
            adapter_id=self.adapter_id,
            adapter_instance_id=self.adapter_instance_id,
            accepted_monotonic_ns=11,
            venue_order_identity=_identity(
                client_order_ref="17",
                venue_order_id="91",
                venue_cancel_id="92",
            ),
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        self.query_calls.append(request)
        return QueryHandle(request.request_id, self.adapter_id, self.adapter_instance_id)

    def drain_broker_events(self, *, wait_timeout_seconds: float = 0.0) -> int:
        self.drain_calls.append(wait_timeout_seconds)
        return 0

    def begin_recovery(self, plan, observer):  # pragma: no cover - protocol completeness.
        raise AssertionError((plan, observer))

    def collect_recovery_evidence(self, handle):  # pragma: no cover - protocol completeness.
        raise AssertionError(handle)


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _binding(mapping) -> AccountExecutionBinding:
    return AccountExecutionBinding(
        binding_id="account-1-emt-v1",
        binding_version=1,
        account_id="account-1",
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
        ownership_scope_hash=AccountFenceScope("account-1", "EMT", "7").scope_hash,
        mapping_manifest_hash=mapping.manifest.manifest_hash,
        capability_manifest_hash=SHA_A,
        journal_plan_hash=SHA_B,
        journal_routing_epoch=1,
        journal_shard_id=0,
    )


def _scheduler() -> ExecutionScheduler:
    return ExecutionScheduler(
        account_id="account-1",
        process_epoch_id="f2-test-process",
        policy=ExecutionSchedulerPolicy(
            policy_id="f2-scheduler",
            version=1,
            policy_hash=SHA_A,
            new_order_capacity=4,
            cancel_capacity=4,
            new_order_high_watermark=3,
            cancel_high_watermark=3,
            max_new_order_bytes=1024,
            max_cancel_bytes=512,
            maximum_inflight_new_orders=4,
        ),
        execution_deadline_policy=ExecutionDeadlinePolicy(
            policy_id="f2-execution-deadline",
            version=1,
            policy_hash=SHA_B,
            native_start_budget_ns=100,
        ),
        cancel_deadline_policy=CancelDeadlinePolicy(
            policy_id="f2-cancel-deadline",
            version=1,
            policy_hash=SHA_C,
            native_start_budget_ns=100,
        ),
    )


def _guard(audit: _Audit) -> ExecutionGuardAdmissionGate:
    return ExecutionGuardAdmissionGate(
        ExecutionGuard(
            ExecutionGuardPolicy(
                policy_id="f2-guard",
                version=1,
                policy_hash=SHA_C,
                account_id="account-1",
                frequency_window_ns=1_000,
                account_order_limit=4,
                strategy_order_limits={"strategy-1": 4},
                symbol_order_limits={"000001": 4},
                side_order_limits={"BUY": 4, "SELL": 4},
                max_price_deviation_bps=1_000,
                max_order_quantity=100,
                max_order_notional_ticks=100_000,
            )
        ),
        audit,
    )


def _trace(*, strategy_order_id: str) -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="account-1",
        strategy_id="strategy-1",
        strategy_order_id=strategy_order_id,
        attribution_binding_id="binding-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _instruction(*, key: str = "1", quantity: int = 100) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"execution-{key}",
        oms_order_id=f"oms-{key}",
        account_id="account-1",
        trading_day=DAY,
        consumer_trace=_trace(strategy_order_id=f"strategy-order-{key}"),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=quantity,
        protection_price_ticks=100,
        limit_price_ticks=100,
    )


def _identity(
    *,
    client_order_ref: str,
    venue_order_id: str,
    venue_cancel_id: str | None = None,
) -> VenueOrderIdentity:
    return VenueOrderIdentity(
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        adapter_client_scope="7",
        client_order_ref=client_order_ref,
        venue_order_id=venue_order_id,
        venue_cancel_id=venue_cancel_id,
    )


def _market() -> GuardMarketSnapshot:
    return GuardMarketSnapshot(
        snapshot_id="market-1",
        symbol="000001",
        market="SZ",
        reference_price_ticks=100,
        minimum_price_ticks=90,
        maximum_price_ticks=110,
        captured_monotonic_ns=0,
        expires_monotonic_ns=1_000,
    )


class ExecutionSessionRuntimeTests(unittest.TestCase):
    def _runtime(self):
        mapping = _mapping()
        binding = _binding(mapping)
        scheduler = _scheduler()
        adapter = _Adapter(mapping)
        supervisor = SessionSupervisor(
            binding=binding,
            activated_mapping=mapping,
            ownership_fence=_Fence(binding.ownership_scope_hash),
            adapter_factory=lambda: adapter,
        )
        router = AccountExecutionRouter(
            plan=AccountExecutionRoutePlan(
                plan_id="f2-route",
                version=1,
                plan_hash=SHA_C,
                bindings=(binding,),
            ),
            schedulers={"account-1": scheduler},
        )
        audit = _Audit()
        runtime = ExecutionSessionRuntime(
            binding=binding,
            router=router,
            scheduler=scheduler,
            supervisor=supervisor,
            guard_admission=_guard(audit),
        )
        observer = _Observer()
        runtime.start(observer, evaluated_wall_ns=1)
        return runtime, adapter, audit, observer

    def test_guard_static_route_scheduler_and_single_dispatcher_form_one_chain(self) -> None:
        runtime, adapter, audit, _observer = self._runtime()
        admission = runtime.admit_submit(
            _instruction(),
            serialized_size_bytes=128,
            market=_market(),
            kill_switches=KillSwitchSnapshot(control_version=0),
            admitted_monotonic_ns=10,
        )
        self.assertTrue(admission.decision.accepted)
        self.assertIsInstance(admission.authorized_result, IngressAdmission)
        self.assertEqual([], adapter.submit_calls)

        dispatched = runtime.dispatch_once(checked_monotonic_ns=11)
        assert dispatched is not None
        self.assertEqual(ExecutionDispatchOutcome.SUBMIT_ACCEPTED, dispatched.outcome)
        self.assertEqual(["execution-1"], [item.execution_id for item in adapter.submit_calls])
        self.assertEqual([], audit.rejections)
        self.assertTrue(runtime.release_new_order_inflight_after_terminal("execution-1"))
        runtime.stop()

    def test_cancel_has_exact_identity_and_preempts_an_queued_new_order(self) -> None:
        runtime, adapter, _audit, _observer = self._runtime()
        runtime.admit_submit(
            _instruction(key="submit"),
            serialized_size_bytes=128,
            market=_market(),
            kill_switches=KillSwitchSnapshot(control_version=0),
            admitted_monotonic_ns=10,
        )
        cancel = CancelExecutionInstruction(
            cancel_request=CancelRequest(
                cancel_request_id="cancel-1",
                oms_order_id="oms-submit",
                execution_id="execution-submit",
                account_id="account-1",
                consumer_trace=_trace(strategy_order_id="strategy-order-submit"),
                reason="controlled cleanup",
                requested_monotonic_ns=10,
            ),
            venue_order_identity=_identity(client_order_ref="17", venue_order_id="91"),
        )
        self.assertIsInstance(
            runtime.admit_cancel(cancel, serialized_size_bytes=64, admitted_monotonic_ns=11),
            IngressAdmission,
        )
        first = runtime.dispatch_once(checked_monotonic_ns=12)
        assert first is not None
        self.assertEqual(ExecutionDispatchOutcome.CANCEL_ACCEPTED, first.outcome)
        self.assertEqual(
            ["cancel-1"], [item.cancel_request.cancel_request_id for item in adapter.cancel_calls]
        )
        second = runtime.dispatch_once(checked_monotonic_ns=13)
        assert second is not None
        self.assertEqual(ExecutionDispatchOutcome.SUBMIT_ACCEPTED, second.outcome)
        runtime.stop()

    def test_rejection_and_callback_gap_never_reach_the_native_adapter(self) -> None:
        runtime, adapter, audit, _observer = self._runtime()
        rejected = runtime.admit_submit(
            _instruction(quantity=101),
            serialized_size_bytes=128,
            market=_market(),
            kill_switches=KillSwitchSnapshot(control_version=0),
            admitted_monotonic_ns=10,
        )
        self.assertFalse(rejected.decision.accepted)
        self.assertIsNone(rejected.authorized_result)
        self.assertEqual(["execution-1:ORDER_QUANTITY_LIMIT_EXCEEDED"], audit.rejections)

        runtime.admit_submit(
            _instruction(key="queued"),
            serialized_size_bytes=128,
            market=_market(),
            kill_switches=KillSwitchSnapshot(control_version=0),
            admitted_monotonic_ns=11,
        )
        assert adapter.observer is not None
        adapter.observer.on_callback_gap(
            CallbackGapEvidence(
                evidence_id="gap-1",
                account_id="account-1",
                producer_id="test",
                first_rejected_sequence=1,
                last_rejected_sequence=1,
                rejected_count=1,
                observed_monotonic_ns=12,
            )
        )
        blocked = runtime.dispatch_once(checked_monotonic_ns=13)
        assert blocked is not None
        self.assertEqual(ExecutionDispatchOutcome.BLOCKED_DISARMED, blocked.outcome)
        self.assertEqual([], adapter.submit_calls)
        self.assertEqual(1, runtime.snapshot().callback_gap_count)
        with self.assertRaisesRegex(ExecutionSessionRuntimeError, "admission is closed"):
            runtime.admit_submit(
                _instruction(key="after-gap"),
                serialized_size_bytes=128,
                market=_market(),
                kill_switches=KillSwitchSnapshot(control_version=0),
                admitted_monotonic_ns=14,
            )
        runtime.stop()

    def test_host_health_close_blocks_mutations_but_keeps_scoped_queries_available(self) -> None:
        runtime, adapter, _audit, _observer = self._runtime()
        runtime.on_host_health_integrity("scope-1", "sdk-abi-drift")
        with self.assertRaisesRegex(ExecutionSessionRuntimeError, "admission is closed"):
            runtime.admit_submit(
                _instruction(),
                serialized_size_bytes=128,
                market=_market(),
                kill_switches=KillSwitchSnapshot(control_version=0),
                admitted_monotonic_ns=10,
            )
        handle = runtime.query(
            BrokerQuery(
                request_id="query-1",
                account_id="account-1",
                venue_id="EMT",
                adapter_instance_id="emt-sim-instance-1",
                query_kind="unfinished_orders",
                page_size=10,
            )
        )
        self.assertEqual("query-1", handle.request_id)
        self.assertEqual(["query-1"], [item.request_id for item in adapter.query_calls])
        runtime.stop()

    def test_callback_drain_stays_inside_the_started_runtime_composition(self) -> None:
        runtime, adapter, _audit, _observer = self._runtime()
        self.assertEqual(0, runtime.drain_broker_events(wait_timeout_seconds=0.25))
        self.assertEqual([0.25], adapter.drain_calls)
        runtime.stop()


if __name__ == "__main__":
    unittest.main()
