"""F6 EMT-only multi-account runtime isolation contracts."""

from __future__ import annotations

import unittest

from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
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
    AccountFenceScope,
    AccountSessionShardBinding,
    AccountSessionShardingError,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
)
from xuanling_tradecore.runtime import (
    EmtAccountRuntimeRegistrationV2,
    EmtAccountRuntimeStateV2,
    EmtMultiAccountRuntimeErrorV2,
    EmtMultiAccountRuntimeSupervisorV2,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


class _Runtime:
    def __init__(self, *, fail_start: bool = False, fail_close: bool = False) -> None:
        self.fail_start = fail_start
        self.fail_close = fail_close
        self.starts = 0
        self.stops = 0
        self.close_reasons: list[str] = []

    def start_disarmed(self) -> None:
        self.starts += 1
        if self.fail_start:
            raise OSError("start failure")

    def stop_disarmed(self) -> None:
        self.stops += 1

    def close_admission(self, *, reason: str) -> None:
        self.close_reasons.append(reason)
        if self.fail_close:
            raise OSError("close failure")


def _scheduler(account_id: str) -> ExecutionScheduler:
    return ExecutionScheduler(
        account_id=account_id,
        process_epoch_id=f"boot:{account_id}",
        policy=ExecutionSchedulerPolicy(
            policy_id="scheduler-policy",
            version=1,
            policy_hash=SHA_A,
            new_order_capacity=8,
            cancel_capacity=4,
            new_order_high_watermark=6,
            cancel_high_watermark=3,
            max_new_order_bytes=1_024,
            max_cancel_bytes=1_024,
        ),
        execution_deadline_policy=ExecutionDeadlinePolicy("execution-deadline", 1, SHA_B, 100),
        cancel_deadline_policy=CancelDeadlinePolicy("cancel-deadline", 1, SHA_C, 100),
    )


def _registration(
    *,
    account_id: str,
    scope: str,
    consumer_account_ids: frozenset[str],
    runtime: _Runtime,
) -> EmtAccountRuntimeRegistrationV2:
    binding = AccountSessionShardBinding(
        shard_id=f"shard-{account_id}",
        account_id=account_id,
        venue_id="EMT",
        adapter_instance_id="emt-adapter-v1",
        adapter_client_scope=scope,
        ownership_scope_hash=AccountFenceScope(account_id, "EMT", scope).scope_hash,
        mapping_manifest_hash=SHA_A,
        capability_manifest_hash=SHA_B,
        journal_plan_hash=SHA_C,
        journal_routing_epoch=1,
        journal_shard_id=0,
    )
    plan = AccountSessionShardPlan(
        plan_id=f"plan-{account_id}",
        version=1,
        plan_hash=SHA_D,
        account_id=account_id,
        shards=(binding,),
    )
    router = AccountSessionShardRouter(
        plan=plan, schedulers={binding.shard_id: _scheduler(account_id)}
    )
    return EmtAccountRuntimeRegistrationV2(
        account_id=account_id,
        consumer_account_ids=consumer_account_ids,
        shard_plan=plan,
        router=router,
        runtime=runtime,
    )


def _trace(account_id: str, consumer_account_id: str) -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id=f"consumer-{consumer_account_id}",
        consumer_account_id=consumer_account_id,
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id="strategy-order-1",
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _instruction(account_id: str, consumer_account_id: str) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"execution-{account_id}",
        oms_order_id=f"oms-{account_id}",
        account_id=account_id,
        trading_day=TradingDayEpoch("2026-09-10"),
        consumer_trace=_trace(account_id, consumer_account_id),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=101,
        limit_price_ticks=100,
    )


class EmtMultiAccountRuntimeTests(unittest.TestCase):
    def test_start_and_failure_are_strictly_account_local(self) -> None:
        first_runtime, second_runtime = _Runtime(fail_start=True), _Runtime()
        first = _registration(
            account_id="account-a",
            scope="scope-a",
            consumer_account_ids=frozenset({"consumer-account-a"}),
            runtime=first_runtime,
        )
        second = _registration(
            account_id="account-b",
            scope="scope-b",
            consumer_account_ids=frozenset({"consumer-account-b"}),
            runtime=second_runtime,
        )
        supervisor = EmtMultiAccountRuntimeSupervisorV2({"account-a": first, "account-b": second})
        with self.assertRaisesRegex(EmtMultiAccountRuntimeErrorV2, "remains DISARMED"):
            supervisor.start_account("account-a")
        self.assertEqual(
            EmtAccountRuntimeStateV2.FAILED_DISARMED, supervisor.snapshot("account-a").state
        )
        self.assertEqual(
            EmtAccountRuntimeStateV2.REGISTERED_DISARMED, supervisor.snapshot("account-b").state
        )
        self.assertEqual(
            EmtAccountRuntimeStateV2.RUNNING_DISARMED, supervisor.start_account("account-b").state
        )
        self.assertEqual([], first_runtime.close_reasons)
        self.assertEqual([], second_runtime.close_reasons)

    def test_consumer_and_native_scope_cannot_cross_accounts(self) -> None:
        runtime_a, runtime_b = _Runtime(), _Runtime()
        first = _registration(
            account_id="account-a",
            scope="scope-a",
            consumer_account_ids=frozenset({"consumer-account-a"}),
            runtime=runtime_a,
        )
        second = _registration(
            account_id="account-b",
            scope="scope-b",
            consumer_account_ids=frozenset({"consumer-account-b"}),
            runtime=runtime_b,
        )
        supervisor = EmtMultiAccountRuntimeSupervisorV2({"account-a": first, "account-b": second})
        supervisor.start_account("account-a")
        supervisor.start_account("account-b")
        self.assertEqual(
            "scope-a",
            supervisor.route_execution(
                _instruction("account-a", "consumer-account-a")
            ).binding.adapter_client_scope,
        )
        with self.assertRaisesRegex(EmtMultiAccountRuntimeErrorV2, "not authorized"):
            supervisor.route_execution(_instruction("account-a", "consumer-account-b"))
        cancel = CancelRequest(
            cancel_request_id="cancel-1",
            oms_order_id="oms-account-a",
            execution_id="execution-account-a",
            account_id="account-a",
            consumer_trace=_trace("account-a", "consumer-account-a"),
            reason="operator",
            requested_monotonic_ns=1,
        )
        with self.assertRaisesRegex(AccountSessionShardingError, "native identity"):
            supervisor.route_cancel(
                cancel,
                VenueOrderIdentity(
                    venue_id="EMT",
                    adapter_instance_id="emt-adapter-v1",
                    adapter_client_scope="scope-b",
                    venue_order_id="native-1",
                ),
            )

    def test_scope_reuse_and_host_failure_fail_closed_without_failover(self) -> None:
        first = _registration(
            account_id="account-a",
            scope="shared-scope",
            consumer_account_ids=frozenset({"consumer-account-a"}),
            runtime=_Runtime(),
        )
        second = _registration(
            account_id="account-b",
            scope="shared-scope",
            consumer_account_ids=frozenset({"consumer-account-b"}),
            runtime=_Runtime(fail_close=True),
        )
        with self.assertRaisesRegex(EmtMultiAccountRuntimeErrorV2, "share one native"):
            EmtMultiAccountRuntimeSupervisorV2({"account-a": first, "account-b": second})
        second = _registration(
            account_id="account-b",
            scope="scope-b",
            consumer_account_ids=frozenset({"consumer-account-b"}),
            runtime=_Runtime(fail_close=True),
        )
        supervisor = EmtMultiAccountRuntimeSupervisorV2({"account-a": first, "account-b": second})
        supervisor.start_account("account-a")
        supervisor.start_account("account-b")
        failed = supervisor.report_account_failure(
            account_id="account-b", reason_code="HOST_HEALTH"
        )
        self.assertEqual(EmtAccountRuntimeStateV2.FAILED_DISARMED, failed.state)
        self.assertEqual("close-admission:OSError", failed.last_error_code)
        self.assertEqual(
            EmtAccountRuntimeStateV2.RUNNING_DISARMED, supervisor.snapshot("account-a").state
        )

    def test_event_routes_only_when_its_account_is_running(self) -> None:
        runtime = _Runtime()
        registration = _registration(
            account_id="account-a",
            scope="scope-a",
            consumer_account_ids=frozenset({"consumer-account-a"}),
            runtime=runtime,
        )
        supervisor = EmtMultiAccountRuntimeSupervisorV2({"account-a": registration})
        event = BrokerEvent(
            event_id="event-1",
            schema_version=1,
            source_key_schema_version=1,
            source_key="source-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id="emt-adapter-v1",
            account_id="account-a",
            trading_day=TradingDayEpoch("2026-09-10"),
            received_monotonic_ns=1,
            venue_order_identity=VenueOrderIdentity(
                venue_id="EMT",
                adapter_instance_id="emt-adapter-v1",
                adapter_client_scope="scope-a",
                venue_order_id="native-1",
            ),
        )
        with self.assertRaisesRegex(EmtMultiAccountRuntimeErrorV2, "not running"):
            supervisor.route_event(event)
        supervisor.start_account("account-a")
        self.assertEqual("scope-a", supervisor.route_event(event).binding.adapter_client_scope)


if __name__ == "__main__":
    unittest.main()
