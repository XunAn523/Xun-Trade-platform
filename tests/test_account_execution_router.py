from __future__ import annotations

import dataclasses
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
    AccountExecutionBinding,
    AccountExecutionRoutePlan,
    AccountExecutionRouter,
    AccountExecutionRoutingError,
    AccountFenceScope,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def _trace(account_id: str) -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id=f"consumer-{account_id}",
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id=f"strategy-order-{account_id}",
        attribution_binding_id=f"attribution-{account_id}",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _instruction(account_id: str) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"execution-{account_id}",
        oms_order_id=f"oms-{account_id}",
        account_id=account_id,
        trading_day=TradingDayEpoch("2026-09-02"),
        consumer_trace=_trace(account_id),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=101,
        limit_price_ticks=100,
    )


def _cancel(account_id: str) -> CancelRequest:
    return CancelRequest(
        cancel_request_id=f"cancel-{account_id}",
        oms_order_id=f"oms-{account_id}",
        execution_id=f"execution-{account_id}",
        account_id=account_id,
        consumer_trace=_trace(account_id),
        reason="cancel",
        requested_monotonic_ns=100,
    )


def _binding(
    account_id: str,
    venue_id: str,
    adapter_instance_id: str,
    adapter_client_scope: str,
    *,
    shard_id: int,
) -> AccountExecutionBinding:
    scope = AccountFenceScope(account_id, venue_id, adapter_client_scope)
    return AccountExecutionBinding(
        binding_id=f"binding-{account_id}",
        binding_version=1,
        account_id=account_id,
        venue_id=venue_id,
        adapter_instance_id=adapter_instance_id,
        adapter_client_scope=adapter_client_scope,
        ownership_scope_hash=scope.scope_hash,
        mapping_manifest_hash=SHA_A,
        capability_manifest_hash=SHA_B,
        journal_plan_hash=SHA_C,
        journal_routing_epoch=3,
        journal_shard_id=shard_id,
    )


def _scheduler(account_id: str) -> ExecutionScheduler:
    return ExecutionScheduler(
        account_id=account_id,
        process_epoch_id="boot-1:process-1",
        policy=ExecutionSchedulerPolicy(
            policy_id="scheduler-v1",
            version=1,
            policy_hash=SHA_A,
            new_order_capacity=4,
            cancel_capacity=2,
            new_order_high_watermark=3,
            cancel_high_watermark=1,
            max_new_order_bytes=1024,
            max_cancel_bytes=512,
        ),
        execution_deadline_policy=ExecutionDeadlinePolicy("execution-v1", 1, SHA_B, 100),
        cancel_deadline_policy=CancelDeadlinePolicy("cancel-v1", 1, SHA_D, 50),
    )


def _plan() -> AccountExecutionRoutePlan:
    return AccountExecutionRoutePlan(
        plan_id="execution-routes-v1",
        version=1,
        plan_hash=SHA_D,
        bindings=(
            _binding("account-emt", "EMT", "emt-sim-v1", "7", shard_id=0),
            _binding("account-qmt", "QMT", "qmt-fake-v1", "scope-qmt-1", shard_id=1),
        ),
    )


def _router() -> AccountExecutionRouter:
    return AccountExecutionRouter(
        plan=_plan(),
        schedulers={
            "account-emt": _scheduler("account-emt"),
            "account-qmt": _scheduler("account-qmt"),
        },
    )


class AccountExecutionRouterTests(unittest.TestCase):
    def test_binding_is_bound_to_the_same_stable_ownership_scope(self) -> None:
        binding = _binding("account-emt", "EMT", "emt-sim-v1", "7", shard_id=0)
        self.assertEqual(
            AccountFenceScope("account-emt", "EMT", "7").scope_hash, binding.ownership_scope_hash
        )
        with self.assertRaises(AccountExecutionRoutingError):
            dataclasses.replace(binding, ownership_scope_hash="f" * 64)

    def test_plan_rejects_duplicate_account_and_native_scope(self) -> None:
        first = _binding("account-emt", "EMT", "emt-sim-v1", "7", shard_id=0)
        with self.assertRaises(AccountExecutionRoutingError):
            AccountExecutionRoutePlan("routes", 1, SHA_A, (first, first))
        reused_scope = _binding("account-other", "EMT", "emt-sim-v1", "7", shard_id=1)
        with self.assertRaises(AccountExecutionRoutingError):
            AccountExecutionRoutePlan("routes", 1, SHA_A, (first, reused_scope))
        second = _binding("account-qmt", "QMT", "qmt-fake-v1", "qmt-1", shard_id=1)
        with self.assertRaises(AccountExecutionRoutingError):
            AccountExecutionRoutePlan(
                "routes",
                1,
                SHA_A,
                (first, dataclasses.replace(second, journal_routing_epoch=4)),
            )

    def test_router_requires_exact_scheduler_accounts_and_identity(self) -> None:
        plan = _plan()
        with self.assertRaises(AccountExecutionRoutingError):
            AccountExecutionRouter(plan=plan, schedulers={"account-emt": _scheduler("account-emt")})
        with self.assertRaises(AccountExecutionRoutingError):
            AccountExecutionRouter(
                plan=plan,
                schedulers={
                    "account-emt": _scheduler("account-qmt"),
                    "account-qmt": _scheduler("account-emt"),
                },
            )

    def test_execution_routes_only_by_approved_account_binding(self) -> None:
        router = _router()
        emt_lane = router.route_execution(_instruction("account-emt"))
        qmt_lane = router.route_execution(_instruction("account-qmt"))
        self.assertEqual("EMT", emt_lane.binding.venue_id)
        self.assertEqual("QMT", qmt_lane.binding.venue_id)
        self.assertIsNot(emt_lane.scheduler, qmt_lane.scheduler)
        with self.assertRaises(AccountExecutionRoutingError):
            router.route_execution(_instruction("account-unknown"))

    def test_cancel_requires_exact_venue_instance_and_client_scope(self) -> None:
        router = _router()
        identity = VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            adapter_client_scope="7",
            venue_order_id="emt-order-1",
        )
        lane = router.route_cancel(_cancel("account-emt"), identity)
        self.assertEqual("account-emt", lane.binding.account_id)
        for mismatched in (
            dataclasses.replace(identity, venue_id="QMT"),
            dataclasses.replace(identity, adapter_instance_id="emt-sim-v2"),
            dataclasses.replace(identity, adapter_client_scope="8"),
        ):
            with self.assertRaises(AccountExecutionRoutingError):
                router.route_cancel(_cancel("account-emt"), mismatched)

    def test_broker_event_cannot_cross_account_route_or_adapter_scope(self) -> None:
        router = _router()
        identity = VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            adapter_client_scope="7",
            client_order_ref="101",
        )
        event = BrokerEvent(
            event_id="event-1",
            schema_version=1,
            source_key_schema_version=1,
            source_key="source-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            account_id="account-emt",
            trading_day=TradingDayEpoch("2026-09-02"),
            received_monotonic_ns=100,
            venue_order_identity=identity,
        )
        self.assertEqual("account-emt", router.route_event(event).binding.account_id)
        with self.assertRaises(AccountExecutionRoutingError):
            router.route_event(dataclasses.replace(event, account_id="account-qmt"))
        with self.assertRaises(AccountExecutionRoutingError):
            router.route_event(
                dataclasses.replace(
                    event,
                    venue_order_identity=dataclasses.replace(
                        identity, adapter_client_scope="foreign-scope"
                    ),
                )
            )


if __name__ == "__main__":
    unittest.main()
