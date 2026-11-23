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
    AccountFenceScope,
    AccountSessionShardBinding,
    AccountSessionShardCallbackRouter,
    AccountSessionShardingError,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64


def _scheduler() -> ExecutionScheduler:
    return ExecutionScheduler(
        account_id="account-emt",
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
        cancel_deadline_policy=CancelDeadlinePolicy("cancel-v1", 1, SHA_C, 50),
    )


def _binding(shard_id: str, client_scope: str) -> AccountSessionShardBinding:
    return AccountSessionShardBinding(
        shard_id=shard_id,
        account_id="account-emt",
        venue_id="EMT",
        adapter_instance_id="emt-sim-v1",
        adapter_client_scope=client_scope,
        ownership_scope_hash=AccountFenceScope("account-emt", "EMT", client_scope).scope_hash,
        mapping_manifest_hash=SHA_A,
        capability_manifest_hash=SHA_B,
        journal_plan_hash=SHA_C,
        journal_routing_epoch=3,
        journal_shard_id=0,
    )


def _router() -> AccountSessionShardRouter:
    shards = (_binding("scope-a", "1"), _binding("scope-b", "2"), _binding("scope-c", "3"))
    plan = AccountSessionShardPlan(
        plan_id="emt-session-shards-v1",
        version=1,
        plan_hash=SHA_D,
        account_id="account-emt",
        shards=shards,
    )
    return AccountSessionShardRouter(
        plan=plan,
        schedulers={shard.shard_id: _scheduler() for shard in shards},
    )


def _instruction(execution_id: str = "execution-1") -> ExecutionInstruction:
    trace = ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-emt",
        account_id="account-emt",
        strategy_id="strategy-1",
        strategy_order_id="strategy-order-1",
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )
    return ExecutionInstruction(
        execution_id=execution_id,
        oms_order_id=f"oms-{execution_id}",
        account_id="account-emt",
        trading_day=TradingDayEpoch("2026-09-02"),
        consumer_trace=trace,
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=101,
        limit_price_ticks=100,
    )


def _identity(client_scope: str) -> VenueOrderIdentity:
    return VenueOrderIdentity(
        venue_id="EMT",
        adapter_instance_id="emt-sim-v1",
        adapter_client_scope=client_scope,
        venue_order_id="venue-order-1",
        client_order_ref="101",
    )


class AccountSessionShardRouterTests(unittest.TestCase):
    def test_batch_native_routes_equal_single_v1_routes(self) -> None:
        router = _router()
        ids = tuple(f"execution-{i}" for i in range(10000))
        batch = router.route_execution_ids(account_id="account-emt", execution_ids=ids, workers=4)
        self.assertEqual(
            batch,
            tuple(router.route_execution_id(account_id="account-emt", execution_id=x) for x in ids),
        )

    def test_batch_rejects_cross_account_and_invalid_identity(self) -> None:
        router = _router()
        with self.assertRaises(AccountSessionShardingError):
            router.route_execution_ids(account_id="another-account", execution_ids=("x",))
        with self.assertRaises(ValueError):
            router.route_execution_ids(account_id="account-emt", execution_ids=("x\ny",))

    def test_new_execution_uses_byte_stable_deterministic_scope(self) -> None:
        router = _router()
        first = router.route_execution(_instruction("execution-stable"))
        second = router.route_execution(_instruction("execution-stable"))
        self.assertEqual(first.binding.shard_id, second.binding.shard_id)
        self.assertIn(first.binding.adapter_client_scope, {"1", "2", "3"})
        self.assertEqual(
            first.binding.shard_id,
            router.plan.shard_for_execution_id(
                account_id="account-emt", execution_id="execution-stable"
            ).shard_id,
        )

    def test_distinct_executions_distribute_across_every_declared_scope(self) -> None:
        router = _router()
        shard_ids = {
            router.route_execution(_instruction(f"execution-{index}")).binding.shard_id
            for index in range(128)
        }
        self.assertEqual({"scope-a", "scope-b", "scope-c"}, shard_ids)

    def test_cancel_and_callback_stick_to_the_native_scope(self) -> None:
        router = _router()
        identity = _identity("2")
        request = CancelRequest(
            cancel_request_id="cancel-1",
            oms_order_id="oms-1",
            execution_id="execution-1",
            account_id="account-emt",
            consumer_trace=_instruction().consumer_trace,
            reason="cancel",
            requested_monotonic_ns=10,
        )
        self.assertEqual("scope-b", router.route_cancel(request, identity).binding.shard_id)
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
            received_monotonic_ns=10,
            venue_order_identity=identity,
        )
        self.assertEqual("scope-b", router.route_event(event).binding.shard_id)

    def test_unknown_scope_or_cross_account_fails_closed(self) -> None:
        router = _router()
        with self.assertRaises(AccountSessionShardingError):
            router.route_event(
                dataclasses.replace(
                    BrokerEvent(
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
                        received_monotonic_ns=10,
                        venue_order_identity=_identity("foreign"),
                    ),
                    account_id="account-emt",
                )
            )
        with self.assertRaises(AccountSessionShardingError):
            router.route_execution_id(account_id="other", execution_id="execution-1")

    def test_plan_rejects_reused_native_scope_and_missing_scheduler(self) -> None:
        first = _binding("scope-a", "1")
        with self.assertRaises(AccountSessionShardingError):
            AccountSessionShardPlan(
                "plan", 1, SHA_A, "account-emt", (first, _binding("scope-b", "1"))
            )
        plan = AccountSessionShardPlan("plan", 1, SHA_A, "account-emt", (first,))
        with self.assertRaises(AccountSessionShardingError):
            AccountSessionShardRouter(plan=plan, schedulers={})

    def test_shared_callback_feed_is_rebound_only_to_the_pre_registered_scope(self) -> None:
        router = _router()
        callback_router = AccountSessionShardCallbackRouter(plan=router.plan)
        target = next(binding for binding in router.plan.shards if binding.shard_id == "scope-b")
        callback_router.register_pre_native(binding=target, client_order_ref="101")
        source_event = BrokerEvent(
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
            received_monotonic_ns=10,
            venue_order_identity=_identity("1"),
        )
        routed = callback_router.route_event(source_event)
        assert routed is not None
        self.assertEqual("scope-b", routed.binding.shard_id)
        assert routed.event.venue_order_identity is not None
        self.assertEqual("2", routed.event.venue_order_identity.adapter_client_scope)
        self.assertEqual("1", source_event.venue_order_identity.adapter_client_scope)
        self.assertIsNone(
            callback_router.route_event(
                dataclasses.replace(
                    source_event,
                    venue_order_identity=dataclasses.replace(
                        source_event.venue_order_identity, client_order_ref="unregistered"
                    ),
                )
            )
        )

    def test_callback_router_seals_registration_and_retains_the_registered_route(self) -> None:
        router = _router()
        callback_router = AccountSessionShardCallbackRouter(plan=router.plan)
        target = next(binding for binding in router.plan.shards if binding.shard_id == "scope-c")
        callback_router.register_pre_native(binding=target, client_order_ref="101")
        self.assertEqual(1, callback_router.seal())
        self.assertEqual(1, callback_router.seal())
        with self.assertRaisesRegex(AccountSessionShardingError, "sealed"):
            callback_router.register_pre_native(binding=target, client_order_ref="102")
        routed = callback_router.route_event(
            BrokerEvent(
                event_id="event-sealed",
                schema_version=1,
                source_key_schema_version=1,
                source_key="source-sealed",
                event_kind=BrokerEventKind.ORDER,
                source_kind=BrokerSourceKind.CALLBACK,
                venue_id="EMT",
                adapter_instance_id="emt-sim-v1",
                account_id="account-emt",
                trading_day=TradingDayEpoch("2026-09-02"),
                received_monotonic_ns=10,
                venue_order_identity=_identity("1"),
            )
        )
        assert routed is not None
        self.assertEqual("scope-c", routed.binding.shard_id)


if __name__ == "__main__":
    unittest.main()
