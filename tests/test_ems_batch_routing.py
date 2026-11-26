from dataclasses import replace
from unittest.mock import patch

import pytest
from xuanling_tradecore.algorithms import MaxQuantitySliceAlgorithm, SliceAlgorithmSpec
from xuanling_tradecore.ems import EmsControlPlaneV1, EmsPendingPlanBufferV1
from xuanling_tradecore.ems.session_sharding import AccountSessionShardingError
from xuanling_tradecore.oms import (
    OmsSidecarFactKind,
    OmsSidecarOrderState,
    OrderStateProjectorV1,
)

from test_account_session_sharding import _instruction, _router
from test_oms_ems_sidecar_v1 import _fact


def test_business_plan_calls_native_batch_router_once_and_preserves_order():
    router = _router()
    parent = _instruction()
    snapshot = OrderStateProjectorV1().apply(
        (
            _fact(
                1,
                OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
            ),
        )
    )
    pending = EmsPendingPlanBufferV1(capacity=2)
    control = EmsControlPlaneV1(session_router=router, pending_plans=pending)
    with patch.object(router, "route_executions", wraps=router.route_executions) as batch:
        admission = control.create_plan(
            plan_id="batch-business-1",
            parent_instruction=parent,
            algorithm=MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 20)),
            snapshot=snapshot,
            required_scope_watermarks={"scope-a": 1},
            risk_policy_hash="b" * 64,
            created_monotonic_ns=100,
            expires_monotonic_ns=200,
        )
    batch.assert_called_once()
    assert admission.plan is not None
    for child in admission.plan.routed_children:
        assert child.shard_id == router.route_execution(child.instruction).binding.shard_id
    assert pending.depth() == 1


def test_entire_batch_rejects_foreign_account_before_native_call():
    router = _router()
    source = _instruction()
    foreign = replace(
        source,
        account_id="other-account",
        consumer_trace=replace(source.consumer_trace, account_id="other-account"),
    )
    with patch("xuanling_tradecore.ems.session_sharding.session_shards") as native:
        with pytest.raises(AccountSessionShardingError, match="crosses account"):
            router.route_executions((_instruction(), foreign))
    native.assert_not_called()


def test_computation_failure_does_not_publish_a_partial_plan():
    router = _router()
    snapshot = OrderStateProjectorV1().apply(
        (
            _fact(
                1,
                OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
                OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
            ),
        )
    )
    pending = EmsPendingPlanBufferV1(capacity=2)
    control = EmsControlPlaneV1(session_router=router, pending_plans=pending)
    with patch.object(router, "route_executions", side_effect=RuntimeError("compute failed")):
        with pytest.raises(RuntimeError, match="compute failed"):
            control.create_plan(
                plan_id="failed",
                parent_instruction=_instruction(),
                algorithm=MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 20)),
                snapshot=snapshot,
                required_scope_watermarks={"scope-a": 1},
                risk_policy_hash="b" * 64,
                created_monotonic_ns=100,
                expires_monotonic_ns=200,
            )
    assert pending.depth() == 0
