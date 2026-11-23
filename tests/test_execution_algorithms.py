from __future__ import annotations

import unittest
from dataclasses import replace

from xuanling_tradecore.algorithms import (
    AlgorithmPlan,
    ExecutionAlgorithmError,
    MaxQuantitySliceAlgorithm,
    SliceAlgorithmSpec,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)


def _parent(*, quantity: int = 10) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-parent-1",
        oms_order_id="oms-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-04"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            strategy_id="strategy-1",
            strategy_order_id="strategy-order-1",
            attribution_binding_id="attribution-1",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        ),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=quantity,
        protection_price_ticks=100,
        limit_price_ticks=99,
    )


class ExecutionAlgorithmTests(unittest.TestCase):
    def test_planner_preserves_parent_lineage_and_never_dispatches(self) -> None:
        plan = MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 4)).plan(_parent())
        self.assertEqual([4, 4, 2], [child.instruction.quantity for child in plan.children])
        self.assertEqual(
            [
                "execution-parent-1:child:1",
                "execution-parent-1:child:2",
                "execution-parent-1:child:3",
            ],
            [child.instruction.execution_id for child in plan.children],
        )
        for index, child in enumerate(plan.children, start=1):
            self.assertEqual("execution-parent-1", child.parent_execution_id)
            self.assertEqual("slice", child.instruction.execution_parameters["algorithm_id"])
            self.assertEqual(
                str(index), child.instruction.execution_parameters["algorithm_child_index"]
            )
            self.assertEqual(
                plan.parent_instruction.consumer_trace, child.instruction.consumer_trace
            )

    def test_plan_rejects_quantity_and_account_scope_mutation(self) -> None:
        plan = MaxQuantitySliceAlgorithm(SliceAlgorithmSpec("slice", 1, 10)).plan(_parent())
        only = plan.children[0]
        altered_quantity = replace(only, instruction=replace(only.instruction, quantity=9))
        with self.assertRaisesRegex(ExecutionAlgorithmError, "preserve parent quantity"):
            AlgorithmPlan("slice", 1, plan.parent_instruction, (altered_quantity,))
        altered_account = replace(only, instruction=replace(only.instruction, market="SH"))
        with self.assertRaisesRegex(ExecutionAlgorithmError, "routing or protection"):
            AlgorithmPlan("slice", 1, plan.parent_instruction, (altered_account,))

    def test_invalid_algorithm_specs_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            SliceAlgorithmSpec("slice", 0, 1)
        with self.assertRaises(ValueError):
            SliceAlgorithmSpec("slice", 1, 0)


if __name__ == "__main__":
    unittest.main()
