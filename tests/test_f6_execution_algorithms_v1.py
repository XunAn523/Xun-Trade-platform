"""F6 pure execution-algorithm selection, scheduling, and audit contracts."""

from __future__ import annotations

import unittest
from pathlib import Path

from xuanling_tradecore.algorithms import (
    AlgorithmAuditFactV1,
    AlgorithmChildStateV1,
    AlgorithmRunControllerV1,
    AlgorithmRunStateV1,
    ExecutionAlgorithmError,
    ExecutionAlgorithmSelectorV1,
    PovAlgorithmV1,
    PovScheduleBucketV1,
    PovSpecV1,
    TargetPositionAlgorithmV1,
    TargetPositionSpecV1,
    TwapAlgorithmV1,
    TwapSpecV1,
    VolumeScheduleBucketV1,
    VwapAlgorithmV1,
    VwapSpecV1,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)


def _parent(*, quantity: int = 10, side: str = "BUY") -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="parent-execution-1",
        oms_order_id="oms-order-1",
        account_id="emt-account-1",
        trading_day=TradingDayEpoch("2026-09-10"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="emt-account-1",
            strategy_id="strategy-1",
            strategy_order_id="strategy-order-1",
            attribution_binding_id="attribution-1",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY,
        ),
        symbol="000001",
        market="SZ",
        side=side,
        price_type="LIMIT",
        business_type="CASH",
        quantity=quantity,
        protection_price_ticks=100,
        limit_price_ticks=99,
    )


class _Audit:
    def __init__(self, *, fail: bool = False) -> None:
        self.facts: list[AlgorithmAuditFactV1] = []
        self.fail = fail

    def append_algorithm_audit(self, fact: AlgorithmAuditFactV1) -> None:
        if self.fail:
            raise OSError("audit unavailable")
        self.facts.append(fact)


class F6ExecutionAlgorithmTests(unittest.TestCase):
    def test_target_position_validates_delta_then_preserves_parent_lineage(self) -> None:
        algorithm = TargetPositionAlgorithmV1(
            TargetPositionSpecV1(
                "target-position", 1, current_position=20, target_position=30, max_child_quantity=4
            )
        )
        plan = algorithm.plan(_parent(quantity=10))
        self.assertEqual([4, 4, 2], [child.instruction.quantity for child in plan.children])
        self.assertTrue(
            all(
                child.instruction.execution_parameters["algorithm_target_position"] == "30"
                for child in plan.children
            )
        )
        with self.assertRaisesRegex(ExecutionAlgorithmError, "does not match"):
            algorithm.plan(_parent(quantity=10, side="SELL"))
        with self.assertRaisesRegex(ExecutionAlgorithmError, "delta is zero"):
            TargetPositionAlgorithmV1(TargetPositionSpecV1("target-position", 1, 30, 30, 1)).plan(
                _parent(quantity=1)
            )

    def test_twap_and_vwap_create_exact_scheduled_children(self) -> None:
        twap = TwapAlgorithmV1(TwapSpecV1("twap", 2, 3, 100, 10))
        twap_plan = twap.plan(_parent(quantity=10))
        self.assertEqual([4, 3, 3], [child.instruction.quantity for child in twap_plan.children])
        self.assertEqual(
            ["100", "110", "120"],
            [
                child.instruction.execution_parameters["algorithm_not_before_monotonic_ns"]
                for child in twap_plan.children
            ],
        )
        vwap = VwapAlgorithmV1(
            VwapSpecV1(
                "vwap",
                3,
                (
                    VolumeScheduleBucketV1("open", 10, 1),
                    VolumeScheduleBucketV1("mid", 20, 2),
                    VolumeScheduleBucketV1("close", 30, 1),
                ),
            )
        )
        vwap_plan = vwap.plan(_parent(quantity=10))
        self.assertEqual([3, 5, 2], [child.instruction.quantity for child in vwap_plan.children])
        self.assertTrue(
            all(
                child.instruction.execution_parameters["algorithm_schedule_kind"] == "VWAP"
                for child in vwap_plan.children
            )
        )
        with self.assertRaisesRegex(ExecutionAlgorithmError, "slice_count exceeds"):
            twap.plan(_parent(quantity=2))
        with self.assertRaisesRegex(ExecutionAlgorithmError, "bucket count exceeds"):
            vwap.plan(_parent(quantity=2))

    def test_pov_never_exceeds_explicit_expected_volume_capacity(self) -> None:
        algorithm = PovAlgorithmV1(
            PovSpecV1(
                "pov",
                1,
                max_participation_bps=5_000,
                buckets=(
                    PovScheduleBucketV1("first", 10, 4),
                    PovScheduleBucketV1("second", 20, 10),
                ),
            )
        )
        plan = algorithm.plan(_parent(quantity=5))
        self.assertEqual([2, 3], [child.instruction.quantity for child in plan.children])
        self.assertEqual(
            "5000",
            plan.children[0].instruction.execution_parameters["algorithm_max_participation_bps"],
        )
        with self.assertRaisesRegex(ExecutionAlgorithmError, "cannot cover"):
            algorithm.plan(_parent(quantity=8))

    def test_selector_rejects_unknown_algorithm(self) -> None:
        twap = TwapAlgorithmV1(TwapSpecV1("twap", 1, 1, 0, 0))
        selector = ExecutionAlgorithmSelectorV1({"twap": twap})
        self.assertIs(twap, selector.select("twap"))
        with self.assertRaisesRegex(ExecutionAlgorithmError, "not registered"):
            selector.select("unregistered")

    def test_lifecycle_requires_audit_then_pauses_releases_and_aggregates_children(self) -> None:
        plan = TwapAlgorithmV1(TwapSpecV1("twap", 1, 2, 100, 10)).plan(_parent(quantity=10))
        audit = _Audit()
        controller = AlgorithmRunControllerV1(
            plan_id="plan-1", plan=plan, audit_port=audit, created_monotonic_ns=1
        )
        self.assertEqual(AlgorithmRunStateV1.PLANNED, controller.snapshot().state)
        self.assertEqual((), controller.release_due(observed_monotonic_ns=100))
        controller.start(observed_monotonic_ns=2)
        first = controller.release_due(observed_monotonic_ns=100)
        self.assertEqual(1, len(first))
        controller.mark_child_accepted(
            execution_id=first[0].instruction.execution_id, observed_monotonic_ns=101
        )
        controller.pause(reason_code="OPERATOR_PAUSE", observed_monotonic_ns=102)
        self.assertEqual((), controller.release_due(observed_monotonic_ns=110))
        controller.resume(reason_code="OPERATOR_RESUME", observed_monotonic_ns=103)
        second = controller.release_due(observed_monotonic_ns=110)
        self.assertEqual(1, len(second))
        controller.mark_child_accepted(
            execution_id=second[0].instruction.execution_id, observed_monotonic_ns=111
        )
        controller.mark_child_terminal(
            execution_id=first[0].instruction.execution_id, observed_monotonic_ns=112
        )
        self.assertEqual(AlgorithmRunStateV1.RUNNING, controller.snapshot().state)
        controller.mark_child_terminal(
            execution_id=second[0].instruction.execution_id, observed_monotonic_ns=113
        )
        snapshot = controller.snapshot()
        self.assertEqual(AlgorithmRunStateV1.COMPLETED, snapshot.state)
        self.assertTrue(
            all(state is AlgorithmChildStateV1.TERMINAL for state in snapshot.child_states.values())
        )
        self.assertEqual(snapshot.audit_sequence, len(audit.facts))
        self.assertEqual("PLAN_CREATED", audit.facts[0].event_kind)
        self.assertEqual("PLAN_COMPLETED", audit.facts[-1].event_kind)

    def test_rejection_or_audit_failure_never_releases_additional_children(self) -> None:
        plan = TwapAlgorithmV1(TwapSpecV1("twap", 1, 2, 0, 0)).plan(_parent(quantity=10))
        audit = _Audit()
        controller = AlgorithmRunControllerV1(
            plan_id="plan-2", plan=plan, audit_port=audit, created_monotonic_ns=1
        )
        controller.start(observed_monotonic_ns=2)
        released = controller.release_due(observed_monotonic_ns=2)
        controller.mark_child_rejected(
            execution_id=released[0].instruction.execution_id,
            reason_code="ADMISSION_REJECTED",
            observed_monotonic_ns=3,
        )
        self.assertEqual(AlgorithmRunStateV1.FAILED, controller.snapshot().state)
        self.assertEqual((), controller.release_due(observed_monotonic_ns=4))
        failing_audit = _Audit()
        failing = AlgorithmRunControllerV1(
            plan_id="plan-3", plan=plan, audit_port=failing_audit, created_monotonic_ns=1
        )
        failing_audit.fail = True
        with self.assertRaises(OSError):
            failing.start(observed_monotonic_ns=2)
        self.assertEqual(AlgorithmRunStateV1.PLANNED, failing.snapshot().state)

    def test_algorithm_package_has_no_adapter_or_network_import(self) -> None:
        package = Path(__file__).parents[1] / "xuanling_tradecore" / "algorithms"
        source = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))
        self.assertNotIn("..adapters", source)
        self.assertNotIn("import socket", source)


if __name__ == "__main__":
    unittest.main()
