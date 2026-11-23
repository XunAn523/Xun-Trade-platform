from __future__ import annotations

import unittest
from types import SimpleNamespace

from xuanling_tradecore.adapters import SubmitAccepted
from xuanling_tradecore.consumer import (
    ConsumerEmsExecutionBridgeV1,
    ConsumerExecutionDispatchStateV1,
)
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.ems import ExecutionDispatchOutcome


class _Runtime:
    def __init__(self, *, binding: ConsumerAccountBinding, route_version: int = 1) -> None:
        self.binding = SimpleNamespace(
            binding_id=binding.binding_id,
            binding_version=route_version,
            account_id=binding.account_id,
            venue_id=binding.venue_id,
            adapter_instance_id=binding.adapter_instance_id,
        )
        self.submit_commands: list[ExecutionInstruction] = []
        self.cancel_commands: list[object] = []
        self.close_reasons: list[str] = []
        self.raise_on_dispatch = False

    def admit_submit(self, instruction: ExecutionInstruction, **_kwargs: object) -> object:
        self.submit_commands.append(instruction)
        return SimpleNamespace(
            decision=SimpleNamespace(accepted=True, code=SimpleNamespace(value="ALLOWED")),
            authorized_result=object(),
        )

    def admit_cancel(self, instruction: object, **_kwargs: object) -> object:
        self.cancel_commands.append(instruction)
        return SimpleNamespace(accepted=True)

    def dispatch_once(self, **_kwargs: object) -> object:
        if self.raise_on_dispatch:
            raise RuntimeError("native call state is uncertain")
        if self.cancel_commands:
            request = self.cancel_commands[-1].cancel_request
            return SimpleNamespace(
                scheduled=SimpleNamespace(command=request),
                outcome=ExecutionDispatchOutcome.CANCEL_ACCEPTED,
                cancel_accepted=SimpleNamespace(cancel_request_id=request.cancel_request_id),
                submit_accepted=None,
            )
        instruction = self.submit_commands[-1]
        identity = VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id="emt-instance-1",
            adapter_client_scope="scope-1",
            client_order_ref="client-1",
            venue_order_id="venue-1",
        )
        return SimpleNamespace(
            scheduled=SimpleNamespace(command=instruction),
            outcome=ExecutionDispatchOutcome.SUBMIT_ACCEPTED,
            submit_accepted=SubmitAccepted(
                execution_id=instruction.execution_id,
                adapter_id="emt-adapter",
                adapter_instance_id="emt-instance-1",
                accepted_monotonic_ns=5,
                venue_order_identity=identity,
            ),
            cancel_accepted=None,
        )

    def close_admission(self, *, reason: str) -> None:
        self.close_reasons.append(reason)

    def release_new_order_inflight_after_terminal(self, execution_id: str) -> bool:
        return execution_id == "execution-1"


class ConsumerExecutionRuntimeV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.binding = ConsumerAccountBinding(
            "binding-1",
            1,
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "SIM",
            "EMT",
            "emt-instance-1",
        )
        trace = ConsumerTrace(
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "strategy-1",
            "strategy-order-1",
            "attribution-1",
            1,
            OrderOrigin.STRATEGY,
        )
        self.instruction = ExecutionInstruction(
            execution_id="execution-1",
            oms_order_id="oms-1",
            account_id="account-1",
            trading_day=TradingDayEpoch("2026-09-18"),
            consumer_trace=trace,
            symbol="000001",
            market="SZ",
            side="BUY",
            price_type="LIMIT",
            business_type="CASH",
            quantity=100,
            protection_price_ticks=100,
            limit_price_ticks=100,
        )
        self.order = SimpleNamespace(
            order=SimpleNamespace(
                oms_order_id="oms-1", account_id="account-1", consumer_trace=trace
            ),
            execution=self.instruction,
        )
        self.runtime = _Runtime(binding=self.binding)
        self.bridge = ConsumerEmsExecutionBridgeV1(
            runtime_for_binding=lambda _binding: self.runtime,  # type: ignore[arg-type]
            binding_for_order=lambda _order: self.binding,  # type: ignore[arg-type]
            market_for_execution=lambda _instruction: SimpleNamespace(),  # type: ignore[arg-type]
            kill_switches_for_account=lambda _account_id: SimpleNamespace(),  # type: ignore[arg-type]
            serialized_size_for_execution=lambda _instruction: 64,
            serialized_size_for_cancel=lambda _request: 64,
            now_monotonic_ns=lambda: 10,
        )

    def test_committed_execution_is_dispatched_once_and_gains_owned_cancel_identity(self) -> None:
        self.assertTrue(self.bridge.enqueue_committed_execution(self.order))  # type: ignore[arg-type]
        self.assertFalse(self.bridge.enqueue_committed_execution(self.order))  # type: ignore[arg-type]
        record = self.bridge.dispatch_next()
        assert record is not None
        self.assertEqual(ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED, record.state)
        self.assertEqual(["execution-1"], [item.execution_id for item in self.runtime.submit_commands])
        self.assertTrue(self.bridge.can_request_cancel(self.order))  # type: ignore[arg-type]
        self.assertTrue(
            self.bridge.enqueue_committed_cancel(
                self.order, cancel_request_id="cancel-1", reason="strategy cleanup"  # type: ignore[arg-type]
            )
        )
        canceled = self.bridge.dispatch_next()
        assert canceled is not None
        self.assertEqual(ConsumerExecutionDispatchStateV1.CANCEL_ACCEPTED, canceled.state)
        self.assertEqual(1, len(self.runtime.cancel_commands))
        self.assertTrue(self.bridge.release_after_terminal(execution_id="execution-1"))

    def test_uncertain_native_dispatch_is_not_requeued_and_closes_admission(self) -> None:
        self.runtime.raise_on_dispatch = True
        self.assertTrue(self.bridge.enqueue_committed_execution(self.order))  # type: ignore[arg-type]
        record = self.bridge.dispatch_next()
        assert record is not None
        self.assertEqual(
            ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION, record.state
        )
        self.assertEqual(["consumer-ems-uncertain-native-dispatch"], self.runtime.close_reasons)
        self.assertFalse(self.bridge.enqueue_committed_execution(self.order))  # type: ignore[arg-type]
        self.assertIsNone(self.bridge.dispatch_next())

    def test_generation_mismatch_fails_closed_before_native_admission(self) -> None:
        self.runtime = _Runtime(binding=self.binding, route_version=2)
        self.assertTrue(self.bridge.enqueue_committed_execution(self.order))  # type: ignore[arg-type]
        record = self.bridge.dispatch_next()
        assert record is not None
        self.assertEqual(
            ConsumerExecutionDispatchStateV1.UNKNOWN_REQUIRES_RECONCILIATION, record.state
        )
        self.assertEqual("BINDING_RUNTIME_MISMATCH", record.result_code)
        self.assertEqual([], self.runtime.submit_commands)
        self.assertEqual(["consumer-binding-runtime-mismatch"], self.runtime.close_reasons)


if __name__ == "__main__":
    unittest.main()
