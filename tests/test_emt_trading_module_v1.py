from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any

from emt_gateway.facade import EmtTraderFacade, SideEffectPermit
from emt_gateway.query_registry import QueryRegistry
from xuanling_tradecore.adapters.emt.trading_module_v1 import (
    EmtControlMutationDispatcherV1,
    EmtFullTradingModuleV1,
    EmtOperationCommandV1,
    EmtOperationLane,
    EmtOperationReceiptV1,
    EmtTradingModuleError,
    EmtTradingQueryServiceV1,
)


class _Api:
    def creditCashRepay(self, amount: float, session_id: int) -> int:
        self.last_amount, self.last_session = amount, session_id
        return 81

    def fundTransfer(self, request: object, session_id: int) -> int:
        self.last_fund_request, self.last_session = request, session_id
        return 0

    def getApiLastError(self) -> dict[str, int]:
        return {"error_id": 9001}

    def queryOrders(self, request: object, session_id: int, request_id: int) -> int:
        self.last_query = (request, session_id, request_id)
        return 0


@dataclass
class _Journal:
    events: list[tuple[str, str]] = field(default_factory=list)

    def pre_native(self, command: EmtOperationCommandV1) -> str:
        self.events.append(("PRE_NATIVE", command.operation_id))
        return "pre:1"

    def call_started(self, command: EmtOperationCommandV1, *, pre_native_reference: str) -> str:
        self.events.append(("CALL_STARTED", pre_native_reference))
        return "started:1"

    def native_accepted(self, command: EmtOperationCommandV1, **kwargs: object) -> None:
        self.events.append(("NATIVE_ACCEPTED", str(kwargs["native_id"])))

    def native_rejected(self, command: EmtOperationCommandV1, **kwargs: object) -> None:
        self.events.append(("NATIVE_REJECTED", str(kwargs["rejection_code"])))

    def native_outcome_unknown(self, command: EmtOperationCommandV1, **kwargs: object) -> None:
        self.events.append(("UNKNOWN", str(kwargs["reason"])))


class _FastPort:
    def __init__(self) -> None:
        self.commands: list[EmtOperationCommandV1] = []

    def dispatch_fast(self, command: EmtOperationCommandV1) -> EmtOperationReceiptV1:
        self.commands.append(command)
        return EmtOperationReceiptV1(
            operation_id=command.operation_id,
            official_name=command.official_name,
            lane=EmtOperationLane.FAST_EXECUTION,
            accepted=True,
            native_id=77,
            accepted_monotonic_ns=1,
            rejection_code=None,
        )


class EmtTradingModuleV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.api = _Api()
        self.facade = EmtTraderFacade(
            self.api,
            session_id=3,
            query_registry=QueryRegistry(),
        )
        self.journal = _Journal()
        self.control = EmtControlMutationDispatcherV1(facade=self.facade, journal=self.journal)

    @staticmethod
    def _command(name: str, args: tuple[Any, ...]) -> EmtOperationCommandV1:
        return EmtOperationCommandV1(
            operation_id="op-1",
            official_name=name,
            account_id="account",
            scope_id="scope-1",
            session_epoch=1,
            native_arguments=args,
            approval_reference="approval-1",
        )

    def test_control_mutation_is_durable_before_native_acceptance(self) -> None:
        receipt = self.control.dispatch(
            self._command("CreditCashRepay", (1.0,)),
            permit=SideEffectPermit.simulated_fixture(),
        )
        self.assertTrue(receipt.accepted)
        self.assertEqual(81, receipt.native_id)
        self.assertEqual(
            [("PRE_NATIVE", "op-1"), ("CALL_STARTED", "pre:1"), ("NATIVE_ACCEPTED", "81")],
            self.journal.events,
        )
        self.assertEqual((1.0, 3), (self.api.last_amount, self.api.last_session))

    def test_return_zero_is_durable_rejection_without_retry(self) -> None:
        receipt = self.control.dispatch(
            self._command("FundTransfer", (object(),)),
            permit=SideEffectPermit.simulated_fixture(),
        )
        self.assertFalse(receipt.accepted)
        self.assertEqual(9001, receipt.rejection_code)
        self.assertEqual("NATIVE_REJECTED", self.journal.events[-1][0])

    def test_fast_mutation_cannot_bypass_fast_owner(self) -> None:
        with self.assertRaisesRegex(EmtTradingModuleError, "fast_execution owner"):
            self.control.dispatch(
                self._command("InsertOrder", (object(),)),
                permit=SideEffectPermit.simulated_fixture(),
            )
        self.assertEqual([], self.journal.events)

    def test_full_module_routes_fast_and_control_to_distinct_owners(self) -> None:
        fast = _FastPort()
        module = EmtFullTradingModuleV1(
            query_service=EmtTradingQueryServiceV1(facade=self.facade),
            control_dispatcher=self.control,
            fast_operation_port=fast,
        )
        receipt = module.dispatch_mutation(
            self._command("InsertOptionCombinedOrder", (object(),)),
            permit=SideEffectPermit.simulated_fixture(),
        )
        self.assertTrue(receipt.accepted)
        self.assertEqual(1, len(fast.commands))

    def test_query_service_dispatches_catalogued_query_only(self) -> None:
        service = EmtTradingQueryServiceV1(facade=self.facade)
        handle = service.begin("QueryOrders", native_arguments=(object(),), correlation_id="query-1")
        self.assertEqual("QueryOrders", handle.api_name)
        self.assertEqual(3, self.api.last_query[1])
        with self.assertRaisesRegex(EmtTradingModuleError, "not a query-lane"):
            service.begin("InsertOrder")
