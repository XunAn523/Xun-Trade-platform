from __future__ import annotations

import pytest
from emt_gateway.facade import SideEffectPermit
from xuanling_tradecore.adapters.emt.trading_catalog_v1 import EmtOperationLane
from xuanling_tradecore.adapters.emt.trading_domains_v1 import (
    EmtCreditTradingServiceV1,
    EmtOptionTradingServiceV1,
    EmtTradingDomainServiceError,
)
from xuanling_tradecore.adapters.emt.trading_module_v1 import (
    EmtOperationCommandV1,
    EmtOperationReceiptV1,
)


class _Module:
    def __init__(self) -> None:
        self.query_service = self
        self.dispatched: list[str] = []
        self.queries: list[str] = []

    def dispatch_mutation(self, command: EmtOperationCommandV1, *, permit: SideEffectPermit) -> EmtOperationReceiptV1:
        self.dispatched.append(command.official_name)
        return EmtOperationReceiptV1(
            operation_id=command.operation_id,
            official_name=command.official_name,
            lane=command.spec.lane,
            accepted=True,
            native_id=1,
            accepted_monotonic_ns=1,
            rejection_code=None,
        )

    def begin(self, official_name: str, **kwargs: object) -> str:
        self.queries.append(official_name)
        return f"query:{official_name}"


def _command(name: str) -> EmtOperationCommandV1:
    return EmtOperationCommandV1(
        operation_id=f"id-{name}",
        official_name=name,
        account_id="sim-account",
        scope_id="scope-a",
        session_epoch=1,
        native_arguments=(),
        approval_reference="fixture-approved",
    )


def test_t3_credit_control_and_fast_mutations_keep_their_catalogue_lanes() -> None:
    module = _Module()
    service = EmtCreditTradingServiceV1(module)
    permit = SideEffectPermit.simulated_fixture()
    assert service.cash_repay(_command("CreditCashRepay"), permit=permit).lane is EmtOperationLane.CONTROL_MUTATION
    assert service.sell_stock_repay_debt_interest_fee(
        _command("CreditSellStockRepayDebtInterestFee"), permit=permit
    ).lane is EmtOperationLane.FAST_EXECUTION
    assert module.dispatched == ["CreditCashRepay", "CreditSellStockRepayDebtInterestFee"]


def test_t4_option_combined_and_exercise_keep_separate_lanes() -> None:
    module = _Module()
    service = EmtOptionTradingServiceV1(module)
    permit = SideEffectPermit.simulated_fixture()
    assert service.exercise(_command("InsertOptionExerciseOrder"), permit=permit).lane is EmtOperationLane.CONTROL_MUTATION
    assert service.insert_combined_order(
        _command("InsertOptionCombinedOrder"), permit=permit
    ).lane is EmtOperationLane.FAST_EXECUTION


def test_domain_services_refuse_cross_domain_names_for_mutations_and_queries() -> None:
    module = _Module()
    service = EmtCreditTradingServiceV1(module)
    with pytest.raises(EmtTradingDomainServiceError, match="does not match"):
        service.cash_repay(_command("FundTransfer"), permit=SideEffectPermit.simulated_fixture())
    with pytest.raises(EmtTradingDomainServiceError, match="does not match"):
        service.begin_query("QueryOptionCombinedOrders")
