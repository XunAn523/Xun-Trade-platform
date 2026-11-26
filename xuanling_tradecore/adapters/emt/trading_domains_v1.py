"""Narrow OMS/EMS services for EMT ordinary, credit and option domains.

This layer intentionally exposes named trading-domain operations rather than a
``call(name, payload)`` escape hatch.  It delegates to the catalogued module
and therefore cannot turn a query or a control operation into an order-lane
call.  Vendor request structures remain private to the platform factory.
"""

from __future__ import annotations

from typing import Any, Sequence

from ...compatibility.emt_trading_platform import QueryCompletion, QueryHandle, SideEffectPermit
from .trading_catalog_v1 import EmtOperationLane, EmtTradingDomain
from .trading_module_v1 import (
    EmtFullTradingModuleV1,
    EmtOperationCommandV1,
    EmtOperationReceiptV1,
    EmtTradingModuleError,
)


class EmtTradingDomainServiceError(EmtTradingModuleError):
    """A request crossed a domain or execution-lane boundary."""


class _EmtTradingDomainServiceV1:
    """Private reusable boundary for named domain façade classes."""

    domain: EmtTradingDomain

    def __init__(self, module: EmtFullTradingModuleV1) -> None:
        self._module = module

    def _dispatch(
        self,
        command: EmtOperationCommandV1,
        *,
        expected_name: str,
        permit: SideEffectPermit,
    ) -> EmtOperationReceiptV1:
        spec = command.spec
        if command.official_name != expected_name or spec.domain is not self.domain:
            raise EmtTradingDomainServiceError("operation does not match its EMS domain method")
        return self._module.dispatch_mutation(command, permit=permit)

    def _begin_query(
        self,
        official_name: str,
        *,
        native_arguments: Sequence[Any] = (),
        correlation_id: str | None = None,
        profile: str,
        workload: str | None = None,
        completion: QueryCompletion | None = None,
        timeout_ns: int = 8_000_000_000,
    ) -> QueryHandle:
        from .trading_catalog_v1 import trader_api_spec_v1

        spec = trader_api_spec_v1(official_name)
        if spec.domain is not self.domain or spec.lane is not EmtOperationLane.QUERY:
            raise EmtTradingDomainServiceError("query does not match its OMS domain")
        return self._module.query_service.begin(
            official_name,
            native_arguments=native_arguments,
            correlation_id=correlation_id,
            profile=profile,
            workload=workload,
            completion=completion,
            timeout_ns=timeout_ns,
        )


class EmtFundTradingServiceV1(_EmtTradingDomainServiceV1):
    """T2 entry points for fund transfer plus account/fund reference queries."""

    domain = EmtTradingDomain.FUND

    def transfer(self, command: EmtOperationCommandV1, *, permit: SideEffectPermit) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="FundTransfer", permit=permit)

    def begin_query(self, official_name: str, **kwargs: Any) -> QueryHandle:
        return self._begin_query(official_name, profile="fund", **kwargs)


class EmtCreditTradingServiceV1(_EmtTradingDomainServiceV1):
    """T3 named two-financing mutations; order-like sell-repay stays Fast."""

    domain = EmtTradingDomain.CREDIT

    def cash_repay(self, command: EmtOperationCommandV1, *, permit: SideEffectPermit) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="CreditCashRepay", permit=permit)

    def cash_repay_specified_debt(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="CreditCashRepaySpecifiedDebt", permit=permit)

    def sell_stock_repay_debt_interest_fee(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        return self._dispatch(
            command,
            expected_name="CreditSellStockRepayDebtInterestFee",
            permit=permit,
        )

    def extend_debt_date(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="CreditExtendDebtDate", permit=permit)

    def quota_transfer(self, command: EmtOperationCommandV1, *, permit: SideEffectPermit) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="CreditQuotaTransfer", permit=permit)

    def margin_available_transfer(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="CreditMarginAvlTransfer", permit=permit)

    def begin_query(self, official_name: str, **kwargs: Any) -> QueryHandle:
        return self._begin_query(official_name, profile="credit", **kwargs)


class EmtOptionTradingServiceV1(_EmtTradingDomainServiceV1):
    """T4 entry points; combined orders stay Fast while exercise is control."""

    domain = EmtTradingDomain.OPTION

    def exercise(self, command: EmtOperationCommandV1, *, permit: SideEffectPermit) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="InsertOptionExerciseOrder", permit=permit)

    def insert_combined_order(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        return self._dispatch(command, expected_name="InsertOptionCombinedOrder", permit=permit)

    def begin_query(self, official_name: str, **kwargs: Any) -> QueryHandle:
        return self._begin_query(official_name, profile="option", **kwargs)
