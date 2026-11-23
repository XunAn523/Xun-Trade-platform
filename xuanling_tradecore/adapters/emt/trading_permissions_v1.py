"""Explicit OMS/EMS permissions for EMT's complete transaction/account scope."""

from __future__ import annotations

from dataclasses import dataclass

from .trading_catalog_v1 import EmtOperationLane, EmtTradingDomain, trader_api_spec_v1


class EmtTradingPermissionError(PermissionError):
    """An SDK operation lacks a narrowly granted OMS/EMS permission."""


def required_permission_for_trader_api_v1(official_name: str) -> str:
    """Return a named permission; lifecycle methods deliberately have no EMS route."""

    spec = trader_api_spec_v1(official_name)
    if spec.lane is EmtOperationLane.SESSION_CONTROL:
        raise EmtTradingPermissionError("session lifecycle is not an OMS/EMS client permission")
    if spec.lane is EmtOperationLane.QUERY:
        return {
            EmtTradingDomain.CREDIT: "credit.query.read",
            EmtTradingDomain.OPTION: "option.query.read",
        }.get(spec.domain, "account.query.read")
    if official_name == "InsertOrder":
        return "trade.order"
    if official_name == "CancelOrder":
        return "trade.cancel"
    if official_name == "FundTransfer":
        return "account.transfer"
    if official_name == "CreditSellStockRepayDebtInterestFee":
        return "credit.repay"
    if official_name.startswith("Credit"):
        return {
            "CreditCashRepay": "credit.repay",
            "CreditCashRepaySpecifiedDebt": "credit.repay",
            "CreditExtendDebtDate": "credit.extend",
            "CreditQuotaTransfer": "credit.quota.transfer",
            "CreditMarginAvlTransfer": "credit.margin.transfer",
        }[official_name]
    if official_name == "InsertOptionExerciseOrder":
        return "option.exercise"
    if official_name == "InsertOptionCombinedOrder":
        return "option.combined.order"
    raise EmtTradingPermissionError("TraderApi operation has no explicit OMS/EMS permission")


@dataclass(frozen=True)
class EmtTradingPermissionSetV1:
    """Immutable least-privilege grants; wildcards are intentionally unsupported."""

    grants: frozenset[str]

    def __post_init__(self) -> None:
        if not self.grants or any(not grant or "*" in grant for grant in self.grants):
            raise EmtTradingPermissionError("EMT permission grants must be explicit non-empty names")

    def require_api(self, official_name: str) -> str:
        required = required_permission_for_trader_api_v1(official_name)
        if required not in self.grants:
            raise EmtTradingPermissionError(f"missing EMT permission: {required}")
        return required

    def require_callback_read(self) -> None:
        if "native.callback.read" not in self.grants:
            raise EmtTradingPermissionError("missing EMT permission: native.callback.read")
