"""Runtime capability inspection for the official EMT 2.27.0 TraderApi.

The gateway must never silently treat an absent Python-binding method as an
implemented interface.  This module carries the official 86-name surface in
the runtime package, so deployment inspection has no dependency on benchmark
tooling or the vendor module import path.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Iterable

OFFICIAL_TRADER_API = (
    "CreateTraderApi",
    "SetCpuAffinity",
    "SetSoftwareVersion",
    "SetHeartBeatInterval",
    "SubscribePublicTopic",
    "RegisterSpi",
    "Login",
    "Logout",
    "Release",
    "GetTradingDay",
    "IsServerRestart",
    "GetApiVersion",
    "GetApiLastError",
    "GetErrMsgByCode",
    "GetClientIDByEMTID",
    "GetAccountByEMTID",
    "InsertOrder",
    "CancelOrder",
    "QueryOrderByEMTID",
    "QueryOrders",
    "QueryUnfinishedOrders",
    "QueryOrdersByPage",
    "QueryTradesByEMTID",
    "QueryTrades",
    "QueryTradesByPage",
    "QueryPosition",
    "QueryPositionByPage",
    "QueryAsset",
    "QueryFund",
    "FundTransfer",
    "QueryFundTransfer",
    "QueryOtherServerFund",
    "QueryETF",
    "QueryETFTickerBasket",
    "QueryETFByPage",
    "QueryIPOInfoList",
    "QueryIPOQuotaInfo",
    "QueryOptionAuctionInfo",
    "QueryIssueInfoList",
    "QuerySecurityInfo",
    "QuerySecurityByPage",
    "QueryHKConnectIndicativeExchangeRateInfo",
    "QueryHkConnectSpreadTable",
    "QueryYesterdayAsset",
    "CreditCashRepay",
    "CreditCashRepaySpecifiedDebt",
    "CreditSellStockRepayDebtInterestFee",
    "CreditExtendDebtDate",
    "CreditQuotaTransfer",
    "CreditMarginAvlTransfer",
    "QueryCreditCashRepayInfo",
    "QueryCreditFundInfo",
    "QueryCreditDebtInfo",
    "QueryCreditDebtInfoByPage",
    "QueryCreditTickerDebtInfo",
    "QueryCreditAssetDebtInfo",
    "QueryCreditTickerAssignInfo",
    "QueryCreditTickerAssignInfoByPage",
    "QueryCreditExcessStock",
    "QueryMulCreditExcessStock",
    "QueryCreditExtendDebtDateOrders",
    "QueryCreditFundExtraInfo",
    "QueryCreditPositionExtraInfo",
    "QueryCreditPledgeStkRate",
    "QueryCreditMarginRate",
    "QueryCreditPositionFullRate",
    "QueryCreditPledgeStkByPage",
    "QueryCreditTargetStkByPage",
    "QueryCreditDebtToRepayTodayByPage",
    "QueryCreditQuotaTransfer",
    "QueryCreditMarginAvlTransfer",
    "InsertOptionExerciseOrder",
    "InsertOptionCombinedOrder",
    "QueryOptionCombinedUnfinishedOrders",
    "QueryOptionCombinedOrderByEMTID",
    "QueryOptionCombinedOrders",
    "QueryOptionCombinedOrdersByPage",
    "QueryOptionCombinedTradesByEMTID",
    "QueryOptionCombinedTrades",
    "QueryOptionCombinedTradesByPage",
    "QueryOptionCombinedPosition",
    "QueryOptionCombinedStrategyInfo",
    "QueryOptionCombinedExecPosition",
    "QueryEndpointId",
    "QueryDMATickerAssignInfoByPage",
    "QueryBlackWhiteListByPage",
)


class CapabilityStatus(str, Enum):
    """Whether the official operation can reach the loaded binding."""

    DIRECT = "direct"
    ADAPTER = "adapter"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class CapabilityEntry:
    """One official method and its observed Python binding route."""

    official_name: str
    binding_name: str
    status: CapabilityStatus
    reason: str | None = None


@dataclass(frozen=True)
class CapabilityReport:
    """Immutable capability evidence suitable for startup logs or a runbook."""

    entries: tuple[CapabilityEntry, ...]

    @property
    def direct_count(self) -> int:
        return sum(entry.status is CapabilityStatus.DIRECT for entry in self.entries)

    @property
    def adapter_count(self) -> int:
        return sum(entry.status is CapabilityStatus.ADAPTER for entry in self.entries)

    @property
    def unsupported_count(self) -> int:
        return sum(entry.status is CapabilityStatus.UNSUPPORTED for entry in self.entries)

    @property
    def complete(self) -> bool:
        return self.unsupported_count == 0

    def entry_for(self, official_name: str) -> CapabilityEntry:
        for entry in self.entries:
            if entry.official_name == official_name:
                return entry
        raise KeyError(f"unknown official TraderApi method: {official_name}")

    def as_dict(self) -> dict[str, object]:
        """Return JSON-serializable capability evidence without native payloads."""

        return {
            "official_count": len(self.entries),
            "direct_count": self.direct_count,
            "adapter_count": self.adapter_count,
            "unsupported_count": self.unsupported_count,
            "complete": self.complete,
            "entries": [{**asdict(entry), "status": entry.status.value} for entry in self.entries],
        }


def python_api_name(official_name: str) -> str:
    """Return the documented lower-camel EMT Python-binding method name."""

    if official_name not in OFFICIAL_TRADER_API:
        raise ValueError(f"unknown official TraderApi method: {official_name}")
    return official_name[0].lower() + official_name[1:]


def inspect_trader_api(api: object | type[Any]) -> CapabilityReport:
    """Inspect a loaded ``TraderApi`` class or instance without calling EMT.

    ``RegisterSpi`` is deliberately marked ``adapter``: the Python binding
    registers callbacks by subclassing ``TraderApi`` through
    :func:`emt_gateway.spi_bridge.build_trader_spi_bridge`, not by exposing a
    separately callable registration method.
    """

    entries: list[CapabilityEntry] = []
    for official_name in OFFICIAL_TRADER_API:
        if official_name == "RegisterSpi":
            entries.append(
                CapabilityEntry(
                    official_name=official_name,
                    binding_name="build_trader_spi_bridge",
                    status=CapabilityStatus.ADAPTER,
                    reason="Python binding uses a TraderApi subclass as TraderSpi",
                )
            )
            continue
        binding_name = python_api_name(official_name)
        if _has_callable_attribute(api, binding_name):
            entries.append(CapabilityEntry(official_name, binding_name, CapabilityStatus.DIRECT))
        else:
            entries.append(
                CapabilityEntry(
                    official_name,
                    binding_name,
                    CapabilityStatus.UNSUPPORTED,
                    reason="loaded binding exposes no callable method with this name",
                )
            )
    return CapabilityReport(tuple(entries))


def require_direct_capabilities(report: CapabilityReport, official_names: Iterable[str]) -> None:
    """Fail startup explicitly when a facade needs an unavailable direct method."""

    unavailable = [
        entry.official_name
        for name in official_names
        for entry in (report.entry_for(name),)
        if entry.status is not CapabilityStatus.DIRECT
    ]
    if unavailable:
        raise RuntimeError(
            "loaded EMT binding cannot provide required direct methods: " + ", ".join(unavailable)
        )


def _has_callable_attribute(api: object | type[Any], name: str) -> bool:
    try:
        return callable(getattr(api, name))
    except (AttributeError, TypeError):
        return False
