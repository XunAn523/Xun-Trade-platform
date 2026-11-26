"""Static EMT 2.27 trading-only catalog and capability gate.

This is the OMS/EMS-facing catalog for the complete ``TraderApi`` / ``TraderSpi``
surface.  It intentionally reuses the platform package's single official-name
source of truth instead of creating a second vendor binding table.  It excludes
``QuoteApi`` by construction and does not import the vendor SDK.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from ...compatibility.emt_trading_platform import (
    OFFICIAL_TRADER_API,
    OFFICIAL_TRADER_SPI,
    CapabilityReport,
    CapabilityStatus,
    inspect_trader_api,
    python_api_name,
    python_callback_name,
)


class EmtTradingCatalogError(RuntimeError):
    """The static EMT trading catalog or runtime binding is inconsistent."""


class EmtTradingDomain(str, Enum):
    LIFECYCLE = "lifecycle"
    SECURITIES = "securities"
    FUND = "fund"
    CREDIT = "credit"
    OPTION = "option"
    REFERENCE = "reference"
    DMA = "dma"


class EmtOperationLane(str, Enum):
    """The only permitted scheduling domains for EMT trading work."""

    FAST_EXECUTION = "fast_execution"
    CONTROL_MUTATION = "control_mutation"
    QUERY = "query"
    SESSION_CONTROL = "session_control"


class EmtCallbackPriority(str, Enum):
    ORDER_CRITICAL = "order_critical"
    CONTROL_CRITICAL = "control_critical"
    SESSION_CRITICAL = "session_critical"
    QUERY_LOW = "query_low"


class EmtCapabilityState(str, Enum):
    SUPPORTED = "supported"
    ADAPTER_OWNED = "adapter_owned"
    UNSUPPORTED_BINDING = "unsupported_binding"


@dataclass(frozen=True)
class TraderApiSpecV1:
    official_name: str
    binding_name: str
    domain: EmtTradingDomain
    lane: EmtOperationLane
    mutating: bool
    expected_callback: str | None


@dataclass(frozen=True)
class TraderSpiSpecV1:
    official_name: str
    binding_name: str
    domain: EmtTradingDomain
    priority: EmtCallbackPriority


@dataclass(frozen=True)
class TraderApiCapabilityV1:
    spec: TraderApiSpecV1
    state: EmtCapabilityState
    reason: str | None


@dataclass(frozen=True)
class TraderSpiCapabilityV1:
    spec: TraderSpiSpecV1
    present: bool
    reason: str | None


_SESSION_OWNED = frozenset(
    {
        "CreateTraderApi",
        "SetCpuAffinity",
        "SetSoftwareVersion",
        "SetHeartBeatInterval",
        "SubscribePublicTopic",
        "RegisterSpi",
        "Login",
        "Logout",
        "Release",
    }
)
_FAST_MUTATIONS = frozenset(
    {
        "InsertOrder",
        "CancelOrder",
        "CreditSellStockRepayDebtInterestFee",
        "InsertOptionCombinedOrder",
    }
)
_CONTROL_MUTATIONS = frozenset(
    {
        "FundTransfer",
        "CreditCashRepay",
        "CreditCashRepaySpecifiedDebt",
        "CreditExtendDebtDate",
        "CreditQuotaTransfer",
        "CreditMarginAvlTransfer",
        "InsertOptionExerciseOrder",
    }
)
_QUERY_CALLBACK_OVERRIDES = {
    "QueryOrderByEMTID": "OnQueryOrder",
    "QueryOrders": "OnQueryOrder",
    "QueryOrdersByPage": "OnQueryOrderByPage",
    "QueryUnfinishedOrders": "OnQueryOrder",
    "QueryTradesByEMTID": "OnQueryTrade",
    "QueryTrades": "OnQueryTrade",
    "QueryTradesByPage": "OnQueryTradeByPage",
    "QueryETFTickerBasket": "OnQueryETFBasket",
    "QueryOptionCombinedUnfinishedOrders": "OnQueryOptionCombinedOrders",
    "QueryOptionCombinedOrderByEMTID": "OnQueryOptionCombinedOrders",
    "QueryOptionCombinedTradesByEMTID": "OnQueryOptionCombinedTrades",
}
_KNOWN_MISSING_LINUX_BINDING = frozenset(
    {
        "CreditMarginAvlTransfer",
        "QueryCreditMarginAvlTransfer",
    }
)
_KNOWN_MISSING_LINUX_CALLBACKS = frozenset(
    {
        "OnCreditMarginAvlTransfer",
        "OnQueryCreditMarginAvlTransfer",
    }
)


def _domain_for(name: str) -> EmtTradingDomain:
    if name in _SESSION_OWNED or name.startswith(("Get", "Set")) or name == "IsServerRestart":
        return EmtTradingDomain.LIFECYCLE
    if "Credit" in name:
        return EmtTradingDomain.CREDIT
    if "Option" in name:
        return EmtTradingDomain.OPTION
    if "Fund" in name or "Asset" in name:
        return EmtTradingDomain.FUND
    if "DMA" in name or "BlackWhite" in name:
        return EmtTradingDomain.DMA
    if any(token in name for token in ("ETF", "IPO", "Issue", "Security", "HKConnect", "HkConnect")):
        return EmtTradingDomain.REFERENCE
    return EmtTradingDomain.SECURITIES


def _api_lane_for(name: str) -> EmtOperationLane:
    if name in _SESSION_OWNED or name.startswith(("Get", "Set")) or name == "IsServerRestart":
        return EmtOperationLane.SESSION_CONTROL
    if name in _FAST_MUTATIONS:
        return EmtOperationLane.FAST_EXECUTION
    if name in _CONTROL_MUTATIONS:
        return EmtOperationLane.CONTROL_MUTATION
    if name.startswith("Query"):
        return EmtOperationLane.QUERY
    raise EmtTradingCatalogError(f"unclassified TraderApi method: {name}")


def _callback_priority_for(name: str) -> EmtCallbackPriority:
    if name in {"OnOrderEvent", "OnTradeEvent", "OnCancelOrderError", "OnOptionCombinedOrderEvent", "OnOptionCombinedTradeEvent"}:
        return EmtCallbackPriority.ORDER_CRITICAL
    if name in {"OnConnected", "OnDisconnected", "OnError"}:
        return EmtCallbackPriority.SESSION_CRITICAL
    if name.startswith("OnQuery"):
        return EmtCallbackPriority.QUERY_LOW
    return EmtCallbackPriority.CONTROL_CRITICAL


def trader_api_specs_v1() -> tuple[TraderApiSpecV1, ...]:
    """Return every official transaction/account API with an explicit lane."""

    specs: list[TraderApiSpecV1] = []
    for name in OFFICIAL_TRADER_API:
        lane = _api_lane_for(name)
        specs.append(
            TraderApiSpecV1(
                official_name=name,
                binding_name=python_api_name(name),
                domain=_domain_for(name),
                lane=lane,
                mutating=lane in {EmtOperationLane.FAST_EXECUTION, EmtOperationLane.CONTROL_MUTATION},
                expected_callback=(
                    _QUERY_CALLBACK_OVERRIDES.get(name, f"On{name}")
                    if lane is EmtOperationLane.QUERY
                    else None
                ),
            )
        )
    _require_complete_api_catalog(specs)
    return tuple(specs)


def trader_spi_specs_v1() -> tuple[TraderSpiSpecV1, ...]:
    """Return every official transaction/account callback with a priority."""

    specs = tuple(
        TraderSpiSpecV1(
            official_name=name,
            binding_name=python_callback_name(name),
            domain=_domain_for(name),
            priority=_callback_priority_for(name),
        )
        for name in OFFICIAL_TRADER_SPI
    )
    _require_complete_spi_catalog(specs)
    return specs


def trader_api_spec_v1(official_name: str) -> TraderApiSpecV1:
    try:
        return _API_SPECS_BY_NAME[official_name]
    except KeyError as exc:
        raise EmtTradingCatalogError(f"unknown TraderApi method: {official_name}") from exc


def trader_spi_spec_v1(official_name: str) -> TraderSpiSpecV1:
    try:
        return _SPI_SPECS_BY_NAME[official_name]
    except KeyError as exc:
        raise EmtTradingCatalogError(f"unknown TraderSpi callback: {official_name}") from exc


def inspect_trader_api_capabilities_v1(api: object | type[Any]) -> tuple[TraderApiCapabilityV1, ...]:
    """Classify every API against the actual binding without calling the SDK."""

    report: CapabilityReport = inspect_trader_api(api)
    result: list[TraderApiCapabilityV1] = []
    for spec in TRADER_API_SPECS_V1:
        entry = report.entry_for(spec.official_name)
        if entry.status is CapabilityStatus.DIRECT:
            state, reason = EmtCapabilityState.SUPPORTED, None
        elif entry.status is CapabilityStatus.ADAPTER:
            state, reason = EmtCapabilityState.ADAPTER_OWNED, entry.reason
        else:
            state, reason = EmtCapabilityState.UNSUPPORTED_BINDING, entry.reason
        result.append(TraderApiCapabilityV1(spec=spec, state=state, reason=reason))
    return tuple(result)


def inspect_trader_spi_capabilities_v1(bridge_type: object | type[Any]) -> tuple[TraderSpiCapabilityV1, ...]:
    """Verify that a prebuilt TraderSpi bridge exposes every official override."""

    result: list[TraderSpiCapabilityV1] = []
    for spec in TRADER_SPI_SPECS_V1:
        try:
            present = callable(getattr(bridge_type, spec.binding_name))
        except (AttributeError, TypeError):
            present = False
        result.append(
            TraderSpiCapabilityV1(
                spec=spec,
                present=present,
                reason=None if present else "bridge exposes no callable callback override",
            )
        )
    return tuple(result)


def known_linux_binding_blockers_v1() -> tuple[str, ...]:
    """Return the explicit 2.27 vendor-Python gaps, never inferred as supported."""

    return tuple(sorted(_KNOWN_MISSING_LINUX_BINDING | _KNOWN_MISSING_LINUX_CALLBACKS))


def _require_complete_api_catalog(specs: list[TraderApiSpecV1]) -> None:
    names = tuple(spec.official_name for spec in specs)
    if len(names) != 86 or names != OFFICIAL_TRADER_API or len(set(names)) != len(names):
        raise EmtTradingCatalogError("TraderApiCatalogV1 must contain the exact official 86-method list")
    if any("Quote" in name for name in names):
        raise EmtTradingCatalogError("QuoteApi methods must not enter the trading catalog")


def _require_complete_spi_catalog(specs: tuple[TraderSpiSpecV1, ...]) -> None:
    names = tuple(spec.official_name for spec in specs)
    if len(names) != 69 or names != OFFICIAL_TRADER_SPI or len(set(names)) != len(names):
        raise EmtTradingCatalogError("TraderSpiCatalogV1 must contain the exact official 69-callback list")
    if any("Quote" in name for name in names):
        raise EmtTradingCatalogError("QuoteApi callbacks must not enter the trading catalog")


TRADER_API_SPECS_V1 = trader_api_specs_v1()
TRADER_SPI_SPECS_V1 = trader_spi_specs_v1()
_API_SPECS_BY_NAME = {spec.official_name: spec for spec in TRADER_API_SPECS_V1}
_SPI_SPECS_BY_NAME = {spec.official_name: spec for spec in TRADER_SPI_SPECS_V1}
