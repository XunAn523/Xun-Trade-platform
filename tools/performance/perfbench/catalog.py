"""The target EMT trading surface used by performance tests.

The performance matrix must consume the same TraderApi/TraderSpi catalog as
the OMS/EMS release gate.  Keeping a second hand-copied list silently omitted
new interfaces in the past; QuoteApi remains excluded by the core catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from xuanling_tradecore.adapters.emt.trading_catalog_v1 import (
    TRADER_API_SPECS_V1,
    TRADER_SPI_SPECS_V1,
    EmtCallbackPriority,
    EmtOperationLane,
)

InterfaceKind = Literal["api", "spi"]


@dataclass(frozen=True)
class InterfaceSpec:
    kind: InterfaceKind
    name: str
    tier: str
    workloads: tuple[str, ...]


TRADER_API = tuple(spec.official_name for spec in TRADER_API_SPECS_V1)
TRADER_SPI = tuple(spec.official_name for spec in TRADER_SPI_SPECS_V1)
_API_BY_NAME = {spec.official_name: spec for spec in TRADER_API_SPECS_V1}
_SPI_BY_NAME = {spec.official_name: spec for spec in TRADER_SPI_SPECS_V1}


def api_tier(name: str) -> str:
    try:
        lane = _API_BY_NAME[name].lane
    except KeyError as exc:
        raise ValueError(f"unclassified TraderApi method: {name}") from exc
    if lane is EmtOperationLane.SESSION_CONTROL:
        return "LIFE"
    if lane is EmtOperationLane.FAST_EXECUTION:
        return "CMD-H"
    if lane is EmtOperationLane.CONTROL_MUTATION:
        return "CMD-A"
    if lane is EmtOperationLane.QUERY:
        return "QUERY"
    raise ValueError(f"unclassified TraderApi method: {name}")


def spi_tier(name: str) -> str:
    try:
        priority = _SPI_BY_NAME[name].priority
    except KeyError as exc:
        raise ValueError(f"unclassified TraderSpi callback: {name}") from exc
    if priority is EmtCallbackPriority.SESSION_CRITICAL:
        return "CB-S"
    if priority is EmtCallbackPriority.ORDER_CRITICAL:
        return "CB-H"
    if priority is EmtCallbackPriority.QUERY_LOW:
        return "CB-Q"
    return "CB-A"


def workloads_for(tier: str) -> tuple[str, ...]:
    return {
        "LIFE": ("cold", "failure"),
        "CMD-H": ("steady", "burst", "throttled", "recovery"),
        "CMD-A": ("steady", "failure", "recovery"),
        "QUERY": ("steady", "burst", "empty", "pagination", "late"),
        "CB-S": ("reconnect", "storm"),
        "CB-H": ("steady", "burst", "duplicate", "out_of_order", "slow_consumer"),
        "CB-A": ("steady", "burst", "failure", "slow_consumer"),
        "CB-Q": ("steady", "burst", "empty", "pagination", "late"),
    }[tier]


def all_specs() -> tuple[InterfaceSpec, ...]:
    api_specs = tuple(
        InterfaceSpec("api", name, api_tier(name), workloads_for(api_tier(name)))
        for name in TRADER_API
    )
    spi_specs = tuple(
        InterfaceSpec("spi", name, spi_tier(name), workloads_for(spi_tier(name)))
        for name in TRADER_SPI
    )
    return api_specs + spi_specs
