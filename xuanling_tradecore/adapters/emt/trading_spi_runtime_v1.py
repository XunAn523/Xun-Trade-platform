"""Configuration-time composition for the complete EMT TraderSpi surface.

The hot callback path already lives in :mod:`emt_gateway.spi_bridge_v2`: it
copies fixed scalars directly into the native fastspi V2 ingress.  This module
does not add a wrapper callback, a lock, a metric or an allocation at C5.  It
only proves at startup that the immutable 69-callback table, native gap-latch
registration and generated Python overrides all describe the same ABI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ...compatibility.emt_trading_platform import (
    FastSpiV2NativeIngressPort,
    build_trader_spi_v2_bridge,
    register_official_trader_spi_v2_kinds,
)
from .trading_catalog_v1 import (
    TRADER_SPI_SPECS_V1,
    EmtCallbackPriority,
    EmtTradingCatalogError,
    inspect_trader_spi_capabilities_v1,
)


class EmtTradingSpiRuntimeError(RuntimeError):
    """The complete TraderSpi bridge cannot be safely composed."""


class EmtSpiCallbackKindRegistrarV1(Protocol):
    """Native configuration seam used before a session is created."""

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None: ...


@dataclass(frozen=True)
class EmtTraderSpiBindingV1:
    """Immutable ABI evidence for one generated callback override."""

    callback_kind: int
    official_name: str
    binding_name: str
    priority: EmtCallbackPriority


def trader_spi_bindings_v1() -> tuple[EmtTraderSpiBindingV1, ...]:
    """Return the only permitted native-code assignment for all 69 callbacks."""

    bindings = tuple(
        EmtTraderSpiBindingV1(
            callback_kind=index,
            official_name=spec.official_name,
            binding_name=spec.binding_name,
            priority=spec.priority,
        )
        for index, spec in enumerate(TRADER_SPI_SPECS_V1, start=1)
    )
    if len(bindings) != 69 or tuple(item.official_name for item in bindings) != tuple(
        spec.official_name for spec in TRADER_SPI_SPECS_V1
    ):
        raise EmtTradingSpiRuntimeError("TraderSpi callback ABI table is incomplete")
    return bindings


TRADER_SPI_BINDINGS_V1 = trader_spi_bindings_v1()


def register_complete_trader_spi_v1(registrar: EmtSpiCallbackKindRegistrarV1) -> None:
    """Register the official table before login; no runtime registration exists."""

    # Reuse the platform implementation so fastspi and this OMS/EMS catalog
    # cannot silently diverge.
    register_official_trader_spi_v2_kinds(registrar)


def build_complete_trader_spi_v1(
    trader_api_base: type[Any],
    *,
    callback_ingress: FastSpiV2NativeIngressPort,
) -> type[Any]:
    """Build and validate a full V2 bridge without opening an EMT session.

    The returned class has one explicit Python override per official callback;
    callback execution itself remains the zero-domain-object implementation in
    ``emt_gateway.spi_bridge_v2``.  A missing override is a startup failure,
    never a best-effort callback path.
    """

    bridge_type = build_trader_spi_v2_bridge(
        trader_api_base,
        callback_ingress=callback_ingress,
    )
    missing = tuple(
        entry.spec.official_name
        for entry in inspect_trader_spi_capabilities_v1(bridge_type)
        if not entry.present
    )
    if missing:
        raise EmtTradingSpiRuntimeError(
            "generated TraderSpi bridge is missing official callbacks: " + ", ".join(missing)
        )
    return bridge_type


def callback_priority_for_kind_v1(callback_kind: int) -> EmtCallbackPriority:
    """Resolve a priority only after native kind decoding or at startup."""

    if type(callback_kind) is not int or not 1 <= callback_kind <= len(TRADER_SPI_BINDINGS_V1):
        raise EmtTradingCatalogError("unknown TraderSpi callback kind")
    return TRADER_SPI_BINDINGS_V1[callback_kind - 1].priority
