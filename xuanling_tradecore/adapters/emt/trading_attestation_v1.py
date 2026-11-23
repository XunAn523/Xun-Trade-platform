"""Non-invasive ABI attestation for the EMT trading-only catalog."""

from __future__ import annotations

from typing import Any

from .trading_catalog_v1 import (
    inspect_trader_api_capabilities_v1,
    inspect_trader_spi_capabilities_v1,
    known_linux_binding_blockers_v1,
)
from .trading_readiness_v1 import _catalog_sha256
from .trading_spi_runtime_v1 import build_complete_trader_spi_v1


class _InertCallbackIngress:
    """Build-time bridge seam; attestation never instantiates an SDK session."""

    def offer(self, **_: object) -> bool:
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        return None


def build_trading_abi_attestation_v1(trader_api_base: type[Any]) -> dict[str, object]:
    """Return a JSON-ready 86/69 ABI report without login, query or mutation."""

    api_entries = inspect_trader_api_capabilities_v1(trader_api_base)
    spi_bridge = build_complete_trader_spi_v1(
        trader_api_base,
        callback_ingress=_InertCallbackIngress(),
    )
    spi_entries = inspect_trader_spi_capabilities_v1(spi_bridge)
    return {
        "schema": "tradecore.emt.trading-abi-attestation.v1",
        "catalog_sha256": _catalog_sha256(),
        "scope": "TraderApi-and-TraderSpi-only; QuoteApi-excluded",
        "api": [
            {
                "official_name": entry.spec.official_name,
                "binding_name": entry.spec.binding_name,
                "domain": entry.spec.domain.value,
                "lane": entry.spec.lane.value,
                "state": entry.state.value,
                "reason": entry.reason,
            }
            for entry in api_entries
        ],
        "spi": [
            {
                "official_name": entry.spec.official_name,
                "binding_name": entry.spec.binding_name,
                "priority": entry.spec.priority.value,
                "present": entry.present,
                "reason": entry.reason,
            }
            for entry in spi_entries
        ],
        "known_linux_binding_blockers": list(known_linux_binding_blockers_v1()),
        "counts": {
            "TraderApi": len(api_entries),
            "TraderSpi": len(spi_entries),
            "api_supported_or_adapter_owned": sum(
                entry.state.value in {"supported", "adapter_owned"} for entry in api_entries
            ),
            "spi_present": sum(entry.present for entry in spi_entries),
        },
    }
