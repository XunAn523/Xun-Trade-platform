"""The sole compatibility boundary for the frozen EMT platform implementation.

New TradeCore adapters may depend on these narrow protocol/type exports but
must not import :mod:`emt_gateway` themselves.  This preserves the versioned
package boundary while the existing facade and native fastspi implementation
are incrementally composed behind the new OMS/EMS contracts.
"""

from emt_gateway.capabilities import (
    OFFICIAL_TRADER_API,
    CapabilityReport,
    CapabilityStatus,
    inspect_trader_api,
    python_api_name,
)
from emt_gateway.facade import EmtTraderFacade, NativeInvocation, SideEffectPermit
from emt_gateway.query_registry import QueryCompletion, QueryHandle
from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI, python_callback_name
from emt_gateway.spi_bridge_v2 import (
    FastSpiV2NativeIngressPort,
    build_trader_spi_v2_bridge,
    register_official_trader_spi_v2_kinds,
)

__all__ = [
    "OFFICIAL_TRADER_API",
    "OFFICIAL_TRADER_SPI",
    "CapabilityReport",
    "CapabilityStatus",
    "EmtTraderFacade",
    "FastSpiV2NativeIngressPort",
    "NativeInvocation",
    "QueryCompletion",
    "QueryHandle",
    "SideEffectPermit",
    "build_trader_spi_v2_bridge",
    "inspect_trader_api",
    "python_api_name",
    "python_callback_name",
    "register_official_trader_spi_v2_kinds",
]
