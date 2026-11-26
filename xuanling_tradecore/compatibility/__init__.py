"""The only TradeCore namespace permitted to bridge frozen P0 compatibility."""

from .emt_trading_platform import (
    OFFICIAL_TRADER_API,
    OFFICIAL_TRADER_SPI,
    CapabilityReport,
    CapabilityStatus,
    EmtTraderFacade,
    FastSpiV2NativeIngressPort,
    NativeInvocation,
    QueryCompletion,
    QueryHandle,
    SideEffectPermit,
    build_trader_spi_v2_bridge,
    inspect_trader_api,
    python_api_name,
    python_callback_name,
    register_official_trader_spi_v2_kinds,
)
from .legacy_order_gateway import (
    LegacyCanonicalInstruction,
    LegacyInvocationContext,
    LegacyOmsPort,
    LegacyOrderGatewayBridge,
    LegacyOrderRequest,
)

__all__ = [
    "LegacyCanonicalInstruction",
    "LegacyInvocationContext",
    "LegacyOmsPort",
    "LegacyOrderGatewayBridge",
    "LegacyOrderRequest",
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
