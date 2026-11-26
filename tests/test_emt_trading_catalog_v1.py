from __future__ import annotations

import unittest

from emt_gateway.spi_bridge import DeferredCallbackIngress, build_trader_spi_bridge
from xuanling_tradecore.adapters.emt.trading_catalog_v1 import (
    TRADER_API_SPECS_V1,
    TRADER_SPI_SPECS_V1,
    EmtCallbackPriority,
    EmtCapabilityState,
    EmtOperationLane,
    inspect_trader_api_capabilities_v1,
    inspect_trader_spi_capabilities_v1,
    known_linux_binding_blockers_v1,
    trader_api_spec_v1,
)


class _CompleteApi:
    pass


for _spec in TRADER_API_SPECS_V1:
    if _spec.official_name != "RegisterSpi":
        setattr(_CompleteApi, _spec.binding_name, lambda self, *args: 0)


class EmtTradingCatalogV1Tests(unittest.TestCase):
    def test_catalog_has_exact_trading_surface_and_no_quote(self) -> None:
        self.assertEqual(86, len(TRADER_API_SPECS_V1))
        self.assertEqual(69, len(TRADER_SPI_SPECS_V1))
        self.assertFalse(any("Quote" in spec.official_name for spec in TRADER_API_SPECS_V1))
        self.assertFalse(any("Quote" in spec.official_name for spec in TRADER_SPI_SPECS_V1))

    def test_lanes_keep_queries_and_control_mutations_off_fast_lane(self) -> None:
        self.assertEqual(EmtOperationLane.FAST_EXECUTION, trader_api_spec_v1("InsertOrder").lane)
        self.assertEqual(EmtOperationLane.FAST_EXECUTION, trader_api_spec_v1("InsertOptionCombinedOrder").lane)
        self.assertEqual(EmtOperationLane.CONTROL_MUTATION, trader_api_spec_v1("FundTransfer").lane)
        self.assertEqual(EmtOperationLane.CONTROL_MUTATION, trader_api_spec_v1("CreditCashRepay").lane)
        self.assertEqual(EmtOperationLane.QUERY, trader_api_spec_v1("QueryOrders").lane)
        self.assertEqual(EmtOperationLane.QUERY, trader_api_spec_v1("QueryOptionCombinedOrders").lane)

    def test_runtime_api_capability_report_is_complete_and_fail_closed(self) -> None:
        report = inspect_trader_api_capabilities_v1(_CompleteApi)
        self.assertEqual(86, len(report))
        self.assertEqual(EmtCapabilityState.ADAPTER_OWNED, report[5].state)
        self.assertTrue(all(item.state is not EmtCapabilityState.UNSUPPORTED_BINDING for item in report))

        delattr(_CompleteApi, "creditMarginAvlTransfer")
        try:
            missing = inspect_trader_api_capabilities_v1(_CompleteApi)
            self.assertEqual(
                EmtCapabilityState.UNSUPPORTED_BINDING,
                next(item.state for item in missing if item.spec.official_name == "CreditMarginAvlTransfer"),
            )
        finally:
            setattr(_CompleteApi, "creditMarginAvlTransfer", lambda self, *args: 0)

    def test_full_spi_bridge_contains_every_override(self) -> None:
        bridge = build_trader_spi_bridge(
            _CompleteApi,
            callback_ingress=DeferredCallbackIngress(),
            account_profile="ordinary",
        )
        report = inspect_trader_spi_capabilities_v1(bridge)
        self.assertEqual(69, len(report))
        self.assertTrue(all(item.present for item in report))
        self.assertEqual(
            EmtCallbackPriority.ORDER_CRITICAL,
            next(item.spec.priority for item in report if item.spec.official_name == "OnOrderEvent"),
        )

    def test_known_vendor_binding_gaps_are_explicit(self) -> None:
        self.assertEqual(
            (
                "CreditMarginAvlTransfer",
                "OnCreditMarginAvlTransfer",
                "OnQueryCreditMarginAvlTransfer",
                "QueryCreditMarginAvlTransfer",
            ),
            known_linux_binding_blockers_v1(),
        )
