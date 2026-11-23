"""Contract tests for the concrete outer EMT request factory."""

from __future__ import annotations

import unittest
from dataclasses import replace

from emt_gateway.tradecore_order_request import (
    EmtTradeCoreOrderRequestError,
    EmtTradeCoreOrderRequestFactory,
)
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtSubmitRequest,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _instruction(*, side: str = "BUY", business_type: str = "CASH") -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="account-1",
        trading_day=TradingDayEpoch("2026-09-04"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id="account-1",
            strategy_id=None,
            strategy_order_id=None,
            attribution_binding_id="binding-1",
            binding_version=1,
            order_origin=OrderOrigin.MANUAL,
        ),
        symbol="000001",
        market="SZ",
        side=side,
        price_type="LIMIT",
        business_type=business_type,
        quantity=100,
        protection_price_ticks=1000,
        limit_price_ticks=999,
    )


def _request(*, side: str = "BUY", business_type: str = "CASH") -> EmtSubmitRequest:
    instruction = _instruction(side=side, business_type=business_type)
    return EmtSubmitRequest(
        instruction=instruction,
        mapped_order=_mapping().map_execution(instruction),
        order_client_id=17,
    )


class TradeCoreOrderRequestFactoryTests(unittest.TestCase):
    def test_constructs_exact_bounded_cash_request(self) -> None:
        self.assertEqual(
            EmtTradeCoreOrderRequestFactory()(_request()),
            {
                "ticker": "000001",
                "market": 1,
                "side": 1,
                "price_type": 1,
                "price": 9.99,
                "quantity": 100,
                "business_type": 0,
                "order_client_id": 17,
            },
        )

    def test_preserves_manifest_mapped_margin_buy_and_short_sell(self) -> None:
        factory = EmtTradeCoreOrderRequestFactory()
        self.assertEqual(
            4, factory(_request(side="MARGIN_BUY", business_type="MARGIN"))["business_type"]
        )
        self.assertEqual(21, factory(_request(side="MARGIN_BUY", business_type="MARGIN"))["side"])
        self.assertEqual(22, factory(_request(side="SHORT_SELL", business_type="MARGIN"))["side"])

    def test_rejects_unbounded_or_unrepresentable_inputs_before_sdk_call(self) -> None:
        factory = EmtTradeCoreOrderRequestFactory()
        request = _request()
        with self.assertRaisesRegex(EmtTradeCoreOrderRequestError, "unapproved"):
            factory(
                replace(
                    request,
                    instruction=replace(request.instruction, execution_parameters={"x": "y"}),
                )
            )
        with self.assertRaisesRegex(EmtTradeCoreOrderRequestError, "positive limit"):
            factory(replace(request, instruction=replace(request.instruction, limit_price_ticks=0)))
        with self.assertRaisesRegex(EmtTradeCoreOrderRequestError, "exceeds 100"):
            factory(replace(request, instruction=replace(request.instruction, quantity=101)))


if __name__ == "__main__":
    unittest.main()
