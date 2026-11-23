from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_mutation import GatewayMutationError
from perfbench.gateway_trade_fill import (
    TradeFillSettings,
    run_gateway_trade_fill,
    select_trade_fill_plan,
)
from perfbench.mutation_fixture import load_mutation_fixture, mutation_fixture_template
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


class GatewayTradeFillTests(unittest.TestCase):
    def _fixture(self, profile: str = "ordinary") -> dict[str, object]:
        fixture = mutation_fixture_template()
        fixture["safety"]["armed"] = True  # type: ignore[index]  # loaded JSON fixture is heterogeneous
        entry = next(
            item
            for item in fixture["cases"]  # type: ignore[index]  # loaded JSON fixture is heterogeneous
            if item["api"] == "InsertOrder" and item["profile"] == profile
        )
        entry.update(
            {
                "enabled": True,
                "request": {
                    "ticker": "000001",
                    "market": 1,
                    "side": 2,
                    "price_type": 4,
                    "price": 0,
                    "quantity": 100,
                    "business_type": 0,
                },
                "rate_limit_per_second": 1,
                "reconciliation_query": "OnTradeEvent",
            }
        )
        return fixture

    def test_preflight_is_bounded_and_never_loads_sdk(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(self._fixture()), encoding="utf-8")
            plan = select_trade_fill_plan(
                load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
            )
            report = run_gateway_trade_fill(
                TradeFillSettings(
                    sdk_root=Path(directory) / "not-loaded-sdk",
                    runtime_dir=Path(directory) / "runtime",
                    accounts=SimulatedAccounts(
                        host="test-host",
                        port=1,
                        ordinary=SimulatedAccount("ordinary", "password"),
                        credit=SimulatedAccount("credit", "password"),
                        option=SimulatedAccount("option", "password"),
                    ),
                    fixture=path,
                    profile="ordinary",
                    cycles=100,
                )
            )
        self.assertEqual(4, plan.order_request["price_type"])
        self.assertEqual(100, plan.order_request["quantity"])
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(100, report["run"]["maximum_commands"])

    def test_plan_accepts_buy_and_rejects_invalid_side(self) -> None:
        fixture = self._fixture()
        insert = next(item for item in fixture["cases"] if item["enabled"])  # type: ignore[index]  # loaded JSON fixture is heterogeneous
        insert["request"]["side"] = 1  # type: ignore[index]  # selected fixture entry contains request
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            self.assertEqual(
                1,
                select_trade_fill_plan(
                    load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
                ).order_request["side"],
            )
            insert["request"]["side"] = 3  # type: ignore[index]  # selected fixture entry contains request
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(GatewayMutationError, "cash buy or sell side"):
                select_trade_fill_plan(
                    load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
                )

    def test_plan_requires_market_specific_nonresting_price_type(self) -> None:
        fixture = self._fixture()
        insert = next(item for item in fixture["cases"] if item["enabled"])  # type: ignore[index]  # selected fixture entry contains request
        insert["request"]["price_type"] = 2  # type: ignore[index]  # intentionally invalid for Shanghai
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(GatewayMutationError, "market-specific"):
                select_trade_fill_plan(
                    load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
                )
            insert["request"]["market"] = 2  # type: ignore[index]  # Shenzhen uses type 2
            path.write_text(json.dumps(fixture), encoding="utf-8")
            self.assertEqual(
                2,
                select_trade_fill_plan(
                    load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
                ).order_request["price_type"],
            )

    def test_plan_rejects_more_than_one_enabled_command(self) -> None:
        fixture = self._fixture()
        extra = next(
            item
            for item in fixture["cases"]  # type: ignore[index]  # loaded JSON fixture is heterogeneous
            if item["api"] == "CancelOrder" and item["profile"] == "ordinary"
        )
        extra.update(
            {
                "enabled": True,
                "request": {},
                "rate_limit_per_second": 1,
                "reconciliation_query": "OnTradeEvent",
            }
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(GatewayMutationError, "exactly one InsertOrder"):
                select_trade_fill_plan(
                    load_mutation_fixture(path, profile="ordinary"), profile="ordinary"
                )
