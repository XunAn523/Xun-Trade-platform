from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_fund_transfer_pair import (
    FundTransferPairSettings,
    _invoke_fund_transfer_command,
    run_fund_transfer_pair,
    select_fund_transfer_plan,
)
from perfbench.mutation_fixture import (
    MutationFixtureEntry,
    load_mutation_fixture,
    mutation_fixture_template,
)
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from perfbench.trace import GatewayTraceCollector


def _accounts() -> SimulatedAccounts:
    profile = SimulatedAccount(account="simulated", password="secret")
    return SimulatedAccounts(
        host="127.0.0.1", port=1, ordinary=profile, credit=profile, option=profile
    )


def _fixture() -> dict[str, object]:
    fixture = mutation_fixture_template()
    fixture["safety"]["armed"] = True  # type: ignore[index]  # loaded JSON fixture is heterogeneous
    entry = next(
        item
        for item in fixture["cases"]  # type: ignore[index]  # loaded JSON fixture is heterogeneous
        if item["api"] == "FundTransfer" and item["profile"] == "ordinary"
    )
    entry.update(
        {
            "enabled": True,
            "request": {"amount": 1.0, "transfer_type": 0},
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryFundTransfer",
        }
    )
    return fixture


class FundTransferPairTests(unittest.TestCase):
    def test_selects_the_single_bounded_fund_transfer(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(_fixture()), encoding="utf-8")
            plan = select_fund_transfer_plan(load_mutation_fixture(path, profile="ordinary"))
        self.assertEqual("FundTransfer", plan.entry.api_name)

    def test_rejects_unbounded_amount(self) -> None:
        fixture = _fixture()
        entry = next(item for item in fixture["cases"] if item["api"] == "FundTransfer")  # type: ignore[index]  # loaded JSON fixture is heterogeneous
        entry["request"] = {"amount": 1.01, "transfer_type": 0}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "no greater than one"):
                select_fund_transfer_plan(load_mutation_fixture(path, profile="ordinary"))

    def test_preflight_does_not_load_sdk_or_log_in(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(_fixture()), encoding="utf-8")
            report = run_fund_transfer_pair(
                FundTransferPairSettings(
                    sdk_root=Path("sdk-not-loaded"),
                    runtime_dir=Path(directory) / "runtime",
                    accounts=_accounts(),
                    fixture=path,
                    pairs=2,
                )
            )
        self.assertFalse(report["executed"])
        self.assertEqual(4, report["run"]["maximum_commands"])

    def test_settings_reject_below_one_second_interval(self) -> None:
        settings = FundTransferPairSettings(
            sdk_root=Path("sdk"),
            runtime_dir=Path("runtime"),
            accounts=_accounts(),
            fixture=Path("fixture"),
            interval_seconds=0.9,
        )
        with self.assertRaisesRegex(RuntimeError, "at or above one second"):
            settings.validate()

    def test_positive_native_serial_is_retained_for_callback_correlation(self) -> None:
        class Journal:
            def append(self, **_: object) -> None:
                return None

        class Facade:
            def invoke(self, *_: object, **__: object) -> object:
                return type("Invocation", (), {"return_value": 12345})()

        source = _invoke_fund_transfer_command(
            entry=MutationFixtureEntry(
                api_name="FundTransfer",
                profile="ordinary",
                enabled=True,
                request={"amount": 1.0, "transfer_type": 0},
                rate_limit_per_second=1,
                reconciliation_query="QueryFundTransfer",
            ),
            facade=Facade(),  # type: ignore[arg-type]  # test double implements only exercised facade calls
            trace=GatewayTraceCollector(),
            journal=Journal(),  # type: ignore[arg-type]  # test double implements only exercised journal calls
            fund_account="simulated",
            password="secret",
            serial_id=54321,
        )
        self.assertIsNotNone(source)
        assert source is not None
        self.assertEqual(12345, source.native_id)
