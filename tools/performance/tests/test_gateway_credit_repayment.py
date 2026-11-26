from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_credit_repayment import (
    CreditQuotaTransferSettings,
    CreditRepaymentError,
    CreditRepaymentSettings,
    _native_last_error_id,
    _quota_reconciliation_args,
    _release_or_retain_credit_session,
    run_credit_quota_transfer,
    run_credit_repayment,
    select_credit_quota_transfer_plan,
    select_credit_repayment_plan,
)
from perfbench.mutation_fixture import MutationFixtureEntry, mutation_fixture_template
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def _entry(api_name: str, request: dict[object, object]) -> MutationFixtureEntry:
    return MutationFixtureEntry(
        api_name=api_name,
        profile="credit",
        enabled=True,
        request=request,  # type: ignore[arg-type]  # synthetic request exercises adapter mapping only
        rate_limit_per_second=1,
        reconciliation_query="QueryCreditCashRepayInfo",
    )


class CreditRepaymentTests(unittest.TestCase):
    def test_isolated_credit_worker_retains_session_instead_of_closing(self) -> None:
        class Session:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        session = Session()
        retained: list[object] = []
        _release_or_retain_credit_session(session, retained.append)
        self.assertEqual([session], retained)
        self.assertFalse(session.closed)

    def test_non_isolated_credit_worker_closes_session(self) -> None:
        class Session:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        session = Session()
        _release_or_retain_credit_session(session, None)
        self.assertTrue(session.closed)

    def test_native_zero_rejection_uses_only_integer_last_error_id(self) -> None:
        class Invocation:
            def __init__(self, value: object) -> None:
                self.return_value = value

        class Facade:
            def __init__(self, value: object) -> None:
                self.value = value

            def invoke(self, name: str) -> Invocation:
                self.name = name
                return Invocation(self.value)

        facade = Facade({"error_id": 230113, "error_msg": "not exported"})
        self.assertEqual(230113, _native_last_error_id(facade))
        self.assertEqual("GetApiLastError", facade.name)
        self.assertIsNone(_native_last_error_id(Facade({"error_id": "230113"})))

    def test_plan_accepts_only_tiny_reviewed_credit_repayments(self) -> None:
        plan = select_credit_repayment_plan(
            (
                _entry("CreditCashRepay", {"amount": 1.0}),
                _entry(
                    "CreditCashRepaySpecifiedDebt",
                    {
                        "request_amount": 1.0,
                        "order_date": 20211231,
                        "debt_id": "local-debt",
                        "debt_type": 0,
                    },
                ),
            )
        )
        self.assertEqual(
            ("CreditCashRepay", "CreditCashRepaySpecifiedDebt"),
            tuple(entry.api_name for entry in plan.entries),
        )

    def test_plan_rejects_expansive_or_unbounded_credit_requests(self) -> None:
        with self.assertRaisesRegex(CreditRepaymentError, "amount"):
            select_credit_repayment_plan((_entry("CreditCashRepay", {"amount": 1.01}),))
        with self.assertRaisesRegex(CreditRepaymentError, "unsupported"):
            select_credit_repayment_plan((_entry("CreditQuotaTransfer", {"amount": 1.0}),))

    def test_plan_accepts_one_isolated_nonresting_credit_sell_repay(self) -> None:
        plan = select_credit_repayment_plan(
            (
                MutationFixtureEntry(
                    api_name="CreditSellStockRepayDebtInterestFee",
                    profile="credit",
                    enabled=True,
                    request={
                        "order": {
                            "ticker": "123456",
                            "market": 1,
                            "side": 23,
                            "price_type": 4,
                            "price": 0,
                            "quantity": 100,
                            "business_type": 4,
                        },
                        "debt_id": "local-debt",
                        "order_client_id": 901000001,
                    },
                    rate_limit_per_second=1,
                    reconciliation_query="QueryOrderByEMTID",
                ),
            )
        )
        self.assertEqual(("CreditSellStockRepayDebtInterestFee",), tuple(item.api_name for item in plan.entries))

    def test_plan_rejects_resting_or_mixed_credit_sell_repay(self) -> None:
        sell = MutationFixtureEntry(
            api_name="CreditSellStockRepayDebtInterestFee",
            profile="credit",
            enabled=True,
            request={
                "order": {
                    "ticker": "123456",
                    "market": 2,
                    "side": 23,
                    "price_type": 1,
                    "price": 1.0,
                    "quantity": 100,
                    "business_type": 4,
                },
                "debt_id": "local-debt",
                "order_client_id": 901000001,
            },
            rate_limit_per_second=1,
            reconciliation_query="QueryOrderByEMTID",
        )
        with self.assertRaisesRegex(CreditRepaymentError, "non-resting"):
            select_credit_repayment_plan((sell,))
        valid_request = {
            "order": {
                "ticker": "123456",
                "market": 2,
                "side": 23,
                "price_type": 2,
                "price": 0,
                "quantity": 100,
                "business_type": 4,
            },
            "debt_id": "local-debt",
            "order_client_id": 901000001,
        }
        with self.assertRaisesRegex(CreditRepaymentError, "isolated"):
            select_credit_repayment_plan(
                (
                    MutationFixtureEntry(
                        api_name="CreditSellStockRepayDebtInterestFee",
                        profile="credit",
                        enabled=True,
                        request=valid_request,
                        rate_limit_per_second=1,
                        reconciliation_query="QueryOrderByEMTID",
                    ),
                    _entry("CreditCashRepay", {"amount": 1.0}),
                )
            )

    def test_preflight_never_loads_sdk_or_sends_a_credit_repayment(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )
        with TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.json"
            template = mutation_fixture_template()
            template["safety"]["armed"] = True
            entry = next(
                item
                for item in template["cases"]
                if item["api"] == "CreditCashRepay" and item["profile"] == "credit"
            )
            entry.update(
                {
                    "enabled": True,
                    "request": {"amount": 1.0},
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryCreditCashRepayInfo",
                }
            )
            fixture.write_text(json.dumps(template), encoding="utf-8")
            report = run_credit_repayment(
                CreditRepaymentSettings(
                    sdk_root=Path(directory) / "sdk-not-loaded",
                    runtime_dir=Path(directory) / "runtime",
                    accounts=accounts,
                    fixture=fixture,
                )
            )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(["CreditCashRepay"], report["run"]["planned_commands"])

    def test_cycle_guard_rejects_more_than_thirty_commands(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )
        with self.assertRaisesRegex(CreditRepaymentError, "1..30"):
            CreditRepaymentSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                fixture=Path("fixture"),
                cycles=31,
            ).validate()

    def test_quota_plan_accepts_only_a_one_unit_status_callback_probe(self) -> None:
        plan = select_credit_quota_transfer_plan(
            (
                MutationFixtureEntry(
                    api_name="CreditQuotaTransfer",
                    profile="credit",
                    enabled=True,
                    request={"serial_id": 0, "amount": 1.0, "transfer_type": 1},
                    rate_limit_per_second=1,
                    reconciliation_query="QueryCreditQuotaTransfer",
                ),
            )
        )
        self.assertEqual("CreditQuotaTransfer", plan.entry.api_name)
        invalid = MutationFixtureEntry(
            api_name="CreditQuotaTransfer",
            profile="credit",
            enabled=True,
            request={"serial_id": 1, "amount": 1.0, "transfer_type": 1},
            rate_limit_per_second=1,
            reconciliation_query="QueryCreditQuotaTransfer",
        )
        with self.assertRaisesRegex(CreditRepaymentError, "serial_id"):
            select_credit_quota_transfer_plan((invalid,))
        self.assertEqual(({"serial_id": 0},), _quota_reconciliation_args())

    def test_quota_preflight_never_loads_sdk_or_sends_a_transfer(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )
        with TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.json"
            template = mutation_fixture_template()
            template["safety"]["armed"] = True
            entry = next(
                item
                for item in template["cases"]
                if item["api"] == "CreditQuotaTransfer" and item["profile"] == "credit"
            )
            entry.update(
                {
                    "enabled": True,
                    "request": {"serial_id": 0, "amount": 1.0, "transfer_type": 1},
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryCreditQuotaTransfer",
                }
            )
            fixture.write_text(json.dumps(template), encoding="utf-8")
            report = run_credit_quota_transfer(
                CreditQuotaTransferSettings(
                    sdk_root=Path(directory) / "sdk-not-loaded",
                    runtime_dir=Path(directory) / "runtime",
                    accounts=accounts,
                    fixture=fixture,
                )
            )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
