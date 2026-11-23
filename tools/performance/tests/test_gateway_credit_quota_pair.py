from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_credit_quota_pair import CreditQuotaPairSettings, run_credit_quota_pair
from perfbench.gateway_credit_repayment import CreditRepaymentError
from perfbench.mutation_fixture import mutation_fixture_template
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


class CreditQuotaPairTests(unittest.TestCase):
    def _accounts(self) -> SimulatedAccounts:
        return SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )

    def test_preflight_never_loads_sdk_or_sends_a_pair(self) -> None:
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
            report = run_credit_quota_pair(
                CreditQuotaPairSettings(
                    sdk_root=Path(directory) / "sdk-not-loaded",
                    runtime_dir=Path(directory) / "runtime",
                    accounts=self._accounts(),
                    fixture=fixture,
                    pairs=2,
                )
            )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(4, report["run"]["maximum_commands"])

    def test_pair_limit_is_fail_closed(self) -> None:
        with self.assertRaisesRegex(CreditRepaymentError, "1..30"):
            CreditQuotaPairSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=self._accounts(),
                fixture=Path("fixture"),
                pairs=31,
            ).validate()
