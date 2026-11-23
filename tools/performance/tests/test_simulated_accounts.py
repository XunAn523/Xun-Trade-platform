from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.simulated_accounts import AccountConfigurationError, load_simulated_accounts


class SimulatedAccountsTests(unittest.TestCase):
    def test_loads_three_accounts(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "accounts.json"
            path.write_text(
                json.dumps(
                    {
                        "gateway": {"host": "127.0.0.1", "port": 19088},
                        "ordinary": {"account": "ordinary", "password": "one"},
                        "credit": {"account": "credit", "password": "two"},
                        "option": {"account": "option", "password": "three"},
                    }
                ),
                encoding="utf-8",
            )
            settings = load_simulated_accounts(path)
        self.assertEqual("127.0.0.1", settings.host)
        self.assertEqual("credit", settings.credit.account)

    def test_rejects_missing_password(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "accounts.json"
            path.write_text("{}", encoding="utf-8")
            with self.assertRaises(AccountConfigurationError):
                load_simulated_accounts(path)

    def test_rejects_unreplaced_template_values(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "accounts.json"
            path.write_text(
                json.dumps(
                    {
                        "gateway": {"host": "<host>", "port": 19088},
                        "ordinary": {"account": "ordinary", "password": "one"},
                        "credit": {"account": "credit", "password": "two"},
                        "option": {"account": "option", "password": "three"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(AccountConfigurationError):
                load_simulated_accounts(path)
