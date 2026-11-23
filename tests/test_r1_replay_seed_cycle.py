from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tools.acceptance.r1_replay_seed_cycle import R1ReplaySeedCycleError, Settings
from tools.performance.perfbench.gateway_mutation import (
    GatewayMutationError,
    GatewayMutationSettings,
)
from tools.performance.perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


class R1ReplaySeedCycleTests(unittest.TestCase):
    def test_seed_requires_bootstrap_for_new_namespace_state(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("sdk", "accounts.json", "fixture.json", "release.json", "trust.json"):
                path = root / name
                if "." in name:
                    path.write_text("{}", encoding="utf-8")
                else:
                    path.mkdir()
            settings = Settings(
                sdk_root=root / "sdk",
                accounts_file=root / "accounts.json",
                fixture=root / "fixture.json",
                tight_burst_order_plan=None,
                profile="credit",
                session_client_id=106,
                trading_day="20260911",
                namespace_release=root / "release.json",
                namespace_trust=root / "trust.json",
                namespace_state=root / "state.json",
                runtime_release_hash="a" * 64,
                output=root / "report.json",
                proposal_output=root / "proposal.json",
                runtime_dir=root / "runtime",
                callback_timeout_seconds=20.0,
                execute_simulated=True,
            )
            with patch(
                "tools.acceptance.r1_replay_seed_cycle.platform.system", return_value="Linux"
            ):
                with self.assertRaisesRegex(R1ReplaySeedCycleError, "bootstrap approval"):
                    settings.validate()

    def test_fixed_replay_identity_requires_one_cycle_and_a_valid_session_scope(self) -> None:
        accounts = SimulatedAccounts(
            host="test",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )
        with self.assertRaisesRegex(GatewayMutationError, "exactly one controlled cycle"):
            GatewayMutationSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                fixture=Path("fixture"),
                cycles=2,
                fixed_native_order_client_id=1,
            ).validate()
        with self.assertRaisesRegex(GatewayMutationError, "session client id"):
            GatewayMutationSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                fixture=Path("fixture"),
                session_client_id=256,
            ).validate()


if __name__ == "__main__":
    unittest.main()
