from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.acceptance.r1_dual_system_cycle import (
    R1DualSystemCycleError,
    Settings,
    _f2_command,
)


class R1DualSystemCycleTests(unittest.TestCase):
    def _settings(self, root: Path, **overrides: object) -> Settings:
        sdk = root / "sdk"
        sdk.mkdir(exist_ok=True)
        expected, accounts, fixture, a_release, a_trust, b_release, b_trust = (
            root / "expected.json",
            root / "accounts.json",
            root / "fixture.json",
            root / "a-release.json",
            root / "a-trust.json",
            root / "b-release.json",
            root / "b-trust.json",
        )
        for path in (expected, accounts, fixture, a_release, a_trust, b_release, b_trust):
            path.write_text("{}", encoding="utf-8")
        values: dict[str, object] = {
            "sdk_root": sdk,
            "expected_library_hashes": expected,
            "accounts_file": accounts,
            "fixture": fixture,
            "output": root / "report.json",
            "runtime_dir": root / "runtime",
            "trading_day": "2026-09-11",
            "system_a_client_id": 7,
            "system_b_client_id": 8,
            "callback_timeout_seconds": 5.0,
            "system_b_hold_seconds": 90.0,
            "system_a_namespace_release": a_release,
            "system_a_namespace_trust": a_trust,
            "system_a_namespace_state": root / "a-state.json",
            "system_b_namespace_release": b_release,
            "system_b_namespace_trust": b_trust,
            "system_b_namespace_state": root / "b-state.json",
            "runtime_release_hash": "a" * 64,
            "system_a_bootstrap_approval_id": "r1-a-approval",
            "system_b_bootstrap_approval_id": "r1-b-approval",
            "bootstrap_approver_id": "r1-test-operator",
        }
        values.update(overrides)
        return Settings(**values)  # type: ignore[arg-type]

    def test_command_uses_distinct_process_roots_and_only_controlled_flags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = self._settings(Path(directory))
            command = _f2_command(
                settings,
                client_id=8,
                runtime_dir=settings.runtime_dir / "system-b",
                output=settings.runtime_dir / "system-b.json",
                hold_before_cancel_seconds=2.0,
                namespace_release=settings.system_b_namespace_release,
                namespace_trust=settings.system_b_namespace_trust,
                namespace_state=settings.system_b_namespace_state,
                bootstrap_approval_id=settings.system_b_bootstrap_approval_id,
            )
        self.assertIn("--r1-authoritative-recovery-probe", command)
        self.assertIn("--execute-simulated", command)
        self.assertIn("--isolated-native-process", command)
        self.assertIn("--hold-before-cancel-seconds", command)
        self.assertIn("--r1-namespace-release", command)
        self.assertIn("--r1-runtime-release-hash", command)
        self.assertNotIn("--preflight-only", command)

    def test_validation_rejects_shared_scope_and_non_linux(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shared = self._settings(root, system_b_client_id=7)
            with patch(
                "tools.acceptance.r1_dual_system_cycle.platform.system", return_value="Linux"
            ):
                with self.assertRaisesRegex(R1DualSystemCycleError, "distinct EMT scopes"):
                    shared.validate()
            settings = self._settings(root, system_b_client_id=8)
            with patch(
                "tools.acceptance.r1_dual_system_cycle.platform.system", return_value="Windows"
            ):
                with self.assertRaisesRegex(R1DualSystemCycleError, "requires Linux"):
                    settings.validate()


if __name__ == "__main__":
    unittest.main()
