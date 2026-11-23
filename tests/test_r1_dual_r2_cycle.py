from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.acceptance.r1_dual_r2_cycle import (
    R1DualR2CycleError,
    Settings,
    _b_command,
    _snapshot_crashed_r2_ledger,
)


class R1DualR2CycleTests(unittest.TestCase):
    def _settings(self, root: Path, **overrides: object) -> Settings:
        sdk = root / "sdk"
        sdk.mkdir(exist_ok=True)
        files = [
            root / "hashes.json",
            root / "accounts.json",
            root / "fixture.json",
            root / "a-release.json",
            root / "a-trust.json",
            root / "b-release.json",
            root / "b-trust.json",
        ]
        for path in files:
            path.write_text("{}", encoding="utf-8")
        values: dict[str, object] = {
            "sdk_root": sdk,
            "expected_library_hashes": files[0],
            "accounts_file": files[1],
            "fixture": files[2],
            "output": root / "evidence.json",
            "runtime_dir": root / "runtime",
            "trading_day": "2026-09-11",
            "system_a_client_id": 101,
            "system_b_client_id": 105,
            "callback_timeout_seconds": 20.0,
            "system_b_hold_seconds": 90.0,
            "system_a_namespace_release": files[3],
            "system_a_namespace_trust": files[4],
            "system_a_namespace_state": root / "a-state.json",
            "system_b_namespace_release": files[5],
            "system_b_namespace_trust": files[6],
            "system_b_namespace_state": root / "b-state.json",
            "runtime_release_hash": "a" * 64,
            "system_a_bootstrap_approval_id": "a-bootstrap",
            "system_b_bootstrap_approval_id": "b-bootstrap",
            "bootstrap_approver_id": "operator",
            "cleanup_approval_id": "a-cleanup",
            "cleanup_approver_id": "operator",
        }
        values.update(overrides)
        return Settings(**values)  # type: ignore[arg-type]

    def test_b_command_keeps_b_in_an_isolated_signed_scope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = self._settings(Path(directory))
            command = _b_command(
                settings,
                runtime_dir=settings.runtime_dir / "system-b",
                output=settings.runtime_dir / "system-b-report.json",
            )
        self.assertIn("--isolated-native-process", command)
        self.assertIn("--r1-authoritative-recovery-probe", command)
        self.assertIn("--hold-before-cancel-seconds", command)
        self.assertIn("90.0", command)
        self.assertNotIn("--r1-recovery-route-ledger", command)

    def test_validation_rejects_short_hold_and_shared_scope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("tools.acceptance.r1_dual_r2_cycle.platform.system", return_value="Linux"):
                with self.assertRaisesRegex(R1DualR2CycleError, "90..180"):
                    self._settings(root, system_b_hold_seconds=30.0).validate()
                with self.assertRaisesRegex(R1DualR2CycleError, "distinct EMT scopes"):
                    self._settings(root, system_b_client_id=101).validate()

    def test_crash_ledger_snapshot_preserves_pre_rehydration_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crashed" / "route-ledger.mmap"
            source.parent.mkdir()
            source.write_bytes(b"pre-native-r2-boundary")
            snapshot = _snapshot_crashed_r2_ledger(
                source, root / "cleanup-source" / "route-ledger.mmap"
            )
            source.write_bytes(b"rehydrated-venue-identity")
            self.assertEqual(b"pre-native-r2-boundary", snapshot.read_bytes())


if __name__ == "__main__":
    unittest.main()
