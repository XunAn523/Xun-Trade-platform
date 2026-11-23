from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.acceptance.r1_r2_crash_boundary import (
    _BOUNDARY,
    R1R2CrashBoundaryError,
    Settings,
    _child_command,
    _load_marker,
)


class R1R2CrashBoundaryTests(unittest.TestCase):
    def _settings(self, root: Path) -> Settings:
        sdk = root / "sdk"
        sdk.mkdir(exist_ok=True)
        paths = {
            name: root / name
            for name in (
                "hashes.json",
                "accounts.json",
                "fixture.json",
                "release.json",
                "trust.json",
            )
        }
        for path in paths.values():
            path.write_text("{}", encoding="utf-8")
        return Settings(
            sdk_root=sdk,
            expected_library_hashes=paths["hashes.json"],
            accounts_file=paths["accounts.json"],
            fixture=paths["fixture.json"],
            output=root / "evidence.json",
            runtime_dir=root / "runtime",
            trading_day="2026-09-11",
            session_client_id=17,
            callback_timeout_seconds=5.0,
            namespace_release=paths["release.json"],
            namespace_trust=paths["trust.json"],
            namespace_state=root / "state.json",
            runtime_release_hash="a" * 64,
            bootstrap_approval_id="r1-test-approval",
            bootstrap_approver_id="r1-test-operator",
        )

    def test_child_uses_isolated_f2_r2_fault_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = _child_command(
                self._settings(root),
                marker=root / "marker.json",
                worker_output=root / "worker.json",
            )
        self.assertIn("--r1-crash-after-native-return", command)
        self.assertIn("--r1-authoritative-recovery-probe", command)
        self.assertIn("--isolated-native-process", command)
        self.assertIn("--execute-simulated", command)

    def test_marker_schema_rejects_payload_or_missing_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "marker.json"
            marker.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "boundary": _BOUNDARY,
                        "observed_monotonic_ns": 1,
                        "payloads_retained": False,
                        "credentials_retained": False,
                    }
                ),
                encoding="utf-8",
            )
            self.assertTrue(_load_marker(marker))
            raw = json.loads(marker.read_text(encoding="utf-8"))
            raw["payload"] = "forbidden"
            marker.write_text(json.dumps(raw), encoding="utf-8")
            self.assertFalse(_load_marker(marker))

    def test_validation_rejects_non_linux(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with patch(
                "tools.acceptance.r1_r2_crash_boundary.platform.system", return_value="Windows"
            ):
                with self.assertRaisesRegex(R1R2CrashBoundaryError, "requires Linux"):
                    self._settings(Path(temporary)).validate()


if __name__ == "__main__":
    unittest.main()
