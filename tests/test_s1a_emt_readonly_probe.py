"""Tests for the profile-governed, login-only EMT S1-A probe."""

from __future__ import annotations

import json
import platform
import socket
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tools.performance.perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from tools.performance.s1a_emt_readonly_probe import (
    S1AProbePlanError,
    load_s1a_probe_plan,
    run_s1a_login_probe,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _expected_library_hashes() -> dict[str, str]:
    if platform.system() == "Linux":
        return {"vnemttrader.so": SHA_A, "libemt_api.so": SHA_B}
    return {
        "vnemttrader.pyd": SHA_A,
        "vnemttrader.dll": SHA_B,
        "emt_api.dll": SHA_C,
    }


def _plan(*, approved: bool = True, allow_submit: bool = False) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "approval": {
            "approved": approved,
            "approval_reference": "S1A-TEST-1",
            "operator_id": "fengyu",
            "window_start_local": (now - timedelta(minutes=1)).isoformat(),
            "window_end_local": (now + timedelta(minutes=1)).isoformat(),
        },
        "sdk": {
            "root": "C:/sdk",
            "expected_api_version": "2.27.0",
            "expected_library_hashes": _expected_library_hashes(),
        },
        "test_host": {"host_id": socket.gethostname()},
        "targets": [
            {
                "account_config_profile": "ordinary",
                "adapter_profile_id": "emt-sdk-probe-2.27.0",
                "adapter_instance_id": "emt-s1a-ordinary-v1",
                "adapter_client_scope": "91",
                "client_id": 91,
            },
            {
                "account_config_profile": "credit",
                "adapter_profile_id": "emt-sdk-probe-2.27.0",
                "adapter_instance_id": "emt-s1a-credit-v1",
                "adapter_client_scope": "92",
                "client_id": 92,
            },
            {
                "account_config_profile": "option",
                "adapter_profile_id": "emt-sdk-probe-2.27.0",
                "adapter_instance_id": "emt-s1a-option-v1",
                "adapter_client_scope": "93",
                "client_id": 93,
            },
        ],
        "operations": {
            "allow_login_and_read_only_queries": True,
            "allow_public_topic_subscription": True,
            "allow_submit": allow_submit,
            "allow_cancel": False,
            "allow_ready_promotion": False,
        },
    }


class _FakeApiBase:
    created_client_ids: list[int] = []
    subscribed = False

    def createTraderApi(self, client_id: int, runtime_dir: str, log_level: int) -> None:
        del runtime_dir, log_level
        self.created_client_ids.append(client_id)

    def setSoftwareVersion(self, version: str) -> None:
        assert version == "xuanling-tradecore/s1a-login-only-probe"

    def setHeartBeatInterval(self, seconds: int) -> None:
        assert seconds == 10

    def subscribePublicTopic(self, resume_type: int) -> None:
        assert resume_type == 2
        self.subscribed = True

    def login(self, *args: object) -> int:
        del args
        return 7

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "2026-09-02"

    def logout(self, session_id: int) -> None:
        assert session_id == 7

    def release(self) -> None:
        return None


class _FakeLoadedSdk:
    trader_api_type = _FakeApiBase


class _FakeLoader:
    def __init__(self, settings) -> None:
        self.settings = settings

    def load(self) -> _FakeLoadedSdk:
        return _FakeLoadedSdk()


class S1AEmtReadOnlyProbeTests(unittest.TestCase):
    def _load(self, payload: dict[str, object]):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_s1a_probe_plan(path)

    def test_plan_requires_approval_and_forbids_mutations(self) -> None:
        with self.assertRaisesRegex(S1AProbePlanError, "not approved"):
            self._load(_plan(approved=False))
        with self.assertRaisesRegex(S1AProbePlanError, "allow_submit false"):
            self._load(_plan(allow_submit=True))

    def test_plan_selects_each_private_account_profile_once(self) -> None:
        plan = self._load(_plan())
        self.assertEqual(
            {target.account_config_profile for target in plan.targets},
            {"ordinary", "credit", "option"},
        )
        self.assertEqual({target.client_id for target in plan.targets}, {91, 92, 93})

    def test_linux_plan_requires_linux_sdk_pair(self) -> None:
        payload = _plan()
        payload["sdk"] = {
            "root": "/opt/emt-sdk",
            "expected_api_version": "2.27.0",
            "expected_library_hashes": {
                "vnemttrader.so": SHA_A,
                "libemt_api.so": SHA_B,
            },
        }
        with patch(
            "tools.performance.s1a_emt_readonly_probe.platform.system", return_value="Linux"
        ):
            plan = self._load(payload)
        self.assertEqual(set(plan.expected_library_hashes), {"vnemttrader.so", "libemt_api.so"})

    def test_login_probe_reports_no_account_or_secret_and_no_business_operation(self) -> None:
        plan = self._load(_plan())
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        _FakeApiBase.created_client_ids = []
        with TemporaryDirectory() as temporary:
            with (
                patch("tools.performance.s1a_emt_readonly_probe.EmtPlatformLoader", _FakeLoader),
                patch(
                    "tools.performance.s1a_emt_readonly_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.performance.s1a_emt_readonly_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_s1a_login_probe(plan, accounts=accounts)
        rendered = json.dumps(report)
        self.assertNotIn("ordinary-secret", rendered)
        self.assertNotIn("credit-secret", rendered)
        self.assertNotIn("password-1", rendered)
        self.assertNotIn('"host_id"', rendered)
        self.assertNotIn("test-host", rendered)
        self.assertEqual(_FakeApiBase.created_client_ids, [91, 92, 93])
        self.assertTrue(all(item["login_succeeded"] for item in report["targets"]))
        self.assertTrue(all(item["api_version_matches_expected"] for item in report["targets"]))
        self.assertTrue(report["safety"]["login_only"])
        self.assertFalse(report["safety"]["queries"])
        self.assertFalse(report["safety"]["submit"])
        self.assertFalse(report["safety"]["cancel"])


if __name__ == "__main__":
    unittest.main()
