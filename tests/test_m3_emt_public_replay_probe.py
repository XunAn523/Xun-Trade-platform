"""Tests for the fail-closed, payload-free M3 EMT RESTART replay probe."""

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

from tools.performance.m3_emt_public_replay_probe import (
    S1AProbePlanError,
    _result_passed,
    load_m3_public_replay_plan,
    run_m3_public_replay_probe,
)
from tools.performance.perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def _expected_library_hashes() -> dict[str, str]:
    if platform.system() == "Linux":
        return {"vnemttrader.so": "a" * 64, "libemt_api.so": "b" * 64}
    return {
        "vnemttrader.pyd": "a" * 64,
        "vnemttrader.dll": "b" * 64,
        "emt_api.dll": "c" * 64,
    }


def _plan(*, allow_replay: bool = True, require_callback: bool = True) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "approval": {
            "approved": True,
            "approval_reference": "M3-REPLAY-TEST-1",
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
                "account_config_profile": profile,
                "adapter_profile_id": "emt-sdk-probe-2.27.0-replay",
                "adapter_instance_id": f"emt-m3-{profile}-replay-v1",
                "adapter_client_scope": str(client_id),
                "client_id": client_id,
            }
            for profile, client_id in (("ordinary", 111), ("credit", 112), ("option", 113))
        ],
        "operations": {
            "allow_login_and_read_only_queries": True,
            "allow_public_topic_subscription": True,
            "allow_replay": allow_replay,
            "allow_submit": False,
            "allow_cancel": False,
            "allow_ready_promotion": False,
        },
        "replay_probe": {
            "resume_type": "RESTART",
            "callback_observation_seconds": 1,
            "require_order_or_trade_callback": require_callback,
        },
    }


class _FakeApiBase:
    subscribed: list[int] = []

    def createTraderApi(self, *args: object) -> None:
        del args

    def setSoftwareVersion(self, value: str) -> None:
        assert value == "xuanling-tradecore/m3-public-replay-probe"

    def setHeartBeatInterval(self, value: int) -> None:
        assert value == 10

    def subscribePublicTopic(self, resume_type: int) -> None:
        self.subscribed.append(resume_type)

    def login(self, *args: object) -> int:
        del args
        self.onOrderEvent({"must-not-persist": "payload"}, {"error_id": 0}, 7)
        return 7

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "20260904"

    def logout(self, session_id: int) -> None:
        assert session_id == 7

    def release(self) -> None:
        return None


class _FakeLoader:
    def __init__(self, settings: object) -> None:
        del settings

    def load(self) -> object:
        return type("Loaded", (), {"trader_api_type": _FakeApiBase})()


class _CancelErrorOnlyFakeApiBase(_FakeApiBase):
    def login(self, *args: object) -> int:
        del args
        self.onCancelOrderError({"must-not-persist": "payload"}, {"error_id": 0}, 7)
        return 7


class _CancelErrorOnlyFakeLoader:
    def __init__(self, settings: object) -> None:
        del settings

    def load(self) -> object:
        return type("Loaded", (), {"trader_api_type": _CancelErrorOnlyFakeApiBase})()


class M3EmtPublicReplayProbeTests(unittest.TestCase):
    def _load(self, payload: dict[str, object]):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_m3_public_replay_plan(path)

    def test_plan_requires_explicit_replay_authorization(self) -> None:
        with self.assertRaisesRegex(S1AProbePlanError, "not approved for RESTART"):
            self._load(_plan(allow_replay=False))

    def test_probe_persists_only_metadata_and_requires_replay_callback(self) -> None:
        plan = self._load(_plan())
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        _FakeApiBase.subscribed = []
        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "tools.performance.m3_emt_public_replay_probe.EmtPlatformLoader", _FakeLoader
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_m3_public_replay_probe(plan, accounts=accounts)
        rendered = json.dumps(report)
        for forbidden in ("ordinary-secret", "password-1", "test-host", "must-not-persist"):
            self.assertNotIn(forbidden, rendered)
        self.assertEqual(_FakeApiBase.subscribed, [0, 0, 0])
        self.assertTrue(
            all(_result_passed(item, require_callback=True) for item in report["targets"])
        )
        self.assertFalse(report["safety"]["submit"])
        self.assertFalse(report["safety"]["cancel"])

    def test_no_callback_is_not_a_pass_when_required(self) -> None:
        item: dict[str, object] = {
            "login_succeeded": True,
            "api_version_matches_expected": True,
            "public_stream_error_id": None,
            "public_replay_callback_observed": False,
        }
        self.assertFalse(_result_passed(item, require_callback=True))

    def test_cancel_error_does_not_shorten_required_order_trade_observation(self) -> None:
        plan = self._load(_plan(require_callback=True))
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "tools.performance.m3_emt_public_replay_probe.EmtPlatformLoader",
                    _CancelErrorOnlyFakeLoader,
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_m3_public_replay_probe(plan, accounts=accounts)
        self.assertTrue(all(item["cancel_error_callback_count"] == 1 for item in report["targets"]))
        self.assertTrue(
            all(item["public_replay_callback_observed"] is False for item in report["targets"])
        )
        self.assertTrue(
            all(item["callback_observation_elapsed_ms"] >= 900 for item in report["targets"])
        )

    def test_isolated_mode_retains_native_wrappers_until_report_is_written(self) -> None:
        plan = self._load(_plan(require_callback=False))
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        retained: list[object] = []
        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "tools.performance.m3_emt_public_replay_probe.EmtPlatformLoader", _FakeLoader
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.performance.m3_emt_public_replay_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_m3_public_replay_probe(
                    plan,
                    accounts=accounts,
                    retain_native_reference=retained.append,
                )
        self.assertEqual(len(retained), 3)
        self.assertTrue(report["safety"]["native_wrapper_retained_until_isolated_exit"])


if __name__ == "__main__":
    unittest.main()
