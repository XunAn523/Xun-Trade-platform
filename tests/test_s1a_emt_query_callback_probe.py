"""Tests for the payload-free EMT S1-A query/callback boundary."""

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
from tools.performance.s1a_emt_query_callback_probe import (
    S1AProbePlanError,
    _result_passed,
    load_s1a_query_callback_plan,
    run_s1a_query_callback_probe,
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


def _plan(*, allow_query: bool = True) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "approval": {
            "approved": True,
            "approval_reference": "S1A-CALLBACK-TEST-1",
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
                "adapter_profile_id": "emt-sdk-probe-2.27.0-readonly-query",
                "adapter_instance_id": f"emt-s1a-{profile}-query-v1",
                "adapter_client_scope": str(client_id),
                "client_id": client_id,
            }
            for profile, client_id in (("ordinary", 101), ("credit", 102), ("option", 103))
        ],
        "operations": {
            "allow_login_and_read_only_queries": True,
            "allow_public_topic_subscription": False,
            "allow_submit": False,
            "allow_cancel": False,
            "allow_ready_promotion": False,
        },
        "callback_probe": {
            "allow_unfinished_orders_query": allow_query,
            "query_api": "queryUnfinishedOrders",
            "terminal_callback": "onQueryOrder",
            "callback_timeout_seconds": 2,
        },
    }


class _FakeApiBase:
    client_ids: list[int] = []
    query_request_ids: list[int] = []

    def createTraderApi(self, client_id: int, runtime_dir: str, log_level: int) -> None:
        del runtime_dir, log_level
        self.client_ids.append(client_id)

    def setSoftwareVersion(self, version: str) -> None:
        assert version == "xuanling-tradecore/s1a-query-callback-probe"

    def setHeartBeatInterval(self, seconds: int) -> None:
        assert seconds == 10

    def login(self, *args: object) -> int:
        del args
        return 7

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "20260902"

    def queryUnfinishedOrders(self, session_id: int, request_id: int) -> int:
        assert session_id == 7
        self.query_request_ids.append(request_id)
        self.onQueryOrder(
            {"order_client_id": "must-not-persist"}, {"error_id": 0}, request_id, True, 7
        )
        return 0

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


class S1AEmtQueryCallbackProbeTests(unittest.TestCase):
    def _load(self, payload: dict[str, object]):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_s1a_query_callback_plan(path)

    def test_callback_plan_rejects_any_query_other_than_the_pinned_one(self) -> None:
        with self.assertRaisesRegex(S1AProbePlanError, "does not permit"):
            self._load(_plan(allow_query=False))
        payload = _plan()
        callback_probe = payload["callback_probe"]
        assert isinstance(callback_probe, dict)
        callback_probe["query_api"] = "queryOrders"
        with self.assertRaisesRegex(S1AProbePlanError, "must pin"):
            self._load(payload)

    def test_query_probe_never_persists_callback_payload_or_account_secrets(self) -> None:
        plan = self._load(_plan())
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        _FakeApiBase.client_ids = []
        _FakeApiBase.query_request_ids = []
        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "tools.performance.s1a_emt_query_callback_probe.EmtPlatformLoader", _FakeLoader
                ),
                patch(
                    "tools.performance.s1a_emt_query_callback_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.performance.s1a_emt_query_callback_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_s1a_query_callback_probe(plan, accounts=accounts)
        rendered = json.dumps(report)
        for forbidden in (
            "ordinary-secret",
            "credit-secret",
            "password-1",
            "test-host",
            "must-not-persist",
            '"host_id"',
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertEqual(_FakeApiBase.client_ids, [101, 102, 103])
        self.assertEqual(_FakeApiBase.query_request_ids, [100101, 100102, 100103])
        self.assertTrue(all(_result_passed(item) for item in report["targets"]))
        self.assertFalse(report["safety"]["callback_payload_persisted"])
        self.assertFalse(report["safety"]["submit"])
        self.assertFalse(report["safety"]["cancel"])


if __name__ == "__main__":
    unittest.main()
