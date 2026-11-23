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

from tools.acceptance.m3_emt_correlated_replay_probe import (
    _passed,
    run_m3_correlated_replay_probe,
)
from tools.performance.perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from xuanling_tradecore.adapters.emt.replay_witness_v1 import issue_emt_replay_witness_v1
from xuanling_tradecore.domain import TradingDayEpoch


def _expected_library_hashes() -> dict[str, str]:
    if platform.system() == "Linux":
        return {"vnemttrader.so": "a" * 64, "libemt_api.so": "b" * 64}
    return {
        "vnemttrader.pyd": "a" * 64,
        "vnemttrader.dll": "b" * 64,
        "emt_api.dll": "c" * 64,
    }


def _plan() -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "approval": {
            "approved": True,
            "approval_reference": "M3-CORRELATED-TEST-1",
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
                "adapter_profile_id": "emt-sdk-probe-2.27.0-replay",
                "adapter_instance_id": "emt-m3-ordinary-replay-v1",
                "adapter_client_scope": "111",
                "client_id": 111,
            },
            {
                "account_config_profile": "credit",
                "adapter_profile_id": "emt-sdk-probe-2.27.0-replay",
                "adapter_instance_id": "emt-m3-credit-replay-v1",
                "adapter_client_scope": "112",
                "client_id": 112,
            },
            {
                "account_config_profile": "option",
                "adapter_profile_id": "emt-sdk-probe-2.27.0-replay",
                "adapter_instance_id": "emt-m3-option-replay-v1",
                "adapter_client_scope": "113",
                "client_id": 113,
            },
        ],
        "operations": {
            "allow_login_and_read_only_queries": True,
            "allow_public_topic_subscription": True,
            "allow_replay": True,
            "allow_submit": False,
            "allow_cancel": False,
            "allow_ready_promotion": False,
        },
        "replay_probe": {
            "resume_type": "RESTART",
            "callback_observation_seconds": 1,
            "require_order_or_trade_callback": True,
        },
    }


class _FakeApi:
    def createTraderApi(self, *args: object) -> None:
        del args

    def setSoftwareVersion(self, value: str) -> None:
        assert value == "tradecore/r1-correlated-restart-replay"

    def setHeartBeatInterval(self, value: int) -> None:
        assert value == 10

    def subscribePublicTopic(self, value: int) -> None:
        assert value == 0

    def login(self, *args: object) -> int:
        del args
        self.onOrderEvent({"order_client_id": 1001, "order_emt_id": 5001}, {"error_id": 0}, 7)
        return 7

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "2026-09-11"

    def logout(self, session_id: int) -> None:
        assert session_id == 7

    def release(self) -> None:
        return None


class _FakeLoader:
    def __init__(self, settings: object) -> None:
        del settings

    def load(self) -> object:
        return type("Loaded", (), {"trader_api_type": _FakeApi})()


class CorrelatedReplayProbeTests(unittest.TestCase):
    def test_exact_digest_match_is_required_and_payload_is_not_reported(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=19088,
            ordinary=SimulatedAccount("ordinary-secret", "password-1"),
            credit=SimulatedAccount("credit-secret", "password-2"),
            option=SimulatedAccount("option-secret", "password-3"),
        )
        witness = issue_emt_replay_witness_v1(
            witness_id="witness-1",
            account_config_profile="ordinary",
            account_id="ordinary-secret",
            adapter_instance_id="emt-m3-ordinary-replay-v1",
            adapter_client_scope="111",
            trading_day=TradingDayEpoch("2026-09-11"),
            namespace_release_hash="a" * 64,
            namespace_manifest_hash="b" * 64,
            order_client_id=1001,
            order_emt_id=5001,
            issued_at_wall_ns=0,
            expires_at_wall_ns=10**20,
        )
        with TemporaryDirectory() as temporary:
            plan_path = Path(temporary) / "plan.json"
            plan_path.write_text(json.dumps(_plan()), encoding="utf-8")
            with (
                patch(
                    "tools.acceptance.m3_emt_correlated_replay_probe.EmtPlatformLoader", _FakeLoader
                ),
                patch(
                    "tools.acceptance.m3_emt_correlated_replay_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.acceptance.m3_emt_correlated_replay_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_m3_correlated_replay_probe(
                    plan_path=plan_path, accounts=accounts, witness=witness
                )
        rendered = json.dumps(report)
        self.assertNotIn("ordinary-secret", rendered)
        self.assertNotIn("1001", rendered)
        self.assertNotIn("5001", rendered)
        self.assertTrue(_passed(report))
        self.assertEqual(1, report["exact_witness_callback_count"])


if __name__ == "__main__":
    unittest.main()
