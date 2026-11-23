"""Tests for the payload-free R1 EMT query-completion semantics probe."""

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

from tools.acceptance.r1_emt_query_semantics_probe import (
    S1AProbePlanError,
    _result_passed,
    load_r1_query_semantics_plan,
    run_r1_query_semantics_probe,
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


def _plan(*, approved: bool = True, fixed_matrix: bool = True) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "approval": {
            "approved": approved,
            "approval_reference": "R1-QUERY-SEMANTICS-TEST-1",
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
                "adapter_profile_id": "emt-sdk-query-semantics",
                "adapter_instance_id": f"emt-query-semantics-{profile}",
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
        "query_semantics_probe": {
            "allow_fixed_query_matrix": fixed_matrix,
            "callback_timeout_seconds": 1,
            "page_size": 100,
            "page_inter_request_delay_ms": 0,
            "max_page_requests": 128,
        },
    }


class _FakeApiBase:
    def createTraderApi(self, *args: object) -> None:
        del args

    def setSoftwareVersion(self, value: str) -> None:
        assert value == "tradecore/r1-query-semantics"

    def setHeartBeatInterval(self, value: int) -> None:
        assert value == 10

    def login(self, *args: object) -> int:
        del args
        return 7

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "20260911"

    def queryUnfinishedOrders(self, session: int, request_id: int) -> int:
        self.onQueryOrder({}, {"error_id": 0}, request_id, True, session)
        return 0

    def queryOrders(self, query: object, session: int, request_id: int) -> int:
        del query
        self.onQueryOrder({}, {"error_id": 0}, request_id, True, session)
        return 0

    def queryPosition(self, ticker: str, session: int, request_id: int) -> int:
        assert ticker == ""
        self.onQueryPosition({}, {"error_id": 0}, request_id, True, session)
        return 0

    def queryAsset(self, session: int, request_id: int) -> int:
        self.onQueryAsset({}, {"error_id": 0}, request_id, True, session)
        return 0

    def queryFund(self, session: int, request_id: int) -> int:
        self.onQueryFund({}, {"error_id": 0}, request_id, session)
        return 0

    def queryCreditFundInfo(self, session: int, request_id: int) -> int:
        self.onQueryCreditFundInfo({}, {"error_id": 0}, request_id, session)
        return 0

    def queryCreditAssetDebtInfo(self, session: int, request_id: int) -> int:
        self.onQueryCreditAssetDebtInfo(0.0, {"error_id": 0}, request_id, session)
        return 0

    def queryCreditPositionExtraInfo(
        self, query: dict[str, int | str], session: int, request_id: int
    ) -> int:
        assert query in ({"market": 1, "ticker": "000000"}, {"market": 2, "ticker": "000000"})
        self.onQueryCreditPositionExtraInfo({}, {"error_id": 0}, request_id, True, session)
        return 0

    def queryOrdersByPage(self, query: dict[str, int], session: int, request_id: int) -> int:
        self.onQueryOrderByPage(
            {}, query["req_count"], 0, query["reference"], request_id, True, session
        )
        return 0

    def queryPositionByPage(self, query: dict[str, int], session: int, request_id: int) -> int:
        self.onQueryPositionByPage(
            {}, query["req_count"], 0, query["reference"], request_id, True, session
        )
        return 0

    def queryTradesByPage(self, query: dict[str, int], session: int, request_id: int) -> int:
        self.onQueryTradeByPage(
            {}, query["req_count"], 0, query["reference"], request_id, True, session
        )
        return 0

    def logout(self, session: int) -> None:
        assert session == 7

    def release(self) -> None:
        return None


class _FakeLoader:
    def __init__(self, settings: object) -> None:
        del settings

    def load(self) -> object:
        return type("Loaded", (), {"trader_api_type": _FakeApiBase})()


class R1EmtQuerySemanticsProbeTests(unittest.TestCase):
    def _load(self, payload: dict[str, object]):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "plan.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_r1_query_semantics_plan(path)

    def test_plan_requires_fixed_matrix_approval(self) -> None:
        with self.assertRaisesRegex(S1AProbePlanError, "fixed matrix"):
            self._load(_plan(fixed_matrix=False))

    def test_terminal_and_short_page_are_completion_evidence(self) -> None:
        plan = self._load(_plan())
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
                    "tools.acceptance.r1_emt_query_semantics_probe.EmtPlatformLoader", _FakeLoader
                ),
                patch(
                    "tools.acceptance.r1_emt_query_semantics_probe.suppress_native_console_output",
                    nullcontext,
                ),
                patch(
                    "tools.acceptance.r1_emt_query_semantics_probe._runtime_dir",
                    return_value=Path(temporary),
                ),
            ):
                report = run_r1_query_semantics_probe(plan, accounts=accounts)
        rendered = json.dumps(report)
        self.assertNotIn("ordinary-secret", rendered)
        self.assertNotIn("password-1", rendered)
        self.assertTrue(all(_result_passed(item) for item in report["targets"]))
        self.assertTrue(
            all(
                item["queries"]["orders_by_page"]["completion"] == "page_chain_complete"
                for item in report["targets"]
            )
        )


if __name__ == "__main__":
    unittest.main()
