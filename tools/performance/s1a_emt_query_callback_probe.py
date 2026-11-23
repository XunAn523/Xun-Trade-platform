"""Approved S1-A EMT query/callback probe with payload-free evidence.

The only business call reachable from this module is
``queryUnfinishedOrders``.  Its callback is used solely to prove a terminal
``onQueryOrder`` delivery.  Neither the returned rows nor their scalar fields
are retained, emitted, or passed outside the native callback boundary.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any, Mapping

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)

from .perfbench.simulated_accounts import SimulatedAccounts, load_simulated_accounts
from .s1a_emt_readonly_probe import (
    S1AProbePlan,
    S1AProbePlanError,
    S1AProbeTarget,
    _api_version_matches,
    _error_id,
    _mapping,
    _text,
    load_s1a_probe_plan,
)

_QUERY_API = "queryUnfinishedOrders"
_CALLBACK_NAME = "onQueryOrder"


@dataclass(frozen=True)
class S1AQueryCallbackPlan:
    """A validated S1-A plan narrowed to one read-only API/callback pair."""

    base: S1AProbePlan
    callback_timeout_seconds: float


def load_s1a_query_callback_plan(
    path: Path, *, now: datetime | None = None
) -> S1AQueryCallbackPlan:
    """Require an approved base plan plus an exact bounded callback operation."""
    base = load_s1a_probe_plan(path, now=now)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:  # Base loader gives the stable error.
        raise S1AProbePlanError("S1-A query callback plan cannot be reread") from exc
    probe = _mapping(
        _mapping(raw, "S1-A query callback plan").get("callback_probe"), "callback probe"
    )
    if probe.get("allow_unfinished_orders_query") is not True:
        raise S1AProbePlanError("S1-A callback plan does not permit queryUnfinishedOrders")
    if _text(probe.get("query_api"), "callback query API") != _QUERY_API:
        raise S1AProbePlanError("S1-A callback plan must pin queryUnfinishedOrders")
    if _text(probe.get("terminal_callback"), "callback terminal method") != _CALLBACK_NAME:
        raise S1AProbePlanError("S1-A callback plan must pin onQueryOrder")
    timeout = probe.get("callback_timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise S1AProbePlanError("callback timeout must be numeric")
    if not 1 <= float(timeout) <= 10:
        raise S1AProbePlanError("callback timeout must be in 1..10 seconds")
    return S1AQueryCallbackPlan(base=base, callback_timeout_seconds=float(timeout))


def run_s1a_query_callback_probe(
    plan: S1AQueryCallbackPlan, *, accounts: SimulatedAccounts
) -> dict[str, object]:
    """Login once per target and prove one terminal query callback without retaining rows."""
    base = plan.base
    loaded = EmtPlatformLoader(
        SdkLoadSettings(
            sdk_root=base.sdk_root,
            expected_hashes=base.expected_library_hashes,
        )
    ).load()
    results = [
        _probe_target(
            loaded.trader_api_type,
            plan=plan,
            target=target,
            accounts=accounts,
        )
        for target in base.targets
    ]
    return {
        "schema_version": 1,
        "probe": "S1-A-EMT-query-callback",
        "approval_reference": base.approval_reference,
        "operator_id": base.operator_id,
        "sdk": {
            "expected_api_version": base.expected_api_version,
            "library_files": sorted(base.expected_library_hashes),
        },
        "safety": {
            "login_only": False,
            "query_api": _QUERY_API,
            "callback_payload_persisted": False,
            "public_topic_subscription": False,
            "replay": False,
            "submit": False,
            "cancel": False,
            "fund_or_credit_mutation": False,
            "ready_promotion": False,
        },
        "targets": results,
    }


def _probe_target(
    trader_api_type: type[Any],
    *,
    plan: S1AQueryCallbackPlan,
    target: S1AProbeTarget,
    accounts: SimulatedAccounts,
) -> dict[str, object]:
    base = plan.base
    account = getattr(accounts, target.account_config_profile)

    class QueryCallbackProbeApi(trader_api_type):
        def __init__(self) -> None:
            super().__init__()
            self._query_request_id: int | None = None
            self._terminal_error_id: int | None = None
            self._terminal_seen = Event()

        def begin_query(self, request_id: int) -> None:
            self._query_request_id = request_id
            self._terminal_error_id = None
            self._terminal_seen.clear()

        def onQueryOrder(self, *args: Any) -> None:
            """Record terminal state only; row payload remains unreachable."""
            error_id, request_id, is_last = _query_terminal_metadata(args)
            if request_id != self._query_request_id or not is_last:
                return
            self._terminal_error_id = error_id
            self._terminal_seen.set()

    api: Any | None = None
    session_id = 0
    api_version: str | None = None
    trading_day: str | None = None
    login_error_id: int | None = None
    query_return_code: int | None = None
    callback_seen = False
    callback_error_id: int | None = None
    try:
        api = QueryCallbackProbeApi()
        request_id = 100_000 + target.client_id
        with suppress_native_console_output():
            api.createTraderApi(target.client_id, str(_runtime_dir(target)), 0)
            api.setSoftwareVersion("xuanling-tradecore/s1a-query-callback-probe")
            api.setHeartBeatInterval(10)
            session_id = int(
                api.login(
                    accounts.host,
                    accounts.port,
                    account.account,
                    account.password,
                    1,
                    "",
                )
            )
            if session_id:
                api_version = str(api.getApiVersion())
                trading_day = str(api.getTradingDay())
                api.begin_query(request_id)
                query_return_code = int(api.queryUnfinishedOrders(session_id, request_id))
                if query_return_code == 0:
                    callback_seen = bool(api._terminal_seen.wait(plan.callback_timeout_seconds))
                    callback_error_id = api._terminal_error_id
            else:
                login_error_id = _error_id(api)
        return {
            "account_config_profile": target.account_config_profile,
            "adapter_profile_id": target.adapter_profile_id,
            "adapter_instance_id": target.adapter_instance_id,
            "adapter_client_scope": target.adapter_client_scope,
            "login_succeeded": session_id != 0,
            "api_version": api_version,
            "api_version_matches_expected": _api_version_matches(
                base.expected_api_version, api_version
            ),
            "trading_day": trading_day,
            "login_error_id": login_error_id,
            "query_api": _QUERY_API,
            "query_return_code": query_return_code,
            "terminal_callback": _CALLBACK_NAME,
            "terminal_callback_observed": callback_seen,
            "terminal_callback_error_id": callback_error_id,
        }
    finally:
        if api is not None:
            _close(api, session_id)


def _query_terminal_metadata(args: tuple[Any, ...]) -> tuple[int | None, int | None, bool]:
    """Extract only error/request/finality from the documented query callback tail."""
    if len(args) < 5:
        return (None, None, False)
    error = args[-4]
    request_id = args[-3]
    is_last = args[-2]
    error_id = error.get("error_id") if isinstance(error, Mapping) else None
    return (
        int(error_id) if isinstance(error_id, int) else None,
        int(request_id) if isinstance(request_id, int) else None,
        bool(is_last),
    )


def _runtime_dir(target: S1AProbeTarget) -> Path:
    directory = (
        Path("data/performance/probes/emt/s1a-query-callback") / target.account_config_profile
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _close(api: Any, session_id: int) -> None:
    with suppress_native_console_output():
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()


def _result_passed(item: Mapping[str, object]) -> bool:
    return (
        item["login_succeeded"] is True
        and item["api_version_matches_expected"] is True
        and item["query_return_code"] == 0
        and item["terminal_callback_observed"] is True
        and item["terminal_callback_error_id"] in (None, 0)
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the approved S1-A EMT query/callback probe without saving query rows"
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load_s1a_query_callback_plan(args.plan)
    report = run_s1a_query_callback_probe(
        plan, accounts=load_simulated_accounts(args.accounts_file)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    passed = all(_result_passed(item) for item in report["targets"])
    print(f"S1A_QUERY_CALLBACK_PASS={passed}")
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
