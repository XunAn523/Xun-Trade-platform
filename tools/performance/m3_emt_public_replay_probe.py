"""Profile-governed M3 EMT public-stream RESTART probe.

The probe calls the documented ``subscribePublicTopic(RESTART)`` before login
and observes only public-stream callback *metadata*.  It cannot query, submit,
cancel, move funds, or promote an account to READY.  A successful login alone
is deliberately not replay evidence: an approved plan may require at least one
order/trade callback, in which case an empty observation window fails closed.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Event
from time import monotonic
from typing import Any, Callable, Mapping

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

_RESTART_RESUME_TYPE = 0
_RESTART_NAME = "RESTART"


@dataclass(frozen=True)
class M3PublicReplayPlan:
    """A plan that authorizes only the documented public-stream restart probe."""

    base: S1AProbePlan
    callback_observation_seconds: float
    require_order_or_trade_callback: bool


def load_m3_public_replay_plan(path: Path, *, now: datetime | None = None) -> M3PublicReplayPlan:
    """Load a fail-closed replay plan with explicit RESTART authorization."""

    base = load_s1a_probe_plan(path, now=now)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:  # Base loader gives stable errors.
        raise S1AProbePlanError("M3 replay plan cannot be reread") from exc
    root = _mapping(raw, "M3 replay plan")
    operations = _mapping(root.get("operations"), "operations")
    if base.allow_public_topic_subscription is not True:
        raise S1AProbePlanError("M3 replay plan does not permit public topic subscription")
    if operations.get("allow_replay") is not True:
        raise S1AProbePlanError("M3 replay plan is not approved for RESTART replay")
    probe = _mapping(root.get("replay_probe"), "replay probe")
    if _text(probe.get("resume_type"), "replay resume type") != _RESTART_NAME:
        raise S1AProbePlanError("M3 replay plan must pin RESTART")
    seconds = probe.get("callback_observation_seconds")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise S1AProbePlanError("replay callback observation must be numeric")
    if not 1 <= float(seconds) <= 15:
        raise S1AProbePlanError("replay callback observation must be in 1..15 seconds")
    required = probe.get("require_order_or_trade_callback")
    if not isinstance(required, bool):
        raise S1AProbePlanError("replay callback requirement must be boolean")
    return M3PublicReplayPlan(
        base=base,
        callback_observation_seconds=float(seconds),
        require_order_or_trade_callback=required,
    )


def run_m3_public_replay_probe(
    plan: M3PublicReplayPlan,
    *,
    accounts: SimulatedAccounts,
    retain_native_reference: Callable[[object], None] | None = None,
) -> dict[str, object]:
    """Run a bounded public-stream RESTART probe without retaining business payloads."""

    base = plan.base
    loaded = EmtPlatformLoader(
        SdkLoadSettings(sdk_root=base.sdk_root, expected_hashes=base.expected_library_hashes)
    ).load()
    results = [
        _probe_target(
            loaded.trader_api_type,
            plan=plan,
            target=target,
            accounts=accounts,
            retain_native_reference=retain_native_reference,
        )
        for target in base.targets
    ]
    return {
        "schema_version": 1,
        "probe": "M3-EMT-public-stream-restart",
        "approval_reference": base.approval_reference,
        "operator_id": base.operator_id,
        "sdk": {
            "expected_api_version": base.expected_api_version,
            "library_files": sorted(base.expected_library_hashes),
        },
        "safety": {
            "public_topic_subscription": True,
            "resume_type": _RESTART_NAME,
            "callback_payload_persisted": False,
            "queries": False,
            "submit": False,
            "cancel": False,
            "fund_or_credit_mutation": False,
            "ready_promotion": False,
            "native_wrapper_retained_until_isolated_exit": retain_native_reference is not None,
        },
        "targets": results,
    }


def _probe_target(
    trader_api_type: type[Any],
    *,
    plan: M3PublicReplayPlan,
    target: S1AProbeTarget,
    accounts: SimulatedAccounts,
    retain_native_reference: Callable[[object], None] | None,
) -> dict[str, object]:
    base = plan.base
    account = getattr(accounts, target.account_config_profile)

    class PublicReplayProbeApi(trader_api_type):
        def __init__(self) -> None:
            super().__init__()
            self._order_callbacks = 0
            self._trade_callbacks = 0
            self._cancel_error_callbacks = 0
            self._last_error_id: int | None = None
            self._public_event_seen = Event()

        def onOrderEvent(self, *args: Any) -> None:
            del args
            self._order_callbacks += 1
            self._public_event_seen.set()

        def onTradeEvent(self, *args: Any) -> None:
            del args
            self._trade_callbacks += 1
            self._public_event_seen.set()

        def onCancelOrderError(self, *args: Any) -> None:
            del args
            self._cancel_error_callbacks += 1
            # A cancel-error callback is public-stream activity, but it is
            # not the order/trade replay evidence this probe is authorized to
            # accept.  Do not let historical cancel errors truncate the full
            # observation window and then report a false negative with a
            # near-zero wait time.

        def onError(self, data: Any) -> None:
            self._last_error_id = _error_id_from_callback(data)

    api: Any | None = None
    session_id = 0
    api_version: str | None = None
    trading_day: str | None = None
    login_error_id: int | None = None
    callback_wait_elapsed_ms = 0
    try:
        api = PublicReplayProbeApi()
        with suppress_native_console_output():
            api.createTraderApi(target.client_id, str(_runtime_dir(target)), 0)
            api.setSoftwareVersion("xuanling-tradecore/m3-public-replay-probe")
            api.setHeartBeatInterval(10)
            api.subscribePublicTopic(_RESTART_RESUME_TYPE)
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
                start = monotonic()
                api._public_event_seen.wait(plan.callback_observation_seconds)
                callback_wait_elapsed_ms = int((monotonic() - start) * 1000)
            else:
                login_error_id = _error_id(api)
        observed = api._order_callbacks + api._trade_callbacks > 0
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
            "resume_type": _RESTART_NAME,
            "callback_observation_elapsed_ms": callback_wait_elapsed_ms,
            "order_callback_count": api._order_callbacks,
            "trade_callback_count": api._trade_callbacks,
            "cancel_error_callback_count": api._cancel_error_callbacks,
            "public_replay_callback_observed": observed,
            "public_stream_error_id": api._last_error_id,
        }
    finally:
        if api is not None:
            if retain_native_reference is not None:
                # This opt-in is only for a standalone, no-mutation probe.
                # Some vendor bindings can block in wrapper finalization after
                # a normal callback window.  The CLI retains the wrapper until
                # it has atomically written its payload-free report, then
                # immediately uses os._exit; no business command is pending.
                retain_native_reference(api)
            else:
                _close(api, session_id)


def _error_id_from_callback(data: object) -> int | None:
    if not isinstance(data, Mapping):
        return None
    value = data.get("error_id")
    return int(value) if isinstance(value, int) else None


def _runtime_dir(target: S1AProbeTarget) -> Path:
    directory = Path("data/performance/probes/emt/m3-public-replay") / target.account_config_profile
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _close(api: Any, session_id: int) -> None:
    with suppress_native_console_output():
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()


def _result_passed(item: Mapping[str, object], *, require_callback: bool) -> bool:
    return (
        item["login_succeeded"] is True
        and item["api_version_matches_expected"] is True
        and item["public_stream_error_id"] in (None, 0)
        and (not require_callback or item["public_replay_callback_observed"] is True)
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the approved M3 EMT RESTART replay probe without business operations"
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--isolate-after-report",
        action="store_true",
        help=(
            "retain native replay wrappers until the payload-free report is written, then "
            "exit the probe process directly; only permitted for this no-mutation probe"
        ),
    )
    args = parser.parse_args()
    plan = load_m3_public_replay_plan(args.plan)
    retained_native_references: list[object] = []
    report = run_m3_public_replay_probe(
        plan,
        accounts=load_simulated_accounts(args.accounts_file),
        retain_native_reference=(
            retained_native_references.append if args.isolate_after_report else None
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    targets = report["targets"]
    if not isinstance(targets, list) or not all(isinstance(item, Mapping) for item in targets):
        raise RuntimeError("M3 replay probe generated an invalid target report")
    passed = all(
        _result_passed(item, require_callback=plan.require_order_or_trade_callback)
        for item in targets
    )
    print(f"M3_PUBLIC_REPLAY_PASS={passed}", flush=True)
    exit_code = 0 if passed else 2
    if args.isolate_after_report:
        os._exit(exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
