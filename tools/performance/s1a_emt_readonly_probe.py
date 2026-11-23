"""Profile-governed S1-A EMT login probe with no business API calls.

The probe accepts only a local ignored plan that names account *profiles*, not
account values.  It verifies the approved time window, host and SDK hashes
before loading the vendor binding.  Each profile is then logged in and out in
sequence; no query, order, cancel, fund or credit method is reachable here.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)

from .perfbench.simulated_accounts import SimulatedAccounts, load_simulated_accounts

_ACCOUNT_PROFILES = frozenset({"ordinary", "credit", "option"})
_REQUIRED_LIBRARY_FILES = frozenset({"vnemttrader.pyd", "vnemttrader.dll", "emt_api.dll"})
_REQUIRED_LINUX_LIBRARY_FILES = frozenset({"vnemttrader.so", "libemt_api.so"})


class S1AProbePlanError(ValueError):
    """The local S1-A plan is incomplete, unapproved, stale, or unsafe."""


@dataclass(frozen=True)
class S1AProbeTarget:
    """One account-profile selector without an account number or credential."""

    account_config_profile: str
    adapter_profile_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    client_id: int


@dataclass(frozen=True)
class S1AProbePlan:
    """Validated, bounded input for a real EMT SDK login-only probe."""

    approval_reference: str
    operator_id: str
    window_start: datetime
    window_end: datetime
    sdk_root: Path
    expected_api_version: str
    expected_library_hashes: Mapping[str, str]
    host_id: str
    targets: tuple[S1AProbeTarget, ...]
    allow_public_topic_subscription: bool


def load_s1a_probe_plan(path: Path, *, now: datetime | None = None) -> S1AProbePlan:
    """Load only an approved, in-window, no-mutation plan from local JSON."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise S1AProbePlanError("S1-A probe plan is missing") from exc
    except json.JSONDecodeError as exc:
        raise S1AProbePlanError("S1-A probe plan is not valid JSON") from exc
    root = _mapping(raw, "S1-A probe plan")
    if root.get("schema_version") != 2:
        raise S1AProbePlanError("S1-A probe plan schema_version must be 2")
    approval = _mapping(root.get("approval"), "approval")
    if approval.get("approved") is not True:
        raise S1AProbePlanError("S1-A probe plan is not approved")
    start = _datetime(approval.get("window_start_local"), "approval window start")
    end = _datetime(approval.get("window_end_local"), "approval window end")
    current = now or datetime.now(timezone.utc)
    if not start <= current <= end:
        raise S1AProbePlanError("current time is outside the approved S1-A probe window")
    if end <= start:
        raise S1AProbePlanError("S1-A probe window end must follow its start")
    sdk = _mapping(root.get("sdk"), "sdk")
    expected_hashes = _library_hashes(
        sdk.get("expected_library_hashes"), system_name=platform.system()
    )
    targets = _targets(root.get("targets"))
    operations = _mapping(root.get("operations"), "operations")
    if operations.get("allow_login_and_read_only_queries") is not True:
        raise S1AProbePlanError("S1-A plan does not permit login")
    for forbidden in ("allow_submit", "allow_cancel", "allow_ready_promotion"):
        if operations.get(forbidden) is not False:
            raise S1AProbePlanError(f"S1-A plan must keep {forbidden} false")
    host = _mapping(root.get("test_host"), "test_host")
    host_id = _text(host.get("host_id"), "test host id")
    if host_id != socket.gethostname():
        raise S1AProbePlanError("S1-A probe plan belongs to another test host")
    return S1AProbePlan(
        approval_reference=_text(approval.get("approval_reference"), "approval reference"),
        operator_id=_text(approval.get("operator_id"), "operator id"),
        window_start=start,
        window_end=end,
        sdk_root=Path(_text(sdk.get("root"), "SDK root")),
        expected_api_version=_text(sdk.get("expected_api_version"), "expected SDK API version"),
        expected_library_hashes=expected_hashes,
        host_id=host_id,
        targets=targets,
        allow_public_topic_subscription=operations.get("allow_public_topic_subscription") is True,
    )


def run_s1a_login_probe(plan: S1AProbePlan, *, accounts: SimulatedAccounts) -> dict[str, object]:
    """Run one login/version/day/logout cycle per configured account profile."""

    loaded = EmtPlatformLoader(
        SdkLoadSettings(
            sdk_root=plan.sdk_root,
            expected_hashes=plan.expected_library_hashes,
        )
    ).load()
    results = [
        _probe_target(loaded.trader_api_type, plan, target, accounts) for target in plan.targets
    ]
    return {
        "schema_version": 1,
        "probe": "S1-A-EMT-login-only",
        "approval_reference": plan.approval_reference,
        "operator_id": plan.operator_id,
        "sdk": {
            "expected_api_version": plan.expected_api_version,
            "library_files": sorted(plan.expected_library_hashes),
        },
        "safety": {
            "login_only": True,
            "queries": False,
            "public_topic_subscription": plan.allow_public_topic_subscription,
            "submit": False,
            "cancel": False,
            "fund_or_credit_mutation": False,
            "ready_promotion": False,
        },
        "targets": results,
    }


def _probe_target(
    trader_api_type: type[Any],
    plan: S1AProbePlan,
    target: S1AProbeTarget,
    accounts: SimulatedAccounts,
) -> dict[str, object]:
    account = getattr(accounts, target.account_config_profile)

    class LoginOnlyProbeApi(trader_api_type):
        pass

    api: Any | None = None
    session_id = 0
    api_version: str | None = None
    trading_day: str | None = None
    error_id: int | None = None
    try:
        api = LoginOnlyProbeApi()
        with suppress_native_console_output():
            api.createTraderApi(target.client_id, str(_runtime_dir(target)), 0)
            api.setSoftwareVersion("xuanling-tradecore/s1a-login-only-probe")
            api.setHeartBeatInterval(10)
            if plan.allow_public_topic_subscription:
                api.subscribePublicTopic(2)
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
            else:
                error_id = _error_id(api)
        version_matches = _api_version_matches(plan.expected_api_version, api_version)
        return {
            "account_config_profile": target.account_config_profile,
            "adapter_profile_id": target.adapter_profile_id,
            "adapter_instance_id": target.adapter_instance_id,
            "adapter_client_scope": target.adapter_client_scope,
            "login_succeeded": session_id != 0,
            "api_version": api_version,
            "api_version_matches_expected": version_matches,
            "trading_day": trading_day,
            "error_id": error_id,
        }
    finally:
        if api is not None:
            _close(api, session_id)


def _runtime_dir(target: S1AProbeTarget) -> Path:
    root = Path("data/performance/probes/emt/s1a-login-only")
    directory = root / target.account_config_profile
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _api_version_matches(expected: str, actual: str | None) -> bool:
    """Treat a numeric SDK build suffix as compatible with its documented API version."""
    if actual is None:
        return False
    try:
        expected_parts = tuple(int(part) for part in expected.split("."))
        actual_parts = tuple(int(part) for part in actual.split("."))
    except ValueError:
        return False
    return (
        len(expected_parts) >= 1
        and len(actual_parts) >= len(expected_parts)
        and actual_parts[: len(expected_parts)] == expected_parts
    )


def _close(api: Any, session_id: int) -> None:
    with suppress_native_console_output():
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()


def _error_id(api: Any) -> int | None:
    try:
        error = api.getApiLastError()
    except Exception:
        return None
    return error.get("error_id") if isinstance(error, dict) else None


def _targets(value: object) -> tuple[S1AProbeTarget, ...]:
    if not isinstance(value, list) or len(value) != 3:
        raise S1AProbePlanError("S1-A plan requires exactly three account-profile targets")
    targets: list[S1AProbeTarget] = []
    for item in value:
        target = _mapping(item, "S1-A target")
        profile = _text(target.get("account_config_profile"), "account config profile")
        if profile not in _ACCOUNT_PROFILES:
            raise S1AProbePlanError("S1-A target has an unsupported account profile")
        client_id = target.get("client_id")
        if (
            isinstance(client_id, bool)
            or not isinstance(client_id, int)
            or not 1 <= client_id <= 255
        ):
            raise S1AProbePlanError("S1-A target client_id must be in 1..255")
        targets.append(
            S1AProbeTarget(
                account_config_profile=profile,
                adapter_profile_id=_text(target.get("adapter_profile_id"), "adapter profile id"),
                adapter_instance_id=_text(target.get("adapter_instance_id"), "adapter instance id"),
                adapter_client_scope=_text(
                    target.get("adapter_client_scope"), "adapter client scope"
                ),
                client_id=client_id,
            )
        )
    if {target.account_config_profile for target in targets} != _ACCOUNT_PROFILES:
        raise S1AProbePlanError("S1-A targets must cover ordinary, credit, and option once")
    if len({target.client_id for target in targets}) != len(targets):
        raise S1AProbePlanError("S1-A targets must use distinct client IDs")
    return tuple(targets)


def _library_hashes(value: object, *, system_name: str) -> Mapping[str, str]:
    source = _mapping(value, "expected library hashes")
    required = {
        "Windows": _REQUIRED_LIBRARY_FILES,
        "Linux": _REQUIRED_LINUX_LIBRARY_FILES,
    }.get(system_name)
    if required is None:
        raise S1AProbePlanError("S1-A probe host platform is unsupported")
    if set(source) != required:
        raise S1AProbePlanError("S1-A plan must pin exactly the host SDK files")
    hashes: dict[str, str] = {}
    for name, digest in source.items():
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise S1AProbePlanError("S1-A SDK file hash is not lowercase SHA-256")
        hashes[name] = digest
    return hashes


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise S1AProbePlanError(f"{field_name} must be an object")
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "<" in value or ">" in value:
        raise S1AProbePlanError(f"{field_name} must be a completed non-empty text value")
    return value.strip()


def _datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise S1AProbePlanError(f"{field_name} must be ISO-8601 text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise S1AProbePlanError(f"{field_name} is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise S1AProbePlanError(f"{field_name} must include a timezone offset")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the approved S1-A EMT login-only probe")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load_s1a_probe_plan(args.plan)
    report = run_s1a_login_probe(plan, accounts=load_simulated_accounts(args.accounts_file))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    targets = report["targets"]
    if not isinstance(targets, list) or not all(isinstance(item, Mapping) for item in targets):
        raise RuntimeError("S1-A login probe generated an invalid target report")
    succeeded = all(item.get("login_succeeded") is True for item in targets)
    versions_match = all(item.get("api_version_matches_expected") is True for item in targets)
    print(f"S1A_LOGIN_ONLY_PASS={succeeded and versions_match}")
    return 0 if succeeded and versions_match else 2


if __name__ == "__main__":
    raise SystemExit(main())
