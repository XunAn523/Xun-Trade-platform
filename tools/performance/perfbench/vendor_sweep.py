"""Controlled execution of the vendor's simulated-trading coverage sample.

This is a compatibility sweep, not an acceptance latency benchmark.  It calls
the vendor-provided sample's supported API paths against one simulated account
at a time, discards all SDK output, and records only method timing and callback
counts.  Exact C0→C5 attribution remains the responsibility of the dedicated
native drivers because several callbacks omit request ids.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from .native_readonly import _load_trader_api, _suppress_native_console_output
from .simulated_accounts import SimulatedAccount, SimulatedAccounts


class VendorSweepError(RuntimeError):
    pass


@dataclass(frozen=True)
class VendorSweepSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    profile: str


def _profile_account(accounts: SimulatedAccounts, profile: str) -> SimulatedAccount:
    profiles = {"ordinary": accounts.ordinary, "credit": accounts.credit, "option": accounts.option}
    try:
        return profiles[profile]
    except KeyError as exc:
        raise VendorSweepError(f"unknown simulated profile: {profile}") from exc


def _sample_path(sdk_root: Path) -> Path:
    path = sdk_root / "test" / "tradertest.py"
    if not path.is_file():
        raise VendorSweepError(f"vendor simulated sample is missing: {path}")
    return path


def _replace_assignment(source: str, name: str, expression: str) -> str:
    updated, replacements = re.subn(
        rf"(?m)^(?P<indent>\s*){re.escape(name)}\s*=\s*[^\r\n]+$",
        rf"\g<indent>{name} = {expression}",
        source,
        count=1,
    )
    if replacements != 1:
        raise VendorSweepError(f"could not locate vendor sample assignment: {name}")
    return updated


def _prepare_source(source: str) -> str:
    """Replace only configuration, delays, and observability hooks in memory."""

    source = source.replace(
        "from time import sleep",
        "from time import sleep as _vendor_sleep\n\ndef sleep(seconds):\n    _vendor_sleep(min(float(seconds), 0.1))",
        1,
    )
    source = _replace_assignment(source, "ip", "_profile['host']")
    source = _replace_assignment(source, "port", "_profile['port']")
    source = _replace_assignment(source, "user", "_profile['account']")
    source = _replace_assignment(source, "password", "_profile['password']")
    source = _replace_assignment(source, "local_ip", "''")
    # The supplied Python sample uses the C++ spelling at this one call site;
    # the shipped binding exposes the normal Python lower-camel spelling.
    source = source.replace(".QueryEndpointId(", ".queryEndpointId(")
    source, substitutions = re.subn(
        r"api\.createTraderApi\(3,\s*os\.getcwd\(\),\s*4\)",
        "api.createTraderApi(_client_id, _runtime_dir, 0)",
        source,
        count=1,
    )
    if substitutions != 1:
        raise VendorSweepError("could not replace vendor CreateTraderApi call")
    supplementary = """    api.setHeartBeatInterval(30)
    api.getErrMsgByCode(0, session)
    api.getClientIDByEMTID(0)
    api.getAccountByEMTID(0)
    reqid += 1
    api.queryOptionCombinedPosition({'combined_security_id': '', 'market': 0}, session, reqid)
    api.creditSellStockRepayDebtInterestFee({}, user, session)

"""
    source, insertions = re.subn(
        r"(?m)^    reqid \+= 1\r?\n(?=    retQueryAsset = api\.queryAsset)",
        supplementary + "    reqid += 1\n",
        source,
        count=1,
    )
    if insertions != 1:
        raise VendorSweepError("could not add supplementary Python binding calls")
    hook = "_instrument_vendor_api(TestApi)\n\n"
    source = source.replace("if __name__ == '__main__':", hook + "if __name__ == '__main__':", 1)
    return source


def _instrument_vendor_api(
    api_class: type[Any], records: dict[str, dict[str, list[int] | int]]
) -> None:
    """Wrap vendor sample methods without retaining inputs or business payloads."""

    for name in dir(api_class):
        if name.startswith("_"):
            continue
        original = getattr(api_class, name)
        if not callable(original):
            continue
        if name.startswith("on"):

            def callback(
                self: Any, *args: Any, _name: str = name, _original: Any = original
            ) -> Any:
                records[_name]["callbacks"] += 1
                return _original(self, *args)

            setattr(api_class, name, callback)
        else:

            def method(self: Any, *args: Any, _name: str = name, _original: Any = original) -> Any:
                started = perf_counter_ns()
                try:
                    return _original(self, *args)
                finally:
                    records[_name]["return_ns"].append(perf_counter_ns() - started)

            setattr(api_class, name, method)


def _summary(samples: list[int]) -> dict[str, int | None]:
    if not samples:
        return {"count": 0, "min_ns": None, "p50_ns": None, "max_ns": None}
    ordered = sorted(samples)
    return {
        "count": len(samples),
        "min_ns": ordered[0],
        "p50_ns": ordered[(len(ordered) - 1) // 2],
        "max_ns": ordered[-1],
    }


def run_vendor_sweep(settings: VendorSweepSettings) -> dict[str, Any]:
    """Run the vendor sample once for a configured simulated-account profile."""

    account = _profile_account(settings.accounts, settings.profile)
    sdk_root = settings.sdk_root.resolve()
    _load_trader_api(sdk_root)
    sample = _sample_path(sdk_root)
    source = _prepare_source(sample.read_text(encoding="utf-8"))
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    records: dict[str, dict[str, list[int] | int]] = defaultdict(
        lambda: {"return_ns": [], "callbacks": 0}
    )
    namespace: dict[str, Any] = {
        "__name__": "__main__",
        "__file__": str(sample),
        "_client_id": 91,
        "_runtime_dir": str(settings.runtime_dir),
        "_profile": {
            "host": settings.accounts.host,
            "port": settings.accounts.port,
            "account": account.account,
            "password": account.password,
        },
        "_instrument_vendor_api": lambda api_class: _instrument_vendor_api(api_class, records),
        "print": lambda *args, **kwargs: None,
    }
    started_ns = perf_counter_ns()
    try:
        with _suppress_native_console_output():
            exec(compile(source, str(sample), "exec"), namespace, namespace)
    except SystemExit:
        pass
    except Exception as exc:
        raise VendorSweepError(f"vendor sweep aborted: {type(exc).__name__}: {exc}") from exc
    elapsed_ns = perf_counter_ns() - started_ns
    operations = [
        {
            "binding_method": name,
            "return_latency": _summary(list(value["return_ns"])),
            "callbacks": value["callbacks"],
        }
        for name, value in sorted(records.items())
    ]
    return {
        "schema_version": 1,
        "driver": "vendor-simulated-sweep",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": settings.profile,
        "elapsed_ns": elapsed_ns,
        "operations": operations,
        "safety": {
            "environment": "vendor-simulated-account",
            "vendor_sample": sample.name,
            "sdk_console_output_discarded": True,
            "credentials_in_report": False,
        },
    }
