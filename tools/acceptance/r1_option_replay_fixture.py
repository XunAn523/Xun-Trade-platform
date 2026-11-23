"""Derive one bounded, private option replay-seed fixture from EMT read-only data.

The option replay seed must never recycle a documentation example or guess an
expired contract.  This probe logs in to the simulated option profile, asks
EMT for option auction metadata, keeps candidate rows only in memory, and
writes one restricted mutation fixture only after selecting a contract that
is currently listed, has a one-contract buy limit and a valid passive price
inside its reported limits.  It does not submit, cancel, subscribe to a
public topic, or retain callback payloads in the report.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Event
from typing import Any, Mapping, Sequence

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)
from emt_gateway.session import normalize_emt_trading_day
from tools.performance.perfbench.mutation_fixture import mutation_fixture_template
from tools.performance.perfbench.simulated_accounts import load_simulated_accounts


class R1OptionReplayFixtureError(RuntimeError):
    """A safe option contract fixture cannot be established."""


@dataclass(frozen=True)
class OptionCandidate:
    ticker: str
    market: int
    price_tick: Decimal
    lower_limit_price: Decimal
    upper_limit_price: Decimal
    last_trade_date: int
    lmt_buy_min_qty: int
    lmt_buy_max_qty: int


@dataclass(frozen=True)
class Settings:
    sdk_root: Path
    accounts_file: Path
    session_client_id: int
    output_fixture: Path
    output_report: Path
    runtime_dir: Path
    timeout_seconds: float
    execute_simulated: bool

    def validate(self) -> None:
        if platform.system() != "Linux":
            raise R1OptionReplayFixtureError("option fixture probe requires Linux")
        if not self.execute_simulated:
            raise R1OptionReplayFixtureError("--execute-simulated is required")
        if not self.sdk_root.is_dir() or not self.accounts_file.is_file():
            raise R1OptionReplayFixtureError("option fixture probe input is unavailable")
        if not 1 <= self.session_client_id <= 255:
            raise R1OptionReplayFixtureError("option fixture EMT client id is invalid")
        if not 5.0 <= self.timeout_seconds <= 60.0:
            raise R1OptionReplayFixtureError("option fixture timeout must be in 5..60 seconds")
        if self.output_fixture.exists() or self.output_report.exists():
            raise R1OptionReplayFixtureError("option fixture probe output must be a new path")


def _decimal(value: object) -> Decimal | None:
    try:
        converted = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return converted if converted.is_finite() else None


def _candidate_from_callback(data: Mapping[str, object]) -> OptionCandidate | None:
    ticker, market = data.get("ticker"), data.get("security_id_source")
    last_trade_date = data.get("last_trade_date")
    minimum, maximum = data.get("lmt_buy_min_qty"), data.get("lmt_buy_max_qty")
    if (
        not isinstance(ticker, str)
        or not ticker.isascii()
        or not ticker.isdecimal()
        or len(ticker) != 8
        or type(market) is not int
        or market not in {1, 2}
        or type(last_trade_date) is not int
        or type(minimum) is not int
        or type(maximum) is not int
    ):
        return None
    tick = _decimal(data.get("price_tick"))
    lower = _decimal(data.get("lower_limit_price"))
    upper = _decimal(data.get("upper_limit_price"))
    if tick is None or lower is None or upper is None or tick <= 0 or lower <= 0 or lower >= upper:
        return None
    if not 1 <= minimum <= 1 <= maximum:
        return None
    return OptionCandidate(
        ticker=ticker,
        market=market,
        price_tick=tick,
        lower_limit_price=lower,
        upper_limit_price=upper,
        last_trade_date=last_trade_date,
        lmt_buy_min_qty=minimum,
        lmt_buy_max_qty=maximum,
    )


def _select_candidate(candidates: list[OptionCandidate], *, trading_day: str) -> OptionCandidate:
    trade_day_number = int(trading_day.replace("-", ""))
    eligible = [item for item in candidates if item.last_trade_date >= trade_day_number]
    if not eligible:
        raise R1OptionReplayFixtureError("EMT returned no currently listed one-contract option")
    # Deterministic selection makes two read-only probes comparable without
    # leaking a contract identifier in evidence output.
    return min(eligible, key=lambda item: (item.market, item.ticker))


def _passive_price(candidate: OptionCandidate) -> Decimal:
    price = candidate.lower_limit_price + candidate.price_tick * 5
    if not candidate.lower_limit_price < price < candidate.upper_limit_price:
        raise R1OptionReplayFixtureError("selected option lacks a safe passive price inside limits")
    return price


def _write_fixture(path: Path, *, candidate: OptionCandidate, price: Decimal) -> None:
    fixture = mutation_fixture_template()
    fixture["safety"]["armed"] = True
    insert = next(
        item
        for item in fixture["cases"]
        if item["api"] == "InsertOrder" and item["profile"] == "option"
    )
    cancel = next(
        item
        for item in fixture["cases"]
        if item["api"] == "CancelOrder" and item["profile"] == "option"
    )
    insert.update(
        {
            "enabled": True,
            "request": {
                "ticker": candidate.ticker,
                "market": candidate.market,
                "side": 1,
                "price_type": 1,
                "price": float(price),
                "quantity": 1,
                "business_type": 10,
                "position_effect": 1,
                "ep_id": 0,
            },
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryOrderByEMTID",
        }
    )
    cancel.update(
        {
            "enabled": True,
            "request": {},
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryOrderByEMTID",
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def run(settings: Settings) -> dict[str, object]:
    settings.validate()
    accounts = load_simulated_accounts(settings.accounts_file)
    account = accounts.option
    loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
    candidates: list[OptionCandidate] = []
    terminal = Event()
    api: Any | None = None
    session_id = 0
    raw_trading_day: str | None = None
    callback_count = 0

    class Api(loaded.trader_api_type):
        def onQueryOptionAuctionInfo(
            self, data: Any, error: Any, request_id: int, is_last: bool, session: int
        ) -> None:
            nonlocal callback_count
            del session
            if request_id != 1:
                return
            callback_count += 1
            error_id = error.get("error_id") if isinstance(error, Mapping) else None
            if error_id not in (None, 0):
                terminal.set()
                return
            if isinstance(data, Mapping):
                candidate = _candidate_from_callback(data)
                if candidate is not None:
                    candidates.append(candidate)
            if is_last:
                terminal.set()

    try:
        api = Api()
        settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        with suppress_native_console_output():
            api.createTraderApi(
                settings.session_client_id, str(settings.runtime_dir / "sdk-logs"), 0
            )
            api.setSoftwareVersion("tradecore/r1-option-replay-fixture")
            api.setHeartBeatInterval(10)
            session_id = int(
                api.login(accounts.host, accounts.port, account.account, account.password, 1, "")
            )
            if session_id <= 0:
                raise R1OptionReplayFixtureError("option fixture EMT login failed")
            raw_trading_day = str(api.getTradingDay())
            # The EMT Python manual specifies a null query parameter for all
            # option contracts; the binding represents that as an empty dict.
            returned = int(api.queryOptionAuctionInfo({}, session_id, 1))
            if returned != 0:
                raise R1OptionReplayFixtureError("option fixture query was not accepted")
        if not terminal.wait(settings.timeout_seconds):
            raise R1OptionReplayFixtureError("option fixture query did not reach last=true")
        trading_day = normalize_emt_trading_day(str(raw_trading_day))
        candidate = _select_candidate(candidates, trading_day=trading_day)
        _write_fixture(
            settings.output_fixture, candidate=candidate, price=_passive_price(candidate)
        )
    finally:
        if api is not None:
            with suppress_native_console_output():
                try:
                    if session_id:
                        api.logout(session_id)
                finally:
                    api.release()
    report = {
        "schema_version": "tradecore-r1-option-replay-fixture-v1",
        "acceptance_status": "passed",
        "environment": "simulated",
        "login_succeeded": session_id != 0,
        "query_callback_count": callback_count,
        "query_terminal": terminal.is_set(),
        "fixture_written": settings.output_fixture.name,
        "payloads_retained": False,
        "credentials_retained": False,
        "mutation": False,
        "ready_promotion": False,
    }
    settings.output_report.parent.mkdir(parents=True, exist_ok=True)
    settings.output_report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--session-client-id", type=int, required=True)
    parser.add_argument("--output-fixture", type=Path, required=True)
    parser.add_argument("--output-report", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--execute-simulated", action="store_true")
    args = parser.parse_args(argv)
    report = run(
        Settings(
            sdk_root=args.sdk_root,
            accounts_file=args.accounts_file,
            session_client_id=args.session_client_id,
            output_fixture=args.output_fixture,
            output_report=args.output_report,
            runtime_dir=args.runtime_dir,
            timeout_seconds=args.timeout,
            execute_simulated=args.execute_simulated,
        )
    )
    print(f"R1_OPTION_REPLAY_FIXTURE_PASS={report['acceptance_status'] == 'passed'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
