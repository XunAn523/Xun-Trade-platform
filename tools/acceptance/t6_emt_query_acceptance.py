"""Summarize payload-free real EMT T6 read-only query evidence.

This tool does not call the SDK.  It verifies and reduces reports already
written by the isolated ``gateway-query-fixture`` runner, so a vendor-wrapper
shutdown hang can never erase a completed query outcome or be mistaken for a
successful graceful logout.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from xuanling_tradecore.adapters.emt.trading_catalog_v1 import trader_api_spec_v1


class T6QueryAcceptanceError(ValueError):
    """A supplied isolated real-query artifact cannot be accepted as evidence."""


_EMT_NATIVE_RETURN_UNSUPPORTED_VENUE = 6077
"""Official EMT 2.27 error list: ``6077`` means the business is unsupported."""


def build_t6_query_acceptance(reports: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Create one exact, payload-free T6 query evidence table.

    An ``ok`` request is verified only when its own request-id observation also
    contains a terminal callback.  Missing terminal callbacks remain
    ``blocked_semantics``.  The one exact native return code documented by the
    supplied EMT 2.27 SDK as “business unsupported” is classified separately
    as ``unsupported_venue``; all other rejections remain blocked.
    """

    observed: dict[tuple[str, str], dict[str, object]] = {}
    profiles: set[str] = set()
    for report in reports:
        profile = _profile(report)
        profiles.add(profile)
        outcomes = _outcomes(report)
        observations = _observations(report)
        for outcome in outcomes:
            api = _required_text(outcome, "api")
            key = (profile, api)
            if key in observed:
                raise T6QueryAcceptanceError(f"duplicate T6 query result: {profile}/{api}")
            observation = observations.get(api)
            if observation is None:
                raise T6QueryAcceptanceError(f"missing request observation: {profile}/{api}")
            spec = trader_api_spec_v1(api)
            if spec.expected_callback is None:
                raise T6QueryAcceptanceError(f"fixture contains a non-query API: {api}")
            outcome_value = _required_text(outcome, "outcome")
            terminal_seen = observation["terminal_seen"]
            verification = _verification(outcome_value, _reason(outcome), terminal_seen)
            observed[key] = {
                "profile": profile,
                "api": api,
                "expected_callback": spec.expected_callback,
                "outcome": outcome_value,
                "reason": _reason(outcome),
                "terminal_seen": terminal_seen,
                "callback_count": observation["callback_count"],
                "verification": verification,
            }
    if not observed:
        raise T6QueryAcceptanceError("at least one gateway-query-fixture report is required")
    cases = [observed[key] for key in sorted(observed)]
    verified = sum(case["verification"] == "terminal_callback_observed" for case in cases)
    timeouts = sum(case["outcome"] == "timeout" for case in cases)
    rejected = sum(case["outcome"] == "rejected" for case in cases)
    unsupported_venue = sum(case["verification"] == "unsupported_venue" for case in cases)
    return {
        "schema": "tradecore.emt.t6-query-acceptance.v1",
        "driver": "gateway-query-fixture-c0-c7",
        "profiles": sorted(profiles),
        "shutdown_semantics": "isolated_after_report; not evidence of graceful vendor logout",
        "cases": cases,
        "summary": {
            "query_cases": len(cases),
            "terminal_callback_verified": verified,
            "blocked_semantics": sum(case["verification"] == "blocked_semantics" for case in cases),
            "unsupported_venue": unsupported_venue,
            "timeouts": timeouts,
            "native_rejected": rejected,
        },
        "release_conclusion": (
            "not_ready: every query contract needs an observed terminal callback; "
            "this evidence must be combined with all other T6 API/SPI, recovery, and hot-path gates"
        ),
    }


def _profile(report: Mapping[str, object]) -> str:
    if report.get("driver") != "gateway-query-fixture-c0-c7":
        raise T6QueryAcceptanceError("report is not a gateway-query-fixture artifact")
    run = report.get("run")
    if not isinstance(run, Mapping):
        raise T6QueryAcceptanceError("query report run is invalid")
    profile = run.get("profile")
    if profile not in {"ordinary", "credit", "option"}:
        raise T6QueryAcceptanceError("query report profile is invalid")
    return profile


def _outcomes(report: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    run = report["run"]
    assert isinstance(run, Mapping)
    values = run.get("operation_outcomes")
    if not isinstance(values, list) or not all(isinstance(value, Mapping) for value in values):
        raise T6QueryAcceptanceError("query operation outcomes are invalid")
    return tuple(values)


def _observations(report: Mapping[str, object]) -> dict[str, dict[str, object]]:
    run = report["run"]
    assert isinstance(run, Mapping)
    values = run.get("operation_observations")
    if not isinstance(values, list) or not all(isinstance(value, Mapping) for value in values):
        raise T6QueryAcceptanceError("query operation observations are invalid")
    result: dict[str, dict[str, object]] = {}
    for value in values:
        api = _required_text(value, "api")
        if api in result:
            raise T6QueryAcceptanceError(f"duplicate request observation: {api}")
        terminal_seen = value.get("terminal_seen")
        callback_count = value.get("callback_count")
        if type(terminal_seen) is not bool or type(callback_count) is not int or callback_count < 0:
            raise T6QueryAcceptanceError(f"query observation is malformed: {api}")
        result[api] = {
            "terminal_seen": terminal_seen,
            "callback_count": callback_count,
        }
    return result


def _required_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise T6QueryAcceptanceError(f"query evidence {field} is invalid")
    return value


def _reason(row: Mapping[str, object]) -> str | int | None:
    value = row.get("reason")
    if value is None or isinstance(value, (str, int)):
        return value
    raise T6QueryAcceptanceError("query evidence reason is invalid")


def _verification(outcome: str, reason: str | int | None, terminal_seen: bool) -> str:
    if outcome == "ok" and terminal_seen:
        return "terminal_callback_observed"
    if outcome == "rejected" and reason == f"native_return_{_EMT_NATIVE_RETURN_UNSUPPORTED_VENUE}":
        return "unsupported_venue"
    return "blocked_semantics"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    reports: list[Mapping[str, object]] = []
    for path in args.input:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise T6QueryAcceptanceError(f"query report must be an object: {path}")
        reports.append(raw)
    result = build_t6_query_acceptance(reports)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "summary": result["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
