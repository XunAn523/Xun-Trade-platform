"""Generate the non-option EMT query fixture used by the ordinary-credit release.

The complete T6 template remains the catalog source of truth.  This generator
only enables additional calls whose parameters are either empty predicates or
documented public identifiers; it never writes credentials, account numbers,
native order IDs, or callback payloads into Git-controlled output.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from tools.acceptance.t6_emt_query_fixture_v1 import t6_query_fixture_v1


class OrdinaryCreditQueryFixtureError(ValueError):
    """The catalog-complete source fixture no longer matches this release plan."""


# Every non-empty ticker here is a public Shenzhen identifier already used by
# the vendor's own Python sample and by the existing T6 rate-query fixture.
# It is neither an account holding nor a locally chosen order target.
_ADDITIONAL_SAFE_CASES: dict[tuple[str, str], list[Any]] = {
    ("ordinary", "QueryAsset"): [],
    ("ordinary", "QueryFund"): [],
    ("ordinary", "QueryFundTransfer"): [{"serial_id": 0}],
    ("ordinary", "QueryIPOInfoList"): [],
    ("ordinary", "QueryIPOQuotaInfo"): [],
    ("ordinary", "QueryIssueInfoList"): [],
    ("ordinary", "QueryOrders"): [{"ticker": "", "begin_time": 0, "end_time": 0}],
    ("ordinary", "QueryPosition"): [""],
    ("ordinary", "QueryTrades"): [{"ticker": "", "begin_time": 0, "end_time": 0}],
    ("ordinary", "QueryYesterdayAsset"): [],
    ("credit", "QueryCreditDebtInfoByPage"): [{"req_count": 10, "reference": 0, "debttype": 0}],
    ("credit", "QueryCreditExcessStock"): [{"ticker": "159934", "market": 1}],
    ("credit", "QueryCreditPositionExtraInfo"): [{"ticker": "159934", "market": 1}],
    ("credit", "QueryCreditTickerAssignInfo"): [{"ticker": "159934", "market": 1}],
    ("credit", "QueryCreditTickerDebtInfo"): [{"ticker": "159934", "market": 1}],
    ("credit", "QueryMulCreditExcessStock"): [{"ticker": "159934", "market": 1}],
}


def ordinary_credit_query_fixture_v1() -> dict[str, Any]:
    """Return a catalog-complete fixture with only ordinary/credit cases armed."""

    fixture = t6_query_fixture_v1()
    cases = fixture.get("cases")
    if not isinstance(cases, list):
        raise OrdinaryCreditQueryFixtureError("source query fixture has no cases")
    seen: set[tuple[str, str]] = set()
    for entry in cases:
        if not isinstance(entry, dict):
            raise OrdinaryCreditQueryFixtureError("source query fixture case is invalid")
        profile, api = entry.get("profile"), entry.get("api")
        if not isinstance(profile, str) or not isinstance(api, str):
            raise OrdinaryCreditQueryFixtureError("source query fixture identity is invalid")
        key = (profile, api)
        if profile == "option":
            entry["enabled"] = False
            entry["notes"] = "disabled by emt-ordinary-credit-v1 release scope"
        elif key in _ADDITIONAL_SAFE_CASES:
            entry["enabled"] = True
            entry["business_args"] = deepcopy(_ADDITIONAL_SAFE_CASES[key])
            entry["notes"] = "ordinary-credit-v1 reviewed read-only query contract"
            seen.add(key)
    if seen != set(_ADDITIONAL_SAFE_CASES):
        missing = sorted(set(_ADDITIONAL_SAFE_CASES) - seen)
        raise OrdinaryCreditQueryFixtureError(
            f"query catalog no longer exposes configured ordinary/credit cases: {missing[:3]}"
        )
    fixture["purpose"] = (
        "Ordinary-credit EMT simulated read-only query contract; Git-generated on target host"
    )
    fixture["instructions"] = (
        "Ordinary/credit scope only. No option case may be enabled. The fixture stores no credentials, "
        "endpoints, account identifiers, order parameters, or callback payloads. Exact-order and "
        "counterparty-fund queries remain runtime-bound and are exercised only by their own controlled drivers."
    )
    # QueryOrders is intentionally an all-history diagnostic.  The isolated
    # acceptance runner must preserve its terminal callback instead of silently
    # truncating after its default 4,096 records.  This does not tune an
    # execution lane or a production hot path.
    fixture["minimum_callback_capacity"] = {
        "ordinary": 8192,
        "credit": 4096,
        "option": 1,
    }
    return fixture


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    fixture = ordinary_credit_query_fixture_v1()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    enabled = sum(bool(case["enabled"]) for case in fixture["cases"])
    print(json.dumps({"output": str(args.output), "enabled_ordinary_credit_cases": enabled}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
