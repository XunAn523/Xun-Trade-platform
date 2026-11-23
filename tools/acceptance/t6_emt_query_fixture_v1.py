"""Generate the reviewed, credential-free T6 read-only query fixture.

The output is deliberately generated on the target host from Git-controlled
code.  It contains only documented empty filters, page cursors and public
instrument identifiers; account credentials and endpoints continue to come
solely from the host-local account profile.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from tools.performance.perfbench.query_fixture import query_fixture_template


class T6QueryFixtureError(ValueError):
    """The checked-in query arguments no longer match the full catalog."""


_SAFE_CASES: dict[tuple[str, str], list[Any]] = {
    # This unfiltered query is the authoritative, payload-free cleanup proof
    # for a controlled order window.  It takes no business arguments and is
    # read-only; the runner records only the terminal flag and item count.
    ("ordinary", "QueryUnfinishedOrders"): [],
    ("ordinary", "QueryBlackWhiteListByPage"): [{"req_count": 200, "reference": 0, "market": 0, "is_black": 0}],
    ("ordinary", "QueryDMATickerAssignInfoByPage"): [{"req_count": 200, "reference": 0}],
    ("ordinary", "QueryETF"): [{"ticker": "510050", "market": 2}],
    ("ordinary", "QueryETFByPage"): [{"reference": 0, "req_count": 1}],
    ("ordinary", "QueryETFTickerBasket"): [{"ticker": "510050", "market": 2}],
    ("ordinary", "QueryEndpointId"): [{"exchange_id": 1}],
    ("ordinary", "QueryHKConnectIndicativeExchangeRateInfo"): [],
    ("ordinary", "QueryHkConnectSpreadTable"): [{"ticker_type": 0}],
    ("ordinary", "QueryOrdersByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("ordinary", "QueryPositionByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("ordinary", "QuerySecurityByPage"): [{"reference": 0, "req_count": 20}],
    ("ordinary", "QuerySecurityInfo"): [{"ticker": "000001", "market": 1}],
    ("ordinary", "QueryTradesByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("credit", "QueryCreditAssetDebtInfo"): [],
    ("credit", "QueryCreditCashRepayInfo"): [],
    ("credit", "QueryCreditDebtInfo"): [],
    ("credit", "QueryCreditDebtToRepayTodayByPage"): [{"req_count": 10, "reference": 0}],
    ("credit", "QueryCreditFundExtraInfo"): [],
    ("credit", "QueryCreditFundInfo"): [],
    ("credit", "QueryCreditMarginRate"): [{"ticker": "000001", "market": 1}],
    ("credit", "QueryCreditPledgeStkByPage"): [{"req_count": 100, "reference": 0}],
    ("credit", "QueryCreditPledgeStkRate"): [{"ticker": "000001", "market": 1}],
    # EMT's own Python sample uses a concrete public Shenzhen ticker here.
    # Empty market/ticker predicates are not a valid request for this API and
    # can leave an otherwise healthy credit session without a terminal reply.
    ("credit", "QueryCreditPositionFullRate"): [{"ticker": "159934", "market": 1}],
    ("credit", "QueryCreditQuotaTransfer"): [{"serial_id": 0}],
    ("credit", "QueryCreditTargetStkByPage"): [{"req_count": 100, "reference": 0}],
    ("credit", "QueryCreditTickerAssignInfoByPage"): [{"req_count": 1, "reference": 0}],
    ("credit", "QueryOrdersByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("credit", "QueryPositionByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("credit", "QueryTradesByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("option", "QueryOptionCombinedOrders"): [{"comb_num": "", "begin_time": 0, "end_time": 0}],
    ("option", "QueryOptionCombinedOrdersByPage"): [{"req_count": 100, "reference": 0, "reserved": 1}],
    ("option", "QueryOptionCombinedPosition"): [{"combined_security_id": "", "market": 0}],
    ("option", "QueryOptionCombinedStrategyInfo"): [],
    ("option", "QueryOptionCombinedTrades"): [{"comb_num": "", "begin_time": 0, "end_time": 0}],
    ("option", "QueryOptionCombinedTradesByPage"): [{"req_count": 100, "reference": 0, "reserved": 1}],
    ("option", "QueryOptionCombinedUnfinishedOrders"): [],
    ("option", "QueryOrdersByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("option", "QueryPositionByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
    ("option", "QueryTradesByPage"): [{"req_count": 10, "reference": 0, "reserved": 0}],
}


def t6_query_fixture_v1() -> dict[str, Any]:
    """Return a catalog-complete fixture with only reviewed read-only cases enabled."""

    fixture = query_fixture_template()
    cases = fixture.get("cases")
    if not isinstance(cases, list):
        raise T6QueryFixtureError("query fixture template has no cases")
    seen: set[tuple[str, str]] = set()
    for entry in cases:
        if not isinstance(entry, dict):
            raise T6QueryFixtureError("query fixture template case is invalid")
        profile, api = entry.get("profile"), entry.get("api")
        if not isinstance(profile, str) or not isinstance(api, str):
            raise T6QueryFixtureError("query fixture template identity is invalid")
        key = (profile, api)
        if key in _SAFE_CASES:
            entry["enabled"] = True
            entry["business_args"] = deepcopy(_SAFE_CASES[key])
            entry["notes"] = "T6 reviewed read-only query contract"
            seen.add(key)
    if seen != set(_SAFE_CASES):
        missing = sorted(set(_SAFE_CASES) - seen)
        raise T6QueryFixtureError(f"T6 query catalog no longer exposes configured cases: {missing[:3]}")
    fixture["purpose"] = "T6 EMT simulated read-only query contract; Git-generated on target host"
    fixture["instructions"] = (
        "Read-only only. Generated from a reviewed contract; it stores no credentials, endpoints, "
        "account identifiers, order parameters, or callback payloads."
    )
    return fixture


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    fixture = t6_query_fixture_v1()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    enabled = sum(bool(case["enabled"]) for case in fixture["cases"])
    print(json.dumps({"output": str(args.output), "enabled_read_only_cases": enabled}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
