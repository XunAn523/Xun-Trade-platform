"""Generate a bounded, reversible real-simulation mutation fixture.

The output contains no endpoint, credential, account identifier, order ID or
business result.  It enables exactly one 0.01-unit ordinary ``FundTransfer``
and its equal inverse is supplied by the paired driver.  This is an
acceptance artifact, never a production configuration or a generic mutation
template.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tools.performance.perfbench.mutation_fixture import (
    mutation_fixture_template,
    validate_mutation_fixture,
)


def _t6_mutation_fixture(
    *,
    api: str,
    profile: str,
    request: dict[str, object],
    reconciliation_query: str,
    notes: str,
) -> dict[str, Any]:
    """Return one armed catalog fixture without account or endpoint data."""

    fixture = mutation_fixture_template()
    safety = fixture["safety"]
    assert isinstance(safety, dict)
    safety["armed"] = True
    cases = fixture["cases"]
    assert isinstance(cases, list)
    enabled = 0
    for case in cases:
        assert isinstance(case, dict)
        if case["api"] == api and case["profile"] == profile:
            case.update(
                {
                    "enabled": True,
                    "request": request,
                    "rate_limit_per_second": 1,
                    "reconciliation_query": reconciliation_query,
                    "notes": notes,
                }
            )
            enabled += 1
    if enabled != 1:
        raise RuntimeError(f"T6 {api} fixture catalog is incomplete")
    validate_mutation_fixture(fixture)
    return fixture


def t6_fund_transfer_fixture_v1() -> dict[str, Any]:
    """Return the ordinary-account one-pair, equal-and-opposite fixture."""

    return _t6_mutation_fixture(
        api="FundTransfer",
        profile="ordinary",
        request={"amount": 0.01, "transfer_type": 0},
        reconciliation_query="QueryFundTransfer",
        notes="T6 one-pair reversible simulated acceptance",
    )


def t6_credit_quota_transfer_fixture_v1() -> dict[str, Any]:
    """Return the credit-account one-pair, equal-and-opposite quota fixture."""

    return _t6_mutation_fixture(
        api="CreditQuotaTransfer",
        profile="credit",
        request={"serial_id": 0, "amount": 0.01, "transfer_type": 0},
        reconciliation_query="QueryCreditQuotaTransfer",
        notes="T6 one-pair reversible simulated credit quota acceptance",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--kind", choices=("fund-transfer", "credit-quota"), default="fund-transfer")
    args = parser.parse_args()
    fixture = (
        t6_fund_transfer_fixture_v1()
        if args.kind == "fund-transfer"
        else t6_credit_quota_transfer_fixture_v1()
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(fixture, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    enabled = [case["api"] for case in fixture["cases"] if case["enabled"]]
    print(json.dumps({"output": str(args.output), "enabled": enabled}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
