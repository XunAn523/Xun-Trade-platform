from __future__ import annotations

from tools.acceptance.t6_controlled_mutation_fixture_v1 import (
    t6_credit_quota_transfer_fixture_v1,
    t6_fund_transfer_fixture_v1,
)
from tools.performance.perfbench.mutation_fixture import (
    load_mutation_fixture,
    validate_mutation_fixture,
)


def test_t6_fund_transfer_fixture_enables_only_one_tiny_reversible_action(tmp_path) -> None:
    fixture = t6_fund_transfer_fixture_v1()
    validate_mutation_fixture(fixture)
    cases = fixture["cases"]
    assert isinstance(cases, list)
    enabled = [case for case in cases if case["enabled"]]
    assert len(enabled) == 1
    assert enabled[0] == {
        "api": "FundTransfer",
        "profile": "ordinary",
        "enabled": True,
        "request": {"amount": 0.01, "transfer_type": 0},
        "rate_limit_per_second": 1,
        "reconciliation_query": "QueryFundTransfer",
        "notes": "T6 one-pair reversible simulated acceptance",
    }
    path = tmp_path / "fixture.json"
    import json

    path.write_text(json.dumps(fixture), encoding="utf-8")
    loaded = load_mutation_fixture(path, profile="ordinary")
    assert len(loaded) == 1
    assert loaded[0].api_name == "FundTransfer"


def test_t6_credit_quota_fixture_enables_only_one_tiny_reversible_action(tmp_path) -> None:
    fixture = t6_credit_quota_transfer_fixture_v1()
    validate_mutation_fixture(fixture)
    enabled = [case for case in fixture["cases"] if case["enabled"]]
    assert enabled == [
        {
            "api": "CreditQuotaTransfer",
            "profile": "credit",
            "enabled": True,
            "request": {"serial_id": 0, "amount": 0.01, "transfer_type": 0},
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryCreditQuotaTransfer",
            "notes": "T6 one-pair reversible simulated credit quota acceptance",
        }
    ]
    path = tmp_path / "fixture.json"
    import json

    path.write_text(json.dumps(fixture), encoding="utf-8")
    loaded = load_mutation_fixture(path, profile="credit")
    assert len(loaded) == 1
    assert loaded[0].api_name == "CreditQuotaTransfer"
