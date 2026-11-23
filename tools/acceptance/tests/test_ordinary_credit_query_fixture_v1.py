from __future__ import annotations

from tools.acceptance.ordinary_credit_query_fixture_v1 import ordinary_credit_query_fixture_v1


def test_ordinary_credit_fixture_enables_only_safe_non_option_cases() -> None:
    fixture = ordinary_credit_query_fixture_v1()
    entries = {(entry["profile"], entry["api"]): entry for entry in fixture["cases"]}

    assert entries[("ordinary", "QueryAsset")]["enabled"]
    assert entries[("ordinary", "QueryFundTransfer")]["business_args"] == [{"serial_id": 0}]
    assert entries[("credit", "QueryCreditTickerDebtInfo")]["enabled"]
    assert entries[("credit", "QueryCreditTickerDebtInfo")]["business_args"] == [
        {"ticker": "159934", "market": 1}
    ]
    assert not any(entry["enabled"] for entry in entries.values() if entry["profile"] == "option")
    assert "Ordinary/credit scope only" in fixture["instructions"]
    assert fixture["minimum_callback_capacity"] == {
        "ordinary": 8192,
        "credit": 4096,
        "option": 1,
    }
