from __future__ import annotations

from tools.acceptance.t6_emt_query_fixture_v1 import _SAFE_CASES, t6_query_fixture_v1
from tools.performance.perfbench.query_fixture import load_query_fixture


def test_t6_fixture_is_catalog_complete_and_enables_only_reviewed_read_only_cases(tmp_path) -> None:
    fixture = t6_query_fixture_v1()
    cases = fixture["cases"]
    assert isinstance(cases, list)
    enabled = {(case["profile"], case["api"]) for case in cases if case["enabled"]}
    assert enabled == set(_SAFE_CASES)
    assert _SAFE_CASES[("credit", "QueryCreditPositionFullRate")] == [
        {"ticker": "159934", "market": 1}
    ]
    assert _SAFE_CASES[("ordinary", "QueryUnfinishedOrders")] == []
    output = tmp_path / "fixture.json"
    import json

    output.write_text(json.dumps(fixture), encoding="utf-8")
    assert len(load_query_fixture(output, profile="ordinary")) == 14
    assert len(load_query_fixture(output, profile="credit")) == 16
    assert len(load_query_fixture(output, profile="option")) == 10
