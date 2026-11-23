from __future__ import annotations

from tools.acceptance.t6_499_burst_fixture_v1 import (
    _BURST_WIDTH,
    t6_499_burst_fixture_v1,
    write_t6_499_burst_fixture,
)
from tools.performance.perfbench.tradecore_tight_burst_order_plan import (
    materialize_historical_controlled_fixture_order_plan,
)


def test_t6_499_fixture_is_armed_simulated_and_binds_a_tight_plan(tmp_path) -> None:
    raw = t6_499_burst_fixture_v1()
    assert raw["safety"]["armed"] is True  # type: ignore[index]
    assert raw["safety"]["environment"] == "simulated"  # type: ignore[index]
    assert raw["safety"]["max_total_submissions"] == _BURST_WIDTH  # type: ignore[index]

    fixture = tmp_path / "t6-499-fixture.json"
    assert write_t6_499_burst_fixture(fixture) == fixture.name
    plan = tmp_path / "plan.json"
    plan_hash = materialize_historical_controlled_fixture_order_plan(
        fixture=fixture, output=plan, expires_at_unix_s=None
    )
    assert len(plan_hash) == 64
