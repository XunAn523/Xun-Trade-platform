from __future__ import annotations

import unittest
from decimal import Decimal

from tools.acceptance.r1_option_replay_fixture import (
    OptionCandidate,
    R1OptionReplayFixtureError,
    _candidate_from_callback,
    _passive_price,
    _select_candidate,
)


class R1OptionReplayFixtureTests(unittest.TestCase):
    def test_selects_only_current_single_contract_candidate(self) -> None:
        expired = OptionCandidate(
            ticker="90000001",
            market=1,
            price_tick=Decimal("0.001"),
            lower_limit_price=Decimal("0.1"),
            upper_limit_price=Decimal("1"),
            last_trade_date=20260910,
            lmt_buy_min_qty=1,
            lmt_buy_max_qty=1,
        )
        current = OptionCandidate(
            ticker="90000002",
            market=2,
            price_tick=Decimal("0.001"),
            lower_limit_price=Decimal("0.1"),
            upper_limit_price=Decimal("1"),
            last_trade_date=20260911,
            lmt_buy_min_qty=1,
            lmt_buy_max_qty=5,
        )
        selected = _select_candidate([expired, current], trading_day="2026-09-11")
        self.assertEqual("90000002", selected.ticker)
        self.assertEqual(Decimal("0.105"), _passive_price(selected))

    def test_rejects_bad_callback_envelope_and_no_current_contract(self) -> None:
        self.assertIsNone(_candidate_from_callback({"ticker": "invalid"}))
        with self.assertRaisesRegex(R1OptionReplayFixtureError, "currently listed"):
            _select_candidate([], trading_day="2026-09-11")


if __name__ == "__main__":
    unittest.main()
