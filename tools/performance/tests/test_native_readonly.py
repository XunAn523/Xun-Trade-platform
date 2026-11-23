from __future__ import annotations

import unittest

from perfbench.native_readonly import READ_ONLY_CASES, _make_measured_api, _query_callback_parts


class NativeReadOnlyTests(unittest.TestCase):
    def test_cases_are_explicitly_read_only(self) -> None:
        names = {case.name for case in READ_ONLY_CASES}
        self.assertEqual(10, len(names))
        forbidden = ("Insert", "Cancel", "Transfer", "Repay", "Exercise", "Combined")
        self.assertFalse(any(token in name for token in forbidden for name in names))

    def test_cases_have_unique_callbacks(self) -> None:
        self.assertTrue(all(case.callback.startswith("onQuery") for case in READ_ONLY_CASES))

    def test_callback_shapes_are_normalized(self) -> None:
        error = {"error_id": 0}
        self.assertEqual((error, 11, True), _query_callback_parts(({}, error, 11, 99)))
        self.assertEqual((error, 12, False), _query_callback_parts(({}, error, 12, False, 99)))

    def test_yesterday_asset_uses_the_documented_terminal_callback_shape(self) -> None:
        class FakeTraderApi:
            pass

        api = _make_measured_api(FakeTraderApi)()
        pending = api.begin(51, "QueryYesterdayAsset", 1)
        api.onQueryYesterdayAsset({}, {"error_id": 0}, 51, True, 99)
        self.assertTrue(pending.done.is_set())
        self.assertEqual(0, pending.error_id)
