from __future__ import annotations

import unittest

from xuanling_tradecore.adapters.emt import (
    EmtRecoveryCertificateCollectorV1,
    EmtRecoveryCertificateErrorV1,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    TradingDayEpoch,
)

DAY = TradingDayEpoch("2026-09-11")


def _event(kind: str, **summary: str) -> BrokerEvent:
    normalized = {
        "callback_name": {
            "orders_by_page": "onQueryOrderByPage",
            "unfinished_orders": "onQueryOrder",
            "trades_by_page": "onQueryTradeByPage",
            "positions_by_page": "onQueryPositionByPage",
            "assets": "onQueryAsset",
            "funds": "onQueryFund",
            "credit_fund_info": "onQueryCreditFundInfo",
            "credit_asset_debt": "onQueryCreditAssetDebtInfo",
            "credit_position_extra_info_sz": "onQueryCreditPositionExtraInfo",
            "credit_position_extra_info_sh": "onQueryCreditPositionExtraInfo",
        }[kind],
        "query_kind": kind,
        "terminal": "true",
        "error_id": "0",
    }
    normalized.update(summary)
    return BrokerEvent(
        event_id=f"event-{kind}-{len(summary)}",
        schema_version=1,
        source_key_schema_version=1,
        source_key=f"test:{kind}:{len(summary)}",
        event_kind=BrokerEventKind.QUERY,
        source_kind=BrokerSourceKind.CALLBACK,
        venue_id="EMT",
        adapter_instance_id="adapter-1",
        account_id="account-1",
        trading_day=DAY,
        received_monotonic_ns=1,
        normalized_summary=normalized,
    )


class EmtRecoveryCertificateTests(unittest.TestCase):
    def _collector(self, *, profile: str = "ordinary") -> EmtRecoveryCertificateCollectorV1:
        return EmtRecoveryCertificateCollectorV1(
            certificate_id="r1-certificate-1",
            account_id="account-1",
            adapter_instance_id="adapter-1",
            adapter_client_scope="scope-1",
            trading_day=DAY,
            account_profile=profile,
            captured_at_wall_ns=10,
        )

    def _record_complete_ordinary(self, collector: EmtRecoveryCertificateCollectorV1) -> None:
        collector.record_terminal(
            _event(
                "orders_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(_event("unfinished_orders", result_count="0"))
        collector.record_terminal(
            _event(
                "trades_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(
            _event(
                "positions_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(_event("assets", result_count="1"))
        collector.record_terminal(_event("funds", result_count="1"))

    def test_complete_ordinary_window_has_all_query_digests(self) -> None:
        collector = self._collector()
        self._record_complete_ordinary(collector)
        result = collector.build(
            callback_replay_complete=True,
            callback_gaps_closed=True,
            stable_double_scan=True,
            completed_at_wall_ns=20,
        )
        self.assertEqual((), result.missing_query_kinds)
        self.assertEqual((), result.pending_paged_query_kinds)
        self.assertTrue(result.negative_confirmation_complete)
        self.assertEqual(result.certificate.certificate_hash, result.certificate.content_sha256)
        self.assertEqual((), result.incompleteness_reasons)

    def test_paged_series_cannot_complete_until_the_last_page(self) -> None:
        collector = self._collector()
        collector.record_terminal(
            _event(
                "orders_by_page",
                page_reference="7",
                page_result_count="100",
                page_has_more="true",
            )
        )
        self.assertEqual(7, collector.next_continuation("orders_by_page"))
        result = collector.build(
            callback_replay_complete=True,
            callback_gaps_closed=True,
            stable_double_scan=True,
            completed_at_wall_ns=20,
        )
        self.assertIn("orders_by_page", result.missing_query_kinds)
        self.assertIn("orders_by_page", result.pending_paged_query_kinds)
        self.assertIn("QUERY_INCOMPLETE", result.incompleteness_reasons)

    def test_missing_replay_and_nonempty_working_orders_remain_incomplete(self) -> None:
        # A window whose unfinished query records an existing order must never
        # be repackaged as a negative confirmation.
        collector = self._collector()
        collector.record_terminal(
            _event(
                "orders_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(_event("unfinished_orders", result_count="1"))
        collector.record_terminal(
            _event(
                "trades_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(
            _event(
                "positions_by_page",
                page_reference="0",
                page_result_count="0",
                page_has_more="false",
            )
        )
        collector.record_terminal(_event("assets", result_count="1"))
        collector.record_terminal(_event("funds", result_count="1"))
        result = collector.build(
            callback_replay_complete=False,
            callback_gaps_closed=True,
            stable_double_scan=True,
            completed_at_wall_ns=20,
        )
        self.assertFalse(result.negative_confirmation_complete)
        self.assertEqual(
            ("CALLBACK_REPLAY_INCOMPLETE", "NEGATIVE_CONFIRMATION_UNAVAILABLE"),
            result.incompleteness_reasons,
        )

    def test_repeated_paged_cursor_and_foreign_event_are_rejected(self) -> None:
        collector = self._collector()
        event = _event(
            "orders_by_page",
            page_reference="7",
            page_result_count="100",
            page_has_more="true",
        )
        collector.record_terminal(event)
        with self.assertRaisesRegex(EmtRecoveryCertificateErrorV1, "repeated a cursor"):
            collector.record_terminal(event)
        foreign = _event("assets", result_count="1")
        object.__setattr__(foreign, "account_id", "other-account")
        with self.assertRaisesRegex(EmtRecoveryCertificateErrorV1, "another scope"):
            collector.record_terminal(foreign)


if __name__ == "__main__":
    unittest.main()
