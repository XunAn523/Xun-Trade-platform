"""Tests for EMT's typed unfinished-orders callback ingress boundary."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import AccountTradingCapabilities, BrokerQuery, RuntimeVenueProfile
from xuanling_tradecore.adapters.base import RecoveryHandle
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtAuthoritativeOrderCollectorV1,
    EmtAuthoritativeOrderScanSeriesCollectorV1,
    EmtAuthoritativeOrderV1,
    EmtBrokerAdapter,
    EmtQueryCallbackIngress,
    EmtReadOnlyIngressError,
    EmtSdkReadOnlyPort,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    CallbackGapEvidence,
    RecoveryPlan,
    TradingDayEpoch,
)

DAY = TradingDayEpoch("2026-09-02")


class _Observer:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


class _Transport:
    def __init__(self, return_code: int = 0) -> None:
        self.return_code = return_code
        self.request_ids: list[int] = []
        self.page_call: tuple[str, int, int, int] | None = None

    def query_unfinished_orders(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_orders(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_trades(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_orders_by_page(
        self, page_size: int, continuation: int, native_request_id: int
    ) -> int:
        self.request_ids.append(native_request_id)
        self.page_call = ("orders", page_size, continuation, native_request_id)
        return self.return_code

    def query_trades_by_page(
        self, page_size: int, continuation: int, native_request_id: int
    ) -> int:
        self.request_ids.append(native_request_id)
        self.page_call = ("trades", page_size, continuation, native_request_id)
        return self.return_code

    def query_positions(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_positions_by_page(
        self, page_size: int, continuation: int, native_request_id: int
    ) -> int:
        self.request_ids.append(native_request_id)
        self.page_call = ("positions", page_size, continuation, native_request_id)
        return self.return_code

    def query_assets(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_funds(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_credit_fund_info(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_credit_asset_debt(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.return_code

    def query_credit_position_extra_info(self, market: int, native_request_id: int) -> int:
        assert market in {1, 2}
        self.request_ids.append(native_request_id)
        return self.return_code


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _query(
    request_id: str,
    *,
    kind: str = "unfinished_orders",
    continuation: int = 0,
    page_size: int = 10,
    capture_authoritative_order_rows: bool = False,
) -> BrokerQuery:
    return BrokerQuery(
        request_id=request_id,
        account_id="account-1",
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        query_kind=kind,
        page_size=page_size,
        continuation=continuation,
        capture_authoritative_order_rows=capture_authoritative_order_rows,
    )


class EmtReadOnlyIngressTests(unittest.TestCase):
    def _started(
        self,
        *,
        transport: _Transport | None = None,
        authoritative_order_observer=None,
        authoritative_row_failure_observer=None,
    ):
        mapping = _mapping()
        observer = _Observer()
        ingress = EmtQueryCallbackIngress(
            mapping,
            trading_day=DAY,
            native_session_id=71,
            session_epoch=3,
            authoritative_order_observer=authoritative_order_observer,
            authoritative_row_failure_observer=authoritative_row_failure_observer,
        )
        port = EmtSdkReadOnlyPort(
            ingress,
            transport=transport or _Transport(),
            first_native_request_id=200,
        )
        adapter = EmtBrokerAdapter(mapping, read_only_port=port)
        adapter.start(observer)
        return adapter, ingress, observer, port

    def test_terminal_callback_becomes_payload_free_query_event_and_duplicate_latches_gap(
        self,
    ) -> None:
        adapter, ingress, observer, port = self._started()
        request_id = "private-domain-request-id"
        handle = adapter.query(_query(request_id))
        self.assertEqual(handle.request_id, request_id)
        event = ingress.on_query_order_terminal(
            native_request_id=200,
            native_session_id=71,
            error_id=0,
            is_last=True,
            received_monotonic_ns=10,
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.normalized_summary["query_kind"], "unfinished_orders")
        self.assertEqual(event.normalized_summary["callback_name"], "onQueryOrder")
        self.assertEqual(event.normalized_summary["result_count"], "0")
        self.assertNotIn(request_id, event.source_key)
        self.assertNotIn(request_id, event.normalized_summary.values())
        self.assertEqual(observer.events, [event])
        self.assertEqual(observer.gaps, [])

        self.assertIsNone(
            ingress.on_query_order_terminal(
                native_request_id=200,
                native_session_id=71,
                error_id=0,
                is_last=True,
                received_monotonic_ns=11,
            )
        )
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual(observer.gaps[0].rejected_count, 1)
        self.assertEqual(port.snapshot().active_query_count, 0)
        self.assertEqual(port.snapshot().callback_gap_count, 1)
        self.assertEqual({"native_request_unregistered": 1}, ingress.callback_gap_reason_counts)
        adapter.stop()
        self.assertFalse(port.snapshot().observer_attached)

    def test_failed_query_is_abandoned_and_unknown_query_kind_is_rejected(self) -> None:
        transport = _Transport(return_code=8)
        adapter, ingress, observer, port = self._started(transport=transport)
        with self.assertRaisesRegex(EmtReadOnlyIngressError, "query kind is not approved"):
            adapter.query(_query("unknown-query", kind="unknown"))
        self.assertEqual(transport.request_ids, [])
        with self.assertRaisesRegex(EmtReadOnlyIngressError, "returned nonzero code 8"):
            adapter.query(_query("unfinished-orders-query"))
        self.assertEqual(transport.request_ids, [200])
        self.assertEqual(port.snapshot().active_query_count, 0)

        self.assertIsNone(
            ingress.on_query_order_terminal(
                native_request_id=200,
                native_session_id=999,
                error_id=0,
                is_last=True,
                received_monotonic_ns=12,
            )
        )
        self.assertEqual(len(observer.events), 0)
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual({"native_session_mismatch": 1}, ingress.callback_gap_reason_counts)

    def test_callback_name_mismatch_is_reported_without_vendor_data(self) -> None:
        adapter, ingress, observer, _port = self._started()
        adapter.query(_query("callback-name-mismatch"))

        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryTrade",
                native_request_id=200,
                native_session_id=71,
                error_id=0,
                is_last=True,
                row_seen=False,
                received_monotonic_ns=12,
            )
        )

        self.assertEqual(len(observer.events), 0)
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual({"callback_name_mismatch": 1}, ingress.callback_gap_reason_counts)

    def test_order_and_trade_page_queries_keep_only_terminal_cursor_scalars(self) -> None:
        transport = _Transport()
        adapter, ingress, observer, _port = self._started(transport=transport)
        order_handle = adapter.query(
            _query("page-orders", kind="orders_by_page", continuation=17, page_size=10)
        )
        self.assertEqual("page-orders", order_handle.request_id)
        self.assertEqual(("orders", 10, 17, 200), transport.page_call)
        event = ingress.on_query_terminal(
            callback_name="onQueryOrderByPage",
            native_request_id=200,
            native_session_id=71,
            error_id=None,
            is_last=True,
            received_monotonic_ns=10,
            page_reference=33,
            page_result_count=10,
        )
        assert event is not None
        self.assertEqual(
            {
                "callback_name": "onQueryOrderByPage",
                "query_kind": "orders_by_page",
                "terminal": "true",
                "error_id": "0",
                "page_reference": "33",
                "page_result_count": "10",
                "page_has_more": "true",
            },
            event.normalized_summary,
        )
        self.assertNotIn("page-orders", event.source_key)

        adapter.query(_query("page-trades", kind="trades_by_page", page_size=7))
        self.assertEqual(("trades", 7, 0, 201), transport.page_call)
        self.assertIsNone(
            ingress.on_query_terminal(
                callback_name="onQueryOrderByPage",
                native_request_id=201,
                native_session_id=71,
                error_id=None,
                is_last=True,
                received_monotonic_ns=11,
                page_reference=0,
                page_result_count=0,
            )
        )
        self.assertEqual(1, len(observer.gaps))
        with self.assertRaisesRegex(EmtReadOnlyIngressError, "controlled maximum"):
            adapter.query(_query("large-page", kind="orders_by_page", page_size=201))
        adapter.stop()

    def test_query_row_presence_is_counted_without_retaining_vendor_rows(self) -> None:
        adapter, ingress, observer, _port = self._started()
        adapter.query(_query("unfinished-count"))
        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryOrder",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10,
            )
        )
        event = ingress.on_query_callback(
            callback_name="onQueryOrder",
            native_request_id=200,
            native_session_id=71,
            error_id=None,
            is_last=True,
            row_seen=True,
            received_monotonic_ns=11,
        )
        assert event is not None
        self.assertEqual("2", event.normalized_summary["result_count"])
        self.assertEqual([event], observer.events)
        adapter.stop()

    def test_asset_fund_position_and_credit_queries_have_explicit_typed_callbacks(self) -> None:
        transport = _Transport()
        adapter, ingress, observer, _port = self._started(transport=transport)
        expected = (
            ("assets", "onQueryAsset"),
            ("funds", "onQueryFund"),
            ("positions", "onQueryPosition"),
            ("credit_fund_info", "onQueryCreditFundInfo"),
            ("credit_asset_debt", "onQueryCreditAssetDebtInfo"),
            ("credit_position_extra_info_sz", "onQueryCreditPositionExtraInfo"),
            ("credit_position_extra_info_sh", "onQueryCreditPositionExtraInfo"),
        )
        for offset, (kind, callback_name) in enumerate(expected):
            adapter.query(_query(f"recovery-{kind}", kind=kind))
            event = ingress.on_query_callback(
                callback_name=callback_name,
                native_request_id=200 + offset,
                native_session_id=71,
                error_id=0,
                is_last=True,
                row_seen=False,
                received_monotonic_ns=10 + offset,
            )
            self.assertIsNotNone(event)
            assert event is not None
            self.assertEqual(kind, event.normalized_summary["query_kind"])
        adapter.query(_query("recovery-positions-page", kind="positions_by_page", page_size=7))
        self.assertEqual(("positions", 7, 0, 207), transport.page_call)
        page_event = ingress.on_query_terminal(
            callback_name="onQueryPositionByPage",
            native_request_id=207,
            native_session_id=71,
            error_id=0,
            is_last=True,
            received_monotonic_ns=21,
            page_reference=0,
            page_result_count=0,
        )
        self.assertIsNotNone(page_event)
        self.assertEqual([], observer.gaps)
        adapter.stop()

    def test_explicit_recovery_collector_accepts_full_orders_page_query_scalars(self) -> None:
        collector = EmtAuthoritativeOrderCollectorV1(scan_id="recovery-orders-a")
        adapter, ingress, observer, _port = self._started(authoritative_order_observer=collector)
        adapter.query(
            _query(
                "recovery-full-orders",
                kind="orders_by_page",
                page_size=200,
                capture_authoritative_order_rows=True,
            )
        )
        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryOrderByPage",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10,
                authoritative_order=EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),
                page_reference=100,
                page_result_count=200,
            )
        )
        event = ingress.on_query_callback(
            callback_name="onQueryOrderByPage",
            native_request_id=200,
            native_session_id=71,
            error_id=0,
            is_last=True,
            row_seen=False,
            received_monotonic_ns=11,
            page_reference=100,
            page_result_count=1,
        )
        self.assertIsNotNone(event)
        scan = collector.seal()
        self.assertTrue(scan.complete)
        self.assertEqual((1000,), tuple(order.order_client_id for order in scan.orders))
        self.assertEqual([], observer.gaps)
        adapter.stop()

    def test_explicit_owned_target_collector_accepts_unfinished_order_scalars(self) -> None:
        """Shared-account F2 preflight may inspect only its exact client reference."""

        collector = EmtAuthoritativeOrderCollectorV1(scan_id="f2-owned-unfinished")
        adapter, ingress, observer, _port = self._started(authoritative_order_observer=collector)
        adapter.query(
            _query(
                "f2-owned-unfinished",
                kind="unfinished_orders",
                capture_authoritative_order_rows=True,
            )
        )
        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryOrder",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10,
                authoritative_order=EmtAuthoritativeOrderV1(9001, 5001, 1, 0, 100),
            )
        )
        event = ingress.on_query_callback(
            callback_name="onQueryOrder",
            native_request_id=200,
            native_session_id=71,
            error_id=0,
            is_last=True,
            row_seen=False,
            received_monotonic_ns=11,
        )
        self.assertIsNotNone(event)
        scan = collector.seal()
        self.assertTrue(scan.complete)
        self.assertEqual((9001,), tuple(order.order_client_id for order in scan.orders))
        self.assertEqual([], observer.gaps)
        adapter.stop()

    def test_non_authoritative_orders_page_does_not_touch_recovery_collector(self) -> None:
        """Certificate pagination must not be mistaken for an active R1 scan."""

        collector = EmtAuthoritativeOrderScanSeriesCollectorV1()
        adapter, ingress, observer, _port = self._started(authoritative_order_observer=collector)
        adapter.query(_query("certificate-orders-page", kind="orders_by_page", page_size=200))
        event = ingress.on_query_callback(
            callback_name="onQueryOrderByPage",
            native_request_id=200,
            native_session_id=71,
            error_id=0,
            is_last=True,
            row_seen=True,
            received_monotonic_ns=10,
            authoritative_order=EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),
            page_reference=0,
            page_result_count=1,
        )
        self.assertIsNotNone(event)
        self.assertEqual([], observer.gaps)
        adapter.stop()

    def test_recovery_collector_missing_required_order_scalars_latches_callback_gap(self) -> None:
        collector = EmtAuthoritativeOrderCollectorV1(scan_id="recovery-orders-missing")
        adapter, ingress, observer, _port = self._started(authoritative_order_observer=collector)
        adapter.query(
            _query("recovery-full-orders", kind="orders", capture_authoritative_order_rows=True)
        )
        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryOrder",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10,
            )
        )
        self.assertEqual(1, len(observer.gaps))
        self.assertFalse(collector.seal().complete)
        adapter.stop()

    def test_recovery_extraction_failure_reports_static_code_without_vendor_row(self) -> None:
        collector = EmtAuthoritativeOrderCollectorV1(scan_id="recovery-orders-invalid")
        failure_codes: list[str] = []
        adapter, ingress, observer, _port = self._started(
            authoritative_order_observer=collector,
            authoritative_row_failure_observer=failure_codes.append,
        )
        adapter.query(
            _query("recovery-full-orders", kind="orders", capture_authoritative_order_rows=True)
        )
        self.assertIsNone(
            ingress.on_query_callback(
                callback_name="onQueryOrder",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10,
                authoritative_order_failure="order_client_id_nonpositive",
            )
        )
        self.assertEqual(["order_client_id_nonpositive"], failure_codes)
        self.assertEqual(1, len(observer.gaps))
        self.assertEqual(
            {"order_client_id_nonpositive": 1}, ingress.authoritative_row_failure_counts
        )
        adapter.stop()

    def test_recovery_duplicate_client_id_rows_remain_available_to_reconciliation(self) -> None:
        collector = EmtAuthoritativeOrderCollectorV1(scan_id="recovery-orders-duplicate-client")
        adapter, ingress, observer, _port = self._started(
            authoritative_order_observer=collector,
        )
        adapter.query(
            _query("recovery-full-orders", kind="orders", capture_authoritative_order_rows=True)
        )
        for order_emt_id in (5001, 5002):
            ingress.on_query_callback(
                callback_name="onQueryOrder",
                native_request_id=200,
                native_session_id=71,
                error_id=None,
                is_last=False,
                row_seen=True,
                received_monotonic_ns=10 + order_emt_id,
                authoritative_order=EmtAuthoritativeOrderV1(1000, order_emt_id, 1, 0, 100),
            )
        event = ingress.on_query_callback(
            callback_name="onQueryOrder",
            native_request_id=200,
            native_session_id=71,
            error_id=0,
            is_last=True,
            row_seen=False,
            received_monotonic_ns=6000,
        )
        self.assertIsNotNone(event)
        scan = collector.seal()
        self.assertEqual(2, len(scan.orders))
        self.assertEqual([], observer.gaps)
        adapter.stop()

    def test_recovery_returns_explicit_incomplete_evidence_without_native_calls(self) -> None:
        adapter, _ingress, _observer, port = self._started()
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="account-1",
            venue_id="EMT",
            adapter_instance_id="emt-sim-instance-1",
            trading_day=DAY,
            journal_id="journal-1",
            expected_journal_lsn=0,
        )
        evidence = adapter.collect_recovery_evidence(adapter.begin_recovery(plan, _observer))
        self.assertEqual(evidence.recovery_run_id, plan.recovery_run_id)
        self.assertFalse(evidence.is_complete)
        self.assertFalse(evidence.callback_replay_complete)
        self.assertFalse(evidence.orders_complete)
        self.assertEqual(port.snapshot().active_query_count, 0)
        with self.assertRaisesRegex(EmtReadOnlyIngressError, "unknown"):
            port.collect_recovery_evidence(
                RecoveryHandle("unknown", "emt-gateway-p0", "emt-sim-instance-1")
            )

    def test_typed_ingress_has_no_sdk_or_legacy_gateway_import(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "xuanling_tradecore"
            / "adapters"
            / "emt"
            / "read_only_ingress.py"
        )
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        forbidden = {"emt_gateway", "vnemttrader"}
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imports.append(node.module)
        self.assertTrue(forbidden.isdisjoint(name.split(".", maxsplit=1)[0] for name in imports))


if __name__ == "__main__":
    unittest.main()
