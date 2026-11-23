"""Tests for the outer platform factory serving the controlled EMT execution Adapter."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from emt_gateway.session import SessionSettings
from emt_gateway.tradecore_execution import (
    EmtPlatformExecutionSessionFactory,
    EmtTradeCoreExecutionSessionError,
    _authoritative_query_order,
    _normalize_query_last,
    _query_row_seen,
    _query_terminal_shape,
    _terminal_query_metadata,
)
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerEventIngress,
    EmtQueryCallbackIngress,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import TradingDayEpoch

DAY = TradingDayEpoch("2026-09-04")


class _Observer:
    def __init__(self) -> None:
        self.events = []
        self.gaps = []

    def on_broker_event(self, event) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence) -> None:
        self.gaps.append(evidence)


class _TraderBase:
    instances = []

    def __init__(self) -> None:
        type(self).instances.append(self)
        self.calls = []

    def createTraderApi(self, *args) -> None:
        self.calls.append(("create", args))

    def setSoftwareVersion(self, value) -> None:
        self.calls.append(("version", value))

    def setHeartBeatInterval(self, value) -> None:
        self.calls.append(("heartbeat", value))

    def subscribePublicTopic(self, resume_type: int) -> None:
        self.calls.append(("subscribe", resume_type))

    def setCpuAffinity(self, receive_cpu: int, process_cpu: int) -> None:
        self.calls.append(("cpu_affinity", receive_cpu, process_cpu))

    def login(self, *args) -> int:
        self.calls.append(("login", args))
        return 71

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "20260904"

    def queryUnfinishedOrders(self, session_id: int, request_id: int) -> int:
        self.calls.append(("query_unfinished_orders", session_id, request_id))
        return 0

    def queryOrders(self, query: dict[str, int | str], session_id: int, request_id: int) -> int:
        self.calls.append(("query_orders", query, session_id, request_id))
        return 0

    def queryTrades(self, query: dict[str, int | str], session_id: int, request_id: int) -> int:
        self.calls.append(("query_trades", query, session_id, request_id))
        return 0

    def queryOrdersByPage(self, query: dict[str, int], session_id: int, request_id: int) -> int:
        self.calls.append(("query_orders_by_page", query, session_id, request_id))
        return 0

    def queryTradesByPage(self, query: dict[str, int], session_id: int, request_id: int) -> int:
        self.calls.append(("query_trades_by_page", query, session_id, request_id))
        return 0

    def queryPosition(self, ticker: str, session_id: int, request_id: int) -> int:
        self.calls.append(("query_positions", ticker, session_id, request_id))
        return 0

    def queryPositionByPage(self, query: dict[str, int], session_id: int, request_id: int) -> int:
        self.calls.append(("query_positions_by_page", query, session_id, request_id))
        return 0

    def queryAsset(self, session_id: int, request_id: int) -> int:
        self.calls.append(("query_assets", session_id, request_id))
        return 0

    def queryFund(self, session_id: int, request_id: int) -> int:
        self.calls.append(("query_funds", session_id, request_id))
        return 0

    def queryCreditFundInfo(self, session_id: int, request_id: int) -> int:
        self.calls.append(("query_credit_fund_info", session_id, request_id))
        return 0

    def queryCreditAssetDebtInfo(self, session_id: int, request_id: int) -> int:
        self.calls.append(("query_credit_asset_debt", session_id, request_id))
        return 0

    def queryCreditPositionExtraInfo(
        self, query: dict[str, int | str], session_id: int, request_id: int
    ) -> int:
        self.calls.append(("query_credit_position_extra_info", query, session_id, request_id))
        return 0

    def logout(self, session_id: int) -> None:
        self.calls.append(("logout", session_id))

    def release(self) -> None:
        self.calls.append(("release",))


class _InvalidMetadataTrader(_TraderBase):
    def getTradingDay(self) -> str:
        return "invalid-trading-day"


class _Provider:
    def __init__(self, trader_api_type=_TraderBase) -> None:
        self._trader_api_type = trader_api_type

    def load(self):
        return SimpleNamespace(trader_api_type=self._trader_api_type)


class _Journal:
    def record_submit_started(self, request) -> None:
        del request

    def record_submit_accepted(self, request, receipt) -> None:
        del request, receipt

    def record_cancel_started(self, request) -> None:
        del request

    def record_cancel_accepted(self, request, receipt) -> None:
        del request, receipt


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


class TradeCoreExecutionSessionTests(unittest.TestCase):
    def test_authoritative_order_extraction_reports_only_static_failure_codes(self) -> None:
        valid, valid_failure = _authoritative_query_order(
            (
                {
                    "order_client_id": 101,
                    "order_emt_id": 501,
                    "order_status": 1,
                    "qty_traded": 0,
                    "qty_left": 100,
                },
                {},
                11,
                True,
                71,
            )
        )
        self.assertIsNotNone(valid)
        self.assertIsNone(valid_failure)
        zero_client_id, zero_client_id_failure = _authoritative_query_order(
            (
                {
                    "order_client_id": 0,
                    "order_emt_id": 501,
                    "order_status": 1,
                    "qty_traded": 0,
                    "qty_left": 100,
                },
                {},
                11,
                True,
                71,
            )
        )
        self.assertIsNotNone(zero_client_id)
        self.assertIsNone(zero_client_id_failure)
        malformed, malformed_failure = _authoritative_query_order(
            (
                {
                    "order_client_id": -1,
                    "order_emt_id": 501,
                    "order_status": 1,
                    "qty_traded": 0,
                    "qty_left": 100,
                },
                {},
                11,
                True,
                71,
            )
        )
        self.assertIsNone(malformed)
        self.assertEqual("order_client_id_negative", malformed_failure)

    def test_query_terminal_flag_accepts_only_boolean_or_binding_zero_one(self) -> None:
        self.assertTrue(_normalize_query_last(True))
        self.assertFalse(_normalize_query_last(False))
        self.assertTrue(_normalize_query_last(1))
        self.assertFalse(_normalize_query_last(0))
        self.assertIsNone(_normalize_query_last(2))
        self.assertIsNone(_normalize_query_last("true"))
        self.assertEqual("last=bool-1", _query_terminal_shape(({}, {}, 1, True, 9)))
        self.assertEqual("last=int-0", _query_terminal_shape(({}, {}, 1, 0, 9)))
        self.assertEqual("last=int-other", _query_terminal_shape(({}, {}, 1, 2, 9)))

    def test_empty_mapping_terminal_is_not_counted_as_a_query_row(self) -> None:
        self.assertFalse(_query_row_seen(None))
        self.assertFalse(_query_row_seen({}))
        self.assertTrue(_query_row_seen({"opaque": object()}))
        self.assertTrue(_query_row_seen(object()))
        self.assertEqual(
            (11, 71, 0, True, False),
            _terminal_query_metadata(({}, {"error_id": 0}, 11, True, 71)),
        )

    def test_factory_opens_after_both_observers_attach_and_releases_once(self) -> None:
        mapping = _mapping()
        observer = _Observer()
        query_ingress = EmtQueryCallbackIngress(
            mapping, trading_day=DAY, native_session_id=None, session_epoch=1
        )
        event_ingress = EmtBrokerEventIngress(mapping, trading_day=DAY)
        query_ingress.attach_observer(observer)
        event_ingress.attach_observer(observer)
        with tempfile.TemporaryDirectory() as directory:
            lifecycle_markers: list[str] = []
            native_callback_entries: list[str] = []
            factory = EmtPlatformExecutionSessionFactory(
                sdk_provider=_Provider(),
                settings=SessionSettings(
                    host="sim-host",
                    port=12345,
                    account="sim-account",
                    password="sim-password",
                    client_id=7,
                    sdk_log_dir=Path(directory) / "logs",
                ),
                order_request_factory=lambda request: {"order_client_id": request.order_client_id},
                native_call_journal=_Journal(),
                cancel_request_id_for_order=lambda order_emt_id: None,
                lifecycle_observer=lifecycle_markers.append,
                native_callback_observer=native_callback_entries.append,
            )
            session = factory.open(
                query_callback_ingress=query_ingress,
                broker_event_ingress=event_ingress,
            )
            self.assertEqual(session.native_session_id, 71)
            self.assertEqual(session.api_version, "2.27.0.0")
            self.assertEqual(session.trading_day, "2026-09-04")
            self.assertTrue(factory.snapshot().session_open)
            self.assertEqual(0, session.query_unfinished_orders(11))
            self.assertEqual(0, session.query_orders(12))
            self.assertEqual(0, session.query_trades(13))
            self.assertEqual(0, session.query_orders_by_page(10, 0, 14))
            self.assertEqual(0, session.query_trades_by_page(10, 7, 15))
            self.assertEqual(0, session.query_positions(16))
            self.assertEqual(0, session.query_positions_by_page(10, 0, 17))
            self.assertEqual(0, session.query_assets(18))
            self.assertEqual(0, session.query_funds(19))
            self.assertEqual(0, session.query_credit_fund_info(20))
            self.assertEqual(0, session.query_credit_asset_debt(21))
            self.assertEqual(0, session.query_credit_position_extra_info(1, 22))
            _TraderBase.instances[-1].onOrderEvent({})
            self.assertEqual(["onOrderEvent"], native_callback_entries)
            session.close()
            self.assertFalse(factory.snapshot().session_open)
            self.assertIsNone(factory._settings)
            self.assertIsNone(session._settings)
            with self.assertRaisesRegex(EmtTradeCoreExecutionSessionError, "consumed login material"):
                factory.open(
                    query_callback_ingress=query_ingress,
                    broker_event_ingress=event_ingress,
                )
        calls = _TraderBase.instances[-1].calls
        self.assertIn(("subscribe", 2), calls)
        self.assertIn(("query_unfinished_orders", 71, 11), calls)
        self.assertIn(
            ("query_orders_by_page", {"req_count": 10, "reference": 0, "reserved": 0}, 71, 14),
            calls,
        )
        self.assertIn(
            ("query_trades_by_page", {"req_count": 10, "reference": 7, "reserved": 0}, 71, 15),
            calls,
        )
        self.assertIn(("query_positions", "", 71, 16), calls)
        self.assertIn(
            ("query_positions_by_page", {"req_count": 10, "reference": 0, "reserved": 0}, 71, 17),
            calls,
        )
        self.assertIn(("query_assets", 71, 18), calls)
        self.assertIn(("query_funds", 71, 19), calls)
        self.assertIn(
            ("query_credit_position_extra_info", {"market": 1, "ticker": "000000"}, 71, 22),
            calls,
        )
        self.assertIn(("logout", 71), calls)
        self.assertIn(("release",), calls)
        self.assertEqual(
            [
                "native_logout_starting",
                "native_logout_returned",
                "native_release_starting",
                "native_release_returned",
            ],
            lifecycle_markers[-4:],
        )

    def test_execution_session_applies_requested_sdk_cpu_affinity_after_login(self) -> None:
        mapping = _mapping()
        observer = _Observer()
        query_ingress = EmtQueryCallbackIngress(
            mapping, trading_day=DAY, native_session_id=None, session_epoch=1
        )
        event_ingress = EmtBrokerEventIngress(mapping, trading_day=DAY)
        query_ingress.attach_observer(observer)
        event_ingress.attach_observer(observer)
        _TraderBase.instances = []
        with tempfile.TemporaryDirectory() as directory:
            lifecycle_markers: list[str] = []
            factory = EmtPlatformExecutionSessionFactory(
                sdk_provider=_Provider(),
                settings=SessionSettings(
                    host="sim-host",
                    port=12345,
                    account="sim-account",
                    password="sim-password",
                    client_id=7,
                    sdk_log_dir=Path(directory) / "logs",
                    sdk_receive_cpu=17,
                    sdk_process_cpu=18,
                ),
                order_request_factory=lambda request: {"order_client_id": request.order_client_id},
                native_call_journal=_Journal(),
                cancel_request_id_for_order=lambda order_emt_id: None,
                lifecycle_observer=lifecycle_markers.append,
            )
            session = factory.open(
                query_callback_ingress=query_ingress,
                broker_event_ingress=event_ingress,
            )
            session.close()
        calls = _TraderBase.instances[0].calls
        self.assertEqual(
            [
                "create",
                "version",
                "heartbeat",
                "subscribe",
                "login",
                "cpu_affinity",
                "logout",
                "release",
            ],
            [call[0] for call in calls],
        )
        self.assertEqual(("cpu_affinity", 17, 18), calls[5])
        self.assertIn("native_cpu_affinity_set", lifecycle_markers)

    def test_isolated_factory_retains_failed_native_session_until_process_exit(self) -> None:
        mapping = _mapping()
        observer = _Observer()
        query_ingress = EmtQueryCallbackIngress(
            mapping, trading_day=DAY, native_session_id=None, session_epoch=1
        )
        event_ingress = EmtBrokerEventIngress(mapping, trading_day=DAY)
        query_ingress.attach_observer(observer)
        event_ingress.attach_observer(observer)
        _InvalidMetadataTrader.instances = []
        with tempfile.TemporaryDirectory() as directory:
            lifecycle_markers: list[str] = []
            retained: list[object] = []
            factory = EmtPlatformExecutionSessionFactory(
                sdk_provider=_Provider(_InvalidMetadataTrader),
                settings=SessionSettings(
                    host="sim-host",
                    port=12345,
                    account="sim-account",
                    password="sim-password",
                    client_id=7,
                    sdk_log_dir=Path(directory) / "logs",
                ),
                order_request_factory=lambda request: {"order_client_id": request.order_client_id},
                native_call_journal=_Journal(),
                cancel_request_id_for_order=lambda order_emt_id: None,
                lifecycle_observer=lifecycle_markers.append,
                native_reference_retainer=retained.append,
            )
            with self.assertRaises(EmtTradeCoreExecutionSessionError):
                factory.open(
                    query_callback_ingress=query_ingress,
                    broker_event_ingress=event_ingress,
                )

        self.assertFalse(factory.snapshot().opening)
        self.assertFalse(factory.snapshot().session_open)
        self.assertEqual(1, len(retained))
        self.assertIn("native_failed_session_retain_starting", lifecycle_markers)
        self.assertIn("native_failed_session_retained", lifecycle_markers)
        calls = _InvalidMetadataTrader.instances[-1].calls
        self.assertIn(("logout", 71), calls)
        self.assertIn(("release",), calls)


if __name__ == "__main__":
    unittest.main()
