from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_ROOT = ROOT / "tools" / "performance"
if str(PERFORMANCE_ROOT) not in sys.path:
    sys.path.insert(0, str(PERFORMANCE_ROOT))

from emt_gateway.spi_bridge import (
    OFFICIAL_TRADER_SPI,
    DeferredCallbackIngress,
    build_trader_spi_bridge,
    python_callback_name,
)
from perfbench.catalog import TRADER_SPI


@dataclass(frozen=True)
class CallbackCall:
    name: str
    profile: str
    workload: str
    order_emt_id: int | None
    operation_native_id: int | None


class IngressSpy:
    def __init__(self) -> None:
        self.calls: list[CallbackCall] = []

    def receive(
        self,
        callback_name: str,
        *,
        profile: str,
        workload: str,
        order_emt_id: int | None = None,
        operation_native_id: int | None = None,
        request_id: int | None = None,
        is_last: bool | None = None,
    ) -> str:
        self.calls.append(
            CallbackCall(callback_name, profile, workload, order_emt_id, operation_native_id)
        )
        return "callback-correlation"


class FakeTraderApi:
    def __init__(self) -> None:
        self.base_initialized = True


class SpiBridgeTests(unittest.TestCase):
    def test_registers_all_official_callback_overrides(self) -> None:
        ingress = IngressSpy()
        bridge_type = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="credit"
        )
        bridge = bridge_type()

        self.assertEqual(69, len(OFFICIAL_TRADER_SPI))
        self.assertEqual(TRADER_SPI, OFFICIAL_TRADER_SPI)
        self.assertTrue(
            all(hasattr(bridge, python_callback_name(name)) for name in OFFICIAL_TRADER_SPI)
        )
        self.assertTrue(bridge.base_initialized)

    def test_copies_only_order_and_operation_ids_into_ingress(self) -> None:
        ingress = IngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="credit"
        )()
        bridge.onOrderEvent(
            {"order_emt_id": 101, "order_cancel_emt_id": 202, "sensitive": "never retained"}
        )
        bridge.onConnected()

        self.assertEqual(
            CallbackCall("OnOrderEvent", "credit", "steady", 101, 202),
            ingress.calls[0],
        )
        self.assertEqual(
            CallbackCall("OnConnected", "ordinary", "reconnect", None, None),
            ingress.calls[1],
        )

    def test_uses_credit_quota_serial_id_as_the_opaque_operation_key(self) -> None:
        ingress = IngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="credit"
        )()
        bridge.onCreditQuotaTransfer({"serial_id": 303, "amount": "never retained"})

        self.assertEqual(
            CallbackCall("OnCreditQuotaTransfer", "credit", "steady", None, 303),
            ingress.calls[0],
        )

    def test_normalizes_order_and_trade_facts_without_retaining_native_payloads(self) -> None:
        ingress = LedgerIngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="ordinary"
        )()
        bridge.onOrderEvent(
            {
                "order_emt_id": 101,
                "order_client_id": "strategy-order-101",
                "order_status": 4,
                "qty_traded": 0,
                "qty_left": 100,
                "update_time": 20260827100000000,
                "sensitive": {"must": "not be retained"},
            }
        )
        bridge.onTradeEvent(
            {
                "order_emt_id": 101,
                "order_client_id": "strategy-order-101",
                "market": 2,
                "report_index": 77,
                "exec_id": "vendor-exec-ignored-when-report-index-exists",
                "quantity": 100,
                "price": 1.61,
                "sensitive": ["must", "not", "be", "retained"],
            }
        )

        order_fact, trade_fact = ingress.ledger_facts
        self.assertEqual("ORDER_UPDATE", order_fact.kind.value)
        self.assertEqual("strategy-order-101", order_fact.client_order_id)
        self.assertEqual(100, order_fact.leaves_qty)
        self.assertEqual("TRADE", trade_fact.kind.value)
        self.assertEqual("2", trade_fact.market)
        self.assertEqual("report:77", trade_fact.execution_key)
        self.assertEqual("1.61", trade_fact.price)

    def test_query_callbacks_forward_only_request_and_terminal_metadata(self) -> None:
        ingress = QueryIngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="ordinary"
        )()
        bridge.onQueryAsset({}, {}, 77, True, 11)
        bridge.onQueryFund({}, {}, 78, 11)
        bridge.onQueryOrderByPage({"opaque": "row"}, 1, 2, 9, 79, True, 11)
        bridge.onQueryYesterdayAsset({}, {}, 80, True, 11)

        self.assertEqual(("OnQueryAsset", 77, True), ingress.query_calls[0])
        self.assertEqual(("OnQueryFund", 78, None), ingress.query_calls[1])
        self.assertEqual(("OnQueryOrderByPage", 79, True), ingress.query_calls[2])
        self.assertEqual(("OnQueryYesterdayAsset", 80, True), ingress.query_calls[3])
        self.assertEqual((True, 9), ingress.semantic_calls[2])

    def test_query_callbacks_normalize_only_cpython_zero_one_terminal_flags(self) -> None:
        ingress = QueryIngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="credit"
        )()

        bridge.onQueryCreditPositionFullRate({}, {}, 81, 1, 11)
        bridge.onQueryCreditPositionFullRate({}, {}, 82, 0, 11)
        bridge.onQueryCreditPositionFullRate({}, {}, 83, 2, 11)

        self.assertEqual(
            (
                ("OnQueryCreditPositionFullRate", 81, True),
                ("OnQueryCreditPositionFullRate", 82, False),
                ("OnQueryCreditPositionFullRate", None, None),
            ),
            tuple(ingress.query_calls),
        )

    def test_capability_specific_callbacks_keep_their_matrix_profile_on_public_stream_replay(
        self,
    ) -> None:
        ingress = IngressSpy()
        bridge = build_trader_spi_bridge(
            FakeTraderApi, callback_ingress=ingress, account_profile="ordinary"
        )()
        bridge.onQueryCreditPledgeStkRate()
        bridge.onOptionCombinedOrderEvent()

        self.assertEqual("credit", ingress.calls[0].profile)
        self.assertEqual("option", ingress.calls[1].profile)

    def test_deferred_ingress_drops_pre_session_replay_then_forwards_once_attached(self) -> None:
        router = DeferredCallbackIngress()
        self.assertIsNone(router.receive("OnConnected", profile="ordinary", workload="reconnect"))
        ingress = IngressSpy()
        router.attach(ingress)
        self.assertEqual(
            "callback-correlation",
            router.receive("OnOrderEvent", profile="ordinary", workload="steady"),
        )
        self.assertEqual("OnOrderEvent", ingress.calls[0].name)
        with self.assertRaisesRegex(RuntimeError, "already attached"):
            router.attach(ingress)


class QueryIngressSpy(IngressSpy):
    def __init__(self) -> None:
        super().__init__()
        self.query_calls: list[tuple[str, int | None, bool | None]] = []
        self.semantic_calls: list[tuple[bool | None, int | None]] = []

    def receive(self, callback_name: str, **kwargs: object) -> str:
        self.query_calls.append(
            (
                callback_name,
                kwargs.get("request_id") if isinstance(kwargs.get("request_id"), int) else None,
                kwargs.get("is_last") if type(kwargs.get("is_last")) is bool else None,
            )
        )
        self.semantic_calls.append(
            (
                kwargs.get("query_item_present")
                if type(kwargs.get("query_item_present")) is bool
                else None,
                kwargs.get("query_reference")
                if isinstance(kwargs.get("query_reference"), int)
                else None,
            )
        )
        return "callback-correlation"


class LedgerIngressSpy:
    def __init__(self) -> None:
        self.ledger_facts: list[object] = []

    def receive(self, callback_name: str, **kwargs: object) -> str:
        fact = kwargs.get("ledger_fact")
        if fact is not None:
            self.ledger_facts.append(fact)
        return "callback-correlation"
