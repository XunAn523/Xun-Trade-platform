"""Contract tests for the no-domain-object fastspi v2 Python binding bridge."""

from __future__ import annotations

import unittest

from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI, python_callback_name
from emt_gateway.spi_bridge_v2 import (
    FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT,
    FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT,
    FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT,
    FASTSPI_V2_FLAG_QUERY,
    FASTSPI_V2_FLAG_QUERY_LAST,
    FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT,
    FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT,
    FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT,
    FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT,
    build_trader_spi_v2_bridge,
    register_official_trader_spi_v2_kinds,
)


class _TraderApiBase:
    pass


class _Ingress:
    def __init__(self) -> None:
        self.offers: list[dict[str, object]] = []
        self.gaps: list[tuple[int, int]] = []
        self.raise_on_offer = False

    def offer(self, **kwargs: object) -> bool:
        if self.raise_on_offer:
            raise RuntimeError("native ingress rejected malformed scalar")
        self.offers.append(kwargs)
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        self.gaps.append((callback_kind, observed_monotonic_ns))


class _KindRegistrar:
    def __init__(self) -> None:
        self.entries: list[tuple[int, str]] = []

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None:
        self.entries.append((callback_kind, label))


class TraderSpiV2BridgeTests(unittest.TestCase):
    def test_official_callback_registration_matches_bridge_codes(self) -> None:
        registrar = _KindRegistrar()
        register_official_trader_spi_v2_kinds(registrar)
        self.assertEqual(
            tuple(enumerate(OFFICIAL_TRADER_SPI, start=1)),
            tuple(registrar.entries),
        )

    def test_each_official_callback_uses_a_stable_scalar_kind_code(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        for callback_kind, official_name in enumerate(OFFICIAL_TRADER_SPI, start=1):
            getattr(bridge, python_callback_name(official_name))()
            offer = ingress.offers[-1]
            self.assertEqual(callback_kind, offer["callback_kind"])
            self.assertEqual(0, offer["order_emt_id"])
            self.assertEqual(b"", offer["error_bytes"])

    def test_borrowed_mapping_is_reduced_to_fixed_scalars(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onOrderEvent(
            {
                "client_id": 2,
                "order_client_id": 3,
                "order_emt_id": 4,
                "order_cancel_emt_id": 5,
                "order_status": 6,
                "quantity": 7,
                "report_index": 8,
                "update_time": 20260827100000000,
                "qty_traded": 1,
                "qty_left": 6,
                "market": 2,
                "price": 1.61,
                "error_msg": b"bounded",
                "unbounded_payload": "must not be queued",
            }
        )
        offer = ingress.offers[0]
        self.assertEqual(2, offer["client_id"])
        self.assertEqual(3, offer["order_client_id"])
        self.assertEqual(4, offer["order_emt_id"])
        self.assertEqual(5, offer["order_cancel_emt_id"])
        self.assertEqual(6, offer["status"])
        self.assertEqual(7, offer["quantity"])
        self.assertEqual(8, offer["trade_id_hash"])
        self.assertEqual(b"bounded", offer["error_bytes"])
        self.assertEqual(20260827100000000, offer["order_update_time"])
        self.assertEqual(1, offer["qty_traded"])
        self.assertEqual(6, offer["qty_left"])
        self.assertEqual(2, offer["trade_market"])
        self.assertEqual(1.61, offer["trade_price"])
        self.assertEqual(8, offer["trade_report_index"])
        flags = offer["flags"]
        self.assertIsInstance(flags, int)
        assert isinstance(flags, int)
        self.assertEqual(
            FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT
            | FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT
            | FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT
            | FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT
            | FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT
            | FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT
            | (1 << 3),
            flags,
        )
        self.assertNotIn("unbounded_payload", offer)

    def test_query_terminal_metadata_is_encoded_as_flags(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onQueryOrder({"order_emt_id": 4}, 99, True)
        flags = ingress.offers[0]["flags"]
        self.assertIsInstance(flags, int)
        assert isinstance(flags, int)
        self.assertEqual(0b111, flags)

    def test_paged_query_copies_cursor_reference_without_payload_retention(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onQueryOrderByPage({}, {}, 17, 99, True)
        offer = ingress.offers[0]
        self.assertEqual(99, offer["client_id"])
        self.assertEqual(17, offer["query_reference"])
        self.assertEqual(
            FASTSPI_V2_FLAG_QUERY
            | FASTSPI_V2_FLAG_QUERY_LAST
            | FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT,
            offer["flags"],
        )

    def test_error_summary_preserves_empty_and_unicode_until_native_boundary(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onOrderEvent({}, {"error_msg": ""}, 9)
        empty = ingress.offers[-1]
        self.assertEqual("", empty["error_bytes"])
        self.assertEqual(1 << 3, empty["flags"])

        long_unicode_error = "错误" * 100
        bridge.onOrderEvent({}, {"error_message": long_unicode_error}, 9)
        unicode_offer = ingress.offers[-1]
        self.assertEqual(long_unicode_error, unicode_offer["error_bytes"])
        self.assertEqual(1 << 3, unicode_offer["flags"])

    def test_separate_error_mapping_supplies_status_and_summary(self) -> None:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onCancelOrderError(
            {"order_emt_id": 44, "order_cancel_emt_id": 55},
            {"error_id": 7, "error_msg": "撤单被拒绝"},
            9,
        )
        offer = ingress.offers[-1]
        self.assertEqual(7, offer["status"])
        self.assertEqual("撤单被拒绝", offer["error_bytes"])

    def test_unexpected_native_failure_is_a_callback_gap_not_an_exception(self) -> None:
        ingress = _Ingress()
        ingress.raise_on_offer = True
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        bridge.onTradeEvent({"order_emt_id": 4})
        self.assertEqual((), tuple(ingress.offers))
        self.assertEqual(1, len(ingress.gaps))
        self.assertEqual(5, ingress.gaps[0][0])


if __name__ == "__main__":
    unittest.main()
