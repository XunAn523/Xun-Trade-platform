"""P0 contract tests for the deterministic, no-I/O broker adapter fixture."""

from __future__ import annotations

import unittest

from emt_gateway.native_api import NativeOrderPort
from tests.contracts.broker_adapter import (
    BrokerAdapterContract,
    BrokerCallbackKind,
    BrokerCallbackSourceContract,
    FakeNativePort,
    NativeSendResult,
)


def _require_native_order_port(port: NativeOrderPort) -> NativeOrderPort:
    """Static seam check for the non-runtime-checkable production protocol."""

    return port


class FakeNativePortContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.port = FakeNativePort(
            order_results=(
                NativeSendResult(native_id=101),
                NativeSendResult(native_id=0, error_code=211008),
            ),
            cancel_results=(
                NativeSendResult(native_id=201),
                NativeSendResult(native_id=0, error_code=211009),
            ),
        )

    def test_implements_current_command_and_callback_contracts(self) -> None:
        self.assertIs(self.port, _require_native_order_port(self.port))
        self.assertIsInstance(self.port, BrokerAdapterContract)
        self.assertIsInstance(self.port, BrokerCallbackSourceContract)

    def test_order_acceptance_and_rejection_follow_native_result_semantics(self) -> None:
        self.assertEqual(
            101, self.port.insert_order(command_id="command-1", client_order_id="client-1")
        )
        self.assertEqual(0, self.port.get_last_error_code())
        self.assertEqual(
            0, self.port.insert_order(command_id="command-2", client_order_id="client-2")
        )
        self.assertEqual(211008, self.port.get_last_error_code())
        self.assertEqual(
            [("command-1", "client-1"), ("command-2", "client-2")],
            self.port.order_calls,
        )

    def test_cancel_acceptance_and_rejection_follow_native_result_semantics(self) -> None:
        self.assertEqual(201, self.port.cancel_order(command_id="cancel-1", order_emt_id=101))
        self.assertEqual(0, self.port.get_last_error_code())
        self.assertEqual(0, self.port.cancel_order(command_id="cancel-2", order_emt_id=101))
        self.assertEqual(211009, self.port.get_last_error_code())
        self.assertEqual([("cancel-1", 101), ("cancel-2", 101)], self.port.cancel_calls)

    def test_order_trade_and_cancel_error_callbacks_are_payload_free_facts(self) -> None:
        self.port.emit_order_report(order_emt_id=101, status="LIVE")
        self.port.emit_trade(order_emt_id=101, market="SZ", execution_id="execution-1")
        self.port.emit_cancel_error(order_emt_id=101, cancel_emt_id=201, error_code=110003)

        callbacks = self.port.drain_callbacks()
        self.assertEqual(
            (BrokerCallbackKind.ORDER, BrokerCallbackKind.TRADE, BrokerCallbackKind.CANCEL_ERROR),
            tuple(callback.kind for callback in callbacks),
        )
        self.assertEqual(
            ("OnOrderEvent", "OnTradeEvent", "OnCancelOrderError"),
            tuple(callback.callback_name for callback in callbacks),
        )
        self.assertTrue(all(not hasattr(callback, "payload") for callback in callbacks))
        self.assertEqual("SZ:execution-1", callbacks[1].execution_key)
        self.assertEqual(110003, callbacks[2].error_code)

    def test_query_fixture_emits_paginated_callbacks_with_a_terminal_page(self) -> None:
        self.port.configure_query_pages(
            query_name="QueryOrdersByPage",
            pages=(({"order_emt_id": 101}, {"order_emt_id": 102}), ({"order_emt_id": 103},)),
        )

        self.assertEqual(0, self.port.request_query(request_id=79, query_name="QueryOrdersByPage"))
        callbacks = self.port.drain_callbacks()

        self.assertEqual(
            (BrokerCallbackKind.QUERY_PAGE, BrokerCallbackKind.QUERY_PAGE),
            tuple(callback.kind for callback in callbacks),
        )
        self.assertEqual((1, 2), tuple(callback.page_number for callback in callbacks))
        self.assertEqual((2, 1), tuple(callback.row_count for callback in callbacks))
        self.assertEqual((False, True), tuple(callback.is_last for callback in callbacks))
        self.assertEqual(
            ("query:QueryOrdersByPage:79:1", "query:QueryOrdersByPage:79:2"),
            tuple(callback.source_key for callback in callbacks),
        )

    def test_replay_keeps_callback_identity_and_never_repeats_a_native_side_effect(self) -> None:
        self.port.insert_order(command_id="command-1", client_order_id="client-1")
        self.port.emit_order_report(order_emt_id=101, status="LIVE")
        original = self.port.drain_callbacks()

        replayed = self.port.replay_callbacks()
        delivered_replay = self.port.drain_callbacks()

        self.assertEqual(
            tuple(callback.source_key for callback in original),
            tuple(callback.source_key for callback in replayed),
        )
        self.assertEqual(replayed, delivered_replay)
        self.assertTrue(all(callback.replayed for callback in replayed))
        self.assertEqual([("command-1", "client-1")], self.port.order_calls)
        self.assertEqual([], self.port.cancel_calls)


if __name__ == "__main__":
    unittest.main()
