from __future__ import annotations

import unittest

from emt_gateway.native_api import EmtOrderAdapter, NativeAdapterError


class FakeBinding:
    def __init__(self) -> None:
        self.insert_calls: list[tuple[object, int]] = []
        self.cancel_calls: list[tuple[int, int]] = []

    def insertOrder(self, request: object, session_id: int) -> int:
        self.insert_calls.append((request, session_id))
        return 71

    def cancelOrder(self, order_emt_id: int, session_id: int) -> int:
        self.cancel_calls.append((order_emt_id, session_id))
        return 72

    def getApiLastError(self) -> dict[str, int]:
        return {"error_id": 211008}


class NativeAdapterTests(unittest.TestCase):
    def test_forwards_only_the_required_emt_calls(self) -> None:
        binding = FakeBinding()
        adapter = EmtOrderAdapter(
            binding,
            session_id=12,
            order_request_factory=lambda command_id, client_order_id: {
                "command": command_id,
                "client": client_order_id,
            },
        )
        self.assertEqual(
            71, adapter.insert_order(command_id="command-1", client_order_id="client-1")
        )
        self.assertEqual(72, adapter.cancel_order(command_id="command-2", order_emt_id=71))
        self.assertEqual(211008, adapter.get_last_error_code())
        self.assertEqual(12, binding.insert_calls[0][1])
        self.assertEqual([(71, 12)], binding.cancel_calls)

    def test_requires_an_active_session(self) -> None:
        with self.assertRaises(NativeAdapterError):
            EmtOrderAdapter(FakeBinding(), session_id=0, order_request_factory=lambda *_: {})
