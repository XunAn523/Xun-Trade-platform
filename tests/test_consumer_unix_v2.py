from __future__ import annotations

import json
import os
import socket
import struct
import unittest
from pathlib import Path
from threading import Barrier, Thread
from typing import Any

from jsonschema import Draft202012Validator
from xuanling_tradecore.consumer import (
    BoundedConsumerEventSubscriptionV2,
    ConsumerLiveSubscriptionV2,
    ConsumerSubscriptionLimitsV2,
    ConsumerUnixSocketError,
    ConsumerUnixSocketListenerV2,
    ConsumerWireStreamDecoderV2,
    MessageKind,
    PositionedConsumerEventV2,
    encode_frame,
)
from xuanling_tradecore.consumer.request_trace_v2 import request_trace_scope
from xuanling_tradecore.consumer.unix_v2 import (
    _matches_snapshot_request_schema,
    _valid_paged_snapshot_request,
    _valid_poll_request,
    _valid_snapshot_request,
    _valid_submit_request,
)
from xuanling_tradecore.consumer.wire_v2 import decode_frame
from xuanling_tradecore.domain import (
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    RedisStreamIdV1,
    TradingDayEpoch,
)


class ConsumerUnixV2Tests(unittest.TestCase):
    def test_oversize_response_becomes_correlated_error_frame(self) -> None:
        listener = ConsumerUnixSocketListenerV2(
            socket_path=Path.cwd() / "consumer-test.sock",
            gateway=object(),
            principal_for_peer=lambda _: object(),
            request_schema={"type": "object"},
            request_handler=lambda _session, _request: {
                "message_type": "response",
                "schema_version": 2,
                "request_id": "large-response",
                "status": "COMPLETED",
                "payload": {"value": "x" * 1_048_576},
            },
        )
        request = encode_frame(
            MessageKind.REQUEST,
            42,
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": "ListAccounts",
                "request_id": "large-response",
                "payload": {},
            },
        )
        trace: dict[str, object] = {}
        with request_trace_scope(trace):
            response = decode_frame(listener.response_for_frame(object(), decode_frame(request)))
        self.assertEqual(MessageKind.ERROR, response.header.message_kind)
        self.assertEqual(42, response.header.correlation_id)
        self.assertEqual("large-response", response.payload["request_id"])
        self.assertEqual("FRAME_TOO_LARGE", response.payload["code"])
        self.assertEqual("response_encode", trace["failure_stage"])
        self.assertEqual("FRAME_TOO_LARGE", trace["failure_code"])

    def test_poll_fast_validation_is_safe_under_pinned_schema(self) -> None:
        schema = json.loads(
            (
                Path(__file__).parents[1] / "contracts/consumer/v2/schemas/request.schema.json"
            ).read_text()
        )
        validator = Draft202012Validator(schema)
        base = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "PollEvents",
            "request_id": "r",
            "payload": {"cursor": "signed", "limit": 100},
        }
        self.assertTrue(_valid_poll_request(base))
        for cursor in ("", "x", "x" * 4096, "x" * 4097, None, 42, [], {}):
            for limit in (0, 1, 100, 10000, 10001, True, False, "1", None, 1.0, 1.5):
                request = {**base, "payload": {"cursor": cursor, "limit": limit}}
                with self.subTest(cursor_type=type(cursor), limit=limit):
                    if _valid_poll_request(request):
                        self.assertTrue(validator.is_valid(request))
        for candidate in (
            {**base, "extra": 1},
            {**base, "schema_version": True},
            {**base, "schema_version": 2.0},
            {**base, "request_id": ""},
            {**base, "request_id": "x" * 129},
            {**base, "message_type": "response"},
            {**base, "operation": "Other"},
            {**base, "payload": []},
            {**base, "payload": {"cursor": "x", "limit": 1, "extra": 1}},
        ):
            self.assertFalse(_valid_poll_request(candidate))

    def test_submit_fast_validation_is_a_subset_of_pinned_json_schema(self) -> None:
        schema_path = (
            Path(__file__).parents[1]
            / "contracts"
            / "consumer"
            / "v2"
            / "schemas"
            / "request.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema)
        self.assertTrue(_matches_snapshot_request_schema(schema))
        self.assertFalse(_matches_snapshot_request_schema({**schema, "maxProperties": 3}))
        body = {
            "consumer_account_id": "account",
            "strategy_id": "strategy",
            "strategy_order_id": "order",
            "trading_day": "2026-09-29",
            "symbol": "TEST",
            "market": "SIM",
            "side": "BUY",
            "price_type": "LIMIT",
            "business_type": "CASH",
            "quantity": 1,
            "protection_price_ticks": 0,
        }
        base = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "SubmitSignal",
            "request_id": "r",
            "payload": body,
        }
        self.assertTrue(_valid_submit_request(base))
        candidates = [
            base,
            {**base, "payload": {**body, "limit_price_ticks": 1}},
            {**base, "payload": {**body, "limit_price_ticks": None}},
            {**base, "payload": {**body, "execution_parameters": {"route": "direct"}}},
        ]
        for field, values in {
            "message_type": ("other", None),
            "schema_version": (True, 1, "2", 2.0),
            "operation": ("GetAccountOrderSnapshot", None),
            "request_id": ("", "x" * 129, None),
            "payload": (
                {},
                [],
                {**body, "extra": "x"},
                {**body, "limit_price_ticks": -1},
                {**body, "limit_price_ticks": True},
                {**body, "execution_parameters": {"": "x"}},
                {**body, "execution_parameters": {"x": "x" * 513}},
                {**body, "execution_parameters": {str(i): "x" for i in range(33)}},
            ),
        }.items():
            candidates.extend({**base, field: value} for value in values)
        for field, values in {
            "consumer_account_id": ("", "x" * 129, None),
            "strategy_id": ("", "x" * 129, 1),
            "strategy_order_id": ("", "x" * 129, None),
            "trading_day": ("2026-9-29", "٢٠٢٦-٠٩-٢٩", None),
            "symbol": ("", "x" * 65, None),
            "market": ("", "x" * 65, None),
            "side": ("", "x" * 65, None),
            "price_type": ("", "x" * 65, None),
            "business_type": ("", "x" * 65, None),
            "quantity": (0, True, 1.0, None),
            "protection_price_ticks": (-1, True, 0.0, None),
        }.items():
            candidates.extend({**base, "payload": {**body, field: value}} for value in values)
        candidates.extend(
            ({**base, "other": "x"}, {k: v for k, v in base.items() if k != "payload"})
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                if _valid_submit_request(candidate):
                    self.assertTrue(validator.is_valid(candidate))

    def test_paged_snapshot_fast_validation_is_a_subset_of_pinned_schema(self) -> None:
        schema_path = (
            Path(__file__).parents[1]
            / "contracts"
            / "consumer"
            / "v2"
            / "schemas"
            / "request.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema)
        candidates: list[dict[str, Any]] = []
        for operation, body in (
            ("ReadOrderSnapshotPage", {"page_cursor": "cts2_token"}),
            ("CreateOrderSnapshot", {"consumer_account_id": "account", "page_size": 100}),
        ):
            base: dict[str, Any] = {
                "message_type": "request",
                "schema_version": 2,
                "operation": operation,
                "request_id": "request-1",
                "payload": body,
            }
            candidates.extend(
                (
                    base,
                    {**base, "schema_version": True},
                    {**base, "request_id": ""},
                    {**base, "request_id": "x" * 129},
                    {**base, "payload": {**body, "extra": 1}},
                )
            )
        candidates.extend(
            (
                {
                    "message_type": "request",
                    "schema_version": 2,
                    "operation": "ReadOrderSnapshotPage",
                    "request_id": "request-1",
                    "payload": {"page_cursor": ""},
                },
                {
                    "message_type": "request",
                    "schema_version": 2,
                    "operation": "CreateOrderSnapshot",
                    "request_id": "request-1",
                    "payload": {"consumer_account_id": "account", "page_size": 201},
                },
            )
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                if _valid_paged_snapshot_request(candidate):
                    self.assertTrue(validator.is_valid(candidate))

    def test_snapshot_fast_validation_is_a_subset_of_pinned_json_schema(self) -> None:
        schema_path = (
            Path(__file__).parents[1]
            / "contracts"
            / "consumer"
            / "v2"
            / "schemas"
            / "request.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        validator = Draft202012Validator(schema)
        self.assertTrue(_matches_snapshot_request_schema(schema))
        self.assertFalse(_matches_snapshot_request_schema({**schema, "maxProperties": 3}))
        base = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "GetAccountOrderSnapshot",
            "request_id": "r",
            "payload": {"consumer_account_id": "account"},
        }
        candidates = [base, {**base, "payload": {**base["payload"], "reset_context": "r"}}]
        for field, values in {
            "message_type": ("other", None),
            "schema_version": (True, 1, "2", 2.0),
            "operation": ("SubmitSignal", None),
            "request_id": ("", "x" * 129, 4, None, "雪" * 128),
            "payload": (
                {},
                {"consumer_account_id": ""},
                {"consumer_account_id": "x" * 129},
                {"consumer_account_id": 4},
                {"consumer_account_id": "a", "reset_context": ""},
                {"consumer_account_id": "a", "reset_context": "x" * 129},
                {"consumer_account_id": "a", "reset_context": None},
                {"consumer_account_id": "a", "other": "x"},
                [],
            ),
        }.items():
            candidates.extend({**base, field: value} for value in values)
        candidates.extend(
            ({**base, "other": "x"}, {k: v for k, v in base.items() if k != "payload"})
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                if _valid_snapshot_request(candidate):
                    self.assertTrue(validator.is_valid(candidate))

    @staticmethod
    def _item() -> PositionedConsumerEventV2:
        trace = ConsumerTrace(
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "strategy-1",
            "strategy-order-1",
            "attribution-1",
            1,
            OrderOrigin.STRATEGY,
        )
        event = ConsumerOrderEvent(
            "event-1",
            ConsumerEventKind.ORDER,
            2,
            "account-1",
            TradingDayEpoch("2026-09-18"),
            "journal-1",
            1,
            trace,
            "oms-1",
            "execution-1",
            OmsOrderState.WORKING,
            1,
        )
        return PositionedConsumerEventV2(ConsumerEventPositionV2(RedisStreamIdV1(1, 0), 0), event)

    def test_fragmented_and_coalesced_frames_are_exactly_assembled(self) -> None:
        one = encode_frame(MessageKind.REQUEST, 1, {"message_type": "request"})
        two = encode_frame(MessageKind.RESPONSE, 2, {"message_type": "response"})
        decoder = ConsumerWireStreamDecoderV2()
        self.assertEqual((), decoder.feed(one[:7]))
        self.assertEqual(7, decoder.pending_bytes)
        frames = decoder.feed(one[7:] + two)
        self.assertEqual((1, 2), tuple(frame.header.correlation_id for frame in frames))
        self.assertEqual(0, decoder.pending_bytes)

    def test_rejects_an_oversized_aggregate(self) -> None:
        with self.assertRaises(ConsumerUnixSocketError):
            ConsumerWireStreamDecoderV2().feed(b"x" * 1_048_609)

    def test_rejects_ancillary_socket_data_before_request_dispatch(self) -> None:
        if not hasattr(socket.socket, "recvmsg"):
            self.skipTest("ancillary inspection requires recvmsg")
        left, right = socket.socketpair()
        listener = ConsumerUnixSocketListenerV2(
            socket_path=Path.cwd() / "consumer-test.sock",
            gateway=object(),
            principal_for_peer=lambda _: object(),
            request_schema={"type": "object"},
            request_handler=lambda session, request: {},
        )
        try:
            right.sendmsg([b"x"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, b"\x00\x00\x00\x00")])
            with self.assertRaisesRegex(ConsumerUnixSocketError, "ancillary"):
                listener._receive_without_ancillary(left)
        finally:
            left.close()
            right.close()

    def test_rejected_truncated_rights_do_not_leak_received_descriptors(self) -> None:
        if not hasattr(socket.socket, "recvmsg") or not Path("/proc/self/fd").is_dir():
            self.skipTest("received FD accounting requires Linux")
        source = os.open(os.devnull, os.O_RDONLY)
        left, right = socket.socketpair()
        try:
            before = len(list(Path("/proc/self/fd").iterdir()))
            for _ in range(8):
                right.sendmsg(
                    [b"x"],
                    [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack("=32i", *[source] * 32))],
                )
                with self.assertRaisesRegex(ConsumerUnixSocketError, "ancillary"):
                    ConsumerUnixSocketListenerV2._receive_without_ancillary(left, 64)
            self.assertLessEqual(len(list(Path("/proc/self/fd").iterdir())), before)
        finally:
            os.close(source)
            left.close()
            right.close()

    def test_dispatches_only_complete_schema_valid_request_frames(self) -> None:
        left, right = socket.socketpair()
        listener = ConsumerUnixSocketListenerV2(
            socket_path=Path.cwd() / "consumer-test.sock",
            gateway=object(),
            principal_for_peer=lambda _: object(),
            request_schema={"type": "object", "required": ["message_type"]},
            request_handler=lambda session, request: {
                "message_type": "response",
                "schema_version": 2,
                "request_id": request.get("request_id", "r"),
                "status": "COMPLETED",
                "payload": {},
            },
        )
        session = object()
        worker = Thread(target=listener._serve_connection, args=(left, session))
        worker.start()
        right.sendall(
            encode_frame(MessageKind.REQUEST, 7, {"message_type": "request", "request_id": "r"})
        )
        response = decode_frame(right.recv(4096))
        self.assertEqual(7, response.header.correlation_id)
        right.close()
        worker.join(timeout=1)

    def test_opted_in_snapshot_reads_can_dispatch_concurrently(self) -> None:
        barrier = Barrier(2)

        def handler(_session: object, request: dict[str, object]) -> dict[str, object]:
            barrier.wait(timeout=2)
            return {
                "message_type": "response",
                "schema_version": 2,
                "request_id": request["request_id"],
                "status": "COMPLETED",
                "payload": {},
            }

        listener = ConsumerUnixSocketListenerV2(
            socket_path=Path.cwd() / "consumer-test.sock",
            gateway=object(),
            principal_for_peer=lambda _: object(),
            request_schema={"type": "object", "required": ["message_type", "request_id"]},
            request_handler=handler,
            parallel_read_operations=frozenset({"GetAccountOrderSnapshot"}),
        )
        pairs = [socket.socketpair(), socket.socketpair()]
        workers = [
            Thread(target=listener._serve_connection, args=(server, object()))
            for server, _client in pairs
        ]
        try:
            for worker in workers:
                worker.start()
            for index, (_server, client) in enumerate(pairs):
                client.settimeout(3)
                client.sendall(
                    encode_frame(
                        MessageKind.REQUEST,
                        index + 1,
                        {
                            "message_type": "request",
                            "request_id": f"snapshot-{index}",
                            "operation": "GetAccountOrderSnapshot",
                        },
                    )
                )
            for _server, client in pairs:
                self.assertEqual("COMPLETED", decode_frame(client.recv(4096)).payload["status"])
        finally:
            for _server, client in pairs:
                client.shutdown(socket.SHUT_WR)
            for worker in workers:
                worker.join(timeout=3)
            for server, client in pairs:
                server.close()
                client.close()

    def test_live_subscription_pushes_bounded_events_with_replay_cursor(self) -> None:
        left, right = socket.socketpair()
        subscription = BoundedConsumerEventSubscriptionV2(
            limits=ConsumerSubscriptionLimitsV2(max_events=2, max_bytes=1024)
        )
        listener = ConsumerUnixSocketListenerV2(
            socket_path=Path.cwd() / "consumer-test.sock",
            gateway=object(),
            principal_for_peer=lambda _: object(),
            request_schema={"type": "object", "required": ["message_type", "request_id"]},
            request_handler=lambda _session, _request: {},
            live_subscription_for=lambda _session, _request: ConsumerLiveSubscriptionV2(
                subscription=subscription, cursor_for=lambda _item: "signed-cursor-1"
            ),
            live_poll_interval_seconds=0.01,
        )
        worker = Thread(target=listener._serve_connection, args=(left, object()))
        worker.start()
        right.sendall(
            encode_frame(
                MessageKind.REQUEST,
                7,
                {
                    "message_type": "request",
                    "schema_version": 2,
                    "request_id": "subscribe-1",
                    "operation": "SubscribeOrderEvents",
                    "payload": {"cursor": "cursor-1"},
                },
            )
        )
        response = decode_frame(right.recv(4096))
        self.assertEqual(MessageKind.RESPONSE, response.header.message_kind)
        self.assertTrue(subscription.offer(self._item(), encoded_bytes=64))
        pushed = decode_frame(right.recv(4096))
        self.assertEqual(MessageKind.EVENT, pushed.header.message_kind)
        self.assertEqual("signed-cursor-1", pushed.payload["payload"]["event_cursor"])
        right.close()
        worker.join(timeout=1)
