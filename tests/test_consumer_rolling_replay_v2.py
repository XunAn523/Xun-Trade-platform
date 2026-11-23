from types import SimpleNamespace
from unittest.mock import Mock

from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayError
from xuanling_tradecore.consumer.unix_v2 import ConsumerUnixSocketListenerV2
from xuanling_tradecore.consumer.wire_v2 import MessageKind, decode_frame, encode_frame


def test_empty_suffix_plan_revalidates_origin_token_before_delivery(tmp_path):
    gateway = Mock()
    listener = ConsumerUnixSocketListenerV2(
        socket_path=tmp_path / "socket",
        gateway=gateway,
        principal_for_peer=lambda _: None,
        request_schema={},
        request_handler=lambda *_: {},
    )
    request = decode_frame(
        encode_frame(
            MessageKind.REQUEST,
            1,
            {
                "message_type": "request",
                "schema_version": 2,
                "request_id": "suffix",
                "operation": "CreateReplayPartitions",
                "payload": {
                    "consumer_account_id": "test",
                    "partition_count": 1,
                    "after_cursor": "exhausted",
                },
            },
        )
    )
    response = encode_frame(
        MessageKind.RESPONSE,
        1,
        {
            "message_type": "response",
            "schema_version": 2,
            "request_id": "suffix",
            "status": "COMPLETED",
            "payload": {"partitions": []},
        },
    )
    session = SimpleNamespace()
    assert listener.validate_deferred_delivery(session, request, response) == response
    gateway.validate_projection_read_delivery.assert_called_once_with(
        session, operation="CreateReplayPartitions", tokens=("exhausted",)
    )
    gateway.validate_projection_read_delivery.side_effect = ConsumerCursorReplayError(
        "CURSOR_EXPIRED", "origin expired in read worker"
    )
    error = decode_frame(listener.validate_deferred_delivery(session, request, response))
    assert error.payload["code"] == "CURSOR_EXPIRED"
