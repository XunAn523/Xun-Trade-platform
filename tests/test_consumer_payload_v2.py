from __future__ import annotations

from unittest.mock import patch

from xuanling_tradecore.consumer.payload_v2 import (
    CanonicalPayloadV2,
    CanonicalResponsePayloadV2,
    canonical_payload_bytes,
)


def test_canonical_payload_encodes_once_and_copies_its_top_level() -> None:
    source: dict[str, object] = {"status": "COMPLETED", "payload": {"count": 1}}
    payload = CanonicalPayloadV2(source)
    source["status"] = "MUTATED"

    with patch(
        "xuanling_tradecore.consumer.payload_v2._encode_json",
        wraps=__import__(
            "xuanling_tradecore.consumer.payload_v2", fromlist=["_encode_json"]
        )._encode_json,
    ) as encode:
        first = canonical_payload_bytes(payload)
        second = canonical_payload_bytes(payload)

    assert first == second
    assert b'"status":"COMPLETED"' in first
    assert encode.call_count == 1


def test_canonical_response_template_matches_regular_canonical_json() -> None:
    body: dict[str, object] = {"orders": [{"event_id": "event-1"}], "count": 1}
    expected = CanonicalPayloadV2(
        {
            "message_type": "response",
            "schema_version": 2,
            "request_id": 'request-"-一',
            "status": "COMPLETED",
            "payload": body,
        }
    )
    templated = CanonicalResponsePayloadV2(
        request_id='request-"-一',
        payload=body,
        payload_bytes=canonical_payload_bytes(body),
    )

    assert canonical_payload_bytes(templated) == canonical_payload_bytes(expected)
