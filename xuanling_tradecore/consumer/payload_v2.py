"""Shared Consumer JSON and order-event representation for exact byte budgets."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import msgspec

from ..domain import ConsumerOrderEvent

_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")


def _encode_json(value: object) -> bytes:
    """Encode contract JSON with stable key order and UTF-8 text."""

    return _JSON_ENCODER.encode(value)


class CanonicalPayloadV2(Mapping[str, object]):
    """Immutable top-level payload whose canonical bytes are computed once."""

    __slots__ = ("_canonical", "_payload")

    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = dict(payload)
        self._canonical: bytes | None = None

    def __getitem__(self, key: str) -> object:
        return self._payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._payload)

    def __len__(self) -> int:
        return len(self._payload)

    def canonical_bytes(self) -> bytes:
        cached = self._canonical
        if cached is None:
            cached = _encode_json(self._payload)
            self._canonical = cached
        return cached


class CanonicalResponsePayloadV2(CanonicalPayloadV2):
    """Canonical response that reuses one immutable nested payload encoding."""

    __slots__ = ("_payload_bytes", "_request_id")

    def __init__(
        self,
        *,
        request_id: str,
        payload: Mapping[str, object],
        payload_bytes: bytes,
    ) -> None:
        super().__init__(
            {
                "message_type": "response",
                "schema_version": 2,
                "request_id": request_id,
                "status": "COMPLETED",
                "payload": dict(payload),
            }
        )
        self._request_id = request_id
        self._payload_bytes = payload_bytes

    def canonical_bytes(self) -> bytes:
        cached = self._canonical
        if cached is None:
            encoded_request_id = _encode_json(self._request_id)
            cached = b"".join(
                (
                    b'{"message_type":"response","payload":',
                    self._payload_bytes,
                    b',"request_id":',
                    encoded_request_id,
                    b',"schema_version":2,"status":"COMPLETED"}',
                )
            )
            self._canonical = cached
        return cached


def canonical_payload_bytes(payload: Mapping[str, object]) -> bytes:
    if isinstance(payload, CanonicalPayloadV2):
        return payload.canonical_bytes()
    return _encode_json(dict(payload))


def order_event_payload(event: ConsumerOrderEvent, *, position: str = "") -> dict[str, object]:
    result: dict[str, object] = {
        "event_id": event.consumer_event_id,
        "event_kind": event.event_kind.value,
        "payload": {
            "oms_order_id": event.oms_order_id,
            "execution_id": event.execution_id,
            "order_state": event.order_state.value,
            "order_revision": event.order_revision,
        },
    }
    if position:
        result["position"] = position
    return result
