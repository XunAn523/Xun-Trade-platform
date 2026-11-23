"""Small same-host AF_UNIX client for the ConsumerGateway V2 data plane."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Mapping

from ..domain._validation import require_positive
from .base import ConsumerGatewayError
from .unix_v2 import ConsumerWireStreamDecoderV2
from .wire_v2 import MessageKind, encode_frame


class ConsumerUnixClientErrorV2(ConsumerGatewayError):
    """A local Consumer V2 request failed before a valid response was received."""


@dataclass(frozen=True)
class ConsumerUnixClientConfigV2:
    socket_path: Path
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.socket_path.is_absolute():
            raise ConsumerUnixClientErrorV2("Consumer V2 socket path must be absolute")
        if (
            type(self.timeout_seconds) not in (int, float)
            or not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0
        ):
            raise ConsumerUnixClientErrorV2("Consumer V2 client timeout must be positive and finite")


class ConsumerUnixClientV2:
    """One-request-per-connection client; it exposes no Adapter or credential field."""

    def __init__(self, config: ConsumerUnixClientConfigV2) -> None:
        self._config = config
        self._next_correlation_id = 1
        self._lock = Lock()

    def call(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Send one Consumer V2 request and return its matching response/error payload."""

        deadline = monotonic() + self._config.timeout_seconds
        correlation_id = self._allocate_correlation_id()
        frame = encode_frame(MessageKind.REQUEST, correlation_id, request)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        def remaining() -> float:
            budget = deadline - monotonic()
            if budget <= 0:
                raise TimeoutError("Consumer V2 whole-call deadline expired")
            return budget

        try:
            connection.settimeout(remaining())
            connection.connect(str(self._config.socket_path))
            connection.settimeout(remaining())
            connection.sendall(frame)
            decoder = ConsumerWireStreamDecoderV2()
            while True:
                connection.settimeout(remaining())
                chunk = connection.recv(65_536)
                if not chunk:
                    raise ConsumerUnixClientErrorV2("Consumer V2 server closed without response")
                for decoded in decoder.feed(chunk):
                    if decoded.header.correlation_id != correlation_id:
                        raise ConsumerUnixClientErrorV2("Consumer V2 response correlation is invalid")
                    if decoded.header.message_kind not in {MessageKind.RESPONSE, MessageKind.ERROR}:
                        raise ConsumerUnixClientErrorV2("Consumer V2 response kind is invalid")
                    remaining()  # Decoding/validation also belongs to the call budget.
                    return decoded.payload
        except OSError as exc:
            raise ConsumerUnixClientErrorV2("Consumer V2 local socket is unavailable") from exc
        finally:
            connection.close()

    def create_order_snapshot(
        self, *, request_id: str, consumer_account_id: str, page_size: int = 100
    ) -> Mapping[str, object]:
        """Create one fixed large-account snapshot without requesting all orders."""

        return self.call(
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": "CreateOrderSnapshot",
                "request_id": request_id,
                "payload": {
                    "consumer_account_id": consumer_account_id,
                    "page_size": page_size,
                },
            }
        )

    def read_order_snapshot_page(
        self, *, request_id: str, page_cursor: str
    ) -> Mapping[str, object]:
        """Read one immutable page using only the opaque server-signed cursor."""

        return self.call(
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": "ReadOrderSnapshotPage",
                "request_id": request_id,
                "payload": {"page_cursor": page_cursor},
            }
        )

    def _allocate_correlation_id(self) -> int:
        with self._lock:
            correlation_id = self._next_correlation_id
            self._next_correlation_id += 1
            if self._next_correlation_id > (1 << 64) - 1:
                self._next_correlation_id = 1
            require_positive(correlation_id, "Consumer V2 correlation ID")
            return correlation_id
