"""QMT event session run by the Edge's SQLite owner after mTLS authentication.

Only normalized event outboxes belong here. Raw native-ring bytes are rejected;
the callback projector must write to its own scope/epoch-bound normalized store.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Callable

from ...domain._validation import require_nonnegative
from .bridge_wire import QmtBridgeWireFrame, QmtBridgeWireKind
from .edge_store import QmtEdgeStore
from .event_wire import QmtEventWireCodec, QmtEventWireError


class QmtEdgeEventEndpoint:
    """One resumable, bounded event session; one event outstanding until ACK."""

    def __init__(
        self, *, store: QmtEdgeStore, codec: QmtEventWireCodec, healthy: Callable[[], bool]
    ) -> None:
        self._store = store
        self._codec = codec
        store.bind_event_stream(
            scope_digest=codec.scope_digest,
            outbox_epoch=codec.config.outbox_epoch,
            account_id=codec.config.account_id,
        )
        self._healthy = healthy
        self._cursor: int | None = None
        self._pending: QmtBridgeWireFrame | None = None

    def open(self, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        if self._cursor is not None:
            raise QmtEventWireError("QMT event session is already open")
        payload = self._codec.decode_control(
            frame,
            kind=QmtBridgeWireKind.HELLO,
            schema="QmtEventHelloV1",
            fields={"role", "resume_delivery_id"},
        )
        cursor = require_nonnegative(payload["resume_delivery_id"], "QMT resume cursor")
        if payload["role"] != "EVENT" or cursor > self._store.outbox_high_watermark():
            raise QmtEventWireError("QMT event HELLO role or resume cursor is invalid")
        self._cursor = cursor
        return self._codec.control(
            QmtBridgeWireKind.ACCEPTED, "QmtEventHelloAcceptedV1", resume_delivery_id=cursor
        )

    def next_frame(self) -> QmtBridgeWireFrame:
        if self._cursor is None:
            raise QmtEventWireError("QMT event session requires HELLO")
        if not self._healthy():
            return self._codec.control(QmtBridgeWireKind.HEALTH, "QmtEventHealthV1", ready=False)
        if self._pending is not None:
            return self._pending
        record = self._store.outbox_after(self._cursor)
        if record is None:
            return self._codec.control(
                QmtBridgeWireKind.HEALTH, "QmtEventHealthV1", ready=self._healthy()
            )
        if record.delivery_id != self._cursor + 1:
            raise QmtEventWireError("QMT event outbox contains a delivery gap")
        self._pending = self._codec.encode_delivery(
            delivery_id=record.delivery_id, fact_payload=record.payload
        )
        return self._pending

    def acknowledge(self, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        if self._cursor is None or self._pending is None:
            raise QmtEventWireError("QMT event ACK has no in-flight delivery")
        payload = self._codec.decode_control(
            frame,
            kind=QmtBridgeWireKind.ACK,
            schema="QmtEventAckV1",
            fields={"delivery_id", "payload_hash"},
        )
        delivery_id = require_nonnegative(payload["delivery_id"], "QMT ACK delivery id")
        if (
            delivery_id != self._cursor + 1
            or payload["payload_hash"] != sha256(self._pending.payload).hexdigest()
        ):
            raise QmtEventWireError("QMT event ACK does not match the sent fact")
        self._store.acknowledge_outbox(delivery_id)
        self._cursor = delivery_id
        self._pending = None
        return self._codec.control(
            QmtBridgeWireKind.ACK,
            "QmtEventAckV1",
            delivery_id=delivery_id,
            payload_hash=payload["payload_hash"],
        )
