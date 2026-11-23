"""One Linux QMT event connection, pumped by its owning event worker."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from threading import RLock
from time import monotonic_ns
from typing import Callable, Protocol

from .bridge_protocol import QmtBridgeDeliverySink, QmtBridgeEventDelivery
from .bridge_wire import QmtBridgeWireFrame, QmtBridgeWireKind
from .event_wire import QmtEventStreamConfig, QmtEventWireCodec, QmtEventWireError
from .mtls_channel import QmtMtlsClientConfig, QmtMtlsConnection


class QmtEventFrameConnection(Protocol):
    def send(self, frame: QmtBridgeWireFrame) -> None: ...
    def receive(self) -> QmtBridgeWireFrame: ...
    def close(self) -> None: ...


class QmtMtlsEventStream:
    """Single in-flight delivery; ACK is called only by the durable sink.

    No worker is started implicitly. The host calls receive_one from one
    event worker; a read/ACK timeout faults the connection, never a command retry.
    """

    def __init__(
        self,
        *,
        tls: QmtMtlsClientConfig,
        scope_digest: str,
        config: QmtEventStreamConfig,
        connection_factory: Callable[
            [QmtMtlsClientConfig], QmtEventFrameConnection
        ] = QmtMtlsConnection.connect,
    ) -> None:
        self._tls = tls
        self._codec = QmtEventWireCodec(scope_digest=scope_digest, config=config)
        self._factory = connection_factory
        self._cursor = config.resume_delivery_id
        self._connection: QmtEventFrameConnection | None = None
        self._sink: QmtBridgeDeliverySink | None = None
        self._pending: tuple[int, str] | None = None
        self._reader_lock = RLock()

    @property
    def opened(self) -> bool:
        return self._connection is not None

    @property
    def acknowledged_delivery_id(self) -> int:
        return self._cursor

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        with self._reader_lock:
            if self.opened:
                raise QmtEventWireError("QMT event stream is already open")
            connection = self._factory(self._tls)
            try:
                connection.send(
                    self._codec.control(
                        QmtBridgeWireKind.HELLO,
                        "QmtEventHelloV1",
                        role="EVENT",
                        resume_delivery_id=self._cursor,
                    )
                )
                response = self._codec.decode_control(
                    connection.receive(),
                    kind=QmtBridgeWireKind.ACCEPTED,
                    schema="QmtEventHelloAcceptedV1",
                    fields={"resume_delivery_id"},
                )
                if (
                    type(response["resume_delivery_id"]) is not int
                    or response["resume_delivery_id"] != self._cursor
                ):
                    raise QmtEventWireError("QMT event resume cursor was not accepted exactly")
            except BaseException:
                connection.close()
                raise
            self._connection, self._sink = connection, sink

    def close(self) -> None:
        # Closing from the host must interrupt a blocked receive; do not acquire
        # the reader lock here. Never join a worker while holding its sink lock.
        connection, self._connection = self._connection, None
        self._sink = None
        self._pending = None
        if connection is not None:
            connection.close()

    def receive_one(self) -> int:
        with self._reader_lock:
            connection, sink = self._connection, self._sink
            if connection is None or sink is None:
                raise QmtEventWireError("QMT event stream is not open")
            try:
                frame = connection.receive()
                if frame.kind is QmtBridgeWireKind.HEALTH:
                    health = self._codec.decode_control(
                        frame,
                        kind=QmtBridgeWireKind.HEALTH,
                        schema="QmtEventHealthV1",
                        fields={"ready"},
                    )
                    if health["ready"] is not True:
                        raise QmtEventWireError("QMT event stream reports unhealthy")
                    return 0
                delivery = self._codec.decode_delivery(frame)
                if isinstance(delivery, QmtBridgeEventDelivery):
                    # BrokerEvent.received_monotonic_ns belongs to Linux.
                    # The original Edge time remains in its durable outbox and
                    # the wire hash; clocks from separate hosts are not mixed.
                    delivery = replace(
                        delivery,
                        event=replace(delivery.event, received_monotonic_ns=monotonic_ns()),
                    )
                if delivery.delivery_id != self._cursor + 1:
                    raise QmtEventWireError("QMT event delivery is not the next resume sequence")
                self._pending = (delivery.delivery_id, sha256(frame.payload).hexdigest())
                sink(delivery)
                self._require_acknowledged()
                return 1
            except BaseException:
                self.close()
                raise

    def _require_acknowledged(self) -> None:
        if self._pending is not None or not self.opened:
            raise QmtEventWireError("QMT event sink returned without a durable ACK")

    def acknowledge(self, delivery_id: int) -> None:
        with self._reader_lock:
            connection = self._connection
            pending = self._pending
            if (
                connection is None
                or pending is None
                or type(delivery_id) is not int
                or delivery_id != pending[0]
            ):
                raise QmtEventWireError("QMT ACK does not match the in-flight delivery")
            try:
                fields = {"delivery_id": delivery_id, "payload_hash": pending[1]}
                connection.send(
                    self._codec.control(QmtBridgeWireKind.ACK, "QmtEventAckV1", **fields)
                )
                response = self._codec.decode_control(
                    connection.receive(),
                    kind=QmtBridgeWireKind.ACK,
                    schema="QmtEventAckV1",
                    fields=set(fields),
                )
                if type(response["delivery_id"]) is not int or any(
                    response[key] != value for key, value in fields.items()
                ):
                    raise QmtEventWireError("QMT Edge confirmed another ACK")
                self._cursor = delivery_id
                self._pending = None
            except BaseException:
                self.close()
                raise
