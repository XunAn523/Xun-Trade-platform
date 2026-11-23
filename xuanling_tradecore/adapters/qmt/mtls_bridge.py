"""Linux QMT bridge port over the pinned mutual-TLS command stream.

The optional second connection receives normalized durable Edge events. Native
callback projection and complete recovery evidence remain separate P2/P3 gates.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Callable, Protocol

from ...domain import BrokerRecoveryEvidence, CancelRequest, RecoveryPlan, VenueOrderIdentity
from ..base import BrokerQuery, QueryHandle, RecoveryHandle
from .bridge_protocol import (
    QmtBridgeCancelReceipt,
    QmtBridgeDeliverySink,
    QmtBridgeError,
    QmtBridgeHealth,
    QmtBridgePort,
    QmtBridgeSubmitReceipt,
    QmtSubmitCommand,
)
from .bridge_wire import (
    QmtBridgeWireError,
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from .edge_command import QmtEdgeCommandV1
from .event_client import QmtMtlsEventStream
from .event_wire import QmtEventStreamConfig
from .mtls_channel import QmtMtlsChannelError, QmtMtlsClientConfig, QmtMtlsConnection


class QmtMtlsBridgeError(QmtBridgeError):
    """The Linux QMT mTLS bridge cannot authenticate, correlate, or continue."""


class _QmtMtlsFrameConnection(Protocol):
    def send(self, frame: QmtBridgeWireFrame) -> None: ...

    def receive(self) -> QmtBridgeWireFrame: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class QmtMtlsBridgeConfig:
    """Linux-side scope/adapter fence and pinned TLS client configuration."""

    tls: QmtMtlsClientConfig
    scope_digest: str
    adapter_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    events: QmtEventStreamConfig | None = None

    def __post_init__(self) -> None:
        normalized = self.scope_digest.lower()
        if len(normalized) != 64 or any(
            character not in "0123456789abcdef" for character in normalized
        ):
            raise QmtMtlsBridgeError("QMT mTLS bridge scope digest is invalid")
        if not self.adapter_id or not self.adapter_instance_id or not self.adapter_client_scope:
            raise QmtMtlsBridgeError("QMT mTLS bridge adapter identity is invalid")
        object.__setattr__(self, "scope_digest", normalized)


class QmtMtlsBridgePort(QmtBridgePort):
    """Synchronous, bounded command stream; no unauthenticated fallback exists."""

    def __init__(
        self,
        config: QmtMtlsBridgeConfig,
        *,
        connection_factory: Callable[
            [QmtMtlsClientConfig], _QmtMtlsFrameConnection
        ] = QmtMtlsConnection.connect,
    ) -> None:
        self._config = config
        self._connection_factory = connection_factory
        self._connection: _QmtMtlsFrameConnection | None = None
        self._sink: QmtBridgeDeliverySink | None = None
        self._lock = RLock()
        self._events = (
            QmtMtlsEventStream(
                tls=config.tls,
                scope_digest=config.scope_digest,
                config=config.events,
                connection_factory=connection_factory,
            )
            if config.events is not None
            else None
        )
        self._fault_sink: Callable[[], None] | None = None
        self._fault_reported = False

    def set_fault_sink(self, sink: Callable[[], None] | None) -> None:
        self._fault_sink = sink

    def receive_one_event(self) -> int:
        """Called by the QMT connection owner, never by generic OMS/EMS code."""
        events = self._events
        if events is None:
            raise QmtMtlsBridgeError("QMT event connection is not configured")
        try:
            return events.receive_one()
        except BaseException as error:
            self.close()
            if not self._fault_reported:
                self._fault_reported = True
                if self._fault_sink is not None:
                    self._fault_sink()
            if isinstance(error, Exception) and not isinstance(error, QmtBridgeError):
                raise QmtMtlsBridgeError("QMT event delivery failed") from error
            raise

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        with self._lock:
            if self._connection is not None:
                raise QmtMtlsBridgeError("QMT mTLS bridge is already open")
            connection: _QmtMtlsFrameConnection | None = None
            try:
                connection = self._connection_factory(self._config.tls)
                connection.send(self._hello_frame())
                response = connection.receive()
                health = self._decode_health(response)
                if not health.ready:
                    raise QmtMtlsBridgeError("QMT mTLS Edge reports DISARMED or unhealthy")
                if self._events is not None:
                    self._events.open(sink)
            except BaseException as error:
                if self._events is not None:
                    self._events.close()
                if connection is not None:
                    connection.close()
                if isinstance(error, (QmtMtlsChannelError, QmtBridgeWireError, OSError)):
                    raise QmtMtlsBridgeError("QMT mTLS bridge open failed") from error
                raise
            self._connection = connection
            self._sink = sink

    def close(self) -> None:
        with self._lock:
            connection, self._connection, self._sink = self._connection, None, None
            if self._events is not None:
                self._events.close()
            if connection is not None:
                connection.close()

    def health(self) -> QmtBridgeHealth:
        with self._lock:
            try:
                response = self._request(QmtBridgeWireFrame(QmtBridgeWireKind.HEALTH, b"{}"))
                health = self._decode_health(response)
            except QmtMtlsBridgeError:
                self.close()
                raise
            if not health.ready:
                self.close()
            return health

    def submit(self, command: QmtSubmitCommand) -> QmtBridgeSubmitReceipt:
        self._require_event_stream()
        self._require_submit_identity(command)
        accepted = self._command(
            QmtEdgeCommandV1.from_submit(command, scope_digest=self._config.scope_digest)
        )
        return QmtBridgeSubmitReceipt(
            execution_id=command.instruction.execution_id,
            accepted_monotonic_ns=_as_nonnegative_int(accepted, "accepted_monotonic_ns"),
        )

    def cancel(
        self, request: CancelRequest, identity: VenueOrderIdentity
    ) -> QmtBridgeCancelReceipt:
        self._require_event_stream()
        if (
            identity.venue_id != "QMT"
            or identity.adapter_instance_id != self._config.adapter_instance_id
            or identity.adapter_client_scope != self._config.adapter_client_scope
        ):
            raise QmtMtlsBridgeError("QMT mTLS cancel identity has another adapter scope")
        accepted = self._command(
            QmtEdgeCommandV1.from_cancel(request, identity, scope_digest=self._config.scope_digest)
        )
        return QmtBridgeCancelReceipt(
            cancel_request_id=request.cancel_request_id,
            accepted_monotonic_ns=_as_nonnegative_int(accepted, "accepted_monotonic_ns"),
        )

    def query(self, request: BrokerQuery) -> QueryHandle:
        self._require_profile(request.account_id, request.venue_id, request.adapter_instance_id)
        self._command(
            QmtEdgeCommandV1.from_query(
                request,
                scope_digest=self._config.scope_digest,
                adapter_client_scope=self._config.adapter_client_scope,
            )
        )
        return QueryHandle(
            request.request_id, self._config.adapter_id, self._config.adapter_instance_id
        )

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        self._require_profile(plan.account_id, plan.venue_id, plan.adapter_instance_id)
        self._command(
            QmtEdgeCommandV1.from_recovery(
                plan,
                scope_digest=self._config.scope_digest,
                adapter_client_scope=self._config.adapter_client_scope,
            )
        )
        return RecoveryHandle(
            plan.recovery_run_id, self._config.adapter_id, self._config.adapter_instance_id
        )

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        del handle
        raise QmtMtlsBridgeError(
            "QMT mTLS recovery evidence requires the unimplemented P3 event path"
        )

    def acknowledge(self, delivery_id: int) -> None:
        self._require_event_stream()
        assert self._events is not None
        self._events.acknowledge(delivery_id)

    def _require_event_stream(self) -> None:
        if self._events is None or not self._events.opened:
            raise QmtMtlsBridgeError("QMT mutation/ACK requires a connected event stream")

    def _command(self, command: QmtEdgeCommandV1) -> dict[str, object]:
        with self._lock:
            try:
                response = self._request(
                    QmtBridgeWireFrame(QmtBridgeWireKind.COMMAND, command.canonical_bytes)
                )
                return self._decode_acceptance(response, command.request_id)
            except QmtMtlsBridgeError:
                # Never reuse a stream after an ambiguous or rejected command;
                # no reconnect or command replay is performed here.
                self.close()
                raise

    def _decode_acceptance(
        self, response: QmtBridgeWireFrame, request_id: str
    ) -> dict[str, object]:
        if response.kind is QmtBridgeWireKind.REJECTED:
            raise QmtMtlsBridgeError("QMT mTLS Edge rejected command")
        if response.kind is not QmtBridgeWireKind.ACCEPTED:
            raise QmtMtlsBridgeError("QMT mTLS Edge returned another command response kind")
        accepted = self._payload(response)
        if (
            set(accepted)
            != {
                "schema",
                "request_id",
                "command_sequence",
                "accepted_monotonic_ns",
                "state",
            }
            or accepted.get("schema") != "QmtEdgeAcceptanceV1"
        ):
            raise QmtMtlsBridgeError("QMT mTLS Edge acceptance schema is invalid")
        if accepted["request_id"] != request_id:
            raise QmtMtlsBridgeError("QMT mTLS Edge accepted another request")
        if _as_nonnegative_int(accepted, "command_sequence") == 0:
            raise QmtMtlsBridgeError("QMT mTLS Edge acceptance sequence is invalid")
        _as_nonnegative_int(accepted, "accepted_monotonic_ns")
        if accepted["state"] not in (
            "EDGE_ACCEPTED",
            "PUBLISH_PREPARED",
            "PUBLISH_ATTEMPTED",
            "CALL_STARTED",
            "CALL_RETURNED",
        ):
            raise QmtMtlsBridgeError("QMT mTLS Edge acceptance state is invalid or unknown-effect")
        return accepted

    def _request(self, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        connection = self._connection
        if connection is None:
            raise QmtMtlsBridgeError("QMT mTLS bridge is not open")
        try:
            connection.send(frame)
            return connection.receive()
        except (QmtMtlsChannelError, QmtBridgeWireError, OSError) as error:
            self.close()
            raise QmtMtlsBridgeError("QMT mTLS bridge request failed") from error

    def _decode_health(self, frame: QmtBridgeWireFrame) -> QmtBridgeHealth:
        if frame.kind is not QmtBridgeWireKind.HEALTH:
            raise QmtMtlsBridgeError("QMT mTLS Edge did not return a health frame")
        payload = self._payload(frame)
        if (
            set(payload) != {"schema", "ready", "detail_code"}
            or payload.get("schema") != "QmtEdgeHealthV1"
        ):
            raise QmtMtlsBridgeError("QMT mTLS Edge health schema is invalid")
        ready = payload["ready"]
        detail_code = payload["detail_code"]
        if (
            type(ready) is not bool
            or not isinstance(detail_code, str)
            or not detail_code.strip()
            or len(detail_code) > 128
        ):
            raise QmtMtlsBridgeError("QMT mTLS Edge health payload is invalid")
        try:
            return QmtBridgeHealth(ready=ready, detail_code=detail_code)
        except ValueError as error:
            raise QmtMtlsBridgeError("QMT mTLS Edge health payload is invalid") from error

    def _hello_frame(self) -> QmtBridgeWireFrame:
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.HELLO,
            canonical_wire_payload(
                {
                    "schema": "QmtEdgeHelloV1",
                    "role": "COMMAND",
                    "scope_digest": self._config.scope_digest,
                }
            ),
        )

    @staticmethod
    def _payload(frame: QmtBridgeWireFrame) -> dict[str, object]:
        try:
            return dict(decode_canonical_wire_payload(frame.payload))
        except QmtBridgeWireError as error:
            raise QmtMtlsBridgeError("QMT mTLS Edge payload is invalid") from error

    def _require_submit_identity(self, command: QmtSubmitCommand) -> None:
        self._require_profile(command.instruction.account_id, "QMT", command.adapter_instance_id)
        if command.adapter_client_scope != self._config.adapter_client_scope:
            raise QmtMtlsBridgeError("QMT mTLS submit has another adapter scope")

    def _require_profile(self, account_id: str, venue_id: str, adapter_instance_id: str) -> None:
        if (
            venue_id != "QMT"
            or adapter_instance_id != self._config.adapter_instance_id
            or not account_id
            or (self._config.events is not None and account_id != self._config.events.account_id)
        ):
            raise QmtMtlsBridgeError("QMT mTLS command belongs to another adapter profile")


def _as_nonnegative_int(payload: dict[str, object], name: str) -> int:
    value = payload[name]
    if type(value) is not int or value < 0:
        raise QmtMtlsBridgeError(f"QMT mTLS Edge acceptance {name} is invalid")
    return value
