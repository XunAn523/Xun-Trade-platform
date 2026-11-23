"""Wire-level QMT Edge endpoint independent from TLS socket ownership."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .bridge_wire import (
    QmtBridgeWireError,
    QmtBridgeWireFrame,
    QmtBridgeWireKind,
    canonical_wire_payload,
    decode_canonical_wire_payload,
)
from .edge_command import QmtEdgeCommandError, QmtEdgeCommandV1
from .edge_controller import QmtEdgeCommandAcceptance


class QmtEdgeWireEndpointError(RuntimeError):
    """A peer sent an unauthorized or invalid QMT Edge wire message."""


class QmtEdgeCommandAcceptor(Protocol):
    """The small controller seam used by socket and deterministic tests."""

    def accept_and_publish(self, command: QmtEdgeCommandV1) -> QmtEdgeCommandAcceptance:
        """Durably accept then conservatively publish one native command."""

        ...


@dataclass(frozen=True)
class QmtEdgeHealthProjection:
    """Payload-free Edge health sent only after the scope HELLO fence."""

    ready: bool
    detail_code: str


class QmtEdgeWireEndpoint:
    """Validate HELLO/COMMAND/HEALTH records after mTLS leaf pinning."""

    def __init__(
        self,
        *,
        scope_digest: str,
        command_acceptor: QmtEdgeCommandAcceptor,
        health: QmtEdgeHealthProjection,
    ) -> None:
        if len(scope_digest) != 64:
            raise QmtEdgeWireEndpointError("QMT Edge endpoint scope digest is invalid")
        self._scope_digest = scope_digest.lower()
        self._command_acceptor = command_acceptor
        self._health = health

    def handle(self, frame: QmtBridgeWireFrame, *, hello_complete: bool) -> QmtBridgeWireFrame:
        """Process one frame.  The caller owns the connection and session state."""

        if not hello_complete:
            return self._hello(frame)
        if frame.kind is QmtBridgeWireKind.HEALTH:
            return self._health_frame()
        if frame.kind is QmtBridgeWireKind.COMMAND:
            return self._command(frame)
        if frame.kind is QmtBridgeWireKind.ACK:
            return self._reject("ACK_UNSUPPORTED")
        return self._reject("UNEXPECTED_WIRE_KIND")

    def _hello(self, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        if frame.kind is not QmtBridgeWireKind.HELLO:
            raise QmtEdgeWireEndpointError("QMT Edge requires HELLO as the first wire frame")
        payload = _decode_payload(frame)
        if payload != {
            "schema": "QmtEdgeHelloV1",
            "role": "COMMAND",
            "scope_digest": self._scope_digest,
        }:
            raise QmtEdgeWireEndpointError("QMT Edge HELLO has another role or scope")
        return self._health_frame()

    def _command(self, frame: QmtBridgeWireFrame) -> QmtBridgeWireFrame:
        try:
            command = QmtEdgeCommandV1.from_mapping(_decode_payload(frame))
            if command.scope_digest != self._scope_digest:
                raise QmtEdgeCommandError("QMT Edge command scope does not match this endpoint")
            accepted = self._command_acceptor.accept_and_publish(command)
        except (QmtEdgeCommandError, RuntimeError) as error:
            return self._reject(_reject_code(error))
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.ACCEPTED,
            canonical_wire_payload(
                {
                    "schema": "QmtEdgeAcceptanceV1",
                    "request_id": accepted.request_id,
                    "command_sequence": accepted.command_sequence,
                    "accepted_monotonic_ns": accepted.accepted_monotonic_ns,
                    "state": accepted.state,
                }
            ),
        )

    def _health_frame(self) -> QmtBridgeWireFrame:
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.HEALTH,
            canonical_wire_payload(
                {
                    "schema": "QmtEdgeHealthV1",
                    "ready": self._health.ready,
                    "detail_code": self._health.detail_code,
                }
            ),
        )

    @staticmethod
    def _reject(code: str) -> QmtBridgeWireFrame:
        return QmtBridgeWireFrame(
            QmtBridgeWireKind.REJECTED,
            canonical_wire_payload({"schema": "QmtEdgeRejectV1", "code": code}),
        )


def _decode_payload(frame: QmtBridgeWireFrame) -> dict[str, object]:
    try:
        decoded = decode_canonical_wire_payload(frame.payload)
    except QmtBridgeWireError as error:
        raise QmtEdgeWireEndpointError("QMT Edge wire payload is invalid") from error
    return dict(decoded)


def _reject_code(error: RuntimeError) -> str:
    message = str(error).upper().replace(" ", "_")
    compact = "".join(character for character in message if character.isascii() and character.isalnum() or character == "_")
    return compact[:96] or "EDGE_REJECTED"
