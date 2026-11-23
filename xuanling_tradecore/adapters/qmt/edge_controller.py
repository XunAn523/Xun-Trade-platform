"""Windows Edge command admission after mTLS identity verification.

The TLS listener is responsible for authenticating its peer.  This controller
then performs the second, content-level scope fence, commits the request, and
only in an explicitly armed configuration publishes the one binary native
frame.  It contains no QMT API call.
"""

from __future__ import annotations

from dataclasses import dataclass

from .edge_command import QmtEdgeCommandV1
from .edge_config import QmtEdgeConfigV1
from .edge_native_encoder import QmtEdgeNativeEncoder, QmtEdgeNativeEncodingError
from .edge_runtime import QmtEdgeRuntime
from .edge_store import QmtEdgeRequest


class QmtEdgeCommandAdmissionError(RuntimeError):
    """The Edge rejected a command before or at its native ring boundary."""


@dataclass(frozen=True)
class QmtEdgeCommandAcceptance:
    """Durable Edge acceptance; it is not a QMT native or venue result."""

    request_id: str
    command_sequence: int
    accepted_monotonic_ns: int
    state: str


class QmtEdgeCommandController:
    """The sole Edge command admission path for one configured QMT scope."""

    def __init__(self, runtime: QmtEdgeRuntime) -> None:
        self._runtime = runtime

    def accept_and_publish(self, command: QmtEdgeCommandV1) -> QmtEdgeCommandAcceptance:
        config = self._runtime.config
        self._require_identity(config, command)
        if config.is_disarmed:
            raise QmtEdgeCommandAdmissionError("QMT Edge is DISARMED and cannot publish commands")
        policy = config.native_policy
        if policy is None:
            raise QmtEdgeCommandAdmissionError("QMT Edge lacks an approved native policy")
        request = self._runtime.store.accept_request(
            request_id=command.request_id, canonical_content=command.canonical_bytes
        )
        try:
            frame = QmtEdgeNativeEncoder(policy).encode(
                command, command_sequence=request.command_sequence
            )
            published = self._runtime.native_client.publish_durable_request(request, frame)
        except QmtEdgeNativeEncodingError as error:
            raise QmtEdgeCommandAdmissionError("QMT Edge cannot encode the approved native frame") from error
        return self._acceptance_from_request(published.request)

    @staticmethod
    def _require_identity(config: QmtEdgeConfigV1, command: QmtEdgeCommandV1) -> None:
        if command.scope_digest != config.scope_digest.hex():
            raise QmtEdgeCommandAdmissionError("QMT Edge command belongs to another scope digest")
        if (
            (config.account_id is not None and command.account_id != config.account_id)
            or (
                config.adapter_instance_id is not None
                and command.adapter_instance_id != config.adapter_instance_id
            )
            or (
                config.adapter_client_scope is not None
                and command.adapter_client_scope != config.adapter_client_scope
            )
        ):
            raise QmtEdgeCommandAdmissionError("QMT Edge command identity belongs to another scope")

    @staticmethod
    def _acceptance_from_request(request: QmtEdgeRequest) -> QmtEdgeCommandAcceptance:
        return QmtEdgeCommandAcceptance(
            request_id=request.request_id,
            command_sequence=request.command_sequence,
            accepted_monotonic_ns=request.accepted_monotonic_ns,
            state=request.state,
        )
