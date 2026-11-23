"""The Windows Edge owner of command/event SPSC ring roles.

This component never calls a QMT API.  It publishes only already durable Edge
requests and marks publication conservatively before touching shared memory.
The reciprocal event path appends the event to the Edge outbox before it can be
sent across the Linux bridge.
"""

from __future__ import annotations

from dataclasses import dataclass

from .edge_store import QmtEdgeOutboxRecord, QmtEdgeRequest, QmtEdgeStore
from .native_engine_contract import QmtNativeContractError, QmtNativeFrameV1
from .windows_ring import QmtNativeRing, QmtNativeRingEmpty


class QmtNativeEngineClientError(RuntimeError):
    """An Edge/ring ownership or crash-boundary invariant failed."""


class QmtNativeEventPending(QmtNativeEngineClientError):
    """No frame is available; unlike corruption, this is safe to wait for."""


@dataclass(frozen=True)
class QmtNativePublishResult:
    """The Edge result after a command frame is conservatively published."""

    request: QmtEdgeRequest
    command_sequence: int


class QmtNativeEngineClient:
    """The sole Edge command producer and event consumer for one scope."""

    def __init__(
        self,
        *,
        store: QmtEdgeStore,
        command_ring: QmtNativeRing,
        event_ring: QmtNativeRing,
    ) -> None:
        self._store = store
        self._command_ring = command_ring
        self._event_ring = event_ring

    def publish_durable_request(
        self, request: QmtEdgeRequest, frame: QmtNativeFrameV1
    ) -> QmtNativePublishResult:
        """Mark unknown-on-crash before publishing exactly one command frame."""

        if frame.sequence != request.command_sequence:
            raise QmtNativeEngineClientError("QMT command frame has another Edge sequence")
        prepared = self._store.record_publish_prepared(request.request_id, request.command_sequence)
        if prepared.state not in {
            "PUBLISH_PREPARED",
            "PUBLISH_ATTEMPTED",
            "CALL_STARTED",
            "CALL_RETURNED",
        }:
            raise QmtNativeEngineClientError("QMT request cannot enter the native command ring")
        if prepared.state == "PUBLISH_PREPARED":
            try:
                self._command_ring.publish(frame)
            except QmtNativeContractError as error:
                raise QmtNativeEngineClientError("QMT command ring publish failed") from error
        published = self._store.record_publish_attempt(request.request_id, request.command_sequence)
        return QmtNativePublishResult(request=published, command_sequence=request.command_sequence)

    def drain_one_event(self) -> QmtEdgeOutboxRecord:
        """Durably append a native event before returning it for Linux delivery."""

        try:
            frame = self._event_ring.peek()
        except QmtNativeRingEmpty as error:
            raise QmtNativeEventPending("QMT event ring is empty") from error
        except QmtNativeContractError as error:
            raise QmtNativeEngineClientError("QMT event ring consume failed") from error
        event_id = f"qmt-native:{self._event_ring.generation.hex()}:{frame.sequence}"
        record = self._store.append_outbox(event_id=event_id, payload=frame.encode_uncommitted())
        # A crash before this release replays the same generation/sequence;
        # append_outbox is idempotent. A failed SQLite write leaves the ring intact.
        self._event_ring.release_consumed(frame.sequence)
        return record

    def mark_unclean_restart(self) -> tuple[QmtEdgeRequest, ...]:
        """Preserve all pre-native uncertainty for later read-only recovery."""

        return self._store.mark_unknown_effects()
