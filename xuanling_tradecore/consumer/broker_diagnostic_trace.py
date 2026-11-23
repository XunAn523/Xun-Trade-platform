"""Opt-in candidate-only, bounded and redacted broker handoff observations."""

from __future__ import annotations

from hashlib import sha256
from typing import Protocol

from .owner_ipc_v1 import OwnerProtocolError, OwnerRequestV1


class _TraceSink(Protocol):
    def submit(self, record: dict[str, object]) -> None: ...


class BrokerDiagnosticTrace:
    """One reactor producer; the pending map is bounded by original credits.

    The final send timestamps bracket the syscall that completed a packet. A
    receiver on another CPU can observe the packet before that syscall returns.
    Neither timestamp represents a durability or execution boundary.
    """

    def __init__(self, sink: _TraceSink, generation: str) -> None:
        self._sink = sink
        self._generation = generation
        self._pending: dict[int, dict[str, object]] = {}

    def ingested(
        self, request: OwnerRequestV1, worker: int, received: int, offered: int, outstanding: int
    ) -> None:
        serial = request.identity.serial
        if (
            request.identity.generation != self._generation
            or serial in self._pending
            or len(self._pending) >= 128
            or not 0 <= worker < 64
            or not 0 <= outstanding < 128
            or not 0 < received <= offered
        ):
            raise OwnerProtocolError("invalid broker diagnostic boundary")
        self._pending[serial] = {
            "scope": "candidate_broker_handoff",
            "generation": self._generation,
            "serial": serial,
            "request_id_sha256": sha256(request.identity.request_id.encode()).hexdigest(),
            "operation": request.operation,
            "worker_id": worker,
            "broker_outstanding_before": outstanding,
            "broker_frontend_received_ns": received,
            "broker_owner_offered_ns": offered,
        }

    def sent(self, serial: int, before: int, after: int) -> None:
        record = self._pending.pop(serial, None)
        if record is None or not 0 < before <= after:
            raise OwnerProtocolError("missing broker diagnostic packet or send boundary")
        record["broker_final_send_before_ns"] = before
        record["broker_final_send_after_ns"] = after
        self._sink.submit(record)

    def require_drained(self) -> None:
        if self._pending:
            raise OwnerProtocolError("broker diagnostic observations not drained")
