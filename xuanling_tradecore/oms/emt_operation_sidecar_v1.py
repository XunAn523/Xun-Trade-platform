"""Replay-safe OMS sidecar for committed EMT control-operation facts.

The existing order sidecar owns C0--C7 order facts.  This companion owns only
fund/credit/option-control facts after their Journal commit, making a complete
transaction/account OMS possible without adding a dependency to either hot
path.  It stores no vendor request structure or sensitive business payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Mapping


class EmtOperationSidecarError(RuntimeError):
    """A committed EMT operation fact is inconsistent or unrecoverable."""


class EmtOperationFactKindV1(str, Enum):
    INTENT = "INTENT"
    PRE_NATIVE = "PRE_NATIVE"
    CALL_STARTED = "CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    NATIVE_REJECTED = "NATIVE_REJECTED"
    CALLBACK_OBSERVED = "CALLBACK_OBSERVED"
    AUTHORITATIVE_TERMINAL = "AUTHORITATIVE_TERMINAL"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


class EmtOperationOmsStateV1(str, Enum):
    INTENT = "INTENT"
    PRE_NATIVE = "PRE_NATIVE"
    CALL_STARTED = "CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    NATIVE_REJECTED = "NATIVE_REJECTED"
    CALLBACK_OBSERVED = "CALLBACK_OBSERVED"
    AUTHORITATIVE_TERMINAL = "AUTHORITATIVE_TERMINAL"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


_STATE_FOR_FACT = {
    EmtOperationFactKindV1.INTENT: EmtOperationOmsStateV1.INTENT,
    EmtOperationFactKindV1.PRE_NATIVE: EmtOperationOmsStateV1.PRE_NATIVE,
    EmtOperationFactKindV1.CALL_STARTED: EmtOperationOmsStateV1.CALL_STARTED,
    EmtOperationFactKindV1.NATIVE_ACCEPTED: EmtOperationOmsStateV1.NATIVE_ACCEPTED,
    EmtOperationFactKindV1.NATIVE_REJECTED: EmtOperationOmsStateV1.NATIVE_REJECTED,
    EmtOperationFactKindV1.CALLBACK_OBSERVED: EmtOperationOmsStateV1.CALLBACK_OBSERVED,
    EmtOperationFactKindV1.AUTHORITATIVE_TERMINAL: EmtOperationOmsStateV1.AUTHORITATIVE_TERMINAL,
    EmtOperationFactKindV1.RECONCILE_REQUIRED: EmtOperationOmsStateV1.RECONCILE_REQUIRED,
}
_EXPECTED_PREDECESSOR = {
    EmtOperationFactKindV1.INTENT: None,
    EmtOperationFactKindV1.PRE_NATIVE: EmtOperationOmsStateV1.INTENT,
    EmtOperationFactKindV1.CALL_STARTED: EmtOperationOmsStateV1.PRE_NATIVE,
    EmtOperationFactKindV1.NATIVE_ACCEPTED: EmtOperationOmsStateV1.CALL_STARTED,
    EmtOperationFactKindV1.NATIVE_REJECTED: EmtOperationOmsStateV1.CALL_STARTED,
    EmtOperationFactKindV1.CALLBACK_OBSERVED: EmtOperationOmsStateV1.NATIVE_ACCEPTED,
    EmtOperationFactKindV1.AUTHORITATIVE_TERMINAL: EmtOperationOmsStateV1.CALLBACK_OBSERVED,
    EmtOperationFactKindV1.RECONCILE_REQUIRED: None,
}
_TERMINAL = frozenset(
    {
        EmtOperationOmsStateV1.NATIVE_REJECTED,
        EmtOperationOmsStateV1.AUTHORITATIVE_TERMINAL,
        EmtOperationOmsStateV1.RECONCILE_REQUIRED,
    }
)


@dataclass(frozen=True)
class EmtOperationCommittedFactV1:
    """One payload-free Journal fact; ``position`` is source-stream order."""

    scope_id: str
    session_epoch: int
    position: int
    operation_id: str
    official_name: str
    kind: EmtOperationFactKindV1
    payload_hash: str
    native_id: int | None = None
    terminal_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.scope_id or not self.operation_id or not self.official_name:
            raise EmtOperationSidecarError("operation fact identity is invalid")
        if self.session_epoch <= 0 or self.position <= 0:
            raise EmtOperationSidecarError("operation fact epoch or position is invalid")
        if len(self.payload_hash) != 64 or any(char not in "0123456789abcdef" for char in self.payload_hash):
            raise EmtOperationSidecarError("operation fact payload hash must be SHA-256")
        if self.native_id is not None and self.native_id <= 0:
            raise EmtOperationSidecarError("operation fact native serial is invalid")
        if self.terminal_reason is not None and (not self.terminal_reason or len(self.terminal_reason) > 128):
            raise EmtOperationSidecarError("operation fact terminal reason is invalid")

    @property
    def fingerprint(self) -> str:
        payload = "|".join(
            (
                self.scope_id,
                str(self.session_epoch),
                str(self.position),
                self.operation_id,
                self.official_name,
                self.kind.value,
                self.payload_hash,
                str(self.native_id or 0),
                self.terminal_reason or "",
            )
        )
        return sha256(payload.encode("ascii")).hexdigest()


@dataclass(frozen=True)
class EmtOperationOmsSnapshotV1:
    operation_id: str
    official_name: str
    scope_id: str
    session_epoch: int
    state: EmtOperationOmsStateV1
    native_id: int | None
    source_position: int
    terminal_reason: str | None

    @property
    def terminal(self) -> bool:
        return self.state in _TERMINAL


class EmtOperationOmsSidecarV1:
    """Single-threaded committed-fact projector, deliberately adapter-free."""

    def __init__(self) -> None:
        self._snapshots: dict[str, EmtOperationOmsSnapshotV1] = {}
        self._source_positions: dict[tuple[str, int], int] = {}
        self._position_fingerprints: dict[tuple[str, int, int], str] = {}

    def apply(self, fact: EmtOperationCommittedFactV1) -> EmtOperationOmsSnapshotV1:
        stream_key = (fact.scope_id, fact.session_epoch)
        position_key = (*stream_key, fact.position)
        existing_fingerprint = self._position_fingerprints.get(position_key)
        if existing_fingerprint is not None:
            if existing_fingerprint != fact.fingerprint:
                raise EmtOperationSidecarError("source position replays with a different operation fact")
            return self.snapshot(fact.operation_id)
        previous_position = self._source_positions.get(stream_key, 0)
        if fact.position <= previous_position:
            raise EmtOperationSidecarError("operation sidecar source position is not monotonic")
        current = self._snapshots.get(fact.operation_id)
        self._validate_transition(current, fact)
        snapshot = EmtOperationOmsSnapshotV1(
            operation_id=fact.operation_id,
            official_name=fact.official_name,
            scope_id=fact.scope_id,
            session_epoch=fact.session_epoch,
            state=_STATE_FOR_FACT[fact.kind],
            native_id=fact.native_id if fact.native_id is not None else (current.native_id if current else None),
            source_position=fact.position,
            terminal_reason=fact.terminal_reason,
        )
        self._snapshots[fact.operation_id] = snapshot
        self._source_positions[stream_key] = fact.position
        self._position_fingerprints[position_key] = fact.fingerprint
        return snapshot

    def snapshot(self, operation_id: str) -> EmtOperationOmsSnapshotV1:
        try:
            return self._snapshots[operation_id]
        except KeyError as exc:
            raise EmtOperationSidecarError("unknown operation sidecar snapshot") from exc

    def snapshots(self) -> Mapping[str, EmtOperationOmsSnapshotV1]:
        return MappingProxyType(dict(self._snapshots))

    @staticmethod
    def _validate_transition(
        current: EmtOperationOmsSnapshotV1 | None,
        fact: EmtOperationCommittedFactV1,
    ) -> None:
        expected = _EXPECTED_PREDECESSOR[fact.kind]
        if current is None:
            if expected is not None:
                raise EmtOperationSidecarError("operation sidecar fact has no committed predecessor")
            return
        if (
            current.scope_id != fact.scope_id
            or current.session_epoch != fact.session_epoch
            or current.official_name != fact.official_name
        ):
            raise EmtOperationSidecarError("operation fact violates its scope/epoch/interface identity")
        if current.terminal:
            raise EmtOperationSidecarError("operation sidecar cannot transition a terminal operation")
        if fact.kind is EmtOperationFactKindV1.RECONCILE_REQUIRED:
            return
        if current.state is not expected:
            raise EmtOperationSidecarError("operation sidecar fact has an invalid state transition")
        if fact.kind in {EmtOperationFactKindV1.NATIVE_ACCEPTED, EmtOperationFactKindV1.CALLBACK_OBSERVED}:
            if fact.native_id is None:
                raise EmtOperationSidecarError("accepted/callback operation fact requires native serial")
        if current.native_id is not None and fact.native_id is not None and current.native_id != fact.native_id:
            raise EmtOperationSidecarError("operation sidecar native serial changed after acceptance")
