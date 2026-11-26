"""Committed-fact state machine for non-order EMT trading mutations.

Fund transfers, credit repayments/extensions and option exercise have a native
serial number but do not belong to the order C0--C7 lane.  Their state is
therefore reconstructed *after* the operation Journal has committed a fact.
This module never receives a vendor payload or runs on an SDK callback thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from .trading_catalog_v1 import EmtOperationLane
from .trading_module_v1 import EmtOperationCommandV1, EmtOperationReceiptV1


class EmtControlOperationStateError(RuntimeError):
    """A durable control-operation projection would become ambiguous."""


class EmtControlOperationState(str, Enum):
    PRE_NATIVE = "PRE_NATIVE"
    CALL_STARTED = "CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    NATIVE_REJECTED = "NATIVE_REJECTED"
    CALLBACK_OBSERVED = "CALLBACK_OBSERVED"
    AUTHORITATIVE_TERMINAL = "AUTHORITATIVE_TERMINAL"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


_EXPECTED_CALLBACKS = {
    "FundTransfer": "OnFundTransfer",
    "CreditCashRepay": "OnCreditCashRepay",
    "CreditCashRepaySpecifiedDebt": "OnCreditCashRepaySpecifiedDebt",
    "CreditExtendDebtDate": "OnCreditExtendDebtDate",
    "CreditQuotaTransfer": "OnCreditQuotaTransfer",
    "CreditMarginAvlTransfer": "OnCreditMarginAvlTransfer",
    "InsertOptionExerciseOrder": "OnOptionExerciseOrderEvent",
}
_TERMINAL = frozenset(
    {
        EmtControlOperationState.NATIVE_REJECTED,
        EmtControlOperationState.AUTHORITATIVE_TERMINAL,
        EmtControlOperationState.RECONCILE_REQUIRED,
    }
)


@dataclass(frozen=True)
class EmtControlOperationSnapshotV1:
    """Payload-free operation state keyed by scope, epoch and native serial."""

    operation_id: str
    official_name: str
    scope_id: str
    session_epoch: int
    state: EmtControlOperationState
    native_id: int | None
    expected_callback: str
    terminal_reason: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in _TERMINAL


class EmtControlOperationProjectorV1:
    """Apply committed control facts with no automatic native retry capability."""

    def __init__(self) -> None:
        self._by_operation: dict[str, EmtControlOperationSnapshotV1] = {}
        self._by_native: dict[tuple[str, int, int], str] = {}

    def record_pre_native(self, command: EmtOperationCommandV1) -> EmtControlOperationSnapshotV1:
        self._require_control(command)
        if command.operation_id in self._by_operation:
            raise EmtControlOperationStateError("duplicate control operation id")
        snapshot = EmtControlOperationSnapshotV1(
            operation_id=command.operation_id,
            official_name=command.official_name,
            scope_id=command.scope_id,
            session_epoch=command.session_epoch,
            state=EmtControlOperationState.PRE_NATIVE,
            native_id=None,
            expected_callback=_expected_callback(command.official_name),
        )
        self._by_operation[command.operation_id] = snapshot
        return snapshot

    def record_call_started(self, operation_id: str) -> EmtControlOperationSnapshotV1:
        snapshot = self._require_state(operation_id, EmtControlOperationState.PRE_NATIVE)
        return self._replace(snapshot, state=EmtControlOperationState.CALL_STARTED)

    def record_receipt(self, receipt: EmtOperationReceiptV1) -> EmtControlOperationSnapshotV1:
        snapshot = self._require_state(receipt.operation_id, EmtControlOperationState.CALL_STARTED)
        if receipt.official_name != snapshot.official_name:
            raise EmtControlOperationStateError("control receipt operation does not match intent")
        if receipt.lane is not EmtOperationLane.CONTROL_MUTATION:
            raise EmtControlOperationStateError("control projector received a non-control receipt")
        if not receipt.accepted:
            return self._replace(
                snapshot,
                state=EmtControlOperationState.NATIVE_REJECTED,
                terminal_reason=(
                    f"native_rejected:{receipt.rejection_code}"
                    if receipt.rejection_code is not None
                    else "native_rejected"
                ),
            )
        if receipt.native_id is None or receipt.native_id <= 0:
            raise EmtControlOperationStateError("accepted control receipt has no native serial")
        key = (snapshot.scope_id, snapshot.session_epoch, receipt.native_id)
        existing = self._by_native.get(key)
        if existing is not None and existing != receipt.operation_id:
            raise EmtControlOperationStateError("control native serial collides within scope epoch")
        self._by_native[key] = receipt.operation_id
        return self._replace(
            snapshot,
            state=EmtControlOperationState.NATIVE_ACCEPTED,
            native_id=receipt.native_id,
        )

    def observe_callback(
        self,
        *,
        scope_id: str,
        session_epoch: int,
        native_id: int,
        callback_name: str,
    ) -> EmtControlOperationSnapshotV1:
        """Associate an already-committed callback; callback alone is not terminal."""

        operation_id = self._by_native.get((scope_id, session_epoch, native_id))
        if operation_id is None:
            raise EmtControlOperationStateError("unowned control callback requires reconciliation")
        snapshot = self._require_state(operation_id, EmtControlOperationState.NATIVE_ACCEPTED)
        if callback_name != snapshot.expected_callback:
            raise EmtControlOperationStateError("control callback does not match operation kind")
        return self._replace(snapshot, state=EmtControlOperationState.CALLBACK_OBSERVED)

    def reconcile(
        self,
        operation_id: str,
        *,
        authoritative_complete: bool,
        result_code: str,
    ) -> EmtControlOperationSnapshotV1:
        """Close only from a vendor-authoritative query/statement result."""

        snapshot = self._require_state(operation_id, EmtControlOperationState.CALLBACK_OBSERVED)
        if not result_code or len(result_code) > 128:
            raise EmtControlOperationStateError("authoritative control result code is invalid")
        return self._replace(
            snapshot,
            state=(
                EmtControlOperationState.AUTHORITATIVE_TERMINAL
                if authoritative_complete
                else EmtControlOperationState.RECONCILE_REQUIRED
            ),
            terminal_reason=result_code,
        )

    def force_reconcile_required(self, operation_id: str, *, reason: str) -> EmtControlOperationSnapshotV1:
        snapshot = self._lookup(operation_id)
        if snapshot.terminal:
            return snapshot
        if not reason or len(reason) > 128:
            raise EmtControlOperationStateError("control reconcile reason is invalid")
        return self._replace(
            snapshot,
            state=EmtControlOperationState.RECONCILE_REQUIRED,
            terminal_reason=reason,
        )

    def snapshot(self, operation_id: str) -> EmtControlOperationSnapshotV1:
        return self._lookup(operation_id)

    def snapshots(self) -> Mapping[str, EmtControlOperationSnapshotV1]:
        return MappingProxyType(dict(self._by_operation))

    def _lookup(self, operation_id: str) -> EmtControlOperationSnapshotV1:
        try:
            return self._by_operation[operation_id]
        except KeyError as exc:
            raise EmtControlOperationStateError("unknown control operation") from exc

    def _require_state(
        self,
        operation_id: str,
        expected: EmtControlOperationState,
    ) -> EmtControlOperationSnapshotV1:
        snapshot = self._lookup(operation_id)
        if snapshot.state is not expected:
            raise EmtControlOperationStateError(
                f"control operation state must be {expected.value}, got {snapshot.state.value}"
            )
        return snapshot

    def _replace(self, snapshot: EmtControlOperationSnapshotV1, **changes: object) -> EmtControlOperationSnapshotV1:
        replacement = EmtControlOperationSnapshotV1(
            operation_id=snapshot.operation_id,
            official_name=snapshot.official_name,
            scope_id=snapshot.scope_id,
            session_epoch=snapshot.session_epoch,
            state=changes.get("state", snapshot.state),  # type: ignore[arg-type]
            native_id=changes.get("native_id", snapshot.native_id),  # type: ignore[arg-type]
            expected_callback=snapshot.expected_callback,
            terminal_reason=changes.get("terminal_reason", snapshot.terminal_reason),  # type: ignore[arg-type]
        )
        self._by_operation[replacement.operation_id] = replacement
        return replacement

    @staticmethod
    def _require_control(command: EmtOperationCommandV1) -> None:
        if command.spec.lane is not EmtOperationLane.CONTROL_MUTATION:
            raise EmtControlOperationStateError("operation is not a control-lane mutation")


def _expected_callback(official_name: str) -> str:
    try:
        return _EXPECTED_CALLBACKS[official_name]
    except KeyError as exc:
        # Catalogue configuration must make this impossible.  Keep a separate
        # guard so an added vendor operation cannot acquire implicit semantics.
        raise EmtControlOperationStateError(
            f"control operation has no explicit callback contract: {official_name}"
        ) from exc
