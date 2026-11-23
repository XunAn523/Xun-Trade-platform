"""No-SDK QMT fake Adapter for profile and recovery contract tests only."""

from __future__ import annotations

from dataclasses import dataclass

from ...domain import (
    BrokerCapabilities,
    BrokerRecoveryEvidence,
    ExecutionInstruction,
    RecoveryPlan,
)
from ..base import (
    BrokerAdapterError,
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    SubmitAccepted,
)
from ..mapping import ActivatedVenueMapping


class FakeQmtBrokerAdapterError(BrokerAdapterError):
    """The fake profile is intentionally not a QMT order-entry implementation."""


@dataclass(frozen=True)
class FakeQmtBrokerAdapterSnapshot:
    started: bool
    observer_attached: bool
    submit_attempts: int
    cancel_attempts: int
    query_attempts: int
    recovery_attempts: int


class FakeQmtBrokerAdapter:
    """Read-only fake that proves QMT shape without importing or calling a SDK."""

    def __init__(self, mapping: ActivatedVenueMapping) -> None:
        if mapping.manifest.venue_id != "QMT":
            raise FakeQmtBrokerAdapterError("QMT fake Adapter requires a QMT mapping")
        self._mapping = mapping
        self._started = False
        self._observer: BrokerEventObserver | None = None
        self._submit_attempts = 0
        self._cancel_attempts = 0
        self._query_attempts = 0
        self._recovery_attempts = 0
        self._last_plan: RecoveryPlan | None = None

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        if ownership is None:
            raise FakeQmtBrokerAdapterError("QMT fake mapping lacks scope ownership")
        return ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._mapping.runtime.capabilities

    def start(self, observer: BrokerEventObserver) -> None:
        if self._started:
            raise FakeQmtBrokerAdapterError("QMT fake Adapter is already started")
        self._observer = observer
        self._started = True

    def stop(self) -> None:
        self._started = False
        self._observer = None

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        del instruction
        self._submit_attempts += 1
        raise FakeQmtBrokerAdapterError("QMT fake Adapter never submits an order")

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        del instruction
        self._cancel_attempts += 1
        raise FakeQmtBrokerAdapterError("QMT fake Adapter never submits a cancel")

    def query(self, request: BrokerQuery) -> QueryHandle:
        self._require_request_identity(
            account_id=request.account_id,
            venue_id=request.venue_id,
            adapter_instance_id=request.adapter_instance_id,
        )
        self._query_attempts += 1
        return QueryHandle(request.request_id, self.adapter_id, self.adapter_instance_id)

    def begin_recovery(self, plan: RecoveryPlan, observer: BrokerEventObserver) -> RecoveryHandle:
        self._require_request_identity(
            account_id=plan.account_id,
            venue_id=plan.venue_id,
            adapter_instance_id=plan.adapter_instance_id,
        )
        if observer is not self._observer:
            raise FakeQmtBrokerAdapterError("recovery must use the attached observer")
        self._recovery_attempts += 1
        self._last_plan = plan
        return RecoveryHandle(plan.recovery_run_id, self.adapter_id, self.adapter_instance_id)

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        plan = self._last_plan
        if plan is None or handle.recovery_run_id != plan.recovery_run_id:
            raise FakeQmtBrokerAdapterError("QMT fake recovery handle is unknown")
        return BrokerRecoveryEvidence(
            evidence_id=f"qmt-fake-evidence:{plan.recovery_run_id}",
            recovery_run_id=plan.recovery_run_id,
            account_id=plan.account_id,
            venue_id=plan.venue_id,
            adapter_instance_id=plan.adapter_instance_id,
            trading_day=plan.trading_day,
            callback_replay_complete=False,
            orders_complete=False,
            trades_complete=False,
            working_orders_complete=False,
            risk_snapshot_complete=False,
            negative_confirmation_complete=False,
            callback_gaps=plan.callback_gaps,
        )

    def snapshot(self) -> FakeQmtBrokerAdapterSnapshot:
        return FakeQmtBrokerAdapterSnapshot(
            started=self._started,
            observer_attached=self._observer is not None,
            submit_attempts=self._submit_attempts,
            cancel_attempts=self._cancel_attempts,
            query_attempts=self._query_attempts,
            recovery_attempts=self._recovery_attempts,
        )

    def _require_request_identity(
        self, *, account_id: str, venue_id: str, adapter_instance_id: str
    ) -> None:
        account = self._mapping.runtime.account_capabilities.account_id
        if (
            account_id != account
            or venue_id != "QMT"
            or adapter_instance_id != self.adapter_instance_id
        ):
            raise FakeQmtBrokerAdapterError("QMT fake request belongs to another profile")
