"""F6 EMT-only multi-account runtime isolation.

Each logical account owns a static, attested session-shard topology and an
explicit set of consumer accounts.  This supervisor is control-plane-only: it
starts/stops DISARMED runtime compositions and resolves future instructions to
their fixed lane, but it never invokes an Adapter itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol

from ..domain import BrokerEvent, CancelRequest, ExecutionInstruction, VenueOrderIdentity
from ..domain._validation import freeze_texts, require_nonnegative, require_text
from ..ems import (
    AccountSessionShardLane,
    AccountSessionShardPlan,
    AccountSessionShardRouter,
)


class EmtMultiAccountRuntimeErrorV2(RuntimeError):
    """An account-scoped runtime action would violate EMT isolation."""


class EmtAccountRuntimeStateV2(str, Enum):
    """Runtime status never implies ARM, READY, or automatic recovery."""

    REGISTERED_DISARMED = "REGISTERED_DISARMED"
    RUNNING_DISARMED = "RUNNING_DISARMED"
    FAILED_DISARMED = "FAILED_DISARMED"
    STOPPED_DISARMED = "STOPPED_DISARMED"


class EmtAccountRuntimePortV2(Protocol):
    """The account-local composition seam; it has no cross-account operation."""

    def start_disarmed(self) -> None:
        """Acquire only this account's local runtime resources while DISARMED."""

    def stop_disarmed(self) -> None:
        """Stop this account after future admission is closed."""

    def close_admission(self, *, reason: str) -> None:
        """Latch this account closed after a local integrity/capacity failure."""


@dataclass(frozen=True)
class EmtAccountRuntimeRegistrationV2:
    """One account's static session topology and permitted consumer accounts."""

    account_id: str
    consumer_account_ids: frozenset[str]
    shard_plan: AccountSessionShardPlan
    router: AccountSessionShardRouter
    runtime: EmtAccountRuntimePortV2

    def __post_init__(self) -> None:
        require_text(self.account_id, "EMT multi-account account_id", max_length=128)
        consumers = freeze_texts(
            self.consumer_account_ids,
            "EMT multi-account consumer_account_ids",
            max_items=1_024,
            max_length=128,
        )
        if not consumers:
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT multi-account registration requires consumer accounts"
            )
        if self.shard_plan.account_id != self.account_id or self.router.plan != self.shard_plan:
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT multi-account registration router does not match its account plan"
            )
        if any(binding.venue_id != "EMT" for binding in self.shard_plan.shards):
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT multi-account registration contains a non-EMT session shard"
            )
        object.__setattr__(self, "consumer_account_ids", consumers)


@dataclass(frozen=True)
class EmtAccountRuntimeSnapshotV2:
    """Payload-free, account-local operating state for health and evidence."""

    account_id: str
    state: EmtAccountRuntimeStateV2
    transition_count: int
    last_error_code: str | None = None

    def __post_init__(self) -> None:
        require_text(self.account_id, "EMT multi-account snapshot account_id", max_length=128)
        require_nonnegative(self.transition_count, "EMT multi-account transition_count")
        if self.last_error_code is not None:
            require_text(self.last_error_code, "EMT multi-account last_error_code", max_length=128)


class EmtMultiAccountRuntimeSupervisorV2:
    """Contain every lifecycle, consumer, route, and health failure to one account."""

    def __init__(self, registrations: Mapping[str, EmtAccountRuntimeRegistrationV2]) -> None:
        if not registrations:
            raise EmtMultiAccountRuntimeErrorV2("EMT multi-account runtime requires registrations")
        copied = dict(registrations)
        if set(copied) != {registration.account_id for registration in copied.values()}:
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT multi-account registration keys must equal account IDs"
            )
        scopes = tuple(
            (binding.venue_id, binding.adapter_instance_id, binding.adapter_client_scope)
            for registration in copied.values()
            for binding in registration.shard_plan.shards
        )
        if len(scopes) != len(set(scopes)):
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT multi-account registrations share one native session scope"
            )
        self._registrations = copied
        self._snapshots = {
            account_id: EmtAccountRuntimeSnapshotV2(
                account_id=account_id,
                state=EmtAccountRuntimeStateV2.REGISTERED_DISARMED,
                transition_count=0,
            )
            for account_id in copied
        }

    def start_account(self, account_id: str) -> EmtAccountRuntimeSnapshotV2:
        registration, previous = self._require_startable(account_id)
        try:
            registration.runtime.start_disarmed()
        except Exception as exc:
            failed = self._transition(
                previous, EmtAccountRuntimeStateV2.FAILED_DISARMED, type(exc).__name__
            )
            self._snapshots[account_id] = failed
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT account runtime start failed and remains DISARMED"
            ) from exc
        running = self._transition(previous, EmtAccountRuntimeStateV2.RUNNING_DISARMED, None)
        self._snapshots[account_id] = running
        return running

    def stop_account(self, account_id: str) -> EmtAccountRuntimeSnapshotV2:
        registration = self._registration_for(account_id)
        previous = self._snapshots[account_id]
        if previous.state is not EmtAccountRuntimeStateV2.RUNNING_DISARMED:
            raise EmtMultiAccountRuntimeErrorV2(
                "only a running DISARMED EMT account can be stopped"
            )
        try:
            registration.runtime.stop_disarmed()
        except Exception as exc:
            failed = self._transition(
                previous, EmtAccountRuntimeStateV2.FAILED_DISARMED, type(exc).__name__
            )
            self._snapshots[account_id] = failed
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT account runtime stop failed and remains DISARMED"
            ) from exc
        stopped = self._transition(previous, EmtAccountRuntimeStateV2.STOPPED_DISARMED, None)
        self._snapshots[account_id] = stopped
        return stopped

    def report_account_failure(
        self, *, account_id: str, reason_code: str
    ) -> EmtAccountRuntimeSnapshotV2:
        """Close only the affected account; never reroute to another account."""

        registration = self._registration_for(account_id)
        previous = self._snapshots[account_id]
        detail = require_text(reason_code, "EMT account failure reason", max_length=128)
        try:
            registration.runtime.close_admission(reason=detail)
        except Exception as exc:
            detail = f"close-admission:{type(exc).__name__}"
        failed = self._transition(previous, EmtAccountRuntimeStateV2.FAILED_DISARMED, detail)
        self._snapshots[account_id] = failed
        return failed

    def route_execution(self, instruction: ExecutionInstruction) -> AccountSessionShardLane:
        registration = self._require_running_consumer(
            account_id=instruction.account_id,
            consumer_account_id=instruction.consumer_trace.consumer_account_id,
        )
        return registration.router.route_execution(instruction)

    def route_cancel(
        self, request: CancelRequest, venue_order_identity: VenueOrderIdentity
    ) -> AccountSessionShardLane:
        """Route a cancel only through its original account and native scope."""

        registration = self._require_running_consumer(
            account_id=request.account_id,
            consumer_account_id=request.consumer_trace.consumer_account_id,
        )
        return registration.router.route_cancel(request, venue_order_identity)

    def route_event(self, event: BrokerEvent) -> AccountSessionShardLane:
        registration = self._require_running_account(event.account_id)
        return registration.router.route_event(event)

    def snapshot(self, account_id: str) -> EmtAccountRuntimeSnapshotV2:
        self._registration_for(account_id)
        return self._snapshots[account_id]

    def snapshots(self) -> tuple[EmtAccountRuntimeSnapshotV2, ...]:
        return tuple(self._snapshots[account_id] for account_id in sorted(self._snapshots))

    def _require_startable(
        self, account_id: str
    ) -> tuple[EmtAccountRuntimeRegistrationV2, EmtAccountRuntimeSnapshotV2]:
        registration = self._registration_for(account_id)
        snapshot = self._snapshots[account_id]
        if snapshot.state not in {
            EmtAccountRuntimeStateV2.REGISTERED_DISARMED,
            EmtAccountRuntimeStateV2.STOPPED_DISARMED,
        }:
            raise EmtMultiAccountRuntimeErrorV2(
                "EMT account runtime is not startable while active or failed"
            )
        return registration, snapshot

    def _require_running_consumer(
        self, *, account_id: str, consumer_account_id: str
    ) -> EmtAccountRuntimeRegistrationV2:
        registration = self._require_running_account(account_id)
        if consumer_account_id not in registration.consumer_account_ids:
            raise EmtMultiAccountRuntimeErrorV2(
                "consumer account is not authorized for this EMT account runtime"
            )
        return registration

    def _require_running_account(self, account_id: str) -> EmtAccountRuntimeRegistrationV2:
        registration = self._registration_for(account_id)
        if self._snapshots[account_id].state is not EmtAccountRuntimeStateV2.RUNNING_DISARMED:
            raise EmtMultiAccountRuntimeErrorV2("EMT account runtime is not running DISARMED")
        return registration

    def _registration_for(self, account_id: str) -> EmtAccountRuntimeRegistrationV2:
        try:
            return self._registrations[account_id]
        except KeyError as exc:
            raise EmtMultiAccountRuntimeErrorV2("EMT account runtime is not registered") from exc

    @staticmethod
    def _transition(
        snapshot: EmtAccountRuntimeSnapshotV2,
        state: EmtAccountRuntimeStateV2,
        last_error_code: str | None,
    ) -> EmtAccountRuntimeSnapshotV2:
        return EmtAccountRuntimeSnapshotV2(
            account_id=snapshot.account_id,
            state=state,
            transition_count=snapshot.transition_count + 1,
            last_error_code=last_error_code,
        )
