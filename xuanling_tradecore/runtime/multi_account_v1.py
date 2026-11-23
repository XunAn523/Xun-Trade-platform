"""M5 account-scoped runtime supervisor without cross-account failover."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol

from ..domain._validation import require_nonnegative, require_text


class MultiAccountRuntimeErrorV1(RuntimeError):
    """A per-account runtime transition cannot be safely applied."""


class AccountRuntimeStateV1(str, Enum):
    REGISTERED_DISARMED = "REGISTERED_DISARMED"
    RUNNING_DISARMED = "RUNNING_DISARMED"
    FAILED_DISARMED = "FAILED_DISARMED"
    STOPPED_DISARMED = "STOPPED_DISARMED"


class AccountRuntimePortV1(Protocol):
    def start_disarmed(self) -> None:
        """Acquire the account's own resources without authorizing new orders."""

    def stop_disarmed(self) -> None:
        """Release the account's own resources after stopping its admission."""


@dataclass(frozen=True)
class AccountRuntimeRegistrationV1:
    account_id: str
    venue_id: str
    adapter_instance_id: str
    runtime: AccountRuntimePortV1

    def __post_init__(self) -> None:
        require_text(self.account_id, "multi-account runtime account_id")
        require_text(self.venue_id, "multi-account runtime venue_id")
        require_text(self.adapter_instance_id, "multi-account runtime adapter_instance_id")


@dataclass(frozen=True)
class AccountRuntimeSnapshotV1:
    account_id: str
    venue_id: str
    adapter_instance_id: str
    state: AccountRuntimeStateV1
    transition_count: int
    last_error_code: str | None = None

    def __post_init__(self) -> None:
        require_text(self.account_id, "multi-account snapshot account_id")
        require_text(self.venue_id, "multi-account snapshot venue_id")
        require_text(self.adapter_instance_id, "multi-account snapshot adapter_instance_id")
        require_nonnegative(self.transition_count, "multi-account snapshot transition_count")
        if self.last_error_code is not None:
            require_text(self.last_error_code, "multi-account snapshot last_error_code")


class MultiAccountRuntimeSupervisorV1:
    """Contain each startup failure to its account; never imply cross-account readiness."""

    def __init__(self, registrations: Mapping[str, AccountRuntimeRegistrationV1]) -> None:
        if not registrations:
            raise MultiAccountRuntimeErrorV1("multi-account runtime requires registrations")
        if set(registrations) != {item.account_id for item in registrations.values()}:
            raise MultiAccountRuntimeErrorV1("runtime registration keys must equal account IDs")
        scopes = {(item.venue_id, item.adapter_instance_id) for item in registrations.values()}
        if len(scopes) != len(registrations):
            raise MultiAccountRuntimeErrorV1(
                "multiple accounts cannot share one active venue Adapter instance"
            )
        self._registrations = dict(registrations)
        self._snapshots = {
            account_id: AccountRuntimeSnapshotV1(
                account_id=registration.account_id,
                venue_id=registration.venue_id,
                adapter_instance_id=registration.adapter_instance_id,
                state=AccountRuntimeStateV1.REGISTERED_DISARMED,
                transition_count=0,
            )
            for account_id, registration in registrations.items()
        }

    def start_account(self, account_id: str) -> AccountRuntimeSnapshotV1:
        registration, snapshot = self._require_startable(account_id)
        try:
            registration.runtime.start_disarmed()
        except Exception as exc:
            failed = self._transition(
                snapshot, AccountRuntimeStateV1.FAILED_DISARMED, type(exc).__name__
            )
            self._snapshots[account_id] = failed
            raise MultiAccountRuntimeErrorV1(
                "account runtime start failed and remains DISARMED"
            ) from exc
        running = self._transition(snapshot, AccountRuntimeStateV1.RUNNING_DISARMED, None)
        self._snapshots[account_id] = running
        return running

    def stop_account(self, account_id: str) -> AccountRuntimeSnapshotV1:
        registration = self._require_registration(account_id)
        snapshot = self._snapshots[account_id]
        if snapshot.state is not AccountRuntimeStateV1.RUNNING_DISARMED:
            raise MultiAccountRuntimeErrorV1("only a running DISARMED account can be stopped")
        try:
            registration.runtime.stop_disarmed()
        except Exception as exc:
            failed = self._transition(
                snapshot, AccountRuntimeStateV1.FAILED_DISARMED, type(exc).__name__
            )
            self._snapshots[account_id] = failed
            raise MultiAccountRuntimeErrorV1(
                "account runtime stop failed and remains DISARMED"
            ) from exc
        stopped = self._transition(snapshot, AccountRuntimeStateV1.STOPPED_DISARMED, None)
        self._snapshots[account_id] = stopped
        return stopped

    def snapshot(self, account_id: str) -> AccountRuntimeSnapshotV1:
        self._require_registration(account_id)
        return self._snapshots[account_id]

    def snapshots(self) -> tuple[AccountRuntimeSnapshotV1, ...]:
        return tuple(self._snapshots[key] for key in sorted(self._snapshots))

    def _require_registration(self, account_id: str) -> AccountRuntimeRegistrationV1:
        try:
            return self._registrations[account_id]
        except KeyError as exc:
            raise MultiAccountRuntimeErrorV1("account runtime is not registered") from exc

    def _require_startable(
        self, account_id: str
    ) -> tuple[AccountRuntimeRegistrationV1, AccountRuntimeSnapshotV1]:
        registration = self._require_registration(account_id)
        snapshot = self._snapshots[account_id]
        if snapshot.state not in {
            AccountRuntimeStateV1.REGISTERED_DISARMED,
            AccountRuntimeStateV1.STOPPED_DISARMED,
        }:
            raise MultiAccountRuntimeErrorV1(
                "account runtime is not startable while active or failed"
            )
        return registration, snapshot

    @staticmethod
    def _transition(
        snapshot: AccountRuntimeSnapshotV1,
        state: AccountRuntimeStateV1,
        last_error_code: str | None,
    ) -> AccountRuntimeSnapshotV1:
        return AccountRuntimeSnapshotV1(
            account_id=snapshot.account_id,
            venue_id=snapshot.venue_id,
            adapter_instance_id=snapshot.adapter_instance_id,
            state=state,
            transition_count=snapshot.transition_count + 1,
            last_error_code=last_error_code,
        )
