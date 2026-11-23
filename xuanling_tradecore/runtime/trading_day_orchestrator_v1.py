"""M5 account-scoped day-boundary orchestration; it never opens a session."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..domain import AccountLifecycle, TradingDayEpoch
from ..domain._validation import require_nonnegative, require_sha256, require_text


class TradingDayOrchestrationErrorV1(RuntimeError):
    """A new trading day cannot be activated without a safe account boundary."""


@dataclass(frozen=True)
class TradingDayRolloverEvidenceV1:
    account_id: str
    previous_day: TradingDayEpoch
    next_day: TradingDayEpoch
    calendar_evidence_hash: str
    adapter_evidence_hash: str
    recovery_certificate_hash: str
    observed_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.account_id, "rollover account_id")
        if self.previous_day == self.next_day:
            raise TradingDayOrchestrationErrorV1("rollover must change the trading day")
        for name, value in (
            ("calendar_evidence_hash", self.calendar_evidence_hash),
            ("adapter_evidence_hash", self.adapter_evidence_hash),
            ("recovery_certificate_hash", self.recovery_certificate_hash),
        ):
            require_sha256(value, f"rollover {name}")
        require_nonnegative(self.observed_wall_ns, "rollover observed_wall_ns")


class TradingDayRolloverFactPortV1(Protocol):
    def append_rollover(self, evidence: TradingDayRolloverEvidenceV1) -> None:
        """Durably append the approved day boundary before exposing the next day."""


@dataclass(frozen=True)
class AccountTradingDayStateV1:
    account_id: str
    trading_day: TradingDayEpoch
    lifecycle: AccountLifecycle
    adapter_session_active: bool
    unresolved_work_count: int

    def __post_init__(self) -> None:
        require_text(self.account_id, "trading day account_id")
        require_nonnegative(self.unresolved_work_count, "trading day unresolved_work_count")


class TradingDayOrchestratorV1:
    """Advance each account independently only after all prior-day work is closed."""

    def __init__(self, fact_port: TradingDayRolloverFactPortV1) -> None:
        self._fact_port = fact_port
        self._states: dict[str, AccountTradingDayStateV1] = {}

    def register(self, state: AccountTradingDayStateV1) -> None:
        if state.account_id in self._states:
            raise TradingDayOrchestrationErrorV1("trading day account is already registered")
        self._states[state.account_id] = state

    def state_for(self, account_id: str) -> AccountTradingDayStateV1:
        try:
            return self._states[account_id]
        except KeyError as exc:
            raise TradingDayOrchestrationErrorV1("trading day account is not registered") from exc

    def rollover(self, evidence: TradingDayRolloverEvidenceV1) -> AccountTradingDayStateV1:
        state = self.state_for(evidence.account_id)
        if state.lifecycle is not AccountLifecycle.DISARMED:
            raise TradingDayOrchestrationErrorV1("trading day rollover requires DISARMED account")
        if state.adapter_session_active:
            raise TradingDayOrchestrationErrorV1(
                "active Adapter session blocks trading day rollover"
            )
        if state.unresolved_work_count:
            raise TradingDayOrchestrationErrorV1("unresolved work blocks trading day rollover")
        if state.trading_day != evidence.previous_day:
            raise TradingDayOrchestrationErrorV1(
                "rollover previous day does not match account state"
            )
        self._fact_port.append_rollover(evidence)
        next_state = AccountTradingDayStateV1(
            account_id=state.account_id,
            trading_day=evidence.next_day,
            lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            unresolved_work_count=0,
        )
        self._states[state.account_id] = next_state
        return next_state
