"""Bounded EMS price, frequency and four-scope Kill Switch guard."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import RLock
from types import MappingProxyType
from typing import Callable, Generic, Mapping, Protocol, TypeVar, cast

from ..domain import ExecutionInstruction
from ..domain._validation import require_nonnegative, require_positive, require_sha256, require_text


class ExecutionGuardError(RuntimeError):
    """Guard policy or evaluation identity is inconsistent."""


class KillSwitchLevel(str, Enum):
    ACCOUNT = "ACCOUNT"
    STRATEGY = "STRATEGY"
    SYMBOL = "SYMBOL"
    SIDE = "SIDE"


class KillSwitchAction(str, Enum):
    BLOCK_NEW = "BLOCK_NEW"
    CANCEL_OWNED_WORKING = "CANCEL_OWNED_WORKING"


class GuardDecisionCode(str, Enum):
    ALLOWED = "ALLOWED"
    KILL_SWITCH_BLOCK_NEW = "KILL_SWITCH_BLOCK_NEW"
    PRICE_SNAPSHOT_NOT_YET_VALID = "PRICE_SNAPSHOT_NOT_YET_VALID"
    PRICE_SNAPSHOT_STALE = "PRICE_SNAPSHOT_STALE"
    PRICE_OUTSIDE_BAND = "PRICE_OUTSIDE_BAND"
    PRICE_DEVIATION_EXCEEDED = "PRICE_DEVIATION_EXCEEDED"
    LIMIT_PRICE_EXCEEDS_PROTECTION = "LIMIT_PRICE_EXCEEDS_PROTECTION"
    ORDER_QUANTITY_LIMIT_EXCEEDED = "ORDER_QUANTITY_LIMIT_EXCEEDED"
    ORDER_NOTIONAL_LIMIT_EXCEEDED = "ORDER_NOTIONAL_LIMIT_EXCEEDED"
    FREQUENCY_LIMIT_EXCEEDED = "FREQUENCY_LIMIT_EXCEEDED"
    UNAPPROVED_STRATEGY = "UNAPPROVED_STRATEGY"
    UNAPPROVED_SYMBOL = "UNAPPROVED_SYMBOL"
    UNAPPROVED_SIDE = "UNAPPROVED_SIDE"


class FrequencyLimitScope(str, Enum):
    ACCOUNT = "ACCOUNT"
    STRATEGY = "STRATEGY"
    SYMBOL = "SYMBOL"
    SIDE = "SIDE"


def _freeze_positive_limits(values: Mapping[str, int], field_name: str) -> Mapping[str, int]:
    if not values:
        raise ExecutionGuardError(f"{field_name} must not be empty")
    copied: dict[str, int] = {}
    for key, value in values.items():
        canonical_key = require_text(key, f"{field_name} key", max_length=64)
        copied[canonical_key] = require_positive(value, f"{field_name}[{canonical_key}]")
    return cast(Mapping[str, int], MappingProxyType(copied))


@dataclass(frozen=True)
class ExecutionGuardPolicy:
    policy_id: str
    version: int
    policy_hash: str
    account_id: str
    frequency_window_ns: int
    account_order_limit: int
    strategy_order_limits: Mapping[str, int]
    symbol_order_limits: Mapping[str, int]
    side_order_limits: Mapping[str, int]
    max_price_deviation_bps: int
    max_order_quantity: int
    max_order_notional_ticks: int

    def __post_init__(self) -> None:
        require_text(self.policy_id, "execution guard policy_id")
        require_positive(self.version, "execution guard policy version")
        require_sha256(self.policy_hash, "execution guard policy_hash")
        require_text(self.account_id, "execution guard account_id")
        require_positive(self.frequency_window_ns, "guard frequency_window_ns")
        require_positive(self.account_order_limit, "guard account_order_limit")
        require_nonnegative(self.max_price_deviation_bps, "guard max_price_deviation_bps")
        require_positive(self.max_order_quantity, "guard max_order_quantity")
        require_positive(self.max_order_notional_ticks, "guard max_order_notional_ticks")
        if self.max_price_deviation_bps > 10_000:
            raise ExecutionGuardError("max_price_deviation_bps cannot exceed 10000")
        object.__setattr__(
            self,
            "strategy_order_limits",
            _freeze_positive_limits(self.strategy_order_limits, "strategy_order_limits"),
        )
        object.__setattr__(
            self,
            "symbol_order_limits",
            _freeze_positive_limits(self.symbol_order_limits, "symbol_order_limits"),
        )
        object.__setattr__(
            self,
            "side_order_limits",
            _freeze_positive_limits(self.side_order_limits, "side_order_limits"),
        )


@dataclass(frozen=True)
class GuardMarketSnapshot:
    snapshot_id: str
    symbol: str
    market: str
    reference_price_ticks: int
    minimum_price_ticks: int
    maximum_price_ticks: int
    captured_monotonic_ns: int
    expires_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.snapshot_id, "guard market snapshot_id")
        require_text(self.symbol, "guard market symbol", max_length=64)
        require_text(self.market, "guard market", max_length=64)
        require_positive(self.reference_price_ticks, "guard reference_price_ticks")
        require_nonnegative(self.minimum_price_ticks, "guard minimum_price_ticks")
        require_positive(self.maximum_price_ticks, "guard maximum_price_ticks")
        require_nonnegative(self.captured_monotonic_ns, "guard captured_monotonic_ns")
        require_positive(self.expires_monotonic_ns, "guard expires_monotonic_ns")
        if self.minimum_price_ticks > self.reference_price_ticks:
            raise ExecutionGuardError("minimum price exceeds reference price")
        if self.reference_price_ticks > self.maximum_price_ticks:
            raise ExecutionGuardError("reference price exceeds maximum price")
        if self.expires_monotonic_ns <= self.captured_monotonic_ns:
            raise ExecutionGuardError("market snapshot expiry must follow capture")


@dataclass(frozen=True)
class KillSwitchRule:
    switch_id: str
    account_id: str
    level: KillSwitchLevel
    selector: str
    action: KillSwitchAction
    control_command_id: str
    control_version: int
    reason_code: str
    operator_id: str
    approval_reference: str
    durable_fact_reference: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("switch_id", self.switch_id),
            ("account_id", self.account_id),
            ("selector", self.selector),
            ("control_command_id", self.control_command_id),
            ("reason_code", self.reason_code),
            ("operator_id", self.operator_id),
            ("approval_reference", self.approval_reference),
            ("durable_fact_reference", self.durable_fact_reference),
        ):
            require_text(value, f"kill switch {field_name}", max_length=128)
        require_positive(self.control_version, "kill switch control_version")
        if self.level is KillSwitchLevel.ACCOUNT and self.selector != self.account_id:
            raise ExecutionGuardError("account Kill Switch selector must equal account_id")


@dataclass(frozen=True)
class KillSwitchSnapshot:
    control_version: int
    rules: tuple[KillSwitchRule, ...] = ()

    def __post_init__(self) -> None:
        require_nonnegative(self.control_version, "kill switch snapshot control_version")
        rules = tuple(self.rules)
        if len({rule.switch_id for rule in rules}) != len(rules):
            raise ExecutionGuardError("Kill Switch snapshot repeats switch_id")
        if any(rule.control_version > self.control_version for rule in rules):
            raise ExecutionGuardError("Kill Switch rule is newer than its snapshot")
        object.__setattr__(self, "rules", rules)


@dataclass(frozen=True)
class GuardDecision:
    execution_id: str
    account_id: str
    accepted: bool
    code: GuardDecisionCode
    policy_hash: str
    evaluated_monotonic_ns: int
    market_snapshot_id: str
    matched_kill_switch_ids: tuple[str, ...] = ()
    exceeded_frequency_scopes: tuple[FrequencyLimitScope, ...] = ()


@dataclass(frozen=True)
class ExecutionGuardSnapshot:
    policy_hash: str
    evaluated_monotonic_ns: int
    account_window_depth: int
    strategy_window_depths: Mapping[str, int]
    symbol_window_depths: Mapping[str, int]
    side_window_depths: Mapping[str, int]
    accepted_execution_count: int
    decision_counts: Mapping[GuardDecisionCode, int]


class ExecutionRejectedAuditSink(Protocol):
    """Durable audit boundary invoked before a rejected admission returns."""

    def record_execution_rejected(
        self, instruction: ExecutionInstruction, decision: GuardDecision
    ) -> None: ...


AuthorizedResultT = TypeVar("AuthorizedResultT")


@dataclass(frozen=True)
class GuardedAdmissionResult(Generic[AuthorizedResultT]):
    decision: GuardDecision
    authorized_result: AuthorizedResultT | None = None


class ExecutionGuard:
    """Evaluate immutable facts and consume bounded rolling-window quota on pass."""

    def __init__(self, policy: ExecutionGuardPolicy) -> None:
        self._policy = policy
        self._account_times: deque[int] = deque()
        self._strategy_times: dict[str, deque[int]] = {
            key: deque() for key in policy.strategy_order_limits
        }
        self._symbol_times: dict[str, deque[int]] = {
            key: deque() for key in policy.symbol_order_limits
        }
        self._side_times: dict[str, deque[int]] = {key: deque() for key in policy.side_order_limits}
        self._accepted: dict[str, GuardDecision] = {}
        self._accepted_times: deque[tuple[int, str]] = deque()
        self._decision_counts = {code: 0 for code in GuardDecisionCode}
        self._lock = RLock()

    def evaluate(
        self,
        instruction: ExecutionInstruction,
        *,
        market: GuardMarketSnapshot,
        kill_switches: KillSwitchSnapshot,
        evaluated_monotonic_ns: int,
    ) -> GuardDecision:
        if evaluated_monotonic_ns < 0:
            raise ValueError("guard evaluation time must be nonnegative")
        if instruction.account_id != self._policy.account_id:
            raise ExecutionGuardError("instruction belongs to another guard account")
        if market.symbol != instruction.symbol or market.market != instruction.market:
            raise ExecutionGuardError("market snapshot does not match instruction")
        strategy_id = instruction.consumer_trace.strategy_id
        if strategy_id is None or strategy_id not in self._policy.strategy_order_limits:
            return self._reject(
                instruction, market, GuardDecisionCode.UNAPPROVED_STRATEGY, evaluated_monotonic_ns
            )
        if instruction.symbol not in self._policy.symbol_order_limits:
            return self._reject(
                instruction, market, GuardDecisionCode.UNAPPROVED_SYMBOL, evaluated_monotonic_ns
            )
        if instruction.side not in self._policy.side_order_limits:
            return self._reject(
                instruction, market, GuardDecisionCode.UNAPPROVED_SIDE, evaluated_monotonic_ns
            )
        matched = self._matched_switches(instruction, kill_switches)
        if matched:
            return self._reject(
                instruction,
                market,
                GuardDecisionCode.KILL_SWITCH_BLOCK_NEW,
                evaluated_monotonic_ns,
                matched=matched,
            )
        limit_code = self._order_limit_rejection(instruction)
        if limit_code is not None:
            return self._reject(instruction, market, limit_code, evaluated_monotonic_ns)
        price_code = self._price_rejection(instruction, market, evaluated_monotonic_ns)
        if price_code is not None:
            return self._reject(instruction, market, price_code, evaluated_monotonic_ns)
        with self._lock:
            self._prune(evaluated_monotonic_ns)
            existing = self._accepted.get(instruction.execution_id)
            if existing is not None:
                return existing
            queues_and_limits: tuple[tuple[FrequencyLimitScope, deque[int], int], ...] = (
                (
                    FrequencyLimitScope.ACCOUNT,
                    self._account_times,
                    self._policy.account_order_limit,
                ),
                (
                    FrequencyLimitScope.STRATEGY,
                    self._strategy_times[strategy_id],
                    self._policy.strategy_order_limits[strategy_id],
                ),
                (
                    FrequencyLimitScope.SYMBOL,
                    self._symbol_times[instruction.symbol],
                    self._policy.symbol_order_limits[instruction.symbol],
                ),
                (
                    FrequencyLimitScope.SIDE,
                    self._side_times[instruction.side],
                    self._policy.side_order_limits[instruction.side],
                ),
            )
            exceeded = tuple(
                scope for scope, queue, limit in queues_and_limits if len(queue) >= limit
            )
            if exceeded:
                return self._reject(
                    instruction,
                    market,
                    GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED,
                    evaluated_monotonic_ns,
                    exceeded_frequency_scopes=exceeded,
                )
            decision = GuardDecision(
                execution_id=instruction.execution_id,
                account_id=instruction.account_id,
                accepted=True,
                code=GuardDecisionCode.ALLOWED,
                policy_hash=self._policy.policy_hash,
                evaluated_monotonic_ns=evaluated_monotonic_ns,
                market_snapshot_id=market.snapshot_id,
            )
            for _, queue, _ in queues_and_limits:
                queue.append(evaluated_monotonic_ns)
            self._accepted[instruction.execution_id] = decision
            self._accepted_times.append((evaluated_monotonic_ns, instruction.execution_id))
            self._decision_counts[decision.code] += 1
            return decision

    def snapshot(self, *, evaluated_monotonic_ns: int) -> ExecutionGuardSnapshot:
        """Return bounded observability state after applying rolling-window expiry."""

        if evaluated_monotonic_ns < 0:
            raise ValueError("guard snapshot time must be nonnegative")
        with self._lock:
            self._prune(evaluated_monotonic_ns)
            return ExecutionGuardSnapshot(
                policy_hash=self._policy.policy_hash,
                evaluated_monotonic_ns=evaluated_monotonic_ns,
                account_window_depth=len(self._account_times),
                strategy_window_depths=MappingProxyType(
                    {key: len(queue) for key, queue in self._strategy_times.items()}
                ),
                symbol_window_depths=MappingProxyType(
                    {key: len(queue) for key, queue in self._symbol_times.items()}
                ),
                side_window_depths=MappingProxyType(
                    {key: len(queue) for key, queue in self._side_times.items()}
                ),
                accepted_execution_count=len(self._accepted),
                decision_counts=MappingProxyType(dict(self._decision_counts)),
            )

    def _matched_switches(
        self, instruction: ExecutionInstruction, snapshot: KillSwitchSnapshot
    ) -> tuple[str, ...]:
        strategy_id = instruction.consumer_trace.strategy_id
        matches: list[str] = []
        for rule in snapshot.rules:
            if (
                rule.action is not KillSwitchAction.BLOCK_NEW
                or rule.account_id != instruction.account_id
            ):
                continue
            selected = {
                KillSwitchLevel.ACCOUNT: instruction.account_id,
                KillSwitchLevel.STRATEGY: strategy_id,
                KillSwitchLevel.SYMBOL: instruction.symbol,
                KillSwitchLevel.SIDE: instruction.side,
            }[rule.level]
            if selected == rule.selector:
                matches.append(rule.switch_id)
        return tuple(sorted(matches))

    def _order_limit_rejection(self, instruction: ExecutionInstruction) -> GuardDecisionCode | None:
        if instruction.quantity > self._policy.max_order_quantity:
            return GuardDecisionCode.ORDER_QUANTITY_LIMIT_EXCEEDED
        notional_ticks = instruction.quantity * instruction.protection_price_ticks
        if notional_ticks > self._policy.max_order_notional_ticks:
            return GuardDecisionCode.ORDER_NOTIONAL_LIMIT_EXCEEDED
        return None

    def _price_rejection(
        self,
        instruction: ExecutionInstruction,
        market: GuardMarketSnapshot,
        now_ns: int,
    ) -> GuardDecisionCode | None:
        if now_ns < market.captured_monotonic_ns:
            return GuardDecisionCode.PRICE_SNAPSHOT_NOT_YET_VALID
        if now_ns >= market.expires_monotonic_ns:
            return GuardDecisionCode.PRICE_SNAPSHOT_STALE
        protection = instruction.protection_price_ticks
        if not market.minimum_price_ticks <= protection <= market.maximum_price_ticks:
            return GuardDecisionCode.PRICE_OUTSIDE_BAND
        deviation = abs(protection - market.reference_price_ticks) * 10_000
        if deviation > market.reference_price_ticks * self._policy.max_price_deviation_bps:
            return GuardDecisionCode.PRICE_DEVIATION_EXCEEDED
        limit = instruction.limit_price_ticks
        if limit is not None:
            if not market.minimum_price_ticks <= limit <= market.maximum_price_ticks:
                return GuardDecisionCode.PRICE_OUTSIDE_BAND
            if instruction.side in {"BUY", "MARGIN_BUY"} and limit > protection:
                return GuardDecisionCode.LIMIT_PRICE_EXCEEDS_PROTECTION
            if instruction.side in {"SELL", "SHORT_SELL"} and limit < protection:
                return GuardDecisionCode.LIMIT_PRICE_EXCEEDS_PROTECTION
        return None

    def _prune(self, now_ns: int) -> None:
        cutoff = now_ns - self._policy.frequency_window_ns
        all_queues: tuple[deque[int], ...] = (
            self._account_times,
            *self._strategy_times.values(),
            *self._symbol_times.values(),
            *self._side_times.values(),
        )
        for queue in all_queues:
            while queue and queue[0] <= cutoff:
                queue.popleft()
        while self._accepted_times and self._accepted_times[0][0] <= cutoff:
            _, execution_id = self._accepted_times.popleft()
            self._accepted.pop(execution_id, None)

    def _reject(
        self,
        instruction: ExecutionInstruction,
        market: GuardMarketSnapshot,
        code: GuardDecisionCode,
        evaluated_ns: int,
        *,
        matched: tuple[str, ...] = (),
        exceeded_frequency_scopes: tuple[FrequencyLimitScope, ...] = (),
    ) -> GuardDecision:
        decision = GuardDecision(
            execution_id=instruction.execution_id,
            account_id=instruction.account_id,
            accepted=False,
            code=code,
            policy_hash=self._policy.policy_hash,
            evaluated_monotonic_ns=evaluated_ns,
            market_snapshot_id=market.snapshot_id,
            matched_kill_switch_ids=matched,
            exceeded_frequency_scopes=exceeded_frequency_scopes,
        )
        with self._lock:
            self._decision_counts[decision.code] += 1
        return decision


class ExecutionGuardAdmissionGate:
    """Route rejections only to audit and invoke side effects only after a pass."""

    def __init__(
        self,
        guard: ExecutionGuard,
        rejected_audit_sink: ExecutionRejectedAuditSink,
    ) -> None:
        self._guard = guard
        self._rejected_audit_sink = rejected_audit_sink

    def admit(
        self,
        instruction: ExecutionInstruction,
        *,
        market: GuardMarketSnapshot,
        kill_switches: KillSwitchSnapshot,
        evaluated_monotonic_ns: int,
        on_authorized: Callable[[ExecutionInstruction, GuardDecision], AuthorizedResultT],
    ) -> GuardedAdmissionResult[AuthorizedResultT]:
        decision = self._guard.evaluate(
            instruction,
            market=market,
            kill_switches=kill_switches,
            evaluated_monotonic_ns=evaluated_monotonic_ns,
        )
        if not decision.accepted:
            self._rejected_audit_sink.record_execution_rejected(instruction, decision)
            return GuardedAdmissionResult(decision=decision)
        return GuardedAdmissionResult(
            decision=decision,
            authorized_result=on_authorized(instruction, decision),
        )
