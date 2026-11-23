"""M1-01 OMS core with durable-idempotency and reservation boundaries.

This module is deliberately vendor-neutral and has no filesystem, database,
network, SDK, or broker-adapter dependency.  The journal protocol is an
explicit port: the in-memory implementation exists for contract tests only;
M1-03 will bind the same ``append_batch`` boundary to PersistentHotJournal.
"""

from __future__ import annotations

import json
import secrets
import time
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from enum import Enum
from hashlib import sha256
from threading import RLock
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Mapping, Protocol, runtime_checkable

import msgspec

if TYPE_CHECKING:
    from .recovery import OmsRecoverySeedV1

from ..domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerEventPositionV2,
    ConsumerStrategyBinding,
    ConsumerTrace,
    DecisionContext,
    ExecutionInstruction,
    OmsOrder,
    OmsOrderState,
    RedisStreamIdV1,
    RiskSnapshot,
    TradingDayEpoch,
)
from ..domain._validation import (
    DomainValidationError,
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)

OMS_INTENT_HASH_SCHEMA_VERSION = 1


def _empty_text_mapping() -> Mapping[str, str]:
    """Create a precisely typed immutable default for bounded text metadata."""

    return MappingProxyType({})


class OmsOrderBookError(DomainValidationError):
    """The OMS cannot safely accept or apply a business fact."""


class ReservationRiskError(OmsOrderBookError):
    """A reservation would exceed the current, verified risk snapshot."""


class OrderBookStateError(OmsOrderBookError):
    """A requested order transition is not proven by the current order state."""


class IntentAdmissionOutcome(str, Enum):
    """The only outcomes of a new business intent at the OMS boundary."""

    ACCEPTED = "ACCEPTED"
    IDEMPOTENT = "IDEMPOTENT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


class ReservationStatus(str, Enum):
    """Whether a reservation currently reduces locally available resources."""

    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"


class OmsFactKind(str, Enum):
    """M1 logical facts; PersistentHotJournal defines their V2 encoding later."""

    ORDER_ATTRIBUTED = "ORDER_ATTRIBUTED"
    RESERVATION_CREATED = "RESERVATION_CREATED"
    ORDER_STATE_CHANGED = "ORDER_STATE_CHANGED"
    BROKER_TRADE_APPLIED = "BROKER_TRADE_APPLIED"
    RESERVATION_ADJUSTED = "RESERVATION_ADJUSTED"
    RESERVATION_RELEASED = "RESERVATION_RELEASED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    CALL_STARTED = "CALL_STARTED"
    CLIENT_ORDER_REF_ALLOCATED = "CLIENT_ORDER_REF_ALLOCATED"
    EMT_CONSUMER_SUBMIT_DISPOSITION = "EMT_CONSUMER_SUBMIT_DISPOSITION"
    NATIVE_CALL_ENTERED = "NATIVE_CALL_ENTERED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    NATIVE_OBSERVED_FROM_CALLBACK = "NATIVE_OBSERVED_FROM_CALLBACK"
    NATIVE_RETURN_OBSERVED = "NATIVE_RETURN_OBSERVED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    DISPATCH_EXPIRED_BEFORE_NATIVE = "DISPATCH_EXPIRED_BEFORE_NATIVE"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_STARTED = "CANCEL_STARTED"
    CANCEL_NATIVE_ACCEPTED = "CANCEL_NATIVE_ACCEPTED"
    CANCEL_SUBMIT_UNKNOWN = "CANCEL_SUBMIT_UNKNOWN"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    CALLBACK_GAP_DETECTED = "CALLBACK_GAP_DETECTED"
    CALLBACK_GAP_COVERED = "CALLBACK_GAP_COVERED"
    ACCOUNT_STATUS_CHANGED = "ACCOUNT_STATUS_CHANGED"
    TRADING_DAY_EPOCH_SEAL = "TRADING_DAY_EPOCH_SEAL"
    BROKER_EVENT = "BROKER_EVENT"
    ORDER_REJECTED = "ORDER_REJECTED"
    RECOVERY_ELIGIBILITY_EVALUATED = "RECOVERY_ELIGIBILITY_EVALUATED"
    RECOVERY_MATCHED = "RECOVERY_MATCHED"
    RECOVERY_LOCAL_CRASH_BEFORE_NATIVE = "RECOVERY_LOCAL_CRASH_BEFORE_NATIVE"
    RECOVERY_NEGATIVE_CONFIRMED = "RECOVERY_NEGATIVE_CONFIRMED"
    RECOVERY_REMOTE_ROUTE_REBOUND = "RECOVERY_REMOTE_ROUTE_REBOUND"
    RECOVERY_REMOTE_UNATTRIBUTED = "RECOVERY_REMOTE_UNATTRIBUTED"
    RECOVERY_CANCEL_MATCHED = "RECOVERY_CANCEL_MATCHED"
    RECOVERY_REMOTE_AUTHORITY_APPLIED = "RECOVERY_REMOTE_AUTHORITY_APPLIED"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    RECOVERY_RUN_STARTED = "RECOVERY_RUN_STARTED"
    RECOVERY_COVERAGE_VERIFIED = "RECOVERY_COVERAGE_VERIFIED"
    RECOVERY_REMOTE_RISK_APPLIED = "RECOVERY_REMOTE_RISK_APPLIED"
    RECOVERY_RUN_COMPLETED = "RECOVERY_RUN_COMPLETED"
    ACCOUNT_STREAM_RESET = "ACCOUNT_STREAM_RESET"
    ORDER_ATTACHED_EXTERNAL = "ORDER_ATTACHED_EXTERNAL"


class OmsOrderEventKind(str, Enum):
    """Immutable OMS events visible to later consumer/projector layers."""

    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_STATE_CHANGED = "ORDER_STATE_CHANGED"
    TRADE_APPLIED = "TRADE_APPLIED"


@dataclass(frozen=True)
class OmsOrderIntent:
    """A fully bound, fixed-point strategy intent before OMS allocates IDs.

    It intentionally contains no consumer request/correlation id, broker client
    reference, or venue identity.  Those values cannot influence persistent
    business idempotency.
    """

    consumer_trace: ConsumerTrace
    trading_day: TradingDayEpoch
    symbol: str
    market: str
    side: str
    price_type: str
    business_type: str
    quantity: int
    protection_price_ticks: int
    limit_price_ticks: int | None = None
    execution_parameters: Mapping[str, str] = field(default_factory=_empty_text_mapping)

    def __post_init__(self) -> None:
        require_text(self.symbol, "OMS intent symbol", max_length=64)
        require_text(self.market, "OMS intent market", max_length=64)
        require_text(self.side, "OMS intent side", max_length=64)
        require_text(self.price_type, "OMS intent price_type", max_length=64)
        require_text(self.business_type, "OMS intent business_type", max_length=64)
        require_positive(self.quantity, "OMS intent quantity")
        require_nonnegative(self.protection_price_ticks, "OMS intent protection price")
        if self.limit_price_ticks is not None:
            require_nonnegative(self.limit_price_ticks, "OMS intent limit price")
        if self.consumer_trace.strategy_id is None or self.consumer_trace.strategy_order_id is None:
            raise OmsOrderBookError("M1 OMS intent requires strategy identity")
        object.__setattr__(
            self,
            "execution_parameters",
            freeze_text_mapping(self.execution_parameters, "OMS execution parameters"),
        )

    @property
    def account_id(self) -> str:
        """Return the account only from immutable consumer lineage."""

        return self.consumer_trace.account_id


@dataclass(frozen=True)
class ReservationRequirement:
    """Conservative fixed-point resources held while an order can still execute."""

    symbol: str
    cash_minor: int = 0
    margin_minor: int = 0
    sellable_quantity: int = 0
    borrowable_quantity: int = 0
    currency: str = "CNY"

    def __post_init__(self) -> None:
        require_text(self.symbol, "reservation symbol", max_length=64)
        require_text(self.currency, "reservation currency", max_length=16)
        if self.currency != "CNY":
            raise OmsOrderBookError("M1 reservation currency must be CNY")
        for name, value in self.resources.items():
            require_nonnegative(value, f"reservation {name}")

    @property
    def resources(self) -> Mapping[str, int]:
        """Return the four fixed resource values with no mutable backing map."""

        return {
            "cash_minor": self.cash_minor,
            "margin_minor": self.margin_minor,
            "sellable_quantity": self.sellable_quantity,
            "borrowable_quantity": self.borrowable_quantity,
        }

    @property
    def is_empty(self) -> bool:
        """Return whether no resource is held; only released records may be empty."""

        return not any(self.resources.values())

    def scaled_to_leaves(
        self, *, total_quantity: int, leaves_quantity: int
    ) -> ReservationRequirement:
        """Conservatively scale all resources using ceiling division for open leaves."""

        require_positive(total_quantity, "reservation total quantity")
        require_nonnegative(leaves_quantity, "reservation leaves quantity")
        if leaves_quantity > total_quantity:
            raise OmsOrderBookError("reservation leaves exceed order quantity")
        if leaves_quantity == 0:
            return ReservationRequirement.zero_for(self.symbol, currency=self.currency)
        return ReservationRequirement(
            symbol=self.symbol,
            currency=self.currency,
            cash_minor=_ceil_scaled(self.cash_minor, leaves_quantity, total_quantity),
            margin_minor=_ceil_scaled(self.margin_minor, leaves_quantity, total_quantity),
            sellable_quantity=_ceil_scaled(self.sellable_quantity, leaves_quantity, total_quantity),
            borrowable_quantity=_ceil_scaled(
                self.borrowable_quantity, leaves_quantity, total_quantity
            ),
        )

    @classmethod
    def zero_for(cls, symbol: str, *, currency: str = "CNY") -> ReservationRequirement:
        """Return a zero requirement used only after a terminal release."""

        return cls(symbol=symbol, currency=currency)


@dataclass(frozen=True)
class Reservation:
    """One reservation and the immutable decision/risk evidence behind it."""

    reservation_id: str
    oms_order_id: str
    account_id: str
    trading_day: TradingDayEpoch
    risk_snapshot_id: str
    decision_context_id: str
    original_requirement: ReservationRequirement
    active_requirement: ReservationRequirement
    status: ReservationStatus
    reservation_revision: int
    created_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.reservation_id, "reservation id")
        require_text(self.oms_order_id, "reservation OMS order id")
        require_text(self.account_id, "reservation account id")
        require_text(self.risk_snapshot_id, "reservation risk snapshot id")
        require_text(self.decision_context_id, "reservation decision context id")
        require_nonnegative(self.reservation_revision, "reservation revision")
        require_nonnegative(self.created_monotonic_ns, "reservation created time")
        if self.original_requirement.symbol != self.active_requirement.symbol:
            raise OmsOrderBookError("reservation resource symbols differ")
        if self.status is ReservationStatus.RELEASED and any(
            self.active_requirement.resources.values()
        ):
            raise OmsOrderBookError("released reservation retains active resources")

    @property
    def is_active(self) -> bool:
        """Return whether this reservation reduces current local availability."""

        return self.status is ReservationStatus.ACTIVE


@dataclass(frozen=True)
class OrderBookOrder:
    """The complete immutable in-memory OMS view of a single accepted order."""

    order: OmsOrder
    execution: ExecutionInstruction
    decision_context: DecisionContext
    reservation: Reservation
    filled_quantity: int = 0
    trade_keys: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        require_nonnegative(self.filled_quantity, "OMS filled quantity")
        if self.execution.oms_order_id != self.order.oms_order_id:
            raise OmsOrderBookError("execution belongs to another OMS order")
        if self.execution.account_id != self.order.account_id:
            raise OmsOrderBookError("execution account differs from OMS order")
        if self.reservation.oms_order_id != self.order.oms_order_id:
            raise OmsOrderBookError("reservation belongs to another OMS order")
        if self.reservation.account_id != self.order.account_id:
            raise OmsOrderBookError("reservation account differs from OMS order")
        if self.filled_quantity > self.execution.quantity:
            raise OmsOrderBookError("OMS filled quantity exceeds instruction quantity")
        trade_keys = frozenset(self.trade_keys)
        for trade_key in trade_keys:
            require_text(trade_key, "OMS trade key")
        object.__setattr__(self, "trade_keys", trade_keys)

    @property
    def leaves_quantity(self) -> int:
        """Return non-negative unfilled quantity from the immutable instruction."""

        return self.execution.quantity - self.filled_quantity


@dataclass(frozen=True)
class OmsJournalFact:
    """A bounded logical OMS fact awaiting an atomic Journal commit."""

    fact_id: str
    kind: OmsFactKind
    account_id: str
    oms_order_id: str | None
    execution_id: str | None
    attributes: Mapping[str, str] = field(default_factory=_empty_text_mapping)
    recovery_payload: bytes | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.recovery_payload is not None and (
            type(self.recovery_payload) is not bytes or not 0 < len(self.recovery_payload) <= 65_536
        ):
            raise OmsOrderBookError("OMS recovery payload must be bounded immutable bytes")
        require_text(self.fact_id, "OMS fact id")
        require_text(self.account_id, "OMS fact account")
        if self.oms_order_id is not None:
            require_text(self.oms_order_id, "OMS fact order id")
        if self.execution_id is not None:
            require_text(self.execution_id, "OMS fact execution id")
        object.__setattr__(
            self, "attributes", freeze_text_mapping(self.attributes, "OMS fact attributes")
        )


@dataclass(frozen=True)
class OmsJournalCommit:
    """The verified result of one atomic logical fact batch.

    ``first_lsn``/``final_lsn`` retain the internal monotonic fact sequence.
    Redis-backed commits additionally carry the actual Stream ID and one
    ordinal position per member; callers must never derive that position from
    the integer LSN or from the Stream ID display text.
    """

    journal_id: str
    first_lsn: int
    final_lsn: int
    fact_count: int
    stream_id: RedisStreamIdV1 | None = None
    member_positions: tuple[ConsumerEventPositionV2, ...] = ()

    def __post_init__(self) -> None:
        require_text(self.journal_id, "OMS Journal id")
        require_nonnegative(self.first_lsn, "OMS Journal first LSN")
        require_nonnegative(self.final_lsn, "OMS Journal final LSN")
        require_positive(self.fact_count, "OMS Journal fact count")
        if self.final_lsn < self.first_lsn:
            raise OmsOrderBookError("OMS Journal final LSN precedes first LSN")
        if self.final_lsn - self.first_lsn + 1 != self.fact_count:
            raise OmsOrderBookError("OMS Journal LSN range does not match fact count")
        positions = tuple(self.member_positions)
        if self.stream_id is None:
            if positions:
                raise OmsOrderBookError("OMS Journal positions require a Redis Stream ID")
        else:
            if len(positions) != self.fact_count:
                raise OmsOrderBookError("OMS Journal position count does not match fact count")
            if any(position.stream_id != self.stream_id for position in positions):
                raise OmsOrderBookError("OMS Journal member belongs to another Redis Stream ID")
            if tuple(position.fact_ordinal for position in positions) != tuple(
                range(self.fact_count)
            ):
                raise OmsOrderBookError("OMS Journal member ordinals are not canonical")
        object.__setattr__(self, "member_positions", positions)


@runtime_checkable
class OmsFactJournal(Protocol):
    """The only synchronous fact boundary between OMS admission and memory state."""

    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        """Atomically commit facts or raise without exposing a partial batch."""

        ...


@dataclass(frozen=True)
class OmsOrderEvent:
    """An immutable event derived only after its underlying fact batch committed."""

    event_id: str
    kind: OmsOrderEventKind
    account_id: str
    oms_order_id: str
    execution_id: str
    order_state: OmsOrderState
    order_revision: int
    journal_id: str
    journal_lsn: int
    source_key: str
    journal_position: ConsumerEventPositionV2 | None = None

    def __post_init__(self) -> None:
        require_text(self.event_id, "OMS event id")
        require_text(self.account_id, "OMS event account")
        require_text(self.oms_order_id, "OMS event order id")
        require_text(self.execution_id, "OMS event execution id")
        require_nonnegative(self.order_revision, "OMS event order revision")
        require_text(self.journal_id, "OMS event Journal id")
        require_nonnegative(self.journal_lsn, "OMS event Journal LSN")
        require_text(self.source_key, "OMS event source key")
        if self.journal_position is not None and self.journal_position.fact_ordinal < 0:
            raise OmsOrderBookError("OMS event Journal position is invalid")


@dataclass(frozen=True)
class IntentAdmission:
    """The deterministic result of idempotency lookup and, if needed, acceptance."""

    outcome: IntentAdmissionOutcome
    intent_hash: str
    order: OrderBookOrder
    event: OmsOrderEvent | None
    journal_commit: OmsJournalCommit | None
    replayed_event: OmsOrderEvent | None = None

    def __post_init__(self) -> None:
        require_sha256(self.intent_hash, "intent admission hash")
        if self.outcome is IntentAdmissionOutcome.ACCEPTED and (
            self.event is None or self.journal_commit is None
        ):
            raise OmsOrderBookError("accepted intent requires event and Journal commit")
        if self.outcome is IntentAdmissionOutcome.IDEMPOTENT and (
            self.event is not None or self.journal_commit is not None
        ):
            raise OmsOrderBookError("idempotent replay must not create a new fact")
        if self.replayed_event is not None and (
            self.outcome is not IntentAdmissionOutcome.IDEMPOTENT
            or self.replayed_event.oms_order_id != self.order.order.oms_order_id
            or self.replayed_event.account_id != self.order.order.account_id
            or self.replayed_event.execution_id != self.order.execution.execution_id
        ):
            raise OmsOrderBookError("idempotent replay evidence belongs to another result")


class InMemoryOmsFactJournal(OmsFactJournal):
    """A deterministic test double; it is not a durable Journal implementation."""

    def __init__(self, journal_id: str = "m1-01-in-memory") -> None:
        require_text(journal_id, "in-memory OMS Journal id")
        self._journal_id = journal_id
        self._records: list[OmsJournalFact] = []
        self._lock = RLock()

    @property
    def records(self) -> tuple[OmsJournalFact, ...]:
        """Return committed test facts in logical LSN order."""

        with self._lock:
            return tuple(self._records)

    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        """Commit a whole test batch under one method call."""

        if not facts:
            raise OmsOrderBookError("OMS Journal batch cannot be empty")
        batch = tuple(facts)
        if len({fact.fact_id for fact in batch}) != len(batch):
            raise OmsOrderBookError("OMS Journal batch repeats a fact id")
        with self._lock:
            first_lsn = len(self._records) + 1
            self._records.extend(batch)
            return OmsJournalCommit(
                journal_id=self._journal_id,
                first_lsn=first_lsn,
                final_lsn=len(self._records),
                fact_count=len(batch),
            )


class ReservationBook:
    """Account-local conservative reservations; callers hold the account admission lock."""

    def __init__(self) -> None:
        self._reservations: dict[str, Reservation] = {}
        # Callers hold the account lock.  Keep the risk totals in step with
        # committed reservations instead of scanning every historical order
        # on each new admission (quadratic work in a sustained signal run).
        self._active_cash_minor = 0
        self._active_margin_minor = 0
        self._active_by_symbol: dict[str, tuple[int, int]] = {}

    def reservation_for(self, oms_order_id: str) -> Reservation:
        """Return one known reservation without exposing mutable state."""

        return self._reservations[oms_order_id]

    def all_reservations(self) -> tuple[Reservation, ...]:
        """Return a stable snapshot for diagnostics and deterministic tests."""

        return tuple(self._reservations.values())

    def forget_released(self, oms_order_id: str) -> None:
        """Remove only a cold-covered released member, never an active risk hold."""
        reservation = self.reservation_for(oms_order_id)
        if reservation.is_active or not reservation.active_requirement.is_empty:
            raise OmsOrderBookError("active reservation cannot be forgotten")
        del self._reservations[oms_order_id]

    def ensure_available(self, requirement: ReservationRequirement, snapshot: RiskSnapshot) -> None:
        """Validate capacity before the Journal commit while account serialization is held."""

        sellable_reserved, borrowable_reserved = self._active_by_symbol.get(
            requirement.symbol, (0, 0)
        )
        if self._active_cash_minor + requirement.cash_minor > snapshot.available_cash_minor:
            raise ReservationRiskError("cash reservation exceeds the verified risk snapshot")
        if self._active_margin_minor + requirement.margin_minor > snapshot.available_margin_minor:
            raise ReservationRiskError("margin reservation exceeds the verified risk snapshot")
        sellable = snapshot.sellable_quantities.get(requirement.symbol, 0)
        if sellable_reserved + requirement.sellable_quantity > sellable:
            raise ReservationRiskError("sellable reservation exceeds the verified risk snapshot")
        borrowable = snapshot.borrowable_quantities.get(requirement.symbol, 0)
        if borrowable_reserved + requirement.borrowable_quantity > borrowable:
            raise ReservationRiskError("borrowable reservation exceeds the verified risk snapshot")

    def create(self, reservation: Reservation) -> Reservation:
        """Apply an already-Journal-committed new reservation exactly once."""

        existing = self._reservations.get(reservation.oms_order_id)
        if existing is not None:
            if existing != reservation:
                raise OmsOrderBookError("OMS order already has another reservation")
            return existing
        self._apply_active_delta(None, reservation)
        self._reservations[reservation.oms_order_id] = reservation
        return reservation

    def adjust_to_leaves(
        self, *, oms_order_id: str, total_quantity: int, leaves_quantity: int
    ) -> Reservation:
        """Return a reservation adjustment; caller journals it before replacing state."""

        reservation = self.reservation_for(oms_order_id)
        if not reservation.is_active:
            return reservation
        scaled = reservation.original_requirement.scaled_to_leaves(
            total_quantity=total_quantity,
            leaves_quantity=leaves_quantity,
        )
        if leaves_quantity == 0:
            return replace(
                reservation,
                active_requirement=scaled,
                status=ReservationStatus.RELEASED,
                reservation_revision=reservation.reservation_revision + 1,
            )
        return replace(
            reservation,
            active_requirement=scaled,
            reservation_revision=reservation.reservation_revision + 1,
        )

    def replace_committed(self, reservation: Reservation) -> Reservation:
        """Install an already-Journal-committed replacement without recomputing risk."""

        previous = self.reservation_for(reservation.oms_order_id)
        if reservation.reservation_revision <= previous.reservation_revision:
            raise OmsOrderBookError("reservation revision did not advance")
        if (
            reservation.reservation_id != previous.reservation_id
            or reservation.account_id != previous.account_id
            or reservation.original_requirement != previous.original_requirement
        ):
            raise OmsOrderBookError("reservation replacement changes immutable identity")
        self._apply_active_delta(previous, reservation)
        self._reservations[reservation.oms_order_id] = reservation
        return reservation

    def _apply_active_delta(self, previous: Reservation | None, current: Reservation) -> None:
        """Install only committed resource changes under the account lock."""

        # These already-validated immutable requirements are the same inputs
        # used by the historical arithmetic. Do not allocate/validate throwaway
        # zero or aggregate domain objects on every create/check/release.
        old = previous.active_requirement if previous is not None and previous.is_active else None
        new = current.active_requirement if current.is_active else None
        symbol = current.active_requirement.symbol
        if old is not None and old.symbol != symbol:
            raise OmsOrderBookError("reservation replacement changes resource symbol")
        cash_minor = (
            self._active_cash_minor
            + (new.cash_minor if new else 0)
            - (old.cash_minor if old else 0)
        )
        margin_minor = (
            self._active_margin_minor
            + (new.margin_minor if new else 0)
            - (old.margin_minor if old else 0)
        )
        sellable, borrowable = self._active_by_symbol.get(symbol, (0, 0))
        sellable += (new.sellable_quantity if new else 0) - (old.sellable_quantity if old else 0)
        borrowable += (new.borrowable_quantity if new else 0) - (
            old.borrowable_quantity if old else 0
        )
        if min(cash_minor, margin_minor, sellable, borrowable) < 0:
            raise OmsOrderBookError("reservation active totals would become negative")
        self._active_cash_minor = cash_minor
        self._active_margin_minor = margin_minor
        if sellable or borrowable:
            self._active_by_symbol[symbol] = (sellable, borrowable)
        else:
            self._active_by_symbol.pop(symbol, None)


class OrderBook:
    """A per-account-linearized OMS order book with Journal-first mutation semantics."""

    def __init__(
        self,
        *,
        journal: OmsFactJournal,
        id_factory: Callable[[], str] | None = None,
        persist_recovery_seed: bool = False,
    ) -> None:
        self._journal = journal
        self._new_id = id_factory or _uuid7
        self._accounts: dict[str, _AccountBook] = {}
        self._accounts_lock = RLock()
        self._persist_recovery_seed = persist_recovery_seed
        self._recovered_lsn = 0
        self._recovered_journal_id: str | None = None
        self._live_started = False

    @property
    def journal(self) -> OmsFactJournal:
        """Return the exact fact journal for checked runtime composition."""

        return self._journal

    @property
    def persists_recovery_seed(self) -> bool:
        """Whether new admissions contain the canonical recovery seed."""

        return self._persist_recovery_seed

    def admit(
        self,
        *,
        intent: OmsOrderIntent,
        account_binding: ConsumerAccountBinding,
        strategy_binding: ConsumerStrategyBinding,
        attribution_binding: AttributionBinding,
        risk_snapshot: RiskSnapshot,
        decision_context: DecisionContext,
        reservation_requirement: ReservationRequirement,
        now_monotonic_ns: int,
    ) -> IntentAdmission:
        """Accept once, replay equal intents, or record a non-mutating conflict.

        All lookup, risk validation, reservation preparation, Journal commit,
        and in-memory installation execute while holding only this account's
        lock.  This deliberately removes a check-then-freeze race without
        imposing cross-account serialization.
        """

        require_nonnegative(now_monotonic_ns, "OMS admission time")
        self._live_started = True
        self._validate_admission_inputs(
            intent,
            account_binding,
            strategy_binding,
            attribution_binding,
            risk_snapshot,
            decision_context,
            reservation_requirement,
            now_monotonic_ns,
        )
        intent_hash = intent_hash_for(intent)
        account = self._account(intent.account_id)
        return self._admit_to_account(
            account=account,
            intent=intent,
            intent_hash=intent_hash,
            attribution_binding=attribution_binding,
            risk_snapshot=risk_snapshot,
            decision_context=decision_context,
            reservation_requirement=reservation_requirement,
            now_monotonic_ns=now_monotonic_ns,
        )

    def _admit_to_account(
        self,
        *,
        account: _AccountBook,
        intent: OmsOrderIntent,
        intent_hash: str,
        attribution_binding: AttributionBinding,
        risk_snapshot: RiskSnapshot,
        decision_context: DecisionContext,
        reservation_requirement: ReservationRequirement,
        now_monotonic_ns: int,
    ) -> IntentAdmission:
        """Private continuation after this call's full immutable-input validation.

        No reusable authorization token/cache is returned. Both public admit
        paths validate their exact inputs before reaching this continuation.
        Mutable deduplication and reservation capacity remain under account.lock;
        every fact is committed before a hold, order or event is published.
        """
        key = OmsIntentKey(
            account_id=intent.account_id,
            strategy_id=intent.consumer_trace.strategy_id or "",
            strategy_order_id=intent.consumer_trace.strategy_order_id or "",
        )
        with account.lock:
            existing = account.deduplication.get(key)
            if existing is not None:
                if existing.intent_hash == intent_hash:
                    return IntentAdmission(
                        outcome=IntentAdmissionOutcome.IDEMPOTENT,
                        intent_hash=intent_hash,
                        order=existing.order,
                        event=None,
                        journal_commit=None,
                    )
                conflict_fact = self._fact(
                    OmsFactKind.IDEMPOTENCY_CONFLICT,
                    account_id=intent.account_id,
                    oms_order_id=existing.order.order.oms_order_id,
                    execution_id=existing.order.execution.execution_id,
                    attributes={
                        "existing_intent_hash": existing.intent_hash,
                        "incoming_intent_hash": intent_hash,
                        "strategy_id": key.strategy_id,
                        "strategy_order_id": key.strategy_order_id,
                    },
                )
                commit = self._commit((conflict_fact,))
                return IntentAdmission(
                    outcome=IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT,
                    intent_hash=intent_hash,
                    order=existing.order,
                    event=None,
                    journal_commit=commit,
                )
            account.reservations.ensure_available(reservation_requirement, risk_snapshot)
            order = self._new_order(
                intent=intent,
                intent_hash=intent_hash,
                decision_context=decision_context,
                risk_snapshot=risk_snapshot,
                reservation_requirement=reservation_requirement,
                now_monotonic_ns=now_monotonic_ns,
            )
            facts = self._acceptance_facts(order, attribution_binding)
            commit = self._commit(facts)
            account.reservations.create(order.reservation)
            account.orders[order.order.oms_order_id] = order
            account.deduplication[key] = _IntentRecord(intent_hash=intent_hash, order=order)
            event = self._event(
                OmsOrderEventKind.ORDER_ACCEPTED,
                order,
                commit,
                source_key=f"oms:{order.order.oms_order_id}:accepted",
            )
            account.events.append(event)
            return IntentAdmission(
                outcome=IntentAdmissionOutcome.ACCEPTED,
                intent_hash=intent_hash,
                order=order,
                event=event,
                journal_commit=commit,
            )

    def mark_working(self, *, oms_order_id: str) -> OmsOrderEvent | None:
        """Record the idempotent ACCEPTED→WORKING transition after durable facts."""

        return self._transition(oms_order_id=oms_order_id, target=OmsOrderState.WORKING)

    def reject(self, *, oms_order_id: str) -> OmsOrderEvent | None:
        """Record a terminal rejection and release the still-active reservation."""

        return self._transition(oms_order_id=oms_order_id, target=OmsOrderState.REJECTED)

    def cancel(self, *, oms_order_id: str) -> OmsOrderEvent | None:
        """Record a terminal cancellation and release the still-active reservation."""

        return self._transition(oms_order_id=oms_order_id, target=OmsOrderState.CANCELED)

    def request_cancel(
        self,
        *,
        oms_order_id: str,
        cancel_request_id: str | None = None,
        reason: str | None = None,
    ) -> OmsOrderEvent | None:
        """Durably request cancellation without claiming a venue terminal state.

        The EMS bridge may submit a native cancel only after it proves the
        exact owned venue identity.  A later broker callback is the only path
        that may transition this order to ``CANCELED``.
        """

        if (cancel_request_id is None) != (reason is None):
            raise OmsOrderBookError("cancel request identity and reason must be supplied together")
        attributes = None
        if cancel_request_id is not None and reason is not None:
            attributes = {
                "cancel_request_id": require_text(cancel_request_id, "cancel request id", max_length=128),
                "reason": require_text(reason, "cancel request reason", max_length=256),
            }
        return self._transition(
            oms_order_id=oms_order_id,
            target=OmsOrderState.CANCEL_PENDING,
            first_fact_kind=OmsFactKind.CANCEL_REQUESTED,
            additional_state_attributes=attributes,
        )

    def apply_trade(
        self,
        *,
        oms_order_id: str,
        trade_key: str,
        quantity: int,
    ) -> OmsOrderEvent | None:
        """Apply one unique trade exactly once and shrink the open reservation."""

        self._live_started = True
        require_text(trade_key, "OMS trade key")
        require_positive(quantity, "OMS trade quantity")
        account = self._account_for_order(oms_order_id)
        with account.lock:
            current = self._require_order(account, oms_order_id)
            if trade_key in current.trade_keys:
                return None
            if current.order.state not in {
                OmsOrderState.ACCEPTED,
                OmsOrderState.WORKING,
                OmsOrderState.CANCEL_PENDING,
                OmsOrderState.PARTIALLY_FILLED,
            }:
                raise OrderBookStateError("trade cannot be applied to a terminal or unknown order")
            if quantity > current.leaves_quantity:
                raise OrderBookStateError("trade quantity exceeds OMS order leaves")
            filled_quantity = current.filled_quantity + quantity
            leaves_quantity = current.execution.quantity - filled_quantity
            target = (
                OmsOrderState.FILLED if leaves_quantity == 0 else OmsOrderState.PARTIALLY_FILLED
            )
            updated_order = replace(
                current.order,
                state=target,
                order_revision=current.order.order_revision + 1,
            )
            updated_reservation = account.reservations.adjust_to_leaves(
                oms_order_id=oms_order_id,
                total_quantity=current.execution.quantity,
                leaves_quantity=leaves_quantity,
            )
            updated = replace(
                current,
                order=updated_order,
                reservation=updated_reservation,
                filled_quantity=filled_quantity,
                trade_keys=current.trade_keys | {trade_key},
            )
            facts = (
                self._fact(
                    OmsFactKind.BROKER_TRADE_APPLIED,
                    account_id=current.order.account_id,
                    oms_order_id=oms_order_id,
                    execution_id=current.execution.execution_id,
                    attributes={"trade_key": trade_key, "quantity": str(quantity)},
                ),
                self._reservation_fact(updated),
            )
            commit = self._commit(facts)
            account.reservations.replace_committed(updated_reservation)
            account.orders[oms_order_id] = updated
            event = self._event(
                OmsOrderEventKind.TRADE_APPLIED,
                updated,
                commit,
                source_key=f"trade:{trade_key}",
            )
            account.events.append(event)
            return event

    def order(self, oms_order_id: str) -> OrderBookOrder:
        """Return one immutable order snapshot or raise a non-disclosing local error."""

        account = self._account_for_order(oms_order_id)
        with account.lock:
            return self._require_order(account, oms_order_id)

    def events(self, account_id: str) -> tuple[OmsOrderEvent, ...]:
        """Return one account's immutable OMS event sequence."""

        account = self._account(account_id)
        with account.lock:
            return tuple(account.events)

    def events_from(self, account_id: str, start_index: int) -> tuple[OmsOrderEvent, ...]:
        """Copy only the committed suffix needed by an incremental projector."""

        require_nonnegative(start_index, "OMS event start index")
        account = self._account(account_id)
        with account.lock:
            if start_index > len(account.events):
                raise OmsOrderBookError("OMS event start index exceeds committed history")
            if isinstance(account.events, list):
                return tuple(account.events[start_index:])
            from itertools import islice

            return tuple(islice(account.events, start_index, None))

    def last_order_event(self, oms_order_id: str) -> OmsOrderEvent:
        """Read a committed order tail without copying the account event history."""
        account = self._account_for_order(oms_order_id)
        with account.lock:
            for event in reversed(account.events):
                if event.oms_order_id == oms_order_id:
                    return event
        raise OmsOrderBookError("OMS order has no committed event")

    def event_count(self, account_id: str) -> int:
        """Return committed event cardinality without copying event history."""

        account = self._account(account_id)
        with account.lock:
            return len(account.events)

    def last_event(self, account_id: str) -> OmsOrderEvent | None:
        """Inspect the committed event tail without copying the full event history."""

        account = self._account(account_id)
        with account.lock:
            return account.events[-1] if account.events else None

    def order_count(self, account_id: str) -> int:
        """Return the committed order cardinality without copying account state."""

        account = self._account(account_id)
        with account.lock:
            return len(account.orders)

    def reservations(self, account_id: str) -> tuple[Reservation, ...]:
        """Return one account's reservation snapshot for recovery/projector tests."""

        account = self._account(account_id)
        with account.lock:
            return account.reservations.all_reservations()

    def export_recovery_checkpoint(self) -> bytes:
        """Encode a quiescent replay worker, never the live admission instance."""
        import msgspec

        from .recovery_checkpoint import EventCheckpointRow, OmsCheckpointState, OrderCheckpointRow

        if self._live_started or not self._persist_recovery_seed or not self._recovered_journal_id:
            raise OmsOrderBookError("checkpoint export requires a quiescent replay worker")
        rows: list[OrderCheckpointRow] = []
        events: list[EventCheckpointRow] = []
        for account in self._accounts.values():
            for record in account.deduplication.values():
                current = account.orders[record.order.order.oms_order_id]
                rows.append(
                    OrderCheckpointRow(
                        self._checkpoint_admission_seed(record.order),
                        current.order.state.value,
                        current.order.order_revision,
                        current.filled_quantity,
                        tuple(sorted(current.trade_keys)),
                        current.reservation.reservation_revision,
                    )
                )
            events.extend(
                EventCheckpointRow(
                    event.event_id,
                    event.kind.value,
                    event.account_id,
                    event.oms_order_id,
                    event.execution_id,
                    event.order_state.value,
                    event.order_revision,
                    event.journal_lsn,
                    event.source_key,
                )
                for event in account.events
            )
        return msgspec.json.encode(
            OmsCheckpointState(
                1,
                self._recovered_journal_id,
                self._recovered_lsn,
                tuple(rows),
                tuple(sorted(events, key=lambda event: event.journal_lsn)),
            )
        )

    def _checkpoint_admission_seed(self, initial: OrderBookOrder) -> bytes:
        """Cold checkpoint encoding hook; live admission never calls this."""
        from .recovery import OmsRecoverySeedV1

        return OmsRecoverySeedV1.build(initial).encoded

    def _decode_recovery_admission(self, encoded: bytes) -> OmsRecoverySeedV1:
        from .recovery import OmsRecoverySeedV1

        return OmsRecoverySeedV1.decode_and_verify(encoded)

    def restore_recovery_checkpoint(self, encoded: bytes, *, expected_journal_id: str) -> None:
        """Validate all checkpoint state before atomically installing a startup view.

        Physical integrity/publication is the checkpoint store's responsibility.
        Journal suffix replay must still start at watermark + 1. This operation
        does not append facts, acquire resources a second time, or call adapters.
        """
        from .recovery_checkpoint import decode_checkpoint

        if (
            self._live_started
            or self._recovered_lsn
            or self._accounts
            or not self._persist_recovery_seed
        ):
            raise OmsOrderBookError("checkpoint restore requires a fresh startup worker")
        state = decode_checkpoint(encoded)
        if state.journal_id != expected_journal_id:
            raise OmsOrderBookError("checkpoint Journal identity mismatch")
        accounts: dict[str, _AccountBook] = {}
        orders: dict[str, OrderBookOrder] = {}
        execution_ids: set[str] = set()
        for row in state.orders:
            initial = self._decode_recovery_admission(row.admission_seed).order
            intent = OmsOrderIntent(
                consumer_trace=initial.order.consumer_trace,
                trading_day=initial.order.trading_day,
                symbol=initial.execution.symbol,
                market=initial.execution.market,
                side=initial.execution.side,
                price_type=initial.execution.price_type,
                business_type=initial.execution.business_type,
                quantity=initial.execution.quantity,
                protection_price_ticks=initial.execution.protection_price_ticks,
                limit_price_ticks=initial.execution.limit_price_ticks,
                execution_parameters=initial.execution.execution_parameters,
            )
            target = OmsOrderState(row.state)
            quantity = initial.execution.quantity
            trades = frozenset(row.trade_keys)
            terminal = target in {
                OmsOrderState.FILLED,
                OmsOrderState.CANCELED,
                OmsOrderState.REJECTED,
            }
            if (
                row.order_revision < 0
                or row.reservation_revision < 0
                or intent_hash_for(intent) != initial.order.intent_hash
                or len(trades) != len(row.trade_keys)
                or not 0 <= row.filled_quantity <= quantity
                or bool(trades) != bool(row.filled_quantity)
                or len(trades) > row.filled_quantity
                or (target is OmsOrderState.FILLED) != (row.filled_quantity == quantity)
                or (target is OmsOrderState.PARTIALLY_FILLED and row.filled_quantity == 0)
                or (
                    target
                    in {OmsOrderState.ACCEPTED, OmsOrderState.WORKING, OmsOrderState.REJECTED}
                    and row.filled_quantity != 0
                )
                or target
                not in {
                    OmsOrderState.ACCEPTED,
                    OmsOrderState.WORKING,
                    OmsOrderState.PARTIALLY_FILLED,
                    OmsOrderState.CANCEL_PENDING,
                    OmsOrderState.CANCELED,
                    OmsOrderState.FILLED,
                    OmsOrderState.REJECTED,
                    OmsOrderState.SUBMIT_UNKNOWN,
                }
            ):
                raise OmsOrderBookError("checkpoint order state/quantity is inconsistent")
            active = initial.reservation.original_requirement.scaled_to_leaves(
                total_quantity=quantity,
                leaves_quantity=0 if terminal else quantity - row.filled_quantity,
            )
            current = replace(
                initial,
                order=replace(initial.order, state=target, order_revision=row.order_revision),
                filled_quantity=row.filled_quantity,
                trade_keys=trades,
                reservation=replace(
                    initial.reservation,
                    active_requirement=active,
                    status=ReservationStatus.RELEASED if terminal else ReservationStatus.ACTIVE,
                    reservation_revision=row.reservation_revision,
                ),
            )
            order_id = current.order.oms_order_id
            account = accounts.setdefault(current.order.account_id, _AccountBook())
            key = OmsIntentKey(
                current.order.account_id,
                current.order.consumer_trace.strategy_id or "",
                current.order.consumer_trace.strategy_order_id or "",
            )
            if (
                order_id in orders
                or current.execution.execution_id in execution_ids
                or key in account.deduplication
            ):
                raise OmsOrderBookError(
                    "checkpoint contains duplicate order/intent/execution identity"
                )
            orders[order_id] = current
            execution_ids.add(current.execution.execution_id)
            account.orders[order_id] = current
            account.deduplication[key] = _IntentRecord(initial.order.intent_hash, initial)
            account.reservations.create(current.reservation)
        last_lsn = 0
        histories: dict[str, list[OmsOrderEvent]] = {}
        for row in state.events:
            order = orders.get(row.oms_order_id)
            if (
                order is None
                or row.account_id != order.order.account_id
                or row.execution_id != order.execution.execution_id
                or not last_lsn < row.journal_lsn <= state.watermark
                or row.event_id != f"oms-event:{state.journal_id}:{row.journal_lsn}"
            ):
                raise OmsOrderBookError("checkpoint event position/identity is invalid")
            event = OmsOrderEvent(
                row.event_id,
                OmsOrderEventKind(row.kind),
                row.account_id,
                row.oms_order_id,
                row.execution_id,
                OmsOrderState(row.order_state),
                row.order_revision,
                state.journal_id,
                row.journal_lsn,
                row.source_key,
            )
            history = histories.setdefault(row.oms_order_id, [])
            if not history:
                if (
                    event.kind is not OmsOrderEventKind.ORDER_ACCEPTED
                    or event.order_revision != 0
                    or event.order_state is not OmsOrderState.ACCEPTED
                    or event.source_key != f"oms:{row.oms_order_id}:accepted"
                ):
                    raise OmsOrderBookError("checkpoint lacks initial acceptance event")
            else:
                previous = history[-1]
                if event.order_revision != previous.order_revision + 1:
                    raise OmsOrderBookError("checkpoint event revision is discontinuous")
                if event.kind is OmsOrderEventKind.TRADE_APPLIED:
                    if (
                        previous.order_state
                        not in {
                            OmsOrderState.ACCEPTED,
                            OmsOrderState.WORKING,
                            OmsOrderState.PARTIALLY_FILLED,
                            OmsOrderState.CANCEL_PENDING,
                        }
                        or event.order_state
                        not in {OmsOrderState.PARTIALLY_FILLED, OmsOrderState.FILLED}
                        or not event.source_key.startswith("trade:")
                        or event.source_key.removeprefix("trade:") not in order.trade_keys
                    ):
                        raise OmsOrderBookError("checkpoint trade event is inconsistent")
                elif event.kind is OmsOrderEventKind.ORDER_STATE_CHANGED:
                    _validate_transition(previous.order_state, event.order_state)
                    if event.source_key != f"oms:{row.oms_order_id}:state:{row.order_revision}":
                        raise OmsOrderBookError("checkpoint transition source is invalid")
                else:
                    raise OmsOrderBookError("checkpoint repeated acceptance event")
            history.append(event)
            accounts[row.account_id].events.append(event)
            last_lsn = row.journal_lsn
        for order_id, order in orders.items():
            history = histories.get(order_id, [])
            trade_sources = [
                e.source_key.removeprefix("trade:")
                for e in history
                if e.kind is OmsOrderEventKind.TRADE_APPLIED
            ]
            adjustments = len(trade_sources) + int(
                order.order.state in {OmsOrderState.CANCELED, OmsOrderState.REJECTED}
            )
            if (
                not history
                or history[-1].order_state != order.order.state
                or history[-1].order_revision != order.order.order_revision
                or len(trade_sources) != len(set(trade_sources))
                or frozenset(trade_sources) != order.trade_keys
                or order.reservation.reservation_revision != adjustments
            ):
                raise OmsOrderBookError("checkpoint final state/reservation is inconsistent")
        self._accounts = accounts
        self._recovered_lsn, self._recovered_journal_id = state.watermark, state.journal_id

    def restore_committed_batch(
        self, facts: tuple[OmsJournalFact, ...], commit: OmsJournalCommit
    ) -> OmsOrderEvent | None:
        """Startup-only, journal-verified replay; never append facts or call a broker.

        Every batch is validated before changing reservations/order state. The
        caller must prove physical commit/CRC and supply consecutive batch ranges.
        Legacy admissions without the canonical recovery envelope fail closed.
        """

        if (
            not self._persist_recovery_seed
            or self._live_started
            or not facts
            or commit.first_lsn != self._recovered_lsn + 1
            or commit.fact_count != len(facts)
            or (
                self._recovered_journal_id is not None
                and self._recovered_journal_id != commit.journal_id
            )
        ):
            raise OmsOrderBookError("OMS replay scope/position or lifecycle is invalid")
        first = facts[0]
        if any(
            (fact.account_id, fact.oms_order_id, fact.execution_id)
            != (first.account_id, first.oms_order_id, first.execution_id)
            for fact in facts
        ):
            raise OmsOrderBookError("OMS replay batch crosses identities")
        account = self._account(first.account_id)
        with account.lock:
            kind = OmsOrderEventKind.ORDER_STATE_CHANGED
            source = ""
            previous: OrderBookOrder | None = None
            key: OmsIntentKey | None = None
            if first.kind is OmsFactKind.ORDER_ATTRIBUTED:
                if (
                    tuple(f.kind for f in facts)
                    != (OmsFactKind.ORDER_ATTRIBUTED, OmsFactKind.RESERVATION_CREATED)
                    or first.recovery_payload is None
                ):
                    raise OmsOrderBookError("OMS admission lacks a complete recovery envelope")
                order = self._decode_recovery_admission(first.recovery_payload).order
                execution = order.execution
                recovered_intent = OmsOrderIntent(
                    consumer_trace=order.order.consumer_trace,
                    trading_day=order.order.trading_day,
                    symbol=execution.symbol,
                    market=execution.market,
                    side=execution.side,
                    price_type=execution.price_type,
                    business_type=execution.business_type,
                    quantity=execution.quantity,
                    protection_price_ticks=execution.protection_price_ticks,
                    limit_price_ticks=execution.limit_price_ticks,
                    execution_parameters=execution.execution_parameters,
                )
                expected_attribution = {
                    "intent_hash": order.order.intent_hash,
                    "attribution_binding_id": order.order.consumer_trace.attribution_binding_id,
                    "binding_version": str(order.order.consumer_trace.binding_version),
                    "decision_context_id": order.decision_context.decision_context_id,
                    "risk_snapshot_id": order.decision_context.risk_snapshot_id,
                }
                if (
                    order.order.account_id != first.account_id
                    or order.order.oms_order_id != first.oms_order_id
                    or order.execution.execution_id != first.execution_id
                    or first.attributes.get("intent_hash") != order.order.intent_hash
                    or intent_hash_for(recovered_intent) != order.order.intent_hash
                    or dict(first.attributes) != expected_attribution
                    or dict(facts[1].attributes) != dict(_reservation_attributes(order.reservation))
                ):
                    raise OmsOrderBookError("OMS recovery envelope/fact identity mismatch")
                key = OmsIntentKey(
                    first.account_id,
                    order.order.consumer_trace.strategy_id or "",
                    order.order.consumer_trace.strategy_order_id or "",
                )
                if first.oms_order_id in account.orders or key in account.deduplication:
                    raise OmsOrderBookError("OMS replay repeats an admitted identity")
                kind = OmsOrderEventKind.ORDER_ACCEPTED
                source = f"oms:{order.order.oms_order_id}:accepted"
            else:
                if first.oms_order_id is None:
                    raise OmsOrderBookError("OMS replay fact has no order identity")
                previous = self._require_order(account, first.oms_order_id)
                if previous.execution.execution_id != first.execution_id:
                    raise OmsOrderBookError("OMS replay execution identity mismatch")
                if first.kind is OmsFactKind.IDEMPOTENCY_CONFLICT:
                    if (
                        len(facts) != 1
                        or first.attributes.get("existing_intent_hash")
                        != previous.order.intent_hash
                        or first.attributes.get("incoming_intent_hash")
                        in (None, previous.order.intent_hash)
                        or first.attributes.get("strategy_id")
                        != previous.order.consumer_trace.strategy_id
                        or first.attributes.get("strategy_order_id")
                        != previous.order.consumer_trace.strategy_order_id
                    ):
                        raise OmsOrderBookError("OMS replay conflict is not proven")
                    self._recovered_lsn = commit.final_lsn
                    self._recovered_journal_id = commit.journal_id
                    return None
                if first.kind is OmsFactKind.BROKER_TRADE_APPLIED:
                    trade_key = first.attributes.get("trade_key", "")
                    quantity = int(first.attributes.get("quantity", "0"))
                    if (
                        not trade_key
                        or trade_key in previous.trade_keys
                        or quantity <= 0
                        or quantity > previous.leaves_quantity
                        or previous.order.state
                        not in {
                            OmsOrderState.ACCEPTED,
                            OmsOrderState.WORKING,
                            OmsOrderState.CANCEL_PENDING,
                            OmsOrderState.PARTIALLY_FILLED,
                        }
                    ):
                        raise OmsOrderBookError("OMS replay trade is duplicate or invalid")
                    filled = previous.filled_quantity + quantity
                    leaves = previous.execution.quantity - filled
                    target = OmsOrderState.FILLED if leaves == 0 else OmsOrderState.PARTIALLY_FILLED
                    reservation = account.reservations.adjust_to_leaves(
                        oms_order_id=first.oms_order_id,
                        total_quantity=previous.execution.quantity,
                        leaves_quantity=leaves,
                    )
                    order = replace(
                        previous,
                        order=replace(
                            previous.order,
                            state=target,
                            order_revision=previous.order.order_revision + 1,
                        ),
                        reservation=reservation,
                        filled_quantity=filled,
                        trade_keys=previous.trade_keys | {trade_key},
                    )
                    expected = (
                        OmsFactKind.BROKER_TRADE_APPLIED,
                        OmsFactKind.RESERVATION_RELEASED
                        if leaves == 0
                        else OmsFactKind.RESERVATION_ADJUSTED,
                    )
                    kind, source = OmsOrderEventKind.TRADE_APPLIED, f"trade:{trade_key}"
                else:
                    state_index = 1 if first.kind is OmsFactKind.CANCEL_REQUESTED else 0
                    if (
                        len(facts) <= state_index
                        or facts[state_index].kind is not OmsFactKind.ORDER_STATE_CHANGED
                    ):
                        raise OmsOrderBookError("OMS replay operation is not supported")
                    state = facts[state_index]
                    target = OmsOrderState(state.attributes.get("to", ""))
                    if state.attributes.get("from") != previous.order.state.value or (
                        state_index
                        and (
                            first.attributes != state.attributes
                            or target is not OmsOrderState.CANCEL_PENDING
                        )
                    ):
                        raise OmsOrderBookError("OMS replay transition predecessor mismatch")
                    if state_index:
                        keys = set(first.attributes)
                        if keys == {"from", "to", "cancel_request_id", "reason"}:
                            require_text(
                                first.attributes["cancel_request_id"],
                                "cancel request id",
                                max_length=128,
                            )
                            require_text(first.attributes["reason"], "cancel request reason", max_length=256)
                        elif keys != {"from", "to"}:
                            raise OmsOrderBookError("OMS replay cancellation metadata is invalid")
                    _validate_transition(previous.order.state, target)
                    reservation = previous.reservation
                    terminal = target in {OmsOrderState.CANCELED, OmsOrderState.REJECTED}
                    if terminal:
                        reservation = account.reservations.adjust_to_leaves(
                            oms_order_id=first.oms_order_id,
                            total_quantity=previous.execution.quantity,
                            leaves_quantity=0,
                        )
                    order = replace(
                        previous,
                        order=replace(
                            previous.order,
                            state=target,
                            order_revision=previous.order.order_revision + 1,
                        ),
                        reservation=reservation,
                    )
                    expected = (
                        ((OmsFactKind.CANCEL_REQUESTED,) if state_index else ())
                        + (OmsFactKind.ORDER_STATE_CHANGED,)
                        + ((OmsFactKind.RESERVATION_RELEASED,) if terminal else ())
                    )
                    source = f"oms:{first.oms_order_id}:state:{order.order.order_revision}"
                if tuple(f.kind for f in facts) != expected:
                    raise OmsOrderBookError("OMS replay state/reservation batch is incomplete")
                if order.reservation != previous.reservation and (
                    dict(facts[-1].attributes) != dict(_reservation_attributes(order.reservation))
                ):
                    raise OmsOrderBookError("OMS replay reservation does not match the transition")
            if previous is None:
                if key is None:
                    raise OmsOrderBookError("OMS replay admission key is missing")
                account.reservations.create(order.reservation)
                account.deduplication[key] = _IntentRecord(order.order.intent_hash, order)
            elif previous.reservation != order.reservation:
                account.reservations.replace_committed(order.reservation)
            account.orders[order.order.oms_order_id] = order
            event = self._event(kind, order, commit, source_key=source)
            account.events.append(event)
            self._recovered_lsn = commit.final_lsn
            self._recovered_journal_id = commit.journal_id
            return event

    def _transition(
        self,
        *,
        oms_order_id: str,
        target: OmsOrderState,
        first_fact_kind: OmsFactKind | None = None,
        additional_state_attributes: Mapping[str, str] | None = None,
    ) -> OmsOrderEvent | None:
        self._live_started = True
        account = self._account_for_order(oms_order_id)
        with account.lock:
            current = self._require_order(account, oms_order_id)
            if current.order.state is target:
                return None
            _validate_transition(current.order.state, target)
            updated_reservation = current.reservation
            state_attributes = {"from": current.order.state.value, "to": target.value}
            if additional_state_attributes is not None:
                if first_fact_kind is not OmsFactKind.CANCEL_REQUESTED or set(
                    additional_state_attributes
                ) != {"cancel_request_id", "reason"}:
                    raise OmsOrderBookError("additional transition attributes are invalid")
                state_attributes.update(additional_state_attributes)
            facts: list[OmsJournalFact] = []
            if first_fact_kind is not None:
                facts.append(
                    self._fact(
                        first_fact_kind,
                        account_id=current.order.account_id,
                        oms_order_id=oms_order_id,
                        execution_id=current.execution.execution_id,
                        attributes=state_attributes,
                    )
                )
            facts.append(
                self._fact(
                    OmsFactKind.ORDER_STATE_CHANGED,
                    account_id=current.order.account_id,
                    oms_order_id=oms_order_id,
                    execution_id=current.execution.execution_id,
                    attributes=state_attributes,
                )
            )
            if target in {OmsOrderState.CANCELED, OmsOrderState.REJECTED}:
                updated_reservation = account.reservations.adjust_to_leaves(
                    oms_order_id=oms_order_id,
                    total_quantity=current.execution.quantity,
                    leaves_quantity=0,
                )
                facts.append(
                    self._reservation_fact(replace(current, reservation=updated_reservation))
                )
            updated = replace(
                current,
                order=replace(
                    current.order,
                    state=target,
                    order_revision=current.order.order_revision + 1,
                ),
                reservation=updated_reservation,
            )
            commit = self._commit(tuple(facts))
            if updated_reservation != current.reservation:
                account.reservations.replace_committed(updated_reservation)
            account.orders[oms_order_id] = updated
            event = self._event(
                OmsOrderEventKind.ORDER_STATE_CHANGED,
                updated,
                commit,
                source_key=f"oms:{oms_order_id}:state:{updated.order.order_revision}",
            )
            account.events.append(event)
            return event

    def _new_order(
        self,
        *,
        intent: OmsOrderIntent,
        intent_hash: str,
        decision_context: DecisionContext,
        risk_snapshot: RiskSnapshot,
        reservation_requirement: ReservationRequirement,
        now_monotonic_ns: int,
    ) -> OrderBookOrder:
        oms_order_id = self._new_id()
        execution_id = self._new_id()
        reservation = Reservation(
            reservation_id=self._new_id(),
            oms_order_id=oms_order_id,
            account_id=intent.account_id,
            trading_day=intent.trading_day,
            risk_snapshot_id=risk_snapshot.risk_snapshot_id,
            decision_context_id=decision_context.decision_context_id,
            original_requirement=reservation_requirement,
            active_requirement=reservation_requirement,
            status=ReservationStatus.ACTIVE,
            reservation_revision=0,
            created_monotonic_ns=now_monotonic_ns,
        )
        order = OmsOrder(
            oms_order_id=oms_order_id,
            account_id=intent.account_id,
            trading_day=intent.trading_day,
            consumer_trace=intent.consumer_trace,
            intent_hash=intent_hash,
            order_origin=intent.consumer_trace.order_origin,
            state=OmsOrderState.ACCEPTED,
            created_monotonic_ns=now_monotonic_ns,
        )
        execution = ExecutionInstruction(
            execution_id=execution_id,
            oms_order_id=oms_order_id,
            account_id=intent.account_id,
            trading_day=intent.trading_day,
            consumer_trace=intent.consumer_trace,
            symbol=intent.symbol,
            market=intent.market,
            side=intent.side,
            price_type=intent.price_type,
            business_type=intent.business_type,
            quantity=intent.quantity,
            protection_price_ticks=intent.protection_price_ticks,
            limit_price_ticks=intent.limit_price_ticks,
            execution_parameters=intent.execution_parameters,
        )
        return OrderBookOrder(
            order=order,
            execution=execution,
            decision_context=decision_context,
            reservation=reservation,
        )

    def _acceptance_facts(
        self,
        order: OrderBookOrder,
        attribution_binding: AttributionBinding,
    ) -> tuple[OmsJournalFact, ...]:
        recovery_payload: bytes | None = None
        if self._persist_recovery_seed:
            from .recovery import OmsRecoverySeedV1

            recovery_payload = OmsRecoverySeedV1.build(order).encoded
        return (
            self._fact(
                OmsFactKind.ORDER_ATTRIBUTED,
                account_id=order.order.account_id,
                oms_order_id=order.order.oms_order_id,
                execution_id=order.execution.execution_id,
                attributes={
                    "intent_hash": order.order.intent_hash,
                    "attribution_binding_id": attribution_binding.attribution_binding_id,
                    "binding_version": str(attribution_binding.binding_version),
                    "decision_context_id": order.decision_context.decision_context_id,
                    "risk_snapshot_id": order.decision_context.risk_snapshot_id,
                },
                recovery_payload=recovery_payload,
            ),
            self._fact(
                OmsFactKind.RESERVATION_CREATED,
                account_id=order.order.account_id,
                oms_order_id=order.order.oms_order_id,
                execution_id=order.execution.execution_id,
                attributes=_reservation_attributes(order.reservation),
            ),
        )

    def _reservation_fact(self, order: OrderBookOrder) -> OmsJournalFact:
        kind = (
            OmsFactKind.RESERVATION_RELEASED
            if order.reservation.status is ReservationStatus.RELEASED
            else OmsFactKind.RESERVATION_ADJUSTED
        )
        return self._fact(
            kind,
            account_id=order.order.account_id,
            oms_order_id=order.order.oms_order_id,
            execution_id=order.execution.execution_id,
            attributes=_reservation_attributes(order.reservation),
        )

    def _fact(
        self,
        kind: OmsFactKind,
        *,
        account_id: str,
        oms_order_id: str | None,
        execution_id: str | None,
        attributes: Mapping[str, str],
        recovery_payload: bytes | None = None,
    ) -> OmsJournalFact:
        return OmsJournalFact(
            fact_id=self._new_id(),
            kind=kind,
            account_id=account_id,
            oms_order_id=oms_order_id,
            execution_id=execution_id,
            attributes=attributes,
            recovery_payload=recovery_payload,
        )

    def _commit(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        commit = self._journal.append_batch(facts)
        if commit.fact_count != len(facts):
            raise OmsOrderBookError("OMS Journal reported another committed fact count")
        return commit

    def _event(
        self,
        kind: OmsOrderEventKind,
        order: OrderBookOrder,
        commit: OmsJournalCommit,
        *,
        source_key: str,
    ) -> OmsOrderEvent:
        return OmsOrderEvent(
            event_id=(
                f"oms-event:{commit.journal_id}:{commit.final_lsn}"
                if self._persist_recovery_seed
                else self._new_id()
            ),
            kind=kind,
            account_id=order.order.account_id,
            oms_order_id=order.order.oms_order_id,
            execution_id=order.execution.execution_id,
            order_state=order.order.state,
            order_revision=order.order.order_revision,
            journal_id=commit.journal_id,
            journal_lsn=commit.final_lsn,
            source_key=source_key,
            journal_position=(commit.member_positions[-1] if commit.member_positions else None),
        )

    def _validate_admission_inputs(
        self,
        intent: OmsOrderIntent,
        account_binding: ConsumerAccountBinding,
        strategy_binding: ConsumerStrategyBinding,
        attribution_binding: AttributionBinding,
        risk_snapshot: RiskSnapshot,
        decision_context: DecisionContext,
        reservation_requirement: ReservationRequirement,
        now_monotonic_ns: int,
    ) -> None:
        self._validate_binding(intent, account_binding, strategy_binding, attribution_binding)
        self._validate_risk_context(intent, risk_snapshot, decision_context, now_monotonic_ns)
        if reservation_requirement.symbol != intent.symbol:
            raise OmsOrderBookError("reservation symbol differs from intent symbol")
        if reservation_requirement.is_empty:
            raise OmsOrderBookError("new OMS intent requires a non-empty reservation")

    @staticmethod
    def _validate_binding(
        intent: OmsOrderIntent,
        account_binding: ConsumerAccountBinding,
        strategy_binding: ConsumerStrategyBinding,
        attribution_binding: AttributionBinding,
    ) -> None:
        trace = intent.consumer_trace
        if not account_binding.active or not strategy_binding.active:
            raise OmsOrderBookError("inactive consumer binding cannot submit an OMS intent")
        if (
            account_binding.consumer_id != trace.consumer_id
            or account_binding.consumer_account_id != trace.consumer_account_id
            or account_binding.account_id != trace.account_id
            or strategy_binding.consumer_id != trace.consumer_id
            or strategy_binding.consumer_account_id != trace.consumer_account_id
            or strategy_binding.consumer_account_binding_id != account_binding.binding_id
            or strategy_binding.strategy_id != trace.strategy_id
            or attribution_binding.consumer_account_binding_id != account_binding.binding_id
            or attribution_binding.consumer_strategy_binding_id != strategy_binding.binding_id
            or attribution_binding.consumer_id != trace.consumer_id
            or attribution_binding.consumer_account_id != trace.consumer_account_id
            or attribution_binding.strategy_id != trace.strategy_id
            or attribution_binding.attribution_binding_id != trace.attribution_binding_id
            or attribution_binding.binding_version != trace.binding_version
        ):
            raise OmsOrderBookError(
                "consumer/account/strategy binding does not match intent lineage"
            )

    @staticmethod
    def _validate_risk_context(
        intent: OmsOrderIntent,
        risk_snapshot: RiskSnapshot,
        decision_context: DecisionContext,
        now_monotonic_ns: int,
    ) -> None:
        if (
            risk_snapshot.account_id != intent.account_id
            or risk_snapshot.trading_day != intent.trading_day
        ):
            raise OmsOrderBookError("risk snapshot is outside the intent account/day scope")
        if not risk_snapshot.is_fresh_at(now_monotonic_ns):
            raise ReservationRiskError("risk snapshot is stale at OMS admission")
        if (
            decision_context.account_id != intent.account_id
            or decision_context.trading_day != intent.trading_day
            or decision_context.risk_snapshot_id != risk_snapshot.risk_snapshot_id
            or decision_context.risk_rule_bundle_hash != risk_snapshot.rule_bundle_hash
            or decision_context.protection_price_ticks != intent.protection_price_ticks
            or decision_context.decided_monotonic_ns > now_monotonic_ns
        ):
            raise OmsOrderBookError("decision context does not prove this risk admission")

    def _account(self, account_id: str) -> _AccountBook:
        require_text(account_id, "OMS account id")
        with self._accounts_lock:
            existing = self._accounts.get(account_id)
            if existing is None:
                existing = _AccountBook()
                self._accounts[account_id] = existing
            return existing

    def _account_for_order(self, oms_order_id: str) -> _AccountBook:
        require_text(oms_order_id, "OMS order id")
        with self._accounts_lock:
            accounts = tuple(self._accounts.values())
        # Membership changes only at startup. Never wait on account admission
        # while holding the account-map lock (ABBA lock-order inversion).
        for account in accounts:
            with account.lock:
                if oms_order_id in account.orders:
                    return account
        raise OmsOrderBookError("OMS order is unknown")

    @staticmethod
    def _require_order(account: _AccountBook, oms_order_id: str) -> OrderBookOrder:
        try:
            return account.orders[oms_order_id]
        except KeyError as error:
            raise OmsOrderBookError("OMS order is unknown") from error


def _empty_orders() -> dict[str, OrderBookOrder]:
    """Create a precisely typed per-account order map."""

    return {}


def _empty_intent_records() -> dict[OmsIntentKey, _IntentRecord]:
    """Create a precisely typed per-account idempotency map."""

    return {}


def _empty_events() -> list[OmsOrderEvent]:
    """Create a precisely typed per-account event list."""

    return []


@dataclass
class _AccountBook:
    lock: RLock = field(default_factory=RLock)
    orders: dict[str, OrderBookOrder] = field(default_factory=_empty_orders)
    deduplication: dict[OmsIntentKey, _IntentRecord] = field(default_factory=_empty_intent_records)
    reservations: ReservationBook = field(default_factory=ReservationBook)
    events: list[OmsOrderEvent] | deque[OmsOrderEvent] = field(default_factory=_empty_events)


@dataclass(frozen=True)
class OmsIntentKey:
    account_id: str
    strategy_id: str
    strategy_order_id: str


@dataclass(frozen=True)
class _IntentRecord:
    intent_hash: str
    order: OrderBookOrder


def intent_hash_for(intent: OmsOrderIntent) -> str:
    """Return the versioned canonical SHA-256 business idempotency hash."""

    trace = intent.consumer_trace
    canonical = {
        "schema_version": OMS_INTENT_HASH_SCHEMA_VERSION,
        "consumer_id": trace.consumer_id,
        "consumer_account_id": trace.consumer_account_id,
        "account_id": trace.account_id,
        "trading_day": intent.trading_day.value,
        "order_origin": trace.order_origin.value,
        "strategy_id": trace.strategy_id,
        "strategy_order_id": trace.strategy_order_id,
        "symbol": intent.symbol,
        "market": intent.market,
        "side": intent.side,
        "price_type": intent.price_type,
        "business_type": intent.business_type,
        "quantity": intent.quantity,
        "protection_price_ticks": intent.protection_price_ticks,
        "limit_price_ticks": intent.limit_price_ticks,
        "execution_parameters": dict(intent.execution_parameters),
    }
    # Typed intent fields contain no floats or arbitrary objects. ASCII JSON
    # bytes from this encoder match the existing versioned canonical format;
    # DEL/non-ASCII/surrogates/large unsupported integers keep the reference
    # ensure_ascii path. Hash version and accepted business fields do not change.
    try:
        encoded = msgspec.json.encode(canonical, order="sorted")
        if not encoded.isascii() or b"\x7f" in encoded:
            raise ValueError("reference escaping required")
    except (TypeError, ValueError, UnicodeEncodeError, OverflowError):
        encoded = json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    return sha256(encoded).hexdigest()


def _validate_transition(current: OmsOrderState, target: OmsOrderState) -> None:
    allowed: Mapping[OmsOrderState, set[OmsOrderState]] = {
        OmsOrderState.ACCEPTED: {
            OmsOrderState.WORKING,
            OmsOrderState.CANCEL_PENDING,
            OmsOrderState.PARTIALLY_FILLED,
            OmsOrderState.FILLED,
            OmsOrderState.REJECTED,
            OmsOrderState.SUBMIT_UNKNOWN,
        },
        OmsOrderState.WORKING: {
            OmsOrderState.CANCEL_PENDING,
            OmsOrderState.PARTIALLY_FILLED,
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
            OmsOrderState.SUBMIT_UNKNOWN,
        },
        OmsOrderState.PARTIALLY_FILLED: {
            OmsOrderState.CANCEL_PENDING,
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
        },
        OmsOrderState.CANCEL_PENDING: {
            OmsOrderState.PARTIALLY_FILLED,
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
            OmsOrderState.SUBMIT_UNKNOWN,
        },
        OmsOrderState.SUBMIT_UNKNOWN: {
            OmsOrderState.WORKING,
            OmsOrderState.PARTIALLY_FILLED,
            OmsOrderState.FILLED,
            OmsOrderState.CANCELED,
            OmsOrderState.REJECTED,
        },
        OmsOrderState.FILLED: set(),
        OmsOrderState.CANCELED: set(),
        OmsOrderState.REJECTED: set(),
    }
    if target not in allowed[current]:
        raise OrderBookStateError(
            f"OMS transition {current.value} -> {target.value} is not allowed"
        )


def _reservation_attributes(reservation: Reservation) -> Mapping[str, str]:
    active = reservation.active_requirement
    return MappingProxyType(
        {
            "reservation_id": reservation.reservation_id,
            "risk_snapshot_id": reservation.risk_snapshot_id,
            "decision_context_id": reservation.decision_context_id,
            "reservation_revision": str(reservation.reservation_revision),
            "reservation_status": reservation.status.value,
            "symbol": active.symbol,
            "cash_minor": str(active.cash_minor),
            "margin_minor": str(active.margin_minor),
            "sellable_quantity": str(active.sellable_quantity),
            "borrowable_quantity": str(active.borrowable_quantity),
        }
    )


def _ceil_scaled(value: int, numerator: int, denominator: int) -> int:
    return (value * numerator + denominator - 1) // denominator


def _uuid7() -> str:
    """Generate a UUIDv7-shaped local identifier until Python exposes uuid.uuid7."""

    milliseconds = time.time_ns() // 1_000_000
    if milliseconds >= 1 << 48:
        raise OmsOrderBookError("system clock exceeds UUIDv7 timestamp range")
    # One CSPRNG draw supplies the SAME 74 uniformly random bits. Splitting
    # that draw avoids a second getrandom/GIL handoff for each local identity;
    # no PRNG, entropy cache, fork-sensitive pool or weaker random source.
    entropy = secrets.randbits(74)
    value = (
        (milliseconds << 80)
        | (0b0111 << 76)
        | ((entropy >> 62) << 64)
        | (0b10 << 62)
        | (entropy & ((1 << 62) - 1))
    )
    return str(uuid.UUID(int=value))
