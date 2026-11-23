"""Consumer-facing contracts for the internal TradeCore data plane.

The concrete Unix-domain-socket transport, cursor signing, Journal replay, and
OMS implementation arrive in later milestones.  This module fixes the
in-process contract now, with no broker, database, filesystem, or network
dependency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol, runtime_checkable

from ..domain import (
    AccountLifecycle,
    AdmissionGate,
    AdmissionGateKind,
    CancelRequest,
    ConsumerApiVersion,
    ConsumerAuditTrail,
    ConsumerEvent,
    ConsumerEventCursor,
    ConsumerEventCursorV2,
    ConsumerEventPositionV2,
    ConsumerPrincipal,
    ConsumerTrace,
    ReconciliationStatus,
    TradingDayEpoch,
)
from ..domain._validation import (
    DomainValidationError,
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_text,
)


class ConsumerGatewayError(RuntimeError):
    """A stable ConsumerGatewayPort failure with no implied broker side effect."""


class ConsumerAuthorizationError(ConsumerGatewayError):
    """The caller does not own the requested account, strategy, or event."""


class ConsumerSchemaNegotiationError(ConsumerGatewayError):
    """The requested Consumer API or event schema cannot be served safely."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ConsumerNotFoundError(ConsumerGatewayError):
    """An authorized lookup has no matching consumer-visible object."""


def _empty_text_mapping() -> Mapping[str, str]:
    """Return a typed empty map for bounded execution parameters."""

    return {}


@dataclass(frozen=True)
class ConsumerSession:
    """An already-authenticated internal consumer session.

    A transport establishes this from a verified local identity.  A strategy
    cannot provide a different ``consumer_id`` in a request payload.
    """

    session_id: str
    consumer_id: str
    api_version: ConsumerApiVersion
    binding_epoch: int
    event_schema_version: int = 1

    def __post_init__(self) -> None:
        require_text(self.session_id, "consumer session_id")
        require_text(self.consumer_id, "consumer session consumer_id")
        require_positive(self.binding_epoch, "consumer session binding_epoch")
        require_positive(self.event_schema_version, "consumer session event_schema_version")


@dataclass(frozen=True)
class ConsumerVersionRange:
    """Inclusive API and event-schema ranges offered during session setup."""

    minimum_api_version: ConsumerApiVersion
    maximum_api_version: ConsumerApiVersion
    minimum_event_schema_version: int
    maximum_event_schema_version: int

    def __post_init__(self) -> None:
        minimum = (self.minimum_api_version.major, self.minimum_api_version.minor)
        maximum = (self.maximum_api_version.major, self.maximum_api_version.minor)
        if minimum > maximum:
            raise DomainValidationError("consumer API version range is reversed")
        require_positive(self.minimum_event_schema_version, "minimum event schema version")
        require_positive(self.maximum_event_schema_version, "maximum event schema version")
        if self.minimum_event_schema_version > self.maximum_event_schema_version:
            raise DomainValidationError("consumer event schema version range is reversed")


def negotiate_consumer_versions(
    offered: ConsumerVersionRange,
    server_api_versions: tuple[ConsumerApiVersion, ...],
    server_event_schema_versions: tuple[int, ...],
) -> tuple[ConsumerApiVersion, int]:
    """Choose the highest common versions or reject the session without side effects."""

    minimum_api = (offered.minimum_api_version.major, offered.minimum_api_version.minor)
    maximum_api = (offered.maximum_api_version.major, offered.maximum_api_version.minor)
    api_candidates = tuple(
        version
        for version in server_api_versions
        if minimum_api <= (version.major, version.minor) <= maximum_api
    )
    event_candidates = tuple(
        version
        for version in server_event_schema_versions
        if offered.minimum_event_schema_version <= version <= offered.maximum_event_schema_version
    )
    if not api_candidates or not event_candidates:
        raise ConsumerSchemaNegotiationError(
            "UPGRADE_REQUIRED",
            "Consumer API or event schema ranges have no lossless intersection",
        )
    return (
        max(api_candidates, key=lambda version: (version.major, version.minor)),
        max(event_candidates),
    )


@dataclass(frozen=True)
class ConsumerAccountView:
    """The account details visible to the consumer that owns the binding."""

    consumer_account_id: str
    account_display_name: str
    venue_id: str
    capability_profile_id: str
    lifecycle: AccountLifecycle
    reconciliation_status: ReconciliationStatus
    gates: tuple[AdmissionGate, ...] = ()

    def __post_init__(self) -> None:
        require_text(self.consumer_account_id, "consumer account view consumer_account_id")
        require_text(self.account_display_name, "account_display_name", max_length=128)
        require_text(self.venue_id, "consumer account view venue_id")
        require_text(self.capability_profile_id, "consumer account view capability_profile_id")
        gates = tuple(self.gates)
        if len({gate.gate_id for gate in gates}) != len(gates):
            raise DomainValidationError("consumer account view repeats an admission gate")
        if self.lifecycle is AccountLifecycle.READY and (
            self.reconciliation_status is not ReconciliationStatus.VALIDATED or gates
        ):
            raise DomainValidationError(
                "READY account view requires VALIDATED reconciliation and no gates"
            )
        if self.lifecycle is AccountLifecycle.SIMULATOR_ACTIVE and (
            self.venue_id != "EMT"
            or self.capability_profile_id != "emt-simulator-submit-only-v1"
            or self.reconciliation_status is not ReconciliationStatus.NOT_REQUIRED
            or gates
        ):
            raise DomainValidationError("SIMULATOR_ACTIVE is reserved for the EMT simulator profile")
        if self.reconciliation_status is ReconciliationStatus.REQUIRED and (
            AdmissionGateKind.RECONCILIATION not in {gate.kind for gate in gates}
        ):
            raise DomainValidationError("REQUIRED account view requires a RECONCILIATION gate")
        object.__setattr__(self, "gates", gates)


@dataclass(frozen=True)
class ConsumerOrderSignal:
    """A strategy signal before OMS assigns durable order/execution identities.

    ``consumer_request_id`` is a transport correlation only.  It is expressly
    excluded from future canonical business idempotency.  The caller cannot
    provide an OMS id, an execution id, a broker client reference, or a venue
    order identity.
    """

    consumer_request_id: str
    consumer_account_id: str
    strategy_id: str
    strategy_order_id: str
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
        require_text(self.consumer_request_id, "consumer_request_id")
        require_text(self.consumer_account_id, "consumer_account_id")
        require_text(self.strategy_id, "strategy_id")
        require_text(self.strategy_order_id, "strategy_order_id")
        require_text(self.symbol, "signal symbol", max_length=64)
        require_text(self.market, "signal market", max_length=64)
        require_text(self.side, "signal side", max_length=64)
        require_text(self.price_type, "signal price_type", max_length=64)
        require_text(self.business_type, "signal business_type", max_length=64)
        require_positive(self.quantity, "signal quantity")
        require_nonnegative(self.protection_price_ticks, "signal protection_price_ticks")
        if self.limit_price_ticks is not None:
            require_nonnegative(self.limit_price_ticks, "signal limit_price_ticks")
        object.__setattr__(
            self,
            "execution_parameters",
            freeze_text_mapping(self.execution_parameters, "signal execution_parameters"),
        )


@dataclass(frozen=True)
class IntentReceipt:
    """The durable acceptance receipt returned after the OMS pre-native fact.

    M0 defines its shape only.  M1 will make every field derive from one
    committed Journal batch; M2 will expose it through ConsumerGatewayPort.
    """

    consumer_request_id: str
    accepted_event_id: str
    oms_order_id: str
    execution_id: str
    journal_id: str
    journal_lsn: int
    consumer_trace: ConsumerTrace

    def __post_init__(self) -> None:
        require_text(self.consumer_request_id, "receipt consumer_request_id")
        require_text(self.accepted_event_id, "receipt accepted_event_id")
        require_text(self.oms_order_id, "receipt oms_order_id")
        require_text(self.execution_id, "receipt execution_id")
        require_text(self.journal_id, "receipt journal_id")
        require_nonnegative(self.journal_lsn, "receipt journal_lsn")


@dataclass(frozen=True)
class ConsumerEventPage:
    """One account-scoped, fixed-watermark page of replayable consumer events."""

    events: tuple[ConsumerEvent, ...]
    high_watermark_lsn: int
    has_more: bool
    next_cursor: ConsumerEventCursor | None

    def __post_init__(self) -> None:
        events = tuple(self.events)
        require_nonnegative(self.high_watermark_lsn, "event page high_watermark_lsn")
        if self.next_cursor is None:
            raise DomainValidationError("an event page requires an account-scoped next_cursor")
        if self.next_cursor.journal_lsn > self.high_watermark_lsn:
            raise DomainValidationError("event page cursor exceeds its fixed high watermark")
        if any(
            event.consumer_trace.consumer_id != self.next_cursor.consumer_id
            or event.account_id != self.next_cursor.account_id
            for event in events
        ):
            raise DomainValidationError("event page contains another consumer or account event")
        if any(event.journal_lsn > self.high_watermark_lsn for event in events):
            raise DomainValidationError("event page contains an event above high watermark")
        if tuple(sorted(event.journal_lsn for event in events)) != tuple(
            event.journal_lsn for event in events
        ):
            raise DomainValidationError("event page is not ordered by journal LSN")
        object.__setattr__(self, "events", events)


@dataclass(frozen=True)
class ConsumerAccountOrderSnapshot:
    """A consistent, account-scoped snapshot used after ordinary cursor expiry."""

    consumer_account_id: str
    account_id: str
    snapshot_lsn: int
    next_cursor: ConsumerEventCursor
    orders: tuple[ConsumerEvent, ...]

    def __post_init__(self) -> None:
        require_text(self.consumer_account_id, "snapshot consumer_account_id")
        require_text(self.account_id, "snapshot account_id")
        require_nonnegative(self.snapshot_lsn, "snapshot_lsn")
        orders = tuple(self.orders)
        if self.next_cursor.account_id != self.account_id:
            raise DomainValidationError("snapshot cursor belongs to another account")
        if self.next_cursor.journal_lsn != self.snapshot_lsn:
            raise DomainValidationError("snapshot cursor must start at snapshot LSN")
        if any(order.account_id != self.account_id for order in orders):
            raise DomainValidationError("snapshot contains an order from another account")
        if any(
            order.consumer_trace.consumer_id != self.next_cursor.consumer_id for order in orders
        ):
            raise DomainValidationError("snapshot contains an order from another consumer")
        object.__setattr__(self, "orders", orders)


@dataclass(frozen=True)
class PositionedConsumerEventV2:
    """One consumer fact bound to its numeric Redis envelope position."""

    position: ConsumerEventPositionV2
    event: ConsumerEvent


@dataclass(frozen=True)
class ConsumerEventPageV2:
    """One V2 replay page ordered by numeric stream position, never text."""

    items: tuple[PositionedConsumerEventV2, ...]
    high_watermark_position: ConsumerEventPositionV2
    has_more: bool
    next_cursor: ConsumerEventCursorV2

    def __post_init__(self) -> None:
        items = tuple(self.items)
        positions = tuple(item.position for item in items)
        if positions != tuple(sorted(positions)):
            raise DomainValidationError("V2 event page is not numerically position ordered")
        if any(position > self.high_watermark_position for position in positions):
            raise DomainValidationError("V2 event page exceeds its fixed high watermark")
        cursor_position = self.next_cursor.after_position
        if cursor_position is not None and cursor_position > self.high_watermark_position:
            raise DomainValidationError("V2 event page cursor exceeds its fixed high watermark")
        if any(
            item.event.consumer_trace.consumer_id != self.next_cursor.consumer_id
            or item.event.consumer_trace.consumer_account_id != self.next_cursor.consumer_account_id
            or item.event.journal_id != self.next_cursor.journal_id
            for item in items
        ):
            raise DomainValidationError(
                "V2 event page contains another consumer, account binding, or Journal"
            )
        object.__setattr__(self, "items", items)


@dataclass(frozen=True)
class ConsumerAccountOrderSnapshotV2:
    """A V2 snapshot whose restart point is a signed numeric cursor."""

    consumer_account_id: str
    snapshot_position: ConsumerEventPositionV2
    next_cursor: ConsumerEventCursorV2
    orders: tuple[ConsumerEvent, ...]

    def __post_init__(self) -> None:
        require_text(self.consumer_account_id, "V2 snapshot consumer_account_id")
        if self.next_cursor.consumer_account_id != self.consumer_account_id:
            raise DomainValidationError("V2 snapshot cursor belongs to another account binding")
        if self.next_cursor.after_position != self.snapshot_position:
            raise DomainValidationError("V2 snapshot cursor must start at snapshot position")
        orders = tuple(self.orders)
        if any(
            order.consumer_trace.consumer_id != self.next_cursor.consumer_id
            or order.consumer_trace.consumer_account_id != self.consumer_account_id
            or order.journal_id != self.next_cursor.journal_id
            for order in orders
        ):
            raise DomainValidationError(
                "V2 snapshot contains another consumer, account binding, or Journal"
            )
        object.__setattr__(self, "orders", orders)


@runtime_checkable
class ConsumerEventSubscription(Protocol):
    """Optional push delivery handle; cursor replay remains authoritative."""

    def drain(self, *, limit: int) -> tuple[PositionedConsumerEventV2, ...]:
        """Drain a bounded local push batch or require durable replay on close."""

        ...

    def close(self) -> None:
        """Close only this consumer connection without changing account state."""

        ...


@runtime_checkable
class ConsumerGatewayPort(Protocol):
    """The sole data-plane port exposed to internal consumer strategies.

    Implementations must authenticate the connection before creating a
    ``ConsumerSession`` and enforce every request against the current binding
    snapshot.  They must not connect directly to a broker or query PostgreSQL
    synchronously for an individual signal.
    """

    def open_session(
        self,
        principal: ConsumerPrincipal,
        requested_versions: ConsumerVersionRange,
    ) -> ConsumerSession:
        """Select compatible API/event versions for a verified local principal."""

        ...

    def list_accounts(self, session: ConsumerSession) -> tuple[ConsumerAccountView, ...]:
        """Return only accounts bound to the session consumer."""

        ...

    def submit_signal(self, session: ConsumerSession, signal: ConsumerOrderSignal) -> IntentReceipt:
        """Accept a bound strategy signal only after M1's pre-native Journal fact."""

        ...

    def cancel_order(self, session: ConsumerSession, request: CancelRequest) -> IntentReceipt:
        """Start an authorized cancel through the normal OMS/EMS chain."""

        ...

    def get_order(self, session: ConsumerSession, oms_order_id: str) -> ConsumerEvent:
        """Return one authorized order projection or a non-disclosing not-found error."""

        ...

    def get_order_audit_trail(
        self,
        session: ConsumerSession,
        oms_order_id: str,
    ) -> tuple[ConsumerAuditTrail, ...]:
        """Return Journal/archive evidence only; PG is never an audit substitute."""

        ...

    def get_account_order_snapshot(
        self,
        session: ConsumerSession,
        consumer_account_id: str,
    ) -> ConsumerAccountOrderSnapshotV2:
        """Return a binding-filtered snapshot and restart cursor."""

        ...

    def poll_events(
        self,
        session: ConsumerSession,
        cursor: ConsumerEventCursorV2,
        *,
        limit: int,
    ) -> ConsumerEventPageV2:
        """Replay an authorized bounded event page from a signed cursor context."""

        ...

    def subscribe_order_events(
        self,
        session: ConsumerSession,
        cursor: ConsumerEventCursorV2,
    ) -> ConsumerEventSubscription:
        """Optionally subscribe after the caller has caught up through cursor replay."""

        ...
