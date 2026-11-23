"""Vendor-neutral DTOs and session port for a PaperBroker connector.

An implementation of this port may use HTTP, RPC, or a controlled library,
but it must convert vendor payloads here rather than exposing them to OMS/EMS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping, Protocol

from ...domain import (
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    ErrorCategory,
    RecoveryPlan,
    TradingDayEpoch,
)
from ...domain._validation import freeze_text_mapping, require_nonnegative, require_text
from ..base import BrokerQuery, CancelExecutionInstruction, QueryHandle, RecoveryHandle
from ..mapping import MappedVenueOrder
from .dependency import PaperBrokerRuntimeIdentity


class PaperBrokerConnectorError(RuntimeError):
    """A payload-free external session, transport, or wire-contract failure."""


def _empty_summary() -> Mapping[str, str]:
    return {}


@dataclass(frozen=True)
class PaperBrokerHealth:
    """A bounded health-probe result without endpoint details or raw payloads."""

    ready: bool
    detail_code: str

    def __post_init__(self) -> None:
        require_text(self.detail_code, "paperbroker health detail_code", max_length=128)


@dataclass(frozen=True)
class PaperBrokerOrderRequest:
    """Mapped request delivered to the external connector implementation."""

    instruction_id: str
    account_id: str
    mapped_order: MappedVenueOrder
    symbol: str
    quantity: int
    limit_price_ticks: int | None
    client_order_ref: str

    def __post_init__(self) -> None:
        require_text(self.instruction_id, "paperbroker instruction_id")
        require_text(self.account_id, "paperbroker account_id")
        require_text(self.symbol, "paperbroker symbol", max_length=64)
        require_nonnegative(self.quantity, "paperbroker quantity")
        require_text(self.client_order_ref, "paperbroker client_order_ref")
        if self.limit_price_ticks is not None:
            require_nonnegative(self.limit_price_ticks, "paperbroker limit_price_ticks")


@dataclass(frozen=True)
class PaperBrokerSubmitReceipt:
    """A normalized external acceptance; it does not claim a state transition."""

    accepted_monotonic_ns: int
    venue_order_id: str | None

    def __post_init__(self) -> None:
        require_nonnegative(self.accepted_monotonic_ns, "paperbroker submit accepted time")
        if self.venue_order_id is not None:
            require_text(self.venue_order_id, "paperbroker venue_order_id")


@dataclass(frozen=True)
class PaperBrokerCancelReceipt:
    """A normalized external cancel acceptance; terminal state arrives as an event."""

    accepted_monotonic_ns: int
    venue_cancel_id: str | None

    def __post_init__(self) -> None:
        require_nonnegative(self.accepted_monotonic_ns, "paperbroker cancel accepted time")
        if self.venue_cancel_id is not None:
            require_text(self.venue_cancel_id, "paperbroker venue_cancel_id")


@dataclass(frozen=True)
class PaperBrokerNativeEvent:
    """The bounded, vendor-free event shape produced by a connector codec."""

    external_event_id: str
    event_kind: BrokerEventKind
    source_kind: BrokerSourceKind
    account_id: str
    trading_day: TradingDayEpoch
    received_monotonic_ns: int
    client_order_ref: str | None = None
    venue_order_id: str | None = None
    venue_cancel_id: str | None = None
    execution_id: str | None = None
    cancel_request_id: str | None = None
    trade_key: str | None = None
    filled_qty: int | None = None
    leaves_qty: int | None = None
    error_category: ErrorCategory | None = None
    normalized_summary: Mapping[str, str] = field(default_factory=_empty_summary)

    def __post_init__(self) -> None:
        require_text(self.external_event_id, "paperbroker external_event_id", max_length=128)
        require_text(self.account_id, "paperbroker event account_id")
        require_nonnegative(self.received_monotonic_ns, "paperbroker event received time")
        for name, value in (
            ("client_order_ref", self.client_order_ref),
            ("venue_order_id", self.venue_order_id),
            ("venue_cancel_id", self.venue_cancel_id),
            ("execution_id", self.execution_id),
            ("cancel_request_id", self.cancel_request_id),
            ("trade_key", self.trade_key),
        ):
            if value is not None:
                require_text(value, f"paperbroker event {name}")
        if self.event_kind is BrokerEventKind.TRADE and self.trade_key is None:
            raise PaperBrokerConnectorError("PaperBroker trade event lacks stable trade_key")
        if self.event_kind is BrokerEventKind.CANCEL_ERROR and self.cancel_request_id is None:
            raise PaperBrokerConnectorError("PaperBroker cancel error lacks cancel_request_id")
        if self.filled_qty is not None:
            require_nonnegative(self.filled_qty, "paperbroker event filled_qty")
        if self.leaves_qty is not None:
            require_nonnegative(self.leaves_qty, "paperbroker event leaves_qty")
        object.__setattr__(
            self,
            "normalized_summary",
            freeze_text_mapping(self.normalized_summary, "paperbroker event summary"),
        )


PaperBrokerEventSink = Callable[[PaperBrokerNativeEvent], None]


class PaperBrokerSessionPort(Protocol):
    """The only seam allowed to communicate with an external PaperBroker."""

    def open(self, event_sink: PaperBrokerEventSink) -> PaperBrokerRuntimeIdentity:
        """Open one session and register the bounded external event sink."""

        ...

    def close(self) -> None:
        """Release the external session and prevent further callbacks."""

        ...

    def health(self) -> PaperBrokerHealth:
        """Probe the connected endpoint without sending an order."""

        ...

    def submit(self, request: PaperBrokerOrderRequest) -> PaperBrokerSubmitReceipt:
        """Submit one already-mapped request."""

        ...

    def cancel(self, instruction: CancelExecutionInstruction) -> PaperBrokerCancelReceipt:
        """Submit one cancel tied to an already-proven native identity."""

        ...

    def query(self, request: BrokerQuery) -> QueryHandle:
        """Start one read-only query; rows arrive by the registered event sink."""

        ...

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        """Start one read-only external recovery operation."""

        ...

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        """Return completeness-explicit recovery evidence."""

        ...
