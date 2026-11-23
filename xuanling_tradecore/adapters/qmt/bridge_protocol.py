"""Typed Linux-to-Windows boundary for the TradeCore QMT Adapter Path.

The values here deliberately contain no QMT SDK objects.  A concrete Windows
edge serializes them to the authenticated bridge protocol only after the
adapter has completed its local PRE_DISPATCH fence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping, Protocol, runtime_checkable

from ...domain import (
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    CallbackGapEvidence,
    CancelRequest,
    ErrorCategory,
    ExecutionInstruction,
    RecoveryPlan,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from ...domain._validation import (
    freeze_text_mapping,
    require_nonnegative,
    require_text,
)
from ..base import BrokerAdapterError, BrokerQuery, QueryHandle, RecoveryHandle
from ..mapping import MappedVenueOrder


class QmtBridgeError(BrokerAdapterError):
    """A payload-free Edge bridge or protocol failure."""


def _empty_summary() -> Mapping[str, str]:
    return {}


@dataclass(frozen=True)
class QmtBridgeHealth:
    """The bounded health projection used before the adapter accepts work."""

    ready: bool
    detail_code: str

    def __post_init__(self) -> None:
        require_text(self.detail_code, "QMT bridge health detail_code", max_length=128)


@dataclass(frozen=True)
class QmtSubmitCommand:
    """One mapped order accepted by Edge, not an assertion of venue acceptance."""

    instruction: ExecutionInstruction
    mapped_order: MappedVenueOrder
    client_order_ref: str
    adapter_instance_id: str
    adapter_client_scope: str

    def __post_init__(self) -> None:
        require_text(self.client_order_ref, "QMT submit client_order_ref", max_length=128)
        require_text(self.adapter_instance_id, "QMT submit adapter_instance_id")
        require_text(self.adapter_client_scope, "QMT submit adapter_client_scope")


@dataclass(frozen=True)
class QmtBridgeSubmitReceipt:
    """An Edge durable acceptance, optionally including a proven native identity."""

    execution_id: str
    accepted_monotonic_ns: int
    venue_order_identity: VenueOrderIdentity | None = None

    def __post_init__(self) -> None:
        require_text(self.execution_id, "QMT bridge submit execution_id")
        require_nonnegative(self.accepted_monotonic_ns, "QMT bridge submit accepted time")


@dataclass(frozen=True)
class QmtBridgeCancelReceipt:
    """An Edge durable cancel acceptance; terminal state remains callback-driven."""

    cancel_request_id: str
    accepted_monotonic_ns: int
    venue_order_identity: VenueOrderIdentity | None = None

    def __post_init__(self) -> None:
        require_text(self.cancel_request_id, "QMT bridge cancel request_id")
        require_nonnegative(self.accepted_monotonic_ns, "QMT bridge cancel accepted time")


@dataclass(frozen=True)
class QmtBridgeNativeEvent:
    """Bounded normalized Edge event before it becomes a domain ``BrokerEvent``."""

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
        require_text(self.external_event_id, "QMT bridge external_event_id", max_length=128)
        require_text(self.account_id, "QMT bridge event account_id")
        require_nonnegative(self.received_monotonic_ns, "QMT bridge event received time")
        for name in (
            "client_order_ref",
            "venue_order_id",
            "venue_cancel_id",
            "execution_id",
            "cancel_request_id",
            "trade_key",
        ):
            value = getattr(self, name)
            if value is not None:
                require_text(value, f"QMT bridge event {name}")
        if self.event_kind is BrokerEventKind.TRADE and self.trade_key is None:
            raise QmtBridgeError("QMT trade event lacks a stable trade_key")
        if self.event_kind is BrokerEventKind.CANCEL_ERROR and self.cancel_request_id is None:
            raise QmtBridgeError("QMT cancel error event lacks cancel_request_id")
        if self.filled_qty is not None:
            require_nonnegative(self.filled_qty, "QMT bridge event filled_qty")
        if self.leaves_qty is not None:
            require_nonnegative(self.leaves_qty, "QMT bridge event leaves_qty")
        object.__setattr__(
            self,
            "normalized_summary",
            freeze_text_mapping(self.normalized_summary, "QMT bridge event summary"),
        )


@dataclass(frozen=True)
class QmtBridgeEventDelivery:
    """One at-least-once Edge event delivery that needs an explicit ACK."""

    delivery_id: int
    event: QmtBridgeNativeEvent

    def __post_init__(self) -> None:
        require_nonnegative(self.delivery_id, "QMT bridge delivery_id")


@dataclass(frozen=True)
class QmtBridgeGapDelivery:
    """A conservative Edge gap signal that must reach the durable observer."""

    delivery_id: int
    evidence: CallbackGapEvidence

    def __post_init__(self) -> None:
        require_nonnegative(self.delivery_id, "QMT bridge gap delivery_id")


QmtBridgeDelivery = QmtBridgeEventDelivery | QmtBridgeGapDelivery
QmtBridgeDeliverySink = Callable[[QmtBridgeDelivery], None]


@runtime_checkable
class QmtBridgeFaultSource(Protocol):
    """Optional transport fault notification, internal to the QMT adapter."""

    def set_fault_sink(self, sink: Callable[[], None] | None) -> None: ...


class QmtBridgePort(Protocol):
    """The only Linux seam allowed to communicate with Windows Edge."""

    def open(self, sink: QmtBridgeDeliverySink) -> None:
        """Attach the delivery sink before any query, replay, or mutation."""

        ...

    def close(self) -> None:
        """Release bridge streams and reject future callback delivery."""

        ...

    def health(self) -> QmtBridgeHealth:
        """Return Edge/ring/QMT health without placing an order."""

        ...

    def submit(self, command: QmtSubmitCommand) -> QmtBridgeSubmitReceipt:
        """Persist one command at Edge and publish it to the native path."""

        ...

    def cancel(
        self, request: CancelRequest, identity: VenueOrderIdentity
    ) -> QmtBridgeCancelReceipt:
        """Persist one exact-target cancel at Edge."""

        ...

    def query(self, request: BrokerQuery) -> QueryHandle:
        """Start one read-only QMT query through Edge."""

        ...

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        """Start one read-only recovery collection through Edge."""

        ...

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        """Return only completeness-explicit recovery evidence."""

        ...

    def acknowledge(self, delivery_id: int) -> None:
        """Advance Edge outbox only after durable observer delivery."""

        ...
