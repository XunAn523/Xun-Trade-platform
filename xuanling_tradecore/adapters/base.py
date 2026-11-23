"""Stable, venue-neutral BrokerAdapter contract.

An adapter is the only layer that may later translate TradeCore domain values
to an EMT, QMT, or PaperBroker implementation.  This M0 module contains only
contracts and immutable data; it performs no broker I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ..domain import (
    BrokerCapabilities,
    BrokerEvent,
    BrokerRecoveryEvidence,
    CallbackGapEvidence,
    CancelRequest,
    ExecutionInstruction,
    RecoveryPlan,
    VenueOrderIdentity,
)
from ..domain._validation import (
    DomainValidationError,
    require_nonnegative,
    require_positive,
    require_text,
)


class BrokerAdapterError(RuntimeError):
    """A normalized adapter boundary error without a raw SDK payload."""


@dataclass(frozen=True)
class SubmitAccepted:
    """Adapter-level acceptance of one already-authorized submit dispatch."""

    execution_id: str
    adapter_id: str
    adapter_instance_id: str
    accepted_monotonic_ns: int
    venue_order_identity: VenueOrderIdentity | None = None

    def __post_init__(self) -> None:
        require_text(self.execution_id, "submit accepted execution_id")
        require_text(self.adapter_id, "submit accepted adapter_id")
        require_text(self.adapter_instance_id, "submit accepted adapter_instance_id")
        require_nonnegative(self.accepted_monotonic_ns, "submit accepted time")
        if self.venue_order_identity is not None:
            if self.venue_order_identity.adapter_instance_id != self.adapter_instance_id:
                raise DomainValidationError("submit accepted identity has another adapter instance")


@dataclass(frozen=True)
class CancelExecutionInstruction:
    """The EMS cancel dispatch bound to a proven venue order identity."""

    cancel_request: CancelRequest
    venue_order_identity: VenueOrderIdentity

    def __post_init__(self) -> None:
        if self.cancel_request.account_id != self.cancel_request.consumer_trace.account_id:
            raise DomainValidationError("cancel execution account does not match consumer trace")


@dataclass(frozen=True)
class CancelAccepted:
    """Adapter-level acceptance of one already-authorized cancel dispatch."""

    cancel_request_id: str
    adapter_id: str
    adapter_instance_id: str
    accepted_monotonic_ns: int
    venue_order_identity: VenueOrderIdentity | None = None

    def __post_init__(self) -> None:
        require_text(self.cancel_request_id, "cancel accepted cancel_request_id")
        require_text(self.adapter_id, "cancel accepted adapter_id")
        require_text(self.adapter_instance_id, "cancel accepted adapter_instance_id")
        require_nonnegative(self.accepted_monotonic_ns, "cancel accepted time")
        if self.venue_order_identity is not None:
            if self.venue_order_identity.adapter_instance_id != self.adapter_instance_id:
                raise DomainValidationError("cancel accepted identity has another adapter instance")


@dataclass(frozen=True)
class BrokerQuery:
    """A bounded, account-scoped read-only broker query request."""

    request_id: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    query_kind: str
    page_size: int
    continuation: int = 0
    capture_authoritative_order_rows: bool = False

    def __post_init__(self) -> None:
        require_text(self.request_id, "broker query request_id")
        require_text(self.account_id, "broker query account_id")
        require_text(self.venue_id, "broker query venue_id")
        require_text(self.adapter_instance_id, "broker query adapter_instance_id")
        require_text(self.query_kind, "broker query query_kind")
        require_positive(self.page_size, "broker query page_size")
        require_nonnegative(self.continuation, "broker query continuation")
        if type(self.capture_authoritative_order_rows) is not bool:
            raise DomainValidationError(
                "broker query capture_authoritative_order_rows must be boolean"
            )
        if self.capture_authoritative_order_rows and self.query_kind not in {
            "orders",
            "orders_by_page",
            "unfinished_orders",
        }:
            raise DomainValidationError(
                "broker query authoritative row capture requires an approved order query"
            )


@dataclass(frozen=True)
class QueryHandle:
    """A stable handle whose terminality is reported by normalized BrokerEvent facts."""

    request_id: str
    adapter_id: str
    adapter_instance_id: str

    def __post_init__(self) -> None:
        require_text(self.request_id, "query handle request_id")
        require_text(self.adapter_id, "query handle adapter_id")
        require_text(self.adapter_instance_id, "query handle adapter_instance_id")


@dataclass(frozen=True)
class RecoveryHandle:
    """A handle for a read-only adapter recovery run."""

    recovery_run_id: str
    adapter_id: str
    adapter_instance_id: str

    def __post_init__(self) -> None:
        require_text(self.recovery_run_id, "recovery handle recovery_run_id")
        require_text(self.adapter_id, "recovery handle adapter_id")
        require_text(self.adapter_instance_id, "recovery handle adapter_instance_id")


@runtime_checkable
class BrokerEventObserver(Protocol):
    """The callback ingress owned by the OMS/EMS side of an adapter boundary."""

    def on_broker_event(self, event: BrokerEvent) -> None:
        """Receive one normalized, source-keyed broker fact."""

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        """Receive a conservative callback-ingress gap that requires reconciliation."""


@runtime_checkable
class BrokerAdapter(Protocol):
    """The only vendor-pluggable execution and recovery boundary.

    ``submit`` and ``cancel`` mean that the adapter has accepted a dispatch,
    not that an order is working, cancelled, or filled.  Those outcomes arrive
    only through ``BrokerEventObserver``.  Implementations must preserve order
    for a single execution and never expose raw SDK objects across this port.
    """

    @property
    def adapter_id(self) -> str:
        """Return the immutable adapter implementation identity."""

        ...

    @property
    def adapter_instance_id(self) -> str:
        """Return the configured instance/profile identity."""

        ...

    @property
    def capabilities(self) -> BrokerCapabilities:
        """Return the explicit, versioned capability declaration."""

        ...

    def start(self, observer: BrokerEventObserver) -> None:
        """Attach the durable observer before login, replay, or query activity."""

        ...

    def stop(self) -> None:
        """Stop new adapter work without claiming broker state transitions."""

        ...

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        """Dispatch one EMS-authorized instruction after the M1 Journal commit."""

        ...

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        """Dispatch one EMS-authorized cancel after the M1 Journal commit."""

        ...

    def query(self, request: BrokerQuery) -> QueryHandle:
        """Begin a read-only query whose facts arrive through the observer."""

        ...

    def begin_recovery(
        self,
        plan: RecoveryPlan,
        observer: BrokerEventObserver,
    ) -> RecoveryHandle:
        """Start an authorized recovery read scope with ingress already attached."""

        ...

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        """Return completeness-explicit evidence; empty collections are not proof."""

        ...
