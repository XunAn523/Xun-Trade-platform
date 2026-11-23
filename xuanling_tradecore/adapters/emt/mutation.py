"""Typed, SDK-free mutation seam for the EMT BrokerAdapter.

The concrete platform layer owns the vendor request object and the native
``insertOrder``/``cancelOrder`` calls.  TradeCore supplies only a manifest
mapped, account-fenced request and accepts only normalized numeric receipts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ...domain import CancelRequest, ExecutionInstruction
from ...domain._validation import require_nonnegative, require_positive, require_text
from ..mapping import MappedVenueOrder


class EmtMutationPortError(RuntimeError):
    """An injected EMT mutation implementation violated the adapter contract."""


@dataclass(frozen=True)
class EmtSubmitRequest:
    """One manifest-mapped order request, without a vendor request object."""

    instruction: ExecutionInstruction
    mapped_order: MappedVenueOrder
    order_client_id: int

    def __post_init__(self) -> None:
        require_positive(self.order_client_id, "EMT order_client_id")
        if self.instruction.execution_id != self.mapped_order.execution_id:
            raise EmtMutationPortError("EMT mapped order belongs to another execution")
        if self.instruction.account_id != self.mapped_order.account_id:
            raise EmtMutationPortError("EMT mapped order belongs to another account")


@dataclass(frozen=True)
class EmtSubmitReceipt:
    """A native InsertOrder acceptance, not an order-state transition."""

    order_emt_id: int
    accepted_monotonic_ns: int

    def __post_init__(self) -> None:
        require_positive(self.order_emt_id, "EMT order_emt_id")
        require_nonnegative(self.accepted_monotonic_ns, "EMT submit accepted time")


@dataclass(frozen=True)
class EmtCancelRequest:
    """One proven native cancel target, independent of the vendor binding."""

    cancel_request: CancelRequest
    order_emt_id: int

    def __post_init__(self) -> None:
        require_positive(self.order_emt_id, "EMT cancel order_emt_id")
        require_text(self.cancel_request.cancel_request_id, "EMT cancel_request_id")


@dataclass(frozen=True)
class EmtCancelReceipt:
    """A native CancelOrder acceptance, not a cancellation terminal event."""

    order_cancel_emt_id: int
    accepted_monotonic_ns: int

    def __post_init__(self) -> None:
        require_positive(self.order_cancel_emt_id, "EMT order_cancel_emt_id")
        require_nonnegative(self.accepted_monotonic_ns, "EMT cancel accepted time")


class EmtMutationPort(Protocol):
    """The platform-owned native mutation seam used after EMS durability gates."""

    def submit(self, request: EmtSubmitRequest) -> EmtSubmitReceipt:
        """Perform one permitted native InsertOrder call."""

        ...

    def cancel(self, request: EmtCancelRequest) -> EmtCancelReceipt:
        """Perform one permitted native CancelOrder call."""

        ...
