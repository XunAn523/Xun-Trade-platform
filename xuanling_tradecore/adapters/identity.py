"""Vendor-neutral client-reference allocation and venue-identity lifecycle contracts.

The client reference that a venue receives is allocated by EMS.  It is never a
strategy parameter, never a consumer-provided retry key, and never recovered
from PostgreSQL.  M0 keeps these values as pure contracts and deterministic
test doubles; M1 replaces the in-memory recovery input with verified Journal
facts before a real adapter may dispatch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Protocol, runtime_checkable

from ..domain import TradingDayEpoch, VenueOrderIdentity
from ..domain._validation import (
    DomainValidationError,
    require_nonnegative,
    require_positive,
    require_text,
)


class ClientOrderRefAllocationError(DomainValidationError):
    """A client-order reference cannot be safely allocated or recovered."""


def canonical_venue_order_identity_hash(identity: VenueOrderIdentity) -> str:
    """Return the versioned digest used to bind a cancel route to one order."""

    payload = {
        "adapter_client_scope": identity.adapter_client_scope,
        "adapter_instance_id": identity.adapter_instance_id,
        "client_order_ref": identity.client_order_ref,
        "schema_version": 1,
        "venue_cancel_id": identity.venue_cancel_id,
        "venue_id": identity.venue_id,
        "venue_order_id": identity.venue_order_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
    return sha256(encoded).hexdigest()


def canonical_venue_order_target_hash(identity: VenueOrderIdentity) -> str:
    """Return the stable target-order digest used before a cancel has an ID.

    A cancel route is created before the venue allocates ``venue_cancel_id``.
    Its target proof therefore deliberately excludes that later result while
    retaining the client and venue-order identifiers that identify the order
    being canceled.
    """

    payload = {
        "adapter_client_scope": identity.adapter_client_scope,
        "adapter_instance_id": identity.adapter_instance_id,
        "client_order_ref": identity.client_order_ref,
        "schema_version": 1,
        "venue_id": identity.venue_id,
        "venue_order_id": identity.venue_order_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
    return sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ClientOrderRefScope:
    """The exact account/day/adapter namespace in which a reference is unique."""

    account_id: str
    trading_day: TradingDayEpoch
    venue_id: str
    adapter_instance_id: str
    adapter_client_scope: str

    def __post_init__(self) -> None:
        require_text(self.account_id, "client order ref account_id")
        require_text(self.venue_id, "client order ref venue_id")
        require_text(self.adapter_instance_id, "client order ref adapter_instance_id")
        require_text(self.adapter_client_scope, "client order ref adapter_client_scope")


@dataclass(frozen=True)
class ClientOrderRefAllocationRequest:
    """The only caller-supplied input for one EMS-generated client reference."""

    execution_id: str
    scope: ClientOrderRefScope

    def __post_init__(self) -> None:
        require_text(self.execution_id, "client order ref execution_id")


@dataclass(frozen=True)
class ClientOrderRefLease:
    """One allocated reference and its monotonic Journal recovery sequence."""

    execution_id: str
    scope: ClientOrderRefScope
    client_order_ref: str
    allocation_sequence: int

    def __post_init__(self) -> None:
        require_text(self.execution_id, "client order ref lease execution_id")
        require_text(self.client_order_ref, "client order ref lease client_order_ref")
        require_positive(self.allocation_sequence, "client order ref allocation_sequence")


@dataclass(frozen=True)
class ClientOrderRefRecoveryInput:
    """A complete, already-verified Journal view used to reopen one scope.

    This value deliberately contains only identifiers and monotonic allocation
    facts.  Its producer must be the M1 verified Journal Reader; callers may
    not synthesize a recovery result from a PG projection or a live query.
    """

    recovery_evidence_id: str
    journal_id: str
    scope: ClientOrderRefScope
    committed_allocation_sequence: int
    leases: tuple[ClientOrderRefLease, ...]
    is_complete: bool

    def __post_init__(self) -> None:
        require_text(self.recovery_evidence_id, "client order ref recovery_evidence_id")
        require_text(self.journal_id, "client order ref journal_id")
        require_nonnegative(
            self.committed_allocation_sequence,
            "client order ref committed_allocation_sequence",
        )
        leases = tuple(self.leases)
        for lease in leases:
            if lease.scope != self.scope:
                raise ClientOrderRefAllocationError(
                    "recovery lease has another client order ref scope"
                )
            if lease.allocation_sequence > self.committed_allocation_sequence:
                raise ClientOrderRefAllocationError(
                    "recovery lease exceeds committed allocation sequence"
                )
        execution_ids = tuple(lease.execution_id for lease in leases)
        refs = tuple(lease.client_order_ref for lease in leases)
        sequences = tuple(lease.allocation_sequence for lease in leases)
        if len(set(execution_ids)) != len(execution_ids):
            raise ClientOrderRefAllocationError("recovery input repeats an execution_id")
        if len(set(refs)) != len(refs):
            raise ClientOrderRefAllocationError("recovery input repeats a client order ref")
        if len(set(sequences)) != len(sequences):
            raise ClientOrderRefAllocationError("recovery input repeats an allocation sequence")
        object.__setattr__(self, "leases", leases)


@runtime_checkable
class ClientOrderRefAllocator(Protocol):
    """The M1 Journal-backed allocation boundary for one adapter identity scope."""

    @property
    def scope(self) -> ClientOrderRefScope:
        """Return the immutable scope in which this allocator may issue references."""

        ...

    def recover(self, recovery: ClientOrderRefRecoveryInput) -> None:
        """Open the scope from complete, verified Journal allocation facts only."""

        ...

    def allocate(self, request: ClientOrderRefAllocationRequest) -> ClientOrderRefLease:
        """Allocate or idempotently return the reference for one execution id."""

        ...


@runtime_checkable
class VenueOrderIdentityLifecycle(Protocol):
    """Create immutable native identities without letting callers supply native keys."""

    def allocated(self, lease: ClientOrderRefLease) -> VenueOrderIdentity:
        """Create the first identity from the EMS allocation only."""

        ...

    def observe_venue_order_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_order_id: str,
    ) -> VenueOrderIdentity:
        """Bind a venue order id once it has been received from the adapter boundary."""

        ...

    def observe_venue_cancel_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_cancel_id: str,
    ) -> VenueOrderIdentity:
        """Bind a venue cancel id once it has been received from the adapter boundary."""

        ...


@dataclass(frozen=True)
class CanonicalVenueOrderIdentityLifecycle:
    """Pure reference lifecycle implementation shared by deterministic test adapters."""

    scope: ClientOrderRefScope

    def allocated(self, lease: ClientOrderRefLease) -> VenueOrderIdentity:
        """Create an identity that contains only the EMS-issued client reference."""

        self._require_scope(lease.scope)
        return VenueOrderIdentity(
            venue_id=self.scope.venue_id,
            adapter_instance_id=self.scope.adapter_instance_id,
            adapter_client_scope=self.scope.adapter_client_scope,
            client_order_ref=lease.client_order_ref,
        )

    def observe_venue_order_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_order_id: str,
    ) -> VenueOrderIdentity:
        """Append one immutable venue order identity without overwriting a conflict."""

        self._require_identity(identity)
        require_text(venue_order_id, "venue order id")
        if identity.venue_order_id is not None and identity.venue_order_id != venue_order_id:
            raise ClientOrderRefAllocationError("venue order identity cannot be overwritten")
        return VenueOrderIdentity(
            venue_id=identity.venue_id,
            adapter_instance_id=identity.adapter_instance_id,
            adapter_client_scope=identity.adapter_client_scope,
            client_order_ref=identity.client_order_ref,
            venue_order_id=venue_order_id,
            venue_cancel_id=identity.venue_cancel_id,
        )

    def observe_venue_cancel_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_cancel_id: str,
    ) -> VenueOrderIdentity:
        """Append one immutable venue cancel identity without overwriting a conflict."""

        self._require_identity(identity)
        require_text(venue_cancel_id, "venue cancel id")
        if identity.venue_cancel_id is not None and identity.venue_cancel_id != venue_cancel_id:
            raise ClientOrderRefAllocationError("venue cancel identity cannot be overwritten")
        return VenueOrderIdentity(
            venue_id=identity.venue_id,
            adapter_instance_id=identity.adapter_instance_id,
            adapter_client_scope=identity.adapter_client_scope,
            client_order_ref=identity.client_order_ref,
            venue_order_id=identity.venue_order_id,
            venue_cancel_id=venue_cancel_id,
        )

    def _require_scope(self, scope: ClientOrderRefScope) -> None:
        if scope != self.scope:
            raise ClientOrderRefAllocationError("client order ref lease has another scope")

    def _require_identity(self, identity: VenueOrderIdentity) -> None:
        if (
            identity.venue_id != self.scope.venue_id
            or identity.adapter_instance_id != self.scope.adapter_instance_id
            or identity.adapter_client_scope != self.scope.adapter_client_scope
        ):
            raise ClientOrderRefAllocationError("venue order identity has another scope")
