"""EMT's constrained, in-memory M0 client-order-id test allocator.

The official EMT ``order_client_id`` field is a nonzero ``uint32`` allocated
by EMS.  This M0 implementation intentionally has no persistence and cannot
be wired to an SDK request; it exists to prove the future Journal recovery and
scope contract before M1 supplies the durable implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock

from ...domain import TradingDayEpoch
from ...domain._validation import require_nonnegative, require_text
from ...oms.identity_journal import ClientOrderRefJournal
from ..identity import (
    ClientOrderRefAllocationError,
    ClientOrderRefAllocationRequest,
    ClientOrderRefAllocator,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
    ClientOrderRefScope,
)

EMT_CLIENT_ORDER_ID_MAX = 2**32 - 1


def _empty_leases() -> dict[str, ClientOrderRefLease]:
    """Create the precisely typed empty allocation state for the M0 test double."""

    return {}


@dataclass(frozen=True)
class EmtClientOrderRefScope:
    """The EMT scope in which an ``order_client_id`` must never be reused."""

    account_id: str
    trading_day: TradingDayEpoch
    adapter_instance_id: str
    emt_client_id: int

    def __post_init__(self) -> None:
        require_text(self.account_id, "EMT client order ref account_id")
        require_text(self.adapter_instance_id, "EMT client order ref adapter_instance_id")
        require_nonnegative(self.emt_client_id, "EMT client_id")
        if self.emt_client_id > 255:
            raise ClientOrderRefAllocationError("EMT client_id exceeds uint8")

    @property
    def canonical_scope(self) -> ClientOrderRefScope:
        """Return the vendor-neutral scope used by the allocator contracts."""

        return ClientOrderRefScope(
            account_id=self.account_id,
            trading_day=self.trading_day,
            venue_id="EMT",
            adapter_instance_id=self.adapter_instance_id,
            adapter_client_scope=str(self.emt_client_id),
        )


@dataclass
class EmtClientOrderIdAllocator(ClientOrderRefAllocator):
    """Strict M0 test double for a future Journal-backed EMT uint32 allocator."""

    emt_scope: EmtClientOrderRefScope
    _recovered: bool = field(default=False, init=False, repr=False)
    _watermark: int = field(default=0, init=False, repr=False)
    _leases_by_execution: dict[str, ClientOrderRefLease] = field(
        default_factory=_empty_leases,
        init=False,
        repr=False,
    )

    @property
    def scope(self) -> ClientOrderRefScope:
        """Return the immutable canonical EMT client-id scope."""

        return self.emt_scope.canonical_scope

    def recover(self, recovery: ClientOrderRefRecoveryInput) -> None:
        """Accept only a complete Journal recovery input for this exact EMT scope."""

        if self._recovered:
            raise ClientOrderRefAllocationError(
                "EMT client order id allocator is already recovered"
            )
        if recovery.scope != self.scope:
            raise ClientOrderRefAllocationError(
                "EMT recovery input has another client order id scope"
            )
        if not recovery.is_complete:
            raise ClientOrderRefAllocationError("EMT client order id recovery input is incomplete")
        observed_watermark = recovery.committed_allocation_sequence
        leases_by_execution: dict[str, ClientOrderRefLease] = {}
        for lease in recovery.leases:
            native_ref = self.parse_client_order_ref(lease.client_order_ref)
            if lease.allocation_sequence != native_ref:
                raise ClientOrderRefAllocationError(
                    "EMT recovery allocation sequence does not equal order_client_id"
                )
            leases_by_execution[lease.execution_id] = lease
            observed_watermark = max(observed_watermark, native_ref)
        if observed_watermark > EMT_CLIENT_ORDER_ID_MAX:
            raise ClientOrderRefAllocationError("EMT recovery watermark exceeds uint32")
        self._leases_by_execution = leases_by_execution
        self._watermark = observed_watermark
        self._recovered = True

    def allocate(self, request: ClientOrderRefAllocationRequest) -> ClientOrderRefLease:
        """Issue one monotonic, nonzero uint32 reference after complete recovery."""

        if request.scope != self.scope:
            raise ClientOrderRefAllocationError(
                "EMT allocation request has another client order id scope"
            )
        if not self._recovered:
            raise ClientOrderRefAllocationError(
                "EMT client order id allocation requires complete Journal recovery"
            )
        existing = self._leases_by_execution.get(request.execution_id)
        if existing is not None:
            return existing
        if self._watermark >= EMT_CLIENT_ORDER_ID_MAX:
            raise ClientOrderRefAllocationError("EMT client order id uint32 space is exhausted")
        next_ref = self._watermark + 1
        lease = ClientOrderRefLease(
            execution_id=request.execution_id,
            scope=self.scope,
            client_order_ref=str(next_ref),
            allocation_sequence=next_ref,
        )
        self._watermark = next_ref
        self._leases_by_execution[lease.execution_id] = lease
        return lease

    @staticmethod
    def parse_client_order_ref(value: str) -> int:
        if not value.isascii() or not value.isdecimal():
            raise ClientOrderRefAllocationError(
                "EMT order_client_id must be an ASCII decimal uint32"
            )
        parsed = int(value)
        if parsed <= 0 or parsed > EMT_CLIENT_ORDER_ID_MAX:
            raise ClientOrderRefAllocationError("EMT order_client_id must be a nonzero uint32")
        return parsed


@dataclass
class JournalBackedEmtClientOrderIdAllocator(ClientOrderRefAllocator):
    """EMT uint32 allocator whose recovery and leases come only from a Journal.

    The allocator never queries PostgreSQL or a broker.  It first commits a
    candidate lease through ``ClientOrderRefJournal`` and only then exposes the
    corresponding in-memory mapping.  M1-03 replaces the test journal backing
    with the verified PersistentHotJournal implementation.
    """

    emt_scope: EmtClientOrderRefScope
    journal: ClientOrderRefJournal
    _recovered: bool = field(default=False, init=False, repr=False)
    _watermark: int = field(default=0, init=False, repr=False)
    _leases_by_execution: dict[str, ClientOrderRefLease] = field(
        default_factory=_empty_leases,
        init=False,
        repr=False,
    )
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    @property
    def scope(self) -> ClientOrderRefScope:
        """Return the exact account/day/EMT-client namespace for this allocator."""

        return self.emt_scope.canonical_scope

    def recover_from_journal(self) -> None:
        """Open only from the exact complete recovery view produced by this Journal."""

        self.recover(self.journal.read_recovery(self.scope))

    def recover(self, recovery: ClientOrderRefRecoveryInput) -> None:
        """Reject recovery input not byte-for-byte represented by the configured Journal."""

        with self._lock:
            if self._recovered:
                raise ClientOrderRefAllocationError(
                    "EMT Journal-backed client order id allocator is already recovered"
                )
            expected = self.journal.read_recovery(self.scope)
            if recovery != expected:
                raise ClientOrderRefAllocationError(
                    "EMT client order id recovery must come from the configured Journal"
                )
            if recovery.scope != self.scope:
                raise ClientOrderRefAllocationError(
                    "EMT recovery input has another client order id scope"
                )
            if not recovery.is_complete:
                raise ClientOrderRefAllocationError(
                    "EMT client order id recovery input is incomplete"
                )
            observed_watermark = recovery.committed_allocation_sequence
            leases_by_execution: dict[str, ClientOrderRefLease] = {}
            for lease in recovery.leases:
                native_ref = EmtClientOrderIdAllocator.parse_client_order_ref(
                    lease.client_order_ref
                )
                if lease.allocation_sequence != native_ref:
                    raise ClientOrderRefAllocationError(
                        "EMT recovery allocation sequence does not equal order_client_id"
                    )
                leases_by_execution[lease.execution_id] = lease
                observed_watermark = max(observed_watermark, native_ref)
            if observed_watermark > EMT_CLIENT_ORDER_ID_MAX:
                raise ClientOrderRefAllocationError("EMT recovery watermark exceeds uint32")
            self._watermark = observed_watermark
            self._leases_by_execution = leases_by_execution
            self._recovered = True

    def allocate(self, request: ClientOrderRefAllocationRequest) -> ClientOrderRefLease:
        """Commit then expose one monotonic nonzero ``order_client_id`` lease."""

        with self._lock:
            if request.scope != self.scope:
                raise ClientOrderRefAllocationError(
                    "EMT allocation request has another client order id scope"
                )
            if not self._recovered:
                raise ClientOrderRefAllocationError(
                    "EMT client order id allocation requires complete Journal recovery"
                )
            existing = self._leases_by_execution.get(request.execution_id)
            if existing is not None:
                return existing
            if self._watermark >= EMT_CLIENT_ORDER_ID_MAX:
                raise ClientOrderRefAllocationError("EMT client order id uint32 space is exhausted")
            next_ref = self._watermark + 1
            candidate = ClientOrderRefLease(
                execution_id=request.execution_id,
                scope=self.scope,
                client_order_ref=str(next_ref),
                allocation_sequence=next_ref,
            )
            committed = self.journal.append_lease(candidate)
            if committed.lease != candidate:
                raise ClientOrderRefAllocationError(
                    "EMT Journal committed another client order ref lease"
                )
            self._watermark = next_ref
            self._leases_by_execution[candidate.execution_id] = candidate
            return candidate
