"""Journal ports for durable client references and venue order identities.

M1-02 establishes the recovery and commit boundary without choosing a storage
medium.  ``InMemoryClientOrderRefJournal`` is a deterministic test double only.
M1-03 will implement these same ports from verified PersistentHotJournal facts;
neither PostgreSQL nor a broker query is an allowed recovery source.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock
from typing import Callable, Protocol, runtime_checkable

from ..adapters.identity import (
    CanonicalVenueOrderIdentityLifecycle,
    ClientOrderRefAllocationError,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
    ClientOrderRefScope,
    VenueOrderIdentityLifecycle,
)
from ..domain import VenueOrderIdentity
from ..domain._validation import require_positive, require_text


@dataclass(frozen=True)
class ClientOrderRefJournalCommit:
    """A committed allocation fact that an allocator may now retain in memory."""

    journal_id: str
    commit_sequence: int
    lease: ClientOrderRefLease

    def __post_init__(self) -> None:
        require_text(self.journal_id, "client order ref Journal id")
        require_positive(self.commit_sequence, "client order ref Journal commit sequence")


@runtime_checkable
class ClientOrderRefJournal(Protocol):
    """The verified Journal boundary for one venue client-reference namespace."""

    def read_recovery(self, scope: ClientOrderRefScope) -> ClientOrderRefRecoveryInput:
        """Return only a complete or explicitly incomplete Journal-derived recovery view."""

        ...

    def append_lease(self, lease: ClientOrderRefLease) -> ClientOrderRefJournalCommit:
        """Commit one lease before an allocator exposes it to any caller."""

        ...

    def read_committed_lease(
        self,
        scope: ClientOrderRefScope,
        execution_id: str,
    ) -> ClientOrderRefLease | None:
        """Return an exact committed lease without searching another scope or epoch."""

        ...


@dataclass(frozen=True)
class VenueOrderIdentityJournalCommit:
    """A committed normalized venue identity fact for the exact client-ref scope."""

    journal_id: str
    commit_sequence: int
    scope: ClientOrderRefScope
    identity: VenueOrderIdentity

    def __post_init__(self) -> None:
        require_text(self.journal_id, "venue identity Journal id")
        require_positive(self.commit_sequence, "venue identity Journal commit sequence")
        if self.identity.client_order_ref is None:
            raise ClientOrderRefAllocationError(
                "venue identity Journal requires a client order ref"
            )
        _require_identity_scope(self.scope, self.identity)


@runtime_checkable
class VenueOrderIdentityJournal(Protocol):
    """The Journal persistence boundary for immutable native-order identities."""

    def read_venue_identity(
        self,
        scope: ClientOrderRefScope,
        client_order_ref: str,
    ) -> VenueOrderIdentity | None:
        """Read only an exact account/day/adapter/reference identity; never guess globally."""

        ...

    def append_venue_identity(
        self,
        scope: ClientOrderRefScope,
        identity: VenueOrderIdentity,
    ) -> VenueOrderIdentityJournalCommit:
        """Commit a monotonic identity enrichment or fail without overwriting history."""

        ...


class InMemoryClientOrderRefJournal(ClientOrderRefJournal, VenueOrderIdentityJournal):
    """Thread-safe contract-test Journal; production must use PersistentHotJournal."""

    def __init__(self, journal_id: str = "m1-02-in-memory") -> None:
        require_text(journal_id, "in-memory client order ref Journal id")
        self._journal_id = journal_id
        self._lock = RLock()
        self._next_commit_sequence = 1
        self._leases: dict[ClientOrderRefScope, dict[str, ClientOrderRefJournalCommit]] = {}
        self._identities: dict[
            tuple[ClientOrderRefScope, str], VenueOrderIdentityJournalCommit
        ] = {}
        self._identity_history: list[VenueOrderIdentityJournalCommit] = []
        self._complete_scopes: dict[ClientOrderRefScope, bool] = {}

    @property
    def lease_records(self) -> tuple[ClientOrderRefJournalCommit, ...]:
        """Return committed lease facts in their Journal commit order."""

        with self._lock:
            return tuple(
                sorted(
                    (
                        record
                        for by_execution in self._leases.values()
                        for record in by_execution.values()
                    ),
                    key=lambda record: record.commit_sequence,
                )
            )

    @property
    def identity_records(self) -> tuple[VenueOrderIdentityJournalCommit, ...]:
        """Return committed identity enrichments in Journal commit order."""

        with self._lock:
            return tuple(self._identity_history)

    def mark_recovery_complete(self, scope: ClientOrderRefScope, *, is_complete: bool) -> None:
        """Expose an explicit incomplete-recovery condition for fail-closed tests."""

        with self._lock:
            self._complete_scopes[scope] = is_complete

    def read_recovery(self, scope: ClientOrderRefScope) -> ClientOrderRefRecoveryInput:
        """Construct a recovery input solely from this test Journal's committed leases."""

        with self._lock:
            leases = tuple(
                record.lease
                for record in sorted(
                    self._leases.get(scope, {}).values(),
                    key=lambda record: record.lease.allocation_sequence,
                )
            )
            watermark = max((lease.allocation_sequence for lease in leases), default=0)
            return ClientOrderRefRecoveryInput(
                recovery_evidence_id=f"{self._journal_id}:client-order-ref-recovery",
                journal_id=self._journal_id,
                scope=scope,
                committed_allocation_sequence=watermark,
                leases=leases,
                is_complete=self._complete_scopes.get(scope, True),
            )

    def append_lease(self, lease: ClientOrderRefLease) -> ClientOrderRefJournalCommit:
        """Commit one lease once; conflicting execution/reference reuse fails closed."""

        with self._lock:
            by_execution = self._leases.setdefault(lease.scope, {})
            existing = by_execution.get(lease.execution_id)
            if existing is not None:
                if existing.lease != lease:
                    raise ClientOrderRefAllocationError(
                        "Journal execution already has another client order ref lease"
                    )
                return existing
            for committed in by_execution.values():
                current = committed.lease
                if current.client_order_ref == lease.client_order_ref:
                    raise ClientOrderRefAllocationError("Journal repeats a client order ref")
                if current.allocation_sequence == lease.allocation_sequence:
                    raise ClientOrderRefAllocationError("Journal repeats an allocation sequence")
            last_sequence = max(
                (record.lease.allocation_sequence for record in by_execution.values()),
                default=0,
            )
            if lease.allocation_sequence <= last_sequence:
                raise ClientOrderRefAllocationError(
                    "Journal allocation sequence does not advance monotonically"
                )
            record = ClientOrderRefJournalCommit(
                journal_id=self._journal_id,
                commit_sequence=self._allocate_commit_sequence(),
                lease=lease,
            )
            by_execution[lease.execution_id] = record
            return record

    def read_committed_lease(
        self,
        scope: ClientOrderRefScope,
        execution_id: str,
    ) -> ClientOrderRefLease | None:
        """Read a committed lease in its exact account/day/adapter scope only."""

        require_text(execution_id, "client order ref lookup execution id")
        with self._lock:
            existing = self._leases.get(scope, {}).get(execution_id)
            return None if existing is None else existing.lease

    def read_venue_identity(
        self,
        scope: ClientOrderRefScope,
        client_order_ref: str,
    ) -> VenueOrderIdentity | None:
        """Return an exact scope match, never a same-number match from another epoch."""

        require_text(client_order_ref, "venue identity lookup client order ref")
        with self._lock:
            existing = self._identities.get((scope, client_order_ref))
            return None if existing is None else existing.identity

    def append_venue_identity(
        self,
        scope: ClientOrderRefScope,
        identity: VenueOrderIdentity,
    ) -> VenueOrderIdentityJournalCommit:
        """Persist only monotonic additions to an exact client-reference identity."""

        _require_identity_scope(scope, identity)
        client_order_ref = identity.client_order_ref
        assert client_order_ref is not None  # checked by _require_identity_scope
        key = (scope, client_order_ref)
        with self._lock:
            existing = self._identities.get(key)
            if existing is not None:
                if existing.identity == identity:
                    return existing
                _require_identity_enrichment(existing.identity, identity)
            record = VenueOrderIdentityJournalCommit(
                journal_id=self._journal_id,
                commit_sequence=self._allocate_commit_sequence(),
                scope=scope,
                identity=identity,
            )
            self._identities[key] = record
            self._identity_history.append(record)
            return record

    def _allocate_commit_sequence(self) -> int:
        sequence = self._next_commit_sequence
        self._next_commit_sequence += 1
        return sequence


@dataclass(frozen=True)
class JournalBackedVenueOrderIdentityLifecycle(VenueOrderIdentityLifecycle):
    """Persist every identity allocation/enrichment before returning it to EMS."""

    scope: ClientOrderRefScope
    lease_journal: ClientOrderRefJournal
    identity_journal: VenueOrderIdentityJournal
    _delegate: CanonicalVenueOrderIdentityLifecycle = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_delegate", CanonicalVenueOrderIdentityLifecycle(self.scope))

    def allocated(self, lease: ClientOrderRefLease) -> VenueOrderIdentity:
        """Return a prior committed identity or commit its initial client reference."""

        candidate = self._delegate.allocated(lease)
        committed_lease = self.lease_journal.read_committed_lease(self.scope, lease.execution_id)
        if committed_lease != lease:
            raise ClientOrderRefAllocationError(
                "venue identity requires an exact committed client order ref lease"
            )
        client_order_ref = candidate.client_order_ref
        assert client_order_ref is not None
        existing = self.identity_journal.read_venue_identity(self.scope, client_order_ref)
        if existing is not None:
            _require_identity_enrichment(candidate, existing)
            return existing
        self.identity_journal.append_venue_identity(self.scope, candidate)
        return candidate

    def observe_venue_order_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_order_id: str,
    ) -> VenueOrderIdentity:
        """Commit a venue order id before exposing its enriched identity."""

        return self._persist_enrichment(
            identity,
            lambda current: self._delegate.observe_venue_order_id(
                current, venue_order_id=venue_order_id
            ),
        )

    def observe_venue_cancel_id(
        self,
        identity: VenueOrderIdentity,
        *,
        venue_cancel_id: str,
    ) -> VenueOrderIdentity:
        """Commit a venue cancel id before exposing its enriched identity."""

        return self._persist_enrichment(
            identity,
            lambda current: self._delegate.observe_venue_cancel_id(
                current, venue_cancel_id=venue_cancel_id
            ),
        )

    def _persist_enrichment(
        self,
        identity: VenueOrderIdentity,
        operation: Callable[[VenueOrderIdentity], VenueOrderIdentity],
    ) -> VenueOrderIdentity:
        _require_identity_scope(self.scope, identity)
        client_order_ref = identity.client_order_ref
        assert client_order_ref is not None
        existing = self.identity_journal.read_venue_identity(self.scope, client_order_ref)
        if existing is None:
            raise ClientOrderRefAllocationError("venue identity is not committed in the Journal")
        if existing != identity:
            raise ClientOrderRefAllocationError(
                "venue identity does not match the committed Journal fact"
            )
        candidate = operation(existing)
        self.identity_journal.append_venue_identity(self.scope, candidate)
        return candidate


def _require_identity_scope(scope: ClientOrderRefScope, identity: VenueOrderIdentity) -> None:
    if identity.client_order_ref is None:
        raise ClientOrderRefAllocationError("venue identity requires client order ref persistence")
    if (
        identity.venue_id != scope.venue_id
        or identity.adapter_instance_id != scope.adapter_instance_id
        or identity.adapter_client_scope != scope.adapter_client_scope
    ):
        raise ClientOrderRefAllocationError("venue identity has another client order ref scope")


def _require_identity_enrichment(
    existing: VenueOrderIdentity,
    candidate: VenueOrderIdentity,
) -> None:
    if (
        existing.venue_id != candidate.venue_id
        or existing.adapter_instance_id != candidate.adapter_instance_id
        or existing.adapter_client_scope != candidate.adapter_client_scope
        or existing.client_order_ref != candidate.client_order_ref
    ):
        raise ClientOrderRefAllocationError("venue identity cannot change its allocation scope")
    for field_name in ("venue_order_id", "venue_cancel_id"):
        old = getattr(existing, field_name)
        new = getattr(candidate, field_name)
        if old is not None and new != old:
            raise ClientOrderRefAllocationError(
                "venue identity Journal cannot overwrite an observed id"
            )
