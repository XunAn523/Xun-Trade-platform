"""Vendor-neutral route-token codecs and deterministic M0 directory test double."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Protocol, runtime_checkable

from ..domain import AdapterRouteToken, RouteDirectoryCapacityPlan, RouteTokenBinding
from ..domain._validation import require_nonnegative, require_sha256, require_text
from .identity import ClientOrderRefScope


class RouteTokenCodecError(ValueError):
    """A route token cannot be encoded, extracted, normalized, or recovered safely."""


@runtime_checkable
class AdapterRouteTokenCodec(Protocol):
    """Profile-owned conversion between an opaque token and one native field."""

    @property
    def binding(self) -> RouteTokenBinding:
        """Return the exact field/path contract implemented by this codec."""

        ...

    def encode(
        self,
        token: AdapterRouteToken,
        *,
        allocated_client_order_ref: str | None = None,
    ) -> str:
        """Encode a core token without accepting caller-controlled business text."""

        ...

    def normalize_extracted(
        self,
        *,
        path: str,
        native_value: object,
        expected_token: AdapterRouteToken | None,
    ) -> AdapterRouteToken:
        """Normalize one declared callback/query/replay field or fail closed."""

        ...


@dataclass(frozen=True)
class Sha256AdapterRouteTokenFactory:
    """Pure M0 token factory; M1 persists the result before native dispatch."""

    namespace_hash: str
    token_version: int = 1

    def __post_init__(self) -> None:
        require_sha256(self.namespace_hash, "route token namespace_hash")
        if self.token_version != 1:
            raise RouteTokenCodecError("M0 route token factory supports only token version 1")

    def create(
        self,
        *,
        execution_id: str,
        scope: ClientOrderRefScope,
        allocation_nonce: int,
    ) -> AdapterRouteToken:
        """Hash length-prefixed identity inputs into 128 opaque bits."""

        execution = require_text(execution_id, "route token execution_id").encode("utf-8")
        nonce = require_nonnegative(allocation_nonce, "route token allocation_nonce")
        if nonce > (1 << 64) - 1:
            raise RouteTokenCodecError("route token allocation_nonce exceeds uint64")
        fields = (
            bytes.fromhex(self.namespace_hash),
            execution,
            scope.account_id.encode("utf-8"),
            scope.trading_day.value.encode("utf-8"),
            scope.venue_id.encode("utf-8"),
            scope.adapter_instance_id.encode("utf-8"),
            scope.adapter_client_scope.encode("utf-8"),
            struct.pack(">Q", nonce),
        )
        digest = sha256()
        for field_value in fields:
            digest.update(struct.pack(">I", len(field_value)))
            digest.update(field_value)
        return AdapterRouteToken(version=self.token_version, token_bytes=digest.digest()[:16])


@dataclass(frozen=True)
class RouteTokenDirectoryEntry:
    """Journal-recoverable binding between an execution, token, and native reference."""

    execution_id: str
    scope: ClientOrderRefScope
    token: AdapterRouteToken
    client_order_ref: str
    binding_id: str
    allocation_sequence: int
    tombstoned: bool = False

    def __post_init__(self) -> None:
        require_text(self.execution_id, "route directory execution_id")
        require_text(self.client_order_ref, "route directory client_order_ref")
        require_text(self.binding_id, "route directory binding_id")
        if self.allocation_sequence <= 0:
            raise RouteTokenCodecError("route directory allocation_sequence must be positive")


@dataclass(frozen=True)
class RouteTokenDirectoryRecoveryInput:
    """Complete verified Journal input; PostgreSQL and broker queries are forbidden sources."""

    recovery_evidence_id: str
    journal_id: str
    scope: ClientOrderRefScope
    entries: tuple[RouteTokenDirectoryEntry, ...]
    is_complete: bool

    def __post_init__(self) -> None:
        require_text(self.recovery_evidence_id, "route directory recovery_evidence_id")
        require_text(self.journal_id, "route directory journal_id")
        entries = tuple(self.entries)
        if any(entry.scope != self.scope for entry in entries):
            raise RouteTokenCodecError("route directory recovery contains another scope")
        for field_name, values in (
            ("execution_id", tuple(entry.execution_id for entry in entries)),
            ("token", tuple(entry.token.to_text() for entry in entries)),
            ("client_order_ref", tuple(entry.client_order_ref for entry in entries)),
            ("allocation_sequence", tuple(str(entry.allocation_sequence) for entry in entries)),
        ):
            if len(values) != len(set(values)):
                raise RouteTokenCodecError(f"route directory recovery repeats {field_name}")
        object.__setattr__(self, "entries", entries)


@dataclass(frozen=True)
class RouteDirectoryCapacitySnapshot:
    active_entries: int
    tombstone_entries: int
    total_bytes: int
    gc_lag_seconds: int
    client_ref_headroom: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("active_entries", self.active_entries),
            ("tombstone_entries", self.tombstone_entries),
            ("total_bytes", self.total_bytes),
            ("gc_lag_seconds", self.gc_lag_seconds),
            ("client_ref_headroom", self.client_ref_headroom),
        ):
            require_nonnegative(value, f"route capacity {field_name}")


class RouteDirectoryAdmission(str, Enum):
    AVAILABLE = "AVAILABLE"
    WARN = "WARN"
    STOP_NEW = "STOP_NEW"


def evaluate_route_directory_admission(
    plan: RouteDirectoryCapacityPlan,
    snapshot: RouteDirectoryCapacitySnapshot,
) -> RouteDirectoryAdmission:
    """Evaluate only the new-order gate; callbacks, cancels, and recovery do not wait here."""

    values = {
        "active": snapshot.active_entries,
        "tombstones": snapshot.tombstone_entries,
        "total_bytes": snapshot.total_bytes,
        "gc_lag_seconds": snapshot.gc_lag_seconds,
        "client_ref_headroom": snapshot.client_ref_headroom,
    }
    if plan.stop_new_required(**values):
        return RouteDirectoryAdmission.STOP_NEW
    byte_ratio = snapshot.total_bytes * 10_000 // plan.max_total_bytes
    utilization = max(
        plan.utilization_basis_points(
            active=snapshot.active_entries,
            tombstones=snapshot.tombstone_entries,
        ),
        byte_ratio,
    )
    return (
        RouteDirectoryAdmission.WARN
        if utilization >= plan.warn_basis_points
        else RouteDirectoryAdmission.AVAILABLE
    )


def _empty_entries_by_execution() -> dict[str, RouteTokenDirectoryEntry]:
    return {}


@dataclass
class InMemoryRouteTokenDirectory:
    """Deterministic no-I/O fake proving recovery, collision, and capacity semantics."""

    scope: ClientOrderRefScope
    capacity_plan: RouteDirectoryCapacityPlan
    _recovered: bool = field(default=False, init=False, repr=False)
    _entries_by_execution: dict[str, RouteTokenDirectoryEntry] = field(
        default_factory=_empty_entries_by_execution,
        init=False,
        repr=False,
    )

    def recover(self, recovery: RouteTokenDirectoryRecoveryInput) -> None:
        if self._recovered:
            raise RouteTokenCodecError("route directory is already recovered")
        if recovery.scope != self.scope:
            raise RouteTokenCodecError("route directory recovery has another scope")
        if not recovery.is_complete:
            raise RouteTokenCodecError("route directory recovery input is incomplete")
        self._entries_by_execution = {entry.execution_id: entry for entry in recovery.entries}
        self._recovered = True

    def register_new(
        self,
        entry: RouteTokenDirectoryEntry,
        capacity: RouteDirectoryCapacitySnapshot,
    ) -> RouteTokenDirectoryEntry:
        if not self._recovered:
            raise RouteTokenCodecError("route directory requires complete Journal recovery")
        if entry.scope != self.scope:
            raise RouteTokenCodecError("route directory entry has another scope")
        existing = self._entries_by_execution.get(entry.execution_id)
        if existing is not None:
            if existing != entry:
                raise RouteTokenCodecError("execution already has another route token entry")
            return existing
        if (
            evaluate_route_directory_admission(self.capacity_plan, capacity)
            is RouteDirectoryAdmission.STOP_NEW
        ):
            raise RouteTokenCodecError("route directory capacity requires STOP_NEW")
        if any(
            current.token == entry.token or current.client_order_ref == entry.client_order_ref
            for current in self._entries_by_execution.values()
        ):
            raise RouteTokenCodecError("route token or client reference collision")
        self._entries_by_execution[entry.execution_id] = entry
        return entry

    def find_by_client_order_ref(self, client_order_ref: str) -> RouteTokenDirectoryEntry | None:
        require_text(client_order_ref, "route directory lookup client_order_ref")
        for entry in self._entries_by_execution.values():
            if entry.client_order_ref == client_order_ref:
                return entry
        return None
