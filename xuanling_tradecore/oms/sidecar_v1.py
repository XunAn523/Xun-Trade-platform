"""Committed-fact OMS sidecar with replay-safe, immutable read generations.

This module is intentionally outside the adapter and mmap-writer call stacks.
It consumes only facts that a Journal reader has already proved committed.  It
therefore cannot turn a query, projector, manifest or EMS decision into a
dependency of the C0--C4A submit path or C5--C7 callback-durability path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from enum import Enum
from hashlib import sha256
from threading import Lock
from types import MappingProxyType
from typing import Iterable, Mapping

from ..persistence.mmap_pre_native_journal import MmapJournalRecordKind, MmapPreNativeRecord


class OmsSidecarError(RuntimeError):
    """A sidecar fact, replay stream or query generation is not safe to use."""


class OmsSidecarFactKind(str, Enum):
    """Payload-normalized facts accepted by the out-of-band projector."""

    PRE_NATIVE_COMMITTED = "PRE_NATIVE_COMMITTED"
    NATIVE_CALL_STARTED = "NATIVE_CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    ORDER_CALLBACK = "ORDER_CALLBACK"
    TRADE_CALLBACK = "TRADE_CALLBACK"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"
    CANCEL_CALLBACK = "CANCEL_CALLBACK"
    RECONCILIATION_RESULT = "RECONCILIATION_RESULT"


class OmsSidecarOrderState(str, Enum):
    """Versioned projector state; it is not a new adapter-side authority."""

    INTENT_ACCEPTED = "INTENT_ACCEPTED"
    PRE_NATIVE_COMMITTED = "PRE_NATIVE_COMMITTED"
    NATIVE_CALL_STARTED = "NATIVE_CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    LOST = "LOST"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


_TERMINAL_STATES = frozenset(
    {
        OmsSidecarOrderState.FILLED,
        OmsSidecarOrderState.CANCELLED,
        OmsSidecarOrderState.REJECTED,
        OmsSidecarOrderState.EXPIRED,
        OmsSidecarOrderState.LOST,
        OmsSidecarOrderState.RECONCILE_REQUIRED,
    }
)
_STATE_RANK = {
    OmsSidecarOrderState.INTENT_ACCEPTED: 1,
    OmsSidecarOrderState.PRE_NATIVE_COMMITTED: 2,
    OmsSidecarOrderState.NATIVE_CALL_STARTED: 3,
    OmsSidecarOrderState.NATIVE_ACCEPTED: 4,
    OmsSidecarOrderState.WORKING: 5,
    OmsSidecarOrderState.PARTIALLY_FILLED: 6,
    OmsSidecarOrderState.CANCEL_PENDING: 7,
    OmsSidecarOrderState.FILLED: 8,
    OmsSidecarOrderState.CANCELLED: 8,
    OmsSidecarOrderState.REJECTED: 8,
    OmsSidecarOrderState.EXPIRED: 8,
    OmsSidecarOrderState.LOST: 8,
    OmsSidecarOrderState.RECONCILE_REQUIRED: 99,
}


def _text(value: object, name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise OmsSidecarError(f"{name} is invalid")
    return value


def _sha256(value: str, name: str) -> str:
    value = _text(value, name, maximum=64)
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise OmsSidecarError(f"{name} must be a lower-case SHA-256")
    return value


@dataclass(frozen=True)
class OmsSidecarFactV1:
    """One committed, bounded fact with enough identity for safe replay.

    ``position`` is monotonic only inside ``scope_id``.  ``payload_hash`` is a
    normalized digest, not a raw SDK object, account secret or unbounded error
    message.  A callback may omit ``order_state`` when its compact mmap frame
    intentionally contains only recovery identity; a richer reader can later
    supply the authoritative normalized state at the same boundary.
    """

    scope_id: str
    position: int
    schema_version: int
    activation_epoch: int
    route_version: int
    kind: OmsSidecarFactKind
    execution_id: str
    canonical_client_order_ref: str
    payload_hash: str
    observed_monotonic_ns: int
    external_order_id: str | None = None
    order_state: OmsSidecarOrderState | None = None
    state_version: int = 0
    trade_key: str | None = None
    cumulative_quantity: int | None = None
    leaves_quantity: int | None = None

    def __post_init__(self) -> None:
        _text(self.scope_id, "sidecar scope_id")
        _text(self.execution_id, "sidecar execution_id")
        _text(self.canonical_client_order_ref, "sidecar client reference")
        _sha256(self.payload_hash, "sidecar payload_hash")
        if self.position < 1 or self.schema_version < 1 or self.activation_epoch < 1:
            raise OmsSidecarError("sidecar position, schema version or activation epoch is invalid")
        if self.route_version < 1 or self.state_version < 0 or self.observed_monotonic_ns < 0:
            raise OmsSidecarError("sidecar route, state version or observed time is invalid")
        if self.external_order_id is not None:
            _text(self.external_order_id, "sidecar external order id")
        if self.trade_key is not None:
            _text(self.trade_key, "sidecar trade key")
        if self.kind is OmsSidecarFactKind.TRADE_CALLBACK and self.trade_key is None:
            raise OmsSidecarError("trade callback requires a stable trade key")
        for name, value in (
            ("cumulative quantity", self.cumulative_quantity),
            ("leaves quantity", self.leaves_quantity),
        ):
            if value is not None and value < 0:
                raise OmsSidecarError(f"sidecar {name} is invalid")

    @property
    def fingerprint(self) -> str:
        """Return a canonical digest used to detect duplicate-position conflicts."""

        payload = {
            "scope_id": self.scope_id,
            "position": self.position,
            "schema_version": self.schema_version,
            "activation_epoch": self.activation_epoch,
            "route_version": self.route_version,
            "kind": self.kind.value,
            "execution_id": self.execution_id,
            "canonical_client_order_ref": self.canonical_client_order_ref,
            "payload_hash": self.payload_hash,
            "observed_monotonic_ns": self.observed_monotonic_ns,
            "external_order_id": self.external_order_id,
            "order_state": self.order_state.value if self.order_state else None,
            "state_version": self.state_version,
            "trade_key": self.trade_key,
            "cumulative_quantity": self.cumulative_quantity,
            "leaves_quantity": self.leaves_quantity,
        }
        return sha256(_canonical_json(payload)).hexdigest()


@dataclass(frozen=True)
class OmsSidecarOrderRefV1:
    """Small immutable order state and ID reference published to query readers."""

    execution_id: str
    scope_id: str
    canonical_client_order_ref: str
    state: OmsSidecarOrderState
    state_version: int
    applied_position: int
    activation_epoch: int
    route_version: int
    external_order_id: str | None = None
    cumulative_quantity: int = 0
    leaves_quantity: int | None = None
    trade_keys: frozenset[str] = frozenset()
    reconciliation_required: bool = False

    def __post_init__(self) -> None:
        _text(self.execution_id, "sidecar order execution_id")
        _text(self.scope_id, "sidecar order scope_id")
        _text(self.canonical_client_order_ref, "sidecar order client reference")
        if self.state_version < 0 or self.applied_position < 1:
            raise OmsSidecarError("sidecar order version or position is invalid")
        if (
            self.cumulative_quantity < 0
            or self.leaves_quantity is not None
            and self.leaves_quantity < 0
        ):
            raise OmsSidecarError("sidecar order quantities are invalid")
        if self.external_order_id is not None:
            _text(self.external_order_id, "sidecar order external id")
        keys = frozenset(self.trade_keys)
        for key in keys:
            _text(key, "sidecar order trade key")
        object.__setattr__(self, "trade_keys", keys)


@dataclass(frozen=True)
class OmsSidecarTradeRefV1:
    """One immutable, de-duplicated venue trade reference in a query generation."""

    trade_key: str
    execution_id: str
    scope_id: str
    external_order_id: str | None
    applied_position: int
    cumulative_quantity: int

    def __post_init__(self) -> None:
        _text(self.trade_key, "sidecar trade key")
        _text(self.execution_id, "sidecar trade execution_id")
        _text(self.scope_id, "sidecar trade scope_id")
        if self.external_order_id is not None:
            _text(self.external_order_id, "sidecar trade external order id")
        if self.applied_position < 1 or self.cumulative_quantity < 0:
            raise OmsSidecarError("sidecar trade position or quantity is invalid")


def _empty_trade_mapping() -> Mapping[str, OmsSidecarTradeRefV1]:
    return {}


def _empty_health_mapping() -> Mapping[str, str]:
    return {}


@dataclass(frozen=True)
class OmsSidecarSnapshotV1:
    """An atomically publishable, immutable OMS query generation."""

    generation: int
    as_of_monotonic_ns: int
    per_scope_applied_watermark: Mapping[str, int]
    orders_by_execution_id: Mapping[str, OmsSidecarOrderRefV1]
    orders_by_client_order_ref: Mapping[str, OmsSidecarOrderRefV1]
    orders_by_external_order_id: Mapping[str, OmsSidecarOrderRefV1]
    trades_by_key: Mapping[str, OmsSidecarTradeRefV1] = field(default_factory=_empty_trade_mapping)
    health: Mapping[str, str] = field(default_factory=_empty_health_mapping)
    reconciliation_required_execution_ids: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.generation < 0 or self.as_of_monotonic_ns < 0:
            raise OmsSidecarError("sidecar snapshot generation or time is invalid")
        watermarks = dict(self.per_scope_applied_watermark)
        if any(not scope or position < 0 for scope, position in watermarks.items()):
            raise OmsSidecarError("sidecar snapshot watermark is invalid")
        execution = dict(self.orders_by_execution_id)
        client = dict(self.orders_by_client_order_ref)
        external = dict(self.orders_by_external_order_id)
        trades: dict[str, OmsSidecarTradeRefV1] = {
            key: value for key, value in self.trades_by_key.items()
        }
        health: dict[str, str] = {key: value for key, value in self.health.items()}
        if any(key != value.execution_id for key, value in execution.items()):
            raise OmsSidecarError("sidecar execution index is inconsistent")
        if any(key != value.canonical_client_order_ref for key, value in client.items()):
            raise OmsSidecarError("sidecar client index is inconsistent")
        if any(value.external_order_id != key for key, value in external.items()):
            raise OmsSidecarError("sidecar external index is inconsistent")
        if any(key != value.trade_key for key, value in trades.items()):
            raise OmsSidecarError("sidecar trade index is inconsistent")
        if any(not key or not value for key, value in health.items()):
            raise OmsSidecarError("sidecar health index is invalid")
        required = frozenset(self.reconciliation_required_execution_ids)
        if not required.issubset(execution):
            raise OmsSidecarError("sidecar reconciliation index references an unknown order")
        object.__setattr__(self, "per_scope_applied_watermark", MappingProxyType(watermarks))
        object.__setattr__(self, "orders_by_execution_id", MappingProxyType(execution))
        object.__setattr__(self, "orders_by_client_order_ref", MappingProxyType(client))
        object.__setattr__(self, "orders_by_external_order_id", MappingProxyType(external))
        object.__setattr__(self, "trades_by_key", MappingProxyType(trades))
        object.__setattr__(self, "health", MappingProxyType(health))
        object.__setattr__(self, "reconciliation_required_execution_ids", required)

    @property
    def is_authoritative(self) -> bool:
        """Return false whenever the sidecar has a non-authoritative conflict/gap."""

        return not self.reconciliation_required_execution_ids


class OmsSidecarFactReaderV1:
    """Validate one scope's committed fact positions before projector delivery."""

    def __init__(
        self,
        *,
        scope_id: str,
        activation_epoch: int,
        route_version: int,
        initial_watermark: int = 0,
    ) -> None:
        self._scope_id = _text(scope_id, "fact reader scope_id")
        if activation_epoch < 1 or route_version < 1 or initial_watermark < 0:
            raise OmsSidecarError("fact reader epoch or route version is invalid")
        self._activation_epoch = activation_epoch
        self._route_version = route_version
        self._last_position = initial_watermark
        self._seen: dict[int, str] = {}

    @property
    def watermark(self) -> int:
        return self._last_position

    def rewind_to(self, watermark: int) -> None:
        """Restore a previously committed watermark after an out-of-band sink failure.

        The method is only for non-authoritative projector consumers that read
        a Journal again after their own transaction aborts.  It cannot advance
        state or repair a gap, and it deliberately retains duplicate digests at
        or before the retained watermark.
        """

        if type(watermark) is not int or watermark < 0 or watermark > self._last_position:
            raise OmsSidecarError("fact reader rewind watermark is invalid")
        self._last_position = watermark
        self._seen = {
            position: digest for position, digest in self._seen.items() if position <= watermark
        }

    def read_committed(self, facts: Iterable[OmsSidecarFactV1]) -> tuple[OmsSidecarFactV1, ...]:
        """Return only a contiguous, ownership-valid fact suffix.

        Repeated byte-identical positions are harmless replay evidence and are
        omitted.  Any position gap, modified duplicate, stale epoch or route
        change is a fail-closed reader error before a projector sees it.
        """

        emitted: list[OmsSidecarFactV1] = []
        for fact in facts:
            if fact.scope_id != self._scope_id:
                raise OmsSidecarError("fact reader received a cross-scope frame")
            if (
                fact.activation_epoch != self._activation_epoch
                or fact.route_version != self._route_version
            ):
                raise OmsSidecarError("fact reader received a stale epoch or route frame")
            known = self._seen.get(fact.position)
            if known is not None:
                if known != fact.fingerprint:
                    raise OmsSidecarError("fact reader found a conflicting duplicate position")
                continue
            if fact.position != self._last_position + 1:
                raise OmsSidecarError("fact reader found a Journal position gap")
            self._seen[fact.position] = fact.fingerprint
            self._last_position = fact.position
            emitted.append(fact)
        return tuple(emitted)


class MmapSidecarFactReaderV1:
    """Lift existing payload-free mmap records into structural sidecar facts.

    A compact callback mmap record cannot honestly infer a venue order state.
    It is therefore lifted with ``order_state=None``; the normalized callback
    reader may enrich the same committed boundary before calling the projector.
    No raw SDK payload is reconstructed or retained here.
    """

    _KINDS = {
        MmapJournalRecordKind.ORDER_PRE_NATIVE: OmsSidecarFactKind.PRE_NATIVE_COMMITTED,
        MmapJournalRecordKind.ORDER_CALL_STARTED: OmsSidecarFactKind.NATIVE_CALL_STARTED,
        MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED: OmsSidecarFactKind.NATIVE_ACCEPTED,
        MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED: OmsSidecarFactKind.ORDER_CALLBACK,
        MmapJournalRecordKind.CANCEL_STARTED: OmsSidecarFactKind.NATIVE_CALL_STARTED,
        MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED: OmsSidecarFactKind.CANCEL_ACCEPTED,
        MmapJournalRecordKind.CANCEL_REJECTED: OmsSidecarFactKind.CANCEL_CALLBACK,
    }

    def __init__(self, *, scope_id: str, activation_epoch: int, route_version: int) -> None:
        self._scope_id = _text(scope_id, "mmap sidecar scope_id")
        self._activation_epoch = activation_epoch
        self._route_version = route_version

    def read(self, records: Iterable[MmapPreNativeRecord]) -> tuple[OmsSidecarFactV1, ...]:
        result: list[OmsSidecarFactV1] = []
        for record in records:
            try:
                kind = self._KINDS[record.kind]
            except KeyError as error:
                raise OmsSidecarError(
                    "mmap record kind cannot be lifted into the sidecar"
                ) from error
            state = {
                OmsSidecarFactKind.PRE_NATIVE_COMMITTED: OmsSidecarOrderState.PRE_NATIVE_COMMITTED,
                OmsSidecarFactKind.NATIVE_CALL_STARTED: OmsSidecarOrderState.NATIVE_CALL_STARTED,
                OmsSidecarFactKind.NATIVE_ACCEPTED: OmsSidecarOrderState.NATIVE_ACCEPTED,
                OmsSidecarFactKind.CANCEL_ACCEPTED: OmsSidecarOrderState.CANCEL_PENDING,
                OmsSidecarFactKind.CANCEL_CALLBACK: OmsSidecarOrderState.REJECTED,
            }.get(kind)
            result.append(
                OmsSidecarFactV1(
                    scope_id=self._scope_id,
                    position=record.sequence,
                    schema_version=1,
                    activation_epoch=self._activation_epoch,
                    route_version=self._route_version,
                    kind=kind,
                    execution_id=record.execution_id,
                    canonical_client_order_ref=record.client_order_ref,
                    payload_hash=record.payload_hash,
                    observed_monotonic_ns=record.monotonic_ns,
                    external_order_id=record.native_order_id or None,
                    order_state=state,
                    state_version=record.sequence,
                )
            )
        return tuple(result)


class OrderStateProjectorV1:
    """Apply committed facts and publish immutable state generations off path."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._orders: dict[str, OmsSidecarOrderRefV1] = {}
        self._trades: dict[str, OmsSidecarTradeRefV1] = {}
        self._watermarks: dict[str, int] = {}
        self._fingerprints: dict[tuple[str, int], str] = {}
        self._reconciliation_required: set[str] = set()
        self._generation = 0

    def apply(self, facts: Iterable[OmsSidecarFactV1]) -> OmsSidecarSnapshotV1:
        """Apply a fact sequence without taking any adapter or writer lock."""

        facts = tuple(facts)
        with self._lock:
            for fact in facts:
                self._apply_one(fact)
            self._generation += 1
            return self._snapshot(
                as_of_monotonic_ns=max((fact.observed_monotonic_ns for fact in facts), default=0)
            )

    def apply_unpublished(self, facts: Iterable[OmsSidecarFactV1]) -> int:
        """Apply a committed batch without materializing a query generation.

        A high-rate sidecar reader can use this in its own actor and publish one
        immutable snapshot for many applied batches.  This remains outside all
        execution and mmap-writer paths; it exists to keep query-copy work from
        becoming an accidental fact-ingestion dependency.
        """

        facts = tuple(facts)
        with self._lock:
            for fact in facts:
                self._apply_one(fact)
        return max((fact.observed_monotonic_ns for fact in facts), default=0)

    def snapshot(self, *, as_of_monotonic_ns: int = 0) -> OmsSidecarSnapshotV1:
        """Create a consistent immutable query generation."""

        with self._lock:
            self._generation += 1
            return self._snapshot(as_of_monotonic_ns=as_of_monotonic_ns)

    def _apply_one(self, fact: OmsSidecarFactV1) -> None:
        position_key = (fact.scope_id, fact.position)
        fingerprint = fact.fingerprint
        known = self._fingerprints.get(position_key)
        if known is not None:
            if known != fingerprint:
                raise OmsSidecarError("projector received a conflicting duplicate position")
            return
        expected = self._watermarks.get(fact.scope_id, 0) + 1
        if fact.position != expected:
            raise OmsSidecarError("projector received a non-contiguous fact stream")
        self._fingerprints[position_key] = fingerprint
        self._watermarks[fact.scope_id] = fact.position
        existing = self._orders.get(fact.execution_id)
        if existing is None:
            initial = fact.order_state or OmsSidecarOrderState.INTENT_ACCEPTED
            existing = OmsSidecarOrderRefV1(
                execution_id=fact.execution_id,
                scope_id=fact.scope_id,
                canonical_client_order_ref=fact.canonical_client_order_ref,
                state=initial,
                state_version=fact.state_version,
                applied_position=fact.position,
                activation_epoch=fact.activation_epoch,
                route_version=fact.route_version,
                external_order_id=fact.external_order_id,
                cumulative_quantity=fact.cumulative_quantity or 0,
                leaves_quantity=fact.leaves_quantity,
                trade_keys=(
                    frozenset() if fact.trade_key is None else frozenset((fact.trade_key,))
                ),
                reconciliation_required=initial is OmsSidecarOrderState.RECONCILE_REQUIRED,
            )
            self._orders[fact.execution_id] = existing
            if fact.trade_key is not None:
                self._trades[fact.trade_key] = OmsSidecarTradeRefV1(
                    trade_key=fact.trade_key,
                    execution_id=existing.execution_id,
                    scope_id=existing.scope_id,
                    external_order_id=existing.external_order_id,
                    applied_position=fact.position,
                    cumulative_quantity=existing.cumulative_quantity,
                )
            if existing.reconciliation_required:
                self._reconciliation_required.add(existing.execution_id)
            return
        if (
            existing.scope_id != fact.scope_id
            or existing.canonical_client_order_ref != fact.canonical_client_order_ref
            or existing.activation_epoch != fact.activation_epoch
            or existing.route_version != fact.route_version
            or existing.external_order_id is not None
            and fact.external_order_id is not None
            and existing.external_order_id != fact.external_order_id
        ):
            self._mark_reconcile(existing, fact.position)
            return
        if fact.external_order_id is not None:
            for order in self._orders.values():
                if (
                    order.execution_id != existing.execution_id
                    and order.external_order_id == fact.external_order_id
                ):
                    self._mark_reconcile(existing, fact.position)
                    self._mark_reconcile(order, order.applied_position)
                    return
        if fact.trade_key is not None:
            known_trade = self._trades.get(fact.trade_key)
            if known_trade is not None and known_trade.execution_id != existing.execution_id:
                self._mark_reconcile(existing, fact.position)
                self._mark_reconcile(
                    self._orders[known_trade.execution_id], known_trade.applied_position
                )
                return
        next_state = self._next_state(existing, fact)
        trade_keys: frozenset[str] = (
            existing.trade_keys
            if fact.trade_key is None
            else existing.trade_keys | frozenset((fact.trade_key,))
        )
        updated = replace(
            existing,
            state=next_state,
            state_version=max(existing.state_version, fact.state_version),
            applied_position=fact.position,
            external_order_id=fact.external_order_id or existing.external_order_id,
            cumulative_quantity=max(existing.cumulative_quantity, fact.cumulative_quantity or 0),
            leaves_quantity=fact.leaves_quantity
            if fact.leaves_quantity is not None
            else existing.leaves_quantity,
            trade_keys=frozenset(trade_keys),
            reconciliation_required=existing.reconciliation_required
            or next_state is OmsSidecarOrderState.RECONCILE_REQUIRED,
        )
        self._orders[updated.execution_id] = updated
        if fact.trade_key is not None:
            self._trades[fact.trade_key] = OmsSidecarTradeRefV1(
                trade_key=fact.trade_key,
                execution_id=updated.execution_id,
                scope_id=updated.scope_id,
                external_order_id=updated.external_order_id,
                applied_position=fact.position,
                cumulative_quantity=updated.cumulative_quantity,
            )
        if updated.reconciliation_required:
            self._reconciliation_required.add(updated.execution_id)

    def _next_state(
        self, existing: OmsSidecarOrderRefV1, fact: OmsSidecarFactV1
    ) -> OmsSidecarOrderState:
        candidate = fact.order_state
        if candidate is None:
            return existing.state
        if existing.reconciliation_required or candidate is OmsSidecarOrderState.RECONCILE_REQUIRED:
            return OmsSidecarOrderState.RECONCILE_REQUIRED
        if fact.state_version < existing.state_version:
            return existing.state
        if candidate == existing.state:
            return existing.state
        if existing.state in _TERMINAL_STATES:
            self._mark_reconcile(existing, fact.position)
            return OmsSidecarOrderState.RECONCILE_REQUIRED
        # A callback may legitimately arrive before InsertOrder returns.  A
        # late local NATIVE_ACCEPTED record enriches the external ID but never
        # rolls the visible callback state backward.
        if (
            candidate is OmsSidecarOrderState.NATIVE_ACCEPTED
            and _STATE_RANK[existing.state] > _STATE_RANK[candidate]
        ):
            return existing.state
        if _STATE_RANK[candidate] < _STATE_RANK[existing.state]:
            self._mark_reconcile(existing, fact.position)
            return OmsSidecarOrderState.RECONCILE_REQUIRED
        return candidate

    def _mark_reconcile(self, order: OmsSidecarOrderRefV1, position: int) -> None:
        self._orders[order.execution_id] = replace(
            order,
            state=OmsSidecarOrderState.RECONCILE_REQUIRED,
            applied_position=max(order.applied_position, position),
            reconciliation_required=True,
        )
        self._reconciliation_required.add(order.execution_id)

    def _snapshot(self, *, as_of_monotonic_ns: int) -> OmsSidecarSnapshotV1:
        client = {order.canonical_client_order_ref: order for order in self._orders.values()}
        external = {
            order.external_order_id: order
            for order in self._orders.values()
            if order.external_order_id is not None
        }
        return OmsSidecarSnapshotV1(
            generation=self._generation,
            as_of_monotonic_ns=as_of_monotonic_ns,
            per_scope_applied_watermark=self._watermarks,
            orders_by_execution_id=self._orders,
            orders_by_client_order_ref=client,
            orders_by_external_order_id=external,
            trades_by_key=self._trades,
            health={
                "reconciliation": "REQUIRED" if self._reconciliation_required else "VALID",
                "projector": "CURRENT",
            },
            reconciliation_required_execution_ids=frozenset(self._reconciliation_required),
        )


class ImmutableOmsSnapshotStoreV1:
    """Publish a bounded history via one atomic immutable-reference replacement."""

    def __init__(self, *, history_capacity: int = 4) -> None:
        if not 1 <= history_capacity <= 64:
            raise OmsSidecarError("snapshot history capacity is invalid")
        self._history_capacity = history_capacity
        self._publish_lock = Lock()
        self._latest = OmsSidecarSnapshotV1(0, 0, {}, {}, {}, {})
        self._history: tuple[OmsSidecarSnapshotV1, ...] = (self._latest,)

    def publish(self, snapshot: OmsSidecarSnapshotV1) -> None:
        """Publish only a newer generation; readers never acquire this lock."""

        with self._publish_lock:
            if snapshot.generation <= self._latest.generation:
                raise OmsSidecarError("snapshot publication is not monotonic")
            self._latest = snapshot
            self._history = (*self._history, snapshot)[-self._history_capacity :]

    def latest(self) -> OmsSidecarSnapshotV1:
        """Return one complete immutable reference without an adapter-side lock."""

        return self._latest

    def recent(self) -> tuple[OmsSidecarSnapshotV1, ...]:
        return self._history


@dataclass(frozen=True)
class OmsSidecarManifestSegmentV1:
    """One scope-local Journal segment identity for an async checkpoint."""

    scope_id: str
    journal_generation: int
    start_position: int
    end_position: int
    schema_hash: str
    segment_checksum: str
    writer_instance_id: str
    activation_epoch: int

    def __post_init__(self) -> None:
        _text(self.scope_id, "manifest segment scope")
        _text(self.writer_instance_id, "manifest writer instance")
        _sha256(self.schema_hash, "manifest schema hash")
        _sha256(self.segment_checksum, "manifest segment checksum")
        if (
            self.journal_generation < 1
            or self.start_position < 1
            or self.end_position < self.start_position
            or self.activation_epoch < 1
        ):
            raise OmsSidecarError("manifest segment position or generation is invalid")


@dataclass(frozen=True)
class OmsSidecarManifestV1:
    """Checksum-linked async checkpoint metadata; never a Journal commit gate."""

    manifest_id: str
    manifest_version: int
    created_monotonic_ns: int
    previous_manifest_hash: str | None
    per_scope_segment_ranges: Mapping[str, tuple[int, int]]
    per_scope_applied_watermark: Mapping[str, int]
    snapshot_generation: int
    manifest_hash: str
    segments: tuple[OmsSidecarManifestSegmentV1, ...] = ()

    def __post_init__(self) -> None:
        _text(self.manifest_id, "manifest id")
        if (
            self.manifest_version < 1
            or self.created_monotonic_ns < 0
            or self.snapshot_generation < 0
        ):
            raise OmsSidecarError("manifest version, time or generation is invalid")
        if self.previous_manifest_hash is not None:
            _sha256(self.previous_manifest_hash, "previous manifest hash")
        _sha256(self.manifest_hash, "manifest hash")
        ranges = dict(self.per_scope_segment_ranges)
        watermarks = dict(self.per_scope_applied_watermark)
        if set(ranges) != set(watermarks) or not ranges:
            raise OmsSidecarError("manifest scopes do not match")
        for scope, bounds in ranges.items():
            _text(scope, "manifest scope")
            if len(bounds) != 2 or bounds[0] < 1 or bounds[1] < bounds[0]:
                raise OmsSidecarError("manifest segment range is invalid")
            if watermarks[scope] < bounds[1]:
                raise OmsSidecarError("manifest watermark precedes its segment range")
        segments = tuple(self.segments)
        if segments:
            if {segment.scope_id for segment in segments} != set(ranges) or len(segments) != len(
                ranges
            ):
                raise OmsSidecarError("manifest segments do not exactly cover its scopes")
            for segment in segments:
                if ranges[segment.scope_id] != (segment.start_position, segment.end_position):
                    raise OmsSidecarError("manifest segment range differs from its scope range")
        object.__setattr__(self, "per_scope_segment_ranges", MappingProxyType(ranges))
        object.__setattr__(self, "per_scope_applied_watermark", MappingProxyType(watermarks))
        object.__setattr__(self, "segments", segments)
        if self.manifest_hash != self.expected_hash:
            raise OmsSidecarError("manifest checksum is invalid")

    @property
    def expected_hash(self) -> str:
        return sha256(_canonical_json(self._unsigned_payload())).hexdigest()

    def _unsigned_payload(self) -> Mapping[str, object]:
        return {
            "manifest_id": self.manifest_id,
            "manifest_version": self.manifest_version,
            "created_monotonic_ns": self.created_monotonic_ns,
            "previous_manifest_hash": self.previous_manifest_hash,
            "per_scope_segment_ranges": {
                key: list(value) for key, value in sorted(self.per_scope_segment_ranges.items())
            },
            "per_scope_applied_watermark": dict(sorted(self.per_scope_applied_watermark.items())),
            "snapshot_generation": self.snapshot_generation,
            "segments": [
                {
                    "scope_id": segment.scope_id,
                    "journal_generation": segment.journal_generation,
                    "start_position": segment.start_position,
                    "end_position": segment.end_position,
                    "schema_hash": segment.schema_hash,
                    "segment_checksum": segment.segment_checksum,
                    "writer_instance_id": segment.writer_instance_id,
                    "activation_epoch": segment.activation_epoch,
                }
                for segment in sorted(self.segments, key=lambda item: item.scope_id)
            ],
        }

    @classmethod
    def create(
        cls,
        *,
        manifest_id: str,
        created_monotonic_ns: int,
        previous_manifest_hash: str | None,
        per_scope_segment_ranges: Mapping[str, tuple[int, int]],
        per_scope_applied_watermark: Mapping[str, int],
        snapshot_generation: int,
        segments: tuple[OmsSidecarManifestSegmentV1, ...] = (),
    ) -> "OmsSidecarManifestV1":
        payload = {
            "manifest_id": manifest_id,
            "manifest_version": 1,
            "created_monotonic_ns": created_monotonic_ns,
            "previous_manifest_hash": previous_manifest_hash,
            "per_scope_segment_ranges": {
                key: list(value) for key, value in sorted(per_scope_segment_ranges.items())
            },
            "per_scope_applied_watermark": dict(sorted(per_scope_applied_watermark.items())),
            "snapshot_generation": snapshot_generation,
            "segments": [
                {
                    "scope_id": segment.scope_id,
                    "journal_generation": segment.journal_generation,
                    "start_position": segment.start_position,
                    "end_position": segment.end_position,
                    "schema_hash": segment.schema_hash,
                    "segment_checksum": segment.segment_checksum,
                    "writer_instance_id": segment.writer_instance_id,
                    "activation_epoch": segment.activation_epoch,
                }
                for segment in sorted(segments, key=lambda item: item.scope_id)
            ],
        }
        return cls(
            manifest_id=manifest_id,
            manifest_version=1,
            created_monotonic_ns=created_monotonic_ns,
            previous_manifest_hash=previous_manifest_hash,
            per_scope_segment_ranges=per_scope_segment_ranges,
            per_scope_applied_watermark=per_scope_applied_watermark,
            snapshot_generation=snapshot_generation,
            segments=segments,
            manifest_hash=sha256(_canonical_json(payload)).hexdigest(),
        )


class OmsSidecarManifestChainV1:
    """In-memory manifest verifier used by checkpoint/replay orchestration."""

    def __init__(self) -> None:
        self._manifests: list[OmsSidecarManifestV1] = []

    def append(self, manifest: OmsSidecarManifestV1) -> None:
        previous = self._manifests[-1].manifest_hash if self._manifests else None
        if manifest.previous_manifest_hash != previous:
            raise OmsSidecarError("manifest chain predecessor is invalid")
        self._manifests.append(manifest)

    def latest(self) -> OmsSidecarManifestV1 | None:
        return self._manifests[-1] if self._manifests else None

    def all(self) -> tuple[OmsSidecarManifestV1, ...]:
        return tuple(self._manifests)


class OmsSnapshotQueryServiceV1:
    """Read-only query facade over a supplied immutable snapshot store."""

    def __init__(self, store: ImmutableOmsSnapshotStoreV1) -> None:
        self._store = store

    def snapshot(self) -> OmsSidecarSnapshotV1:
        return self._store.latest()

    def order_by_execution_id(self, execution_id: str) -> OmsSidecarOrderRefV1 | None:
        return self._store.latest().orders_by_execution_id.get(execution_id)

    def order_by_client_order_ref(self, client_order_ref: str) -> OmsSidecarOrderRefV1 | None:
        return self._store.latest().orders_by_client_order_ref.get(client_order_ref)

    def order_by_external_order_id(self, external_order_id: str) -> OmsSidecarOrderRefV1 | None:
        return self._store.latest().orders_by_external_order_id.get(external_order_id)

    def trade_by_key(self, trade_key: str) -> OmsSidecarTradeRefV1 | None:
        return self._store.latest().trades_by_key.get(trade_key)


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )
