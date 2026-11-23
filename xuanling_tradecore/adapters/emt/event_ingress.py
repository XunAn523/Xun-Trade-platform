"""Bounded, SDK-free normalization ingress for EMT order-side callbacks."""

from __future__ import annotations

import json
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from hashlib import sha256
from math import isfinite
from threading import Event, RLock
from typing import Mapping

from ...domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    CallbackGapEvidence,
    ErrorCategory,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from ...domain._validation import freeze_text_mapping, require_nonnegative, require_text
from ..base import BrokerEventObserver
from ..mapping import ActivatedVenueMapping
from .identity import EmtClientOrderIdAllocator
from .namespace_recovery import NamespaceOwnership, OrderClientIdNamespaceDirectoryV1


class EmtBrokerEventIngressError(RuntimeError):
    """A normalized EMT callback cannot be safely admitted or attributed."""


def _empty_summary() -> Mapping[str, str]:
    return {}


@dataclass(frozen=True)
class EmtNativeBrokerEvent:
    """Scalar-only callback values copied by the platform bridge then queued here."""

    event_kind: BrokerEventKind
    account_id: str
    trading_day: TradingDayEpoch
    received_monotonic_ns: int
    order_emt_id: int | None = None
    order_client_id: int | None = None
    order_cancel_emt_id: int | None = None
    order_status: int | None = None
    filled_qty: int | None = None
    leaves_qty: int | None = None
    trade_key: str | None = None
    cancel_request_id: str | None = None
    error_category: ErrorCategory | None = None
    normalized_summary: Mapping[str, str] = field(default_factory=_empty_summary)

    def __post_init__(self) -> None:
        require_text(self.account_id, "EMT callback account_id")
        require_nonnegative(self.received_monotonic_ns, "EMT callback received time")
        for name, value in (
            ("order_emt_id", self.order_emt_id),
            ("order_client_id", self.order_client_id),
            ("order_cancel_emt_id", self.order_cancel_emt_id),
            ("order_status", self.order_status),
            ("filled_qty", self.filled_qty),
            ("leaves_qty", self.leaves_qty),
        ):
            if value is not None:
                require_nonnegative(value, f"EMT callback {name}")
        if self.order_emt_id == 0 or self.order_client_id == 0 or self.order_cancel_emt_id == 0:
            raise EmtBrokerEventIngressError("EMT callback native identifiers must be positive")
        if self.event_kind is BrokerEventKind.TRADE and self.trade_key is None:
            raise EmtBrokerEventIngressError("EMT trade callback lacks stable trade_key")
        if self.event_kind is BrokerEventKind.CANCEL_ERROR and self.cancel_request_id is None:
            raise EmtBrokerEventIngressError("EMT cancel error lacks cancel_request_id")
        if self.trade_key is not None:
            require_text(self.trade_key, "EMT callback trade_key")
        if self.cancel_request_id is not None:
            require_text(self.cancel_request_id, "EMT callback cancel_request_id")
        object.__setattr__(
            self,
            "normalized_summary",
            freeze_text_mapping(self.normalized_summary, "EMT callback summary"),
        )


class EmtBrokerEventIngress:
    """Deduplicate and deliver order-side callbacks through a bounded queue."""

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        trading_day: TradingDayEpoch,
        capacity: int = 1024,
        namespace_directory: OrderClientIdNamespaceDirectoryV1 | None = None,
    ) -> None:
        if mapping.manifest.venue_id != "EMT":
            raise EmtBrokerEventIngressError("EMT event ingress requires an EMT mapping")
        if capacity <= 0:
            raise EmtBrokerEventIngressError("EMT event ingress capacity must be positive")
        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtBrokerEventIngressError("EMT event ingress requires scope ownership")
        self._mapping = mapping
        self._trading_day = trading_day
        self._capacity = capacity
        self._observer: BrokerEventObserver | None = None
        self._events: deque[EmtNativeBrokerEvent] = deque()
        self._fingerprints: dict[str, str] = {}
        self._rejected_count = 0
        # Reasons are fixed, payload-free bridge categories. They are
        # diagnostic evidence for a closed callback gap, never a substitute
        # for the gap itself or a reason to continue a measured acceptance.
        self._callback_gap_reason_counts: dict[str, int] = {}
        self._namespace_directory = namespace_directory
        self._verified_foreign_namespace_callback_count = 0
        # This is a bounded, in-process-only correlation cache. Entries are
        # admitted only after a client reference resolves to a signed foreign
        # lease. It lets a later EMT cancel callback that omits the client
        # reference prove its ownership by the prior verified venue identity.
        self._verified_foreign_order_ids: OrderedDict[int, None] = OrderedDict()
        # A process-isolated same-account topology can receive a cancellation
        # error callback for an order whose pre-native route was sealed into a
        # different scope.  The native bridge increments this counter only
        # after matching that callback's client reference against its static
        # foreign-scope plan; it is not an unknown-callback escape hatch.
        self._verified_foreign_cancel_callback_count = 0
        self._lock = RLock()
        # The native callback thread only normalizes and enqueues.  A backend
        # callback worker can block on this edge and perform Redis I/O outside
        # the SDK callback, rather than periodically polling an empty queue.
        self._delivery_available = Event()

    def attach_observer(self, observer: BrokerEventObserver) -> None:
        with self._lock:
            if self._observer is not None and self._observer is not observer:
                raise EmtBrokerEventIngressError("EMT event ingress already has another observer")
            self._observer = observer

    def detach_observer(self, observer: BrokerEventObserver) -> None:
        with self._lock:
            if self._observer is not observer:
                raise EmtBrokerEventIngressError("EMT event ingress observer does not match")
            self._observer = None
            self._events.clear()
            self._delivery_available.clear()

    def offer(self, event: EmtNativeBrokerEvent) -> None:
        """Admit one already-extracted callback or emit a conservative gap on overflow."""

        if event.account_id != self._mapping.runtime.account_capabilities.account_id:
            raise EmtBrokerEventIngressError("EMT callback belongs to another profile")
        if event.trading_day != self._trading_day:
            raise EmtBrokerEventIngressError("EMT callback belongs to another trading day")
        namespace_result = self._classify_namespace_callback(event)
        if namespace_result is NamespaceOwnership.FOREIGN_RESERVED:
            assert event.order_client_id is not None
            self.record_verified_foreign_client_callback(
                order_client_id=event.order_client_id,
                order_emt_id=event.order_emt_id,
                cancellation=event.event_kind is BrokerEventKind.CANCEL_ERROR,
            )
            return
        if namespace_result is NamespaceOwnership.UNKNOWN_NAMESPACE:
            with self._lock:
                observer = self._require_observer()
                self._callback_gap_reason_counts["namespace_callback_unresolved"] = (
                    self._callback_gap_reason_counts.get("namespace_callback_unresolved", 0) + 1
                )
                gap = self._new_gap(event.account_id, event.received_monotonic_ns)
                self._delivery_available.set()
            observer.on_callback_gap(gap)
            return
        source_key = self._source_key(event)
        fingerprint = self._fingerprint(event)
        with self._lock:
            observer = self._require_observer()
            previous = self._fingerprints.get(source_key)
            if previous is not None:
                if previous != fingerprint:
                    raise EmtBrokerEventIngressError(
                        "EMT callback source key conflicts with prior fact"
                    )
                return
            self._fingerprints[source_key] = fingerprint
            if len(self._events) < self._capacity:
                self._events.append(event)
                self._delivery_available.set()
                return
            gap = self._new_gap(event.account_id, event.received_monotonic_ns)
            self._delivery_available.set()
        observer.on_callback_gap(gap)

    def force_gap(
        self,
        *,
        account_id: str,
        observed_monotonic_ns: int,
        reason: str = "unspecified",
    ) -> None:
        """Report a malformed/unknown platform callback without preserving its payload."""

        if account_id != self._mapping.runtime.account_capabilities.account_id:
            raise EmtBrokerEventIngressError("EMT callback gap belongs to another profile")
        if not reason.isascii() or not reason or len(reason) > 96:
            raise EmtBrokerEventIngressError("EMT callback gap reason is invalid")
        with self._lock:
            observer = self._require_observer()
            self._callback_gap_reason_counts[reason] = (
                self._callback_gap_reason_counts.get(reason, 0) + 1
            )
            gap = self._new_gap(account_id, observed_monotonic_ns)
            self._delivery_available.set()
        observer.on_callback_gap(gap)

    @property
    def callback_gap_reason_counts(self) -> dict[str, int]:
        """Return a copy of payload-free malformed-callback categories."""

        with self._lock:
            return dict(self._callback_gap_reason_counts)

    def record_verified_foreign_cancel_callback(self) -> None:
        """Record a sealed-plan foreign cancellation callback without retaining it.

        The caller must already have proved that the callback's client
        reference belongs to another live scope of this same account.  The
        callback is deliberately not converted to a local event or gap.
        """

        with self._lock:
            self._require_observer()
            self._verified_foreign_cancel_callback_count += 1

    def record_verified_foreign_client_callback(
        self,
        *,
        order_client_id: int,
        order_emt_id: int | None,
        cancellation: bool,
    ) -> bool:
        """Count and retain a bounded identity only after a signed foreign lookup.

        The return value is false for a local, unknown, or unconfigured
        namespace. Callers must then continue normal fail-closed processing.
        No callback payload is retained.
        """

        directory = self._namespace_directory
        if directory is None:
            return False
        if directory.lookup(order_client_id).ownership is not NamespaceOwnership.FOREIGN_RESERVED:
            return False
        with self._lock:
            self._require_observer()
            self._verified_foreign_namespace_callback_count += 1
            if order_emt_id is not None:
                self._verified_foreign_order_ids[order_emt_id] = None
                self._verified_foreign_order_ids.move_to_end(order_emt_id)
                if len(self._verified_foreign_order_ids) > self._capacity:
                    self._verified_foreign_order_ids.popitem(last=False)
            if cancellation:
                self._verified_foreign_cancel_callback_count += 1
        return True

    def record_verified_foreign_cancel_callback_for_order(self, order_emt_id: int) -> bool:
        """Count a client-less cancel callback only if a signed foreign order proved it."""

        if type(order_emt_id) is not int or order_emt_id <= 0:
            return False
        with self._lock:
            if order_emt_id not in self._verified_foreign_order_ids:
                return False
            self._require_observer()
            self._verified_foreign_order_ids.move_to_end(order_emt_id)
            self._verified_foreign_namespace_callback_count += 1
            self._verified_foreign_cancel_callback_count += 1
        return True

    @property
    def verified_foreign_cancel_callback_count(self) -> int:
        """Return the number of static-plan foreign cancel callbacks ignored here."""

        with self._lock:
            return self._verified_foreign_cancel_callback_count

    @property
    def verified_foreign_namespace_callback_count(self) -> int:
        """Return foreign order/trade/cancel callbacks excluded from local OMS."""

        with self._lock:
            return self._verified_foreign_namespace_callback_count

    def drain(self, *, wait_timeout_seconds: float = 0.0) -> int:
        """Deliver normalized facts in callback arrival order.

        A positive timeout waits for native callback work before draining.  It
        lets a backend callback worker sleep without adding a polling interval
        to the callback-to-journal path; observer delivery still never occurs
        on the native SDK callback thread.
        """

        if not isfinite(wait_timeout_seconds) or wait_timeout_seconds < 0:
            raise EmtBrokerEventIngressError(
                "EMT callback drain wait timeout must be a finite non-negative value"
            )
        if wait_timeout_seconds > 0:
            self._delivery_available.wait(wait_timeout_seconds)

        delivered = 0
        while True:
            with self._lock:
                observer = self._require_observer()
                if not self._events:
                    self._delivery_available.clear()
                    return delivered
                event = self._events.popleft()
            observer.on_broker_event(self._normalize(event))
            delivered += 1

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        return ownership.adapter_instance_id

    @property
    def account_id(self) -> str:
        return self._mapping.runtime.account_capabilities.account_id

    @property
    def trading_day(self) -> TradingDayEpoch:
        return self._trading_day

    @property
    def observer_attached(self) -> bool:
        with self._lock:
            return self._observer is not None

    def _normalize(self, event: EmtNativeBrokerEvent) -> BrokerEvent:
        source_key = self._source_key(event)
        identity = self._identity(event)
        summary = dict(event.normalized_summary)
        if event.order_status is not None:
            summary["order_status"] = str(event.order_status)
        return BrokerEvent(
            event_id=source_key,
            schema_version=1,
            source_key_schema_version=1,
            source_key=source_key,
            event_kind=event.event_kind,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id=self.adapter_instance_id,
            account_id=event.account_id,
            trading_day=event.trading_day,
            received_monotonic_ns=event.received_monotonic_ns,
            venue_order_identity=identity,
            cancel_request_id=event.cancel_request_id,
            trade_key=event.trade_key,
            filled_qty=event.filled_qty,
            leaves_qty=event.leaves_qty,
            error_category=event.error_category,
            normalized_summary=summary,
        )

    def _identity(self, event: EmtNativeBrokerEvent) -> VenueOrderIdentity | None:
        if (
            event.order_emt_id is None
            and event.order_client_id is None
            and event.order_cancel_emt_id is None
        ):
            return None
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        client_ref = None
        if event.order_client_id is not None:
            client_ref = str(event.order_client_id)
            if (
                EmtClientOrderIdAllocator.parse_client_order_ref(client_ref)
                != event.order_client_id
            ):
                raise EmtBrokerEventIngressError("EMT callback order_client_id is invalid")
        return VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id=ownership.adapter_instance_id,
            adapter_client_scope=ownership.adapter_client_scope,
            client_order_ref=client_ref,
            venue_order_id=str(event.order_emt_id) if event.order_emt_id is not None else None,
            venue_cancel_id=(
                str(event.order_cancel_emt_id) if event.order_cancel_emt_id is not None else None
            ),
        )

    def _classify_namespace_callback(
        self, event: EmtNativeBrokerEvent
    ) -> NamespaceOwnership | None:
        """Classify only when R1 provides a verified immutable directory.

        The lookup has no I/O and runs before a local event reaches the
        projection path.  A missing/unknown client tag is deliberately a
        callback gap rather than an opportunity to guess that it is ours.
        """

        directory = self._namespace_directory
        if directory is None:
            return None
        if event.order_client_id is None:
            return NamespaceOwnership.UNKNOWN_NAMESPACE
        return directory.lookup(event.order_client_id).ownership

    def _source_key(self, event: EmtNativeBrokerEvent) -> str:
        payload = {
            "event_kind": event.event_kind.value,
            "account_id": event.account_id,
            "trading_day": event.trading_day.value,
            "order_emt_id": event.order_emt_id,
            "order_client_id": event.order_client_id,
            "order_cancel_emt_id": event.order_cancel_emt_id,
            "order_status": event.order_status,
            "filled_qty": event.filled_qty,
            "leaves_qty": event.leaves_qty,
            "trade_key": event.trade_key,
            "cancel_request_id": event.cancel_request_id,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return f"E1:{sha256(encoded).hexdigest()}"

    @staticmethod
    def _fingerprint(event: EmtNativeBrokerEvent) -> str:
        payload = {
            "source": EmtBrokerEventIngress._source_key_static(event),
            "error_category": event.error_category.value if event.error_category else None,
            "summary": dict(event.normalized_summary),
        }
        return sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _source_key_static(event: EmtNativeBrokerEvent) -> str:
        payload = {
            "event_kind": event.event_kind.value,
            "account_id": event.account_id,
            "trading_day": event.trading_day.value,
            "order_emt_id": event.order_emt_id,
            "order_client_id": event.order_client_id,
            "order_cancel_emt_id": event.order_cancel_emt_id,
            "order_status": event.order_status,
            "filled_qty": event.filled_qty,
            "leaves_qty": event.leaves_qty,
            "trade_key": event.trade_key,
            "cancel_request_id": event.cancel_request_id,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return f"E1:{sha256(encoded).hexdigest()}"

    def _require_observer(self) -> BrokerEventObserver:
        if self._observer is None:
            raise EmtBrokerEventIngressError("EMT event ingress requires an attached observer")
        return self._observer

    def _new_gap(self, account_id: str, observed_monotonic_ns: int) -> CallbackGapEvidence:
        self._rejected_count += 1
        sequence = self._rejected_count
        return CallbackGapEvidence(
            evidence_id=f"emt-event-ingress-gap:v1:{self.adapter_instance_id}:{sequence}",
            account_id=account_id,
            producer_id=f"emt-event-ingress:v1:{self.adapter_id}",
            first_rejected_sequence=sequence,
            last_rejected_sequence=sequence,
            rejected_count=1,
            observed_monotonic_ns=observed_monotonic_ns,
        )
