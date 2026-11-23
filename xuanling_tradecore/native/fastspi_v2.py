"""Fixed-layout callback records and the optional Linux ``fastspi v2`` ring.

V2 deliberately does not accept Python callback objects.  Producers provide a
bounded scalar record; consumer-side code is responsible for creating domain
objects, correlation IDs, tracing and durable facts after the record has left
the native ring.  The Python implementation is functional only and cannot be
used as performance evidence.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from math import isfinite
from threading import Event, Lock
from typing import Any, Callable, Protocol, cast


def _load_native_type(exported_name: str) -> type[Any] | None:
    """Resolve one optional Linux extension type without an import dependency."""

    try:  # Built only for the supported Linux deployment target.
        native_module = import_module("xuanling_tradecore._fastspi_v2")
    except ImportError:  # pragma: no cover - depends on target compiler/platform.
        return None
    native_type = getattr(native_module, exported_name, None)
    if not isinstance(native_type, type):
        return None
    return cast(type[Any], native_type)


NativeFastSpiV2Ring = _load_native_type("NativeFastSpiV2Ring")
NativeFastSpiV2GapLatch = _load_native_type("NativeFastSpiV2GapLatch")
NativeFastSpiV2Ingress = _load_native_type("NativeFastSpiV2Ingress")

MAX_ERROR_BYTES = 192
MAX_EXECUTION_KEY_BYTES = 64
MAX_CALLBACK_KIND = 0xFFFF
MAX_PRODUCER_ID = 0xFFFF_FFFF
MAX_UNSIGNED_64 = 0xFFFF_FFFF_FFFF_FFFF
MIN_CAPACITY = 2
MAX_CAPACITY = 1 << 20


class FastSpiVersion(str, Enum):
    """The only supported callback-ring implementations in a local profile."""

    V1 = "v1"
    V2 = "v2"


class FastSpiV2ActivationError(RuntimeError):
    """A profile attempted to select V2 outside its fail-closed preconditions."""


class FastSpiV2EquivalenceEvidencePort(Protocol):
    """The post-test evidence object required before a V2 runtime is bound."""

    @property
    def certificate_sha256(self) -> str: ...

    def assert_runtime_equivalent(self) -> None: ...


@dataclass(frozen=True)
class FastSpiRuntimeSelection:
    """Versioned local selection metadata; it never opens an SDK session.

    ``equivalence_certificate_sha256`` is a release-evidence reference, not a
    permit to arm an account.  It is mandatory for V2 so no deployment can
    replace V1 merely because a native extension happens to import.
    """

    version: FastSpiVersion
    equivalence_certificate_sha256: str | None = None

    def __post_init__(self) -> None:
        certificate = self.equivalence_certificate_sha256
        if self.version is FastSpiVersion.V1:
            if certificate is not None:
                raise ValueError("fastspi v1 selection cannot carry a v2 equivalence certificate")
            return
        if (
            certificate is None
            or len(certificate) != 64
            or any(character not in "0123456789abcdef" for character in certificate)
        ):
            raise ValueError("fastspi v2 selection requires a SHA-256 equivalence certificate")

    def validate_disarmed_transition(
        self,
        *,
        current: FastSpiRuntimeSelection,
        account_lifecycle: str,
        native_v2_available: bool,
    ) -> None:
        """Permit a version selection change only before an account is armed."""

        if account_lifecycle != "DISARMED":
            raise FastSpiV2ActivationError(
                "fastspi version selection can change only while the account is DISARMED"
            )
        if self.version is FastSpiVersion.V2 and not native_v2_available:
            raise FastSpiV2ActivationError("fastspi v2 selection requires its native backend")
        if current.version is self.version:
            return

    def validate_runtime_binding(
        self,
        *,
        current: FastSpiRuntimeSelection,
        account_lifecycle: str,
        native_v2_available: bool,
        equivalence_evidence: FastSpiV2EquivalenceEvidencePort | None,
    ) -> None:
        """Validate a real V2 binding against executable, matching evidence.

        Selection metadata alone remains useful for configuration inspection.
        A session/runtime builder must use this stricter method so a free-form
        SHA reference cannot stand in for a complete V1/V2 proof.
        """

        self.validate_disarmed_transition(
            current=current,
            account_lifecycle=account_lifecycle,
            native_v2_available=native_v2_available,
        )
        if self.version is FastSpiVersion.V1:
            return
        if equivalence_evidence is None:
            raise FastSpiV2ActivationError(
                "fastspi v2 runtime binding requires equivalence evidence"
            )
        if equivalence_evidence.certificate_sha256 != self.equivalence_certificate_sha256:
            raise FastSpiV2ActivationError(
                "fastspi v2 runtime binding evidence does not match the configured certificate"
            )
        equivalence_evidence.assert_runtime_equivalent()


class FastSpiV2Unavailable(RuntimeError):
    """A caller required the Linux native V2 callback ring but it is absent."""


@dataclass(frozen=True)
class FastSpiV2Record:
    """One bounded scalar callback record suitable for a native POD slot.

    A zero ID denotes "not present"; ``flags`` is reserved for the producer
    mapping to state presence and callback-specific interpretation.  The
    record intentionally contains no account number, route token, raw SDK
    object, Python object or unbounded payload.
    """

    callback_kind: int
    producer_id: int
    callback_seq: int
    observed_monotonic_ns: int
    client_id: int = 0
    order_client_id: int = 0
    order_emt_id: int = 0
    order_cancel_emt_id: int = 0
    status: int = 0
    quantity: int = 0
    trade_id_hash: int = 0
    flags: int = 0
    error_bytes: bytes = b""
    order_update_time: int = 0
    qty_traded: int = 0
    qty_left: int = 0
    trade_market: int = 0
    trade_price: float = 0.0
    trade_report_index: int = 0
    trade_execution_id: bytes = b""
    query_reference: int = 0

    def __post_init__(self) -> None:
        _require_range(self.callback_kind, "fastspi v2 callback_kind", 1, MAX_CALLBACK_KIND)
        _require_range(self.producer_id, "fastspi v2 producer_id", 0, MAX_PRODUCER_ID)
        _require_range(self.callback_seq, "fastspi v2 callback_seq", 1, MAX_UNSIGNED_64)
        _require_range(
            self.observed_monotonic_ns,
            "fastspi v2 observed_monotonic_ns",
            0,
            MAX_UNSIGNED_64,
        )
        for field_name, value in (
            ("client_id", self.client_id),
            ("order_client_id", self.order_client_id),
            ("order_emt_id", self.order_emt_id),
            ("order_cancel_emt_id", self.order_cancel_emt_id),
            ("trade_id_hash", self.trade_id_hash),
        ):
            _require_range(value, f"fastspi v2 {field_name}", 0, MAX_UNSIGNED_64)
        _require_range(self.status, "fastspi v2 status", -(1 << 31), (1 << 31) - 1)
        _require_range(self.quantity, "fastspi v2 quantity", -(1 << 63), (1 << 63) - 1)
        for field_name, value in (
            ("order_update_time", self.order_update_time),
            ("qty_traded", self.qty_traded),
            ("qty_left", self.qty_left),
            ("trade_report_index", self.trade_report_index),
            ("query_reference", self.query_reference),
        ):
            _require_range(value, f"fastspi v2 {field_name}", 0, MAX_UNSIGNED_64)
        _require_range(self.trade_market, "fastspi v2 trade_market", -(1 << 31), (1 << 31) - 1)
        if type(self.trade_price) not in {float, int} or not isfinite(float(self.trade_price)):
            raise ValueError("fastspi v2 trade_price must be a finite numeric scalar")
        _require_range(self.flags, "fastspi v2 flags", 0, MAX_PRODUCER_ID)
        if len(self.error_bytes) > MAX_ERROR_BYTES:
            raise ValueError("fastspi v2 error_bytes must be bounded UTF-8 bytes")
        try:
            self.error_bytes.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("fastspi v2 error_bytes must be valid UTF-8") from error
        if len(self.trade_execution_id) > MAX_EXECUTION_KEY_BYTES:
            raise ValueError("fastspi v2 trade_execution_id must be bounded UTF-8 bytes")
        try:
            self.trade_execution_id.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("fastspi v2 trade_execution_id must be valid UTF-8") from error

    def native_arguments(self) -> tuple[object, ...]:
        """Return the exact scalar argument layout consumed by the C backend."""

        return (
            self.callback_kind,
            self.producer_id,
            self.callback_seq,
            self.observed_monotonic_ns,
            self.client_id,
            self.order_client_id,
            self.order_emt_id,
            self.order_cancel_emt_id,
            self.status,
            self.quantity,
            self.trade_id_hash,
            self.flags,
            self.error_bytes,
            self.order_update_time,
            self.qty_traded,
            self.qty_left,
            self.trade_market,
            self.trade_price,
            self.trade_report_index,
            self.trade_execution_id,
            self.query_reference,
        )

    @classmethod
    def from_native_tuple(cls, raw: tuple[Any, ...]) -> FastSpiV2Record:
        """Reconstruct a Python value only on the consumer side of the ring."""

        if len(raw) != 21:
            raise RuntimeError("fastspi v2 native ring returned an invalid record")
        return cls(
            callback_kind=_native_int(raw[0], "callback_kind"),
            producer_id=_native_int(raw[1], "producer_id"),
            callback_seq=_native_int(raw[2], "callback_seq"),
            observed_monotonic_ns=_native_int(raw[3], "observed_monotonic_ns"),
            client_id=_native_int(raw[4], "client_id"),
            order_client_id=_native_int(raw[5], "order_client_id"),
            order_emt_id=_native_int(raw[6], "order_emt_id"),
            order_cancel_emt_id=_native_int(raw[7], "order_cancel_emt_id"),
            status=_native_int(raw[8], "status"),
            quantity=_native_int(raw[9], "quantity"),
            trade_id_hash=_native_int(raw[10], "trade_id_hash"),
            flags=_native_int(raw[11], "flags"),
            error_bytes=_native_bytes(raw[12]),
            order_update_time=_native_int(raw[13], "order_update_time"),
            qty_traded=_native_int(raw[14], "qty_traded"),
            qty_left=_native_int(raw[15], "qty_left"),
            trade_market=_native_int(raw[16], "trade_market"),
            trade_price=_native_float(raw[17], "trade_price"),
            trade_report_index=_native_int(raw[18], "trade_report_index"),
            trade_execution_id=_native_bytes(raw[19]),
            query_reference=_native_int(raw[20], "query_reference"),
        )


@dataclass(frozen=True)
class FastSpiV2RingSnapshot:
    """Bounded operational counters; it contains no callback payload."""

    version: int
    native_available: bool
    capacity: int
    depth: int
    high_watermark: int
    pushed_count: int
    popped_count: int
    overflow_count: int
    wakeup_count: int
    wakeup_failures: int


class FastSpiV2SequenceAllocator:
    """Allocate strictly increasing sequences in each producer's local domain.

    A sequence is reserved before a ring attempt.  Therefore a later successful
    record may skip values after an overflow; that gap is intentional evidence
    for the account-level ``CallbackGapLatch`` and is never silently repaired.
    """

    def __init__(self, *, maximum_producers: int = 16) -> None:
        if not 1 <= maximum_producers <= 16:
            raise ValueError("fastspi v2 maximum_producers must be in 1..16")
        self._maximum_producers = maximum_producers
        self._sequences: dict[int, int] = {}
        self._lock = Lock()

    def reserve(self, producer_id: int) -> int:
        _require_range(producer_id, "fastspi v2 producer_id", 0, MAX_PRODUCER_ID)
        with self._lock:
            current = self._sequences.get(producer_id)
            if current is None:
                if len(self._sequences) >= self._maximum_producers:
                    raise FastSpiV2Unavailable("fastspi v2 producer bound is exhausted")
                current = 0
            if current >= MAX_UNSIGNED_64:
                raise FastSpiV2Unavailable("fastspi v2 producer sequence is exhausted")
            next_sequence = current + 1
            self._sequences[producer_id] = next_sequence
            return next_sequence


class CallbackGapRecorderPort(Protocol):
    """The no-I/O account latch used when an ingress offer cannot fit."""

    @property
    def active(self) -> bool: ...

    def record_rejection(
        self,
        *,
        producer_id: str,
        callback_seq: int,
        callback_kind: str,
        observed_monotonic_ns: int,
    ) -> None: ...


@dataclass(frozen=True)
class FastSpiV2IngressSnapshot:
    """No-payload result of the v2 ingress safety boundary."""

    ring: FastSpiV2RingSnapshot
    disarmed: bool
    callback_gap_pending: bool
    callback_gap_latch_failed: bool


@dataclass(frozen=True)
class FastSpiV2GapProducerSnapshot:
    """One native gap-latch producer range, reconstructed only outside C5."""

    producer_id: int
    first_rejected_callback_seq: int
    last_rejected_callback_seq: int
    rejected_count: int
    first_observed_monotonic_ns: int
    last_observed_monotonic_ns: int


@dataclass(frozen=True)
class FastSpiV2GapLatchSnapshot:
    """Fixed scalar native gap evidence awaiting durable worker integration."""

    active: bool
    registration_failure: bool
    callback_kind_codes: tuple[int, ...]
    producers: tuple[FastSpiV2GapProducerSnapshot, ...]


class FastSpiV2NativeGapLatch:
    """Linux native atomic gap latch used at the C5 callback boundary.

    Producer and callback-kind labels are configuration-time diagnostics only.
    The native producer receives their numeric IDs and neither allocates a
    Python domain object nor acquires the M1 reference latch lock on overflow.
    A later worker converts this scalar snapshot into durable gap facts.
    """

    def __init__(
        self,
        *,
        maximum_producers: int = 16,
        maximum_callback_kinds: int = 128,
    ) -> None:
        if NativeFastSpiV2GapLatch is None:
            raise FastSpiV2Unavailable("native fastspi v2 gap latch is unavailable")
        if not 1 <= maximum_producers <= 16:
            raise ValueError("fastspi v2 native gap maximum_producers must be in 1..16")
        if not 1 <= maximum_callback_kinds <= 1024:
            raise ValueError("fastspi v2 native gap maximum_callback_kinds must be in 1..1024")
        self._backend: Any = NativeFastSpiV2GapLatch(
            maximum_producers=maximum_producers,
            maximum_callback_kinds=maximum_callback_kinds,
        )
        self._maximum_callback_kinds = maximum_callback_kinds
        self._producer_labels: dict[int, str] = {}
        self._callback_kind_labels: dict[int, str] = {}

    @property
    def active(self) -> bool:
        return bool(self._backend.active)

    @property
    def registration_failure(self) -> bool:
        return bool(self._backend.registration_failure)

    def register_producer(self, *, producer_id: int, label: str) -> None:
        _require_range(producer_id, "fastspi v2 gap producer_id", 0, MAX_PRODUCER_ID)
        _require_bounded_ascii(label, "fastspi v2 gap producer label", maximum_length=128)
        existing = self._producer_labels.get(producer_id)
        if existing is not None and existing != label:
            raise ValueError("fastspi v2 gap producer id cannot be relabelled")
        if not bool(self._backend.register_producer(producer_id)):
            raise FastSpiV2Unavailable("fastspi v2 native gap producer bound is exhausted")
        self._producer_labels[producer_id] = label

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None:
        _require_range(
            callback_kind,
            "fastspi v2 gap callback_kind",
            1,
            self._maximum_callback_kinds,
        )
        _require_bounded_ascii(label, "fastspi v2 gap callback kind", maximum_length=64)
        existing = self._callback_kind_labels.get(callback_kind)
        if existing is not None and existing != label:
            raise ValueError("fastspi v2 gap callback kind cannot be relabelled")
        self._callback_kind_labels[callback_kind] = label

    def producer_label(self, producer_id: int) -> str:
        """Resolve a configuration-time producer label outside the native path."""

        try:
            return self._producer_labels[producer_id]
        except KeyError as exc:
            raise FastSpiV2IntegrityError(
                "fastspi v2 native gap producer label is not registered"
            ) from exc

    def callback_kind_label(self, callback_kind: int) -> str:
        """Resolve a configuration-time callback label outside the native path."""

        try:
            return self._callback_kind_labels[callback_kind]
        except KeyError as exc:
            raise FastSpiV2IntegrityError(
                "fastspi v2 native gap callback kind label is not registered"
            ) from exc

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool:
        """Latch one already-rejected C5 record without durable I/O."""

        return bool(
            self._backend.record_rejection(
                producer_id,
                callback_seq,
                callback_kind,
                observed_monotonic_ns,
            )
        )

    def snapshot(self) -> FastSpiV2GapLatchSnapshot:
        raw = cast(tuple[Any, ...], self._backend.snapshot())
        if len(raw) != 4 or type(raw[0]) is not bool or type(raw[1]) is not bool:
            raise FastSpiV2IntegrityError(
                "fastspi v2 native gap latch returned an invalid snapshot"
            )
        raw_kinds = raw[2]
        raw_producers = raw[3]
        if not isinstance(raw_kinds, tuple) or not isinstance(raw_producers, tuple):
            raise FastSpiV2IntegrityError("fastspi v2 native gap latch collections are invalid")
        kind_values = cast(tuple[object, ...], raw_kinds)
        producer_values = cast(tuple[object, ...], raw_producers)
        callback_kind_codes = tuple(
            _native_int(value, "gap callback kind") for value in kind_values
        )
        producers: list[FastSpiV2GapProducerSnapshot] = []
        for raw_producer_value in producer_values:
            if not isinstance(raw_producer_value, tuple):
                raise FastSpiV2IntegrityError("fastspi v2 native gap producer is invalid")
            raw_producer = cast(tuple[object, ...], raw_producer_value)
            if len(raw_producer) != 6:
                raise FastSpiV2IntegrityError("fastspi v2 native gap producer is invalid")
            producers.append(
                FastSpiV2GapProducerSnapshot(
                    producer_id=_native_int(raw_producer[0], "gap producer id"),
                    first_rejected_callback_seq=_native_int(raw_producer[1], "gap first sequence"),
                    last_rejected_callback_seq=_native_int(raw_producer[2], "gap last sequence"),
                    rejected_count=_native_int(raw_producer[3], "gap rejected count"),
                    first_observed_monotonic_ns=_native_int(raw_producer[4], "gap first time"),
                    last_observed_monotonic_ns=_native_int(raw_producer[5], "gap last time"),
                )
            )
        return FastSpiV2GapLatchSnapshot(
            active=raw[0],
            registration_failure=raw[1],
            callback_kind_codes=callback_kind_codes,
            producers=tuple(producers),
        )

    def native_backend_for_c5(self) -> Any:
        """Return the extension object for another C5-native facade only."""

        return self._backend


class FastSpiV2NativeIngress:
    """C5 producer facade that keeps sequence, ring offer and gap latch native.

    ``offer`` transfers only the documented fixed scalar layout to the C11
    backend.  It intentionally has no UUID, dataclass, trace, metric, I/O or
    lifecycle-observer hook.  Domain construction begins only after the
    consumer pops a record from :class:`FastSpiV2Ring`.
    """

    def __init__(
        self,
        *,
        ring: FastSpiV2Ring,
        gap_latch: FastSpiV2NativeGapLatch,
        producer_id: int,
    ) -> None:
        _require_range(producer_id, "fastspi v2 native ingress producer_id", 0, MAX_PRODUCER_ID)
        if NativeFastSpiV2Ingress is None or not ring.native_available:
            raise FastSpiV2Unavailable("native fastspi v2 ingress is unavailable")
        self._ring = ring
        self._gap_latch = gap_latch
        self._backend: Any = NativeFastSpiV2Ingress(
            ring=ring.native_backend_for_c5(),
            gap_latch=gap_latch.native_backend_for_c5(),
            producer_id=producer_id,
        )

    @property
    def disarmed(self) -> bool:
        return bool(self._backend.disarmed)

    @property
    def callback_gap_latch_failed(self) -> bool:
        return bool(self._backend.callback_gap_latch_failed)

    @property
    def callback_gap_pending(self) -> bool:
        return self.callback_gap_latch_failed or self._gap_latch.active

    def offer(
        self,
        *,
        callback_kind: int,
        observed_monotonic_ns: int,
        client_id: int = 0,
        order_client_id: int = 0,
        order_emt_id: int = 0,
        order_cancel_emt_id: int = 0,
        status: int = 0,
        quantity: int = 0,
        trade_id_hash: int = 0,
        flags: int = 0,
        error_bytes: bytes | str = b"",
        order_update_time: int = 0,
        qty_traded: int = 0,
        qty_left: int = 0,
        trade_market: int = 0,
        trade_price: float | int = 0.0,
        trade_report_index: int = 0,
        trade_execution_id: bytes | str = b"",
        query_reference: int = 0,
    ) -> bool:
        """Attempt once; native C5 validates and UTF-8-boundary-truncates text."""

        return bool(
            self._backend.offer(
                callback_kind,
                observed_monotonic_ns,
                client_id,
                order_client_id,
                order_emt_id,
                order_cancel_emt_id,
                status,
                quantity,
                trade_id_hash,
                flags,
                error_bytes,
                order_update_time,
                qty_traded,
                qty_left,
                trade_market,
                trade_price,
                trade_report_index,
                trade_execution_id,
                query_reference,
            )
        )

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        """Close admission if a Python binding boundary cannot form scalars."""

        self._backend.force_gap(callback_kind, observed_monotonic_ns)


class FastSpiV2IntegrityError(RuntimeError):
    """The single consumer observed an impossible producer sequence order."""

    def __init__(self, message: str, *, record: FastSpiV2Record | None = None) -> None:
        super().__init__(message)
        self.record = record


@dataclass(frozen=True)
class FastSpiV2CallbackKind:
    """A stable numeric-to-name mapping resolved only after a record is popped."""

    code: int
    name: str

    def __post_init__(self) -> None:
        _require_range(self.code, "fastspi v2 callback kind code", 1, MAX_CALLBACK_KIND)
        if not self.name or not self.name.isascii() or len(self.name) > 64:
            raise ValueError("fastspi v2 callback kind name must be bounded ASCII text")


class FastSpiV2CallbackKindRegistry:
    """Immutable callback-kind decoder; it never touches a vendor SDK."""

    def __init__(self, entries: tuple[FastSpiV2CallbackKind, ...]) -> None:
        if not entries:
            raise ValueError("fastspi v2 callback kind registry must not be empty")
        by_code = {entry.code: entry for entry in entries}
        if len(by_code) != len(entries) or len({entry.name for entry in entries}) != len(entries):
            raise ValueError("fastspi v2 callback kind registry must be one-to-one")
        self._by_code = by_code

    def name_for(self, code: int) -> str:
        try:
            return self._by_code[code].name
        except KeyError as exc:
            raise FastSpiV2IntegrityError("fastspi v2 callback kind is not registered") from exc


@dataclass(frozen=True)
class FastSpiV2DrainResult:
    """Bounded consumer result without any durable or business side effect."""

    records_drained: int
    more_available: bool
    last_sequences: tuple[tuple[int, int], ...]


class FastSpiV2Consumer:
    """Single-consumer sequence fence and eventfd-safe ring drain helper.

    The handler runs after C5 and may allocate domain objects or write durable
    facts in a later integration.  This helper never silently coalesces records
    and does not acknowledge a callback, clear a gap latch or authorize work.
    """

    def __init__(self, *, ring: FastSpiV2Ring) -> None:
        self._ring = ring
        self._last_sequences: dict[int, int] = {}

    def drain(
        self,
        *,
        maximum_records: int,
        handle_record: Callable[[FastSpiV2Record], None],
    ) -> FastSpiV2DrainResult:
        if not 1 <= maximum_records <= 1024:
            raise ValueError("fastspi v2 maximum_records must be in 1..1024")
        self._ring.drain_wakeup()
        drained = 0
        while drained < maximum_records:
            record = self._ring.pop()
            if record is None:
                break
            previous = self._last_sequences.get(record.producer_id, 0)
            if record.callback_seq <= previous:
                raise FastSpiV2IntegrityError(
                    "fastspi v2 producer sequence did not advance",
                    record=record,
                )
            self._last_sequences[record.producer_id] = record.callback_seq
            handle_record(record)
            drained += 1
        more_available = self._ring.snapshot().depth > 0
        return FastSpiV2DrainResult(
            records_drained=drained,
            more_available=more_available,
            last_sequences=tuple(sorted(self._last_sequences.items())),
        )


class FastSpiV2CallbackIngress:
    """Offer fixed callback records and close admission on any ring overflow.

    This integration is intentionally a functional seam while the C producer
    bridge and atomic latch are introduced.  It preserves the required safety
    ordering now: reserve producer sequence, try the bounded ring once, record
    an overflow in the account latch without I/O, and leave the account
    DISARMED.  It must never retry, wait for capacity, or synthesize a record.
    """

    def __init__(
        self,
        *,
        ring: FastSpiV2Ring,
        sequence_allocator: FastSpiV2SequenceAllocator,
        callback_gap_latch: CallbackGapRecorderPort,
        producer_id: int,
        producer_label: str,
    ) -> None:
        _require_range(producer_id, "fastspi v2 producer_id", 0, MAX_PRODUCER_ID)
        if not producer_label or not producer_label.isascii() or len(producer_label) > 128:
            raise ValueError("fastspi v2 producer_label must be bounded ASCII text")
        self._ring = ring
        self._sequence_allocator = sequence_allocator
        self._callback_gap_latch = callback_gap_latch
        self._producer_id = producer_id
        self._producer_label = producer_label
        self._disarmed = False
        self._callback_gap_latch_failed = False
        self._state_lock = Lock()

    def offer(
        self,
        *,
        callback_kind: int,
        callback_kind_label: str,
        observed_monotonic_ns: int,
        client_id: int = 0,
        order_client_id: int = 0,
        order_emt_id: int = 0,
        order_cancel_emt_id: int = 0,
        status: int = 0,
        quantity: int = 0,
        trade_id_hash: int = 0,
        flags: int = 0,
        error_bytes: bytes = b"",
        order_update_time: int = 0,
        qty_traded: int = 0,
        qty_left: int = 0,
        trade_market: int = 0,
        trade_price: float = 0.0,
        trade_report_index: int = 0,
        trade_execution_id: bytes = b"",
        query_reference: int = 0,
    ) -> int | None:
        """Offer one callback once and return its sequence only on acceptance."""

        if (
            not callback_kind_label
            or not callback_kind_label.isascii()
            or len(callback_kind_label) > 64
        ):
            raise ValueError("fastspi v2 callback_kind_label must be bounded ASCII text")
        callback_seq = self._sequence_allocator.reserve(self._producer_id)
        try:
            latch_active = self._callback_gap_latch.active
        except Exception:
            with self._state_lock:
                self._disarmed = True
                self._callback_gap_latch_failed = True
            return None
        with self._state_lock:
            closed = self._disarmed or self._callback_gap_latch_failed
        if closed or latch_active:
            self._close_and_record_gap(
                callback_seq=callback_seq,
                callback_kind_label=callback_kind_label,
                observed_monotonic_ns=observed_monotonic_ns,
            )
            return None
        record = FastSpiV2Record(
            callback_kind=callback_kind,
            producer_id=self._producer_id,
            callback_seq=callback_seq,
            observed_monotonic_ns=observed_monotonic_ns,
            client_id=client_id,
            order_client_id=order_client_id,
            order_emt_id=order_emt_id,
            order_cancel_emt_id=order_cancel_emt_id,
            status=status,
            quantity=quantity,
            trade_id_hash=trade_id_hash,
            flags=flags,
            error_bytes=error_bytes,
            order_update_time=order_update_time,
            qty_traded=qty_traded,
            qty_left=qty_left,
            trade_market=trade_market,
            trade_price=trade_price,
            trade_report_index=trade_report_index,
            trade_execution_id=trade_execution_id,
            query_reference=query_reference,
        )
        if self._ring.try_push(record):
            return callback_seq
        self._close_and_record_gap(
            callback_seq=callback_seq,
            callback_kind_label=callback_kind_label,
            observed_monotonic_ns=observed_monotonic_ns,
        )

        return None

    def _close_and_record_gap(
        self,
        *,
        callback_seq: int,
        callback_kind_label: str,
        observed_monotonic_ns: int,
    ) -> None:
        """Keep later callbacks out of the ring once any account gap is active."""

        with self._state_lock:
            self._disarmed = True
        try:
            self._callback_gap_latch.record_rejection(
                producer_id=self._producer_label,
                callback_seq=callback_seq,
                callback_kind=callback_kind_label,
                observed_monotonic_ns=observed_monotonic_ns,
            )
        except Exception:
            # The caller is a vendor callback boundary.  A broken diagnostic
            # latch must make recovery more conservative, never surface across
            # the ABI or permit new work.
            with self._state_lock:
                self._callback_gap_latch_failed = True

    def snapshot(self) -> FastSpiV2IngressSnapshot:
        with self._state_lock:
            disarmed = self._disarmed
            latch_failed = self._callback_gap_latch_failed
        return FastSpiV2IngressSnapshot(
            ring=self._ring.snapshot(),
            disarmed=disarmed,
            callback_gap_pending=latch_failed or self._callback_gap_latch.active,
            callback_gap_latch_failed=latch_failed,
        )


class _PythonFastSpiV2Ring:
    """Functional fallback only; never a Linux performance acceptance backend."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._items: deque[FastSpiV2Record] = deque()
        self._lock = Lock()
        self._available = Event()
        self._high_watermark = 0
        self._pushed_count = 0
        self._popped_count = 0
        self._overflow_count = 0
        self._wakeup_count = 0

    def try_push(self, record: FastSpiV2Record) -> bool:
        with self._lock:
            if len(self._items) >= self._capacity:
                self._overflow_count += 1
                return False
            was_empty = not self._items
            self._items.append(record)
            self._pushed_count += 1
            self._high_watermark = max(self._high_watermark, len(self._items))
            if was_empty:
                self._wakeup_count += 1
                self._available.set()
            return True

    def pop(self) -> FastSpiV2Record | None:
        with self._lock:
            if not self._items:
                return None
            self._popped_count += 1
            return self._items.popleft()

    def wait_for_items(self, timeout_seconds: float) -> bool:
        return self._available.wait(timeout_seconds)

    def drain_wakeup(self) -> int:
        self._available.clear()
        with self._lock:
            if self._items:
                self._available.set()
            return 1

    def snapshot(self) -> FastSpiV2RingSnapshot:
        with self._lock:
            return FastSpiV2RingSnapshot(
                version=2,
                native_available=False,
                capacity=self._capacity,
                depth=len(self._items),
                high_watermark=self._high_watermark,
                pushed_count=self._pushed_count,
                popped_count=self._popped_count,
                overflow_count=self._overflow_count,
                wakeup_count=self._wakeup_count,
                wakeup_failures=0,
            )


class FastSpiV2Ring:
    """A fixed-record MPSC callback ring with an optional Linux native backend."""

    def __init__(
        self,
        capacity: int,
        *,
        require_native: bool = False,
        prefer_native: bool = True,
    ) -> None:
        _require_capacity(capacity)
        if NativeFastSpiV2Ring is not None and prefer_native:
            self._backend: Any = NativeFastSpiV2Ring(capacity)
            self._native_available = True
        elif require_native:
            raise FastSpiV2Unavailable("native fastspi v2 extension is unavailable on this runtime")
        else:
            self._backend = _PythonFastSpiV2Ring(capacity)
            self._native_available = False

    @property
    def native_available(self) -> bool:
        return self._native_available

    @property
    def wakeup_fd(self) -> int | None:
        if not self._native_available:
            return None
        return int(self._backend.wakeup_fd)

    def native_backend_for_c5(self) -> Any:
        """Return the extension object for another C5-native facade only."""

        if not self._native_available:
            raise FastSpiV2Unavailable("native fastspi v2 ring is unavailable")
        return self._backend

    def try_push(self, record: FastSpiV2Record) -> bool:
        if self._native_available:
            return bool(self._backend.try_push(*record.native_arguments()))
        return self._backend.try_push(record)

    def pop(self) -> FastSpiV2Record | None:
        raw = self._backend.pop()
        if raw is None:
            return None
        if self._native_available:
            if not isinstance(raw, tuple):
                raise RuntimeError("fastspi v2 native ring returned an invalid object")
            return FastSpiV2Record.from_native_tuple(cast(tuple[Any, ...], raw))
        if not isinstance(raw, FastSpiV2Record):
            raise RuntimeError("fastspi v2 fallback ring returned an invalid object")
        return raw

    def wait_for_items(self, timeout_seconds: float) -> bool:
        if timeout_seconds < 0:
            raise ValueError("fastspi v2 timeout must be non-negative")
        if self._native_available:
            return bool(self._backend.wait_for_items(timeout_seconds))
        return self._backend.wait_for_items(timeout_seconds)

    def drain_wakeup(self) -> int:
        return int(self._backend.drain_wakeup())

    def snapshot(self) -> FastSpiV2RingSnapshot:
        if not self._native_available:
            return self._backend.snapshot()
        return FastSpiV2RingSnapshot(
            version=2,
            native_available=True,
            capacity=int(self._backend.capacity),
            depth=int(self._backend.depth),
            high_watermark=int(self._backend.high_watermark),
            pushed_count=int(self._backend.pushed_count),
            popped_count=int(self._backend.popped_count),
            overflow_count=int(self._backend.overflow_count),
            wakeup_count=int(self._backend.wakeup_count),
            wakeup_failures=int(self._backend.wakeup_failures),
        )


def _require_capacity(capacity: int) -> None:
    if (
        type(capacity) is not int
        or not MIN_CAPACITY <= capacity <= MAX_CAPACITY
        or capacity & (capacity - 1)
    ):
        raise ValueError("fastspi v2 capacity must be a power of two in the supported range")


def _require_range(value: object, field_name: str, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{field_name} is out of range")


def _require_bounded_ascii(value: str, field_name: str, *, maximum_length: int) -> None:
    if not value or not value.isascii() or len(value) > maximum_length:
        raise ValueError(f"{field_name} must be bounded ASCII text")


def _native_int(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise RuntimeError(f"fastspi v2 native {field_name} is invalid")
    return value


def _native_float(value: object, field_name: str) -> float:
    if type(value) is not float or not isfinite(value):
        raise RuntimeError(f"fastspi v2 native {field_name} is invalid")
    return value


def _native_bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise RuntimeError("fastspi v2 native error bytes are invalid")
    return value
