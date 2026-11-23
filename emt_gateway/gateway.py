"""P2 order/cancel path with C0--C7 performance instrumentation.

This is a deliberately narrow core: strategy protocols and vendor structures
remain outside it, the native SDK is accessed only through ``NativeOrderPort``.
Callbacks use the optional-native ``fastspi`` MPSC boundary; deployments can
make a missing C extension an explicit startup failure.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from hashlib import sha256
from threading import Event, Lock
from time import perf_counter_ns, sleep
from typing import Callable, Protocol, cast
from uuid import uuid4

from .fastspi import FastSpiMpscRing
from .journal import DurableJournal, JournalAppend, JournalRecord
from .native_api import NativeOrderPort
from .order_ledger import (
    AsyncPostgresOrderLedger,
    LedgerCallbackFact,
    LedgerOrderIntent,
)
from .telemetry import NullPerformanceTrace, PerformanceTracePort


class GatewayError(RuntimeError):
    """A stable local gateway rejection before a new native side effect."""


class UnknownOrderError(GatewayError):
    """A cancel target has no proven, owned EMT order mapping."""


class CommandState(str, Enum):
    RECEIVED = "RECEIVED"
    REJECTED_LOCAL = "REJECTED_LOCAL"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    CALL_STARTED = "CALL_STARTED"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    LOCAL_SEND_REJECTED = "LOCAL_SEND_REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"


@dataclass(frozen=True)
class OrderIntent:
    client_order_id: str
    payload_hash: str
    profile: str = "ordinary"
    workload: str = "steady"
    ledger_intent: LedgerOrderIntent | None = None


@dataclass(frozen=True)
class CancelIntent:
    client_cancel_id: str
    target_client_order_id: str
    profile: str = "ordinary"
    workload: str = "steady"
    ledger_account_id: str | None = None
    ledger_trading_day: str | None = None

    @property
    def payload_hash(self) -> str:
        return sha256(self.target_client_order_id.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CommandResult:
    command_id: str
    correlation_id: str
    state: CommandState
    journal_lsn: int | None
    native_id: int | None
    native_error_code: int | None


@dataclass
class _Command:
    command_id: str
    correlation_id: str
    client_key: tuple[str, str]
    interface: str
    profile: str
    workload: str
    payload_hash: str
    state: CommandState = CommandState.RECEIVED
    journal_lsn: int | None = None
    native_id: int | None = None
    native_error_code: int | None = None
    ledger_intent: LedgerOrderIntent | None = None
    ledger_target: _LedgerTarget | None = None

    def result(self) -> CommandResult:
        return CommandResult(
            command_id=self.command_id,
            correlation_id=self.correlation_id,
            state=self.state,
            journal_lsn=self.journal_lsn,
            native_id=self.native_id,
            native_error_code=self.native_error_code,
        )


@dataclass(frozen=True)
class _LedgerTarget:
    account_id: str
    trading_day: str
    gateway_order_id: str
    order_emt_id: int


@dataclass(frozen=True)
class CallbackRecord:
    callback_correlation_id: str
    callback_name: str
    profile: str
    workload: str
    callback_entry_ns: int
    ring_enqueue_ns: int
    order_emt_id: int | None
    operation_native_id: int | None
    producer_id: str
    callback_seq: int
    request_id: int | None = None
    is_last: bool | None = None
    query_item_present: bool | None = None
    query_reference: int | None = None
    ledger_fact: LedgerCallbackFact | None = None


@dataclass(frozen=True)
class CallbackDelivery:
    delivery_id: str
    callback_name: str
    callback_correlation_id: str
    source_command_id: str | None
    event_lsn: int


class CallbackLifecycleObserver(Protocol):
    """Optional observer for query correlation; it receives no native payload."""

    def on_enqueued(self, record: CallbackRecord) -> None: ...

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None: ...

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None: ...


class CallbackGapLatchPort(Protocol):
    """Account-bound, in-memory overflow latch; implementations perform no I/O."""

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


class _AsyncLedgerCallbackObserver:
    """Fan out callback facts to the async PG projector after C6 is visible."""

    def __init__(
        self,
        *,
        delegate: CallbackLifecycleObserver | None,
        enqueue_projection: Callable[[CallbackRecord], None],
    ) -> None:
        self._delegate = delegate
        self._enqueue_projection = enqueue_projection

    def on_enqueued(self, record: CallbackRecord) -> None:
        if self._delegate is not None:
            self._delegate.on_enqueued(record)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        if self._delegate is not None:
            self._delegate.on_durable(record, durable_ns)
        # ``enqueue_projection`` only calls Queue.put_nowait and swallows all
        # projection-side errors.  It cannot turn a durable callback into a
        # gateway failure or delay the strategy delivery with PG I/O.
        self._enqueue_projection(record)

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        if self._delegate is not None:
            self._delegate.on_ack(record, acknowledged_ns)


class CallbackPipeline:
    """Moves minimal callback facts from fastspi to durable event evidence."""

    def __init__(
        self,
        *,
        journal: DurableJournal,
        trace: PerformanceTracePort,
        lookup_source: Callable[[CallbackRecord], _Command | None],
        capacity: int,
        lifecycle_observer: CallbackLifecycleObserver | None = None,
        require_native_fastspi: bool = False,
        callback_gap_latch: CallbackGapLatchPort | None = None,
        callback_producer_id: str = "p0-callback-pipeline",
    ) -> None:
        if (
            not callback_producer_id
            or not callback_producer_id.isascii()
            or len(callback_producer_id) > 128
        ):
            raise ValueError("callback producer_id must be bounded ASCII text")
        self._journal = journal
        self._trace = trace
        self._lookup_source = lookup_source
        self._ring = FastSpiMpscRing(capacity, require_native=require_native_fastspi)
        self._callbacks_available = Event()
        self._lifecycle_observer = lifecycle_observer
        self._deliveries: dict[str, tuple[CallbackRecord, _Command | None]] = {}
        self._delivery_lock = Lock()
        self._producer_lock = Lock()
        self._callback_sequence = 0
        self._callback_producer_id = callback_producer_id
        self._callback_gap_latch = callback_gap_latch
        self._callback_gap_available = Event()
        self._source_stages: dict[str, set[str]] = {}
        self.disarmed = False
        self.callback_gap_latch_failed = False

    @property
    def native_fastspi_available(self) -> bool:
        """Whether this callback boundary uses the optional native MPSC ring.

        Callers may report this capability, but must still decide separately
        whether their environment and workload make a throughput claim valid.
        """

        return self._ring.native_available

    def receive(
        self,
        callback_name: str,
        *,
        profile: str,
        workload: str,
        order_emt_id: int | None = None,
        operation_native_id: int | None = None,
        request_id: int | None = None,
        is_last: bool | None = None,
        query_item_present: bool | None = None,
        query_reference: int | None = None,
        ledger_fact: LedgerCallbackFact | None = None,
    ) -> str | None:
        """P2 callback entry: copy minimal ids, enqueue, and return immediately."""

        now = perf_counter_ns()
        callback_correlation_id = str(uuid4())
        self._trace.begin(
            callback_correlation_id,
            kind="spi",
            interface=callback_name,
            profile=profile,
            workload=workload,
            timestamp_ns=now,
        )
        with self._producer_lock:
            self._callback_sequence += 1
            callback_sequence = self._callback_sequence
            record = CallbackRecord(
                callback_correlation_id=callback_correlation_id,
                callback_name=callback_name,
                profile=profile,
                workload=workload,
                callback_entry_ns=now,
                ring_enqueue_ns=0,
                order_emt_id=order_emt_id,
                operation_native_id=operation_native_id,
                producer_id=self._callback_producer_id,
                callback_seq=callback_sequence,
                request_id=request_id,
                is_last=is_last,
                query_item_present=query_item_present,
                query_reference=query_reference,
                ledger_fact=ledger_fact,
            )
            enqueued_ns = self._ring.try_push(record)
            if enqueued_ns is None:
                gap_observed_ns = perf_counter_ns()
                if self._callback_gap_latch is None:
                    self.callback_gap_latch_failed = True
                else:
                    try:
                        self._callback_gap_latch.record_rejection(
                            producer_id=self._callback_producer_id,
                            callback_seq=callback_sequence,
                            callback_kind=callback_name,
                            observed_monotonic_ns=gap_observed_ns,
                        )
                    except Exception:
                        # No exception may cross the SDK callback boundary.  The
                        # account remains disarmed, and the host must treat this
                        # diagnostic-latch failure as an unbounded recovery gap.
                        self.callback_gap_latch_failed = True
        if enqueued_ns is None:
            self.disarmed = True
            self._callback_gap_available.set()
            self._trace.record_metric("dropped_callbacks", 1)
            self._trace.record_metric("callback_ring_depth", self._ring.depth)
            self._trace.complete(callback_correlation_id, "failed", "callback_ring_overflow")
            return None
        self._trace.mark(callback_correlation_id, "C5", enqueued_ns)
        # Wake the durable consumer immediately after C5 is trace-visible.
        # Lifecycle hooks and depth telemetry are deliberately kept after the
        # wake-up: neither authorizes an ACK, and doing them first adds avoidable
        # scheduler tail to C5->C6 under a sustained callback stream.
        self._callbacks_available.set()
        if self._lifecycle_observer is not None:
            self._lifecycle_observer.on_enqueued(replace(record, ring_enqueue_ns=enqueued_ns))
        self._trace.record_metric("callback_ring_depth", self._ring.depth)
        return callback_correlation_id

    @property
    def callback_gap_pending(self) -> bool:
        latch = self._callback_gap_latch
        return self.callback_gap_latch_failed or (latch is not None and latch.active)

    def wait_for_callback_gap(self, timeout_seconds: float) -> bool:
        """Wait on the local wake-up only; the producer never persists the gap."""

        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        return self._callback_gap_available.wait(timeout_seconds)

    def acknowledge_callback_gap_persisted(self) -> bool:
        """Clear the wake-up only after the injected latch has no pending snapshot."""

        latch = self._callback_gap_latch
        if self.callback_gap_latch_failed or (latch is not None and latch.active):
            return False
        self._callback_gap_available.clear()
        if self.callback_gap_latch_failed or (latch is not None and latch.active):
            self._callback_gap_available.set()
            return False
        return True

    def wait_for_callbacks(self, timeout_seconds: float) -> bool:
        """Block a consumer until a callback may be ready to drain.

        The event is only a wake-up hint.  Consumers still pop from the MPSC
        ring, including a second pop after clearing it, so a producer cannot
        lose a notification in the clear/set race.
        """

        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        return self._callbacks_available.wait(timeout_seconds)

    def normalize_one(self) -> CallbackDelivery | None:
        """Durably record one callback and allocate a strategy delivery id."""

        record = self._pop_available()
        if record is None:
            return None
        deliveries = self._normalize_records((record,))
        assert len(deliveries) == 1
        return deliveries[0]

    def normalize_post_ring_record(self, record: CallbackRecord) -> CallbackDelivery:
        """Normalize one record that was materialized by a later ingress worker.

        V2 creates this record only after its native C5 slot has been popped.
        Consequently UUID creation, tracing and lifecycle observation remain
        outside the SDK callback boundary while V1 keeps its existing public
        ``receive`` and ring behavior unchanged.
        """

        self._trace.begin(
            record.callback_correlation_id,
            kind="spi",
            interface=record.callback_name,
            profile=record.profile,
            workload=record.workload,
            timestamp_ns=record.callback_entry_ns,
        )
        self._trace.mark(record.callback_correlation_id, "C5", record.ring_enqueue_ns)
        if self._lifecycle_observer is not None:
            self._lifecycle_observer.on_enqueued(record)
        deliveries = self._normalize_records((record,))
        assert len(deliveries) == 1
        return deliveries[0]

    def normalize_many(
        self,
        max_items: int = 64,
        *,
        collect_wait_seconds: float = 0.0,
    ) -> tuple[CallbackDelivery, ...]:
        """Durably normalize a bounded callback batch with one group flush.

        The method never ACKs on behalf of a consumer.  It only groups records
        that have already crossed C5, and returns them after their event
        evidence is durable.  Thus a failed group remains fail-closed.
        """

        if not 1 <= max_items <= 1024:
            raise ValueError("max_items must be in 1..1024")
        if not 0.0 <= collect_wait_seconds <= 0.010:
            raise ValueError("collect_wait_seconds must be in [0, 0.010]")
        records: list[CallbackRecord] = []
        deadline = perf_counter_ns() + int(collect_wait_seconds * 1_000_000_000)
        while len(records) < max_items:
            record = self._pop_available()
            if record is not None:
                records.append(record)
                continue
            # A lone low-rate callback must not incur the artificial
            # microbatch deadline.  When the ring is empty, persist it now;
            # under a burst the depth remains positive and the loop drains
            # the readily available batch before the shared flush.
            if not records or self._ring.depth == 0 or perf_counter_ns() >= deadline:
                break
            # The ring intentionally has no blocking consumer API.  This tiny,
            # bounded wait coalesces a concurrent burst before one write-through
            # flush; it never acknowledges an undurable callback.
            # On Windows ``sleep(0.0001)`` is typically rounded to a complete
            # scheduler tick (~15.6 ms).  The callback coalescing deadline is
            # intentionally sub-millisecond, so yield without requesting a
            # timed sleep; producers can enqueue before the next ring poll.
            sleep(0)
        return self._normalize_records(tuple(records))

    def _pop_available(self) -> CallbackRecord | None:
        record = self._ring.pop()
        if record is not None:
            return record
        self._callbacks_available.clear()
        # If a producer set the event just before the clear, this second pop
        # observes its record.  If it pushes afterwards, its set remains for
        # the consumer's next blocking wait.
        record = self._ring.pop()
        if record is not None:
            self._callbacks_available.set()
        return record

    def _normalize_records(
        self, records: tuple[CallbackRecord, ...]
    ) -> tuple[CallbackDelivery, ...]:
        if not records:
            return ()
        sources = tuple(self._lookup_source(record) for record in records)
        entries = tuple(
            JournalAppend(
                kind="NATIVE_CALLBACK",
                command_id=source.command_id if source else None,
                correlation_id=record.callback_correlation_id,
                payload_hash=sha256(
                    f"{record.callback_name}:{record.order_emt_id}:{record.operation_native_id}".encode(
                        "utf-8"
                    )
                ).hexdigest(),
            )
            for record, source in zip(records, sources)
        )
        try:
            append_batch = getattr(self._journal, "append_batch", None)
            if callable(append_batch):
                events = tuple(
                    cast(
                        Callable[[tuple[JournalAppend, ...]], tuple[JournalRecord, ...]],
                        append_batch,
                    )(entries)
                )
            else:
                events = tuple(
                    self._journal.append(
                        kind=entry.kind,
                        command_id=entry.command_id,
                        correlation_id=entry.correlation_id,
                        payload_hash=entry.payload_hash,
                    )
                    for entry in entries
                )
            if len(events) != len(records):
                raise GatewayError("callback journal returned an incomplete batch")
        except Exception:
            self.disarmed = True
            for record, source in zip(records, sources):
                self._trace.complete(
                    record.callback_correlation_id, "failed", "event_journal_failure"
                )
                if source is not None:
                    self._trace.complete(source.correlation_id, "failed", "event_journal_failure")
            raise
        durable_ns = perf_counter_ns()
        deliveries: list[CallbackDelivery] = []
        for record, source, event in zip(records, sources, events):
            self._trace.mark(record.callback_correlation_id, "C6", durable_ns)
            if self._lifecycle_observer is not None:
                self._lifecycle_observer.on_durable(record, durable_ns)
            if source is not None:
                self._mark_source_once(source, "C5", record.ring_enqueue_ns)
                self._mark_source_once(source, "C6", durable_ns)
            delivery = CallbackDelivery(
                delivery_id=str(uuid4()),
                callback_name=record.callback_name,
                callback_correlation_id=record.callback_correlation_id,
                source_command_id=source.command_id if source else None,
                event_lsn=event.lsn,
            )
            with self._delivery_lock:
                self._deliveries[delivery.delivery_id] = (record, source)
            deliveries.append(delivery)
        self._trace.record_metric("callback_ring_depth", self._ring.depth)
        return tuple(deliveries)

    def ack(self, delivery_id: str) -> None:
        """Record strategy durable ACK; no callback or command is replayed here."""

        with self._delivery_lock:
            try:
                record, source = self._deliveries.pop(delivery_id)
            except KeyError as exc:
                raise GatewayError("unknown callback delivery") from exc
        acknowledged_ns = perf_counter_ns()
        self._trace.mark(record.callback_correlation_id, "C7", acknowledged_ns)
        if self._lifecycle_observer is not None:
            self._lifecycle_observer.on_ack(record, acknowledged_ns)
        self._trace.complete(record.callback_correlation_id, "ok")
        if source is not None and self._mark_source_once(source, "C7", acknowledged_ns):
            self._trace.complete(source.correlation_id, "ok")

    def _mark_source_once(self, source: _Command, stage: str, timestamp_ns: int) -> bool:
        """Join only the first callback lifecycle to a command trace.

        EMT may notify the same order/cancel state more than once.  Every
        notification remains its own SPI trace, but a command has one C5-C7
        path; recording later notifications again would corrupt a valid trace
        with duplicate stage timestamps.
        """

        with self._delivery_lock:
            stages = self._source_stages.setdefault(source.command_id, set())
            if stage in stages:
                return False
            stages.add(stage)
        self._trace.mark(source.correlation_id, stage, timestamp_ns)
        return True


class OrderGateway:
    """Durable-before-native order/cancel emitter with correlation-safe callbacks."""

    def __init__(
        self,
        *,
        native: NativeOrderPort,
        command_journal: DurableJournal,
        event_journal: DurableJournal,
        trace: PerformanceTracePort | None = None,
        admit: Callable[[str, str], None] | None = None,
        callback_capacity: int = 4096,
        callback_lifecycle_observer: CallbackLifecycleObserver | None = None,
        require_native_fastspi: bool = False,
        callback_gap_latch: CallbackGapLatchPort | None = None,
        callback_producer_id: str = "p0-callback-pipeline",
        order_ledger: AsyncPostgresOrderLedger | None = None,
        ledger_account_id: str | None = None,
        ledger_trading_day: str | None = None,
        reconcile_ledger_on_start: bool = True,
    ) -> None:
        self._native = native
        self._command_journal = command_journal
        self._trace = trace or NullPerformanceTrace()
        self._admit = admit or self._default_admit
        self._order_ledger = order_ledger
        if order_ledger is None:
            if ledger_account_id is not None or ledger_trading_day is not None:
                raise ValueError("ledger scope requires an order_ledger")
            self._ledger_scope: tuple[str, str] | None = None
        else:
            if not isinstance(order_ledger, AsyncPostgresOrderLedger):
                raise TypeError(
                    "OrderGateway requires AsyncPostgresOrderLedger; direct PostgreSQL is not a hot-path API"
                )
            if not isinstance(ledger_account_id, str) or not ledger_account_id.strip():
                raise ValueError("ledger_account_id is required with an order_ledger")
            if not isinstance(ledger_trading_day, str) or not ledger_trading_day.strip():
                raise ValueError("ledger_trading_day is required with an order_ledger")
            self._ledger_scope = (ledger_account_id, ledger_trading_day)
            if reconcile_ledger_on_start:
                order_ledger.begin_reconciliation(
                    account_id=ledger_account_id,
                    trading_day=ledger_trading_day,
                    event_id=f"gateway-start:{uuid4()}",
                    reason="gateway_process_start",
                )
        self._commands: dict[tuple[str, str], _Command] = {}
        self._native_correlations: dict[int, _Command] = {}
        self._lock = Lock()
        self.callbacks = CallbackPipeline(
            journal=event_journal,
            trace=self._trace,
            lookup_source=self._lookup_callback_source,
            capacity=callback_capacity,
            lifecycle_observer=(
                _AsyncLedgerCallbackObserver(
                    delegate=callback_lifecycle_observer,
                    enqueue_projection=self._enqueue_callback_projection,
                )
                if order_ledger is not None
                else callback_lifecycle_observer
            ),
            require_native_fastspi=require_native_fastspi,
            callback_gap_latch=callback_gap_latch,
            callback_producer_id=callback_producer_id,
        )

    def submit_order(self, intent: OrderIntent) -> CommandResult:
        command, is_new = self._begin(
            client_key=("order", intent.client_order_id),
            interface="InsertOrder",
            profile=intent.profile,
            workload=intent.workload,
            payload_hash=intent.payload_hash,
            ledger_intent=intent.ledger_intent,
        )
        if not is_new or command.state is not CommandState.RECEIVED:
            return command.result()
        return self._execute(
            command,
            lambda: self._native.insert_order(
                command_id=command.command_id,
                client_order_id=intent.client_order_id,
            ),
        )

    def cancel_order(self, intent: CancelIntent) -> CommandResult:
        with self._lock:
            target = self._commands.get(("order", intent.target_client_order_id))
            if (
                target is not None
                and target.state is CommandState.NATIVE_ACCEPTED
                and target.native_id is not None
            ):
                target_native_id = target.native_id
                ledger_target = self._ledger_target_for_command(target)
            else:
                target_native_id = None
                ledger_target = None
        if target_native_id is None:
            # PostgreSQL is an asynchronous projection and is never read on
            # the order path.  After a restart a cancel is enabled only by the
            # controlled EMT reconciliation which restores an in-memory proven
            # mapping; a stale/missing projection cannot add latency or become
            # a source of unsafe cancel authority.
            raise UnknownOrderError("cancel target has no in-memory proven native order mapping")
        command, is_new = self._begin(
            client_key=("cancel", intent.client_cancel_id),
            interface="CancelOrder",
            profile=intent.profile,
            workload=intent.workload,
            payload_hash=intent.payload_hash,
            ledger_target=ledger_target,
        )
        if not is_new or command.state is not CommandState.RECEIVED:
            return command.result()
        return self._execute(
            command,
            lambda: self._native.cancel_order(
                command_id=command.command_id,
                order_emt_id=target_native_id,
            ),
        )

    def _begin(
        self,
        *,
        client_key: tuple[str, str],
        interface: str,
        profile: str,
        workload: str,
        payload_hash: str,
        ledger_intent: LedgerOrderIntent | None = None,
        ledger_target: _LedgerTarget | None = None,
    ) -> tuple[_Command, bool]:
        if not client_key[1] or not payload_hash:
            raise GatewayError("client id and payload hash are required")
        with self._lock:
            existing = self._commands.get(client_key)
            if existing is not None:
                if existing.payload_hash != payload_hash:
                    raise GatewayError(
                        "idempotency conflict: client id has a different payload hash"
                    )
                return existing, False
            command = _Command(
                command_id=str(uuid4()),
                correlation_id=str(uuid4()),
                client_key=client_key,
                interface=interface,
                profile=profile,
                workload=workload,
                payload_hash=payload_hash,
                ledger_intent=ledger_intent,
                ledger_target=ledger_target,
            )
            self._commands[client_key] = command
        self._trace.begin(
            command.correlation_id,
            kind="api",
            interface=interface,
            profile=profile,
            workload=workload,
        )
        try:
            if interface == "InsertOrder" and (
                self.callbacks.disarmed or self.callbacks.callback_gap_pending
            ):
                raise GatewayError("callback ingress is disarmed")
            self._admit(interface, payload_hash)
        except Exception as exc:
            self._set_state(command, CommandState.REJECTED_LOCAL)
            self._trace.complete(command.correlation_id, "rejected", type(exc).__name__)
            return command, True
        self._trace.mark(command.correlation_id, "C1")
        self._enqueue_ledger_admission(command)
        return command, True

    def _execute(self, command: _Command, native_call: Callable[[], int]) -> CommandResult:
        try:
            # Persisting CALL_STARTED before crossing the native boundary is
            # deliberately conservative: a crash immediately afterward may
            # over-classify an unsent command as ambiguous, but can never cause
            # an automatic duplicate send.  It replaces the former PREPARED +
            # CALL_STARTED pair and removes one synchronous flush from C0-C4.
            call_started = self._command_journal.append(
                kind="CALL_STARTED",
                command_id=command.command_id,
                correlation_id=command.correlation_id,
                payload_hash=command.payload_hash,
            )
        except Exception:
            self._set_state(command, CommandState.REJECTED_LOCAL)
            self._trace.complete(command.correlation_id, "failed", "command_journal_failure")
            return command.result()
        self._set_state(command, CommandState.CALL_STARTED, call_started)
        self._enqueue_ledger_call_started(command)
        self._trace.mark(command.correlation_id, "C2")
        self._trace.mark(command.correlation_id, "C3")
        try:
            native_id = int(native_call())
        except Exception as exc:
            self._trace.mark(command.correlation_id, "C4")
            self._record_submit_unknown(command, type(exc).__name__)
            return command.result()
        self._trace.mark(command.correlation_id, "C4")
        if native_id == 0:
            try:
                error_code = int(self._native.get_last_error_code())
            except Exception:
                self._record_submit_unknown(command, "native_error_lookup_failure")
                return command.result()
            try:
                rejected = self._command_journal.append(
                    kind="LOCAL_SEND_REJECTED",
                    command_id=command.command_id,
                    correlation_id=command.correlation_id,
                    payload_hash=command.payload_hash,
                )
            except Exception:
                self._record_submit_unknown(command, "send_rejected_journal_failure")
                return command.result()
            self._enqueue_ledger_native_rejection(command, "native_zero_return")
            self._set_state(
                command, CommandState.LOCAL_SEND_REJECTED, rejected, native_error_code=error_code
            )
            self._trace.complete(command.correlation_id, "rejected", "native_zero_return")
            return command.result()
        try:
            accepted = self._command_journal.append(
                kind="NATIVE_ACCEPTED",
                command_id=command.command_id,
                correlation_id=command.correlation_id,
                payload_hash=command.payload_hash,
            )
        except Exception:
            self._record_submit_unknown(command, "native_accepted_journal_failure")
            return command.result()
        self._set_state(command, CommandState.NATIVE_ACCEPTED, accepted, native_id=native_id)
        self._enqueue_ledger_native_acceptance(command, native_id)
        with self._lock:
            self._native_correlations[native_id] = command
        return command.result()

    def _record_submit_unknown(self, command: _Command, reason: str) -> None:
        self._enqueue_ledger_submit_unknown(command, reason)
        try:
            unknown = self._command_journal.append(
                kind="SUBMIT_UNKNOWN",
                command_id=command.command_id,
                correlation_id=command.correlation_id,
                payload_hash=command.payload_hash,
            )
        except Exception:
            unknown = None
        self._set_state(command, CommandState.SUBMIT_UNKNOWN, unknown)
        self._trace.complete(command.correlation_id, "failed", reason)

    def _enqueue_ledger_admission(self, command: _Command) -> None:
        """Queue a projection fact without authorizing or delaying EMT."""

        if self._order_ledger is None:
            return
        if command.interface == "InsertOrder":
            metadata = command.ledger_intent
            if metadata is None or self._ledger_scope != (
                metadata.account_id,
                metadata.trading_day,
            ):
                return
            self._order_ledger.record_intent(
                event_id=f"{command.command_id}:ORDER_INTENT",
                account_id=metadata.account_id,
                trading_day=metadata.trading_day,
                gateway_order_id=command.command_id,
                strategy_id=metadata.strategy_id,
                strategy_order_id=metadata.strategy_order_id,
                client_order_id=command.client_key[1],
                ticker=metadata.ticker,
                market=metadata.market,
                side=metadata.side,
                order_qty=metadata.order_qty,
                limit_price=metadata.limit_price,
            )
            return
        if command.interface == "CancelOrder":
            target = command.ledger_target
            if target is None or self._ledger_scope != (target.account_id, target.trading_day):
                return
            self._order_ledger.record_cancel_requested(
                event_id=f"{command.command_id}:CANCEL_REQUESTED",
                account_id=target.account_id,
                trading_day=target.trading_day,
                gateway_order_id=target.gateway_order_id,
            )

    def _enqueue_ledger_call_started(self, command: _Command) -> None:
        if self._order_ledger is None or command.interface != "InsertOrder":
            return
        metadata = command.ledger_intent
        if metadata is None or self._ledger_scope != (metadata.account_id, metadata.trading_day):
            return
        self._order_ledger.record_call_started(
            event_id=f"{command.command_id}:CALL_STARTED",
            account_id=metadata.account_id,
            trading_day=metadata.trading_day,
            gateway_order_id=command.command_id,
        )

    def _enqueue_ledger_native_acceptance(self, command: _Command, native_id: int) -> None:
        if self._order_ledger is None:
            return
        if command.interface == "InsertOrder":
            metadata = command.ledger_intent
            if metadata is None or self._ledger_scope != (
                metadata.account_id,
                metadata.trading_day,
            ):
                return
            self._order_ledger.record_sdk_accepted(
                event_id=f"{command.command_id}:SDK_ACCEPTED",
                account_id=metadata.account_id,
                trading_day=metadata.trading_day,
                gateway_order_id=command.command_id,
                order_emt_id=native_id,
            )
        elif command.interface == "CancelOrder":
            target = command.ledger_target
            if target is None or self._ledger_scope != (target.account_id, target.trading_day):
                return
            self._order_ledger.record_cancel_accepted(
                event_id=f"{command.command_id}:CANCEL_ACCEPTED",
                account_id=target.account_id,
                trading_day=target.trading_day,
                gateway_order_id=target.gateway_order_id,
                cancel_emt_id=native_id,
            )

    def _enqueue_ledger_native_rejection(self, command: _Command, reason: str) -> None:
        if self._order_ledger is None:
            return
        if command.interface == "InsertOrder":
            metadata = command.ledger_intent
            if metadata is None or self._ledger_scope != (
                metadata.account_id,
                metadata.trading_day,
            ):
                return
            self._order_ledger.record_sdk_rejected(
                event_id=f"{command.command_id}:SDK_REJECTED",
                account_id=metadata.account_id,
                trading_day=metadata.trading_day,
                gateway_order_id=command.command_id,
                reason=reason,
            )
        elif command.interface == "CancelOrder":
            target = command.ledger_target
            if target is None or self._ledger_scope != (target.account_id, target.trading_day):
                return
            self._order_ledger.record_cancel_rejected(
                event_id=f"{command.command_id}:CANCEL_REJECTED",
                account_id=target.account_id,
                trading_day=target.trading_day,
                gateway_order_id=target.gateway_order_id,
                reason=reason,
            )

    def _enqueue_ledger_submit_unknown(self, command: _Command, reason: str) -> None:
        if self._order_ledger is None:
            return
        if (
            command.interface == "InsertOrder"
            and command.ledger_intent is not None
            and self._ledger_scope
            == (command.ledger_intent.account_id, command.ledger_intent.trading_day)
        ):
            metadata = command.ledger_intent
            self._order_ledger.record_submit_unknown(
                event_id=f"{command.command_id}:SUBMIT_UNKNOWN",
                account_id=metadata.account_id,
                trading_day=metadata.trading_day,
                gateway_order_id=command.command_id,
                reason=reason,
            )
        elif (
            command.interface == "CancelOrder"
            and command.ledger_target is not None
            and self._ledger_scope
            == (command.ledger_target.account_id, command.ledger_target.trading_day)
        ):
            target = command.ledger_target
            self._order_ledger.require_reconciliation(
                event_id=f"{command.command_id}:CANCEL_RECONCILE_REQUIRED",
                account_id=target.account_id,
                trading_day=target.trading_day,
                gateway_order_id=target.gateway_order_id,
                reason=f"cancel_submit_unknown:{reason}",
            )

    @staticmethod
    def _ledger_target_for_command(command: _Command) -> _LedgerTarget | None:
        metadata = command.ledger_intent
        if metadata is None or command.native_id is None:
            return None
        return _LedgerTarget(
            account_id=metadata.account_id,
            trading_day=metadata.trading_day,
            gateway_order_id=command.command_id,
            order_emt_id=command.native_id,
        )

    def _enqueue_callback_projection(self, record: CallbackRecord) -> None:
        """Queue a callback fact for the PG worker without touching PostgreSQL.

        The callback journal has already become durable when this method is
        called. A full queue, a malformed projection fact, or an unavailable
        PostgreSQL instance affects only the replaceable read model; C5--C7
        and strategy ACK remain governed by the event journal.
        """

        ledger = self._order_ledger
        scope = self._ledger_scope
        fact = record.ledger_fact
        if ledger is None or scope is None or fact is None:
            return
        account_id, trading_day = scope
        try:
            ledger.record_callback_fact(
                account_id=account_id,
                trading_day=trading_day,
                fact=fact,
            )
        except Exception:
            # The adapter already absorbs enqueue failures. Retaining this
            # boundary keeps the non-blocking callback contract intact if a
            # future asynchronous projection adapter is substituted.
            return

    def _set_state(
        self,
        command: _Command,
        state: CommandState,
        journal: JournalRecord | None = None,
        *,
        native_id: int | None = None,
        native_error_code: int | None = None,
    ) -> None:
        with self._lock:
            command.state = state
            if journal is not None:
                command.journal_lsn = journal.lsn
            if native_id is not None:
                command.native_id = native_id
            if native_error_code is not None:
                command.native_error_code = native_error_code

    def _lookup_callback_source(self, record: CallbackRecord) -> _Command | None:
        with self._lock:
            if record.operation_native_id is not None:
                source = self._native_correlations.get(record.operation_native_id)
                if source is not None:
                    return source
            if record.order_emt_id is not None:
                return self._native_correlations.get(record.order_emt_id)
            return None

    @staticmethod
    def _default_admit(interface: str, payload_hash: str) -> None:
        if interface not in {"InsertOrder", "CancelOrder"}:
            raise GatewayError("unsupported command interface")
        if len(payload_hash) != 64 or any(
            character not in "0123456789abcdef" for character in payload_hash
        ):
            raise GatewayError("payload_hash must be a lowercase SHA-256 hex digest")
