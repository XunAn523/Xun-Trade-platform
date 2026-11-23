"""Bounded, payload-free operational telemetry outside the C0--C7 path.

``emit`` is deliberately limited to validating a pre-defined event and a
non-blocking in-memory enqueue.  It never constructs OpenTelemetry spans,
writes logs, opens sockets, or waits for an exporter.  A background worker
performs the optional OTLP metric aggregation and export instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from queue import Empty, Full, Queue
from re import fullmatch
from threading import Event, Lock, Thread
from time import monotonic, perf_counter_ns
from typing import Protocol


class TelemetryError(ValueError):
    """A caller supplied an unsafe or malformed telemetry field."""


class TelemetryExportPort(Protocol):
    """Background-only export boundary; implementations may perform I/O."""

    def export(self, events: tuple["OperationalTelemetryEvent", ...]) -> None: ...

    def close(self) -> None: ...


_IDENTIFIER_PATTERN = r"[A-Za-z0-9._:-]{1,128}"
_EVENT_PATTERN = r"[a-z][a-z0-9_.-]{0,63}"


@dataclass(frozen=True)
class OperationalTelemetryEvent:
    """The complete, versioned field allow-list for operational telemetry.

    The raw account identifier is intentionally absent from this object.  It
    is converted at construction to a stable, non-reversible SHA-256 prefix.
    No employee name, account display name, credential, token, payload, raw
    callback object, or broker error text has a field in this type.
    """

    event_name: str
    timestamp_ns: int
    consumer_id: str | None = None
    consumer_account_id: str | None = None
    strategy_id: str | None = None
    account_id_hash: str | None = None
    journal_lsn: int | None = None
    command_id: str | None = None
    execution_id: str | None = None
    order_client_id: str | None = None
    session_state: str | None = None
    failure_category: str | None = None

    def __post_init__(self) -> None:
        """Enforce the allow-list even if a caller bypasses ``create``."""

        _require_pattern(self.event_name, "event_name", _EVENT_PATTERN)
        if type(self.timestamp_ns) is not int or self.timestamp_ns < 0:
            raise TelemetryError("timestamp_ns must be a non-negative integer")
        if self.journal_lsn is not None and (
            type(self.journal_lsn) is not int or self.journal_lsn < 0
        ):
            raise TelemetryError("journal_lsn must be a non-negative integer when supplied")
        for field, value in (
            ("consumer_id", self.consumer_id),
            ("consumer_account_id", self.consumer_account_id),
            ("strategy_id", self.strategy_id),
            ("command_id", self.command_id),
            ("execution_id", self.execution_id),
            ("order_client_id", self.order_client_id),
            ("session_state", self.session_state),
            ("failure_category", self.failure_category),
        ):
            _optional_identifier(value, field)
        if self.account_id_hash is not None and not fullmatch(
            r"sha256:[0-9a-f]{16}", self.account_id_hash
        ):
            raise TelemetryError("account_id_hash has an invalid value")

    @classmethod
    def create(
        cls,
        *,
        event_name: str,
        consumer_id: str | None = None,
        consumer_account_id: str | None = None,
        strategy_id: str | None = None,
        account_id: str | None = None,
        journal_lsn: int | None = None,
        command_id: str | None = None,
        execution_id: str | None = None,
        order_client_id: int | str | None = None,
        session_state: str | None = None,
        failure_category: str | None = None,
    ) -> "OperationalTelemetryEvent":
        """Create a safe event without retaining raw account data."""

        _require_pattern(event_name, "event_name", _EVENT_PATTERN)
        return cls(
            event_name=event_name,
            timestamp_ns=perf_counter_ns(),
            consumer_id=_optional_identifier(consumer_id, "consumer_id"),
            consumer_account_id=_optional_identifier(consumer_account_id, "consumer_account_id"),
            strategy_id=_optional_identifier(strategy_id, "strategy_id"),
            account_id_hash=_account_hash(account_id),
            journal_lsn=journal_lsn,
            command_id=_optional_identifier(command_id, "command_id"),
            execution_id=_optional_identifier(execution_id, "execution_id"),
            order_client_id=_optional_identifier(
                str(order_client_id) if order_client_id is not None else None,
                "order_client_id",
            ),
            session_state=_optional_identifier(session_state, "session_state"),
            failure_category=_optional_identifier(failure_category, "failure_category"),
        )

    def attributes(self) -> dict[str, str | int]:
        """Return only the allow-listed attributes for an OTLP metric point."""

        values: dict[str, str | int | None] = {
            "event.name": self.event_name,
            "consumer.id": self.consumer_id,
            "consumer.account.id": self.consumer_account_id,
            "strategy.id": self.strategy_id,
            "account.id_hash": self.account_id_hash,
            "journal.lsn": self.journal_lsn,
            "command.id": self.command_id,
            "execution.id": self.execution_id,
            "order.client_id": self.order_client_id,
            "session.state": self.session_state,
            "failure.category": self.failure_category,
        }
        return {key: value for key, value in values.items() if value is not None}


@dataclass(frozen=True)
class TelemetryStats:
    accepted: int
    dropped: int
    exported: int
    export_failed: int
    queue_depth: int


class BoundedOperationalTelemetry:
    """A bounded, explicit-lifecycle telemetry producer.

    Start and close this component from control-plane startup/shutdown code,
    never from an EMT callback.  ``emit`` is safe to call from C0--C7 but does
    not guarantee telemetry delivery; business correctness never depends on
    this queue.
    """

    def __init__(
        self,
        *,
        exporter: TelemetryExportPort,
        capacity: int = 4_096,
        batch_size: int = 128,
        poll_interval_seconds: float = 0.050,
    ) -> None:
        if not 1 <= capacity <= 65_536:
            raise TelemetryError("capacity must be in 1..65536")
        if not 1 <= batch_size <= capacity:
            raise TelemetryError("batch_size must be in 1..capacity")
        if not 0.001 <= poll_interval_seconds <= 1.0:
            raise TelemetryError("poll_interval_seconds must be in 0.001..1.0")
        self._exporter = exporter
        self._queue: Queue[OperationalTelemetryEvent] = Queue(maxsize=capacity)
        self._batch_size = batch_size
        self._poll_interval_seconds = poll_interval_seconds
        self._stopped = Event()
        self._started = Event()
        self._worker: Thread | None = None
        self._lifecycle_lock = Lock()
        self._stats_lock = Lock()
        self._accepted = 0
        self._dropped = 0
        self._exported = 0
        self._export_failed = 0

    def start(self) -> None:
        """Start the exporter worker once, outside any trading callback."""

        with self._lifecycle_lock:
            if self._started.is_set():
                raise TelemetryError("telemetry worker is already started")
            if self._stopped.is_set():
                raise TelemetryError("telemetry worker is already closed")
            worker = Thread(target=self._run, name="emt-operational-telemetry", daemon=True)
            self._worker = worker
            self._started.set()
            worker.start()

    def emit(self, event: object) -> bool:
        """Attempt only a non-blocking enqueue; never invoke the exporter."""

        if not isinstance(event, OperationalTelemetryEvent):
            raise TypeError("event must be an OperationalTelemetryEvent")
        # Keep the close/emit transition atomic: otherwise an emitter can see
        # an open queue while shutdown has already allowed the worker to exit.
        with self._lifecycle_lock:
            if self._stopped.is_set():
                self._increment("_dropped")
                return False
            try:
                self._queue.put_nowait(event)
            except Full:
                self._increment("_dropped")
                return False
        self._increment("_accepted")
        return True

    def stats(self) -> TelemetryStats:
        with self._stats_lock:
            return TelemetryStats(
                accepted=self._accepted,
                dropped=self._dropped,
                exported=self._exported,
                export_failed=self._export_failed,
                queue_depth=self._queue.qsize(),
            )

    def wait_until_idle(self, timeout_seconds: float) -> bool:
        """Control-plane test helper; never call on the trading hot path."""

        if timeout_seconds < 0:
            raise TelemetryError("timeout_seconds must be non-negative")
        deadline = monotonic() + timeout_seconds
        while monotonic() < deadline:
            stats = self.stats()
            if stats.queue_depth == 0 and stats.accepted == stats.exported + stats.export_failed:
                return True
            self._stopped.wait(0.002)
        return False

    def close(self) -> None:
        """Stop after draining queued events; exporter shutdown is control-plane work."""

        with self._lifecycle_lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
            worker = self._worker
        if worker is not None:
            worker.join(timeout=max(1.0, self._poll_interval_seconds * 4))
        try:
            self._exporter.close()
        except Exception:
            # Shutdown telemetry failure must not alter transaction recovery.
            self._increment("_export_failed")

    def _run(self) -> None:
        while not self._stopped.is_set() or not self._queue.empty():
            batch = self._next_batch()
            if not batch:
                continue
            try:
                self._exporter.export(batch)
            except Exception:
                self._increment("_export_failed", len(batch))
                # Export failure means these best-effort events have been
                # dropped.  It is observable, but does not alter C0--C7.
                self._increment("_dropped", len(batch))
            else:
                self._increment("_exported", len(batch))
            finally:
                for _ in batch:
                    self._queue.task_done()

    def _next_batch(self) -> tuple[OperationalTelemetryEvent, ...]:
        try:
            first = self._queue.get(timeout=self._poll_interval_seconds)
        except Empty:
            return ()
        events = [first]
        while len(events) < self._batch_size:
            try:
                events.append(self._queue.get_nowait())
            except Empty:
                break
        return tuple(events)

    def _increment(self, field: str, amount: int = 1) -> None:
        with self._stats_lock:
            setattr(self, field, getattr(self, field) + amount)


class OtlpGrpcMetricExporter:
    """Optional OTLP/gRPC metric exporter, constructed only outside C0--C7."""

    def __init__(
        self,
        *,
        endpoint: str,
        service_name: str = "emt-gateway",
        export_interval_millis: int = 10_000,
        timeout_seconds: float = 1.0,
    ) -> None:
        if not endpoint.strip():
            raise TelemetryError("endpoint is required")
        _require_pattern(service_name, "service_name", _EVENT_PATTERN)
        if not 1_000 <= export_interval_millis <= 300_000:
            raise TelemetryError("export_interval_millis must be in 1000..300000")
        if not 0.1 <= timeout_seconds <= 10.0:
            raise TelemetryError("timeout_seconds must be in 0.1..10")
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource

        exporter = OTLPMetricExporter(endpoint=endpoint, timeout=timeout_seconds)
        reader = PeriodicExportingMetricReader(
            exporter, export_interval_millis=export_interval_millis
        )
        provider = MeterProvider(
            resource=Resource.create({SERVICE_NAME: service_name}),
            metric_readers=(reader,),
        )
        self._provider = provider
        self._counter = provider.get_meter("emt_gateway.operational_telemetry").create_counter(
            "emt_gateway_operational_events_total",
            description="Payload-free operational events emitted after C0--C7",
        )

    def export(self, events: tuple[OperationalTelemetryEvent, ...]) -> None:
        for event in events:
            self._counter.add(1, event.attributes())

    def close(self) -> None:
        self._provider.shutdown()


def _optional_identifier(value: str | None, field: str) -> str | None:
    if value is None:
        return None
    _require_pattern(value, field, _IDENTIFIER_PATTERN)
    return value


def _account_hash(account_id: object | None) -> str | None:
    if account_id is None:
        return None
    if not isinstance(account_id, str) or not 1 <= len(account_id.strip()) <= 256:
        raise TelemetryError("account_id must contain 1..256 characters when supplied")
    return f"sha256:{sha256(account_id.encode('utf-8')).hexdigest()[:16]}"


def _require_pattern(value: object, field: str, pattern: str) -> None:
    if not isinstance(value, str) or not fullmatch(pattern, value):
        raise TelemetryError(f"{field} has an invalid value")
