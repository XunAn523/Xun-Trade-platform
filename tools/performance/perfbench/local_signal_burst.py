"""Account-free 300-signals/second gateway-local stress measurement.

The driver deliberately substitutes a thread-safe in-process native port for
EMT.  It therefore measures only the gateway's durable C0--C4 admission and
C5--C7 callback path under concurrent signal arrival; it cannot place an
order, load an SDK, open a network connection, or make a counter-latency
claim.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Barrier, BrokenBarrierError, Condition, Event, Lock, Thread
from time import monotonic, perf_counter_ns
from typing import Any

from emt_gateway.gateway import CommandState, OrderGateway, OrderIntent
from emt_gateway.journal import FileJournal, GroupCommitJournal, JournalAppendTiming

from .histogram import ExactHistogram
from .report_schema import finalize_performance_report
from .trace import GatewayTraceCollector, TraceRecord


class LocalSignalBurstError(RuntimeError):
    """The account-free local signal benchmark cannot complete."""


@dataclass(frozen=True)
class LocalSignalBurstSettings:
    runtime_dir: Path
    signals_per_second: int = 300
    rounds: int = 10
    warmup_rounds: int = 0
    journal_mode: str = "group-commit"
    group_commit_max_wait_ms: float = 0.0
    group_commit_max_batch_size: int = 128
    callback_batch_size: int = 64
    callback_collect_wait_ms: float = 0.0
    arrival_pattern: str = "uniform"

    def validate(self) -> None:
        if not 1 <= self.signals_per_second <= 300:
            raise LocalSignalBurstError("signals_per_second must be in 1..300")
        if not 1 <= self.rounds <= 600:
            raise LocalSignalBurstError("rounds must be in 1..600")
        if not 0 <= self.warmup_rounds <= 60:
            raise LocalSignalBurstError("warmup_rounds must be in 0..60")
        if self.journal_mode not in {"write-through", "group-commit"}:
            raise LocalSignalBurstError("journal_mode must be write-through or group-commit")
        if not 0.0 <= self.group_commit_max_wait_ms <= 10.0:
            raise LocalSignalBurstError("group_commit_max_wait_ms must be in 0..10")
        if not 1 <= self.group_commit_max_batch_size <= 1024:
            raise LocalSignalBurstError("group_commit_max_batch_size must be in 1..1024")
        if not 1 <= self.callback_batch_size <= 1024:
            raise LocalSignalBurstError("callback_batch_size must be in 1..1024")
        if not 0.0 <= self.callback_collect_wait_ms <= 10.0:
            raise LocalSignalBurstError("callback_collect_wait_ms must be in 0..10")
        if self.arrival_pattern not in {"uniform", "burst"}:
            raise LocalSignalBurstError("arrival_pattern must be uniform or burst")


class _NoopNative:
    """Thread-safe positive identifiers without SDK, network, or order entry."""

    def __init__(self) -> None:
        self._next_id = 1
        self._lock = Lock()

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        del command_id, client_order_id
        with self._lock:
            native_id = self._next_id
            self._next_id += 1
        return native_id

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        del command_id, order_emt_id
        raise AssertionError("local signal benchmark never issues cancellation")

    def get_last_error_code(self) -> int:
        return 0


class _CallbackDrainer:
    def __init__(self, gateway: OrderGateway, batch_size: int, collect_wait_ms: float) -> None:
        self._gateway = gateway
        self._batch_size = batch_size
        self._collect_wait_seconds = collect_wait_ms / 1_000
        self._stop = Event()
        self._failure: BaseException | None = None
        self._delivered = 0
        self._condition = Condition(Lock())
        self._thread = Thread(
            target=self._run, name="emt-local-signal-callback-drainer", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def wait_for(self, count: int, timeout_seconds: float) -> None:
        deadline = monotonic() + timeout_seconds
        with self._condition:
            while self._delivered < count and self._failure is None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise LocalSignalBurstError("callback drain timed out")
                self._condition.wait(remaining)
            if self._failure is not None:
                raise LocalSignalBurstError("callback drainer failed") from self._failure

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)
        if self._thread.is_alive():
            raise LocalSignalBurstError("callback drainer did not stop")
        if self._failure is not None:
            raise LocalSignalBurstError("callback drainer failed") from self._failure

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                deliveries = self._gateway.callbacks.normalize_many(
                    self._batch_size,
                    collect_wait_seconds=self._collect_wait_seconds,
                )
                if not deliveries:
                    self._gateway.callbacks.wait_for_callbacks(0.050)
                    continue
                for delivery in deliveries:
                    self._gateway.callbacks.ack(delivery.delivery_id)
                with self._condition:
                    self._delivered += len(deliveries)
                    self._condition.notify_all()
        except BaseException as exc:
            with self._condition:
                self._failure = exc
                self._condition.notify_all()


def run_local_signal_burst(settings: LocalSignalBurstSettings) -> dict[str, Any]:
    """Execute bounded local bursts, never touching EMT or business payloads."""

    settings.validate()
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    timing_history_capacity = settings.signals_per_second * (
        settings.rounds + settings.warmup_rounds
    )
    command_file = FileJournal(
        settings.runtime_dir / "local-signal-commands.emtj",
        timing_history_capacity=timing_history_capacity,
    )
    event_file = FileJournal(
        settings.runtime_dir / "local-signal-events.emtj",
        timing_history_capacity=timing_history_capacity,
    )
    command_journal: Any = command_file
    event_journal: Any = event_file
    if settings.journal_mode == "group-commit":
        options = {
            "max_batch_size": settings.group_commit_max_batch_size,
            "max_wait_seconds": settings.group_commit_max_wait_ms / 1_000,
            # The uncontended fast path retains the same durable append but
            # avoids a producer-to-worker handoff. Once another append is
            # active, arrivals join the bounded FIFO group instead.
            # At the one-signal-per-second safety gate there is no useful
            # batching opportunity, so remove the producer-to-worker handoff.
            # Sustained 300/s traffic remains on the commit worker: direct
            # flushes there would contend with callback flushes and worsen the
            # storage tail.
            "direct_when_idle": settings.arrival_pattern == "uniform"
            and settings.signals_per_second == 1,
            # Only CALL_STARTED authorizes native side effects. Outcomes stay
            # durable and make forward progress, but do not sit ahead of a
            # new pre-native safety record in a mixed high-rate batch.
            "priority_kinds": ("CALL_STARTED",),
        }
        command_journal = GroupCommitJournal(command_file, **options)
        # Commands and callback evidence always use independent files. Enable
        # callback group commit only for an actual burst: on a uniformly
        # arriving stream it would add a worker hand-off yet have no records to
        # coalesce between adjacent ~3.3 ms arrivals at 300/s.
        if settings.arrival_pattern == "burst":
            event_journal = GroupCommitJournal(event_file, **options)
    trace = GatewayTraceCollector()
    gateway = OrderGateway(
        native=_NoopNative(),
        command_journal=command_journal,
        event_journal=event_journal,
        trace=trace,
        callback_capacity=max(4096, settings.signals_per_second * 4),
        require_native_fastspi=False,
    )
    drainer = _CallbackDrainer(
        gateway, settings.callback_batch_size, settings.callback_collect_wait_ms
    )
    drainer.start()
    accepted = 0
    started_ns = perf_counter_ns()
    round_elapsed_ns: list[int] = []
    try:
        with ThreadPoolExecutor(
            max_workers=settings.signals_per_second, thread_name_prefix="emt-local-signal"
        ) as executor:
            for round_number in range(settings.warmup_rounds + settings.rounds):
                target = started_ns + round_number * 1_000_000_000
                _wait_until(target)
                round_started_ns = perf_counter_ns()
                if settings.arrival_pattern == "burst":
                    futures = _submit_burst(
                        executor, gateway, settings.signals_per_second, round_number
                    )
                else:
                    futures = _submit_uniform(
                        executor,
                        gateway,
                        settings.signals_per_second,
                        round_number,
                        target,
                    )
                results = [future.result(timeout=15.0) for future in futures]
                if round_number >= settings.warmup_rounds:
                    round_elapsed_ns.append(perf_counter_ns() - round_started_ns)
                accepted += sum(results)
                drainer.wait_for(accepted, timeout_seconds=15.0)
        finished_ns = perf_counter_ns()
        measurement_start_ns = started_ns + settings.warmup_rounds * 1_000_000_000
        report = _report(
            settings=settings,
            trace=trace,
            accepted=accepted - settings.warmup_rounds * settings.signals_per_second,
            total_elapsed_ns=finished_ns - started_ns,
            measurement_elapsed_ns=finished_ns - measurement_start_ns,
            round_elapsed_ns=round_elapsed_ns,
            measurement_start_ns=measurement_start_ns,
            callback_disarmed=gateway.callbacks.disarmed,
            native_fastspi_available=gateway.callbacks.native_fastspi_available,
            command_journal_timings=command_file.timing_history(),
            event_journal_timings=event_file.timing_history(),
        )
        return report
    finally:
        try:
            drainer.close()
        finally:
            for journal in (event_journal, command_journal):
                if isinstance(journal, GroupCommitJournal):
                    journal.close()
            event_file.close()
            command_file.close()


def _submit_burst(
    executor: ThreadPoolExecutor,
    gateway: OrderGateway,
    signals_per_second: int,
    round_number: int,
) -> list[Any]:
    barrier = Barrier(signals_per_second + 1)
    futures = [
        executor.submit(_submit_signal, gateway, barrier, round_number, signal_number)
        for signal_number in range(signals_per_second)
    ]
    try:
        barrier.wait(timeout=10.0)
    except BrokenBarrierError as exc:
        raise LocalSignalBurstError(
            "signal workers did not reach the concurrent start barrier"
        ) from exc
    return futures


def _submit_uniform(
    executor: ThreadPoolExecutor,
    gateway: OrderGateway,
    signals_per_second: int,
    round_number: int,
    window_start_ns: int,
) -> list[Any]:
    futures: list[Any] = []
    for signal_number in range(signals_per_second):
        target = window_start_ns + signal_number * 1_000_000_000 // signals_per_second
        _wait_until(target)
        futures.append(executor.submit(_submit_signal, gateway, None, round_number, signal_number))
    return futures


def _submit_signal(
    gateway: OrderGateway,
    barrier: Barrier | None,
    round_number: int,
    signal_number: int,
) -> int:
    if barrier is not None:
        try:
            barrier.wait(timeout=10.0)
        except BrokenBarrierError as exc:
            raise LocalSignalBurstError("signal worker start barrier failed") from exc
    client_order_id = f"local-{round_number}-{signal_number}"
    payload_hash = sha256(client_order_id.encode("ascii")).hexdigest()
    result = gateway.submit_order(
        OrderIntent(client_order_id=client_order_id, payload_hash=payload_hash)
    )
    if result.state is not CommandState.NATIVE_ACCEPTED or result.native_id is None:
        raise LocalSignalBurstError("local signal was not accepted by the durable gateway path")
    callback_id = gateway.callbacks.receive(
        "OnOrderEvent",
        profile="ordinary",
        workload="steady",
        order_emt_id=result.native_id,
    )
    if callback_id is None:
        raise LocalSignalBurstError("local callback ring overflowed")
    return 1


def _wait_until(target_ns: int) -> None:
    while True:
        remaining_ns = target_ns - perf_counter_ns()
        if remaining_ns <= 0:
            return
        Event().wait(min(remaining_ns / 1_000_000_000, 0.001))


def _report(
    *,
    settings: LocalSignalBurstSettings,
    trace: GatewayTraceCollector,
    accepted: int,
    total_elapsed_ns: int,
    measurement_elapsed_ns: int,
    round_elapsed_ns: list[int],
    measurement_start_ns: int,
    callback_disarmed: bool,
    native_fastspi_available: bool,
    command_journal_timings: tuple[JournalAppendTiming, ...],
    event_journal_timings: tuple[JournalAppendTiming, ...],
) -> dict[str, Any]:
    records = trace.records()
    command_records = _completed(
        records, kind="api", interface="InsertOrder", minimum_c0_ns=measurement_start_ns
    )
    callback_trace_records = _completed(
        records, kind="spi", interface="OnOrderEvent", minimum_c0_ns=measurement_start_ns
    )
    target = settings.signals_per_second * settings.rounds
    report = {
        "schema_version": 1,
        "driver": "gateway-local-signal-burst",
        "measurement_scope": "local gateway only; no EMT SDK, account, network, or order entry",
        "evidence_classification": {
            "classification": "local_stub_reference",
            "counter_performance_eligible": False,
            "release_gate_eligible": False,
            "must_not_merge_with_emt_trace_evidence": True,
        },
        "safety": {
            "sdk_loaded": False,
            "login": False,
            "network": False,
            "order_entry": False,
            "native_fastspi_required": False,
            "native_fastspi_available": native_fastspi_available,
            "business_payload_in_report": False,
        },
        "run": {
            "signals_per_second": settings.signals_per_second,
            "arrival_pattern": settings.arrival_pattern,
            "worker_pool_size": settings.signals_per_second,
            "concurrent_signals_per_round": settings.signals_per_second
            if settings.arrival_pattern == "burst"
            else None,
            "rounds": settings.rounds,
            "warmup_rounds": settings.warmup_rounds,
            "target_signals": target,
            "accepted_signals": accepted,
            "rejected_or_missing_signals": target - accepted,
            "total_elapsed_ns": total_elapsed_ns,
            "measurement_elapsed_ns": measurement_elapsed_ns,
            "effective_signals_per_second": accepted * 1_000_000_000 / measurement_elapsed_ns,
            "round_elapsed": _summary(round_elapsed_ns),
            "journal_mode": settings.journal_mode,
            "group_commit_max_wait_ms": settings.group_commit_max_wait_ms
            if settings.journal_mode == "group-commit"
            else None,
            "group_commit_max_batch_size": settings.group_commit_max_batch_size
            if settings.journal_mode == "group-commit"
            else None,
            "group_commit_direct_when_idle": (
                settings.arrival_pattern == "uniform" and settings.signals_per_second == 1
                if settings.journal_mode == "group-commit"
                else None
            ),
            "journal_lanes": {
                "command": "local-signal-commands.emtj",
                "callback": "local-signal-events.emtj",
                "independent_group_commit_workers": (
                    settings.journal_mode == "group-commit" and settings.arrival_pattern == "burst"
                ),
            },
            "callback_batch_size": settings.callback_batch_size,
            "callback_collect_wait_ms": settings.callback_collect_wait_ms,
            "callback_disarmed": callback_disarmed,
        },
        "latency": {
            "C0_to_C1_admission": _latency(command_records, "C0", "C1"),
            "C1_to_C2_command_durable": _latency(command_records, "C1", "C2"),
            "C2_to_C3_native_handoff": _latency(command_records, "C2", "C3"),
            "C3_to_C4_native_return": _latency(command_records, "C3", "C4"),
            "C0_to_C4_signal_accept": _latency(command_records, "C0", "C4"),
            "C4_to_C5_callback_wait_excluded": _latency(command_records, "C4", "C5"),
            # The callback trace begins at physical callback entry, which can
            # legitimately fall just after the scheduled measurement boundary
            # for a signal submitted during warmup.  The command trace is
            # linked to the first matching callback by the gateway and has the
            # source C0 timestamp, so it is the authoritative population for
            # excluding warmup from C5--C7 while retaining the same stages.
            "C5_to_C6_callback_durable": _latency(command_records, "C5", "C6"),
            "C6_to_C7_callback_ack": _latency(command_records, "C6", "C7"),
            "C5_to_C7_callback_complete": _latency(command_records, "C5", "C7"),
        },
        "journal_commit_breakdown": {
            "command": _journal_timing_summary(command_journal_timings),
            "callback": _journal_timing_summary(event_journal_timings),
        },
        "one_signal_per_second_local_gate": _one_signal_per_second_gate(
            settings=settings,
            command_records=command_records,
            callback_trace_records=callback_trace_records,
        ),
    }
    return finalize_performance_report(report, requested_driver="gateway-local-signal-burst")


def _completed(
    records: tuple[TraceRecord, ...], *, kind: str, interface: str, minimum_c0_ns: int = 0
) -> list[TraceRecord]:
    return [
        record
        for record in records
        if record.kind == kind
        and record.interface == interface
        and record.outcome == "ok"
        and record.timestamps_ns.get("C0", 0) >= minimum_c0_ns
    ]


def _latency(records: list[TraceRecord], start: str, end: str) -> dict[str, int | None]:
    histogram = ExactHistogram()
    for record in records:
        if start in record.timestamps_ns and end in record.timestamps_ns:
            histogram.record(record.timestamps_ns[end] - record.timestamps_ns[start])
    return _summary_from_histogram(histogram)


def _one_signal_per_second_gate(
    *,
    settings: LocalSignalBurstSettings,
    command_records: list[TraceRecord],
    callback_trace_records: list[TraceRecord],
) -> dict[str, Any]:
    """Check only local stages; C4→C5 is deliberately EMT-external."""

    measured = {
        "C0_to_C1_admission": _latency(command_records, "C0", "C1"),
        "C1_to_C2_command_durable": _latency(command_records, "C1", "C2"),
        "C2_to_C3_native_handoff": _latency(command_records, "C2", "C3"),
        "C3_to_C4_native_return": _latency(command_records, "C3", "C4"),
        "C5_to_C6_callback_durable": _latency(command_records, "C5", "C6"),
        "C6_to_C7_callback_ack": _latency(command_records, "C6", "C7"),
    }
    applicable = settings.arrival_pattern == "uniform" and settings.signals_per_second == 1
    budget_ns = 1_000_000
    failed_paths = [
        path
        for path, summary in measured.items()
        if summary["count"] and isinstance(summary["p99_ns"], int) and summary["p99_ns"] > budget_ns
    ]
    return {
        "applicable": applicable,
        "budget_ms": 1.0,
        "excluded_path": "C4_to_C5_callback_wait_excluded",
        "measurements": measured,
        "passed": applicable and not failed_paths,
        "failed_paths": failed_paths if applicable else [],
        "callback_trace_records_after_boundary": len(callback_trace_records),
    }


def _summary(samples: list[int]) -> dict[str, int | None]:
    histogram = ExactHistogram()
    for sample in samples:
        histogram.record(sample)
    return _summary_from_histogram(histogram)


def _journal_timing_summary(samples: tuple[JournalAppendTiming, ...]) -> dict[str, Any]:
    """Summarize durable batches without recording any journal identity."""

    fields = (
        "validation_and_encoding_ns",
        "lock_wait_ns",
        "frame_encoding_ns",
        "write_ns",
        "flush_ns",
        "total_ns",
    )
    return {
        "durable_batches": len(samples),
        "durable_records": sum(sample.entry_count for sample in samples),
        "per_batch": {
            field: _summary([getattr(sample, field) for sample in samples]) for field in fields
        },
    }


def _summary_from_histogram(histogram: ExactHistogram) -> dict[str, int | None]:
    if histogram.count:
        return histogram.summary()
    return {
        "count": 0,
        "min_ns": None,
        "p50_ns": None,
        "p95_ns": None,
        "p99_ns": None,
        "p999_ns": None,
        "max_ns": None,
    }
