"""Same-session native-direct versus durable-gateway A/B command benchmark.

This driver is intentionally narrower than a generic performance benchmark.
It executes a bounded simulated ``InsertOrder -> CancelOrder`` cycle in two
paths on **the same EMT session and account**, alternates the path order every
cycle, and never retries a side-effecting request:

* ``native_direct`` calls the loaded Python binding directly.  It measures
  C0->C4 (synchronous binding return), C4->C5 (first matching EMT callback),
  and C0->C5.  It deliberately has no command journal or ACK stages.
* ``gateway_full`` uses ``OrderGateway``, ``FileJournal``, native fastspi and
  the durable callback/ACK pipeline.  It measures C0->C2, C2->C4, C4->C5,
  C5->C6 and C6->C7 from the real gateway trace.

The reported native/gateway difference is an *observed sequential A/B delta*,
not proof that a particular EMT network or counter component caused it.  The
driver is a controlled simulated-account fixture only; it is not a production
SLA or throughput driver.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from math import ceil
from pathlib import Path
from threading import Event, Lock
from time import perf_counter_ns
from typing import Any

from emt_gateway.gateway import (
    CancelIntent,
    CommandResult,
    CommandState,
    OrderGateway,
    OrderIntent,
)
from emt_gateway.journal import FileJournal
from emt_gateway.native_api import EmtOrderAdapter
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import CallbackIngressPort

from .gateway_mutation import (
    GatewayMutationError,
    OrderCancelPlan,
    _CallbackDrainer,
    _new_native_client_id,
    _trace_record,
    _wait_for_command_slot,
    _write_payload_free_trace,
    select_order_cancel_plan,
)
from .histogram import ExactHistogram
from .mutation_fixture import load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector, TraceRecord


class GatewayAbError(RuntimeError):
    """The controlled same-session comparison cannot be run safely."""


@dataclass(frozen=True)
class GatewayAbSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    trace_output: Path | None = None
    profile: str = "ordinary"
    cycles: int = 1
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 1.0
    execute_simulated: bool = False
    require_native_fastspi: bool = True

    def validate(self) -> None:
        if self.profile not in {"ordinary", "credit", "option"}:
            raise GatewayAbError(
                "A/B order comparison supports ordinary, credit or option simulated accounts only"
            )
        if not 1 <= self.cycles <= 10:
            raise GatewayAbError("A/B comparison cycles must be in 1..10")
        if self.callback_timeout_seconds <= 0:
            raise GatewayAbError("callback_timeout_seconds must be positive")
        if self.interval_seconds < 1.0:
            raise GatewayAbError("A/B comparison interval_seconds must stay at or above one second")


@dataclass(frozen=True)
class DirectTiming:
    cycle: int
    interface: str
    outcome: str
    reason: str | None
    c0_ns: int
    c4_ns: int | None
    c5_ns: int | None

    def latency(self, start: str, end: str) -> int | None:
        timestamps = {"C0": self.c0_ns, "C4": self.c4_ns, "C5": self.c5_ns}
        started, ended = timestamps[start], timestamps[end]
        if started is None or ended is None or ended < started:
            return None
        return ended - started


@dataclass
class _DirectPending:
    interface: str
    started_ns: int
    expected_native_id: int | None = None
    returned_ns: int | None = None
    candidates: list[tuple[int | None, int | None, int]] = field(default_factory=list)
    callback_ns: int | None = None
    done: Event = field(default_factory=Event)


class _DirectCallbackProbe:
    """Capture only timing and native correlation IDs for one direct command.

    The callback bridge intentionally exposes no payload to this probe.  An
    order callback can race the synchronous ``InsertOrder``/``CancelOrder``
    return, so candidates are retained only until the returned EMT ID is known
    and then matched by the correct order or cancel-operation ID.
    """

    def __init__(self) -> None:
        self._active: _DirectPending | None = None
        self._lock = Lock()

    def begin(self, interface: str) -> _DirectPending:
        if interface not in {"InsertOrder", "CancelOrder"}:
            raise GatewayAbError(f"unsupported direct comparison interface: {interface}")
        pending = _DirectPending(interface=interface, started_ns=perf_counter_ns())
        with self._lock:
            if self._active is not None:
                raise GatewayAbError("a direct callback comparison is already active")
            self._active = pending
        return pending

    def mark_return(self, pending: _DirectPending, native_id: int, returned_ns: int) -> None:
        with self._lock:
            if self._active is not pending:
                raise GatewayAbError("direct callback comparison lost its active request")
            pending.returned_ns = returned_ns
            pending.expected_native_id = native_id
            self._match_locked(pending)

    def observe(
        self,
        callback_name: str,
        *,
        order_emt_id: int | None,
        operation_native_id: int | None,
    ) -> None:
        if callback_name != "OnOrderEvent":
            return
        observed_ns = perf_counter_ns()
        with self._lock:
            pending = self._active
            if pending is None:
                return
            pending.candidates.append((order_emt_id, operation_native_id, observed_ns))
            self._match_locked(pending)

    def finish(self, pending: _DirectPending, timeout_seconds: float) -> int | None:
        if not pending.done.wait(timeout_seconds):
            self.abort(pending)
            return None
        with self._lock:
            if self._active is pending:
                self._active = None
            return pending.callback_ns

    def abort(self, pending: _DirectPending) -> None:
        """Disarm a failed direct request without waiting or retrying it."""

        with self._lock:
            if self._active is pending:
                self._active = None

    @staticmethod
    def _match_locked(pending: _DirectPending) -> None:
        expected = pending.expected_native_id
        if expected is None or pending.callback_ns is not None:
            return
        for order_emt_id, operation_native_id, observed_ns in pending.candidates:
            matched = (
                order_emt_id == expected
                if pending.interface == "InsertOrder"
                else operation_native_id == expected
            )
            if matched:
                pending.callback_ns = observed_ns
                pending.done.set()
                return


class _AbCallbackIngress:
    """Observe direct-command callbacks and forward every callback to gateway."""

    def __init__(self) -> None:
        self.direct = _DirectCallbackProbe()
        self._target: CallbackIngressPort | None = None
        self._lock = Lock()

    def attach(self, target: CallbackIngressPort) -> None:
        with self._lock:
            if self._target is not None:
                raise GatewayAbError("A/B callback ingress is already attached")
            self._target = target

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
    ) -> str | None:
        self.direct.observe(
            callback_name,
            order_emt_id=order_emt_id,
            operation_native_id=operation_native_id,
        )
        with self._lock:
            target = self._target
        if target is None:
            return None
        return target.receive(
            callback_name,
            profile=profile,
            workload=workload,
            order_emt_id=order_emt_id,
            operation_native_id=operation_native_id,
            request_id=request_id,
            is_last=is_last,
            query_item_present=query_item_present,
            query_reference=query_reference,
        )


def run_gateway_ab(settings: GatewayAbSettings) -> dict[str, Any]:
    """Preflight or execute the bounded same-session native/gateway A/B run."""

    settings.validate()
    try:
        plan = select_order_cancel_plan(
            load_mutation_fixture(settings.fixture, profile=settings.profile),
            profile=settings.profile,
        )
    except GatewayMutationError as exc:
        raise GatewayAbError(str(exc)) from exc
    if not settings.execute_simulated:
        return _preflight_report(settings, plan)
    return _execute_same_session(settings, plan)


def _preflight_report(settings: GatewayAbSettings, plan: OrderCancelPlan) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "driver": "gateway-ab-order-c0-c7",
        "executed": False,
        "measurement_scope": (
            "preflight only; a real run alternates native direct and durable gateway "
            "order/cancel paths on one EMT simulated-account session"
        ),
        "safety": {
            "mode": "preflight-only",
            "sdk_loaded": False,
            "login": False,
            "order_entry": False,
            "cancellation": False,
            "automatic_retry": False,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "run": {
            "profile": plan.profile,
            "planned_cycles": settings.cycles,
            "planned_commands": settings.cycles * 4,
            "interval_seconds": max(settings.interval_seconds, 1.0 / plan.rate_limit_per_second),
            "path_order": "cycle 1 native_direct then gateway_full; subsequent cycles alternate first path",
        },
        "stage_contract": {
            "native_direct": ["C0_to_C4", "C4_to_C5", "C0_to_C5"],
            "gateway_full": ["C0_to_C2", "C2_to_C4", "C4_to_C5", "C5_to_C6", "C6_to_C7"],
            "not_applicable_for_native_direct": ["C0_to_C2", "C2_to_C4", "C5_to_C6", "C6_to_C7"],
        },
    }


def _execute_same_session(settings: GatewayAbSettings, plan: OrderCancelPlan) -> dict[str, Any]:
    """Run direct and gateway branches without creating a second EMT session."""

    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    trace = GatewayTraceCollector()
    ingress = _AbCallbackIngress()
    session: EmtSession | None = None
    command_journal: FileJournal | None = None
    event_journal: FileJournal | None = None
    drainer: _CallbackDrainer | None = None
    direct_timings: list[DirectTiming] = []
    gateway_records: list[tuple[int, str, TraceRecord | None]] = []
    outcomes: list[dict[str, str | int | None]] = []
    allocated_client_ids: set[int] = set()
    native_client_ids: dict[str, int] = {}
    completed_cycles = 0
    last_command_started_at: float | None = None
    interval = max(settings.interval_seconds, 1.0 / plan.rate_limit_per_second)
    try:
        loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
        account = getattr(settings.accounts, settings.profile)
        session = EmtSession.from_loaded_sdk(
            loaded,
            callback_ingress=ingress,
            settings=SessionSettings(
                host=settings.accounts.host,
                port=settings.accounts.port,
                account=account.account,
                password=account.password,
                client_id={"ordinary": 101, "credit": 102, "option": 103}[settings.profile],
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile=settings.profile,
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "gateway-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "gateway-events.emtj")

        def make_gateway_order_request(command_id: str, client_order_id: str) -> dict[str, Any]:
            del command_id
            try:
                native_client_id = native_client_ids[client_order_id]
            except KeyError as exc:
                raise GatewayAbError("gateway order request has no owned native client id") from exc
            return {**plan.order_request, "order_client_id": native_client_id}

        gateway = OrderGateway(
            native=EmtOrderAdapter(
                session.api,
                session_id=session.session_id,
                order_request_factory=make_gateway_order_request,
            ),
            command_journal=command_journal,
            event_journal=event_journal,
            trace=trace,
            callback_capacity=1024,
            require_native_fastspi=settings.require_native_fastspi,
        )
        ingress.attach(gateway.callbacks)
        drainer = _CallbackDrainer(gateway.callbacks)
        drainer.start()

        for cycle in range(1, settings.cycles + 1):
            branches = (
                ("native_direct", "gateway_full")
                if cycle % 2
                else ("gateway_full", "native_direct")
            )
            cycle_ok = True
            for branch in branches:
                if branch == "native_direct":
                    direct, branch_outcomes, last_command_started_at = _run_direct_cycle(
                        api=session.api,
                        session_id=session.session_id,
                        probe=ingress.direct,
                        plan=plan,
                        cycle=cycle,
                        allocated_client_ids=allocated_client_ids,
                        last_command_started_at=last_command_started_at,
                        interval_seconds=interval,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    direct_timings.extend(direct)
                    outcomes.extend(branch_outcomes)
                    cycle_ok = all(item.outcome == "ok" for item in direct)
                else:
                    records, branch_outcomes, last_command_started_at = _run_gateway_cycle(
                        gateway=gateway,
                        trace=trace,
                        drainer=drainer,
                        plan=plan,
                        cycle=cycle,
                        profile=settings.profile,
                        allocated_client_ids=allocated_client_ids,
                        native_client_ids=native_client_ids,
                        last_command_started_at=last_command_started_at,
                        interval_seconds=interval,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    gateway_records.extend(records)
                    outcomes.extend(branch_outcomes)
                    cycle_ok = all(
                        record is not None and record.outcome == "ok" for _, _, record in records
                    )
                if not cycle_ok:
                    break
            if not cycle_ok:
                break
            completed_cycles = cycle

        return _comparison_report(
            settings=settings,
            snapshot=snapshot,
            interval=interval,
            completed_cycles=completed_cycles,
            direct_timings=direct_timings,
            gateway_records=gateway_records,
            outcomes=outcomes,
            trace=trace,
        )
    finally:
        try:
            if drainer is not None:
                drainer.close()
        finally:
            try:
                if session is not None:
                    session.close()
            finally:
                for journal in (event_journal, command_journal):
                    if journal is not None:
                        journal.close()


def _run_direct_cycle(
    *,
    api: Any,
    session_id: int,
    probe: _DirectCallbackProbe,
    plan: OrderCancelPlan,
    cycle: int,
    allocated_client_ids: set[int],
    last_command_started_at: float | None,
    interval_seconds: float,
    timeout_seconds: float,
) -> tuple[list[DirectTiming], list[dict[str, str | int | None]], float]:
    client_id = int(_new_native_client_id(allocated_client_ids))
    allocated_client_ids.add(client_id)
    request = {**plan.order_request, "order_client_id": client_id}
    timings: list[DirectTiming] = []
    outcomes: list[dict[str, str | int | None]] = []

    last_command_started_at = _wait_for_command_slot(last_command_started_at, interval_seconds)
    order_timing, native_order_id = _measure_direct_command(
        api=api,
        session_id=session_id,
        probe=probe,
        cycle=cycle,
        interface="InsertOrder",
        request=request,
        order_emt_id=None,
        timeout_seconds=timeout_seconds,
    )
    timings.append(order_timing)
    outcomes.append(_direct_outcome(order_timing))
    if order_timing.outcome != "ok" or native_order_id is None:
        return timings, outcomes, last_command_started_at

    last_command_started_at = _wait_for_command_slot(last_command_started_at, interval_seconds)
    cancel_timing, _ = _measure_direct_command(
        api=api,
        session_id=session_id,
        probe=probe,
        cycle=cycle,
        interface="CancelOrder",
        request=None,
        order_emt_id=native_order_id,
        timeout_seconds=timeout_seconds,
    )
    timings.append(cancel_timing)
    outcomes.append(_direct_outcome(cancel_timing))
    return timings, outcomes, last_command_started_at


def _measure_direct_command(
    *,
    api: Any,
    session_id: int,
    probe: _DirectCallbackProbe,
    cycle: int,
    interface: str,
    request: dict[str, Any] | None,
    order_emt_id: int | None,
    timeout_seconds: float,
) -> tuple[DirectTiming, int | None]:
    pending = probe.begin(interface)
    native_id: int | None = None
    try:
        if interface == "InsertOrder":
            if request is None:
                raise GatewayAbError("direct InsertOrder requires a request")
            native_id = int(api.insertOrder(request, session_id))
        elif interface == "CancelOrder":
            if order_emt_id is None:
                raise GatewayAbError("direct CancelOrder requires a proved order_emt_id")
            native_id = int(api.cancelOrder(order_emt_id, session_id))
        else:
            raise GatewayAbError(f"unsupported direct command: {interface}")
    except Exception as exc:
        # No native call is retried.  The returned timing remains usable only
        # as a failure diagnostic, never as an A/B latency sample.
        probe.abort(pending)
        return DirectTiming(
            cycle,
            interface,
            "failed",
            type(exc).__name__,
            pending.started_ns,
            perf_counter_ns(),
            None,
        ), None

    returned_ns = perf_counter_ns()
    if native_id == 0:
        error_id = _native_error_id(api)
        probe.abort(pending)
        return DirectTiming(
            cycle,
            interface,
            "rejected",
            f"native_zero_return_{error_id}",
            pending.started_ns,
            returned_ns,
            None,
        ), None
    probe.mark_return(pending, native_id, returned_ns)
    callback_ns = probe.finish(pending, timeout_seconds)
    if callback_ns is None:
        return DirectTiming(
            cycle,
            interface,
            "timeout",
            "direct_command_callback_timeout",
            pending.started_ns,
            returned_ns,
            None,
        ), native_id
    if callback_ns < returned_ns:
        return DirectTiming(
            cycle,
            interface,
            "failed",
            "callback_arrived_before_native_return",
            pending.started_ns,
            returned_ns,
            callback_ns,
        ), native_id
    return DirectTiming(
        cycle, interface, "ok", None, pending.started_ns, returned_ns, callback_ns
    ), native_id


def _native_error_id(api: Any) -> int | str:
    try:
        error = api.getApiLastError()
    except Exception:
        return "unavailable"
    if isinstance(error, dict) and isinstance(error.get("error_id"), int):
        return int(error["error_id"])
    return "unavailable"


def _direct_outcome(timing: DirectTiming) -> dict[str, str | int | None]:
    return {
        "cycle": timing.cycle,
        "path": "native_direct",
        "api": timing.interface,
        "outcome": timing.outcome,
        "reason": timing.reason,
    }


def _run_gateway_cycle(
    *,
    gateway: OrderGateway,
    trace: GatewayTraceCollector,
    drainer: _CallbackDrainer,
    plan: OrderCancelPlan,
    cycle: int,
    profile: str,
    allocated_client_ids: set[int],
    native_client_ids: dict[str, int],
    last_command_started_at: float | None,
    interval_seconds: float,
    timeout_seconds: float,
) -> tuple[list[tuple[int, str, TraceRecord | None]], list[dict[str, str | int | None]], float]:
    native_client_id = int(_new_native_client_id(allocated_client_ids))
    allocated_client_ids.add(native_client_id)
    client_order_id = f"ab-gateway-order-{native_client_id}"
    native_client_ids[client_order_id] = native_client_id
    canonical_order = json.dumps(
        plan.order_request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    payload_hash = sha256(f"{canonical_order}:{native_client_id}".encode("utf-8")).hexdigest()
    records: list[tuple[int, str, TraceRecord | None]] = []
    outcomes: list[dict[str, str | int | None]] = []

    last_command_started_at = _wait_for_command_slot(last_command_started_at, interval_seconds)
    submitted = gateway.submit_order(
        OrderIntent(
            client_order_id=client_order_id,
            payload_hash=payload_hash,
            profile=profile,
            workload="steady",
        )
    )
    order_record = _await_gateway_command(submitted, trace, drainer, timeout_seconds)
    records.append((cycle, "InsertOrder", order_record))
    outcomes.append(_gateway_outcome(cycle, "InsertOrder", submitted, order_record))
    if (
        submitted.state is not CommandState.NATIVE_ACCEPTED
        or order_record is None
        or order_record.outcome != "ok"
    ):
        return records, outcomes, last_command_started_at

    last_command_started_at = _wait_for_command_slot(last_command_started_at, interval_seconds)
    cancelled = gateway.cancel_order(
        CancelIntent(
            client_cancel_id=f"ab-gateway-cancel-{native_client_id}",
            target_client_order_id=client_order_id,
            profile=profile,
            workload="steady",
        )
    )
    cancel_record = _await_gateway_command(cancelled, trace, drainer, timeout_seconds)
    records.append((cycle, "CancelOrder", cancel_record))
    outcomes.append(_gateway_outcome(cycle, "CancelOrder", cancelled, cancel_record))
    return records, outcomes, last_command_started_at


def _await_gateway_command(
    result: CommandResult,
    trace: GatewayTraceCollector,
    drainer: _CallbackDrainer,
    timeout_seconds: float,
) -> TraceRecord | None:
    if result.state is CommandState.NATIVE_ACCEPTED:
        drainer.wait_for_command(trace, result.correlation_id, timeout_seconds)
    return _trace_record(trace, result.correlation_id)


def _gateway_outcome(
    cycle: int,
    interface: str,
    result: CommandResult,
    record: TraceRecord | None,
) -> dict[str, str | int | None]:
    return {
        "cycle": cycle,
        "path": "gateway_full",
        "api": interface,
        "gateway_state": result.state.value,
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "native_error_code": result.native_error_code,
    }


_GATEWAY_SEGMENTS = (
    ("C0", "C2"),
    ("C2", "C4"),
    ("C4", "C5"),
    ("C5", "C6"),
    ("C6", "C7"),
)
_DIRECT_SEGMENTS = (("C0", "C4"), ("C4", "C5"), ("C0", "C5"))


def _comparison_report(
    *,
    settings: GatewayAbSettings,
    snapshot: Any,
    interval: float,
    completed_cycles: int,
    direct_timings: list[DirectTiming],
    gateway_records: list[tuple[int, str, TraceRecord | None]],
    outcomes: list[dict[str, str | int | None]],
    trace: GatewayTraceCollector,
) -> dict[str, Any]:
    direct = _summarize_direct(direct_timings)
    gateway = _summarize_gateway(gateway_records)
    report: dict[str, Any] = {
        "schema_version": 1,
        "driver": "gateway-ab-order-c0-c7",
        "executed": True,
        "measurement_scope": (
            "same host, same account and one active EMT session; native direct and durable gateway "
            "order/cancel cycles alternate at a bounded low rate; not a production SLA or throughput claim"
        ),
        "safety": {
            "mode": "bounded-sequential-simulated-order-ab",
            "order_entry": True,
            "cancellation": True,
            "automatic_retry": False,
            "native_fastspi_required": settings.require_native_fastspi,
            "credentials_in_report": False,
            "business_payload_in_report": False,
            "direct_path_durable": False,
            "manual_reconciliation_required_after_direct_path_failure": True,
        },
        "run": {
            "profile": settings.profile,
            "requested_cycles": settings.cycles,
            "completed_cycles": completed_cycles,
            "stopped_early": completed_cycles != settings.cycles,
            "interval_seconds": interval,
            "path_order": "cycle 1 native_direct then gateway_full; subsequent cycles alternate first path",
            "api_version": snapshot.api_version,
            "trading_day": snapshot.trading_day,
            "outcomes": outcomes,
        },
        "native_direct": {
            "stage_availability": {
                "measured": [f"{start}_to_{end}" for start, end in _DIRECT_SEGMENTS],
                "not_applicable": ["C0_to_C2", "C2_to_C4", "C5_to_C6", "C6_to_C7"],
            },
            "operations": direct,
        },
        "gateway_full": {
            "stage_availability": {
                "measured": [f"{start}_to_{end}" for start, end in _GATEWAY_SEGMENTS]
            },
            "operations": gateway,
        },
        "observed_ab_delta": _summarize_ab_delta(direct_timings, gateway_records),
        "interpretation": {
            "gateway_C0_to_C2": "gateway admission plus durable COMMAND_PREPARED; absent by design in native direct",
            "gateway_C2_to_C4": "durable CALL_STARTED plus native SDK call/return",
            "C4_to_C5": "native return to first matching EMT callback arrival; includes EMT/network/counter time",
            "gateway_C5_to_C6": "fastspi dequeue and durable event-journal work",
            "gateway_C6_to_C7": "strategy-visible delivery acknowledgement",
            "delta": "sequential A/B observation only; EMT response jitter prevents treating the difference as a causal component attribution",
        },
    }
    if settings.trace_output is not None:
        # Gateway collector events are already payload-free.  Direct branch
        # timings remain in this report but are not fabricated as C0--C7
        # gateway traces, so they cannot accidentally qualify a P0 gate.
        events = trace.export_events()
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _summarize_direct(timings: list[DirectTiming]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for interface in ("InsertOrder", "CancelOrder"):
        entries = [item for item in timings if item.interface == interface]
        successful = [item for item in entries if item.outcome == "ok"]
        result[interface] = {
            "attempts": len(entries),
            "successes": len(successful),
            "failures": len(entries) - len(successful),
            "latency": {
                f"{start}_to_{end}": _histogram_summary(
                    item.latency(start, end) for item in successful
                )
                for start, end in _DIRECT_SEGMENTS
            },
        }
    return result


def _summarize_gateway(
    records: list[tuple[int, str, TraceRecord | None]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for interface in ("InsertOrder", "CancelOrder"):
        entries = [record for _, name, record in records if name == interface]
        successful = [record for record in entries if record is not None and record.outcome == "ok"]
        assert all(record is not None for record in successful)
        result[interface] = {
            "attempts": len(entries),
            "successes": len(successful),
            "failures": len(entries) - len(successful),
            "latency": {
                f"{start}_to_{end}": _histogram_summary(
                    _record_latency(record, start, end)
                    for record in successful
                    if record is not None
                )
                for start, end in _GATEWAY_SEGMENTS
            },
        }
    return result


def _summarize_ab_delta(
    direct_timings: list[DirectTiming],
    gateway_records: list[tuple[int, str, TraceRecord | None]],
) -> dict[str, dict[str, Any]]:
    """Compare matched cycle/interface observations without hiding negatives."""

    direct_by_key = {
        (item.cycle, item.interface): item for item in direct_timings if item.outcome == "ok"
    }
    gateway_by_key = {
        (cycle, interface): record
        for cycle, interface, record in gateway_records
        if record is not None and record.outcome == "ok"
    }
    result: dict[str, dict[str, Any]] = {}
    for interface in ("InsertOrder", "CancelOrder"):
        c0_to_c4: list[int] = []
        c0_to_c5: list[int] = []
        for cycle in range(1, 11):
            direct = direct_by_key.get((cycle, interface))
            gateway = gateway_by_key.get((cycle, interface))
            if direct is None or gateway is None:
                continue
            direct_c0_c4 = direct.latency("C0", "C4")
            direct_c0_c5 = direct.latency("C0", "C5")
            gateway_c0_c4 = _record_latency(gateway, "C0", "C4")
            gateway_c0_c5 = _record_latency(gateway, "C0", "C5")
            if direct_c0_c4 is not None and gateway_c0_c4 is not None:
                c0_to_c4.append(gateway_c0_c4 - direct_c0_c4)
            if direct_c0_c5 is not None and gateway_c0_c5 is not None:
                c0_to_c5.append(gateway_c0_c5 - direct_c0_c5)
        result[interface] = {
            "matched_cycles": max(len(c0_to_c4), len(c0_to_c5)),
            "gateway_minus_native_direct": {
                "C0_to_C4": _signed_summary(c0_to_c4),
                "C0_to_C5": _signed_summary(c0_to_c5),
            },
        }
    return result


def _record_latency(record: TraceRecord, start: str, end: str) -> int | None:
    started = record.timestamps_ns.get(start)
    ended = record.timestamps_ns.get(end)
    if started is None or ended is None or ended < started:
        return None
    return ended - started


def _histogram_summary(values: Any) -> dict[str, int | None]:
    histogram = ExactHistogram()
    for value in values:
        if value is not None:
            histogram.record(value)
    if not histogram.count:
        return {
            "count": 0,
            "min_ns": None,
            "p50_ns": None,
            "p95_ns": None,
            "p99_ns": None,
            "p999_ns": None,
            "max_ns": None,
        }
    return histogram.summary()


def _signed_summary(values: list[int]) -> dict[str, int | None]:
    """Percentiles for A/B differences, which may legitimately be negative."""

    if not values:
        return {
            "count": 0,
            "min_ns": None,
            "p50_ns": None,
            "p95_ns": None,
            "p99_ns": None,
            "p999_ns": None,
            "max_ns": None,
        }
    ordered = sorted(values)

    def percentile(quantile: float) -> int:
        return ordered[ceil(quantile * len(ordered)) - 1]

    return {
        "count": len(ordered),
        "min_ns": ordered[0],
        "p50_ns": percentile(0.50),
        "p95_ns": percentile(0.95),
        "p99_ns": percentile(0.99),
        "p999_ns": percentile(0.999),
        "max_ns": ordered[-1],
    }
