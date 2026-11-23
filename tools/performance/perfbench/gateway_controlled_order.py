"""Low-rate, real simulated order experiments through the durable gateway.

This driver is intentionally not a throughput benchmark.  It executes the
four locally approved 000010 scenarios serially, at no more than one native
command per second.  Every accepted order receives exactly one cancel attempt:
that is the measurement in cancellation scenarios and the cleanup path in fill
scenarios, so a residual test order is never knowingly left resting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock
from time import monotonic
from typing import Any, Callable

from emt_gateway.facade import EmtTraderFacade
from emt_gateway.gateway import (
    CallbackRecord,
    CancelIntent,
    CommandState,
    OrderGateway,
    OrderIntent,
)
from emt_gateway.journal import FileJournal, GroupCommitJournal
from emt_gateway.native_api import EmtOrderAdapter
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .controlled_order_fixture import (
    ControlledOrderFixture,
    ControlledOrderScenario,
    load_controlled_order_fixture,
)
from .gateway_mutation import (
    _CallbackDrainer,
    _command_outcome,
    _MutationCallbackIngress,
    _new_native_client_id,
    _query_callbacks,
    _reconcile_order,
    _trace_is_not_ok,
    _wait_for_command_slot,
    _write_payload_free_trace,
)
from .gateway_report import build_gateway_report
from .histogram import ExactHistogram
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector, TraceRecord


class ControlledOrderError(RuntimeError):
    """The controlled order experiment cannot safely continue."""


_TARGET_SEGMENTS = (
    ("C0", "C1"),
    ("C1", "C2"),
    ("C2", "C3"),
    ("C3", "C4"),
    ("C0", "C2"),
    ("C2", "C4"),
    ("C4", "C5"),
    ("C5", "C6"),
    ("C6", "C7"),
)
_ONE_HZ_LOCAL_SEGMENTS = (
    ("C0", "C1"),
    ("C1", "C2"),
    ("C2", "C3"),
    ("C3", "C4"),
    ("C5", "C6"),
    ("C6", "C7"),
)


@dataclass(frozen=True)
class ControlledOrderSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    scenario_ids: tuple[str, ...] = ()
    callback_timeout_seconds: float = 8.0
    trace_output: Path | None = None
    local_latency_gate_report: Path | None = None
    skip_local_latency_gate: bool = False
    execute_simulated: bool = False
    require_native_fastspi: bool = True
    verify_cancel_error: bool = False
    # Acceptance-only: hold the vendor wrapper until the CLI has persisted the
    # payload-free report and exits with os._exit().  The default path keeps the
    # normal session.close() lifecycle unchanged.
    retain_native_reference: Callable[[object], None] | None = field(
        default=None, repr=False, compare=False
    )

    def validate(self) -> None:
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise ControlledOrderError("controlled-order callback_timeout_seconds must be in 3..30")


class _OwnedTradeObserver:
    """Observe own fill and deliberately-negative cancel callbacks without payloads."""

    def __init__(self) -> None:
        self._owned: set[int] = set()
        self._traded: set[int] = set()
        # EMT cancel-error callbacks carry both the original order identifier
        # and the native id allocated to the *cancel* operation.  Retain only
        # those correlation scalars so a delayed callback for an earlier
        # cancel can never satisfy the deliberate duplicate-cancel probe.
        self._cancel_error_pairs: set[tuple[int, int]] = set()
        self._changed = Event()
        self._lock = Lock()

    def own(self, order_emt_id: int) -> None:
        with self._lock:
            self._owned.add(order_emt_id)
            if order_emt_id in self._traded:
                self._changed.set()

    def seen(self, order_emt_id: int) -> bool:
        with self._lock:
            return order_emt_id in self._traded

    def wait_for_trade(self, order_emt_id: int, timeout_seconds: float) -> bool:
        deadline = monotonic() + timeout_seconds
        while True:
            if self.seen(order_emt_id):
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            self._changed.wait(min(remaining, 0.05))
            self._changed.clear()

    def wait_for_cancel_error(
        self,
        order_emt_id: int,
        cancel_emt_id: int,
        timeout_seconds: float,
    ) -> bool:
        deadline = monotonic() + timeout_seconds
        while True:
            with self._lock:
                if (order_emt_id, cancel_emt_id) in self._cancel_error_pairs:
                    return True
                self._changed.clear()
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            self._changed.wait(min(remaining, 0.05))

    def on_enqueued(self, record: CallbackRecord) -> None:
        del record

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        del record, durable_ns

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        del acknowledged_ns
        with self._lock:
            if record.callback_name == "OnTradeEvent" and record.order_emt_id is not None:
                self._traded.add(record.order_emt_id)
            elif (
                record.callback_name == "OnCancelOrderError"
                and record.order_emt_id is not None
                and record.operation_native_id is not None
            ):
                self._cancel_error_pairs.add((record.order_emt_id, record.operation_native_id))
            else:
                return
            if record.order_emt_id is not None and record.order_emt_id in self._owned:
                self._changed.set()


def run_controlled_order_experiment(settings: ControlledOrderSettings) -> dict[str, Any]:
    """Preflight or execute the fixture's bounded simulated scenarios."""

    settings.validate()
    fixture = load_controlled_order_fixture(settings.fixture)
    scenarios = fixture.selected(settings.scenario_ids or None)
    if settings.verify_cancel_error and (
        len(scenarios) != 1 or scenarios[0].objective != "cancel" or scenarios[0].cycles != 1
    ):
        raise ControlledOrderError(
            "cancel-error verification requires exactly one one-cycle cancel scenario"
        )
    if not settings.execute_simulated:
        return _preflight_report(fixture, scenarios)
    if not fixture.safety.armed:
        raise ControlledOrderError(
            "controlled-order fixture is disarmed; preflight completed but no order may be sent"
        )
    return _execute(settings, fixture, scenarios)


def _preflight_report(
    fixture: ControlledOrderFixture,
    scenarios: tuple[ControlledOrderScenario, ...],
) -> dict[str, Any]:
    submissions = sum(item.cycles for item in scenarios)
    return {
        "schema_version": 1,
        "driver": "gateway-controlled-order-c0-c7",
        "executed": False,
        "safety": {
            "mode": "preflight-only",
            "fixture_armed": fixture.safety.armed,
            "sdk_loaded": False,
            "login": False,
            "order_entry": False,
            "automatic_retry": False,
            "credentials_in_report": False,
            "business_payload_in_report": False,
        },
        "run": {
            "scenario_ids": [item.scenario_id for item in scenarios],
            "planned_order_submissions": submissions,
            "maximum_native_commands": submissions * 2,
            "minimum_command_interval_seconds": fixture.safety.min_command_interval_seconds,
            "cancel_timeout_seconds": fixture.safety.cancel_timeout_seconds,
            "account_profile": fixture.safety.account_profile,
            "allowed_ticker_count": len(fixture.safety.allowed_tickers),
            "maximum_order_quantity": fixture.safety.max_order_quantity,
            "random_seed": fixture.safety.random_seed,
            "fill_evidence": "OnTradeEvent; every fill order has one cleanup-cancel attempt",
            "cancel_evidence": "CancelOrder plus QueryOrderByEMTID; unexpected trade stops the batch",
        },
    }


def _execute(
    settings: ControlledOrderSettings,
    fixture: ControlledOrderFixture,
    scenarios: tuple[ControlledOrderScenario, ...],
) -> dict[str, Any]:
    local_latency_gate = _local_latency_gate_evidence(settings)
    trace = GatewayTraceCollector()
    ingress = DeferredCallbackIngress()
    registry = QueryRegistry(trace=trace)
    session: EmtSession | None = None
    command_file: FileJournal | None = None
    command_journal: GroupCommitJournal | None = None
    command_events: FileJournal | None = None
    query_events: FileJournal | None = None
    drainer: _CallbackDrainer | None = None
    outcomes: list[dict[str, Any]] = []
    completed: dict[str, int] = {scenario.scenario_id: 0 for scenario in scenarios}
    stop_reason: str | None = None
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    try:
        loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
        account = settings.accounts.ordinary
        session = EmtSession.from_loaded_sdk(
            loaded,
            callback_ingress=ingress,
            settings=SessionSettings(
                host=settings.accounts.host,
                port=settings.accounts.port,
                account=account.account,
                password=account.password,
                # Keep the controlled order path on the same configured EMT
                # client identity as the verified ordinary-account login probe.
                client_id=94,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="ordinary",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_file = FileJournal(settings.runtime_dir / "controlled-order-commands.emtj")
        command_journal = GroupCommitJournal(
            command_file,
            max_batch_size=128,
            max_wait_seconds=0.0001,
            direct_when_idle=True,
        )
        command_events = FileJournal(settings.runtime_dir / "controlled-order-events.emtj")
        query_events = FileJournal(settings.runtime_dir / "controlled-order-query-events.emtj")
        native_requests: dict[str, dict[str, Any]] = {}

        def make_native_order_request(command_id: str, client_order_id: str) -> dict[str, Any]:
            del command_id
            try:
                return native_requests[client_order_id]
            except KeyError as exc:
                raise ControlledOrderError(
                    "controlled-order request lacks an owned client id"
                ) from exc

        observer = _OwnedTradeObserver()
        gateway = OrderGateway(
            native=EmtOrderAdapter(
                session.api,
                session_id=session.session_id,
                order_request_factory=make_native_order_request,
            ),
            command_journal=command_journal,
            event_journal=command_events,
            trace=trace,
            callback_capacity=1024,
            callback_lifecycle_observer=observer,
            require_native_fastspi=settings.require_native_fastspi,
        )
        query_callbacks = _query_callbacks(
            query_events, trace, registry, settings.require_native_fastspi
        )
        ingress.attach(
            _MutationCallbackIngress(commands=gateway.callbacks, queries=query_callbacks)
        )
        drainer = _CallbackDrainer(gateway.callbacks)
        drainer.start()
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )
        allocated_client_ids: set[int] = set()
        last_started_at: float | None = None

        for scenario in scenarios:
            for cycle in range(1, scenario.cycles + 1):
                client_id = int(_new_native_client_id(allocated_client_ids))
                allocated_client_ids.add(client_id)
                client_order_id = f"perf-controlled-{client_id}"
                request = {
                    **scenario.order_request(seed=fixture.safety.random_seed, cycle=cycle),
                    "order_client_id": client_id,
                }
                native_requests[client_order_id] = request
                canonical_request = json.dumps(
                    request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
                )
                payload_hash = sha256(canonical_request.encode("utf-8")).hexdigest()
                last_started_at = _wait_for_command_slot(
                    last_started_at,
                    fixture.safety.min_command_interval_seconds,
                )
                submitted = gateway.submit_order(
                    OrderIntent(
                        client_order_id=client_order_id,
                        payload_hash=payload_hash,
                        profile="ordinary",
                        workload="steady",
                    )
                )
                insert = _command_outcome(
                    cycle,
                    "InsertOrder",
                    submitted,
                    trace,
                    drainer,
                    settings.callback_timeout_seconds,
                )
                outcome: dict[str, Any] = {
                    "scenario_id": scenario.scenario_id,
                    "cycle": cycle,
                    "objective": scenario.objective,
                    "insert": insert,
                    "trade_callback_seen": False,
                    "cancel": None,
                    "cancel_error": None,
                    "reconciliation": None,
                }
                outcomes.append(outcome)
                if submitted.state is not CommandState.NATIVE_ACCEPTED or _trace_is_not_ok(
                    trace, submitted.correlation_id
                ):
                    stop_reason = "insert_failed_or_unverified"
                    break
                if submitted.native_id is None:
                    raise ControlledOrderError(
                        "accepted controlled-order command lacks EMT order id"
                    )
                observer.own(submitted.native_id)

                if scenario.objective == "fill":
                    outcome["trade_callback_seen"] = observer.wait_for_trade(
                        submitted.native_id,
                        settings.callback_timeout_seconds,
                    )
                else:
                    # The enforced command gap doubles as a short observation
                    # window.  A trade in a cancellation case is a safety
                    # failure, but we still issue the owned-order cleanup.
                    last_started_at = _wait_for_command_slot(
                        last_started_at,
                        fixture.safety.min_command_interval_seconds,
                    )
                    outcome["trade_callback_seen"] = observer.seen(submitted.native_id)

                if scenario.objective == "fill":
                    last_started_at = _wait_for_command_slot(
                        last_started_at,
                        fixture.safety.min_command_interval_seconds,
                    )
                cancelled = gateway.cancel_order(
                    CancelIntent(
                        client_cancel_id=f"perf-controlled-cancel-{client_id}",
                        target_client_order_id=client_order_id,
                        profile="ordinary",
                        workload="steady",
                    )
                )
                cancel = _command_outcome(
                    cycle,
                    "CancelOrder",
                    cancelled,
                    trace,
                    drainer,
                    fixture.safety.cancel_timeout_seconds,
                )
                outcome["cancel"] = cancel
                if cancelled.state is CommandState.NATIVE_ACCEPTED and not _trace_is_not_ok(
                    trace, cancelled.correlation_id
                ):
                    outcome["reconciliation"] = _reconcile_order(
                        cycle=cycle,
                        facade=facade,
                        callbacks=query_callbacks,
                        registry=registry,
                        trace=trace,
                        profile="ordinary",
                        order_emt_id=submitted.native_id,
                        timeout_seconds=fixture.safety.cancel_timeout_seconds,
                    )
                else:
                    stop_reason = "cancel_failed_or_ambiguous_manual_reconciliation_required"
                    break
                if outcome["reconciliation"]["outcome"] != "ok":
                    stop_reason = "order_reconciliation_failed"
                    break
                if scenario.objective == "fill" and not outcome["trade_callback_seen"]:
                    stop_reason = "fill_trade_callback_not_observed"
                    break
                if scenario.objective == "cancel" and (
                    bool(outcome["trade_callback_seen"]) or observer.seen(submitted.native_id)
                ):
                    stop_reason = "unexpected_trade_observed_during_cancel_scenario"
                    break
                if settings.verify_cancel_error:
                    # The first cancel has reached a terminal authoritative
                    # query result.  A second request for this *own* terminal
                    # order cannot alter venue state and is the smallest real
                    # fixture that can require EMT's OnCancelOrderError path.
                    last_started_at = _wait_for_command_slot(
                        last_started_at,
                        fixture.safety.min_command_interval_seconds,
                    )
                    repeat = gateway.cancel_order(
                        CancelIntent(
                            client_cancel_id=f"perf-controlled-cancel-error-{client_id}",
                            target_client_order_id=client_order_id,
                            profile="ordinary",
                            workload="steady",
                        )
                    )
                    repeat_outcome = _command_outcome(
                        cycle,
                        "CancelOrder",
                        repeat,
                        trace,
                        drainer,
                        fixture.safety.cancel_timeout_seconds,
                    )
                    cancel_error_seen = (
                        repeat.native_id is not None
                        and observer.wait_for_cancel_error(
                            submitted.native_id,
                            repeat.native_id,
                            fixture.safety.cancel_timeout_seconds,
                        )
                    )
                    outcome["cancel_error"] = {
                        "command_outcome": repeat_outcome["outcome"],
                        "callback_seen": cancel_error_seen,
                    }
                    if (
                        repeat.state is not CommandState.NATIVE_ACCEPTED
                        or _trace_is_not_ok(trace, repeat.correlation_id)
                        or not cancel_error_seen
                    ):
                        stop_reason = "cancel_error_callback_not_observed"
                        break
                completed[scenario.scenario_id] = cycle
            if stop_reason is not None:
                break
        return _report(
            trace=trace,
            snapshot=snapshot,
            fixture=fixture,
            scenarios=scenarios,
            outcomes=outcomes,
            completed=completed,
            stop_reason=stop_reason,
            settings=settings,
            local_latency_gate=local_latency_gate,
        )
    finally:
        try:
            if drainer is not None:
                drainer.close()
        finally:
            try:
                if session is not None:
                    if settings.retain_native_reference is not None:
                        settings.retain_native_reference(session)
                    else:
                        session.close()
            finally:
                for journal in (query_events, command_events, command_journal, command_file):
                    if journal is not None:
                        journal.close()


def _report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    fixture: ControlledOrderFixture,
    scenarios: tuple[ControlledOrderScenario, ...],
    outcomes: list[dict[str, Any]],
    completed: dict[str, int],
    stop_reason: str | None,
    settings: ControlledOrderSettings,
    local_latency_gate: dict[str, str | bool | None],
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-controlled-order-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "serial simulated 000010 order experiment; measures gateway C0-C7 evidence, "
        "not a throughput benchmark or production SLA"
    )
    report["safety"] = {
        "environment": "simulated",
        "maximum_order_submissions": fixture.safety.max_total_submissions,
        "maximum_native_commands": fixture.safety.max_total_commands,
        "minimum_command_interval_seconds": fixture.safety.min_command_interval_seconds,
        "cancel_timeout_seconds": fixture.safety.cancel_timeout_seconds,
        "account_profile": fixture.safety.account_profile,
        "allowed_ticker_count": len(fixture.safety.allowed_tickers),
        "maximum_order_quantity": fixture.safety.max_order_quantity,
        "automatic_retry": False,
        "cleanup_cancel_after_each_submission": True,
        "cancel_error_verification": settings.verify_cancel_error,
        "stop_on_any_failure": True,
        "native_fastspi_required": settings.require_native_fastspi,
        "local_one_hz_gate": local_latency_gate,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "scenario_ids": [scenario.scenario_id for scenario in scenarios],
        "random_seed": fixture.safety.random_seed,
        "requested_cycles": {scenario.scenario_id: scenario.cycles for scenario in scenarios},
        "completed_cycles": completed,
        "stopped_early": stop_reason is not None,
        "stop_reason": stop_reason,
        "commands": outcomes,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
        "percentile_interpretation": "10 samples per scenario: p99 and p99.9 are sample maxima, not an SLA estimate",
    }
    report["targeted_command_latency"] = _targeted_command_latency(trace.records())
    report["one_signal_per_second_local_gate"] = _one_signal_per_second_local_gate(
        trace.records(),
        fixture=fixture,
    )
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _targeted_command_latency(records: tuple[TraceRecord, ...]) -> dict[str, dict[str, Any]]:
    """Summarize the five A/B segments without inserting zero-valued stages."""

    result: dict[str, dict[str, Any]] = {}
    for interface in ("InsertOrder", "CancelOrder"):
        selected = [
            record
            for record in records
            if record.kind == "api"
            and record.interface == interface
            and record.profile == "ordinary"
        ]
        successful = [record for record in selected if record.outcome == "ok"]
        result[interface] = {
            "attempts": len(selected),
            "successes": len(successful),
            "failures": len(selected) - len(successful),
            "latency": {
                f"{start}_to_{end}": _histogram_or_empty(
                    record.timestamps_ns[end] - record.timestamps_ns[start]
                    for record in successful
                    if start in record.timestamps_ns and end in record.timestamps_ns
                )
                for start, end in _TARGET_SEGMENTS
            },
        }
    return result


def _histogram_or_empty(samples: Any) -> dict[str, int | None]:
    histogram = ExactHistogram()
    for sample in samples:
        histogram.record(sample)
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


def _one_signal_per_second_local_gate(
    records: tuple[TraceRecord, ...],
    *,
    fixture: ControlledOrderFixture,
) -> dict[str, Any]:
    """Evaluate only local C0--C4 and C5--C7 segments at the fixture's 1 Hz rate."""

    successful = [
        record
        for record in records
        if record.kind == "api"
        and record.interface in {"InsertOrder", "CancelOrder"}
        and record.profile == "ordinary"
        and record.outcome == "ok"
    ]
    measurements = {
        f"{start}_to_{end}": _histogram_or_empty(
            record.timestamps_ns[end] - record.timestamps_ns[start]
            for record in successful
            if start in record.timestamps_ns and end in record.timestamps_ns
        )
        for start, end in _ONE_HZ_LOCAL_SEGMENTS
    }
    budget_ns = 1_000_000
    failed_paths = [
        name
        for name, summary in measurements.items()
        if summary["count"] == 0
        or not isinstance(summary["p99_ns"], int)
        or summary["p99_ns"] > budget_ns
    ]
    applicable = fixture.safety.min_command_interval_seconds >= 1.0
    return {
        "applicable": applicable,
        "budget_ms": 1.0,
        "excluded_path": "C4_to_C5_emt_callback_wait",
        "measurements": measurements,
        "passed": applicable and bool(successful) and not failed_paths,
        "failed_paths": failed_paths if applicable else [],
        "sample_caveat": "The bounded fixture yields an observational p99 (near the sample maximum), not an SLA.",
    }


def _local_latency_gate_evidence(settings: ControlledOrderSettings) -> dict[str, str | bool | None]:
    """Return explicit gate evidence before the SDK can be loaded.

    Default runs require the account-free 1 Hz report.  A human can bypass it
    only through the dedicated CLI flag; that exception is retained in the
    resulting real-order report rather than silently weakening the default.
    """

    if settings.skip_local_latency_gate:
        return {
            "mode": "explicitly_bypassed",
            "passed": None,
            "report": None,
        }
    path = settings.local_latency_gate_report

    if path is None:
        raise ControlledOrderError(
            "a passing --local-latency-gate-report is required before controlled order entry"
        )
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlledOrderError("cannot read local latency gate report") from exc
    if not isinstance(report, dict) or report.get("driver") != "gateway-local-signal-burst":
        raise ControlledOrderError("local latency gate report has the wrong driver")
    safety = report.get("safety")
    run = report.get("run")
    gate = report.get("one_signal_per_second_local_gate")
    if not isinstance(safety, dict) or safety.get("order_entry") is not False:
        raise ControlledOrderError("local latency gate report must be account-free")
    if (
        not isinstance(run, dict)
        or run.get("signals_per_second") != 1
        or run.get("arrival_pattern") != "uniform"
    ):
        raise ControlledOrderError(
            "local latency gate report must measure uniform one signal per second"
        )
    if (
        not isinstance(gate, dict)
        or gate.get("applicable") is not True
        or gate.get("passed") is not True
    ):
        raise ControlledOrderError(
            "local 1 Hz latency gate did not pass; controlled order entry is blocked"
        )
    return {
        "mode": "passed_required",
        "passed": True,
        "report": path.name,
    }
