"""Controlled real EMT order/cancel measurement through the durable gateway.

This is intentionally a narrow mutation driver. Each bounded cycle executes
one ordinary cash-equity, credit-account, or option-account *limit* order, one cancellation for
that returned order, and a callback-driven ``QueryOrderByEMTID`` reconciliation
after each step. Option orders are restricted to a single opening contract
and exist only to establish a simulated position for later option
mutation tests.
It is not a throughput test and it never enables fund, credit, option-combined
or batch commands. A local fixture must be complete, armed, and explicitly
paired with the command-line execution flag before any EMT request is sent.
"""

from __future__ import annotations

import json
import re
import secrets
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic, sleep
from typing import Any, Callable, Mapping

from emt_gateway.facade import EmtTraderFacade
from emt_gateway.gateway import (
    CallbackPipeline,
    CancelIntent,
    CommandResult,
    CommandState,
    OrderGateway,
    OrderIntent,
)
from emt_gateway.journal import FileJournal
from emt_gateway.native_api import EmtOrderAdapter
from emt_gateway.order_ledger import LedgerCallbackFact
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import CallbackIngressPort, DeferredCallbackIngress

from .gateway_readonly import _drain_until_complete
from .gateway_report import build_gateway_report
from .mutation_fixture import MutationFixtureEntry, load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


class GatewayMutationError(RuntimeError):
    """A controlled mutation run is unsafe, incomplete, or cannot proceed."""


@dataclass(frozen=True)
class GatewayMutationSettings:
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
    session_client_id: int | None = None
    fixed_native_order_client_id: int | None = None
    order_request_override: Mapping[str, Any] | None = field(
        default=None, repr=False, compare=False
    )
    on_cycle_completed: Callable[[str, int, int], None] | None = field(
        default=None, repr=False, compare=False
    )
    # Limited to a throw-away acceptance process.  Keeping the wrapper alive
    # until the report is written lets the CLI exit without entering a vendor
    # finalizer that is known to block on some Linux binding paths.
    retain_native_reference: Callable[[object], None] | None = field(
        default=None, repr=False, compare=False
    )

    def validate(self) -> None:
        if self.profile not in {"ordinary", "credit", "option"}:
            raise GatewayMutationError(
                "the bounded order/cancel fixture supports ordinary, credit or option simulated accounts only"
            )
        if not 1 <= self.cycles <= 10:
            raise GatewayMutationError(
                "mutation cycles must be in 1..10 for the controlled batch driver"
            )
        if self.callback_timeout_seconds <= 0:
            raise GatewayMutationError("callback_timeout_seconds must be positive")
        if self.interval_seconds < 1.0:
            raise GatewayMutationError("mutation interval_seconds must stay at or above one second")
        if self.session_client_id is not None and not 1 <= self.session_client_id <= 255:
            raise GatewayMutationError("custom EMT session client id must be in 1..255")
        if self.fixed_native_order_client_id is not None:
            if not 1 <= self.fixed_native_order_client_id <= 2**31 - 1:
                raise GatewayMutationError("fixed native order client id is invalid")
            if self.cycles != 1:
                raise GatewayMutationError(
                    "fixed native order client id requires exactly one controlled cycle"
                )
        if self.on_cycle_completed is not None and self.fixed_native_order_client_id is None:
            raise GatewayMutationError(
                "cycle completion observer requires a fixed native order client id"
            )


@dataclass(frozen=True)
class OrderCancelPlan:
    """The only mutation plan this first driver can execute."""

    order_request: dict[str, Any]
    rate_limit_per_second: int
    reconciliation_query: str
    profile: str


class _MutationCallbackIngress:
    """Route query callbacks separately from command callbacks without payloads."""

    def __init__(self, *, commands: CallbackIngressPort, queries: CallbackIngressPort) -> None:
        self._commands = commands
        self._queries = queries

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
        target = self._queries if callback_name.startswith("OnQuery") else self._commands
        metadata: dict[str, int | bool | None] = {
            "order_emt_id": order_emt_id,
            "operation_native_id": operation_native_id,
            "request_id": request_id,
            "is_last": is_last,
        }
        if callback_name.startswith("OnQuery"):
            if query_item_present is not None:
                metadata["query_item_present"] = query_item_present
            if query_reference is not None:
                metadata["query_reference"] = query_reference
        if ledger_fact is not None:
            metadata["ledger_fact"] = ledger_fact
        return target.receive(
            callback_name,
            profile=profile,
            workload=workload,
            **metadata,
        )


class _CallbackDrainer:
    """Continuously durable-ACK command callbacks during a mutation run.

    A mutation measurement must not leave a later order notification in the
    ring while a reconciliation query or rate limiter is running.  That would
    measure harness queue starvation as ``C0→C7`` callback latency.  This
    single-consumer worker keeps the P2 callback pipeline live throughout the
    session; command completion still remains correlation-specific.
    """

    def __init__(self, callbacks: CallbackPipeline) -> None:
        self._callbacks = callbacks
        self._stop = Event()
        self._thread: Thread | None = None
        self._failure: Exception | None = None
        self._failure_lock = Lock()

    def start(self) -> None:
        if self._thread is not None:
            raise GatewayMutationError("callback drainer is already started")
        self._thread = Thread(target=self._run, name="emt-mutation-callback-drainer", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                raise GatewayMutationError("callback drainer did not stop")

    def wait_for_command(
        self, trace: GatewayTraceCollector, correlation_id: str, timeout_seconds: float
    ) -> None:
        deadline = monotonic() + timeout_seconds
        while True:
            record = _trace_record(trace, correlation_id)
            if record is not None and record.outcome is not None:
                return
            if self.failure is not None:
                trace.complete(correlation_id, "failed", "command_callback_drainer_failure")
                return
            if self._callbacks.disarmed:
                trace.complete(correlation_id, "failed", "command_callback_pipeline_disarmed")
                return
            if monotonic() >= deadline:
                trace.complete(correlation_id, "timeout", "command_callback_timeout")
                return
            sleep(0.001)

    @property
    def failure(self) -> Exception | None:
        with self._failure_lock:
            return self._failure

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                deliveries = self._callbacks.normalize_many(256, collect_wait_seconds=0.001)
                if deliveries:
                    for delivery in deliveries:
                        self._callbacks.ack(delivery.delivery_id)
                    continue
            except Exception as exc:
                with self._failure_lock:
                    self._failure = exc
                return
            self._callbacks.wait_for_callbacks(0.050)


_ORDER_FIELDS = frozenset(
    {
        "ticker",
        "market",
        "side",
        "price_type",
        "price",
        "quantity",
        "business_type",
        "position_effect",
        "ep_id",
    }
)
_REQUIRED_ORDER_FIELDS = frozenset(
    {
        "ticker",
        "market",
        "side",
        "price_type",
        "price",
        "quantity",
        "business_type",
    }
)
_ORDINARY_TICKER = re.compile(r"[0-9]{6}\Z")
_OPTION_TICKER = re.compile(r"[0-9]{8}\Z")
_CREDIT_MARGIN_SIDES = frozenset({21, 22})


def select_order_cancel_plan(
    entries: tuple[MutationFixtureEntry, ...],
    *,
    profile: str = "ordinary",
) -> OrderCancelPlan:
    """Fail closed unless the fixture enables exactly one bounded order cycle."""

    by_api = {entry.api_name: entry for entry in entries}
    required = {"InsertOrder", "CancelOrder"}
    if set(by_api) != required or len(by_api) != len(entries):
        raise GatewayMutationError(
            "the initial mutation driver requires exactly enabled InsertOrder and CancelOrder cases"
        )
    order, cancel = by_api["InsertOrder"], by_api["CancelOrder"]
    if (
        order.reconciliation_query != "QueryOrderByEMTID"
        or cancel.reconciliation_query != "QueryOrderByEMTID"
    ):
        raise GatewayMutationError(
            "order and cancel reconciliation_query must be QueryOrderByEMTID"
        )
    if order.rate_limit_per_second != 1 or cancel.rate_limit_per_second != 1:
        raise GatewayMutationError(
            "the initial mutation driver permits exactly one command per second"
        )
    if cancel.request:
        raise GatewayMutationError(
            "CancelOrder request must be empty; the gateway cancels only its own returned order id"
        )
    return OrderCancelPlan(
        order_request=_validated_limit_order(order.request, profile=profile),
        rate_limit_per_second=1,
        reconciliation_query="QueryOrderByEMTID",
        profile=profile,
    )


def run_gateway_mutation(settings: GatewayMutationSettings) -> dict[str, Any]:
    """Preflight or run the bounded ordinary simulated order/cancel cycle.

    Without ``execute_simulated`` this function reads and validates only the
    ignored local fixture; it does not load the SDK, log in, or call EMT.
    """

    settings.validate()
    plan = (
        OrderCancelPlan(
            order_request=_validated_limit_order(
                settings.order_request_override, profile=settings.profile
            ),
            rate_limit_per_second=1,
            reconciliation_query="QueryOrderByEMTID",
            profile=settings.profile,
        )
        if settings.order_request_override is not None
        else select_order_cancel_plan(
            load_mutation_fixture(settings.fixture, profile=settings.profile),
            profile=settings.profile,
        )
    )
    if (
        plan.profile == "credit"
        and plan.order_request["business_type"] == 4
        and settings.cycles != 1
    ):
        raise GatewayMutationError(
            "a simulated margin order fixture permits exactly one order/cancel cycle per run"
        )
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-mutation-order-cancel-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "order_entry": False,
                "cancellation": False,
                "reconciliation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": plan.profile,
                "planned_commands": ["InsertOrder", "CancelOrder"],
                "reconciliation_query": plan.reconciliation_query,
                "planned_cycles": settings.cycles,
                "maximum_commands": settings.cycles * 2,
                "interval_seconds": max(
                    settings.interval_seconds, 1.0 / plan.rate_limit_per_second
                ),
            },
        }

    return _execute_plan(settings, plan)


def _execute_plan(settings: GatewayMutationSettings, plan: OrderCancelPlan) -> dict[str, Any]:
    """Run one no-retry native order/cancel sequence and retain safe evidence."""

    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    ingress = DeferredCallbackIngress()
    session: EmtSession | None = None
    command_journal: FileJournal | None = None
    command_events: FileJournal | None = None
    query_events: FileJournal | None = None
    command_drainer: _CallbackDrainer | None = None
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
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
                client_id=(
                    settings.session_client_id
                    if settings.session_client_id is not None
                    else {"ordinary": 94, "credit": 95, "option": 96}[settings.profile]
                ),
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile=settings.profile,
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "commands.emtj")
        command_events = FileJournal(settings.runtime_dir / "command-events.emtj")
        query_events = FileJournal(settings.runtime_dir / "query-events.emtj")

        native_client_ids: dict[str, int] = {}

        def make_native_order_request(command_id: str, client_order_id: str) -> dict[str, Any]:
            del command_id
            try:
                native_client_id = native_client_ids[client_order_id]
            except KeyError as exc:
                raise GatewayMutationError("native order request has no owned client id") from exc
            # The only injected field is a per-run client reference.  It is
            # intentionally absent from the fixture and reports.
            return {**plan.order_request, "order_client_id": native_client_id}

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
            require_native_fastspi=settings.require_native_fastspi,
        )
        query_callbacks = _query_callbacks(
            query_events, trace, registry, settings.require_native_fastspi
        )
        ingress.attach(
            _MutationCallbackIngress(commands=gateway.callbacks, queries=query_callbacks)
        )
        command_drainer = _CallbackDrainer(gateway.callbacks)
        command_drainer.start()
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )

        interval = max(settings.interval_seconds, 1.0 / plan.rate_limit_per_second)
        outcomes: list[dict[str, str | int | None]] = []
        reconciliation: list[dict[str, str | int | None]] = []
        completed_cycles = 0
        completed_cycle_identity: tuple[int, int] | None = None
        last_command_started_at: float | None = None
        for cycle in range(1, settings.cycles + 1):
            native_client_id = (
                str(settings.fixed_native_order_client_id)
                if settings.fixed_native_order_client_id is not None
                else _new_native_client_id(set(native_client_ids.values()))
            )
            client_order_id = f"perf-order-{native_client_id}"
            native_client_ids[client_order_id] = int(native_client_id)
            canonical_order = json.dumps(
                plan.order_request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            order_payload_hash = sha256(
                f"{canonical_order}:{native_client_id}".encode("utf-8")
            ).hexdigest()

            last_command_started_at = _wait_for_command_slot(last_command_started_at, interval)
            submitted = gateway.submit_order(
                OrderIntent(
                    client_order_id=client_order_id,
                    payload_hash=order_payload_hash,
                    profile=settings.profile,
                    workload="steady",
                )
            )
            outcomes.append(
                _command_outcome(
                    cycle,
                    "InsertOrder",
                    submitted,
                    trace,
                    command_drainer,
                    settings.callback_timeout_seconds,
                )
            )
            if submitted.state is not CommandState.NATIVE_ACCEPTED or _trace_is_not_ok(
                trace, submitted.correlation_id
            ):
                break

            order_reconciliation = _reconcile_order(
                cycle=cycle,
                facade=facade,
                callbacks=query_callbacks,
                registry=registry,
                trace=trace,
                profile=settings.profile,
                order_emt_id=submitted.native_id,
                timeout_seconds=settings.callback_timeout_seconds,
            )
            reconciliation.append(order_reconciliation)
            if order_reconciliation["outcome"] != "ok":
                break

            last_command_started_at = _wait_for_command_slot(last_command_started_at, interval)
            cancelled = gateway.cancel_order(
                CancelIntent(
                    client_cancel_id=f"perf-cancel-{native_client_id}",
                    target_client_order_id=client_order_id,
                    profile=settings.profile,
                    workload="steady",
                )
            )
            outcomes.append(
                _command_outcome(
                    cycle,
                    "CancelOrder",
                    cancelled,
                    trace,
                    command_drainer,
                    settings.callback_timeout_seconds,
                )
            )
            if cancelled.state is not CommandState.NATIVE_ACCEPTED or _trace_is_not_ok(
                trace, cancelled.correlation_id
            ):
                break

            cancel_reconciliation = _reconcile_order(
                cycle=cycle,
                facade=facade,
                callbacks=query_callbacks,
                registry=registry,
                trace=trace,
                profile=settings.profile,
                order_emt_id=submitted.native_id,
                timeout_seconds=settings.callback_timeout_seconds,
            )
            reconciliation.append(cancel_reconciliation)
            if cancel_reconciliation["outcome"] != "ok":
                break
            completed_cycles = cycle
            if submitted.native_id is None:
                raise GatewayMutationError("accepted controlled order lacks native order id")
            try:
                completed_cycle_identity = (int(native_client_id), int(submitted.native_id))
            except (TypeError, ValueError) as exc:
                raise GatewayMutationError(
                    "accepted controlled order identity is not a positive integer pair"
                ) from exc
        if settings.on_cycle_completed is not None:
            if completed_cycles != settings.cycles or completed_cycle_identity is None:
                raise GatewayMutationError(
                    "controlled cycle did not complete before witness issuance"
                )
            settings.on_cycle_completed(
                str(snapshot.trading_day),
                completed_cycle_identity[0],
                completed_cycle_identity[1],
            )
        return _mutation_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            interval=interval,
            outcomes=outcomes,
            reconciliation=reconciliation,
            completed_cycles=completed_cycles,
        )
    finally:
        try:
            if command_drainer is not None:
                command_drainer.close()
        finally:
            try:
                if session is not None:
                    if settings.retain_native_reference is not None:
                        settings.retain_native_reference(session)
                    else:
                        session.close()
            finally:
                for journal in (query_events, command_events, command_journal):
                    if journal is not None:
                        journal.close()


def _query_callbacks(
    journal: FileJournal,
    trace: GatewayTraceCollector,
    registry: QueryRegistry,
    require_native_fastspi: bool,
) -> CallbackPipeline:
    return CallbackPipeline(
        journal=journal,
        trace=trace,
        lookup_source=lambda record: None,
        capacity=1024,
        lifecycle_observer=registry,
        require_native_fastspi=require_native_fastspi,
    )


def _command_outcome(
    cycle: int,
    api_name: str,
    result: CommandResult,
    trace: GatewayTraceCollector,
    callback_drainer: _CallbackDrainer,
    timeout_seconds: float,
) -> dict[str, str | int | None]:
    if result.state is CommandState.NATIVE_ACCEPTED:
        callback_drainer.wait_for_command(trace, result.correlation_id, timeout_seconds)
    record = _trace_record(trace, result.correlation_id)
    return {
        "cycle": cycle,
        "api": api_name,
        "gateway_state": result.state.value,
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "native_error_code": result.native_error_code,
    }


def _reconcile_order(
    *,
    cycle: int,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    profile: str,
    order_emt_id: int | None,
    timeout_seconds: float,
) -> dict[str, str | int | None]:
    if order_emt_id is None:
        raise GatewayMutationError("cannot reconcile a command without a returned native order id")
    handle = facade.invoke_query(
        "QueryOrderByEMTID",
        business_args=(order_emt_id,),
        profile=profile,
        workload="steady",
        timeout_ns=int(timeout_seconds * 1_000_000_000),
    )
    _drain_until_complete(
        callbacks=callbacks,
        registry=registry,
        timeout_seconds=timeout_seconds,
    )
    record = _trace_record(trace, handle.correlation_id)
    return {
        "cycle": cycle,
        "api": "QueryOrderByEMTID",
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
    }


def _mutation_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: GatewayMutationSettings,
    interval: float,
    outcomes: list[dict[str, str | int | None]],
    reconciliation: list[dict[str, str | int | None]],
    completed_cycles: int,
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-mutation-order-cancel-c0-c7"
    report["executed"] = True
    report["safety"] = {
        "mode": "bounded-sequential-simulated-order-cycles",
        "maximum_commands": settings.cycles * 2,
        "order_entry": True,
        "cancellation": True,
        "fund_or_credit_mutation": False,
        "option_mutation": settings.profile == "option",
        "automatic_retry": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": settings.profile,
        "requested_cycles": settings.cycles,
        "completed_cycles": completed_cycles,
        "stopped_early": completed_cycles != settings.cycles,
        "interval_seconds": interval,
        "commands": outcomes,
        "reconciliation": reconciliation,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _wait_for_command_slot(last_started_at: float | None, interval_seconds: float) -> float:
    """Return the start time of the next native command after the hard gap."""

    if last_started_at is not None:
        remaining = interval_seconds - (monotonic() - last_started_at)
        if remaining > 0:
            sleep(remaining)
    return monotonic()


def _new_native_client_id(allocated: set[int]) -> str:
    """Generate one positive vendor client id that is unique within this run."""

    for _ in range(100):
        candidate = secrets.randbelow(900_000_000) + 100_000_000
        if candidate not in allocated:
            return str(candidate)
    raise GatewayMutationError(
        "could not allocate a unique native client id for the controlled batch"
    )


def _write_payload_free_trace(path: Path, events: list[dict[str, Any]]) -> None:
    """Persist the reportable C0-C7 facts required for cross-batch aggregation."""

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = "\n".join(json.dumps(event, ensure_ascii=True, sort_keys=True) for event in events)
    path.write_text(f"{serialized}\n" if serialized else "", encoding="utf-8")


def _trace_record(trace: GatewayTraceCollector, correlation_id: str) -> Any | None:
    return next(
        (record for record in trace.records() if record.correlation_id == correlation_id), None
    )


def _trace_is_not_ok(trace: GatewayTraceCollector, correlation_id: str) -> bool:
    record = _trace_record(trace, correlation_id)
    return record is None or record.outcome != "ok"


def _validated_limit_order(request: Mapping[str, Any], *, profile: str) -> dict[str, Any]:
    """Accept a small explicit ordinary, credit, or option limit-order envelope."""

    if set(request) - _ORDER_FIELDS or not _REQUIRED_ORDER_FIELDS.issubset(request):
        raise GatewayMutationError("InsertOrder request must use the bounded limit-order field set")
    ticker = request.get("ticker")
    ticker_pattern = _OPTION_TICKER if profile == "option" else _ORDINARY_TICKER
    if not isinstance(ticker, str) or not ticker_pattern.fullmatch(ticker):
        expected = "eight" if profile == "option" else "six"
        raise GatewayMutationError(f"{profile} InsertOrder ticker must be an {expected}-digit code")
    if request.get("market") not in {1, 2}:
        raise GatewayMutationError("InsertOrder market is invalid")
    if request.get("price_type") != 1:
        raise GatewayMutationError("the controlled fixture permits limit orders only")
    price = request.get("price")
    if type(price) not in {int, float} or not 0 < float(price) < 1_000_000:
        raise GatewayMutationError("InsertOrder price must be a finite positive limit price")
    quantity = request.get("quantity")
    if type(quantity) is not int or not 1 <= quantity <= 100:
        raise GatewayMutationError("the controlled fixture limits quantity to 1..100")
    if profile == "ordinary":
        if request.get("side") not in {1, 2}:
            raise GatewayMutationError("ordinary cash-equity order side is invalid")
        if request.get("business_type") != 0:
            raise GatewayMutationError(
                "the ordinary controlled fixture permits cash-equity business_type 0 only"
            )
    elif profile == "credit":
        business_type = request.get("business_type")
        side = request.get("side")
        if business_type == 0 and side in {1, 2}:
            pass
        elif business_type == 4 and side in _CREDIT_MARGIN_SIDES:
            if quantity != 100:
                raise GatewayMutationError(
                    "a simulated margin order must use exactly one 100-share lot"
                )
        else:
            raise GatewayMutationError(
                "credit fixture permits only cash business_type 0 (side 1/2) or margin business_type 4 (side 21/22)"
            )
    elif profile == "option":
        if request.get("business_type") != 10:
            raise GatewayMutationError(
                "the option controlled fixture permits option business_type 10 only"
            )
        if quantity != 1:
            raise GatewayMutationError(
                "the option controlled fixture permits only one opening contract"
            )
        if request.get("position_effect") != 1:
            raise GatewayMutationError(
                "the option controlled fixture requires position_effect 1 (open)"
            )
        if request.get("ep_id") != 0:
            raise GatewayMutationError("the option controlled fixture requires ep_id 0")
    else:
        raise GatewayMutationError("profile must be ordinary, credit or option")
    for name in ("position_effect", "ep_id"):
        if name in request and (type(request[name]) is not int or request[name] < 0):
            raise GatewayMutationError(f"InsertOrder {name} must be a non-negative integer")
    return dict(request)
