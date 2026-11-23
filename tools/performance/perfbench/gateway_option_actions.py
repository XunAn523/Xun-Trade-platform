"""Bounded real option-combination and exercise measurement through the gateway.

Each run permits exactly one reviewed option action at a rate of one command
per second.  The command is recorded durably before native submission, is
correlated to its native order id and callback, then followed by a read-only
reconciliation query.  It is deliberately a compatibility measurement, not a
throughput test: a callback timeout or a failed reconciliation stops the run
and never causes a retry.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Mapping
from uuid import uuid4

from emt_gateway.facade import EmtTraderFacade, SideEffectPermit
from emt_gateway.gateway import CallbackPipeline, CallbackRecord
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .gateway_mutation import _MutationCallbackIngress, _write_payload_free_trace
from .gateway_readonly import _drain_until_complete
from .gateway_report import build_gateway_report
from .mutation_fixture import MutationFixtureEntry, load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


class OptionActionError(RuntimeError):
    """The local option-action fixture is unsafe, incomplete, or cannot run."""


@dataclass(frozen=True)
class OptionActionSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    trace_output: Path | None = None
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 1.0
    execute_simulated: bool = False
    require_native_fastspi: bool = True

    def validate(self) -> None:
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise OptionActionError("option callback_timeout_seconds must be in 3..30")
        if self.interval_seconds < 1.0:
            raise OptionActionError(
                "option action interval_seconds must stay at or above one second"
            )


@dataclass(frozen=True)
class OptionActionPlan:
    entry: MutationFixtureEntry
    reconciliation_query: str


@dataclass
class _CommandSource:
    command_id: str
    correlation_id: str
    native_id: int | None = None


_SUPPORTED = frozenset({"InsertOptionCombinedOrder", "InsertOptionExerciseOrder"})
_OPTION_TICKER = re.compile(r"[0-9]{8}\Z")
_COMBINED_FIELDS = frozenset({"market", "quantity", "side", "business_type", "opt_comb_info"})
_COMBINED_INFO_FIELDS = frozenset({"strategy_id", "comb_num", "num_legs", "leg_detail"})
_COMBINED_LEG_FIELDS = frozenset(
    {"leg_security_id", "leg_cntr_type", "leg_side", "leg_covered", "leg_qty"}
)
_EXERCISE_FIELDS = frozenset({"market", "business_type", "quantity", "ticker"})


def select_option_action_plan(entries: tuple[MutationFixtureEntry, ...]) -> OptionActionPlan:
    """Accept one tiny, reviewed option combination or simple exercise action."""

    if len(entries) != 1 or entries[0].api_name not in _SUPPORTED:
        raise OptionActionError("option action fixture must enable exactly one supported command")
    entry = entries[0]
    if entry.rate_limit_per_second != 1:
        raise OptionActionError("option action requires exactly one command per second")
    if entry.api_name == "InsertOptionCombinedOrder":
        if entry.reconciliation_query != "QueryOptionCombinedOrderByEMTID":
            raise OptionActionError(
                "combined action reconciliation_query must be QueryOptionCombinedOrderByEMTID"
            )
        _validate_combined_request(entry.request)
    else:
        if entry.reconciliation_query != "QueryPosition":
            raise OptionActionError("exercise action reconciliation_query must be QueryPosition")
        _validate_exercise_request(entry.request)
    return OptionActionPlan(entry=entry, reconciliation_query=entry.reconciliation_query)


def run_option_action(settings: OptionActionSettings) -> dict[str, Any]:
    """Preflight or execute exactly one local simulated option action."""

    settings.validate()
    plan = select_option_action_plan(load_mutation_fixture(settings.fixture, profile="option"))
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-option-action-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "automatic_retry": False,
                "option_mutation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": "option",
                "planned_commands": [plan.entry.api_name],
                "maximum_commands": 1,
                "interval_seconds": settings.interval_seconds,
                "reconciliation_query": plan.reconciliation_query,
            },
        }
    return _execute_option_action(settings, plan)


def _execute_option_action(
    settings: OptionActionSettings, plan: OptionActionPlan
) -> dict[str, Any]:
    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    ingress = DeferredCallbackIngress()
    by_operation: dict[int, _CommandSource] = {}
    by_order: dict[int, _CommandSource] = {}
    session: EmtSession | None = None
    command_journal: FileJournal | None = None
    event_journal: FileJournal | None = None
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    try:
        loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
        session = EmtSession.from_loaded_sdk(
            loaded,
            callback_ingress=ingress,
            settings=SessionSettings(
                host=settings.accounts.host,
                port=settings.accounts.port,
                account=settings.accounts.option.account,
                password=settings.accounts.option.password,
                client_id=96,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="option",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "option-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "option-events.emtj")
        callbacks = CallbackPipeline(
            journal=event_journal,
            trace=trace,
            lookup_source=lambda record: _lookup_source(by_operation, by_order, record),
            capacity=1024,
            lifecycle_observer=registry,
            require_native_fastspi=settings.require_native_fastspi,
        )
        ingress.attach(_MutationCallbackIngress(commands=callbacks, queries=callbacks))
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )
        outcome, source = _invoke_option_command(plan.entry, facade, trace, command_journal)
        if source is not None and source.native_id is not None:
            by_operation[source.native_id] = source
            by_order[source.native_id] = source
            _drain_command_until_terminal(
                callbacks=callbacks,
                trace=trace,
                correlation_id=source.correlation_id,
                timeout_seconds=settings.callback_timeout_seconds,
            )
            outcome = _command_outcome(plan.entry.api_name, source, trace)
        reconciliation = _reconcile(
            entry=plan.entry,
            facade=facade,
            callbacks=callbacks,
            registry=registry,
            trace=trace,
            native_id=source.native_id if source is not None else None,
            timeout_seconds=settings.callback_timeout_seconds,
        )
        return _option_action_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            plan=plan,
            outcome=outcome,
            reconciliation=reconciliation,
        )
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            for journal in (event_journal, command_journal):
                if journal is not None:
                    journal.close()


def _invoke_option_command(
    entry: MutationFixtureEntry,
    facade: EmtTraderFacade,
    trace: GatewayTraceCollector,
    journal: FileJournal,
) -> tuple[dict[str, str | int | None], _CommandSource | None]:
    command_id, correlation_id = str(uuid4()), str(uuid4())
    source = _CommandSource(command_id, correlation_id)
    canonical = json.dumps(entry.request, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    payload_hash = sha256(canonical.encode("utf-8")).hexdigest()
    trace.begin(
        correlation_id, kind="api", interface=entry.api_name, profile="option", workload="steady"
    )
    trace.mark(correlation_id, "C1")
    try:
        journal.append(
            kind="COMMAND_PREPARED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
        trace.mark(correlation_id, "C2")
        journal.append(
            kind="CALL_STARTED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
        trace.mark(correlation_id, "C3")
    except Exception:
        trace.complete(correlation_id, "failed", "option_command_journal_failure")
        return _command_outcome(entry.api_name, source, trace), None
    try:
        invocation = facade.invoke(
            entry.api_name,
            business_args=(dict(entry.request),),
            side_effect_permit=SideEffectPermit.simulated_fixture(),
            profile="option",
            workload="steady",
        )
        native_id = int(invocation.return_value)
    except Exception as exc:
        trace.mark(correlation_id, "C4")
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", type(exc).__name__)
        return _command_outcome(entry.api_name, source, trace), None
    trace.mark(correlation_id, "C4")
    if native_id == 0:
        _append_best_effort(
            journal, "LOCAL_SEND_REJECTED", command_id, correlation_id, payload_hash
        )
        trace.complete(correlation_id, "rejected", "native_zero_return")
        return _command_outcome(entry.api_name, source, trace), None
    try:
        journal.append(
            kind="NATIVE_ACCEPTED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
    except Exception:
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", "native_accepted_journal_failure")
        return _command_outcome(entry.api_name, source, trace), None
    source.native_id = native_id
    return _command_outcome(entry.api_name, source, trace), source


def _lookup_source(
    by_operation: Mapping[int, _CommandSource],
    by_order: Mapping[int, _CommandSource],
    record: CallbackRecord,
) -> _CommandSource | None:
    if record.operation_native_id is not None:
        source = by_operation.get(record.operation_native_id)
        if source is not None:
            return source
    return by_order.get(record.order_emt_id) if record.order_emt_id is not None else None


def _drain_command_until_terminal(
    *,
    callbacks: CallbackPipeline,
    trace: GatewayTraceCollector,
    correlation_id: str,
    timeout_seconds: float,
) -> None:
    deadline = monotonic() + timeout_seconds
    while True:
        record = _trace_record(trace, correlation_id)
        if record is not None and record.outcome is not None:
            return
        delivery = callbacks.normalize_one()
        if delivery is not None:
            callbacks.ack(delivery.delivery_id)
            continue
        if callbacks.disarmed:
            trace.complete(correlation_id, "failed", "option_callback_pipeline_disarmed")
            return
        if monotonic() >= deadline:
            trace.complete(correlation_id, "timeout", "option_callback_timeout")
            return
        sleep(0.001)


def _reconcile(
    *,
    entry: MutationFixtureEntry,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    native_id: int | None,
    timeout_seconds: float,
) -> dict[str, str | int | bool | None]:
    if entry.api_name == "InsertOptionCombinedOrder":
        if native_id is None:
            return {
                "api": "QueryOptionCombinedOrderByEMTID",
                "outcome": "not_dispatched",
                "reason": "no_native_id",
                "terminal_seen": False,
                "callback_count": 0,
                "item_count": 0,
            }
        api_name, business_args = "QueryOptionCombinedOrderByEMTID", (native_id,)
    else:
        api_name, business_args = "QueryPosition", (entry.request["ticker"][0],)
    try:
        handle = facade.invoke_query(
            api_name,
            business_args=business_args,
            profile="option",
            workload="steady",
            timeout_ns=int(timeout_seconds * 1_000_000_000),
        )
    except Exception as exc:
        return {
            "api": api_name,
            "outcome": "failed",
            "reason": type(exc).__name__,
            "terminal_seen": False,
            "callback_count": 0,
            "item_count": 0,
        }
    _drain_until_complete(callbacks=callbacks, registry=registry, timeout_seconds=timeout_seconds)
    record = _trace_record(trace, handle.correlation_id)
    observation = registry.semantic_observation(handle.correlation_id)
    return {
        "api": api_name,
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "terminal_seen": observation.terminal_seen if observation is not None else False,
        "callback_count": observation.callback_count if observation is not None else 0,
        "item_count": observation.item_count if observation is not None else 0,
    }


def _option_action_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: OptionActionSettings,
    plan: OptionActionPlan,
    outcome: dict[str, str | int | None],
    reconciliation: dict[str, str | int | bool | None],
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-option-action-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "one-pass simulated option compatibility; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": 1,
        "automatic_retry": False,
        "manual_reconciliation_required_after_unknown": True,
        "option_mutation": True,
        "credit_mutation": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": "option",
        "planned_commands": [plan.entry.api_name],
        "completed_commands": 1,
        "stopped_early": outcome["outcome"] != "ok" or reconciliation["outcome"] != "ok",
        "command": outcome,
        "reconciliation": reconciliation,
        "interval_seconds": settings.interval_seconds,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _command_outcome(
    api_name: str, source: _CommandSource, trace: GatewayTraceCollector
) -> dict[str, str | int | None]:
    record = _trace_record(trace, source.correlation_id)
    return {
        "api": api_name,
        "outcome": record.outcome if record is not None else "pending_callback",
        "reason": record.reason if record is not None else None,
    }


def _trace_record(trace: GatewayTraceCollector, correlation_id: str) -> Any | None:
    return next((item for item in trace.records() if item.correlation_id == correlation_id), None)


def _append_best_effort(
    journal: FileJournal, kind: str, command_id: str, correlation_id: str, payload_hash: str
) -> None:
    try:
        journal.append(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
    except Exception:
        return None


def _validate_combined_request(request: Mapping[str, Any]) -> None:
    if set(request) != _COMBINED_FIELDS:
        raise OptionActionError("InsertOptionCombinedOrder request field set is invalid")
    if request.get("market") not in {1, 2} or request.get("quantity") != 1:
        raise OptionActionError("combined action permits one Shenzhen or Shanghai strategy only")
    if request.get("side") != 31 or request.get("business_type") != 13:
        raise OptionActionError(
            "combined action requires EMT_SIDE_OPT_COMBINE and business_type 13"
        )
    info = request.get("opt_comb_info")
    if not isinstance(info, Mapping) or set(info) != _COMBINED_INFO_FIELDS:
        raise OptionActionError("combined action opt_comb_info field set is invalid")
    if not isinstance(info["strategy_id"], str) or not info["strategy_id"]:
        raise OptionActionError("combined action strategy_id is invalid")
    if not isinstance(info["comb_num"], str) or info["comb_num"]:
        raise OptionActionError("combined action only permits a new empty comb_num")
    if (
        info["num_legs"] != 2
        or not isinstance(info["leg_detail"], list)
        or len(info["leg_detail"]) != 2
    ):
        raise OptionActionError("combined action requires exactly two legs")
    seen: set[str] = set()
    sides: set[int] = set()
    for leg in info["leg_detail"]:
        if not isinstance(leg, Mapping) or set(leg) != _COMBINED_LEG_FIELDS:
            raise OptionActionError("combined action leg field set is invalid")
        ticker = leg["leg_security_id"]
        if not isinstance(ticker, str) or not _OPTION_TICKER.fullmatch(ticker) or ticker in seen:
            raise OptionActionError(
                "combined action legs must be distinct eight-digit option contracts"
            )
        seen.add(ticker)
        if leg["leg_cntr_type"] != 1 or leg["leg_covered"] != 0 or leg["leg_qty"] != 1:
            raise OptionActionError("combined action permits one uncovered option contract per leg")
        if leg["leg_side"] not in {1, 2}:
            raise OptionActionError("combined action leg side is invalid")
        sides.add(leg["leg_side"])
    if sides != {1, 2}:
        raise OptionActionError("combined action requires one long and one short leg")


def _validate_exercise_request(request: Mapping[str, Any]) -> None:
    if set(request) != _EXERCISE_FIELDS:
        raise OptionActionError("InsertOptionExerciseOrder request field set is invalid")
    if (
        request.get("market") not in {1, 2}
        or request.get("business_type") != 11
        or request.get("quantity") != 1
    ):
        raise OptionActionError("exercise action permits one simple option exercise only")
    ticker = request.get("ticker")
    if (
        not isinstance(ticker, list)
        or len(ticker) != 2
        or not isinstance(ticker[0], str)
        or ticker[1] != ""
    ):
        raise OptionActionError("exercise action must use one contract and an empty second ticker")
    if not _OPTION_TICKER.fullmatch(ticker[0]):
        raise OptionActionError("exercise action ticker must be an eight-digit option contract")
