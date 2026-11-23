"""Recoverable paired ordinary-account fund-transfer measurement.

Each pair issues a one-unit ordinary-account ``FundTransfer`` in the reviewed
fixture direction and then an equal request in the opposite direction.  The
two legs are deliberately sequential: their callback and history-query
evidence remains unambiguous, and a later leg is never sent when an earlier
leg has an incomplete terminal path.  A credit quota pair may execute in
parallel in another account/session, but this driver is not a throughput
workload and does not make same-account operations concurrent.
"""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable
from uuid import uuid4

from emt_gateway.facade import EmtTraderFacade, SideEffectPermit
from emt_gateway.gateway import CallbackPipeline, CallbackRecord
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .gateway_credit_repayment import (
    CreditRepaymentError,
    _command_outcome,
    _CommandSource,
    _drain_command_until_terminal,
)
from .gateway_mutation import _MutationCallbackIngress, _write_payload_free_trace
from .gateway_readonly import _drain_until_complete
from .gateway_report import build_gateway_report
from .mutation_fixture import MutationFixtureEntry, load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector

_FUND_TRANSFER_FIELDS = frozenset({"amount", "transfer_type"})
_MAXIMUM_SINGLE_TRANSFER = 1.0


@dataclass(frozen=True)
class FundTransferPairSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    pairs: int = 30
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 1.0
    trace_output: Path | None = None
    execute_simulated: bool = False
    require_native_fastspi: bool = True
    retain_native_reference: Callable[[object], None] | None = None

    def validate(self) -> None:
        if not 1 <= self.pairs <= 30:
            raise CreditRepaymentError("fund-transfer pair count must be in 1..30")
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise CreditRepaymentError("fund-transfer callback_timeout_seconds must be in 3..30")
        if self.interval_seconds < 1.0:
            raise CreditRepaymentError(
                "fund-transfer interval_seconds must stay at or above one second"
            )


@dataclass(frozen=True)
class FundTransferPlan:
    """A tiny ordinary-account transfer request without credentials or serials."""

    entry: MutationFixtureEntry


def select_fund_transfer_plan(entries: tuple[MutationFixtureEntry, ...]) -> FundTransferPlan:
    """Fail closed unless one bounded ordinary-account transfer is configured.

    The native request additionally requires a serial, account and password.
    Those values are allocated or injected only in process memory and are
    consequently forbidden from the local fixture, trace, report and journal.
    """

    if len(entries) != 1 or entries[0].api_name != "FundTransfer":
        raise CreditRepaymentError(
            "fund-transfer fixture must enable exactly one FundTransfer command"
        )
    entry = entries[0]
    if entry.rate_limit_per_second != 1:
        raise CreditRepaymentError("FundTransfer requires exactly one command per second")
    if entry.reconciliation_query != "QueryFundTransfer":
        raise CreditRepaymentError("FundTransfer reconciliation_query must be QueryFundTransfer")
    if set(entry.request) != _FUND_TRANSFER_FIELDS:
        raise CreditRepaymentError(
            "FundTransfer request field set must contain only amount and transfer_type"
        )
    amount = entry.request["amount"]
    if type(amount) not in {int, float} or not 0 < float(amount) <= _MAXIMUM_SINGLE_TRANSFER:
        raise CreditRepaymentError(
            "FundTransfer amount must be a finite positive value no greater than one"
        )
    if entry.request["transfer_type"] not in {0, 1}:
        raise CreditRepaymentError("FundTransfer transfer_type must be 0 or 1")
    return FundTransferPlan(entry=entry)


def run_fund_transfer_pair(settings: FundTransferPairSettings) -> dict[str, Any]:
    """Preflight or execute bounded equal-and-opposite transfer requests."""

    settings.validate()
    plan = select_fund_transfer_plan(load_mutation_fixture(settings.fixture, profile="ordinary"))
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-fund-transfer-pair-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "automatic_retry": False,
                "fund_mutation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": "ordinary",
                "planned_commands": ["FundTransfer", "FundTransfer"],
                "planned_pairs": settings.pairs,
                "maximum_commands": settings.pairs * 2,
                "interval_seconds": settings.interval_seconds,
                "reconciliation_query": "QueryFundTransfer",
            },
        }
    return _execute_fund_transfer_pairs(settings, plan)


def _execute_fund_transfer_pairs(
    settings: FundTransferPairSettings,
    plan: FundTransferPlan,
) -> dict[str, Any]:
    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    ingress = DeferredCallbackIngress()
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
                account=settings.accounts.ordinary.account,
                password=settings.accounts.ordinary.password,
                client_id=100,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="ordinary",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "fund-transfer-pair-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "fund-transfer-pair-events.emtj")
        active_source: _CommandSource | None = None

        def lookup(record: CallbackRecord) -> _CommandSource | None:
            if active_source is None or record.callback_name != "OnFundTransfer":
                return None
            if record.operation_native_id == active_source.native_id:
                return active_source
            return None

        callbacks = CallbackPipeline(
            journal=event_journal,
            trace=trace,
            lookup_source=lookup,
            capacity=1024,
            lifecycle_observer=registry,
            require_native_fastspi=settings.require_native_fastspi,
        )
        ingress.attach(_MutationCallbackIngress(commands=callbacks, queries=callbacks))
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )

        first_direction = int(plan.entry.request["transfer_type"])
        directions = (first_direction, 1 - first_direction)
        used_serials: set[int] = set()
        command_outcomes: list[dict[str, str | int | None]] = []
        reconciliations: list[dict[str, str | int | bool | None]] = []
        completed_pairs = 0
        last_started_at: float | None = None
        stopped = False
        for pair in range(1, settings.pairs + 1):
            for direction in directions:
                if last_started_at is not None:
                    remaining = settings.interval_seconds - (monotonic() - last_started_at)
                    if remaining > 0:
                        sleep(remaining)
                entry = replace(
                    plan.entry, request={**plan.entry.request, "transfer_type": direction}
                )
                last_started_at = monotonic()
                active_source = _invoke_fund_transfer_command(
                    entry=entry,
                    facade=facade,
                    trace=trace,
                    journal=command_journal,
                    fund_account=settings.accounts.ordinary.account,
                    password=settings.accounts.ordinary.password,
                    serial_id=_new_transfer_serial(used_serials),
                )
                if active_source is not None:
                    _drain_command_until_terminal(
                        callbacks=callbacks,
                        trace=trace,
                        correlation_id=active_source.correlation_id,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                outcome = _command_outcome("FundTransfer", active_source, trace)
                command_outcomes.append({"pair": pair, **outcome})
                reconciliation = _reconcile_fund_transfer(
                    facade=facade,
                    callbacks=callbacks,
                    registry=registry,
                    trace=trace,
                    timeout_seconds=settings.callback_timeout_seconds,
                )
                reconciliations.append({"pair": pair, **reconciliation})
                active_source = None
                if (
                    outcome["outcome"] != "ok"
                    or reconciliation["outcome"] != "ok"
                    or not reconciliation["terminal_seen"]
                ):
                    stopped = True
                    break
            if stopped:
                break
            completed_pairs = pair
        return _fund_transfer_pair_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            command_outcomes=command_outcomes,
            reconciliations=reconciliations,
            completed_pairs=completed_pairs,
        )
    finally:
        try:
            if session is not None:
                if settings.retain_native_reference is not None:
                    settings.retain_native_reference(session)
                else:
                    session.close()
        finally:
            for journal in (event_journal, command_journal):
                if journal is not None:
                    journal.close()


def _invoke_fund_transfer_command(
    *,
    entry: MutationFixtureEntry,
    facade: EmtTraderFacade,
    trace: GatewayTraceCollector,
    journal: FileJournal,
    fund_account: str,
    password: str,
    serial_id: int,
) -> _CommandSource | None:
    """Journal and submit one request without retaining its business fields."""

    command_id, correlation_id = str(uuid4()), str(uuid4())
    source = _CommandSource(command_id, correlation_id)
    canonical = json.dumps(entry.request, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    payload_hash = sha256(f"{canonical}:{serial_id}".encode("utf-8")).hexdigest()
    trace.begin(
        correlation_id, kind="api", interface="FundTransfer", profile="ordinary", workload="steady"
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
        trace.complete(correlation_id, "failed", "fund_transfer_command_journal_failure")
        return None
    request = {
        **entry.request,
        "serial_id": serial_id,
        "fund_account": fund_account,
        "password": password,
    }
    try:
        invocation = facade.invoke(
            "FundTransfer",
            business_args=(request,),
            side_effect_permit=SideEffectPermit.simulated_fixture(),
            profile="ordinary",
            workload="steady",
        )
        native_result = int(invocation.return_value)
    except Exception as exc:
        trace.mark(correlation_id, "C4")
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", type(exc).__name__)
        return None
    trace.mark(correlation_id, "C4")
    # FundTransfer returns its native serial on a successful local send.  A
    # zero result is the only local-send failure form, and the returned serial
    # rather than the client-provided request field is the callback key.
    if native_result == 0:
        _append_best_effort(
            journal, "LOCAL_SEND_REJECTED", command_id, correlation_id, payload_hash
        )
        trace.complete(correlation_id, "rejected", "native_zero_return")
        return None
    try:
        journal.append(
            kind="NATIVE_DISPATCHED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
    except Exception:
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", "fund_transfer_dispatched_journal_failure")
        return None
    source.native_id = native_result
    return source


def _reconcile_fund_transfer(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    timeout_seconds: float,
) -> dict[str, str | int | bool | None]:
    try:
        handle = facade.invoke_query(
            "QueryFundTransfer",
            business_args=({"serial_id": 0},),
            profile="ordinary",
            workload="steady",
            timeout_ns=int(timeout_seconds * 1_000_000_000),
        )
    except Exception as exc:
        return {
            "api": "QueryFundTransfer",
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
        "api": "QueryFundTransfer",
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "terminal_seen": observation.terminal_seen if observation is not None else False,
        "callback_count": observation.callback_count if observation is not None else 0,
        "item_count": observation.item_count if observation is not None else 0,
    }


def _new_transfer_serial(allocated: set[int]) -> int:
    """Allocate a positive, per-run unique opaque serial without persisting it."""

    for _ in range(100):
        candidate = secrets.randbelow(900_000_000) + 100_000_000
        if candidate not in allocated:
            allocated.add(candidate)
            return candidate
    raise CreditRepaymentError("could not allocate a unique FundTransfer serial")


def _append_best_effort(
    journal: FileJournal,
    kind: str,
    command_id: str,
    correlation_id: str,
    payload_hash: str,
) -> None:
    try:
        journal.append(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
    except Exception:
        pass


def _trace_record(trace: GatewayTraceCollector, correlation_id: str) -> Any | None:
    return next(
        (record for record in trace.records() if record.correlation_id == correlation_id), None
    )


def _fund_transfer_pair_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: FundTransferPairSettings,
    command_outcomes: list[dict[str, str | int | None]],
    reconciliations: list[dict[str, str | int | bool | None]],
    completed_pairs: int,
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-fund-transfer-pair-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "sequential paired simulated ordinary fund-transfer baseline; "
        "a credit quota pair may run independently in parallel; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": settings.pairs * 2,
        "maximum_single_transfer": _MAXIMUM_SINGLE_TRANSFER,
        "opposite_direction_requested_for_each_completed_pair": completed_pairs == settings.pairs,
        "automatic_retry": False,
        "fund_mutation": True,
        "credit_mutation": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": "ordinary",
        "requested_pairs": settings.pairs,
        "completed_pairs": completed_pairs,
        "stopped_early": completed_pairs != settings.pairs,
        "commands": command_outcomes,
        "reconciliation": reconciliations,
        "interval_seconds": settings.interval_seconds,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report
