"""Bounded real credit-repayment measurement through the durable gateway.

The EMT credit-repayment APIs do not accept a client-supplied id, so this
driver is deliberately a one-pass, no-retry compatibility measurement rather
than a throughput workload.  Every request is first written as a hash-only
command intent, then is correlated to its returned EMT operation id and to
the corresponding SPI notification.  A payload-free repayment-history query
is issued after each completed notification.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable, Mapping
from uuid import uuid4

from emt_gateway.facade import EmtTraderFacade, SideEffectPermit
from emt_gateway.gateway import CallbackPipeline, CallbackRecord
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .gateway_mutation import (
    _MutationCallbackIngress,
    _reconcile_order,
    _write_payload_free_trace,
)
from .gateway_readonly import _drain_until_complete
from .gateway_report import build_gateway_report
from .mutation_fixture import MutationFixtureEntry, load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


class CreditRepaymentError(RuntimeError):
    """A credit repayment fixture is unsafe, incomplete, or cannot run."""


@dataclass(frozen=True)
class CreditRepaymentSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    trace_output: Path | None = None
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 1.0
    cycles: int = 1
    execute_simulated: bool = False
    require_native_fastspi: bool = True
    # Acceptance-only: retain the vendor wrapper until the CLI has persisted
    # its payload-free report and terminates the dedicated worker.  This
    # leaves normal service shutdown unchanged while avoiding a known vendor
    # finalizer instability on the Linux test binding.
    retain_native_reference: Callable[[object], None] | None = field(
        default=None, repr=False, compare=False
    )

    def validate(self) -> None:
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise CreditRepaymentError("credit callback_timeout_seconds must be in 3..30")
        if self.interval_seconds < 1.0:
            raise CreditRepaymentError(
                "credit repayment interval_seconds must stay at or above one second"
            )
        if not 1 <= self.cycles <= 30:
            raise CreditRepaymentError("credit repayment cycles must be in 1..30")


@dataclass(frozen=True)
class CreditRepaymentPlan:
    """One or two deliberately tiny, no-retry repayment requests."""

    entries: tuple[MutationFixtureEntry, ...]


@dataclass(frozen=True)
class CreditQuotaTransferSettings:
    """Settings for one fully bounded simulated two-node quota probe."""

    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    trace_output: Path | None = None
    callback_timeout_seconds: float = 8.0
    execute_simulated: bool = False
    require_native_fastspi: bool = True

    def validate(self) -> None:
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise CreditRepaymentError("quota callback_timeout_seconds must be in 3..30")


@dataclass(frozen=True)
class CreditQuotaTransferPlan:
    entry: MutationFixtureEntry


@dataclass
class _CommandSource:
    command_id: str
    correlation_id: str
    native_id: int | None = None
    native_error_id: int | None = None


_SUPPORTED = frozenset(
    {
        "CreditCashRepay",
        "CreditCashRepaySpecifiedDebt",
        "CreditSellStockRepayDebtInterestFee",
    }
)
_SPECIFIED_DEBT_FIELDS = frozenset({"request_amount", "order_date", "debt_id", "debt_type"})
_CREDIT_SELL_REPAY_FIELDS = frozenset({"order", "debt_id", "order_client_id"})
_CREDIT_SELL_ORDER_FIELDS = frozenset(
    {"ticker", "market", "side", "price_type", "price", "quantity", "business_type"}
)
_MAXIMUM_SINGLE_REPAYMENT = 1.0
_MAXIMUM_CREDIT_SELL_REPAY_QUANTITY = 100
_QUOTA_FIELDS = frozenset({"serial_id", "amount", "transfer_type"})


def select_credit_repayment_plan(entries: tuple[MutationFixtureEntry, ...]) -> CreditRepaymentPlan:
    """Accept only the reviewed, tiny credit-repayment subset.

    A direct repayment needs only an amount.  A specified-debt repayment must
    identify exactly one existing cash debt.  Both are capped at one currency
    unit, limited to one command per second and can never be retried by this
    driver.
    """

    if not entries:
        raise CreditRepaymentError("credit repayment fixture enables no commands")
    names = {entry.api_name for entry in entries}
    if len(names) != len(entries) or not names <= _SUPPORTED:
        raise CreditRepaymentError("credit repayment fixture enables an unsupported command")
    if "CreditSellStockRepayDebtInterestFee" in names and names != {
        "CreditSellStockRepayDebtInterestFee"
    }:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee must run as one isolated no-retry command"
        )
    for entry in entries:
        if entry.rate_limit_per_second != 1:
            raise CreditRepaymentError(
                "credit repayment commands require exactly one command per second"
            )
        expected_reconciliation = (
            "QueryOrderByEMTID"
            if entry.api_name == "CreditSellStockRepayDebtInterestFee"
            else "QueryCreditCashRepayInfo"
        )
        if entry.reconciliation_query != expected_reconciliation:
            raise CreditRepaymentError(
                f"{entry.api_name} reconciliation_query must be {expected_reconciliation}"
            )
        if entry.api_name == "CreditCashRepay":
            _validate_cash_repay_request(entry.request)
        elif entry.api_name == "CreditCashRepaySpecifiedDebt":
            _validate_specified_debt_request(entry.request)
        else:
            _validate_credit_sell_repay_request(entry.request)
    return CreditRepaymentPlan(entries=entries)


def select_credit_quota_transfer_plan(
    entries: tuple[MutationFixtureEntry, ...],
) -> CreditQuotaTransferPlan:
    """Accept one 1-unit credit quota transfer and its mandatory readback."""

    if len(entries) != 1 or entries[0].api_name != "CreditQuotaTransfer":
        raise CreditRepaymentError(
            "quota fixture must enable exactly one CreditQuotaTransfer command"
        )
    entry = entries[0]
    if entry.rate_limit_per_second != 1:
        raise CreditRepaymentError("quota transfer requires exactly one command per second")
    if entry.reconciliation_query != "QueryCreditQuotaTransfer":
        raise CreditRepaymentError("quota reconciliation_query must be QueryCreditQuotaTransfer")
    if set(entry.request) != _QUOTA_FIELDS:
        raise CreditRepaymentError("CreditQuotaTransfer request field set is invalid")
    if entry.request["serial_id"] != 0:
        raise CreditRepaymentError(
            "CreditQuotaTransfer serial_id must be zero before the native callback assigns it"
        )
    _validate_amount(entry.request["amount"], "CreditQuotaTransfer amount")
    if entry.request["transfer_type"] not in {0, 1}:
        raise CreditRepaymentError("CreditQuotaTransfer transfer_type must be 0 or 1")
    return CreditQuotaTransferPlan(entry=entry)


def run_credit_quota_transfer(settings: CreditQuotaTransferSettings) -> dict[str, Any]:
    """Preflight or run one no-retry simulated credit-quota operation.

    The supplied binding may return the newly allocated operation serial, but
    some simulated-node paths return zero before issuing the same callback.
    Both non-negative forms are accepted as a native dispatch; the serial is
    retained only in process memory for callback correlation and is never
    written into reports or traces.
    """

    settings.validate()
    plan = select_credit_quota_transfer_plan(
        load_mutation_fixture(settings.fixture, profile="credit")
    )
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-credit-quota-transfer-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "automatic_retry": False,
                "credit_mutation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": "credit",
                "planned_commands": [plan.entry.api_name],
                "maximum_commands": 1,
                "reconciliation_query": "QueryCreditQuotaTransfer",
            },
        }
    return _execute_credit_quota_transfer(settings, plan)


def _execute_credit_quota_transfer(
    settings: CreditQuotaTransferSettings,
    plan: CreditQuotaTransferPlan,
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
                account=settings.accounts.credit.account,
                password=settings.accounts.credit.password,
                client_id=98,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="credit",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "quota-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "quota-events.emtj")
        source: _CommandSource | None = None

        def lookup(record: CallbackRecord) -> _CommandSource | None:
            nonlocal source
            # The fixture permits exactly one action after attachment.  The
            # official quota callback carries a serial id.  A positive native
            # return must agree with that opaque id; a zero native return is
            # a documented simulated-node dispatch form and can only be
            # associated because this fixture has exactly one active action.
            if source is not None and record.callback_name == "OnCreditQuotaTransfer":
                if source.native_id is None or record.operation_native_id == source.native_id:
                    return source
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
        source = _invoke_credit_quota_command(
            entry=plan.entry,
            facade=facade,
            trace=trace,
            journal=command_journal,
            fund_account=settings.accounts.credit.account,
        )
        if source is not None:
            _drain_command_until_terminal(
                callbacks=callbacks,
                trace=trace,
                correlation_id=source.correlation_id,
                timeout_seconds=settings.callback_timeout_seconds,
            )
        outcome = _command_outcome("CreditQuotaTransfer", source, trace)
        reconciliation = _reconcile_credit_quota_transfer(
            facade=facade,
            callbacks=callbacks,
            registry=registry,
            trace=trace,
            timeout_seconds=settings.callback_timeout_seconds,
        )
        return _credit_quota_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            outcome=outcome,
            reconciliation=reconciliation,
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


def _invoke_credit_quota_command(
    *,
    entry: MutationFixtureEntry,
    facade: EmtTraderFacade,
    trace: GatewayTraceCollector,
    journal: FileJournal,
    fund_account: str,
) -> _CommandSource | None:
    command_id, correlation_id = str(uuid4()), str(uuid4())
    source = _CommandSource(command_id, correlation_id)
    canonical = json.dumps(entry.request, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    payload_hash = sha256(canonical.encode("utf-8")).hexdigest()
    trace.begin(
        correlation_id,
        kind="api",
        interface="CreditQuotaTransfer",
        profile="credit",
        workload="steady",
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
        trace.complete(correlation_id, "failed", "quota_command_journal_failure")
        return None
    # fund_account is injected solely from the separately ignored account
    # configuration.  It is never present in the fixture, journal, trace or
    # report.
    request = {**entry.request, "fund_account": fund_account}
    try:
        invocation = facade.invoke(
            "CreditQuotaTransfer",
            business_args=(request,),
            side_effect_permit=SideEffectPermit.simulated_fixture(),
            profile="credit",
            workload="steady",
        )
        native_result = int(invocation.return_value)
    except Exception as exc:
        trace.mark(correlation_id, "C4")
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", type(exc).__name__)
        return None
    trace.mark(correlation_id, "C4")
    if native_result < 0:
        _append_best_effort(
            journal, "LOCAL_SEND_REJECTED", command_id, correlation_id, payload_hash
        )
        trace.complete(correlation_id, "rejected", "native_negative_return")
        return None
    try:
        journal.append(
            kind="NATIVE_ACCEPTED" if native_result > 0 else "NATIVE_DISPATCHED",
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
    except Exception:
        _append_best_effort(journal, "SUBMIT_UNKNOWN", command_id, correlation_id, payload_hash)
        trace.complete(correlation_id, "failed", "quota_dispatched_journal_failure")
        return None
    if native_result > 0:
        source.native_id = native_result
    return source


def _reconcile_credit_quota_transfer(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    timeout_seconds: float,
) -> dict[str, str | int | bool | None]:
    try:
        handle = facade.invoke_query(
            "QueryCreditQuotaTransfer",
            business_args=_quota_reconciliation_args(),
            profile="credit",
            workload="steady",
            timeout_ns=int(timeout_seconds * 1_000_000_000),
        )
    except Exception as exc:
        return {
            "api": "QueryCreditQuotaTransfer",
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
        "api": "QueryCreditQuotaTransfer",
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "terminal_seen": observation.terminal_seen if observation is not None else False,
        "callback_count": observation.callback_count if observation is not None else 0,
        "item_count": observation.item_count if observation is not None else 0,
    }


def _quota_reconciliation_args() -> tuple[dict[str, int], ...]:
    """Match the binding's documented quota-transfer filter structure."""

    return ({"serial_id": 0},)


def _credit_quota_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: CreditQuotaTransferSettings,
    outcome: dict[str, str | int | None],
    reconciliation: dict[str, str | int | bool | None],
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-credit-quota-transfer-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "one-pass simulated two-node quota compatibility; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": 1,
        "maximum_transfer_amount": _MAXIMUM_SINGLE_REPAYMENT,
        "automatic_retry": False,
        "credit_mutation": True,
        "option_mutation": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": "credit",
        "planned_commands": ["CreditQuotaTransfer"],
        "completed_commands": 1,
        "stopped_early": outcome["outcome"] != "ok" or reconciliation["outcome"] != "ok",
        "command": outcome,
        "reconciliation": reconciliation,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def run_credit_repayment(settings: CreditRepaymentSettings) -> dict[str, Any]:
    """Preflight or run the one-pass simulated credit-repayment fixture."""

    settings.validate()
    plan = select_credit_repayment_plan(load_mutation_fixture(settings.fixture, profile="credit"))
    if settings.cycles > 1 and tuple(entry.api_name for entry in plan.entries) != (
        "CreditCashRepay",
    ):
        raise CreditRepaymentError("credit repayment cycles above one require only CreditCashRepay")
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-credit-repayment-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "automatic_retry": False,
                "credit_mutation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": "credit",
                "planned_commands": [entry.api_name for entry in plan.entries],
                "planned_cycles": settings.cycles,
                "maximum_commands": len(plan.entries) * settings.cycles,
                "interval_seconds": settings.interval_seconds,
                "reconciliation_queries": {
                    entry.api_name: entry.reconciliation_query for entry in plan.entries
                },
            },
        }
    return _execute_credit_repayments(settings, plan)


def _execute_credit_repayments(
    settings: CreditRepaymentSettings,
    plan: CreditRepaymentPlan,
) -> dict[str, Any]:
    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    ingress = DeferredCallbackIngress()
    command_sources: dict[int, _CommandSource] = {}
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
                account=settings.accounts.credit.account,
                password=settings.accounts.credit.password,
                client_id=97,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="credit",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "credit-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "credit-events.emtj")
        callbacks = CallbackPipeline(
            journal=event_journal,
            trace=trace,
            lookup_source=lambda record: _lookup_source(command_sources, record),
            capacity=1024,
            lifecycle_observer=registry,
            require_native_fastspi=settings.require_native_fastspi,
        )
        ingress.attach(_MutationCallbackIngress(commands=callbacks, queries=callbacks))
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )
        command_outcomes: list[dict[str, str | int | None]] = []
        reconciliations: list[dict[str, str | int | bool | None]] = []
        first_command = True
        stop = False
        for cycle in range(1, settings.cycles + 1):
            for entry in plan.entries:
                if not first_command:
                    sleep(settings.interval_seconds)
                first_command = False
                outcome, source = _invoke_credit_command(
                    entry=entry,
                    facade=facade,
                    trace=trace,
                    journal=command_journal,
                )
                if source is not None and source.native_id is not None:
                    command_sources[source.native_id] = source
                    _drain_command_until_terminal(
                        callbacks=callbacks,
                        trace=trace,
                        correlation_id=source.correlation_id,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    outcome = _command_outcome(entry.api_name, source, trace)
                command_outcomes.append({"cycle": cycle, **outcome})
                if entry.api_name == "CreditSellStockRepayDebtInterestFee":
                    reconciliation = _reconcile_credit_sell_repay(
                        cycle=cycle,
                        source=source,
                        facade=facade,
                        callbacks=callbacks,
                        registry=registry,
                        trace=trace,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                else:
                    reconciliation = _reconcile_cash_repayment(
                        facade=facade,
                        callbacks=callbacks,
                        registry=registry,
                        trace=trace,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                reconciliations.append({"cycle": cycle, **reconciliation})
                # A callback timeout after C3/C4 is a submit-unknown outcome,
                # not an excuse to skip mandated read-only reconciliation.
                # The query cannot authorize a retry, so the batch stops.
                if (
                    outcome["outcome"] != "ok"
                    or reconciliation["outcome"] != "ok"
                    or not reconciliation["terminal_seen"]
                ):
                    stop = True
                    break
            if stop:
                break
        return _credit_repayment_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            plan=plan,
            command_outcomes=command_outcomes,
            reconciliations=reconciliations,
        )
    finally:
        try:
            _release_or_retain_credit_session(session, settings.retain_native_reference)
        finally:
            for journal in (event_journal, command_journal):
                if journal is not None:
                    journal.close()


def _invoke_credit_command(
    *,
    entry: MutationFixtureEntry,
    facade: EmtTraderFacade,
    trace: GatewayTraceCollector,
    journal: FileJournal,
) -> tuple[dict[str, str | int | None], _CommandSource | None]:
    command_id = str(uuid4())
    correlation_id = str(uuid4())
    source = _CommandSource(command_id, correlation_id)
    canonical = json.dumps(entry.request, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    payload_hash = sha256(canonical.encode("utf-8")).hexdigest()
    trace.begin(
        correlation_id, kind="api", interface=entry.api_name, profile="credit", workload="steady"
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
        trace.complete(correlation_id, "failed", "credit_command_journal_failure")
        return _command_outcome(entry.api_name, source, trace), None
    try:
        invocation = facade.invoke(
            entry.api_name,
            business_args=_business_args_for(entry),
            side_effect_permit=SideEffectPermit.simulated_fixture(),
            profile="credit",
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
        source.native_error_id = _native_last_error_id(facade)
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


def _business_args_for(entry: MutationFixtureEntry) -> tuple[Any, ...]:
    if entry.api_name == "CreditCashRepay":
        return (float(entry.request["amount"]),)
    if entry.api_name == "CreditSellStockRepayDebtInterestFee":
        # The supplied EMT structure requires a user-defined order_client_id.
        # Its reserved acceptance identity lives only in the ignored private
        # fixture, so a supervisor can query EMT by that identifier after a
        # process loss.  It is never copied to a report, trace or Journal
        # payload; the durable frame retains only the complete payload hash.
        return (
            {
                **dict(entry.request["order"]),
                "order_client_id": int(entry.request["order_client_id"]),
            },
            str(entry.request["debt_id"]),
        )
    return (dict(entry.request),)


def _lookup_source(
    sources: Mapping[int, _CommandSource], record: CallbackRecord
) -> _CommandSource | None:
    if record.operation_native_id is not None:
        source = sources.get(record.operation_native_id)
        if source is not None:
            return source
    if record.order_emt_id is not None:
        return sources.get(record.order_emt_id)
    return None


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
            trace.complete(correlation_id, "failed", "credit_callback_pipeline_disarmed")
            return
        if monotonic() >= deadline:
            trace.complete(correlation_id, "timeout", "credit_callback_timeout")
            return
        sleep(0.001)


def _reconcile_cash_repayment(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    timeout_seconds: float,
) -> dict[str, str | int | bool | None]:
    handle = facade.invoke_query(
        "QueryCreditCashRepayInfo",
        profile="credit",
        workload="steady",
        timeout_ns=int(timeout_seconds * 1_000_000_000),
    )
    _drain_until_complete(
        callbacks=callbacks,
        registry=registry,
        timeout_seconds=timeout_seconds,
    )
    record = _trace_record(trace, handle.correlation_id)
    observation = registry.semantic_observation(handle.correlation_id)
    return {
        "api": "QueryCreditCashRepayInfo",
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
        "terminal_seen": observation.terminal_seen if observation is not None else False,
        "callback_count": observation.callback_count if observation is not None else 0,
        "item_count": observation.item_count if observation is not None else 0,
    }


def _reconcile_credit_sell_repay(
    *,
    cycle: int,
    source: _CommandSource | None,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    timeout_seconds: float,
) -> dict[str, str | int | None]:
    """Perform the required order lookup without inventing a native id.

    A local reject or unknown dispatch has no safe ``order_emt_id`` to query.
    The report must retain that missing reconciliation as a structured failure
    rather than letting the acceptance process throw before it writes evidence.
    """

    if source is None or source.native_id is None:
        return {
            "cycle": cycle,
            "api": "QueryOrderByEMTID",
            "outcome": "not_started",
            "reason": "native_order_id_unavailable",
        }
    return _reconcile_order(
        cycle=cycle,
        facade=facade,
        callbacks=callbacks,
        registry=registry,
        trace=trace,
        profile="credit",
        order_emt_id=source.native_id,
        timeout_seconds=timeout_seconds,
    )


def _command_outcome(
    api_name: str,
    source: _CommandSource | None,
    trace: GatewayTraceCollector,
) -> dict[str, str | int | None]:
    correlation_id = source.correlation_id if source is not None else None
    record = _trace_record(trace, correlation_id) if correlation_id is not None else None
    outcome: dict[str, str | int | None] = {
        "api": api_name,
        "outcome": record.outcome if record is not None else "pending_callback",
        "reason": record.reason if record is not None else None,
    }
    if source is not None:
        outcome["native_error_id"] = source.native_error_id
    return outcome


def _native_last_error_id(facade: EmtTraderFacade) -> int | None:
    """Read the SDK's immediate rejection code without copying vendor text.

    EMT returns zero for a definite local rejection.  Capturing only the
    integer error id makes that distinction actionable in a payload-free
    acceptance report, while a missing or malformed response remains an
    explicit ``None`` rather than a guessed error category.
    """

    try:
        value = facade.invoke("GetApiLastError").return_value
    except Exception:
        return None
    if not isinstance(value, Mapping):
        return None
    error_id = value.get("error_id")
    return error_id if type(error_id) is int else None


def _trace_record(trace: GatewayTraceCollector, correlation_id: str | None) -> Any | None:
    if correlation_id is None:
        return None
    return next((item for item in trace.records() if item.correlation_id == correlation_id), None)


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
        return None


def _release_or_retain_credit_session(
    session: EmtSession | None,
    retain_native_reference: Callable[[object], None] | None,
) -> None:
    """Close normal sessions; retain the SDK wrapper only in the isolated worker.

    The reviewed one-shot CLI worker exits only after it has persisted its
    payload-free report and trace.  The Linux vendor wrapper has an observed
    finalizer failure mode, so acceptance isolation retains it until that
    process exits instead of entering ``logout/release`` while evidence is
    still at risk.  Normal service and non-isolated paths retain the ordinary
    close lifecycle.
    """

    if session is None:
        return
    if retain_native_reference is not None:
        retain_native_reference(session)
    else:
        session.close()


def _credit_repayment_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: CreditRepaymentSettings,
    plan: CreditRepaymentPlan,
    command_outcomes: list[dict[str, str | int | None]],
    reconciliations: list[dict[str, str | int | bool | None]],
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-credit-repayment-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "bounded sequential simulated credit repayment compatibility; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": len(plan.entries) * settings.cycles,
        "maximum_single_repayment": _MAXIMUM_SINGLE_REPAYMENT,
        "maximum_credit_sell_repay_quantity": _MAXIMUM_CREDIT_SELL_REPAY_QUANTITY,
        "automatic_retry": False,
        "manual_reconciliation_required_after_unknown": True,
        "credit_mutation": True,
        "option_mutation": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": "credit",
        "planned_commands": [entry.api_name for entry in plan.entries],
        "planned_cycles": settings.cycles,
        "completed_commands": len(command_outcomes),
        "stopped_early": len(command_outcomes) != len(plan.entries) * settings.cycles,
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


def _validate_cash_repay_request(request: Mapping[str, Any]) -> None:
    if set(request) != {"amount"}:
        raise CreditRepaymentError("CreditCashRepay request must contain only amount")
    _validate_amount(request["amount"], "CreditCashRepay amount")


def _validate_specified_debt_request(request: Mapping[str, Any]) -> None:
    if set(request) != _SPECIFIED_DEBT_FIELDS:
        raise CreditRepaymentError("CreditCashRepaySpecifiedDebt request field set is invalid")
    _validate_amount(request["request_amount"], "CreditCashRepaySpecifiedDebt request_amount")
    order_date = request["order_date"]
    if type(order_date) is not int or not 20_000_000 <= order_date <= 29_991_231:
        raise CreditRepaymentError("CreditCashRepaySpecifiedDebt order_date must be YYYYMMDD")
    debt_id = request["debt_id"]
    if not isinstance(debt_id, str) or not 1 <= len(debt_id) <= 64:
        raise CreditRepaymentError("CreditCashRepaySpecifiedDebt debt_id is invalid")
    if request["debt_type"] not in {0, 1}:
        raise CreditRepaymentError("CreditCashRepaySpecifiedDebt debt_type must be 0 or 1")


def _validate_credit_sell_repay_request(request: Mapping[str, Any]) -> None:
    """Validate the only non-resting credit sell-repay acceptance envelope.

    EMT requires a normal ``EMTOrderInsertInfo`` plus a specified debt id for
    this API.  The fixture carries a reserved, recovery-queryable client id,
    but never account identity; the latter remains in the separately ignored
    session configuration.
    """

    if set(request) != _CREDIT_SELL_REPAY_FIELDS:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee request must contain only order, debt_id and order_client_id"
        )
    debt_id = request["debt_id"]
    if not isinstance(debt_id, str) or not 1 <= len(debt_id) <= 64 or not debt_id.isascii():
        raise CreditRepaymentError("CreditSellStockRepayDebtInterestFee debt_id is invalid")
    order_client_id = request["order_client_id"]
    if type(order_client_id) is not int or not 1 <= order_client_id <= 2**31 - 1:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee order_client_id is invalid"
        )
    order = request["order"]
    if not isinstance(order, Mapping) or set(order) != _CREDIT_SELL_ORDER_FIELDS:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee order field set is invalid"
        )
    ticker = order["ticker"]
    if not isinstance(ticker, str) or len(ticker) != 6 or not ticker.isdecimal():
        raise CreditRepaymentError("CreditSellStockRepayDebtInterestFee ticker is invalid")
    market = order["market"]
    if market not in {1, 2}:
        raise CreditRepaymentError("CreditSellStockRepayDebtInterestFee market is invalid")
    # EMT documents side 23 as sell-stock-to-repay-margin and business type 4
    # as the financing/margin domain.  The price type is non-resting: best-five
    # cancel for Shanghai, best-or-cancel for Shenzhen.
    if order["side"] != 23 or order["business_type"] != 4:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee requires side 23 and business_type 4"
        )
    expected_price_type = 4 if market == 1 else 2
    if order["price_type"] != expected_price_type or order["price"] != 0:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee requires the market-specific non-resting price type and price 0"
        )
    if order["quantity"] != _MAXIMUM_CREDIT_SELL_REPAY_QUANTITY:
        raise CreditRepaymentError(
            "CreditSellStockRepayDebtInterestFee requires exactly one 100-share lot"
        )


def _validate_amount(value: object, label: str) -> None:
    if type(value) not in {int, float} or not math.isfinite(float(value)):
        raise CreditRepaymentError(f"{label} must be a finite number")
    if not 0 < float(value) <= _MAXIMUM_SINGLE_REPAYMENT:
        raise CreditRepaymentError(f"{label} must be in (0, {_MAXIMUM_SINGLE_REPAYMENT}]")
