"""Recoverable paired credit-quota baseline measurement.

Each pair performs one 1-unit quota transfer in the fixture's approved
direction and one equal transfer in the opposite direction.  The driver is
strictly sequential, waits at least one second between native calls and stops
at the first incomplete command or reconciliation.  A completed pair has no
net quota-direction change and is therefore suitable for a simulated P0
compatibility baseline; it is not a throughput workload.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable

from emt_gateway.facade import EmtTraderFacade
from emt_gateway.gateway import CallbackPipeline, CallbackRecord
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .gateway_credit_repayment import (
    CreditQuotaTransferPlan,
    CreditRepaymentError,
    _command_outcome,
    _drain_command_until_terminal,
    _invoke_credit_quota_command,
    _reconcile_credit_quota_transfer,
    select_credit_quota_transfer_plan,
)
from .gateway_mutation import _MutationCallbackIngress, _write_payload_free_trace
from .gateway_report import build_gateway_report
from .mutation_fixture import load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


@dataclass(frozen=True)
class CreditQuotaPairSettings:
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
            raise CreditRepaymentError("quota pair count must be in 1..30")
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise CreditRepaymentError("quota pair callback_timeout_seconds must be in 3..30")
        if self.interval_seconds < 1.0:
            raise CreditRepaymentError(
                "quota pair interval_seconds must stay at or above one second"
            )


def run_credit_quota_pair(settings: CreditQuotaPairSettings) -> dict[str, Any]:
    """Preflight or execute bounded equal-and-opposite quota-transfer pairs."""

    settings.validate()
    plan = select_credit_quota_transfer_plan(
        load_mutation_fixture(settings.fixture, profile="credit")
    )
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-credit-quota-pair-c0-c7",
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
                "planned_commands": ["CreditQuotaTransfer", "CreditQuotaTransfer"],
                "planned_pairs": settings.pairs,
                "maximum_commands": settings.pairs * 2,
                "interval_seconds": settings.interval_seconds,
                "reconciliation_query": "QueryCreditQuotaTransfer",
            },
        }
    return _execute_credit_quota_pairs(settings, plan)


def _execute_credit_quota_pairs(
    settings: CreditQuotaPairSettings,
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
                client_id=99,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="credit",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "quota-pair-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "quota-pair-events.emtj")
        active_source: Any | None = None

        def lookup(record: CallbackRecord) -> Any | None:
            if active_source is None or record.callback_name != "OnCreditQuotaTransfer":
                return None
            if (
                active_source.native_id is None
                or record.operation_native_id == active_source.native_id
            ):
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
                active_source = _invoke_credit_quota_command(
                    entry=entry,
                    facade=facade,
                    trace=trace,
                    journal=command_journal,
                    fund_account=settings.accounts.credit.account,
                )
                if active_source is not None:
                    _drain_command_until_terminal(
                        callbacks=callbacks,
                        trace=trace,
                        correlation_id=active_source.correlation_id,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                outcome = _command_outcome("CreditQuotaTransfer", active_source, trace)
                command_outcomes.append({"pair": pair, **outcome})
                reconciliation = _reconcile_credit_quota_transfer(
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
        return _quota_pair_report(
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


def _quota_pair_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: CreditQuotaPairSettings,
    command_outcomes: list[dict[str, str | int | None]],
    reconciliations: list[dict[str, str | int | bool | None]],
    completed_pairs: int,
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-credit-quota-pair-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "sequential paired simulated credit quota baseline; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": settings.pairs * 2,
        "maximum_single_transfer": 1.0,
        "completed_pairs_restore_net_direction": completed_pairs == settings.pairs,
        "automatic_retry": False,
        "credit_mutation": True,
        "option_mutation": False,
        "native_fastspi_required": settings.require_native_fastspi,
        "credentials_in_report": False,
        "business_payload_in_report": False,
    }
    report["run"] = {
        "profile": "credit",
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
