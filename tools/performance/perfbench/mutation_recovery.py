"""Fail-closed cleanup for unfinished orders created on the simulated account.

This is a recovery control, not a performance workload.  It queries current
unfinished ordinary-account orders through the real SDK, retains only native
order IDs in process memory, and cancels them only after a terminal query
callback proves the query completed.  Reports contain counts, never IDs or
business payloads.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from uuid import uuid4

from emt_gateway.facade import EmtTraderFacade
from emt_gateway.gateway import CallbackLifecycleObserver, CallbackPipeline, CallbackRecord
from emt_gateway.journal import FileJournal
from emt_gateway.native_api import EmtOrderAdapter
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry
from emt_gateway.session import EmtSession, SessionSettings

from .gateway_report import build_gateway_report
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


class MutationRecoveryError(RuntimeError):
    """The simulated-order recovery control cannot run safely."""


@dataclass(frozen=True)
class MutationRecoverySettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    trace_output: Path | None = None
    report_output: Path | None = None
    callback_timeout_seconds: float = 8.0
    cancellation_interval_seconds: float = 0.02
    cancel_unfinished_orders: bool = True
    execute_simulated: bool = False
    require_native_fastspi: bool = True
    # Some vendor Python bindings may stall in their native finalizer after a
    # read-only report has been durably written.  This option is deliberately
    # limited to the external one-shot driver: it exits only after the query
    # evidence is persisted and never changes the query/cancellation policy.
    isolate_after_report: bool = False

    def validate(self) -> None:
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise MutationRecoveryError("callback_timeout_seconds must be in 3..30")
        if not 0.005 <= self.cancellation_interval_seconds <= 1.0:
            raise MutationRecoveryError("cancellation_interval_seconds must be in .005..1")


class _OutstandingOrderIds(CallbackLifecycleObserver):
    """Read only native order IDs already minimized by the SPI bridge."""

    def __init__(self) -> None:
        self._ids: set[int] = set()

    def on_enqueued(self, record: CallbackRecord) -> None:
        if record.callback_name == "OnQueryOrder" and record.order_emt_id is not None:
            self._ids.add(record.order_emt_id)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        return None

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        return None

    def ids(self) -> tuple[int, ...]:
        return tuple(sorted(self._ids))


class _LifecycleFanout(CallbackLifecycleObserver):
    def __init__(self, *observers: CallbackLifecycleObserver) -> None:
        self._observers = observers

    def on_enqueued(self, record: CallbackRecord) -> None:
        for observer in self._observers:
            observer.on_enqueued(record)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        for observer in self._observers:
            observer.on_durable(record, durable_ns)

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        for observer in self._observers:
            observer.on_ack(record, acknowledged_ns)


def run_mutation_recovery(settings: MutationRecoverySettings) -> dict[str, Any]:
    """Query unfinished orders, then optionally cancel them after full proof."""

    settings.validate()
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-mutation-recovery",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "query": False,
                "cancellation": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
        }
    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    outstanding = _OutstandingOrderIds()
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    journal = FileJournal(settings.runtime_dir / f"mutation-recovery-{uuid4().hex}.emtj")
    callbacks = CallbackPipeline(
        journal=journal,
        trace=trace,
        lookup_source=lambda record: None,
        capacity=1024,
        lifecycle_observer=_LifecycleFanout(registry, outstanding),
        require_native_fastspi=settings.require_native_fastspi,
    )
    session: EmtSession | None = None
    try:
        loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
        session = EmtSession.from_loaded_sdk(
            loaded,
            callback_ingress=callbacks,
            settings=SessionSettings(
                host=settings.accounts.host,
                port=settings.accounts.port,
                account=settings.accounts.ordinary.account,
                password=settings.accounts.ordinary.password,
                client_id=96,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="ordinary",
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        facade = EmtTraderFacade(
            session.api, session_id=session.session_id, query_registry=registry, trace=trace
        )
        handle = facade.invoke_query(
            "QueryUnfinishedOrders",
            profile="ordinary",
            workload="steady",
            timeout_ns=int(settings.callback_timeout_seconds * 1_000_000_000),
        )
        _drain_until_complete(callbacks, registry, settings.callback_timeout_seconds)
        query_record = _trace_record(trace, handle.correlation_id)
        query_complete = query_record is not None and query_record.outcome == "ok"
        candidates = outstanding.ids()
        cancellation_attempts = cancellation_accepts = 0
        if query_complete and settings.cancel_unfinished_orders:
            adapter = EmtOrderAdapter(
                session.api,
                session_id=session.session_id,
                order_request_factory=lambda command_id, client_order_id: {},
            )
            for index, order_emt_id in enumerate(candidates):
                cancellation_attempts += 1
                if (
                    adapter.cancel_order(
                        command_id=f"recovery-{order_emt_id}", order_emt_id=order_emt_id
                    )
                    != 0
                ):
                    cancellation_accepts += 1
                if index + 1 < len(candidates):
                    # Recovery never retries a native cancellation. Pace one
                    # attempt per order so cleanup cannot trigger counter
                    # cancel-rate control itself.
                    sleep(settings.cancellation_interval_seconds)
            _drain_for(callbacks, 2.0)
        report = _recovery_report(
            trace=trace,
            settings=settings,
            snapshot=snapshot,
            query_complete=query_complete,
            outstanding_count=len(candidates),
            cancellation_attempts=cancellation_attempts,
            cancellation_accepts=cancellation_accepts,
        )
        if settings.report_output is not None:
            settings.report_output.parent.mkdir(parents=True, exist_ok=True)
            settings.report_output.write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        if settings.isolate_after_report:
            # The report and payload-free trace were closed before this point.
            # Avoid invoking an unbounded vendor finalizer in a subprocess
            # whose sole purpose is the already-complete recovery proof.
            os._exit(0)
        return report
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            journal.close()


def _drain_until_complete(
    callbacks: CallbackPipeline, registry: QueryRegistry, timeout_seconds: float
) -> None:
    deadline = monotonic() + timeout_seconds
    while registry.active_count:
        delivery = callbacks.normalize_one()
        if delivery is not None:
            callbacks.ack(delivery.delivery_id)
            continue
        if monotonic() >= deadline:
            registry.expire(force=True)
            return
        sleep(0.001)


def _drain_for(callbacks: CallbackPipeline, duration_seconds: float) -> None:
    deadline = monotonic() + duration_seconds
    while monotonic() < deadline:
        delivery = callbacks.normalize_one()
        if delivery is not None:
            callbacks.ack(delivery.delivery_id)
            continue
        sleep(0.001)


def _trace_record(trace: GatewayTraceCollector, correlation_id: str) -> Any | None:
    return next(
        (record for record in trace.records() if record.correlation_id == correlation_id), None
    )


def _recovery_report(
    *,
    trace: GatewayTraceCollector,
    settings: MutationRecoverySettings,
    snapshot: Any,
    query_complete: bool,
    outstanding_count: int,
    cancellation_attempts: int,
    cancellation_accepts: int,
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-mutation-recovery"
    report["executed"] = True
    report["measurement_scope"] = (
        "simulated-account recovery control; not an EMT throughput workload"
    )
    report["safety"] = {
        "query": "QueryUnfinishedOrders only",
        "cancellation": (
            "only after a complete terminal query callback"
            if settings.cancel_unfinished_orders
            else "disabled: query-only verification"
        ),
        "order_entry": False,
        "fund_or_credit_mutation": False,
        "credentials_in_report": False,
        "business_payload_in_report": False,
        "native_fastspi_required": settings.require_native_fastspi,
    }
    report["run"] = {
        "query_complete": query_complete,
        "outstanding_order_count": outstanding_count,
        "cancellation_attempts": cancellation_attempts,
        "cancellation_accepts": cancellation_accepts,
        "cancellation_interval_ms": settings.cancellation_interval_seconds * 1_000,
        "cancellation_requested": settings.cancel_unfinished_orders,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _write_payload_free_trace(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = "\n".join(json.dumps(event, ensure_ascii=True, sort_keys=True) for event in events)
    path.write_text(f"{serialized}\n" if serialized else "", encoding="utf-8")
