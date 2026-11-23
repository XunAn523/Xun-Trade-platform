"""Controlled simulated marketable fill callback measurement.

This driver exists specifically to collect real ``OnTradeEvent`` and
``OnHoldingChangeEvent`` evidence from the normal and credit simulated
accounts.  It permits only a bounded cash-equity buy or sell request using the
market-specific EMT immediate-or-cancel price type.  Therefore any unfilled
remainder is cancelled by the venue rather than being left as an open test
order.  Requests are
strictly sequential, have a one-second minimum spacing, and stop on the first
missing owned-trade callback.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from threading import Event, Lock
from time import monotonic, sleep
from typing import Any, Callable, Mapping

from emt_gateway.gateway import CallbackRecord, CommandState, OrderGateway, OrderIntent
from emt_gateway.journal import FileJournal
from emt_gateway.native_api import EmtOrderAdapter
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.session import EmtSession, SessionSettings
from emt_gateway.spi_bridge import DeferredCallbackIngress

from .gateway_mutation import (
    GatewayMutationError,
    _CallbackDrainer,
    _command_outcome,
    _MutationCallbackIngress,
    _new_native_client_id,
    _trace_is_not_ok,
    _write_payload_free_trace,
)
from .gateway_report import build_gateway_report
from .mutation_fixture import MutationFixtureEntry, load_mutation_fixture
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector

_TRADE_FILL_FIELDS = frozenset(
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
_CASH_TICKER = re.compile(r"[0-9]{6}\Z")
_SHANGHAI_BEST5_OR_CANCEL = 4
_SHENZHEN_BEST_OR_CANCEL = 2
_MAXIMUM_QUANTITY = 100


@dataclass(frozen=True)
class TradeFillSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture: Path
    profile: str
    cycles: int = 100
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 1.0
    trace_output: Path | None = None
    execute_simulated: bool = False
    require_native_fastspi: bool = True
    # Restricted to a throw-away acceptance worker: see the matching CLI
    # --isolate-after-report path.  Normal service shutdown still closes the
    # vendor session in-process.
    retain_native_reference: Callable[[object], None] | None = field(
        default=None, repr=False, compare=False
    )

    def validate(self) -> None:
        if self.profile not in {"ordinary", "credit"}:
            raise GatewayMutationError(
                "trade-fill measurement supports ordinary or credit accounts only"
            )
        if not 1 <= self.cycles <= 100:
            raise GatewayMutationError("trade-fill cycles must be in 1..100")
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise GatewayMutationError("trade-fill callback_timeout_seconds must be in 3..30")
        if self.interval_seconds < 1.0:
            raise GatewayMutationError(
                "trade-fill interval_seconds must stay at or above one second"
            )


@dataclass(frozen=True)
class TradeFillPlan:
    """One opaque simulated best-or-cancel cash-equity order request."""

    order_request: dict[str, Any]
    profile: str


class _OwnedTradeObserver:
    """Retain only owned native ids long enough to prove their callbacks.

    The observer never serializes an id, callback payload, or business field.
    It treats a callback as evidence only when its opaque order id was returned
    by this process's own InsertOrder call.
    """

    def __init__(self) -> None:
        self._owned: set[int] = set()
        self._trade_seen: set[int] = set()
        self._lock = Lock()
        self._changed = Event()

    def own(self, order_emt_id: int) -> None:
        with self._lock:
            self._owned.add(order_emt_id)
            if order_emt_id in self._trade_seen:
                self._changed.set()

    def wait_for_trade(self, order_emt_id: int, timeout_seconds: float) -> bool:
        deadline = monotonic() + timeout_seconds
        while True:
            with self._lock:
                if order_emt_id in self._trade_seen:
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
        if record.callback_name != "OnTradeEvent" or record.order_emt_id is None:
            return
        with self._lock:
            self._trade_seen.add(record.order_emt_id)
            if record.order_emt_id in self._owned:
                self._changed.set()


def select_trade_fill_plan(
    entries: tuple[MutationFixtureEntry, ...],
    *,
    profile: str,
) -> TradeFillPlan:
    """Fail closed unless exactly one small immediate cash order is enabled."""

    if len(entries) != 1 or entries[0].api_name != "InsertOrder":
        raise GatewayMutationError("trade-fill fixture must enable exactly one InsertOrder command")
    entry = entries[0]
    if entry.rate_limit_per_second != 1:
        raise GatewayMutationError("trade-fill InsertOrder requires exactly one command per second")
    if entry.reconciliation_query != "OnTradeEvent":
        raise GatewayMutationError(
            "trade-fill InsertOrder reconciliation_query must be OnTradeEvent"
        )
    return TradeFillPlan(
        order_request=_validated_best_or_cancel_order(entry.request, profile=profile),
        profile=profile,
    )


def run_gateway_trade_fill(settings: TradeFillSettings) -> dict[str, Any]:
    """Preflight or execute sequential real simulated fill samples."""

    settings.validate()
    plan = select_trade_fill_plan(
        load_mutation_fixture(settings.fixture, profile=settings.profile),
        profile=settings.profile,
    )
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-trade-fill-c0-c7",
            "executed": False,
            "safety": {
                "mode": "preflight-only",
                "sdk_loaded": False,
                "login": False,
                "order_entry": False,
                "automatic_retry": False,
                "credentials_in_report": False,
                "business_payload_in_report": False,
            },
            "run": {
                "profile": plan.profile,
                "planned_commands": ["InsertOrder"],
                "planned_cycles": settings.cycles,
                "maximum_commands": settings.cycles,
                "interval_seconds": settings.interval_seconds,
                "required_callback": "OnTradeEvent",
            },
        }
    return _execute_trade_fill(settings, plan)


def _execute_trade_fill(settings: TradeFillSettings, plan: TradeFillPlan) -> dict[str, Any]:
    trace = GatewayTraceCollector()
    ingress = DeferredCallbackIngress()
    session: EmtSession | None = None
    command_journal: FileJournal | None = None
    event_journal: FileJournal | None = None
    drainer: _CallbackDrainer | None = None
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
                client_id={"ordinary": 104, "credit": 105}[settings.profile],
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile=settings.profile,
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        command_journal = FileJournal(settings.runtime_dir / "trade-fill-commands.emtj")
        event_journal = FileJournal(settings.runtime_dir / "trade-fill-events.emtj")
        native_client_ids: dict[str, int] = {}

        def make_native_order_request(command_id: str, client_order_id: str) -> dict[str, Any]:
            del command_id
            try:
                native_client_id = native_client_ids[client_order_id]
            except KeyError as exc:
                raise GatewayMutationError(
                    "native trade-fill request has no owned client id"
                ) from exc
            return {**plan.order_request, "order_client_id": native_client_id}

        observer = _OwnedTradeObserver()
        gateway = OrderGateway(
            native=EmtOrderAdapter(
                session.api,
                session_id=session.session_id,
                order_request_factory=make_native_order_request,
            ),
            command_journal=command_journal,
            event_journal=event_journal,
            trace=trace,
            callback_capacity=1024,
            callback_lifecycle_observer=observer,
            require_native_fastspi=settings.require_native_fastspi,
        )
        ingress.attach(
            _MutationCallbackIngress(commands=gateway.callbacks, queries=gateway.callbacks)
        )
        drainer = _CallbackDrainer(gateway.callbacks)
        drainer.start()

        outcomes: list[dict[str, str | int | bool | None]] = []
        completed_cycles = 0
        last_started_at: float | None = None
        for cycle in range(1, settings.cycles + 1):
            if last_started_at is not None:
                remaining = settings.interval_seconds - (monotonic() - last_started_at)
                if remaining > 0:
                    sleep(remaining)
            native_client_id = _new_native_client_id(set(native_client_ids.values()))
            client_order_id = f"perf-fill-{native_client_id}"
            native_client_ids[client_order_id] = int(native_client_id)
            canonical_order = json.dumps(
                plan.order_request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            payload_hash = sha256(
                f"{canonical_order}:{native_client_id}".encode("utf-8")
            ).hexdigest()
            last_started_at = monotonic()
            submitted = gateway.submit_order(
                OrderIntent(
                    client_order_id=client_order_id,
                    payload_hash=payload_hash,
                    profile=settings.profile,
                    workload="steady",
                )
            )
            command = _command_outcome(
                cycle,
                "InsertOrder",
                submitted,
                trace,
                drainer,
                settings.callback_timeout_seconds,
            )
            trade_seen = False
            if submitted.state is CommandState.NATIVE_ACCEPTED and submitted.native_id is not None:
                observer.own(submitted.native_id)
                trade_seen = observer.wait_for_trade(
                    submitted.native_id, settings.callback_timeout_seconds
                )
            outcomes.append(
                {
                    "cycle": cycle,
                    "gateway_state": command["gateway_state"],
                    "command_outcome": command["outcome"],
                    "trade_callback_seen": trade_seen,
                }
            )
            if (
                submitted.state is not CommandState.NATIVE_ACCEPTED
                or _trace_is_not_ok(trace, submitted.correlation_id)
                or not trade_seen
            ):
                break
            completed_cycles = cycle
        return _trade_fill_report(
            trace=trace,
            snapshot=snapshot,
            settings=settings,
            outcomes=outcomes,
            completed_cycles=completed_cycles,
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
                for journal in (event_journal, command_journal):
                    if journal is not None:
                        journal.close()


def _trade_fill_report(
    *,
    trace: GatewayTraceCollector,
    snapshot: Any,
    settings: TradeFillSettings,
    outcomes: list[dict[str, str | int | bool | None]],
    completed_cycles: int,
) -> dict[str, Any]:
    events = trace.export_events()
    report = build_gateway_report(events)
    report["driver"] = "gateway-trade-fill-c0-c7"
    report["executed"] = True
    report["measurement_scope"] = (
        "sequential simulated best-or-cancel cash-order callback baseline; not a throughput workload"
    )
    report["safety"] = {
        "maximum_commands": settings.cycles,
        "maximum_quantity_per_order": _MAXIMUM_QUANTITY,
        "price_type": "market-specific-best-or-cancel",
        "marketable_side": "buy_or_sell",
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
        "interval_seconds": settings.interval_seconds,
        "required_callback": "OnTradeEvent",
        "commands": outcomes,
        "api_version": snapshot.api_version,
        "trading_day": snapshot.trading_day,
    }
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        report["trace_artifact"] = settings.trace_output.name
    return report


def _validated_best_or_cancel_order(request: Mapping[str, Any], *, profile: str) -> dict[str, Any]:
    """Accept only the non-resting simulated cash-equity envelope for this runner."""

    if profile not in {"ordinary", "credit"}:
        raise GatewayMutationError("trade-fill profile must be ordinary or credit")
    if set(request) != _TRADE_FILL_FIELDS:
        raise GatewayMutationError(
            "trade-fill InsertOrder must use the exact bounded cash-order field set"
        )
    if not isinstance(request.get("ticker"), str) or not _CASH_TICKER.fullmatch(request["ticker"]):
        raise GatewayMutationError(
            "trade-fill InsertOrder ticker must be a six-digit cash-equity code"
        )
    if request.get("market") not in {1, 2}:
        raise GatewayMutationError("trade-fill InsertOrder market is invalid")
    if request.get("side") not in {1, 2}:
        raise GatewayMutationError("trade-fill InsertOrder permits only cash buy or sell side")
    expected_price_type = (
        _SHANGHAI_BEST5_OR_CANCEL if request["market"] == 1 else _SHENZHEN_BEST_OR_CANCEL
    )
    if request.get("price_type") != expected_price_type:
        raise GatewayMutationError(
            "trade-fill InsertOrder requires the market-specific non-resting price type"
        )
    price = request.get("price")
    if type(price) not in {int, float} or float(price) != 0.0:
        raise GatewayMutationError("trade-fill InsertOrder requires price 0 for non-resting mode")
    if request.get("quantity") != _MAXIMUM_QUANTITY:
        raise GatewayMutationError("trade-fill InsertOrder requires exactly 100 shares")
    if request.get("business_type") != 0:
        raise GatewayMutationError("trade-fill InsertOrder requires cash-equity business_type 0")
    return dict(request)
