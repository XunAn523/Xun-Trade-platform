"""Low-rate real EMT query driver using the gateway's complete C0--C7 path.

This driver is intentionally limited to state-reading queries. It uses the
real cross-platform loader, session, facade, SPI bridge, native fastspi ring,
event journal and ACK path, but it cannot place/cancel orders or mutate funds.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from time import monotonic, sleep
from typing import Any

from emt_gateway.facade import EmtTraderFacade
from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.query_registry import QueryRegistry, QuerySemanticObservation
from emt_gateway.session import EmtSession, SessionSettings

from .gateway_report import build_gateway_report
from .simulated_accounts import SimulatedAccounts
from .trace import GatewayTraceCollector


class GatewayReadOnlyError(RuntimeError):
    """The safe real-query driver cannot construct or complete a run."""


@dataclass(frozen=True)
class ReadOnlyCase:
    api_name: str
    business_args: tuple[Any, ...] = ()
    semantic: "QuerySemanticExpectation | None" = None


@dataclass(frozen=True)
class QuerySemanticExpectation:
    """Payload-free acceptance rules for an enabled semantic query fixture."""

    workload: str
    expected_item_count: int | None = None
    minimum_pages: int | None = None
    minimum_items_per_page: int | None = None


FULL_CHAIN_READ_ONLY_CASES: tuple[ReadOnlyCase, ...] = (
    ReadOnlyCase("QueryAsset"),
    ReadOnlyCase("QueryFund"),
    ReadOnlyCase("QueryPosition", ("",)),
    ReadOnlyCase("QueryUnfinishedOrders"),
    ReadOnlyCase("QueryOrders", ({"ticker": "", "begin_time": 0, "end_time": 0},)),
    ReadOnlyCase("QueryTrades", ({"ticker": "", "begin_time": 0, "end_time": 0},)),
    ReadOnlyCase("QueryIPOQuotaInfo"),
    ReadOnlyCase("QueryIssueInfoList"),
)
"""Read-only queries which currently produce a complete C0--C7 trace."""

KNOWN_CALLBACK_GAPS: tuple[ReadOnlyCase, ...] = (
    ReadOnlyCase("QueryIPOInfoList"),
    ReadOnlyCase("QueryYesterdayAsset"),
)
"""Vendor binding/counter combinations verified to omit terminal callbacks.

They remain in the 86-interface matrix and may be explicitly probed, but do
not belong in a passing C0--C7 baseline until EMT supplies the callback.
"""

_BURST_PROBE_TIMEOUT_SECONDS = 2.0
"""Bound one concurrent-callback capability probe before P0 expansion."""

_COMMON_READ_ONLY_CASES = FULL_CHAIN_READ_ONLY_CASES[:6]
_OPTION_FULL_CHAIN_READ_ONLY_CASES = tuple(
    case for case in _COMMON_READ_ONLY_CASES if case.api_name != "QueryFund"
)
_OPTION_KNOWN_CALLBACK_GAPS = (ReadOnlyCase("QueryFund"),)


def read_only_cases_for(profile: str) -> tuple[ReadOnlyCase, ...]:
    """Keep profile-specific runs inside their catalog-approved scenarios."""

    if profile not in {"ordinary", "credit", "option"}:
        raise GatewayReadOnlyError("profile must be ordinary, credit or option")
    if profile == "ordinary":
        return FULL_CHAIN_READ_ONLY_CASES
    if profile == "credit":
        return _COMMON_READ_ONLY_CASES
    return _OPTION_FULL_CHAIN_READ_ONLY_CASES


def known_callback_gaps_for(profile: str) -> tuple[ReadOnlyCase, ...]:
    """Return documented callback gaps applicable to one account profile."""

    if profile not in {"ordinary", "credit", "option"}:
        raise GatewayReadOnlyError("profile must be ordinary, credit or option")
    if profile == "ordinary":
        return KNOWN_CALLBACK_GAPS
    if profile == "option":
        return _OPTION_KNOWN_CALLBACK_GAPS
    return ()


@dataclass(frozen=True)
class GatewayReadOnlySettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    profile: str = "ordinary"
    iterations: int = 1
    callback_timeout_seconds: float = 8.0
    interval_seconds: float = 0.25
    require_native_fastspi: bool = True
    probe_known_callback_gaps: bool = False
    trace_output: Path | None = None
    workload: str = "steady"
    burst_width: int = 1
    late_delay_seconds: float = 0.0
    callback_capacity: int = 4096

    def validate(self) -> None:
        if self.profile not in {"ordinary", "credit", "option"}:
            raise GatewayReadOnlyError("profile must be ordinary, credit or option")
        if self.workload == "steady" and (self.iterations < 1 or self.iterations > 10):
            raise GatewayReadOnlyError(
                "steady iterations must be in 1..10 for the controlled low-rate query driver"
            )
        if self.workload == "burst" and (self.iterations < 1 or self.iterations > 25):
            raise GatewayReadOnlyError("burst rounds must be in 1..25")
        if self.workload in {"late", "empty", "pagination"} and (
            self.iterations < 1 or self.iterations > 100
        ):
            raise GatewayReadOnlyError("late iterations must be in 1..100")
        if self.workload not in {"steady", "burst", "late", "empty", "pagination"}:
            raise GatewayReadOnlyError(
                "read-only driver supports steady, burst, late, empty or pagination workload"
            )
        if self.workload in {"steady", "late", "empty", "pagination"} and self.burst_width != 1:
            raise GatewayReadOnlyError(
                "steady, late, empty and pagination workloads require burst_width=1"
            )
        if self.workload == "burst" and not 2 <= self.burst_width <= 4:
            raise GatewayReadOnlyError("burst workload requires burst_width in 2..4")
        if self.workload == "late" and not 0.01 <= self.late_delay_seconds <= 1.0:
            raise GatewayReadOnlyError("late workload requires late_delay_seconds in 0.01..1.0")
        if self.workload != "late" and self.late_delay_seconds != 0.0:
            raise GatewayReadOnlyError("late_delay_seconds is valid only for the late workload")
        if self.callback_timeout_seconds <= 0:
            raise GatewayReadOnlyError("callback_timeout_seconds must be positive")
        if self.interval_seconds < 0.25:
            raise GatewayReadOnlyError(
                "interval_seconds must stay at or above the approved 0.25 seconds"
            )
        if not 1 <= self.callback_capacity <= 65_536:
            raise GatewayReadOnlyError("callback_capacity must be in 1..65536")


def run_gateway_readonly(
    settings: GatewayReadOnlySettings,
    *,
    cases: tuple[ReadOnlyCase, ...] | None = None,
    known_callback_gaps: tuple[ReadOnlyCase, ...] | None = None,
    driver_name: str = "gateway-readonly-c0-c7",
    retain_native_reference: Callable[[object], None] | None = None,
) -> dict[str, Any]:
    """Execute bounded read-only queries sequentially and return safe evidence.

    The default catalog is the low-rate gateway smoke baseline.  The optional
    ``cases`` route is used only by the separately validated local query
    fixture; it never enables state-changing APIs.

    ``retain_native_reference`` is an acceptance-only escape hatch for the
    documented EMT Linux wrapper finalizer hang.  The caller must retain the
    supplied session only inside a dedicated process which persists the report
    and immediately calls :func:`os._exit`.  Normal runtime and normal test
    paths always close the session through the vendor logout/release sequence.
    """

    settings.validate()
    account = getattr(settings.accounts, settings.profile)
    client_id = {"ordinary": 91, "credit": 92, "option": 93}[settings.profile]
    trace = GatewayTraceCollector()
    registry = QueryRegistry(trace=trace)
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    event_journal = FileJournal(settings.runtime_dir / f"{settings.profile}-events.emtj")
    callbacks = CallbackPipeline(
        journal=event_journal,
        trace=trace,
        lookup_source=lambda record: None,
        capacity=settings.callback_capacity,
        lifecycle_observer=registry,
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
                account=account.account,
                password=account.password,
                client_id=client_id,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile=settings.profile,
                sdk_log_level=0,
            ),
        )
        snapshot = session.start()
        facade = EmtTraderFacade(
            session.api,
            session_id=session.session_id,
            query_registry=registry,
            trace=trace,
        )
        selected_cases = cases if cases is not None else read_only_cases_for(settings.profile)
        selected_known_callback_gaps = (
            known_callback_gaps
            if known_callback_gaps is not None
            else known_callback_gaps_for(settings.profile)
        )
        if not selected_cases:
            raise GatewayReadOnlyError("at least one read-only query case is required")
        operation_outcomes: list[dict[str, str | int | None]] = []
        operation_observations: list[dict[str, str | int | bool | None]] = []
        semantic_outcomes: list[dict[str, str | int | bool | None]] = []
        evidence_correlation_ids: set[str] = set()
        burst_probe_outcomes: list[dict[str, str | bool]] = []
        execution_cases = selected_cases
        if settings.workload == "burst":
            eligible_cases: list[ReadOnlyCase] = []
            for case in selected_cases:
                outcomes, correlation_ids = _invoke_read_only_burst(
                    facade=facade,
                    callbacks=callbacks,
                    registry=registry,
                    trace=trace,
                    case=case,
                    profile=settings.profile,
                    width=2,
                    timeout_seconds=min(
                        settings.callback_timeout_seconds, _BURST_PROBE_TIMEOUT_SECONDS
                    ),
                )
                operation_outcomes.extend(outcomes)
                evidence_correlation_ids.update(correlation_ids)
                passed = _burst_probe_passed(outcomes)
                burst_probe_outcomes.append({"api": case.api_name, "passed": passed})
                if passed:
                    eligible_cases.append(case)
                sleep(settings.interval_seconds)
            execution_cases = tuple(eligible_cases)
        for _ in range(settings.iterations):
            for case in execution_cases:
                if settings.workload == "burst":
                    outcomes, correlation_ids = _invoke_read_only_burst(
                        facade=facade,
                        callbacks=callbacks,
                        registry=registry,
                        trace=trace,
                        case=case,
                        profile=settings.profile,
                        width=settings.burst_width,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    operation_outcomes.extend(outcomes)
                    evidence_correlation_ids.update(correlation_ids)
                elif settings.workload == "pagination":
                    outcomes, correlation_ids, semantic_outcome = _invoke_paginated_read_only_case(
                        facade=facade,
                        callbacks=callbacks,
                        registry=registry,
                        trace=trace,
                        case=case,
                        profile=settings.profile,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    operation_outcomes.extend(outcomes)
                    semantic_outcomes.append(semantic_outcome)
                    if semantic_outcome["passed"]:
                        evidence_correlation_ids.update(correlation_ids)
                else:
                    outcome, correlation_id, observation = _invoke_read_only_case(
                        facade=facade,
                        callbacks=callbacks,
                        registry=registry,
                        trace=trace,
                        case=case,
                        profile=settings.profile,
                        workload=settings.workload,
                        post_c5_delay_seconds=settings.late_delay_seconds,
                        timeout_seconds=settings.callback_timeout_seconds,
                    )
                    operation_outcomes.append(outcome)
                    operation_observations.append(
                        _payload_free_operation_observation(outcome, observation)
                    )
                    if settings.workload == "empty":
                        semantic_outcome = _evaluate_empty_semantics(case, outcome, observation)
                        semantic_outcomes.append(semantic_outcome)
                        if semantic_outcome["passed"]:
                            evidence_correlation_ids.add(correlation_id)
                    else:
                        evidence_correlation_ids.add(correlation_id)
                sleep(settings.interval_seconds)
        known_gap_probes: list[dict[str, str | None]] = []
        if settings.probe_known_callback_gaps:
            for case in selected_known_callback_gaps:
                outcome, _, _ = _invoke_read_only_case(
                    facade=facade,
                    callbacks=callbacks,
                    registry=registry,
                    trace=trace,
                    case=case,
                    profile=settings.profile,
                    workload=settings.workload,
                    post_c5_delay_seconds=0.0,
                    timeout_seconds=settings.callback_timeout_seconds,
                )
                known_gap_probes.append(
                    {
                        "api": case.api_name,
                        "outcome": outcome["outcome"],
                        "reason": outcome["reason"],
                        "classification": "known_no_terminal_callback",
                    }
                )
                sleep(settings.interval_seconds)
        expected_query_callbacks = {
            callback_name
            for case in selected_cases
            if (callback_name := facade.spec_for(case.api_name).callback_name) is not None
        }
        evidence_events = _select_evidence_events(
            trace.export_events(),
            evidence_correlation_ids=evidence_correlation_ids,
            query_callback_names=expected_query_callbacks,
            profile=settings.profile,
            workload=settings.workload,
        )
        report = build_gateway_report(evidence_events)
        if settings.trace_output is not None:
            _write_payload_free_trace(settings.trace_output, evidence_events)
            report["trace_artifact"] = settings.trace_output.name
        report["driver"] = driver_name
        report["safety"] = {
            "mode": (
                "bounded-query-burst"
                if settings.workload == "burst"
                else "controlled-post-c5-delay"
                if settings.workload == "late"
                else "payload-free-query-pagination"
                if settings.workload == "pagination"
                else "payload-free-empty-query"
                if settings.workload == "empty"
                else "sequential-read-only"
            ),
            "order_entry": False,
            "cancellation": False,
            "fund_or_credit_mutation": False,
            "native_fastspi_required": settings.require_native_fastspi,
            "interval_seconds": settings.interval_seconds,
            "burst_width": settings.burst_width,
            "late_delay_seconds": settings.late_delay_seconds,
            "callback_capacity": settings.callback_capacity,
        }
        report["run"] = {
            "profile": settings.profile,
            "iterations": settings.iterations,
            "workload": settings.workload,
            "burst_width": settings.burst_width,
            "late_delay_seconds": settings.late_delay_seconds,
            "callback_capacity": settings.callback_capacity,
            "query_cases": [case.api_name for case in selected_cases],
            "burst_probe": {
                "width": 2,
                "timeout_seconds": min(
                    settings.callback_timeout_seconds, _BURST_PROBE_TIMEOUT_SECONDS
                ),
                "eligible_cases": [case.api_name for case in execution_cases]
                if settings.workload == "burst"
                else [],
                "outcomes": burst_probe_outcomes,
            },
            "operation_outcomes": operation_outcomes,
            # This deliberately retains only completion and cardinality facts.
            # It is safe to use for fixture-readiness decisions without writing
            # response payloads, account balances, instrument identifiers or
            # opaque continuation references into performance evidence.
            "operation_observations": operation_observations,
            "semantic_outcomes": semantic_outcomes,
            "known_callback_gaps": [
                {
                    "api": case.api_name,
                    "classification": "known_no_terminal_callback",
                    "reason": "vendor_binding_or_counter_omits_terminal_callback",
                }
                for case in selected_known_callback_gaps
            ],
            "known_callback_probes": known_gap_probes,
            "api_version": snapshot.api_version,
            "trading_day": snapshot.trading_day,
        }
        return report
    finally:
        try:
            if session is not None:
                if retain_native_reference is not None:
                    retain_native_reference(session)
                else:
                    session.close()
        finally:
            event_journal.close()


def _write_payload_free_trace(path: Path, events: list[dict[str, Any]]) -> None:
    """Persist only the collector's already payload-free trace events."""

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = "\n".join(json.dumps(event, ensure_ascii=True, sort_keys=True) for event in events)
    path.write_text(f"{serialized}\n" if serialized else "", encoding="utf-8")


def _select_evidence_events(
    events: list[dict[str, Any]],
    *,
    evidence_correlation_ids: set[str],
    query_callback_names: set[str],
    profile: str,
    workload: str,
) -> list[dict[str, Any]]:
    """Keep selected query API traces and their own SPI lifecycle traces.

    QueryRegistry joins C5--C7 to the active API request so API evidence is
    complete. The callback pipeline simultaneously creates its independent
    `OnQuery*` trace; retaining only API correlation IDs made that trace
    disappear from archived real-counter evidence. Include just callbacks
    selected for this run and its exact profile/workload, never unrelated
    session-startup or public-stream events.
    """

    return [
        event
        for event in events
        if event.get("event_type") == "metric"
        or event.get("correlation_id") in evidence_correlation_ids
        or (
            event.get("kind") == "spi"
            and event.get("interface") in query_callback_names
            and event.get("profile") == profile
            and event.get("workload") == workload
        )
    ]


def _invoke_read_only_case(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    case: ReadOnlyCase,
    profile: str,
    workload: str,
    post_c5_delay_seconds: float,
    timeout_seconds: float,
) -> tuple[dict[str, str | int | None], str, QuerySemanticObservation | None]:
    """Execute one query and return only its safe outcome metadata."""

    handle = facade.invoke_query(
        case.api_name,
        business_args=case.business_args,
        profile=profile,
        workload=workload,
        timeout_ns=int(timeout_seconds * 1_000_000_000),
    )
    if post_c5_delay_seconds:
        _delay_after_c5(
            trace=trace,
            correlation_id=handle.correlation_id,
            timeout_seconds=timeout_seconds,
            delay_seconds=post_c5_delay_seconds,
        )
    _drain_until_complete(
        callbacks=callbacks,
        registry=registry,
        timeout_seconds=timeout_seconds,
    )
    record = next(
        (item for item in trace.records() if item.correlation_id == handle.correlation_id),
        None,
    )
    observation = registry.semantic_observation(handle.correlation_id)
    outcome = {
        "api": case.api_name,
        "outcome": record.outcome if record is not None else "missing_trace",
        "reason": record.reason if record is not None else "missing_trace",
    }
    _classify_incomplete_terminal_callback(outcome, observation)
    return (
        outcome,
        handle.correlation_id,
        observation,
    )


def _classify_incomplete_terminal_callback(
    outcome: dict[str, str | int | None],
    observation: QuerySemanticObservation | None,
) -> None:
    """Refine a timeout without weakening callback-driven completion.

    A query whose native invocation returned success but then times out has
    materially different failure modes: the venue may emit no callback at all,
    or it may emit non-terminal callbacks without the required final
    ``is_last=True``.  Keep those facts in the payload-free report so an EMT
    compatibility issue cannot be confused with a local queue or parser loss.
    """

    if outcome.get("outcome") != "timeout" or observation is None:
        return
    if observation.callback_count == 0:
        outcome["reason"] = "native_accepted_no_callback"
    elif not observation.terminal_seen:
        outcome["reason"] = "native_accepted_missing_is_last_true"


def _evaluate_empty_semantics(
    case: ReadOnlyCase,
    outcome: dict[str, str | int | None],
    observation: QuerySemanticObservation | None,
) -> dict[str, str | int | bool | None]:
    """Verify an empty response without retaining a callback payload."""

    expectation = case.semantic
    if (
        expectation is None
        or expectation.workload != "empty"
        or expectation.expected_item_count is None
    ):
        raise GatewayReadOnlyError(f"{case.api_name} lacks an empty semantic expectation")
    if outcome["outcome"] != "ok":
        return {
            "api": case.api_name,
            "workload": "empty",
            "passed": False,
            "reason": outcome["reason"],
            "item_count": None,
        }
    if observation is None or not observation.terminal_seen:
        return {
            "api": case.api_name,
            "workload": "empty",
            "passed": False,
            "reason": "missing_terminal_observation",
            "item_count": None,
        }
    passed = observation.item_count == expectation.expected_item_count
    return {
        "api": case.api_name,
        "workload": "empty",
        "passed": passed,
        "reason": None if passed else "unexpected_nonempty_result",
        "item_count": observation.item_count,
    }


def _payload_free_operation_observation(
    outcome: dict[str, str | int | None],
    observation: QuerySemanticObservation | None,
) -> dict[str, str | int | bool | None]:
    """Summarize one query without retaining an EMT response or cursor."""

    return {
        "api": outcome["api"],
        "outcome": outcome["outcome"],
        "reason": outcome["reason"],
        "terminal_seen": observation.terminal_seen if observation is not None else False,
        # A bounded scalar diagnostic only; this never persists native rows,
        # errors, request ids, account values or opaque cursor values.
        "terminal_flag_states": (
            observation.terminal_flag_states if observation is not None else "none"
        ),
        "callback_count": observation.callback_count if observation is not None else 0,
        "item_count": observation.item_count if observation is not None else 0,
    }


def _invoke_paginated_read_only_case(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    case: ReadOnlyCase,
    profile: str,
    timeout_seconds: float,
) -> tuple[list[dict[str, str | int | None]], set[str], dict[str, str | int | bool | None]]:
    """Use the first returned opaque cursor for exactly one continuation page."""

    expectation = case.semantic
    if expectation is None or expectation.workload != "pagination":
        raise GatewayReadOnlyError(f"{case.api_name} lacks a pagination semantic expectation")
    first_outcome, first_correlation_id, first_observation = _invoke_read_only_case(
        facade=facade,
        callbacks=callbacks,
        registry=registry,
        trace=trace,
        case=case,
        profile=profile,
        workload="pagination",
        post_c5_delay_seconds=0.0,
        timeout_seconds=timeout_seconds,
    )
    outcomes = [first_outcome]
    correlation_ids = {first_correlation_id}
    if first_outcome["outcome"] != "ok" or first_observation is None:
        return (
            outcomes,
            correlation_ids,
            {
                "api": case.api_name,
                "workload": "pagination",
                "passed": False,
                "reason": first_outcome["reason"] or "missing_first_page_observation",
                "page_count": 0,
            },
        )
    minimum_items = expectation.minimum_items_per_page or 1
    reference = first_observation.query_reference
    if first_observation.item_count < minimum_items or reference is None or reference <= 0:
        return (
            outcomes,
            correlation_ids,
            {
                "api": case.api_name,
                "workload": "pagination",
                "passed": False,
                "reason": "first_page_is_not_continuable",
                "page_count": 1,
                "item_count": first_observation.item_count,
            },
        )
    continuation_case = _case_with_continuation_reference(case, reference)
    second_outcome, second_correlation_id, second_observation = _invoke_read_only_case(
        facade=facade,
        callbacks=callbacks,
        registry=registry,
        trace=trace,
        case=continuation_case,
        profile=profile,
        workload="pagination",
        post_c5_delay_seconds=0.0,
        timeout_seconds=timeout_seconds,
    )
    outcomes.append(second_outcome)
    correlation_ids.add(second_correlation_id)
    passed = (
        second_outcome["outcome"] == "ok"
        and second_observation is not None
        and second_observation.terminal_seen
        and second_observation.item_count >= minimum_items
        and expectation.minimum_pages == 2
    )
    return (
        outcomes,
        correlation_ids,
        {
            "api": case.api_name,
            "workload": "pagination",
            "passed": passed,
            "reason": None if passed else (second_outcome["reason"] or "second_page_not_completed"),
            "page_count": 2 if second_observation is not None else 1,
            "item_count": (first_observation.item_count + second_observation.item_count)
            if second_observation
            else first_observation.item_count,
        },
    )


def _case_with_continuation_reference(case: ReadOnlyCase, reference: int) -> ReadOnlyCase:
    if len(case.business_args) != 1 or not isinstance(case.business_args[0], dict):
        raise GatewayReadOnlyError(
            f"{case.api_name} pagination fixture must have one dictionary argument"
        )
    args = dict(case.business_args[0])
    args["reference"] = reference
    return replace(case, business_args=(args,))


def _delay_after_c5(
    *,
    trace: GatewayTraceCollector,
    correlation_id: str,
    timeout_seconds: float,
    delay_seconds: float,
) -> bool:
    """Inject consumer delay only after the real callback reached the fast ring."""

    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        record = next(
            (item for item in trace.records() if item.correlation_id == correlation_id), None
        )
        if record is not None and "C5" in record.timestamps_ns:
            sleep(delay_seconds)
            return True
        sleep(0.001)
    return False


def _invoke_read_only_burst(
    *,
    facade: EmtTraderFacade,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    trace: GatewayTraceCollector,
    case: ReadOnlyCase,
    profile: str,
    width: int,
    timeout_seconds: float,
) -> tuple[list[dict[str, str | None]], set[str]]:
    """Issue a bounded same-query burst before draining its callback stream."""

    handles = [
        facade.invoke_query(
            case.api_name,
            business_args=case.business_args,
            profile=profile,
            workload="burst",
            timeout_ns=int(timeout_seconds * 1_000_000_000),
        )
        for _ in range(width)
    ]
    _drain_until_complete(
        callbacks=callbacks,
        registry=registry,
        timeout_seconds=timeout_seconds,
    )
    outcomes: list[dict[str, str | None]] = []
    correlation_ids: set[str] = set()
    for handle in handles:
        record = next(
            (item for item in trace.records() if item.correlation_id == handle.correlation_id),
            None,
        )
        outcomes.append(
            {
                "api": case.api_name,
                "outcome": record.outcome if record is not None else "missing_trace",
                "reason": record.reason if record is not None else "missing_trace",
            }
        )
        correlation_ids.add(handle.correlation_id)
    return outcomes, correlation_ids


def _burst_probe_passed(outcomes: list[dict[str, str | None]]) -> bool:
    """Permit P0 expansion only after every bounded probe request completed."""

    return bool(outcomes) and all(outcome.get("outcome") == "ok" for outcome in outcomes)


def _drain_until_complete(
    *,
    callbacks: CallbackPipeline,
    registry: QueryRegistry,
    timeout_seconds: float,
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
