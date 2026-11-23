"""Controlled, local callback-pipeline injection evidence.

This runner never loads the EMT SDK, opens a network connection, or uses an
account.  It drives the gateway callback boundary with minimal facts only, so
its output is useful for gateway resilience development but is deliberately
ineligible as evidence of EMT counter performance or release readiness.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Any
from uuid import uuid4

from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import FileJournal, InMemoryJournal, JournalRecord

from .gateway_report import build_gateway_report
from .trace import GatewayTraceCollector


class CallbackInjectionError(ValueError):
    """The local injection settings cannot safely form a bounded run."""


@dataclass(frozen=True)
class CallbackInjectionSettings:
    """Bounded settings for an account-free callback-pipeline test."""

    iterations: int = 100
    burst_width: int = 4
    slow_consumer_delay_seconds: float = 0.01
    trace_output: Path | None = None
    fixture_owner: str = ""
    runtime_dir: Path | None = None
    journal_mode: str = "file"
    require_native_fastspi: bool = True

    def validate(self) -> None:
        if not 1 <= self.iterations <= 100:
            raise CallbackInjectionError("callback injection iterations must be in 1..100")
        if not 2 <= self.burst_width <= 8:
            raise CallbackInjectionError("callback injection burst_width must be in 2..8")
        if not 0.001 <= self.slow_consumer_delay_seconds <= 1.0:
            raise CallbackInjectionError("slow_consumer_delay_seconds must be in 0.001..1.0")
        if self.fixture_owner and not self.fixture_owner.strip():
            raise CallbackInjectionError("fixture_owner must be non-blank when supplied")
        if self.journal_mode not in {"file", "memory"}:
            raise CallbackInjectionError("journal_mode must be file or memory")
        if self.journal_mode == "file" and self.runtime_dir is None:
            raise CallbackInjectionError("runtime_dir is required when journal_mode is file")


@dataclass(frozen=True)
class _InjectionCase:
    callback_name: str
    profile: str
    workload: str
    mode: str
    semantic_scope: str

    @property
    def key(self) -> str:
        return f"spi:{self.callback_name}:{self.profile}:{self.workload}"


_CASES: tuple[_InjectionCase, ...] = (
    _InjectionCase("OnDisconnected", "ordinary", "reconnect", "single", "callback-pipeline-only"),
    _InjectionCase("OnConnected", "ordinary", "reconnect", "single", "callback-pipeline-only"),
    _InjectionCase("OnOrderEvent", "ordinary", "burst", "burst", "bounded-serial-burst"),
    _InjectionCase(
        "OnOrderEvent", "ordinary", "duplicate", "duplicate", "duplicate-delivery-not-deduplication"
    ),
    _InjectionCase(
        "OnOrderEvent", "ordinary", "out_of_order", "out_of_order", "arrival-order-not-reordering"
    ),
    _InjectionCase(
        "OnOrderEvent", "ordinary", "slow_consumer", "slow_consumer", "post-c5-consumer-delay"
    ),
    _InjectionCase("OnQueryOrder", "ordinary", "empty", "query_empty", "terminal-empty-callback"),
    _InjectionCase(
        "OnQueryOrderByPage",
        "ordinary",
        "pagination",
        "query_pagination",
        "two-page-callback-shape",
    ),
)


class _MeasuredFileJournal:
    """Record write-through lag without exposing any persisted identity."""

    def __init__(self, journal: FileJournal, trace: GatewayTraceCollector) -> None:
        self._journal = journal
        self._trace = trace

    def append(
        self,
        *,
        kind: str,
        command_id: str | None,
        correlation_id: str,
        payload_hash: str,
    ) -> JournalRecord:
        record = self._journal.append(
            kind=kind,
            command_id=command_id,
            correlation_id=correlation_id,
            payload_hash=payload_hash,
        )
        if self._journal.last_flush_ns is not None:
            self._trace.record_metric("journal_durable_lag_ns", self._journal.last_flush_ns)
        return record

    def records(self) -> tuple[JournalRecord, ...]:
        return self._journal.records()


def run_callback_injection(settings: CallbackInjectionSettings) -> dict[str, Any]:
    """Exercise the gateway callback boundary using only synthetic callbacks.

    The separate ``.injected.traces.jsonl`` artifact must never be supplied to
    the real EMT trace aggregation, workload readiness, or release gate.
    """

    settings.validate()
    trace = GatewayTraceCollector()
    journal, file_journal, journal_artifact = _open_journal(settings, trace)
    callbacks = CallbackPipeline(
        journal=journal,
        trace=trace,
        lookup_source=lambda record: None,
        capacity=settings.burst_width,
        require_native_fastspi=settings.require_native_fastspi,
    )
    try:
        results: list[dict[str, Any]] = []
        for case in _CASES:
            before_journal_count = len(journal.records())
            requested = enqueued = acknowledgements = 0
            for _ in range(settings.iterations):
                correlation_ids = _inject_round(callbacks, case, settings)
                requested += len(correlation_ids)
                enqueued += sum(correlation_id is not None for correlation_id in correlation_ids)
                if case.mode == "slow_consumer":
                    sleep(settings.slow_consumer_delay_seconds)
                acknowledgements += _drain(callbacks)
            durable = len(journal.records()) - before_journal_count
            results.append(
                {
                    "key": case.key,
                    "callback": case.callback_name,
                    "profile": case.profile,
                    "workload": case.workload,
                    "injection_mode": case.mode,
                    "semantic_scope": case.semantic_scope,
                    "requested_callbacks": requested,
                    "enqueued_callbacks": enqueued,
                    "durable_callbacks": durable,
                    "acknowledged_callbacks": acknowledgements,
                    "dropped_callbacks": requested - enqueued,
                    "pipeline_complete": (
                        requested == enqueued == durable == acknowledgements
                        and not callbacks.disarmed
                    ),
                }
            )

        events = trace.export_events()
        matrix_report = build_gateway_report(events)
        matrix_projection = _matrix_projection(matrix_report, results)
        trace_artifact: str | None = None
        if settings.trace_output is not None:
            _write_controlled_injection_trace(settings.trace_output, events)
            trace_artifact = settings.trace_output.name
        return {
            "schema_version": 1,
            "driver": "gateway-callback-injection",
            "measurement_scope": (
                "controlled local callback injection through CallbackPipeline and a "
                f"{settings.journal_mode} journal; excludes EMT SDK, network, account, and counter"
            ),
            "evidence_classification": {
                "artifact_type": "controlled_gateway_injection",
                "counter_performance_eligible": False,
                "release_gate_eligible": False,
                "must_not_merge_with_emt_trace_evidence": True,
                "allowed_use": "gateway callback-pipeline resilience development only",
            },
            "fixture": {
                "fixture_id": "gateway-callback-pipeline-injection-v1",
                "owner": settings.fixture_owner or None,
                "controls": {
                    "reconnect": "local callback sequence; no network disconnection",
                    "burst": "bounded serial enqueue before drain",
                    "duplicate": "repeated arrival; deduplication is not asserted",
                    "out_of_order": "paired arrival shape; consumer reordering is not asserted",
                    "slow_consumer": "bounded post-C5 delay before durable drain",
                    "query": "minimal empty and two-page callback facts; no EMT payload",
                },
            },
            "run": {
                "iterations": settings.iterations,
                "burst_width": settings.burst_width,
                "slow_consumer_delay_seconds": settings.slow_consumer_delay_seconds,
                "journal_mode": settings.journal_mode,
                "journal_artifact": journal_artifact,
                "native_fastspi_required": settings.require_native_fastspi,
                "native_fastspi_available": callbacks.native_fastspi_available,
                "disarmed": callbacks.disarmed,
                "trace_artifact": trace_artifact,
            },
            "callback_pipeline_evidence": results,
            "matrix_projection": matrix_projection,
            "resource_metrics": matrix_report["resource_metrics"],
        }
    finally:
        if file_journal is not None:
            file_journal.close()


def _open_journal(
    settings: CallbackInjectionSettings,
    trace: GatewayTraceCollector,
) -> tuple[InMemoryJournal | _MeasuredFileJournal, FileJournal | None, str | None]:
    if settings.journal_mode == "memory":
        return InMemoryJournal(), None, None
    assert settings.runtime_dir is not None
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    journal = FileJournal(settings.runtime_dir / f"callback-injection-{uuid4().hex}.emtj")
    return _MeasuredFileJournal(journal, trace), journal, journal.path.name


def _inject_round(
    callbacks: CallbackPipeline,
    case: _InjectionCase,
    settings: CallbackInjectionSettings,
) -> list[str | None]:
    """Inject one bounded round without attaching any business identifiers."""

    if case.mode in {"single", "query_empty"}:
        return [_receive(callbacks, case, query_item_present=False, is_last=True)]
    if case.mode in {"burst", "slow_consumer"}:
        return [_receive(callbacks, case) for _ in range(settings.burst_width)]
    if case.mode in {"duplicate", "out_of_order"}:
        # The pair intentionally carries no event sequence or business id. It
        # can prove transport survival only, never deduplication/reordering.
        return [_receive(callbacks, case), _receive(callbacks, case)]
    if case.mode == "query_pagination":
        return [
            _receive(callbacks, case, query_item_present=True, query_reference=1, is_last=False),
            _receive(callbacks, case, query_item_present=True, query_reference=0, is_last=True),
        ]
    raise CallbackInjectionError(f"unknown injection mode: {case.mode}")


def _receive(
    callbacks: CallbackPipeline,
    case: _InjectionCase,
    *,
    query_item_present: bool | None = None,
    query_reference: int | None = None,
    is_last: bool | None = None,
) -> str | None:
    return callbacks.receive(
        case.callback_name,
        profile=case.profile,
        workload=case.workload,
        query_item_present=query_item_present,
        query_reference=query_reference,
        is_last=is_last,
    )


def _drain(callbacks: CallbackPipeline) -> int:
    acknowledgements = 0
    while deliveries := callbacks.normalize_many(64):
        for delivery in deliveries:
            callbacks.ack(delivery.delivery_id)
            acknowledgements += 1
    return acknowledgements


def _matrix_projection(report: dict[str, Any], cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expose trace shape/latency without creating a releasable P0 report."""

    by_key = {str(item["key"]): item for item in report["scenarios"]}
    projection: list[dict[str, Any]] = []
    for case in cases:
        item = by_key[case["key"]]
        projection.append(
            {
                "key": case["key"],
                "attempts": item["attempts"],
                "complete_traces": item["successful_complete_traces"],
                "failed_traces": item["failed_or_rejected_traces"],
                "required_paths_complete": not item["missing_paths"],
                "matrix_p0_minimum_successes": item["p0_minimum_successes"],
                "matrix_p0_sample_count_met": item["successful_complete_traces"]
                >= item["p0_minimum_successes"],
                "latency": item["latency"],
                "not_a_counter_or_release_result": True,
            }
        )
    return projection


def _write_controlled_injection_trace(path: Path, events: list[dict[str, Any]]) -> None:
    """Write an explicitly ineligible JSONL stream beside the local report."""

    path.parent.mkdir(parents=True, exist_ok=True)
    provenance = {
        "event_type": "artifact_classification",
        "artifact_type": "controlled_gateway_injection",
        "counter_performance_eligible": False,
        "release_gate_eligible": False,
        "must_not_merge_with_emt_trace_evidence": True,
    }
    serialized = "\n".join(
        json.dumps(event, ensure_ascii=True, sort_keys=True) for event in (provenance, *events)
    )
    path.write_text(f"{serialized}\n" if serialized else "", encoding="utf-8")
