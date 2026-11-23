"""Aggregate C0--C7 trace events into a strict, catalog-complete report."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .catalog import TRADER_API, TRADER_SPI
from .histogram import ExactHistogram
from .matrix import Scenario, all_scenarios, scenario_as_dict
from .trace import MetricSample, TraceRecord, collector_from_events

_CONTROLLED_INJECTION_ARTIFACT_TYPE = "controlled_gateway_injection"
_ARTIFACT_CLASSIFICATION_EVENT = "artifact_classification"


def _empty_summary() -> dict[str, int | None]:
    return {
        "count": 0,
        "min_ns": None,
        "p50_ns": None,
        "p95_ns": None,
        "p99_ns": None,
        "p999_ns": None,
        "max_ns": None,
    }


def _path_summary(records: Iterable[TraceRecord], start: str, end: str) -> dict[str, int | None]:
    histogram = ExactHistogram()
    for record in records:
        if start in record.timestamps_ns and end in record.timestamps_ns:
            histogram.record(record.timestamps_ns[end] - record.timestamps_ns[start])
    return histogram.summary() if histogram.count else _empty_summary()


def _metric_summary(samples: Iterable[MetricSample]) -> dict[str, dict[str, int]]:
    grouped: dict[str, ExactHistogram] = {}
    for sample in samples:
        histogram = grouped.setdefault(sample.metric, ExactHistogram())
        histogram.record(sample.value)
    return {
        metric: {key.removesuffix("_ns"): value for key, value in histogram.summary().items()}
        for metric, histogram in sorted(grouped.items())
    }


def _status_for(scenario: Scenario, records: list[TraceRecord]) -> tuple[str, list[str], int, int]:
    if not records:
        return "pending", list(scenario.required_path_names), 0, 0
    failures = sum(record.outcome not in (None, "ok") for record in records)
    successes = [record for record in records if record.outcome == "ok"]
    missing = [
        f"{start}_to_{end}"
        for start, end in scenario.required_paths
        if any(
            start not in record.timestamps_ns or end not in record.timestamps_ns
            for record in successes
        )
    ]
    if failures:
        return "failed", missing, len(successes), failures
    if not successes:
        return "incomplete", list(scenario.required_path_names), 0, failures
    if missing:
        return "incomplete", missing, len(successes), failures
    if len(successes) < scenario.p0_minimum_successes:
        return "insufficient_samples", [], len(successes), failures
    return "passed_p0", [], len(successes), failures


def build_gateway_report(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Produce a report where absent trace evidence is explicitly non-passing.

    The report intentionally makes no production-SLA claim.  Its P0 pass state
    only proves that a scenario met the documented minimum sample count and
    emitted every required C0--C7 path in the local test environment.
    """

    buffered_events = list(events)
    _reject_controlled_injection_artifact(buffered_events)
    collector = collector_from_events(buffered_events)
    records = collector.records()
    grouped: dict[str, list[TraceRecord]] = defaultdict(list)
    for record in records:
        grouped[record.scenario_key].append(record)

    results: list[dict[str, Any]] = []
    for scenario in all_scenarios():
        scenario_records = grouped[scenario.key]
        status, missing_paths, successes, failures = _status_for(scenario, scenario_records)
        paths = {
            f"{start}_to_{end}": _path_summary(scenario_records, start, end)
            for start, end in scenario.required_paths
        }
        results.append(
            {
                **scenario_as_dict(scenario),
                "evidence_status": status,
                "attempts": len(scenario_records),
                "successful_complete_traces": successes,
                "failed_or_rejected_traces": failures,
                "missing_paths": missing_paths,
                "latency": paths,
            }
        )

    statuses = Counter(result["evidence_status"] for result in results)
    return {
        "schema_version": 1,
        "driver": "gateway-trace-report",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "measurement_scope": "gateway C0-C7 trace evidence; not a production SLA declaration",
        "catalog": {"trader_api": len(TRADER_API), "trader_spi": len(TRADER_SPI)},
        "summary": {
            "scenarios": len(results),
            "traces": len(records),
            "by_evidence_status": dict(sorted(statuses.items())),
            "p0_ready": statuses.get("passed_p0", 0) == len(results),
            "acceptance_ready": False,
            "acceptance_rule": "Every scenario needs approved budgets and acceptance_minimum_successes; this report only evaluates P0.",
        },
        "resource_metrics": _metric_summary(collector.metrics()),
        "scenarios": results,
    }


def _reject_controlled_injection_artifact(events: Iterable[Mapping[str, Any]]) -> None:
    """Keep local callback injection out of EMT counter/release evidence.

    A controlled injection trace is intentionally structurally similar to a
    real C0--C7 trace.  It therefore carries a payload-free JSONL provenance
    record which the official aggregator must reject before calculating any
    P0, baseline, acceptance, workload-readiness, or release result.
    """

    for event in events:
        if (
            event.get("event_type") == _ARTIFACT_CLASSIFICATION_EVENT
            and event.get("artifact_type") == _CONTROLLED_INJECTION_ARTIFACT_TYPE
        ):
            raise ValueError(
                "controlled gateway injection trace is ineligible for EMT counter, acceptance, workload, or release evidence"
            )
