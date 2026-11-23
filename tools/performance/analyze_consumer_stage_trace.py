"""Offline bounded inclusive-stage analysis; never a performance/READY certificate."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from hdrh.histogram import HdrHistogram
from join_consumer_source_timing import _json, _segment_budget
from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json
from xuanling_tradecore.persistence.stage_timing import _STAGES


def analyze(
    manifests: list[Path],
    *,
    expected_clock: str,
    expected_journal: str,
    expected_final_lsn: int | None = None,
) -> dict:
    if (
        not 1 <= len(manifests) <= 16
        or len({path.resolve() for path in manifests}) != len(manifests)
        or len(expected_clock) != 64
        or any(c not in "0123456789abcdef" for c in expected_clock)
        or not expected_journal
        or (
            expected_final_lsn is not None
            and (type(expected_final_lsn) is not int or expected_final_lsn < 0)
        )
    ):
        raise ValueError("invalid stage-analysis scope")
    files = []
    for path in manifests:
        manifest = _json(path)
        status = _json(path.with_suffix("").with_suffix(".status.json"))
        if (
            status.get("schema_version") != "consumer-stage-trace-v1"
            or status.get("clock_identity_sha256") != expected_clock
            or status.get("clock") != "time.monotonic_ns"
            or status.get("journal_id") != expected_journal
            or manifest.get("journal_id") != expected_journal
            or any(
                status.get(key) is not value
                for key, value in (
                    ("closed", True),
                    ("drain_complete", True),
                    ("writer_failed", False),
                    ("status_provider_failed", False),
                )
            )
            or status.get("queue_dropped") != 0
            or status.get("capacity_dropped") != 0
            or status.get("submitted") != status.get("written")
        ):
            raise ValueError("stage identity or complete evidence status failed")
        count = _segment_budget(path.parent, manifest["segments"], 2 * 1024**3)
        if verify_closed_request_trace(path) != count or count != status["written"]:
            raise ValueError("stage count/hash-chain differs")
        files.append((status["anchor_monotonic_ns"], path, manifest))
    totals, calls, maxima = Counter(), Counter(), Counter()
    histograms = {}
    seen_components = Counter()
    committed_facts = committed_batches = failed_transactions = 0
    next_lsn = 1
    for _, path, manifest in sorted(files, key=lambda item: item[0]):
        for segment in manifest["segments"]:
            for line in (path.parent / segment["file"]).open(encoding="utf-8"):
                row = json.loads(line)
                component = row.get("component")
                start, end = row.get("start_ns"), row.get("end_ns")
                values = row.get("values")
                if (
                    row.get("schema") != "consumer-stage-timing-v1"
                    or row.get("inclusive_stages") is not True
                    or component not in {"reactor", "projector", "projection-source"}
                    or type(start) is not int
                    or type(end) is not int
                    or not 0 < start <= end < 2**63
                    or not isinstance(values, dict)
                    or any(type(value) not in (int, bool) or value < 0 for value in values.values())
                ):
                    raise ValueError("invalid stage observation")
                observed_calls, observed_totals, observed_max = (
                    row.get("calls"),
                    row.get("total_ns"),
                    row.get("maximum_ns"),
                )
                if (
                    not all(
                        isinstance(v, dict) for v in (observed_calls, observed_totals, observed_max)
                    )
                    or set(observed_calls) != set(observed_totals)
                    or set(observed_calls) != set(observed_max)
                    or not set(observed_calls) <= _STAGES
                ):
                    raise ValueError("unknown or missing stage counters")
                seen_components[component] += 1
                for name, count in observed_calls.items():
                    total, maximum = observed_totals[name], observed_max[name]
                    if (
                        type(count) is not int
                        or count < 1
                        or type(total) is not int
                        or type(maximum) is not int
                        or not 0 <= maximum <= total <= end - start
                        or total > 60_000_000_000
                    ):
                        raise ValueError("reversed or invalid stage duration")
                    key = component + "." + name
                    totals[key] += total
                    calls[key] += count
                    maxima[key] = max(maxima[key], maximum)
                    if key not in histograms:
                        histograms[key] = HdrHistogram(1, 60_000_000_000, 3)
                    histogram = histograms[key]
                    histogram.record_value(max(1, total))
                if "committed_batches" in values:
                    committed = values.get("committed")
                    facts, batches = values.get("committed_facts"), values["committed_batches"]
                    first, final = values.get("first_lsn"), values.get("final_lsn")
                    if (
                        component != "projector"
                        or type(committed) is not bool
                        or type(facts) is not int
                        or type(batches) is not int
                    ):
                        raise ValueError("invalid projection commit accounting")
                    if not committed:
                        if facts or batches:
                            raise ValueError("rollback claims committed facts")
                        failed_transactions += 1
                    elif observed_calls.get("sql_commit") != 1:
                        raise ValueError("committed group lacks real explicit FULL call")
                    elif facts:
                        if (
                            first != next_lsn
                            or type(final) is not int
                            or final - first + 1 != facts
                            or batches < 1
                        ):
                            raise ValueError(
                                "projection stage has duplicate or missing source range"
                            )
                        next_lsn = final + 1
                        committed_facts += facts
                        committed_batches += batches
                    elif batches:
                        raise ValueError("projection batch has no facts")
    final_lsn = next_lsn - 1
    if expected_final_lsn is not None and final_lsn > expected_final_lsn:
        raise ValueError("stage source exceeds frozen final boundary")
    return dict(
        schema="consumer-stage-analysis-v1",
        observation_integrity_verified=True,
        source_prefix_complete=final_lsn == expected_final_lsn
        if expected_final_lsn is not None
        else None,
        expected_final_lsn=expected_final_lsn,
        observed_committed_lsn=final_lsn,
        committed_facts=committed_facts,
        committed_batches=committed_batches,
        failed_transactions=failed_transactions,
        components=dict(seen_components),
        clock_identity_sha256=expected_clock,
        runtime_payload_identity="NOT_VERIFIED_BY_THIS_ANALYZER",
        formal_qualification=False,
        performance_status="NOT_EVALUATED",
        scope="COMPLETE_TRACE_LIFECYCLE_NOT_FROZEN_LOAD_WINDOW",
        stages={
            key: dict(
                calls=calls[key],
                total_ns=totals[key],
                true_call_max_ns=maxima[key],
                observed_groups=h.total_count,
                group_total_p50_ns=h.get_value_at_percentile(50),
                group_total_p95_ns=h.get_value_at_percentile(95),
                group_total_p99_ns=h.get_value_at_percentile(99),
            )
            for key, h in histograms.items()
        },
        limitations=[
            "inclusive stages must not be added",
            "group quantiles are not individual SQL-call quantiles",
            "commit envelope includes I/O, locks and scheduling; not physical SSD latency",
            "fresh projection prefix starts at LSN 1; resume needs a separate anchored contract",
            "no server/client join, runtime wheel or source/FULL consumer closure certificate",
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--expected-clock", required=True)
    parser.add_argument("--expected-journal", required=True)
    parser.add_argument("--expected-final-lsn", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(
        args.manifest,
        expected_clock=args.expected_clock,
        expected_journal=args.expected_journal,
        expected_final_lsn=args.expected_final_lsn,
    )
    args.output.mkdir(mode=0o700, exist_ok=False)
    atomic_runtime_json(args.output / "report.json", result)
    print(json.dumps(result, separators=(",", ":")))
    return int(result["source_prefix_complete"] is False)


if __name__ == "__main__":
    raise SystemExit(main())
