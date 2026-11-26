"""Isolated fixed-rate staircase and burst trials; thresholds are experimental, not a live SLA."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from uuid import uuid4

from tools.quality.run_acceptance import ROOT, revision, run_bounded


def summarize(trials: list[dict]) -> list[dict]:
    """Keep the trial population visible; never present an average P99 as a pooled percentile."""
    groups: dict[float, list[dict]] = {}
    for trial in trials:
        if trial["status"] == "passed":
            groups.setdefault(trial["measurement"]["target_rate_per_second"], []).append(
                trial["measurement"]
            )
    rows = []
    for rate, measurements in groups.items():
        row = {"target_rate_per_second": rate, "successful_trials": len(measurements)}
        for name, values in {
            "completed": [m["completed"] for m in measurements],
            "rejected": [m["rejected"] for m in measurements],
            "throughput_per_second": [m["throughput_per_second"] for m in measurements],
            "completion_ratio": [m["completion_ratio"] for m in measurements],
            "per_trial_latency_p99_ms": [
                m["latency"]["p99_ns"] / 10**6 for m in measurements if m["completed"]
            ],
            "per_trial_generator_p99_ms": [
                m["generator_lateness"]["p99_ns"] / 10**6 for m in measurements
            ],
            "per_trial_queue_p99_ms": [m["queue_wait"]["p99_ns"] / 10**6 for m in measurements],
        }.items():
            row[name] = (
                {"min": min(values), "median": statistics.median(values), "max": max(values)}
                if values
                else None
            )
        row["trials_with_completions"] = sum(m["completed"] > 0 for m in measurements)
        if measurements[0]["deadline_ms"] is not None:
            values = [m["within_deadline_ratio_of_offered"] for m in measurements]
            row["within_deadline_ratio_of_offered"] = {
                "min": min(values),
                "median": statistics.median(values),
                "max": max(values),
            }
        rows.append(row)
    return rows


def run_matrix(
    *,
    output: Path,
    rates: list[float],
    orders=500,
    repeats=3,
    capacity=256,
    batch_size=32,
    broker_delay_us=0,
    workers=1,
    deadline_ms=None,
    journal_root=None,
    timeout_seconds=180,
    allow_dirty=False,
) -> dict:
    if not rates or len(rates) > 10 or len(rates) != len(set(rates)):
        raise ValueError("provide one to ten unique positive rates")
    if any(not math.isfinite(r) or not 0 < r <= 10**6 for r in rates):
        raise ValueError("rates must be positive and finite")
    if not 1 <= repeats <= 10 or not 1 <= orders <= 100000:
        raise ValueError("invalid trial bounds")
    source = revision()
    if source["working_tree_dirty"] and not allow_dirty:
        raise ValueError(
            "capacity evidence requires a clean commit; use --allow-dirty only for development"
        )
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    trials = []
    for rate in [*rates, 0.0]:
        for repeat in range(repeats):
            target = output / f"trial-{len(trials):03d}-{uuid4().hex}"
            target.mkdir()
            command = [
                sys.executable,
                "-m",
                "tools.benchmarks.offline_pipeline",
                "--orders",
                str(orders),
                "--batch-size",
                str(batch_size),
                "--capacity",
                str(capacity),
                "--workers",
                str(workers),
                "--broker-delay-us",
                str(broker_delay_us),
                "--target-rate",
                str(rate),
                "--output",
                str(target / "measurement.json"),
            ]
            if deadline_ms is not None:
                command += ["--deadline-ms", str(deadline_ms)]
            if journal_root is not None:
                command += ["--journal-root", str(Path(journal_root).resolve())]
            result = run_bounded(command, target / "trial.log", timeout_seconds)
            result.update(target_rate_per_second=rate, repeat=repeat + 1)
            measurement = target / "measurement.json"
            if result["status"] == "passed":
                try:
                    value = json.loads(measurement.read_text(encoding="utf-8"))
                    if value["source"] != source:
                        raise ValueError("source changed during the matrix")
                    if value["offered"] != value["completed"] + value["rejected"]:
                        raise ValueError("unaccounted requests")
                    if value["fake_broker_calls"] != value["completed"]:
                        raise ValueError("broker call mismatch")
                    if (
                        value["backend"]["engine"] != "cpp20"
                        or value["backend"]["cxx_standard"] != 202002
                    ):
                        raise ValueError("native C++20 backend required for this baseline")
                    result["measurement"] = value
                except (OSError, ValueError, KeyError) as exc:
                    result.update(status="failed", reason=str(exc))
            trials.append(result)
            # Preserve progress even when a later process fails or times out.
            payload = {
                "source": source,
                "scope": "offline fsync component chain; independent process per trial; not live SDK/fence/network acceptance",
                "status": ("passed" if len(trials) == (len(rates) + 1) * repeats else "running")
                if all(t["status"] == "passed" for t in trials)
                else "failed",
                "expected_trials": (len(rates) + 1) * repeats,
                "finished_trials": len(trials),
                "threshold_status": "no production SLA asserted; process/accounting verification only",
                "percentile_method": "nearest rank per trial; summary min/median/max of trial P99, not pooled P99",
                "sample_limit": "bounded sample; not a long-duration soak or statistically established production capacity",
                "summary": summarize(trials),
                "trials": trials,
            }
            (output / "capacity-report.json").write_text(
                json.dumps(payload, indent=2) + "\n", encoding="utf-8"
            )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "build/capacity")
    parser.add_argument("--rates", nargs="+", type=float, default=[25, 50, 100, 200])
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--capacity", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--broker-delay-us", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--deadline-ms", type=float)
    parser.add_argument("--journal-root", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=180)
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()
    result = run_matrix(
        output=args.output_dir, **{k: v for k, v in vars(args).items() if k != "output_dir"}
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "finished_trials": result["finished_trials"],
                "output": str(args.output_dir),
            }
        )
    )
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
