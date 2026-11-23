"""Measure reproducible local hot-path TPS/QPS capacity boundaries.

This driver deliberately exercises the isolated EMS/strategy contracts only.  It
does not import an adapter or an EMT SDK and it cannot issue an order.  Its
results therefore describe the local service-side hot-path boundary, not venue
order acceptance capacity.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .run_e0_s5 import _hdr, _request, _reset

ROOT = Path(__file__).resolve().parent
K6 = ROOT / "k6"


@dataclass(frozen=True)
class CapacityWorkload:
    """One independently measurable local hot path."""

    identifier: str
    script: str
    environment_key: str
    rates: tuple[int, ...]


def _rates(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "rates must be comma-separated positive integers"
        ) from error
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("rates must contain at least one positive integer")
    if tuple(sorted(parsed)) != parsed or len(set(parsed)) != len(parsed):
        raise argparse.ArgumentTypeError("rates must be strictly increasing")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure local EMS/strategy hot-path capacity")
    parser.add_argument(
        "--base-url", required=True, help="one URL or comma-separated isolated shards"
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--k6-bin", default=os.environ.get("K6_BIN", "k6"))
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_EMS_STR_PERF_FAULT_KEY"))
    parser.add_argument("--duration", default="30s", help="steady duration for every trial")
    parser.add_argument(
        "--signal-rates",
        type=_rates,
        default=(2_000, 4_000, 8_000, 12_000, 16_000),
        help="strictly increasing S1 strategy-signal TPS steps",
    )
    parser.add_argument(
        "--event-rates",
        type=_rates,
        default=(5_000, 10_000, 20_000, 30_000, 40_000),
        help="strictly increasing S2 checkpointed-event TPS steps",
    )
    parser.add_argument(
        "--query-rates",
        type=_rates,
        default=(5_000, 10_000, 20_000, 30_000, 40_000),
        help="strictly increasing S3 immutable-snapshot QPS steps",
    )
    parser.add_argument(
        "--workload",
        action="append",
        choices=("H1-SIGNAL", "H2-EVENT", "H3-QUERY"),
        help="run only the named workload; repeat to select more than one",
    )
    parser.add_argument(
        "--key-distribution",
        action="append",
        choices=("uniform", "hot"),
        help="repeat to restrict distributions; default executes both uniform and hot contracts",
    )
    parser.add_argument("--hot-key-ratio-percent", type=int, default=80)
    parser.add_argument(
        "--confirm-runs",
        type=int,
        default=3,
        help="independent passes required for the last stable step",
    )
    parser.add_argument(
        "--signal-preallocated-vus",
        type=int,
        default=2_000,
        help="k6 VUs reserved before H1 starts; prevents startup scaling from defining capacity",
    )
    parser.add_argument(
        "--event-preallocated-vus",
        type=int,
        default=4_000,
        help="k6 VUs reserved before H2 starts; prevents startup scaling from defining capacity",
    )
    parser.add_argument(
        "--query-preallocated-vus",
        type=int,
        default=4_000,
        help="k6 VUs reserved before H3 starts; prevents startup scaling from defining capacity",
    )
    parser.add_argument(
        "--minimum-nofile",
        type=int,
        default=8_192,
        help="minimum Linux sidecar RLIMIT_NOFILE; use 0 only for a smoke run",
    )
    return parser


def _k6(binary: str) -> str:
    resolved = shutil.which(binary)
    if resolved is None:
        raise RuntimeError("k6 is not installed or not on PATH")
    return resolved


def _run(command: Sequence[str], environment: dict[str, str]) -> int:
    """Run k6 in its own process group so an interrupted driver leaves no load behind."""

    process = subprocess.Popen(
        command,
        cwd=ROOT.parents[1],
        env=environment,
        start_new_session=os.name == "posix",
    )
    try:
        return process.wait()
    except KeyboardInterrupt:
        if process.poll() is None:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=10)
        raise


def _interrupt_as_keyboard_interrupt(_: int, __: object) -> None:
    """Give the active k6 process group a chance to receive driver shutdown."""

    raise KeyboardInterrupt


def _summary_metrics(path: Path) -> dict[str, object]:
    """Return only the payload-free completion/error values used for capacity evidence."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return {}
    selected: dict[str, object] = {}
    for key in ("http_reqs", "http_req_failed", "dropped_iterations", "checks"):
        value = metrics.get(key)
        if isinstance(value, dict):
            selected[key] = {
                item: value[item]
                for item in ("count", "rate", "value", "passes", "fails", "thresholds")
                if item in value
            }
    return selected


def _summary_threshold_failures(path: Path) -> list[str]:
    """Return every threshold crossed in a k6 summary export.

    k6 normally returns 99 for a crossed threshold, but the persisted summary
    is the durable measurement source.  Capacity classification must not trust
    a process exit code alone.
    """

    payload = json.loads(path.read_text(encoding="utf-8"))
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return ["missing-metrics"]
    failures: list[str] = []
    for metric_name, metric in metrics.items():
        if not isinstance(metric_name, str) or not isinstance(metric, dict):
            continue
        thresholds = metric.get("thresholds")
        if not isinstance(thresholds, dict):
            continue
        for expression, crossed in thresholds.items():
            if crossed is True:
                failures.append(f"{metric_name}:{expression}")
    return failures


def _health(urls: Sequence[str], minimum_nofile: int) -> list[dict[str, object]]:
    health: list[dict[str, object]] = []
    for url in urls:
        status, payload = _request(f"{url}/perf/v1/ems/health")
        if status != 200 or not isinstance(payload, dict) or payload.get("status") != "READY":
            raise RuntimeError("hot-capacity preflight is not READY")
        limit = payload.get("open_file_soft_limit")
        if minimum_nofile and (not isinstance(limit, int) or limit < minimum_nofile):
            raise RuntimeError(f"sidecar RLIMIT_NOFILE is below required minimum {minimum_nofile}")
        health.append(payload)
    return health


def _trial(
    *,
    workload: CapacityWorkload,
    rate: int,
    ordinal: int,
    k6: str,
    urls: Sequence[str],
    output_dir: Path,
    environment: dict[str, str],
    fault_key: str,
    key_distribution: str,
    hot_key_ratio_percent: int,
) -> dict[str, object]:
    """Run an isolated plateau and retain both k6 and server-side evidence."""

    _reset(urls, fault_key)
    stem = f"{workload.identifier.lower()}-{key_distribution}-{rate}-trial-{ordinal}"
    summary = output_dir / f"{stem}-k6-summary.json"
    scoped_environment = dict(environment)
    scoped_environment[workload.environment_key] = str(rate)
    scoped_environment["KEY_DISTRIBUTION"] = key_distribution
    scoped_environment["HOT_KEY_RATIO_PERCENT"] = str(hot_key_ratio_percent)
    code = _run(
        [
            k6,
            "run",
            "--summary-trend-stats=avg,min,med,p(90),p(95),p(99),p(99.9),max",
            "--summary-export",
            str(summary),
            str(K6 / workload.script),
        ],
        scoped_environment,
    )
    hdr_files: list[str] = []
    for shard, url in enumerate(urls):
        hdr_file = output_dir / f"{stem}-hdr-shard-{shard}.json"
        hdr_file.write_text(
            json.dumps(_hdr(url), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        hdr_files.append(hdr_file.name)
    threshold_failures = (
        _summary_threshold_failures(summary) if summary.exists() else ["missing-summary"]
    )
    passed = code == 0 and not threshold_failures
    return {
        "rate": rate,
        "trial": ordinal,
        "exit_code": code,
        "status": "PASS" if passed else "FAIL",
        "summary": summary.name,
        "summary_metrics": _summary_metrics(summary) if summary.exists() else {},
        "threshold_failures": threshold_failures,
        "hdr": hdr_files,
    }


def _run_workload(
    *,
    workload: CapacityWorkload,
    k6: str,
    urls: Sequence[str],
    output_dir: Path,
    environment: dict[str, str],
    fault_key: str,
    confirm_runs: int,
    key_distribution: str,
    hot_key_ratio_percent: int,
) -> dict[str, object]:
    """Find the first offered-load failure, then independently confirm its predecessor."""

    trials: list[dict[str, object]] = []
    stable_rate: int | None = None
    first_unstable_rate: int | None = None
    for rate in workload.rates:
        trial = _trial(
            workload=workload,
            rate=rate,
            ordinal=1,
            k6=k6,
            urls=urls,
            output_dir=output_dir,
            environment=environment,
            fault_key=fault_key,
            key_distribution=key_distribution,
            hot_key_ratio_percent=hot_key_ratio_percent,
        )
        trials.append(trial)
        if trial["status"] == "PASS":
            stable_rate = rate
            continue
        first_unstable_rate = rate
        break
    confirmation: list[dict[str, object]] = []
    if stable_rate is not None:
        for ordinal in range(2, confirm_runs + 1):
            trial = _trial(
                workload=workload,
                rate=stable_rate,
                ordinal=ordinal,
                k6=k6,
                urls=urls,
                output_dir=output_dir,
                environment=environment,
                fault_key=fault_key,
                key_distribution=key_distribution,
                hot_key_ratio_percent=hot_key_ratio_percent,
            )
            confirmation.append(trial)
        confirmed = len(confirmation) == confirm_runs - 1 and all(
            item["status"] == "PASS" for item in confirmation
        )
    else:
        confirmed = False
    all_trials = [*trials, *confirmation]
    reached_last_step = stable_rate == workload.rates[-1] and first_unstable_rate is None
    return {
        "workload": workload.identifier,
        "data_distribution": {
            "key_distribution": key_distribution,
            "hot_key_ratio_percent": hot_key_ratio_percent,
        },
        "semantic_boundary": {
            "H1-SIGNAL": "strategy signal submitted to EMS admission",
            "H2-EVENT": "synthetic contract event to strategy checkpoint/state apply",
            "H3-QUERY": "strategy immutable snapshot request/response",
        }[workload.identifier],
        "rates": list(workload.rates),
        "trials": all_trials,
        "highest_three_run_stable_rate": stable_rate if confirmed else None,
        "first_unstable_offered_rate": first_unstable_rate,
        "saturation_bound": (
            f">= {stable_rate}"
            if reached_last_step and confirmed
            else f"{stable_rate}..{first_unstable_rate}"
            if stable_rate is not None and first_unstable_rate is not None and confirmed
            else "unproven"
        ),
        "status": "PASS" if confirmed else "FAIL",
        "note": (
            "last configured step remained stable; this is a lower bound, not a hard saturation claim"
            if reached_last_step and confirmed
            else "first k6 contract failure is an offered-load instability boundary; inspect retained HDR and summary before attributing it to a server component"
        ),
    }


def main() -> int:
    args = _parser().parse_args()
    if not args.fault_key:
        print("--fault-key is required", file=sys.stderr)
        return 2
    if args.confirm_runs < 1:
        print("--confirm-runs must be at least 1", file=sys.stderr)
        return 2
    if args.minimum_nofile < 0:
        print("--minimum-nofile must be non-negative", file=sys.stderr)
        return 2
    if not 1 <= args.hot_key_ratio_percent <= 100:
        print("--hot-key-ratio-percent must be in 1..100", file=sys.stderr)
        return 2
    if (
        min(
            args.signal_preallocated_vus,
            args.event_preallocated_vus,
            args.query_preallocated_vus,
        )
        < 1
    ):
        print("all preallocated VU values must be positive", file=sys.stderr)
        return 2
    try:
        k6 = _k6(args.k6_bin)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 2
    urls = tuple(item.strip().rstrip("/") for item in args.base_url.split(",") if item.strip())
    if not urls:
        print("--base-url must not be empty", file=sys.stderr)
        return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment.update(
        {
            "BASE_URL": urls[0],
            "BASE_URLS": ",".join(urls),
            "DURATION": args.duration,
            "FAULT_KEY": args.fault_key,
            "SCOPES": "1" if len(urls) > 1 else environment.get("SCOPES", "4"),
            "SIGNAL_PREALLOCATED_VUS": str(args.signal_preallocated_vus),
            "SIGNAL_MAX_VUS": str(args.signal_preallocated_vus),
            "EVENT_PREALLOCATED_VUS": str(args.event_preallocated_vus),
            "EVENT_MAX_VUS": str(args.event_preallocated_vus),
            "QUERY_PREALLOCATED_VUS": str(args.query_preallocated_vus),
            "QUERY_MAX_VUS": str(args.query_preallocated_vus),
        }
    )
    all_workloads = (
        CapacityWorkload("H1-SIGNAL", "s1_signal_2000_tps.js", "SIGNAL_TPS", args.signal_rates),
        CapacityWorkload("H2-EVENT", "s2_event_5000_tps.js", "EVENT_TPS", args.event_rates),
        CapacityWorkload("H3-QUERY", "s3_snapshot_5000_qps.js", "SNAPSHOT_QPS", args.query_rates),
    )
    selected_workloads = set(args.workload or ())
    workloads = tuple(
        workload
        for workload in all_workloads
        if not selected_workloads or workload.identifier in selected_workloads
    )
    key_distributions = tuple(args.key_distribution or ("uniform", "hot"))
    result: dict[str, object] = {
        "schema": "TradeCoreLocalHotPathCapacityV1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_urls": list(urls),
        "duration": args.duration,
        "confirm_runs": args.confirm_runs,
        "generator_vu_capacity": {
            "signal": args.signal_preallocated_vus,
            "event": args.event_preallocated_vus,
            "query": args.query_preallocated_vus,
        },
        "selected_workloads": [workload.identifier for workload in workloads],
        "key_distributions": list(key_distributions),
        "hot_key_ratio_percent": args.hot_key_ratio_percent,
        "external_venue_calls": False,
        "workloads": [],
    }
    previous_sigterm = signal.signal(signal.SIGTERM, _interrupt_as_keyboard_interrupt)
    try:
        try:
            result["preflight"] = {
                "status": "PASS",
                "health": _health(urls, args.minimum_nofile),
            }
            workload_results = [
                _run_workload(
                    workload=workload,
                    k6=k6,
                    urls=urls,
                    output_dir=args.output_dir,
                    environment=environment,
                    fault_key=args.fault_key,
                    confirm_runs=args.confirm_runs,
                    key_distribution=key_distribution,
                    hot_key_ratio_percent=args.hot_key_ratio_percent,
                )
                for key_distribution in key_distributions
                for workload in workloads
            ]
            result["workloads"] = workload_results
            result["status"] = (
                "PASS" if all(item["status"] == "PASS" for item in workload_results) else "FAIL"
            )
        except KeyboardInterrupt:
            result["status"] = "ABORTED"
            result["error"] = "interrupted; active k6 process group was terminated"
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
            result["status"] = "FAIL"
            result["error"] = str(error)
        output = args.output_dir / "hot-path-capacity-result.json"
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "PASS" else 1
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


if __name__ == "__main__":
    raise SystemExit(main())
