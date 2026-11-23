"""Run W7 only after a fresh zero-unfinished EMT cleanup proof.

The executable EMT command is supplied as a reviewed JSON argv list so this
tool never reads accounts, passwords, order plans or SDK library paths.  The
same process starts k6 W3 and the existing real 499-burst driver together.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import sleep, time
from typing import Any, Sequence
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
K6_W3 = ROOT / "k6" / "w3_mixed_2000_tps_5000_qps.js"
_CPU_LIST = re.compile(r"^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$")


class W7GateError(RuntimeError):
    """A mandatory W7 evidence or execution constraint was not met."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run OMS W7 alongside one real EMT 499 burst")
    parser.add_argument(
        "--base-url", required=True, help="one or comma-separated OMS scope-shard URLs"
    )
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_OMS_PERF_FAULT_KEY"))
    parser.add_argument("--cleanup-report", type=Path, required=True)
    parser.add_argument("--emt-command", type=Path, required=True)
    parser.add_argument("--emt-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--k6-bin", default=os.environ.get("K6_BIN", "k6"))
    parser.add_argument(
        "--k6-cpu-list",
        help=(
            "optional taskset CPU list for the k6 load generator; use CPUs disjoint from "
            "the pinned EMT session scopes and OMS sidecars"
        ),
    )
    parser.add_argument("--duration", default="60s")
    parser.add_argument("--fact-tps", type=int, default=2_000)
    parser.add_argument("--snapshot-qps", type=int, default=5_000)
    parser.add_argument("--max-cleanup-proof-age-seconds", type=int, default=120)
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise W7GateError(f"cannot read required JSON: {path.name}") from error
    if not isinstance(value, dict):
        raise W7GateError(f"required JSON object is invalid: {path.name}")
    return value


def _assert_cleanup_gate(path: Path, maximum_age_seconds: int) -> dict[str, Any]:
    if maximum_age_seconds < 1 or not path.is_file():
        raise W7GateError("cleanup proof is absent")
    age = time() - path.stat().st_mtime
    if age > maximum_age_seconds:
        raise W7GateError("cleanup proof is stale; run a fresh query-only proof")
    report = _load_json(path)
    run = report.get("run")
    if not isinstance(run, dict):
        raise W7GateError("cleanup proof lacks a recovery run")
    if (
        run.get("query_complete") is not True
        or run.get("outstanding_order_count") != 0
        or run.get("cancellation_attempts") != 0
    ):
        raise W7GateError("cleanup proof does not show a fresh query-only zero-unfinished result")
    return report


def _load_command(path: Path) -> tuple[str, ...]:
    value = _load_json(path).get("argv")
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise W7GateError("EMT command must contain a non-empty string argv array")
    return tuple(value)


def _assert_emt_command_output(command: Sequence[str], expected_report: Path) -> None:
    """Prevent a live burst whose persisted evidence cannot be checked by W7."""

    output_positions = [index for index, item in enumerate(command) if item == "--output"]
    if len(output_positions) != 1:
        raise W7GateError("EMT command must contain exactly one --output argument")
    output_position = output_positions[0]
    if output_position + 1 >= len(command):
        raise W7GateError("EMT command --output is missing its report path")
    configured_report = Path(command[output_position + 1])
    if configured_report.resolve() != expected_report.resolve():
        raise W7GateError("EMT command --output does not match --emt-report")


def _k6_cpu_list(value: str | None) -> str | None:
    if value is None:
        return None
    if _CPU_LIST.fullmatch(value) is None:
        raise W7GateError("--k6-cpu-list must be a taskset CPU list")
    return value


def _base_urls(value: str) -> tuple[str, ...]:
    urls = tuple(item.strip().rstrip("/") for item in value.split(",") if item.strip())
    if not urls:
        raise W7GateError("at least one OMS base URL is required")
    return urls


def _assert_emt_499_report(path: Path) -> dict[str, object]:
    report = _load_json(path)
    outcomes = report.get("outcomes")
    verification = report.get("verification")
    if (
        report.get("burst_width") != 499
        or not isinstance(outcomes, dict)
        or not isinstance(verification, dict)
    ):
        raise W7GateError("EMT result lacks the 499-burst contract")
    required_counts = (
        "native_accepted",
        "callback_completed_C7",
        "callback_seen_downstream",
        "callback_complete_C5_to_C7_pairs",
        "cleanup_cancel_attempted",
        "cleanup_cancel_accepted",
    )
    if any(outcomes.get(name) != 499 for name in required_counts):
        raise W7GateError("EMT result does not prove every 499-burst lifecycle boundary")
    if (
        report.get("complete") is not True
        or verification.get("full_acceptance_complete") is not True
        or outcomes.get("callback_gaps") != 0
        or outcomes.get("cleanup_terminal_callback_complete") is not True
        or outcomes.get("cleanup_terminal_inflight_remaining") != 0
    ):
        raise W7GateError("EMT result is incomplete, has callback gaps, or lacks terminal cleanup")
    return {
        "burst_width": 499,
        "native_accepted": 499,
        "callback_completed_C7": 499,
        "callback_gaps": 0,
        "cleanup_terminal_callback_complete": True,
    }


def _fetch_hdr(base_url: str) -> dict[str, Any]:
    try:
        with urlopen(f"{base_url.rstrip('/')}/perf/v1/metrics/hdr", timeout=10) as response:  # noqa: S310
            value = json.loads(response.read().decode("utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise W7GateError("cannot obtain W7 HDR metrics from the OMS platform") from error
    if not isinstance(value, dict) or value.get("format") != "HDRHistogramV1":
        raise W7GateError("W7 HDR metrics have an unexpected contract")
    return value


def _reset_oms_shards(base_urls: Sequence[str], fault_key: str | None) -> None:
    if not fault_key:
        raise W7GateError("W7 requires a fault key to establish a clean OMS baseline")
    for base_url in base_urls:
        request = Request(
            f"{base_url}/perf/v1/admin/fault/reset-benchmark",
            data=b'{"scope_id":"bench-scope-0"}',
            headers={
                "Content-Type": "application/json",
                "X-TradeCore-Perf-Fault-Key": fault_key,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=15) as response:  # noqa: S310 - explicit operator endpoint
                if response.status != 200:
                    raise W7GateError("OMS baseline reset was rejected")
            metric_request = Request(f"{base_url}/perf/v1/metrics/reset", data=b"", method="POST")
            with urlopen(metric_request, timeout=15) as response:  # noqa: S310 - explicit operator endpoint
                if response.status != 204:
                    raise W7GateError("OMS HDR reset was rejected")
        except OSError as error:
            raise W7GateError("cannot establish a clean OMS baseline") from error


def _wait_for_oms_admission(base_urls: Sequence[str], timeout_seconds: float = 10.0) -> None:
    """Ensure k6 has admitted live W3 facts before the EMT burst starts."""

    deadline = time() + timeout_seconds
    while time() < deadline:
        try:
            if all(
                int(_fetch_hdr(url).get("counters", {}).get("fact_admitted_total", 0)) > 0
                for url in base_urls
            ):
                return
        except (OSError, W7GateError, ValueError):
            pass
        sleep(0.02)
    raise W7GateError("W3 did not admit facts on every OMS shard before EMT launch")


def _run(command: Sequence[str], *, environment: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(command, cwd=ROOT.parents[1], env=environment)


def main() -> int:
    args = _parser().parse_args()
    try:
        cleanup = _assert_cleanup_gate(args.cleanup_report, args.max_cleanup_proof_age_seconds)
        command = _load_command(args.emt_command)
        _assert_emt_command_output(command, args.emt_report)
        base_urls = _base_urls(args.base_url)
        k6 = shutil.which(args.k6_bin)
        if k6 is None:
            raise W7GateError("k6 is not installed or not on PATH")
        k6_cpu_list = _k6_cpu_list(args.k6_cpu_list)
        taskset = shutil.which("taskset") if k6_cpu_list is not None else None
        if k6_cpu_list is not None and taskset is None:
            raise W7GateError("--k6-cpu-list requires taskset on PATH")
    except W7GateError as error:
        print(f"W7 BLOCKED: {error}", file=sys.stderr)
        return 3
    args.output_dir.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment.update(
        {
            "BASE_URL": base_urls[0],
            "BASE_URLS": ",".join(base_urls),
            "SCOPES": "1" if len(base_urls) > 1 else environment.get("SCOPES", "4"),
            "DURATION": args.duration,
            "FACT_TPS": str(args.fact_tps),
            "SNAPSHOT_QPS": str(args.snapshot_qps),
        }
    )
    k6_summary = args.output_dir / "w7-k6-w3-summary.json"
    try:
        _reset_oms_shards(base_urls, args.fault_key)
    except W7GateError as error:
        print(f"W7 BLOCKED: {error}", file=sys.stderr)
        return 3
    k6_command = [
        k6,
        "run",
        "--summary-trend-stats=avg,min,med,p(90),p(95),p(99),p(99.9),max",
        "--summary-export",
        str(k6_summary),
        str(K6_W3),
    ]
    if k6_cpu_list is not None:
        assert taskset is not None
        k6_command = [taskset, "-c", k6_cpu_list, *k6_command]
    k6_process = _run(k6_command, environment=environment)
    try:
        _wait_for_oms_admission(base_urls)
    except W7GateError as error:
        k6_process.terminate()
        k6_process.wait()
        print(f"W7 FAIL: {error}", file=sys.stderr)
        return 2
    emt_process = _run(command, environment=environment)
    emt_code = emt_process.wait()
    k6_code = k6_process.wait()
    try:
        hdr = [_fetch_hdr(url) for url in base_urls]
        emt_evidence = _assert_emt_499_report(args.emt_report)
    except W7GateError as error:
        print(f"W7 FAIL: {error}", file=sys.stderr)
        return 2
    result = {
        "schema": "TradeCoreOmsW7ConcurrentResultV1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cleanup_proof": args.cleanup_report.name,
        "cleanup_query_complete": cleanup["run"]["query_complete"],
        "cleanup_unfinished_orders": cleanup["run"]["outstanding_order_count"],
        "fact_tps_target": args.fact_tps,
        "snapshot_qps_target": args.snapshot_qps,
        "k6_cpu_list": k6_cpu_list,
        "oms_base_urls": base_urls,
        "emt_499_command_exit_code": emt_code,
        "emt_499_report": args.emt_report.name,
        "emt_499_evidence": emt_evidence,
        "k6_w3_exit_code": k6_code,
        "k6_summary": k6_summary.name,
        "hdr_by_shard": hdr,
        "status": "PASS" if emt_code == 0 and k6_code == 0 else "FAIL",
    }
    (args.output_dir / "w7-concurrent-result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
