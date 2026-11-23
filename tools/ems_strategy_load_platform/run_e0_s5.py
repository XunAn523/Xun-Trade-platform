"""Run the BenchmarkStandardV1 EMS E0--E5 and strategy S0--S5 gates."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence
from urllib.request import Request, urlopen

from tools.performance.benchmark_evidence_v11 import (
    BenchmarkEvidenceV11Error,
    write_platform_gate_evidence,
)

ROOT = Path(__file__).resolve().parent
K6 = ROOT / "k6"
WORKLOADS = (
    ("E1", "e1_plan_2000_tps.js", True),
    ("E2", "e2_plan_query_5000_qps.js", False),
    ("E3", "e3_child_2000_tps.js", True),
    ("E4", "e4_control_500_tps.js", False),
    ("E5", "e5_mixed.js", True),
    ("S1", "s1_signal_2000_tps.js", True),
    ("S2", "s2_event_5000_tps.js", False),
    ("S3", "s3_snapshot_5000_qps.js", False),
    ("S5", "s5_mixed.js", True),
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EMS/strategy BenchmarkStandardV1 gates")
    parser.add_argument(
        "--base-url", required=True, help="one URL or comma-separated isolated shards"
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--k6-bin", default=os.environ.get("K6_BIN", "k6"))
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_EMS_STR_PERF_FAULT_KEY"))
    parser.add_argument("--duration", default="30s")
    parser.add_argument("--plan-tps", type=int, default=2_000)
    parser.add_argument("--query-qps", type=int, default=5_000)
    parser.add_argument("--control-tps", type=int, default=500)
    parser.add_argument("--event-tps", type=int, default=5_000)
    parser.add_argument("--key-distribution", choices=("uniform", "hot"), default="uniform")
    parser.add_argument("--hot-key-ratio-percent", type=int, default=80)
    parser.add_argument(
        "--benchmark-profile",
        type=Path,
        help="optional BenchmarkProfileV11; emits V2/V3/V5 evidence next to the k6 report",
    )
    parser.add_argument(
        "--minimum-nofile",
        type=int,
        default=8_192,
        help="minimum Linux sidecar RLIMIT_NOFILE; use 0 for a non-Linux smoke run",
    )
    return parser


def _request(
    url: str, *, body: dict[str, object] | None = None, key: str | None = None
) -> tuple[int, dict[str, object] | None]:
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-TradeCore-Perf-Fault-Key"] = key
    request = Request(url, data=data, headers=headers, method="POST" if body is not None else "GET")
    with urlopen(request, timeout=15) as response:  # noqa: S310 - controlled performance endpoint
        raw = response.read()
        return response.status, json.loads(raw.decode("utf-8")) if raw else None


def _k6(binary: str) -> str:
    resolved = shutil.which(binary)
    if resolved is None:
        raise RuntimeError("k6 is not installed or not on PATH")
    return resolved


def _run(command: Sequence[str], environment: dict[str, str]) -> int:
    return subprocess.run(command, cwd=ROOT.parents[1], env=environment, check=False).returncode


def _reset(urls: Sequence[str], key: str) -> None:
    for url in urls:
        status, _ = _request(f"{url}/perf/v1/admin/benchmark/reset", body={}, key=key)
        if status != 200:
            raise RuntimeError(f"benchmark reset returned HTTP {status}")
        status, _ = _request(f"{url}/perf/v1/metrics/reset", body={})
        if status != 204:
            raise RuntimeError(f"metrics reset returned HTTP {status}")


def _hdr(url: str) -> dict[str, object]:
    status, payload = _request(f"{url}/perf/v1/metrics/hdr")
    if status != 200 or not isinstance(payload, dict) or payload.get("format") != "HDRHistogramV1":
        raise RuntimeError("HDR response is invalid")
    return payload


def main() -> int:
    args = _parser().parse_args()
    if not args.fault_key:
        print("--fault-key is required", file=sys.stderr)
        return 2
    if args.minimum_nofile < 0:
        print("--minimum-nofile must be non-negative", file=sys.stderr)
        return 2
    try:
        k6 = _k6(args.k6_bin)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 2
    urls: list[str] = [
        item.strip().rstrip("/") for item in args.base_url.split(",") if item.strip()
    ]
    if not urls:
        print("--base-url must not be empty", file=sys.stderr)
        return 2
    base_url = urls[0]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    environment.update(
        {
            "BASE_URL": base_url,
            "BASE_URLS": ",".join(urls),
            "DURATION": args.duration,
            "PLAN_TPS": str(args.plan_tps),
            "CHILD_TPS": str(args.plan_tps),
            "SIGNAL_TPS": str(args.plan_tps),
            "PLAN_QUERY_QPS": str(args.query_qps),
            "SNAPSHOT_QPS": str(args.query_qps),
            "CONTROL_TPS": str(args.control_tps),
            "EVENT_TPS": str(args.event_tps),
            "FAULT_KEY": args.fault_key,
            "SCOPES": "1" if len(urls) > 1 else environment.get("SCOPES", "4"),
            "KEY_DISTRIBUTION": args.key_distribution,
            "HOT_KEY_RATIO_PERCENT": str(args.hot_key_ratio_percent),
        }
    )
    workloads: list[dict[str, object]] = []
    result: dict[str, object] = {
        "schema": "TradeCoreEmsStrategyGateV1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_urls": urls,
        "data_distribution": {
            "key_distribution": args.key_distribution,
            "hot_key_ratio_percent": args.hot_key_ratio_percent,
        },
        "workloads": workloads,
    }
    try:
        health = []
        for url in urls:
            status, payload = _request(f"{url}/perf/v1/ems/health")
            if status != 200 or not isinstance(payload, dict) or payload.get("status") != "READY":
                raise RuntimeError("E0/S0 health preflight is not READY")
            nofile = payload.get("open_file_soft_limit")
            if args.minimum_nofile and (
                not isinstance(nofile, int) or nofile < args.minimum_nofile
            ):
                raise RuntimeError(
                    f"E0/S0 sidecar RLIMIT_NOFILE is below required minimum {args.minimum_nofile}"
                )
            health.append(payload)
        result["preflight"] = {"status": "PASS", "health": health}
        for gate, script, reset_before in WORKLOADS:
            if reset_before:
                _reset(urls, args.fault_key)
            summary = args.output_dir / f"{gate.lower()}-k6-summary.json"
            code = _run(
                [
                    k6,
                    "run",
                    "--summary-trend-stats=avg,min,med,p(90),p(95),p(99),p(99.9),max",
                    "--summary-export",
                    str(summary),
                    str(K6 / script),
                ],
                environment,
            )
            hdr_files: list[str] = []
            if code == 0:
                for index, url in enumerate(urls):
                    hdr_file = args.output_dir / f"{gate.lower()}-hdr-shard-{index}.json"
                    hdr_file.write_text(
                        json.dumps(_hdr(url), indent=2, sort_keys=True) + "\n", encoding="utf-8"
                    )
                    hdr_files.append(hdr_file.name)
            workloads.append(
                {"gate": gate, "exit_code": code, "summary": summary.name, "hdr": hdr_files}
            )
        fault_output = args.output_dir / "e4-s4-fault-drill.json"
        code = _run(
            [
                sys.executable,
                "-m",
                "tools.ems_strategy_load_platform.run_fault_drills",
                "--base-url",
                base_url,
                "--fault-key",
                args.fault_key,
                "--output",
                str(fault_output),
            ],
            environment,
        )
        result["fault_drill"] = {"exit_code": code, "evidence": fault_output.name}
        success = all(item["exit_code"] == 0 for item in workloads) and code == 0
        result["status"] = "PASS" if success else "FAIL"
    except (OSError, RuntimeError, ValueError) as error:
        result["status"] = "FAIL"
        result["error"] = str(error)
    (args.output_dir / "e0-s5-gate-result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if args.benchmark_profile is not None:
        try:
            paths = write_platform_gate_evidence(
                profile_path=args.benchmark_profile,
                report=result,
                platform="EMS_STRATEGY",
                output_dir=args.output_dir,
            )
            result["v11_gate_evidence"] = {gate: path.name for gate, path in paths.items()}
        except BenchmarkEvidenceV11Error as error:
            result["status"] = "FAIL"
            result["v11_evidence_error"] = str(error)
        (args.output_dir / "e0-s5-gate-result.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
