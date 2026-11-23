"""Run the k6 W0--W5 automatic gates and retain the native k6 summaries."""

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
    ("W0", "w0_contract.js"),
    ("W1", "w1_fact_2000_tps.js"),
    ("W2", "w2_snapshot_5000_qps.js"),
    ("W3", "w3_mixed_2000_tps_5000_qps.js"),
    ("W4", "w4_watermark_health.js"),
    ("W5", "w5_fail_closed.js"),
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute OMS W0--W5 k6 gates")
    parser.add_argument(
        "--base-url",
        required=True,
        help="one URL or a comma-separated set of independently pinned scope-shard URLs",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--k6-bin", default=os.environ.get("K6_BIN", "k6"))
    parser.add_argument("--fault-key", default=os.environ.get("TRADECORE_OMS_PERF_FAULT_KEY"))
    parser.add_argument("--duration", default="60s")
    parser.add_argument("--fact-tps", type=int, default=2_000)
    parser.add_argument("--snapshot-qps", type=int, default=5_000)
    parser.add_argument("--key-distribution", choices=("uniform", "hot"), default="uniform")
    parser.add_argument("--hot-key-ratio-percent", type=int, default=80)
    parser.add_argument(
        "--benchmark-profile",
        type=Path,
        help="optional BenchmarkProfileV11; emits V2/V3/V5 evidence next to the k6 report",
    )
    return parser


def _git_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT.parents[1], text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _k6_command(binary: str) -> str:
    resolved = shutil.which(binary)
    if resolved is None:
        raise RuntimeError(
            "k6 is not installed or not on PATH; install Grafana k6, then rerun this gate"
        )
    return resolved


def _run(command: Sequence[str], *, environment: dict[str, str]) -> int:
    return subprocess.run(command, cwd=ROOT.parents[1], env=environment, check=False).returncode


def _admin_post(base_url: str, action: str, fault_key: str) -> dict[str, object]:
    request = Request(
        f"{base_url}/perf/v1/admin/fault/{action}",
        data=b'{"scope_id":"bench-scope-0"}',
        headers={
            "Content-Type": "application/json",
            "X-TradeCore-Perf-Fault-Key": fault_key,
        },
        method="POST",
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310 - operator supplied endpoint
        if response.status != 200:
            raise RuntimeError(f"{action} returned HTTP {response.status}")
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{action} returned a non-object payload")
    return payload


def _reset_metrics(base_url: str) -> None:
    request = Request(f"{base_url}/perf/v1/metrics/reset", data=b"", method="POST")
    with urlopen(request, timeout=15) as response:  # noqa: S310 - operator supplied endpoint
        if response.status != 204:
            raise RuntimeError(f"metrics reset returned HTTP {response.status}")


def _capture_hdr(base_url: str) -> dict[str, object]:
    with urlopen(f"{base_url}/perf/v1/metrics/hdr", timeout=15) as response:  # noqa: S310
        if response.status != 200:
            raise RuntimeError(f"HDR metrics returned HTTP {response.status}")
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "HDRHistogramV1":
        raise RuntimeError("HDR metrics schema is invalid")
    return payload


def main() -> int:
    args = _parser().parse_args()
    try:
        k6 = _k6_command(args.k6_bin)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 2
    if not args.fault_key:
        print("W5 requires --fault-key or TRADECORE_OMS_PERF_FAULT_KEY", file=sys.stderr)
        return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    base_urls: list[str] = [
        item.strip().rstrip("/") for item in args.base_url.split(",") if item.strip()
    ]
    if not base_urls:
        print("--base-url must contain at least one URL", file=sys.stderr)
        return 2
    primary_base_url = base_urls[0]
    environment.update(
        {
            "BASE_URL": primary_base_url,
            "BASE_URLS": ",".join(base_urls),
            "DURATION": args.duration,
            "FACT_TPS": str(args.fact_tps),
            "SNAPSHOT_QPS": str(args.snapshot_qps),
            "SCOPES": "1" if len(base_urls) > 1 else environment.get("SCOPES", "4"),
            "FAULT_KEY": args.fault_key,
            "KEY_DISTRIBUTION": args.key_distribution,
            "HOT_KEY_RATIO_PERCENT": str(args.hot_key_ratio_percent),
        }
    )
    results: list[dict[str, object]] = []
    preparations: list[dict[str, object]] = []
    for workload, filename in WORKLOADS:
        summary = args.output_dir / f"{workload.lower()}-k6-summary.json"
        workload_environment = dict(environment)
        if workload == "W5":
            # The pause drill proves one scope's fail-closed boundary.  Other
            # independently pinned shards deliberately remain available.
            workload_environment["BASE_URLS"] = primary_base_url
        if workload == "W3":
            # W1 intentionally uses unique synthetic executions. W3 is a
            # separate mixed contract and must begin from the same bounded
            # seed baseline rather than inherit benchmark data growth.
            try:
                reset_payloads = [
                    _admin_post(url, "reset-benchmark", args.fault_key) for url in base_urls
                ]
                for url in base_urls:
                    _reset_metrics(url)
                reset_file = args.output_dir / "w3-clean-baseline.json"
                reset_file.write_text(
                    json.dumps(reset_payloads, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                )
                preparations.append(
                    {"workload": "W3", "status": "RESET", "evidence": reset_file.name}
                )
            except (OSError, RuntimeError, ValueError) as error:
                preparations.append({"workload": "W3", "status": "FAIL", "error": str(error)})
                results.append({"workload": workload, "exit_code": 98, "summary": None})
                continue
        code = _run(
            [
                k6,
                "run",
                "--summary-trend-stats=avg,min,med,p(90),p(95),p(99),p(99.9),max",
                "--summary-export",
                str(summary),
                str(K6 / filename),
            ],
            environment=workload_environment,
        )
        results.append({"workload": workload, "exit_code": code, "summary": summary.name})
        if workload == "W3" and code == 0:
            try:
                for index, url in enumerate(base_urls):
                    hdr_file = args.output_dir / f"w3-hdr-shard-{index}.json"
                    hdr_file.write_text(
                        json.dumps(_capture_hdr(url), indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                preparations.append({"workload": "W3-HDR", "status": "CAPTURED"})
            except (OSError, RuntimeError, ValueError) as error:
                preparations.append({"workload": "W3-HDR", "status": "FAIL", "error": str(error)})
                results.append({"workload": "W3-HDR", "exit_code": 98, "summary": None})
    report = {
        "schema": "TradeCoreOmsLoadGateResultV1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "git_revision": _git_revision(),
        "base_urls": base_urls,
        "fact_tps_target": args.fact_tps,
        "snapshot_qps_target": args.snapshot_qps,
        "data_distribution": {
            "key_distribution": args.key_distribution,
            "hot_key_ratio_percent": args.hot_key_ratio_percent,
        },
        "preparations": preparations,
        "workloads": results,
        "status": "PASS" if all(result["exit_code"] == 0 for result in results) else "FAIL",
    }
    (args.output_dir / "w0-w5-gate-result.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if args.benchmark_profile is not None:
        try:
            paths = write_platform_gate_evidence(
                profile_path=args.benchmark_profile,
                report=report,
                platform="OMS",
                output_dir=args.output_dir,
            )
            report["v11_gate_evidence"] = {gate: path.name for gate, path in paths.items()}
        except BenchmarkEvidenceV11Error as error:
            report["status"] = "FAIL"
            report["v11_evidence_error"] = str(error)
        (args.output_dir / "w0-w5-gate-result.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
