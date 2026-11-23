"""Run the EMS/strategy W7 sub-profile alongside the existing OMS+EMT W7 runner.

The OMS/EMT runner remains the authority for real EMT preflight, 499-burst,
callback completeness and cleanup.  This wrapper starts it unchanged, then
runs the additional E/S workload in the same 60-second window.  It never
constructs an EMT command itself.
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
from typing import Any
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
_CPU_LIST = re.compile(r"^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$")
_LOOPBACK_K6_API_ADDRESS = re.compile(r"^127\.0\.0\.1:(\d{1,5})$")


def _load_command(path: Path) -> tuple[str, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, list)
        or not payload
        or any(not isinstance(item, str) or not item for item in payload)
    ):
        raise RuntimeError("OMS W7 command file must be a non-empty JSON argv array")
    return tuple(payload)


def _request(url: str, *, body: dict[str, object], key: str) -> int:
    request = Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-TradeCore-Perf-Fault-Key": key},
        method="POST",
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310 - controlled benchmark endpoint
        return response.status


def _hdr(url: str) -> dict[str, Any]:
    with urlopen(f"{url}/perf/v1/metrics/hdr", timeout=15) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "HDRHistogramV1":
        raise RuntimeError("EMS/strategy HDR schema is invalid")
    return payload


def _k6(binary: str) -> str:
    resolved = shutil.which(binary)
    if resolved is None:
        raise RuntimeError("k6 is not installed or not on PATH")
    return resolved


def _base_urls(value: str) -> tuple[str, ...]:
    urls = tuple(item.strip().rstrip("/") for item in value.split(",") if item.strip())
    if not urls:
        raise RuntimeError("--oms-base-url must contain at least one URL")
    return urls


def _cpu_list(value: str | None) -> str | None:
    if value is None:
        return None
    if _CPU_LIST.fullmatch(value) is None:
        raise RuntimeError("--ems-k6-cpu-list must be a taskset CPU list")
    return value


def _k6_api_address(value: str) -> str:
    match = _LOOPBACK_K6_API_ADDRESS.fullmatch(value)
    if match is None or not 1 <= int(match.group(1)) <= 65_535:
        raise RuntimeError("--ems-k6-api-address must be a 127.0.0.1 TCP address")
    return value


def _wait_for_oms_admission(urls: tuple[str, ...], timeout_seconds: float = 10.0) -> None:
    """Align E/S load with a *live* OMS W3 window, not process startup time."""

    deadline = time() + timeout_seconds
    while time() < deadline:
        try:
            admitted = []
            for url in urls:
                with urlopen(f"{url}/perf/v1/metrics/hdr", timeout=2) as response:  # noqa: S310
                    payload = json.loads(response.read().decode("utf-8"))
                admitted.append(int(payload.get("counters", {}).get("fact_admitted_total", 0)) > 0)
            if all(admitted):
                return
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        sleep(0.02)
    raise RuntimeError("OMS W3 did not admit facts on every shard before EMS/strategy start")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OMS-W7-EMS-STR concurrent acceptance")
    parser.add_argument(
        "--ems-base-url", required=True, help="one URL or comma-separated EMS/strategy shards"
    )
    parser.add_argument("--ems-fault-key", required=True)
    parser.add_argument(
        "--oms-base-url", required=True, help="one or comma-separated OMS W7 shard URLs"
    )
    parser.add_argument("--oms-w7-command-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--k6-bin", default=os.environ.get("K6_BIN", "k6"))
    parser.add_argument(
        "--ems-k6-cpu-list",
        help="taskset CPU list for E/S k6; must not overlap OMS or EMT scope CPUs",
    )
    parser.add_argument(
        "--ems-k6-api-address",
        default="127.0.0.1:6567",
        help="loopback k6 control address; keep distinct from OMS W7 k6",
    )
    parser.add_argument("--duration", default="60s")
    args = parser.parse_args()
    oms_process: subprocess.Popen[bytes] | None = None
    try:
        k6 = _k6(args.k6_bin)
        oms_command = _load_command(args.oms_w7_command_file)
        oms_urls = _base_urls(args.oms_base_url)
        ems_k6_cpu_list = _cpu_list(args.ems_k6_cpu_list)
        ems_k6_api_address = _k6_api_address(args.ems_k6_api_address)
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2
    urls = [item.strip().rstrip("/") for item in args.ems_base_url.split(",") if item.strip()]
    if not urls:
        print("--ems-base-url must not be empty", file=sys.stderr)
        return 2
    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        for url in urls:
            if (
                _request(f"{url}/perf/v1/admin/benchmark/reset", body={}, key=args.ems_fault_key)
                != 200
            ):
                raise RuntimeError("cannot reset EMS/strategy benchmark state")
            request = Request(f"{url}/perf/v1/metrics/reset", data=b"{}", method="POST")
            with urlopen(request, timeout=15) as response:  # noqa: S310
                if response.status != 204:
                    raise RuntimeError("cannot reset EMS/strategy metrics")
        environment = dict(os.environ)
        environment.update(
            {
                "BASE_URL": urls[0],
                "BASE_URLS": ",".join(urls),
                "DURATION": args.duration,
                "SIGNAL_TPS": "2000",
                "CHILD_TPS": "2000",
                "EVENT_TPS": "5000",
                "SNAPSHOT_QPS": "5000",
                "SCOPES": "1" if len(urls) > 1 else environment.get("SCOPES", "4"),
            }
        )
        ems_summary = args.output_dir / "w7-ems-str-k6-summary.json"
        oms_process = subprocess.Popen(oms_command, cwd=ROOT.parents[1], env=environment)
        _wait_for_oms_admission(oms_urls)
        ems_command = [
            *(["taskset", "-c", ems_k6_cpu_list] if ems_k6_cpu_list is not None else []),
            k6,
            "run",
            "--address",
            ems_k6_api_address,
            "--summary-trend-stats=avg,min,med,p(90),p(95),p(99),p(99.9),max",
            "--summary-export",
            str(ems_summary),
            str(ROOT / "k6" / "w7_ems_str.js"),
        ]
        if ems_k6_cpu_list is not None and shutil.which("taskset") is None:
            raise RuntimeError("--ems-k6-cpu-list requires taskset on PATH")
        ems_code = subprocess.run(
            ems_command,
            cwd=ROOT.parents[1],
            env=environment,
            check=False,
        ).returncode
        oms_code = oms_process.wait()
        hdr_files: list[str] = []
        for index, url in enumerate(urls):
            path = args.output_dir / f"w7-ems-str-hdr-shard-{index}.json"
            path.write_text(
                json.dumps(_hdr(url), indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            hdr_files.append(path.name)
        report: dict[str, object] = {
            "schema": "TradeCoreOmsW7EmsStrV1",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "oms_w7_exit_code": oms_code,
            "oms_base_urls": oms_urls,
            "ems_strategy_k6_exit_code": ems_code,
            "ems_strategy_k6_cpu_list": ems_k6_cpu_list,
            "ems_strategy_k6_api_address": ems_k6_api_address,
            "ems_strategy_summary": ems_summary.name,
            "ems_strategy_hdr": hdr_files,
            "status": "PASS" if oms_code == 0 and ems_code == 0 else "FAIL",
        }
    except (OSError, RuntimeError, ValueError) as error:
        if oms_process is not None and oms_process.poll() is None:
            oms_process.terminate()
            oms_process.wait()
        report = {"schema": "TradeCoreOmsW7EmsStrV1", "status": "FAIL", "error": str(error)}
    (args.output_dir / "w7-ems-str-result.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
