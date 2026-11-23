"""Offline end-to-end Python-call benchmarks, including input/output conversion.

Run python -m tools.benchmarks.hybrid_compute --output benchmark-results.local.json
No throughput threshold is asserted: hardware, storage and CPU load are variable.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns
from typing import Callable

from xuntrade_omsems.compute import assess_orders, backend_info, crc32c, session_shards


def measure(call: Callable[[], object], repeats: int) -> dict[str, float | int]:
    call()  # warmup
    values = []
    for _ in range(repeats):
        started = perf_counter_ns()
        call()
        values.append(perf_counter_ns() - started)
    return {"median_ns": statistics.median(values), "minimum_ns": min(values), "repeats": repeats}


def worker(batch_size: int, repeats: int, workers: int) -> dict[str, object]:
    ids = tuple(f"execution-{i:012d}" for i in range(batch_size))
    rows = tuple((100 + i % 100, 1250, 250000, 250000) for i in range(batch_size))
    frame = bytes(range(256)) * 4
    samples = {
        "crc32c_1024_bytes": measure(lambda: tuple(crc32c(frame) for _ in range(1000)), repeats),
        "session_shards": measure(lambda: session_shards(ids, 31, workers=workers), repeats),
        "assess_orders": measure(lambda: assess_orders(rows, workers=workers), repeats),
    }
    return {"backend": backend_info(), "workers": workers, "samples": samples}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 1000000 or not 1 <= args.repeats <= 100:
        parser.error("batch-size must be 1..1000000; repeats must be 1..100")
    if not 1 <= args.workers <= 32:
        parser.error("workers must be 1..32")
    if args.worker:
        print(json.dumps(worker(args.batch_size, args.repeats, args.workers)))
        return 0
    runs = {}
    for mode, workers in (("python", 1), ("native", 1), ("native", args.workers)):
        command = [
            sys.executable,
            "-m",
            "tools.benchmarks.hybrid_compute",
            "--worker",
            "--batch-size",
            str(args.batch_size),
            "--repeats",
            str(args.repeats),
            "--workers",
            str(workers),
        ]
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "XUNTRADE_BACKEND": mode, "PYTHONUTF8": "1"},
        )
        runs[f"{mode}-{workers}"] = json.loads(completed.stdout)
    reference = runs["python-1"]["samples"]
    for run in runs.values():
        for name, sample in run["samples"].items():
            sample["speedup_vs_python"] = round(
                reference[name]["median_ns"] / sample["median_ns"], 3
            )
    result = {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "batch_size": args.batch_size,
        "crc_calls_per_sample": 1000,
        "runs": runs,
        "scope": "offline compute only; includes CPython conversion; excludes journal fsync and broker",
    }
    payload = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
