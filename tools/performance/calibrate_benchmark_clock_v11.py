"""Measure same-host common-release skew for a BenchmarkStandardV1.1 profile."""

from __future__ import annotations

import argparse
import json
import time
from multiprocessing import get_context
from pathlib import Path
from typing import Any


def _capture_after_release(ready: Any, release: Any, output: Any) -> None:
    """One process-side timestamp capture; no benchmark workload is run."""

    ready.set()
    release.wait()
    output.put(time.monotonic_ns())


def measure_release_skew(*, scope_count: int, samples: int) -> list[int]:
    """Return one actual max-minus-min capture skew for each common release."""

    if not 2 <= scope_count <= 32:
        raise ValueError("scope_count must be in 2..32")
    if not 1 <= samples <= 100:
        raise ValueError("samples must be in 1..100")
    context = get_context("spawn")
    skews: list[int] = []
    for _ in range(samples):
        release = context.Event()
        captures: Any = context.Queue()
        ready_events: list[Any] = [context.Event() for _scope in range(scope_count)]
        processes = [
            context.Process(target=_capture_after_release, args=(ready, release, captures))
            for ready in ready_events
        ]
        try:
            for process in processes:
                process.start()
            deadline = time.monotonic() + 15.0
            while not all(event.is_set() for event in ready_events):
                if (
                    any(not process.is_alive() for process in processes)
                    or time.monotonic() >= deadline
                ):
                    raise RuntimeError("clock calibration worker did not become ready")
                time.sleep(0.001)
            release.set()
            values = [captures.get(timeout=5.0) for _scope in range(scope_count)]
            if any(not isinstance(value, int) or value < 0 for value in values):
                raise RuntimeError("clock calibration capture is invalid")
            skews.append(max(values) - min(values))
        finally:
            release.set()
            for process in processes:
                process.join(timeout=1.0)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=1.0)
    return skews


def _timestamp_read_overhead() -> dict[str, int]:
    """Measure the same timestamp primitive used by every V1.1 stage."""

    values: list[int] = []
    for _ in range(10_000):
        start = time.monotonic_ns()
        values.append(time.monotonic_ns() - start)
    ordered = sorted(values)
    return {"p50_ns": ordered[(len(ordered) - 1) // 2], "p100_ns": ordered[-1]}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure BenchmarkStandardV1.1 common-release clock skew"
    )
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--scope-count", type=int, required=True)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--maximum-skew-ns", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if not args.profile_id or args.maximum_skew_ns < 0:
            raise ValueError("profile_id and maximum_skew_ns are invalid")
        skews = measure_release_skew(scope_count=args.scope_count, samples=args.samples)
        maximum = max(skews)
        result = {
            "schema": "BenchmarkClockCalibrationV11",
            "profile_id": args.profile_id,
            "status": "PASS" if maximum <= args.maximum_skew_ns else "FAIL",
            "monotonic_clock": "monotonic_ns",
            "scope_count": args.scope_count,
            "samples": args.samples,
            "maximum_observed_release_skew_ns": maximum,
            "per_release_skew_ns": skews,
            "timestamp_read_overhead_ns": _timestamp_read_overhead(),
        }
    except (OSError, RuntimeError, ValueError) as error:
        result = {
            "schema": "BenchmarkClockCalibrationV11",
            "profile_id": args.profile_id,
            "status": "FAIL",
            "error": str(error),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
