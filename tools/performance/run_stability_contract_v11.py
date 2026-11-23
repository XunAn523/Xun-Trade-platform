"""Run a declared STAB-L1/L2 command plan without inventing pass evidence.

Commands are supplied by the owning OMS/EMS/strategy platform.  They execute
concurrently, write their own evidence, and must return zero.  L2 additionally
requires active-load fault commands; this coordinator never replaces a fault
with a local sleep or a synthetic PASS.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any


class StabilityContractError(ValueError):
    """The declared stability plan cannot support a V1.1 claim."""


_CORE_WORKLOADS = {"OMS-W3", "EMS-E5", "STR-S5"}


def load_plan(path: Path, *, level: str) -> dict[str, Any]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise StabilityContractError("stability plan is unreadable") from error
    if not isinstance(plan, dict) or plan.get("schema") != "StabilityPlanV11":
        raise StabilityContractError("stability plan schema is invalid")
    if not isinstance(plan.get("profile_id"), str) or not plan["profile_id"]:
        raise StabilityContractError("stability plan profile_id is invalid")
    workloads = plan.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        raise StabilityContractError("stability plan workloads are invalid")
    names: set[str] = set()
    for item in workloads:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise StabilityContractError("stability workload identity is invalid")
        command = item.get("command")
        if (
            not isinstance(command, list)
            or not command
            or any(not isinstance(part, str) or not part for part in command)
        ):
            raise StabilityContractError("stability workload command is invalid")
        names.add(item["name"])
    if not _CORE_WORKLOADS.issubset(names):
        raise StabilityContractError("stability plan must include OMS-W3, EMS-E5 and STR-S5")
    faults = plan.get("faults", [])
    if level == "L2" and (not isinstance(faults, list) or not faults):
        raise StabilityContractError("STAB-L2 requires active-load fault commands")
    if not isinstance(faults, list):
        raise StabilityContractError("stability plan faults are invalid")
    for item in faults:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise StabilityContractError("stability fault identity is invalid")
        command = item.get("command")
        if (
            not isinstance(command, list)
            or not command
            or any(not isinstance(part, str) or not part for part in command)
        ):
            raise StabilityContractError("stability fault command is invalid")
    return plan


def _start(
    command: list[str], *, environment: dict[str, str], output: Path
) -> subprocess.Popen[str]:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        return subprocess.Popen(
            command, env=environment, stdout=handle, stderr=subprocess.STDOUT, text=True
        )


def run(
    *, plan: dict[str, Any], level: str, duration_seconds: int, output_dir: Path
) -> dict[str, Any]:
    """Execute all declared workloads concurrently, then active faults for L2."""

    if level not in {"L1", "L2"} or duration_seconds < 1:
        raise StabilityContractError("stability level or duration is invalid")
    environment = dict(os.environ)
    environment.update(
        {
            "BENCHMARK_STABILITY_LEVEL": level,
            "BENCHMARK_STABILITY_DURATION_SECONDS": str(duration_seconds),
        }
    )
    started = time.monotonic_ns()
    processes: list[tuple[str, subprocess.Popen[str], Path]] = []
    try:
        for item in plan["workloads"]:
            name = item["name"]
            log = output_dir / f"{name.lower()}-stability.log"
            processes.append(
                (name, _start(item["command"], environment=environment, output=log), log)
            )
        # Faults run only while all active-load processes are still alive.
        fault_results: list[dict[str, Any]] = []
        if level == "L2":
            for item in plan["faults"]:
                if any(process.poll() is not None for _name, process, _log in processes):
                    raise StabilityContractError(
                        "a stability workload stopped before active fault injection"
                    )
                log = output_dir / f"fault-{item['name']}.log"
                process = _start(item["command"], environment=environment, output=log)
                code = process.wait(timeout=max(30, duration_seconds))
                fault_results.append({"name": item["name"], "exit_code": code, "log": log.name})
        workload_results = [
            {
                "name": name,
                "exit_code": process.wait(timeout=duration_seconds + 30),
                "log": log.name,
            }
            for name, process, log in processes
        ]
        passed = all(item["exit_code"] == 0 for item in workload_results) and all(
            item["exit_code"] == 0 for item in fault_results
        )
        return {
            "schema": "StabilityResultV11",
            "profile_id": plan["profile_id"],
            "level": level,
            "duration_seconds": duration_seconds,
            "elapsed_ns": time.monotonic_ns() - started,
            "workloads": workload_results,
            "faults": fault_results,
            "status": "PASS" if passed else "FAIL",
        }
    finally:
        for _name, process, _log in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a BenchmarkStandardV1.1 STAB-L1/L2 plan")
    parser.add_argument("--level", choices=("L1", "L2"), required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    duration = (
        args.duration_seconds
        if args.duration_seconds is not None
        else (900 if args.level == "L1" else 1800)
    )
    try:
        plan = load_plan(args.plan, level=args.level)
        result = run(
            plan=plan, level=args.level, duration_seconds=duration, output_dir=args.output_dir
        )
    except (OSError, StabilityContractError, subprocess.TimeoutExpired) as error:
        result = {
            "schema": "StabilityResultV11",
            "level": args.level,
            "status": "FAIL",
            "error": str(error),
        }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "stability-result.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
