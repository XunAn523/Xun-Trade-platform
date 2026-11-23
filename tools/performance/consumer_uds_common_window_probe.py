"""One finite public-client window; does not start/reset the server or Adapter.

Run inside a finite generator cgroup with the union of the explicit disjoint
generator/observer CPU grants. This is SHORT diagnostics, not 12h qualification.
The operator must independently prove final source/FULL closure and service
payload identity; matching client windows alone never grant performance PASS.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from perfbench.short_probe_contract import common_start, common_window
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json, open_private


def validate_cpu_groups(groups: str, observer: str, allowed: set[int]) -> None:
    parts = [*groups.split(";"), observer]
    if not 2 <= len(parts) <= 9 or any(
        not part or any(not v.isascii() or not v.isdecimal() for v in part.split(","))
        for part in parts
    ):
        raise ValueError("explicit bounded generator/observer CPUs required")
    cpus = [int(v) for part in parts for v in part.split(",")]
    if len(cpus) != len(set(cpus)) or not set(cpus) <= allowed:
        raise ValueError("client CPU groups overlap or exceed their existing grant")


def read_report(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024**2:
        raise ValueError("missing or oversized child report")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("invalid child report")
    return value


def run(args) -> dict:
    validate_cpu_groups(args.cpu_groups, args.events_cpus, os.sched_getaffinity(0))
    shards = len(args.cpu_groups.split(";"))
    drain_through_lsn = getattr(args, "drain_through_lsn", None)
    drain_timeout_seconds = getattr(args, "drain_timeout_seconds", 30)
    partition_concurrency = getattr(args, "partition_concurrency", 1)
    if (
        type(args.duration) is not int
        or not 1 <= args.duration <= 60
        or type(args.rate) is not int
        or not 1 <= args.rate <= 2000
        or args.rate % shards
        or type(args.sequence_base) is not int
        or not 0 <= args.sequence_base < 10**17 - shards * 1_000_000
        or (
            drain_through_lsn is not None
            and (type(drain_through_lsn) is not int or not 1 <= drain_through_lsn <= 2**63 - 1)
        )
        or type(drain_timeout_seconds) is not int
        or not 1 <= drain_timeout_seconds <= 60
        or type(partition_concurrency) is not int
        or partition_concurrency not in {1, 2, 4}
    ):
        raise ValueError("invalid short common-window budget")
    args.output.mkdir(mode=0o700, exist_ok=False)
    started = common_start(now_ns=time.monotonic_ns(), supplied_ns=None, lead_seconds=30)
    scripts = Path(__file__).parent
    shared = [
        "--config",
        str(args.config),
        "--duration",
        str(args.duration),
        "--start-ns",
        str(started),
    ]
    event_drain = (
        []
        if drain_through_lsn is None
        else [
            "--drain-through-lsn",
            str(drain_through_lsn),
            "--drain-timeout-seconds",
            str(drain_timeout_seconds),
        ]
    )
    commands = [
        [
            sys.executable,
            str(scripts / "consumer_uds_candidate_shards.py"),
            *shared,
            "--output",
            str(args.output / "load"),
            "--rate",
            str(args.rate),
            "--cpu-groups",
            args.cpu_groups,
            "--sequence-base",
            str(args.sequence_base),
            "--trading-day",
            args.trading_day,
        ],
        [
            sys.executable,
            str(scripts / "consumer_uds_rolling_events_probe.py"),
            *shared,
            "--output",
            str(args.output / "events"),
            "--checkpoint-root",
            str(args.output / "checkpoint"),
            "--cpus",
            args.events_cpus,
            "--partition-concurrency",
            str(partition_concurrency),
            *event_drain,
        ],
    ]
    processes, handles, exits = [], [], []
    failed, reason = False, None
    load = events = None
    summary = {}
    try:
        for name, command in zip(("load", "events"), commands):
            handle = os.fdopen(
                open_private(args.output / f"{name}.stderr", os.O_WRONLY | os.O_CREAT | os.O_EXCL),
                "wb",
            )
            handles.append(handle)
            processes.append(subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=handle))
        deadline = time.monotonic() + 30 + args.duration + drain_timeout_seconds + 5
        for process in processes:
            exits.append(process.wait(timeout=max(1, deadline - time.monotonic())))
        load = read_report(args.output / "load" / "report.json")
        events = read_report(args.output / "events" / "report.json")
        summary = common_window(load, events)
        failed = any(exits) or load.get("failed") is not False or events.get("failed") is not False
    except Exception:
        failed, reason = True, "COMMON_WINDOW_CHILD_OR_EVIDENCE_FAILED"
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()
    final_drain = events.get("final_drain") if isinstance(events, dict) else None
    drain_reached = isinstance(final_drain, dict) and final_drain.get("reached") is True
    result = {
        **summary,
        "schema": "consumer-common-short-window-v1",
        "formal_qualification": False,
        "failed": failed,
        "error_code": reason,
        "child_exit_codes": [p.returncode for p in processes],
        "start_ns": started,
        "end_ns": started + args.duration * 1_000_000_000,
        "performance_status": "NOT_EVALUATED",
        "producer_join_status": "NOT_MEASURED",
        "final_event_drain_performed": drain_through_lsn is not None,
        "final_event_drain_reached": drain_reached if drain_through_lsn is not None else None,
        "final_event_drain_target_lsn": drain_through_lsn,
        "event_partition_concurrency": partition_concurrency,
        "limitations": [
            "window contract only",
            "final drain target is a stop condition, not source authority",
            "offline final source/FULL and server trace join still required",
            "CPU grant is not proof of kernel exclusivity",
        ],
    }
    atomic_runtime_json(args.output / "report.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--cpu-groups", required=True)
    parser.add_argument("--events-cpus", required=True)
    parser.add_argument(
        "--partition-concurrency",
        type=int,
        choices=(1, 2, 4),
        default=1,
        help=(
            "Server-signed replay partitions; one is the qualified latency default, "
            "while two/four remain explicit throughput experiments"
        ),
    )
    parser.add_argument("--sequence-base", type=int, required=True)
    parser.add_argument("--trading-day", required=True)
    parser.add_argument("--drain-through-lsn", type=int)
    parser.add_argument("--drain-timeout-seconds", type=int, default=30)
    result = run(parser.parse_args())
    print(json.dumps(result, separators=(",", ":")))
    return int(result["failed"])


if __name__ == "__main__":
    raise SystemExit(main())
