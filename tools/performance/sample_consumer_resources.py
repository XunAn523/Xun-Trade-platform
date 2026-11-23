"""Finite read-only process/CPU/IRQ sampler for TradeCore's existing cgroup.

No affinity, IRQ, cgroup, service, queue, GC or trading setting is changed.
Missing samples remain NOT_MEASURED, never zeros. Run off the trading CPUs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from hashlib import sha256
from pathlib import Path

from perfbench.short_probe_contract import common_start, tool_revision
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json, open_private

UNITS = frozenset(
    {
        "xuanling-tradecore.service",
        "xuanling-tradecore-fault-supervisor.service",
        "xuanling-tradecore-consumer-uds.service",
        "xuanling-tradecore-longrun-qualification.service",
        "tradecore-performance-candidate.service",
    }
)


def cpu_ids(text: str) -> set[int]:
    result = set()
    for part in text.split(","):
        bounds = part.split("-")
        if len(bounds) not in (1, 2) or any(not v.isascii() or not v.isdecimal() for v in bounds):
            raise ValueError("invalid bounded CPU list")
        first, last = int(bounds[0]), int(bounds[-1])
        if not 0 <= first <= last <= 4095 or len(result) + last - first + 1 > 256:
            raise ValueError("CPU sample budget exceeded")
        result.update(range(first, last + 1))
    return result


def task_stat(text: str) -> dict:
    finish = text.rfind(")")
    begin = text.find("(")
    if begin < 1 or finish < begin:
        raise ValueError("invalid task stat")
    values = text[finish + 2 :].split()
    if len(values) < 37:
        raise ValueError("short task stat")
    names = {
        "minor_faults": 7,
        "major_faults": 9,
        "utime_ticks": 11,
        "stime_ticks": 12,
        "starttime_ticks": 19,
        "processor": 36,
    }
    row = {name: int(values[index]) for name, index in names.items()}
    row["pid"] = int(text[:begin].strip())
    row["block_io_delay_ticks"] = int(values[39]) if len(values) > 39 else None
    if row["pid"] < 1 or any(value < 0 for value in row.values() if value is not None):
        raise ValueError("invalid task counter")
    return row


def interrupts(text: str, cpus: set[int]) -> dict | None:
    lines = text.splitlines()
    if not lines:
        return None
    header = lines[0].split()
    if not header or any(not name.startswith("CPU") or not name[3:].isdecimal() for name in header):
        return None
    indices = {int(name[3:]): index for index, name in enumerate(header)}
    if not cpus <= indices.keys():
        return None
    result = {str(cpu): {} for cpu in sorted(cpus)}
    if len(lines) > 4096:
        raise ValueError("interrupt table budget exceeded")
    for line in lines[1:]:
        label, sep, body = line.partition(":")
        values = body.split()
        if not sep or len(values) < len(header):
            continue  # aggregate ERR/MIS rows are not per-CPU counters
        if len(label.strip()) > 64 or any(not value.isdecimal() for value in values[: len(header)]):
            raise ValueError("invalid interrupt counter")
        for cpu, index in indices.items():
            if cpu in cpus:
                result[str(cpu)][label.strip()] = int(values[index])
    return result if any(result.values()) else None


def read_small(path: Path, limit=8 * 1024**2) -> str:
    with path.open() as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError("resource source exceeds budget")
    return value


def capture(root: Path, cpus: set[int]) -> dict:
    row = dict(monotonic_ns=time.monotonic_ns(), cgroup={}, processes=[], missing=[])
    try:
        row["block_io_delay_accounting_enabled"] = (
            read_small(Path("/proc/sys/kernel/task_delayacct"), 64).strip() == "1"
        )
    except OSError:
        row["block_io_delay_accounting_enabled"] = None
    if row["block_io_delay_accounting_enabled"] is not True:
        row["missing"].append("per_task_block_io_delay_accounting")
    for name in (
        "memory.current",
        "memory.events",
        "cpu.stat",
        "cpu.pressure",
        "memory.pressure",
        "io.stat",
        "pids.current",
        "cpuset.cpus.effective",
    ):
        try:
            row["cgroup"][name] = read_small(root / name, 65536).strip()
        except OSError:
            row["missing"].append("cgroup." + name)
    for source in ("interrupts", "softirqs"):
        try:
            row[source] = interrupts(read_small(Path("/proc") / source), cpus)
            if row[source] is None:
                row["missing"].append(source)
        except OSError:
            row["missing"].append(source)
    try:
        ticks = {}
        for line in read_small(Path("/proc/stat")).splitlines():
            fields = line.split()
            if (
                fields
                and fields[0].startswith("cpu")
                and fields[0][3:].isdecimal()
                and int(fields[0][3:]) in cpus
            ):
                ticks[fields[0]] = list(map(int, fields[1:]))
        row["cpu_ticks"] = ticks
        if len(ticks) != len(cpus):
            row["missing"].append("cpu_ticks")
    except OSError:
        row["missing"].append("cpu_ticks")
    try:
        pids = read_small(root / "cgroup.procs", 65536).split()
        if len(pids) > 256 or any(not pid.isdecimal() for pid in pids):
            raise ValueError("PID sample budget")
    except OSError:
        pids = []
        row["missing"].append("cgroup.procs")
    membership = "0::/" + root.relative_to("/sys/fs/cgroup").as_posix()
    total_tasks = 0
    for pid in pids:
        process_root = Path("/proc") / pid
        try:
            before = task_stat(read_small(process_root / "stat", 65536))
            if membership not in read_small(process_root / "cgroup", 65536).splitlines():
                raise ValueError("process left the owned cgroup")
            process = {"pid": int(pid), "starttime_ticks": before["starttime_ticks"], "threads": []}
            try:
                process["io"] = {
                    key.strip(): int(value)
                    for line in read_small(process_root / "io", 65536).splitlines()
                    for key, sep, value in [line.partition(":")]
                    if sep
                }
            except OSError:
                row["missing"].append("process_io")
            tasks = list((process_root / "task").iterdir())
            total_tasks += len(tasks)
            if total_tasks > 512:
                raise ValueError("TID sample budget")
            for task in tasks:
                try:
                    thread = task_stat(read_small(task / "stat", 65536))
                    status = dict(
                        line.split(":", 1)
                        for line in read_small(task / "status", 65536).splitlines()
                        if ":" in line
                    )
                    for key in (
                        "Name",
                        "Tgid",
                        "Pid",
                        "Cpus_allowed_list",
                        "voluntary_ctxt_switches",
                        "nonvoluntary_ctxt_switches",
                    ):
                        thread[key] = status.get(key, "NOT_MEASURED").strip()
                    try:
                        fields = read_small(task / "schedstat", 65536).split()
                        thread["schedstat"] = (
                            dict(
                                zip(
                                    ("runtime_ns", "runqueue_wait_ns", "timeslices"),
                                    map(int, fields),
                                )
                            )
                            if len(fields) == 3
                            else None
                        )
                        migration = [
                            line.partition(":")[2].strip()
                            for line in read_small(task / "sched", 65536).splitlines()
                            if line.partition(":")[0].strip() == "se.nr_migrations"
                        ]
                        thread["migrations"] = int(migration[0]) if len(migration) == 1 else None
                        if thread["migrations"] is None or thread["schedstat"] is None:
                            row["missing"].append("thread_scheduler_counter")
                    except OSError:
                        row["missing"].append("thread_scheduler")
                    after_thread = task_stat(read_small(task / "stat", 65536))
                    if thread["starttime_ticks"] != after_thread["starttime_ticks"] or int(
                        thread["Tgid"]
                    ) != int(pid):
                        raise ValueError("TID identity changed")
                    process["threads"].append(thread)
                except FileNotFoundError:
                    row["missing"].append("thread_exited_during_sample")
            after = task_stat(read_small(process_root / "stat", 65536))
            if before["starttime_ticks"] != after["starttime_ticks"]:
                raise ValueError("PID identity changed")
            row["processes"].append(process)
        except FileNotFoundError:
            row["missing"].append("process_exited_during_sample")
        except PermissionError:
            row["missing"].append("process_permission")
    return row


def run(
    *, root: Path, output: Path, cpus: set[int], duration: int, start_ns=None, interval_ms=1000
) -> dict:
    if (
        sys.platform != "linux"
        or sys.version_info[:2] != (3, 10)
        or type(duration) is not int
        or not 1 <= duration <= 300
        or type(interval_ms) is not int
        or not 100 <= interval_ms <= 5000
        or root.parent != Path("/sys/fs/cgroup/tradecore.slice")
        or root.name not in UNITS
        or root.is_symlink()
        or not root.is_dir()
        or not output.is_absolute()
        or not cpus
        or not cpus <= cpu_ids(read_small(root / "cpuset.cpus.effective").strip())
    ):
        raise ValueError("invalid bounded project resource sample")
    start = (
        common_start(now_ns=time.monotonic_ns(), supplied_ns=start_ns)
        if start_ns is not None
        else time.monotonic_ns()
    )
    end = start + duration * 1_000_000_000
    clock = monotonic_clock_identity()
    revision = tool_revision()
    output.mkdir(mode=0o700, exist_ok=False)
    digest = sha256()
    written = count = 0
    missing = {}
    expected_samples = (duration * 1000 + interval_ms - 1) // interval_ms
    failed = None
    with os.fdopen(
        open_private(output / "samples.jsonl", os.O_CREAT | os.O_EXCL | os.O_WRONLY), "wb"
    ) as sink:
        planned = start
        while time.monotonic_ns() < end:
            delay = (planned - time.monotonic_ns()) / 1e9
            if delay > 0:
                time.sleep(min(delay, 0.05))
                continue
            skipped = max(0, (time.monotonic_ns() - planned) // (interval_ms * 1_000_000))
            if skipped:
                missing["sample_period_missed"] = missing.get("sample_period_missed", 0) + skipped
                planned += skipped * interval_ms * 1_000_000
            try:
                value = capture(root, cpus)
                value["planned_ns"] = planned
                value["sample_sequence"] = count + 1
                data = json.dumps(value, separators=(",", ":")).encode() + b"\n"
                if written + len(data) > 128 * 1024**2:
                    raise ValueError("resource evidence budget")
                sink.write(data)
                digest.update(data)
                written += len(data)
                count += 1
                for key in value["missing"]:
                    missing[key] = missing.get(key, 0) + 1
            except (OSError, ValueError):
                failed = "RESOURCE_SAMPLE_FAILED"
                break
            planned += interval_ms * 1_000_000
    result = dict(
        schema="consumer-resource-samples-v1",
        clock="time.monotonic_ns",
        clock_identity_sha256=clock,
        tool_revision=revision,
        start_ns=start,
        end_ns=end,
        interval_ms=interval_ms,
        samples=count,
        expected_samples=expected_samples,
        bytes=written,
        sha256=digest.hexdigest(),
        missing=missing,
        error_code=failed,
        formal_qualification=False,
        performance_status="NOT_EVALUATED",
        complete=failed is None
        and count == expected_samples
        and not missing
        and clock is not None
        and clock == monotonic_clock_identity(),
        limitations=[
            "sampling is not continuous tracing",
            "IRQ counters do not prove causality",
            "PSS/frequency/syscall latency NOT_MEASURED",
            "no resource settings changed",
        ],
    )
    atomic_runtime_json(output / "report.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cgroup-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--start-ns", type=int)
    args = parser.parse_args()
    result = run(
        root=args.cgroup_root,
        output=args.output,
        cpus=cpu_ids(args.cpus),
        duration=args.duration,
        start_ns=args.start_ns,
    )
    print(json.dumps(result, separators=(",", ":")))
    return int(not result["complete"])


if __name__ == "__main__":
    raise SystemExit(main())
