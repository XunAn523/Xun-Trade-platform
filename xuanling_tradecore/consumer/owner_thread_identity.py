"""Startup-only, opt-in Linux owner-thread identity evidence; never hot-path tracing."""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from time import monotonic_ns

from ..persistence.clock_identity import monotonic_clock_identity
from ..persistence.runtime_files import atomic_runtime_json

_KNOWN = frozenset({
    "MainThread", "oms-segment-maintenance", "oms-cold-index",
    "source-commit-evidence", "consumer-mock-lifecycle", "consumer-trace-writer",
    "consumer-resource-sampler", "oms-read-fence-publisher", "oms-owner-control",
    "oms-inherited-write", "oms-inherited-read", "consumer-submit-owner",
})
_INDEXED = re.compile(r"consumer-(?:cold-read|uds|uds-overload)-[0-9]{1,3}\Z")


def _start_ticks(tid: int) -> int:
    text = Path(f"/proc/self/task/{tid}/stat").read_text(encoding="ascii")
    # comm is parenthesized and may contain spaces or a closing parenthesis.
    close = text.rfind(")")
    if close < 0:
        raise ValueError("invalid Linux thread stat")
    fields = text[close + 2 :].split()
    if len(fields) < 20 or not fields[19].isascii() or not fields[19].isdecimal():
        raise ValueError("invalid Linux thread identity")
    return int(fields[19])


def owner_thread_snapshot() -> dict:
    if os.name != "posix" or not hasattr(os, "sched_getaffinity"):
        raise ValueError("owner thread identity diagnostic requires Linux")
    started = monotonic_ns()
    clock = monotonic_clock_identity()
    active = threading.enumerate()
    if len(active) > 512:
        raise ValueError("owner thread identity budget exceeded")
    rows = []
    missing = 0
    for thread in active:
        tid = thread.native_id
        if type(tid) is not int or tid < 1:
            missing += 1
            continue
        try:
            start_ticks = _start_ticks(tid)
            affinity = sorted(os.sched_getaffinity(tid))
            if not affinity or start_ticks < 1:
                raise ValueError("invalid thread affinity or identity")
        except (OSError, ValueError):
            missing += 1
            continue
        name = thread.name if thread.name in _KNOWN or _INDEXED.fullmatch(thread.name) else "OTHER"
        rows.append({"native_id": tid, "start_ticks": start_ticks, "role": name, "cpus": affinity})
    finished = monotonic_ns()
    return {
        "schema": "consumer-owner-thread-startup-v1",
        "clock_identity_sha256": clock,
        "pid": os.getpid(),
        "started_ns": started,
        "finished_ns": finished,
        "enumerated": len(active),
        "missing": missing,
        "complete": missing == 0 and clock is not None,
        "unknown_roles": sum(row["role"] == "OTHER" for row in rows),
        "threads": sorted(rows, key=lambda item: item["native_id"]),
        "limitations": [
            "startup snapshot only; later-created threads are not covered",
            "thread names are diagnostic labels, not authorization or lifecycle proof",
            "compare native_id and start_ticks before joining resource samples",
        ],
    }


def write_owner_thread_snapshot(path: Path) -> None:
    atomic_runtime_json(path, owner_thread_snapshot())


def set_evidence_thread_affinity(cpus_text: str) -> None:
    """Candidate-only startup experiment; never move the WRITE or Mock actor.

    The kernel cgroup remains the final CPU grant. Exact readback rejects a
    clipped grant; any failed partial migration restores surviving threads.
    """
    parts = cpus_text.split(",")
    if (
        not 1 <= len(parts) <= 2
        or any(not part.isascii() or not part.isdecimal() for part in parts)
    ):
        raise ValueError("invalid evidence thread CPU grant")
    cpus = {int(part) for part in parts}
    if len(cpus) != len(parts) or any(cpu > 65535 for cpu in cpus):
        raise ValueError("invalid evidence thread CPU grant")
    targets = []
    writers = []
    for thread in threading.enumerate():
        if thread.name not in {"source-commit-evidence", "consumer-trace-writer", "oms-inherited-write"}:
            continue
        tid = thread.native_id
        if type(tid) is not int or tid < 1:
            raise ValueError("missing evidence or WRITE thread identity")
        identity = (tid, _start_ticks(tid), os.sched_getaffinity(tid))
        if thread.name == "oms-inherited-write":
            writers.append(identity)
        else:
            targets.append(identity)
    if len(writers) != 1 or not 1 <= len(targets) <= 4 or cpus & writers[0][2]:
        raise ValueError("evidence and WRITE thread grants overlap or roles are unavailable")
    changed = []
    try:
        for tid, start, previous in targets:
            if _start_ticks(tid) != start:
                raise ValueError("evidence thread identity changed")
            os.sched_setaffinity(tid, cpus)
            changed.append((tid, start, previous))
            if _start_ticks(tid) != start or os.sched_getaffinity(tid) != cpus:
                raise ValueError("evidence CPU grant was clipped or identity changed")
    except BaseException:
        for tid, start, previous in reversed(changed):
            try:
                if _start_ticks(tid) == start:
                    os.sched_setaffinity(tid, previous)
            except (OSError, ValueError):
                pass  # Startup is still rejected; a dead thread needs no restoration.
        raise
