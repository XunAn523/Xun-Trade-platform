"""Finite CPython-3.10 tracker ownership for the standalone candidate CLI only.

Never stop an embedded interpreter's shared tracker and never call its unbounded
_stop(). No signals, name-based process killing or cross-project cleanup.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import re
import stat
import sys
from pathlib import Path
from threading import active_count
from time import monotonic, monotonic_ns, sleep
from typing import Any

from ..persistence.clock_identity import monotonic_clock_identity
from .candidate_systemd import require_coordinated_candidate_unit


def _children() -> set[int]:
    # Include children forked by any parent thread, not only the main task.
    children: set[int] = set()
    for task in Path("/proc/self/task").iterdir():
        children.update(int(v) for v in (task / "children").read_text().split())
    return children


def _members(root: Path) -> set[int]:
    files = [root / "cgroup.procs", *root.glob("*/cgroup.procs")]
    if len(files) > 16 or any(path.parent.is_symlink() for path in files):
        raise RuntimeError("candidate helper cgroup is not bounded")
    # This CLI never creates nested cgroups; unknown nesting cannot be ignored.
    if any(root.glob("*/*/cgroup.procs")):
        raise RuntimeError("candidate helper cgroup nesting is unexpected")
    members: set[int] = set()
    for path in files:
        text = path.read_text()
        if len(text) > 65536:
            raise RuntimeError("candidate helper cgroup membership is oversized")
        members.update(int(value) for value in text.split())
    return members


def _identity(pid: int, parent: int, fd: int) -> tuple[int, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    if int(fields[1]) != parent:
        raise RuntimeError("candidate tracker parent changed")
    args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    programs = [
        re.fullmatch(rb"from multiprocessing.resource_tracker import main;main\(([0-9]+)\)", v)
        for v in args
    ]
    matches = [match for match in programs if match is not None]
    if len(matches) != 1 or not os.path.samefile(f"/proc/{pid}/exe", sys.executable):
        raise RuntimeError("candidate helper is not the expected interpreter tracker")
    writer = os.fstat(fd)
    reader = Path(f"/proc/{pid}/fd/{int(matches[0][1])}").stat()
    if not stat.S_ISFIFO(writer.st_mode) or (writer.st_dev, writer.st_ino) != (
        reader.st_dev,
        reader.st_ino,
    ):
        raise RuntimeError("candidate tracker pipe ownership changed")
    return int(fields[19]), writer.st_dev, writer.st_ino


class OwnedCandidateTracker:
    """Require a fresh, single-purpose CLI before starting any workers.

    A live inherited/shared tracker is refused, not borrowed and later stopped.
    The enclosing systemd cgroup remains the final finite failure containment.
    """

    def __init__(self, *, cgroup_root: Path) -> None:
        require_coordinated_candidate_unit(cgroup_root)
        if (
            sys.platform != "linux"
            or sys.implementation.name != "cpython"
            or sys.version_info[:2] != (3, 10)
            or active_count() != 1
            or mp.active_children()
            or _children()
        ):
            raise RuntimeError("owned tracker requires a fresh CPython 3.10 standalone CLI")
        if _members(cgroup_root) != {os.getpid()}:
            raise RuntimeError("candidate requires a private single-purpose service cgroup")
        self._cgroup_root = cgroup_root
        from multiprocessing import resource_tracker

        tracker: Any = resource_tracker._resource_tracker  # pyright: ignore[reportPrivateUsage]
        if tracker._pid is not None or tracker._fd is not None:
            raise RuntimeError("candidate cannot own an inherited/shared tracker")
        self._tracker = tracker
        self._closed = False
        self._proof: dict[str, object] | None = None
        self._started_ns = monotonic_ns()
        self._parent = os.getpid()
        tracker.ensure_running()
        pid, fd = getattr(tracker, "_pid"), getattr(tracker, "_fd")
        if type(pid) is not int or type(fd) is not int:
            raise RuntimeError("unsupported tracker implementation")
        self._pid: int = pid
        self._fd: int = fd
        # Spawn returns before exec has necessarily completed. Bound this setup
        # race; no private API stop is used if verification cannot be established.
        deadline = monotonic() + 2
        while True:
            try:
                self._identity = _identity(self._pid, self._parent, self._fd)
                break
            except (FileNotFoundError, RuntimeError, ProcessLookupError):
                if monotonic() >= deadline:
                    raise RuntimeError("candidate tracker startup identity unavailable") from None
                sleep(0.005)

    def set_affinity_exact(self, cpus: tuple[int, ...]) -> None:
        """Pin only this verified helper; never select or modify another PID."""
        if (
            self._closed
            or not cpus
            or len(set(cpus)) != len(cpus)
            or any(type(cpu) is not int or cpu < 0 for cpu in cpus)
            or _identity(self._pid, self._parent, self._fd) != self._identity
        ):
            raise RuntimeError("candidate tracker affinity scope is invalid")
        os.sched_setaffinity(self._pid, set(cpus))
        if os.sched_getaffinity(self._pid) != set(cpus):
            raise RuntimeError("candidate tracker affinity was not applied")

    def close(self) -> dict[str, object]:
        if self._closed:
            if self._proof is None:
                raise RuntimeError("candidate tracker previously failed to exit")
            return dict(self._proof)
        tracker = self._tracker
        if (
            _children() != {self._pid}
            or mp.active_children()
            or _members(self._cgroup_root) != {self._parent, self._pid}
        ):
            raise RuntimeError("candidate managed/unknown children still running")
        if not tracker._lock.acquire(timeout=1):
            raise RuntimeError("candidate tracker ownership lock timed out")
        try:
            if (tracker._pid, tracker._fd) != (self._pid, self._fd) or (
                _identity(self._pid, self._parent, self._fd) != self._identity
            ):
                raise RuntimeError("candidate tracker identity changed before drain")
            os.close(self._fd)  # EOF, only after every managed descendant exited.
            tracker._fd = None
            self._closed = True
        finally:
            tracker._lock.release()
        deadline = monotonic() + 2
        while True:
            child, status = os.waitpid(self._pid, os.WNOHANG)
            if child == self._pid:
                break
            if monotonic() >= deadline:
                raise RuntimeError("candidate owned helper did not exit within two seconds")
            sleep(0.005)
        if not tracker._lock.acquire(timeout=1):
            raise RuntimeError("candidate tracker exit lock timed out")
        try:
            if tracker._pid != self._pid or tracker._fd is not None:
                raise RuntimeError("candidate tracker unexpectedly restarted")
            tracker._pid = None
        finally:
            tracker._lock.release()
        if (
            not os.WIFEXITED(status)
            or os.WEXITSTATUS(status) != 0
            or _children()
            or _members(self._cgroup_root) != {self._parent}
        ):
            raise RuntimeError("candidate owned descendants did not all exit normally")
        self._proof = {
            "schema": "candidate-owned-helper-exit-v1",
            "helper_kind": "cpython310-resource-tracker",
            "parent_pid": self._parent,
            "pid": self._pid,
            "start_ticks": self._identity[0],
            "ownership_verified": True,
            "started_ns": self._started_ns,
            "exited_ns": monotonic_ns(),
            "exit_code": 0,
            "remaining_descendants": 0,
            "clock_identity_sha256": monotonic_clock_identity(),
            "forced_kill": False,
        }
        return dict(self._proof)
