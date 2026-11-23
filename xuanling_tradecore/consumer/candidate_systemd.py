"""Read-only launch gate for a coordinated multi-process qualification unit."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path, PurePosixPath


def require_coordinated_candidate_unit(cgroup_root: Path) -> None:
    if sys.platform != "linux":
        raise ValueError("candidate unit gate requires Linux")
    rows = Path("/proc/self/cgroup").read_text()
    if len(rows) > 16384:
        raise ValueError("candidate cgroup membership is oversized")
    unified = [line[3:] for line in rows.splitlines() if line.startswith("0::")]
    if len(unified) != 1 or not unified[0].startswith("/"):
        raise ValueError("candidate requires unambiguous cgroup v2 membership")
    group = PurePosixPath(unified[0])
    if ".." in group.parts or not group.name.endswith(".service"):
        raise ValueError("candidate must run in its own service unit")
    expected = PurePosixPath("/sys/fs/cgroup") / str(group).lstrip("/")
    if PurePosixPath(cgroup_root.as_posix()) != expected:
        raise ValueError("candidate cgroup must match this process, not another unit")
    try:
        result = subprocess.run(
            ["systemctl", "show", group.name, "--property=KillMode", "--value"],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        raise ValueError("candidate unit properties unavailable") from None
    if result.stdout.strip() != "mixed":
        raise ValueError("candidate requires KillMode=mixed for parent-first coordinated shutdown")
