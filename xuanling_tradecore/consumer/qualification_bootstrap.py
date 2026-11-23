"""Cold-path initialization decision for a manually operated qualification unit.

An existing partial runtime is never treated as a fresh namespace. The actual
identity, source and coverage checks remain owned by ColdAwareOmsRuntime.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path


def initialize_new_qualification_runtime(root: Path) -> bool:
    if not root.is_absolute() or root.is_symlink():
        raise ValueError("qualification runtime must be an absolute non-symlink directory")
    parent = root.parent
    metadata = parent.lstat()
    if parent.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("qualification data parent is unsafe")
    if os.name == "posix" and (metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077):
        raise ValueError("qualification data parent is not private to this service")
    if not root.exists():
        return True
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or (
        os.name == "posix" and (metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077)
    ):
        raise ValueError("qualification runtime directory is unsafe")
    if not any(root.iterdir()):
        return True
    identity, journal = root / "identity", root / "journal"
    manifest = journal / "segments.json"
    if (
        identity.is_symlink()
        or not identity.is_dir()
        or journal.is_symlink()
        or not journal.is_dir()
        or manifest.is_symlink()
        or not manifest.is_file()
    ):
        raise ValueError("partial qualification runtime requires explicit recovery, not reset")
    return False
