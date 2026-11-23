"""Owner-scoped runtime leases and durable atomic metadata replacement."""

from __future__ import annotations

import json
import os
import stat
import sys
import uuid
from pathlib import Path
from typing import Mapping


def open_private(path: Path, flags: int) -> int:
    return os.open(path, flags | getattr(os, "O_NOFOLLOW", 0), 0o600)


def require_private_runtime_file(path: Path) -> None:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise ValueError("runtime file is not a regular private file")
    if os.name == "posix" and (metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077):
        raise ValueError("runtime file owner/permissions are unsafe")


class ExclusiveRuntimeLease:
    """Keep the same inode locked; the kernel releases it after process death."""

    def __init__(self, path: Path) -> None:
        self._fd = open_private(path, os.O_RDWR | os.O_CREAT)
        try:
            if sys.platform == "win32":
                import msvcrt

                if os.fstat(self._fd).st_size == 0:
                    os.write(self._fd, b"0")
                os.lseek(self._fd, 0, os.SEEK_SET)
                msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self._fd)
            self._fd = -1
            raise

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1


def atomic_runtime_json(path: Path, payload: Mapping[str, object]) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    descriptor = open_private(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(dict(payload), output, separators=(",", ":"), sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        if sys.platform != "win32":
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
