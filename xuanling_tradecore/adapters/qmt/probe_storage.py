"""Exclusive process lease and separate SQLite identities for transport probes."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


class QmtProbeStorageLease:
    """Kernel-released on process exit; never delete/recreate the lock inode."""

    def __init__(self, path: Path, *, application_id: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.with_suffix(path.suffix + ".owner-lock").open("a+b")
        try:
            if sys.platform == "win32":
                import msvcrt

                if self._file.tell() == 0:
                    self._file.write(b"0")
                    self._file.flush()
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            connection = sqlite3.connect(path, isolation_level=None)
            try:
                connection.execute("BEGIN IMMEDIATE")
                marker = connection.execute("PRAGMA application_id").fetchone()[0]
                tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                if marker != application_id and (marker != 0 or tables):
                    raise RuntimeError("QMT transport probe refuses an existing non-probe database")
                connection.execute(f"PRAGMA application_id={application_id:d}")
                connection.execute("COMMIT")
            finally:
                connection.close()
        except BaseException:
            self._file.close()
            raise

    def close(self) -> None:
        self._file.close()
