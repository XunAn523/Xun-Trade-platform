"""Small cross-platform exclusive file lease for account-owned state."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock


class LeaseError(RuntimeError):
    """Another gateway process already owns the account state directory."""


_LOCAL_LOCK = Lock()
_OWNED_PATHS: set[Path] = set()


class JournalLease:
    """Non-blocking OS file lock with an in-process ownership guard.

    The lock file is intentionally retained after close. Its presence is not
    ownership; only the held byte-range/flock lease is authoritative.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self._file: object | None = None
        self._held = False

    def acquire(self) -> None:
        with _LOCAL_LOCK:
            if self.path in _OWNED_PATHS:
                raise LeaseError(f"journal lease is already held in this process: {self.path}")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            file = self.path.open("a+b")
            try:
                file.seek(0)
                if not file.read(1):
                    file.seek(0)
                    file.write(b"\0")
                    file.flush()
                    os.fsync(file.fileno())
                file.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                file.close()
                raise LeaseError(f"journal lease is held by another process: {self.path}") from exc
            _OWNED_PATHS.add(self.path)
            self._file = file
            self._held = True

    def close(self) -> None:
        with _LOCAL_LOCK:
            if not self._held:
                return
            file = self._file
            try:
                if file is not None:
                    file.seek(0)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                    file.close()
            finally:
                _OWNED_PATHS.discard(self.path)
                self._file = None
                self._held = False

    def __enter__(self) -> JournalLease:
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
