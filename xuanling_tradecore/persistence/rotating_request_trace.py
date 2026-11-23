"""Single-owner background trace sink with bounded, hash-chained file rotation."""

from __future__ import annotations

import os
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns
from typing import BinaryIO, Callable, cast

import msgspec

from .runtime_files import (
    ExclusiveRuntimeLease,
    atomic_runtime_json,
    open_private,
    require_private_runtime_file,
)


class RotatingRequestTraceFile:
    """File-like sink; all methods must run on the owning evidence writer thread."""

    def __init__(
        self,
        path: Path,
        *,
        journal_id: str,
        segment_bytes: int,
        interval_ns: int = 60_000_000_000,
        maximum_segments: int = 4096,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        if (
            type(segment_bytes) is not int
            or segment_bytes < 256
            or type(interval_ns) is not int
            or interval_ns < 1
            or type(maximum_segments) is not int
            or not 2 <= maximum_segments <= 4096
        ):
            raise ValueError("trace rotation capacity is invalid")
        root = path.parent
        if root.is_symlink() or (
            os.name == "posix"
            and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        ):
            raise ValueError("trace root must be private and owner-controlled")
        self._lease = ExclusiveRuntimeLease(path.with_suffix(".owner.lock"))
        self.path = path
        self.manifest = path.with_suffix(".manifest.json")
        self._limit, self._interval, self._maximum = segment_bytes, interval_ns, maximum_segments
        self._clock, self._journal_id = clock, journal_id
        self._sealed: list[dict[str, object]] = []
        self._index = self._bytes = self._sequence = self._records = 0
        self._digest = sha256()
        self._encoder = msgspec.json.Encoder(order="deterministic")
        self._started = clock()
        self._first: int | None = None
        self._closed = False
        self._failed = False
        self._prepared: BinaryIO | None = None
        opened: list[BinaryIO] = []
        try:
            if self.manifest.exists() or self.manifest.is_symlink():
                raise FileExistsError("trace evidence namespace already exists")
            self._file = self._open(0)
            opened.append(self._file)
            self._prepared = self._open(1)
            opened.append(self._prepared)
            self._publish(False)
        except BaseException:
            for file in opened:
                file.close()
            self._lease.close()
            raise

    def _name(self, index: int) -> Path:
        return (
            self.path if index == 0 else self.path.with_name(f"{self.path.stem}-{index:06d}.jsonl")
        )

    def _open(self, index: int) -> BinaryIO:
        return os.fdopen(
            open_private(self._name(index), os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb"
        )

    def _publish(self, closed: bool) -> None:
        atomic_runtime_json(
            self.manifest,
            {
                "schema_version": 1,
                "journal_id": self._journal_id,
                "segments": self._sealed,
                "active": None if closed else self._name(self._index).name,
                "prepared": self._name(self._index + 1).name
                if self._prepared is not None
                else None,
                "closed": closed,
                "failed": self._failed,
                "last_sequence": self._sequence,
            },
        )

    def _seal(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        self._sealed.append(
            {
                "file": self._name(self._index).name,
                "bytes": self._bytes,
                "records": self._records,
                "first_sequence": self._first,
                "last_sequence": self._sequence if self._records else None,
                "sha256": self._digest.hexdigest(),
                "previous_sha256": self._sealed[-1]["sha256"] if self._sealed else None,
            }
        )

    def _rotate(self) -> None:
        if self._prepared is None:
            raise OSError("trace segment count budget exhausted")
        self._seal()
        self._file = self._prepared
        self._index += 1
        self._bytes = self._records = 0
        self._first = None
        self._digest = sha256()
        self._started = self._clock()
        self._prepared = self._open(self._index + 1) if self._index + 1 < self._maximum else None
        self._publish(False)

    def write(self, data: bytes) -> int:
        try:
            return self._write(data)
        except (OSError, ValueError):
            self._failed = True
            raise

    def _write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("trace storage is closed")
        for line in data.splitlines(keepends=True):
            if not line.endswith(b"\n") or len(line) > min(self._limit, 65536):
                raise ValueError("trace row is incomplete/oversized")
            row = msgspec.json.decode(line, type=dict[str, object])
            sequence = row.get("trace_sequence")
            self._append_line(line, sequence)
        return len(data)

    def append_record(self, record: dict[str, object], *, maximum_remaining: int) -> int:
        """Encode one structured row once, then retain the same sequence fences.

        The sink creates the bytes itself: callers cannot supply bytes whose
        sequence disagrees with the object. Raw write() still parses untrusted
        lines. Zero means the caller's remaining whole-trace quota is too small.
        """
        try:
            if self._closed or type(maximum_remaining) is not int or maximum_remaining < 0:
                raise ValueError("invalid structured trace capacity/state")
            line = self._encoder.encode(record) + b"\n"
            if len(line) > maximum_remaining:
                return 0
            self._append_line(line, record.get("trace_sequence"))
            return len(line)
        except (OSError, TypeError, ValueError):
            self._failed = True
            raise

    def _append_line(self, line: bytes, sequence: object) -> None:
        if not line.endswith(b"\n") or len(line) > min(self._limit, 65536):
            raise ValueError("trace row is incomplete/oversized")
        if type(sequence) is not int or sequence != self._sequence + 1:
            raise ValueError("trace sequence is discontinuous")
        if self._bytes and (
            self._bytes + len(line) > self._limit
            or self._clock() - self._started >= self._interval
        ):
            self._rotate()
        self._file.write(line)
        self._digest.update(line)
        self._bytes += len(line)
        self._records += 1
        if self._first is None:
            self._first = sequence
        self._sequence = sequence

    def flush(self) -> None:
        try:
            if not self._closed:
                self._file.flush()
                if self._bytes and self._clock() - self._started >= self._interval:
                    self._rotate()
        except (OSError, ValueError):
            self._failed = True
            raise

    def stats(self) -> dict[str, object]:
        return {
            "mode": "rotating",
            "sealed_segments": len(self._sealed),
            "active_segment": self._index,
            "active_bytes": self._bytes,
            "last_sequence": self._sequence,
            "maximum_segments": self._maximum,
        }

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._seal()
            if self._prepared is not None:
                self._prepared.close()
                self._name(self._index + 1).unlink()  # only our exclusive, unused empty spare
                self._prepared = None
            self._publish(True)
        finally:
            self._file.close()
            if self._prepared is not None:
                self._prepared.close()
            self._lease.close()
            self._closed = True


def verify_closed_request_trace(manifest: Path) -> int:
    """Stream-verify final evidence; unsealed/running evidence cannot claim completeness."""
    require_private_runtime_file(manifest)
    if manifest.stat().st_size > 2 * 1024**2:
        raise ValueError("trace manifest exceeds bounded capacity")
    value = msgspec.json.decode(manifest.read_bytes(), type=dict[str, object])
    rows = value.get("segments")
    if (
        value.get("schema_version") != 1
        or value.get("closed") is not True
        or value.get("failed") is not False
        or not isinstance(rows, list)
        or not 1 <= len(cast(list[object], rows)) <= 4096
    ):
        raise ValueError("trace manifest is unsealed/invalid")
    expected = 1
    previous = None
    seen: set[str] = set()
    for raw in cast(list[object], rows):
        if not isinstance(raw, dict):
            raise ValueError("invalid trace segment")
        row = cast(dict[str, object], raw)
        name = row.get("file")
        if not isinstance(name, str) or Path(name).name != name or name in seen:
            raise ValueError("invalid/duplicate trace filename")
        seen.add(name)
        path = manifest.parent / name
        require_private_runtime_file(path)
        digest = sha256()
        size = count = 0
        first = None
        with path.open("rb") as source:
            for line in iter(lambda: source.readline(65537), b""):
                if len(line) > 65536:
                    raise ValueError("trace row exceeds verification capacity")
                record = msgspec.json.decode(line, type=dict[str, object])
                if not line.endswith(b"\n") or record.get("trace_sequence") != expected:
                    raise ValueError("trace contains missing/duplicate sequence")
                if first is None:
                    first = expected
                digest.update(line)
                size += len(line)
                count += 1
                expected += 1
        if (
            row.get("sha256") != digest.hexdigest()
            or row.get("previous_sha256") != previous
            or row.get("bytes") != size
            or row.get("records") != count
            or row.get("first_sequence") != first
            or row.get("last_sequence") != (expected - 1 if count else None)
        ):
            raise ValueError("trace segment integrity mismatch")
        previous = digest.hexdigest()
    if value.get("last_sequence") != expected - 1:
        raise ValueError("trace final count does not match manifest")
    return expected - 1
