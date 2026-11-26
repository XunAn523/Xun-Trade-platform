"""Cold identity archive segments; never an execution/recovery authority.

The index is derived metadata, rebuilt from CRC-verified files at startup.
Only an explicit maximum_segments policy deletes old sealed segments.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from .journal import FileJournal, JournalAppend, JournalFailure, JournalRecord
from .locking import JournalLease


@dataclass(frozen=True)
class ArchiveStorageSnapshot:
    sealed_segments: int = 0
    retained_record_objects: int = 0
    pruned_segments: int = 0


def _sync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class SegmentedArchiveStore:
    """One serialized cold writer; frame LSNs are local to each segment."""

    def __init__(
        self,
        path: str | Path,
        *,
        maximum_segment_records: int = 4096,
        maximum_segment_bytes: int = 16 * 1024 * 1024,
        maximum_segments: int | None = None,
    ) -> None:
        if (
            type(maximum_segment_records) is not int
            or not 1 <= maximum_segment_records <= 1000000
            or type(maximum_segment_bytes) is not int
            or maximum_segment_bytes < 8192
            or (
                maximum_segments is not None
                and (type(maximum_segments) is not int or maximum_segments < 2)
            )
        ):
            raise ValueError("invalid archive segment or retention policy")
        self.path = Path(path)
        self.directory = self.path.with_name(self.path.name + ".segments")
        if self.path.is_symlink() or self.directory.is_symlink():
            raise JournalFailure("archive paths must not be symlinks")
        self._lease = JournalLease(self.path.with_name(self.path.name + ".archive-owner.lock"))
        self._lease.acquire()
        self._closed = False
        self._failed = False
        self._journal: FileJournal | None = None
        self._maximum_records = maximum_segment_records
        self._maximum_bytes = maximum_segment_bytes
        self._maximum_segments = maximum_segments
        self._pruned = 0
        self._segments: list[dict[str, object]] = []
        self._snapshot = ArchiveStorageSnapshot()
        try:
            self.directory.mkdir(mode=0o700, exist_ok=True)
            expected = self._indexed_hashes()
            previous = None
            for sealed in sorted(self.directory.glob("*.emtj")):
                if not re.fullmatch(r"[0-9]{20}\.emtj", sealed.name):
                    continue
                number = int(sealed.stem)
                if sealed.is_symlink() or (previous is not None and number != previous + 1):
                    raise JournalFailure("archive sealed segment chain is unsafe or has a gap")
                metadata = self._verify_sealed(sealed, number)
                if sealed.name in expected and metadata["sha256"] != expected[sealed.name]:
                    raise JournalFailure("sealed archive differs from its indexed checksum")
                self._segments.append(metadata)
                previous = number
            indexed_numbers = [
                int(Path(name).stem) for name in expected if re.fullmatch(r"[0-9]{20}\.emtj", name)
            ]
            if indexed_numbers and (previous is None or max(indexed_numbers) > previous):
                raise JournalFailure("indexed sealed archive segment is missing")
            self._next_segment = 1 if previous is None else previous + 1
            self._journal = FileJournal(self.path, retained_records=0)
            self._size = self.path.stat().st_size
            self._prune()
            self._checkpoint()
        except BaseException:
            try:
                if self._journal is not None:
                    self._journal.close()
            finally:
                self._lease.close()
            raise

    def _indexed_hashes(self) -> dict[str, str]:
        index = self.directory / "index.json"
        if index.is_symlink():
            raise JournalFailure("archive index must not be a symlink")
        try:
            payload = json.loads(index.read_text(encoding="utf-8"))
            return {entry["file"]: entry["sha256"] for entry in payload["sealed"]}
        except (OSError, ValueError, KeyError, TypeError):
            return {}  # A missing/torn derived index does not erase verified frames.

    @staticmethod
    def _verify_sealed(path: Path, number: int) -> dict[str, object]:
        # No lease file is needed: the store holds the archive-wide owner lease.
        with FileJournal(path, retained_records=0, repair_tail=False, exclusive=False) as journal:
            count = journal.record_count
        if count == 0:
            raise JournalFailure("sealed archive segment is empty")
        return {
            "segment_id": number,
            "file": path.name,
            "records": count,
            "bytes": path.stat().st_size,
            "sha256": _checksum(path),
        }

    def snapshot(self) -> ArchiveStorageSnapshot:
        return self._snapshot  # Immutable cached snapshot: no disk I/O on readers.

    def _prune(self) -> None:
        if self._maximum_segments is not None:
            while len(self._segments) + 1 > self._maximum_segments:
                oldest = self._segments[0]
                (self.directory / str(oldest["file"])).unlink()
                self._segments.pop(0)
                self._pruned += 1
        self._snapshot = ArchiveStorageSnapshot(len(self._segments), 0, self._pruned)

    def _checkpoint(self) -> None:
        assert self._journal is not None
        payload = {
            "schema_version": 1,
            "scope": "non-authoritative identity archive",
            "maximum_segment_records": self._maximum_records,
            "maximum_segment_bytes": self._maximum_bytes,
            "maximum_segments": self._maximum_segments,
            "sealed": self._segments,
            "active": {
                "file": self.path.name,
                "records": self._journal.record_count,
                "bytes": self._size,
                "lsn_scope": "segment-local",
            },
            "snapshot": asdict(self._snapshot),
        }
        temporary = self.directory / "index.tmp"
        if temporary.is_symlink():
            raise JournalFailure("unsafe archive index temporary path")
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.directory / "index.json")
        _sync_directory(self.directory)
        _sync_directory(self.path.parent)

    def _rotate(self) -> None:
        assert self._journal is not None
        self._journal.close()
        destination = self.directory / f"{self._next_segment:020d}.emtj"
        if destination.exists():
            raise JournalFailure("archive rotation would overwrite a sealed segment")
        os.replace(self.path, destination)
        self._segments.append(self._verify_sealed(destination, self._next_segment))
        self._next_segment += 1
        self._journal = FileJournal(self.path, retained_records=0)
        self._size = 0
        self._prune()
        self._checkpoint()

    def append_batch(self, entries: Sequence[JournalAppend]) -> tuple[JournalRecord, ...]:
        if self._closed or self._failed or self._journal is None:
            raise JournalFailure("archive store is closed or failed")
        sizes = tuple(self._journal.frame_size(entry) for entry in entries)
        if any(size > self._maximum_bytes for size in sizes):
            raise JournalFailure("archive record exceeds segment byte budget")
        records: list[JournalRecord] = []
        offset = 0
        try:
            while offset < len(entries):
                if (
                    self._journal.record_count >= self._maximum_records
                    or self._size + sizes[offset] > self._maximum_bytes
                ):
                    self._rotate()
                count, size = 0, 0
                while (
                    offset + count < len(entries)
                    and self._journal.record_count + count < self._maximum_records
                    and self._size + size + sizes[offset + count] <= self._maximum_bytes
                ):
                    size += sizes[offset + count]
                    count += 1
                records.extend(self._journal.append_batch(entries[offset : offset + count]))
                self._size += size
                offset += count
        except BaseException:
            self._failed = True
            raise
        return tuple(records)

    def close(self) -> None:
        if self._closed:
            return
        try:
            if not self._failed:
                self._checkpoint()
        finally:
            self._closed = True
            try:
                if self._journal is not None:
                    self._journal.close()
            finally:
                self._lease.close()
