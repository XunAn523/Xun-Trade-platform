"""Lease-free Journal source bound to an owner-published committed fence.

This is deliberately not a recovery authority and never infers a live cut by
scanning a segment which may be changing.  A unique writer publishes the
complete ``SegmentedJournalReadBound`` through a trusted process-local channel;
this view only validates that bound and resolves the corresponding immutable or
live segment path through the canonical manifest.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, cast

from ..persistence.runtime_files import require_private_runtime_file
from .mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from .segmented_fact_journal import SegmentedJournalReadBound


class PublishedJournalSourceV1:
    """Read-side path authority with no writer lease or mutable mappings."""

    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        profile_hash: str,
        segment_frames: int,
        bound_for: Callable[[], SegmentedJournalReadBound],
        archive_root: Path | None = None,
    ) -> None:
        if (
            not journal_id
            or len(journal_id) > 128
            or len(profile_hash) != 64
            or any(value not in "0123456789abcdef" for value in profile_hash)
            or type(segment_frames) is not int
            or not 2 <= segment_frames <= 1_000_000
            or not callable(bound_for)
        ):
            raise MmapOmsFactJournalRecoveryRequired("invalid published Journal source")
        self.path = Path(root)
        self.archive_path = None if archive_root is None else Path(archive_root)
        self._journal_id = journal_id
        self._profile_hash = profile_hash
        self._frames = segment_frames
        self._bound_for = bound_for
        self._require_private_directory(self.path)
        if self.archive_path is not None:
            self._require_private_directory(self.archive_path)
            if self.archive_path.resolve() == self.path.resolve():
                raise MmapOmsFactJournalRecoveryRequired(
                    "published Journal archive must be separate"
                )

    @staticmethod
    def _require_private_directory(path: Path) -> None:
        if path.is_symlink() or not path.is_dir():
            raise MmapOmsFactJournalRecoveryRequired("Journal source directory is unsafe")
        status = path.stat()
        if os.name == "posix" and (status.st_uid != os.geteuid() or status.st_mode & 0o077):
            raise MmapOmsFactJournalRecoveryRequired("Journal source directory is not private")

    @property
    def journal_id(self) -> str:
        return self._journal_id

    @property
    def profile_hash(self) -> str:
        return self._profile_hash

    def read_bound(self) -> SegmentedJournalReadBound:
        bound = self._bound_for()
        if (
            type(bound) is not SegmentedJournalReadBound
            or bound.journal_id != self._journal_id
            or bound.profile_hash != self._profile_hash
            or bound.segment_frames != self._frames
            or type(bound.active_segment) is not int
            or bound.active_segment < 1
            or type(bound.active_frames) is not int
            or not 0 <= bound.active_frames <= self._frames
            or type(bound.committed_lsn) is not int
            or bound.committed_lsn < 0
            or type(bound.next_batch_id) is not int
            or bound.next_batch_id < 1
        ):
            raise MmapOmsFactJournalRecoveryRequired("published Journal fence is invalid")
        return bound

    def segment_read_path(self, index: int) -> Path:
        if type(index) is not int or not 1 <= index <= 999_999_999_999:
            raise MmapOmsFactJournalRecoveryRequired("Journal segment index is invalid")
        self._require_private_directory(self.path)
        manifest = self.path / "segments.json"
        require_private_runtime_file(manifest)
        if manifest.stat().st_size > 16 * 1024**2:
            raise MmapOmsFactJournalRecoveryRequired("Journal manifest is oversized")
        try:
            decoded: object = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MmapOmsFactJournalRecoveryRequired("Journal manifest is unreadable") from exc
        if not isinstance(decoded, dict):
            raise MmapOmsFactJournalRecoveryRequired("Journal manifest is not an object")
        payload = cast(dict[str, object], decoded)
        entries = payload.get("segments")
        if (
            set(payload) != {"schema_version", "journal_id", "segment_frames", "segments"}
            or payload.get("schema_version") != 1
            or payload.get("journal_id") != self._journal_id
            or payload.get("segment_frames") != self._frames
            or not isinstance(entries, list)
        ):
            raise MmapOmsFactJournalRecoveryRequired("Journal manifest scope changed")
        matched: dict[str, object] | None = None
        seen: set[int] = set()
        for raw in cast(list[object], entries):
            if not isinstance(raw, dict):
                raise MmapOmsFactJournalRecoveryRequired("Journal manifest entry is invalid")
            row = cast(dict[str, object], raw)
            item = row.get("index")
            if (
                type(item) is not int
                or not 1 <= item <= 999_999_999_999
                or item in seen
                or set(row) - {"index", "sha256", "tier"}
                or row.get("tier", "hot") not in {"hot", "archive"}
                or (
                    row.get("tier") == "archive"
                    and (self.archive_path is None or not isinstance(row.get("sha256"), str))
                )
            ):
                raise MmapOmsFactJournalRecoveryRequired("Journal manifest entry is unsafe")
            seen.add(item)
            if item == index:
                matched = row
        if matched is None:
            raise MmapOmsFactJournalRecoveryRequired("Journal segment is not registered")
        root = self.archive_path if matched.get("tier") == "archive" else self.path
        assert root is not None
        self._require_private_directory(root)
        result = root / f"segment-{index:012d}.mmap"
        require_private_runtime_file(result)
        return result
