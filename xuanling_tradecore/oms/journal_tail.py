"""Bounded read-only live Journal tail, independent of writer mappings and history size."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Generator, Protocol

import msgspec

from .mmap_fact_journal_v1 import (
    MmapOmsFactJournalRecoveryRequired,
    MmapOmsFactJournalV1,
    MmapOmsFactRecordV1,
)
from .order_book import OmsJournalCommit
from .segmented_fact_journal import (
    FRAME_BYTES,
    HEADER_BYTES,
    SegmentedJournalReadBound,
    _Segment,  # pyright: ignore[reportPrivateUsage]
)


class JournalTailSource(Protocol):
    """Read-only fence/path authority, not a writer or recovery authorization."""

    @property
    def journal_id(self) -> str: ...

    @property
    def profile_hash(self) -> str: ...

    def read_bound(self) -> SegmentedJournalReadBound: ...

    def segment_read_path(self, index: int) -> Path: ...


class JournalTailCursor(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    journal_id: str
    profile_hash: str
    segment: int = 1
    frame: int = 0
    next_lsn: int = 1
    next_batch_id: int = 1


@dataclass(frozen=True)
class VerifiedJournalBatch:
    before: JournalTailCursor
    after: JournalTailCursor
    records: tuple[MmapOmsFactRecordV1, ...]
    sha256: str

    @property
    def commit(self) -> OmsJournalCommit:
        return OmsJournalCommit(
            self.before.journal_id, self.before.next_lsn, self.after.next_lsn - 1, len(self.records)
        )


def journal_batch_fingerprint(records: tuple[MmapOmsFactRecordV1, ...]) -> str:
    """Canonical fingerprint of physically verified decoded members, not a fence."""
    encoder = msgspec.json.Encoder(order="deterministic")
    digest = sha256()
    for row in records:
        fact = row.fact
        digest.update(
            encoder.encode(
                (
                    row.lsn,
                    row.batch_id,
                    row.ordinal,
                    row.batch_size,
                    fact.fact_id,
                    fact.kind.value,
                    fact.account_id,
                    fact.oms_order_id,
                    fact.execution_id,
                    dict(fact.attributes),
                    fact.recovery_payload,
                )
            )
        )
    return digest.hexdigest()


class SegmentedJournalTail:
    """One reader thread; at most current+successor readonly mappings, one bounded batch.

    The writer fence excludes partially committed active batches. Each included
    member is still physically decoded/CRC/commit checked. No sealed segment is
    rescanned, and no consumer-supplied cursor is accepted as recovery authority.
    """

    def __init__(
        self,
        journal: JournalTailSource,
        *,
        cursor: JournalTailCursor | None = None,
        maximum_batch_facts: int = 128,
        reuse_segment_views: bool = False,
    ) -> None:
        if type(maximum_batch_facts) is not int or not 1 <= maximum_batch_facts <= 256:
            raise ValueError("invalid tail batch capacity")
        if type(reuse_segment_views) is not bool:
            raise ValueError("invalid tail segment reuse flag")
        self._journal, self._maximum = journal, maximum_batch_facts
        self.cursor = cursor or JournalTailCursor(journal.journal_id, journal.profile_hash)
        self._current: _Segment | None = None
        self._reuse_views = reuse_segment_views
        self._successor: _Segment | None = None
        self._successor_identity: tuple[int, int, int] | None = None
        self._anchor_cursor: JournalTailCursor | None = None
        self._anchor_frame: bytes | None = None
        self._window_bound: SegmentedJournalReadBound | None = None
        self._failed = self._closed = False
        bound = journal.read_bound()
        self._frames = bound.segment_frames
        self._validate_cursor()

    @contextmanager
    def source_read_window(self) -> Generator[None, None, None]:
        """One conservative committed prefix for one bounded non-waiting group.

        New source commits belong to the next group. Every included frame still
        has its original physical/CRC/commit validation; no source writes or
        speculative LSNs. End this window BEFORE the SQL transaction begins.
        """
        if self._closed or self._failed or self._window_bound is not None:
            raise MmapOmsFactJournalRecoveryRequired("tail source window is unavailable/nested")
        bound = self._journal.read_bound()
        if (
            bound.journal_id != self.cursor.journal_id
            or bound.profile_hash != self.cursor.profile_hash
            or bound.segment_frames != self._frames
            or self.cursor.next_lsn > bound.committed_lsn + 1
            or self.cursor.segment > bound.active_segment
        ):
            self._failed = True
            raise MmapOmsFactJournalRecoveryRequired("tail source window changed namespace")
        self._window_bound = bound
        try:
            yield
        except BaseException:
            self._failed = True
            raise
        finally:
            self._window_bound = None

    def _validate_cursor(self) -> None:
        cursor = self.cursor
        if (
            cursor.journal_id != self._journal.journal_id
            or cursor.profile_hash != self._journal.profile_hash
            or any(
                type(v) is not int
                for v in (cursor.segment, cursor.frame, cursor.next_lsn, cursor.next_batch_id)
            )
            or cursor.segment < 1
            or not 0 <= cursor.frame <= self._frames
            or cursor.next_lsn < 1
            or cursor.next_batch_id < 1
        ):
            raise MmapOmsFactJournalRecoveryRequired("tail cursor scope/position is invalid")

    def _open(self, index: int) -> _Segment:
        for attempt in range(2):
            try:
                return _Segment(
                    self._journal.segment_read_path(index),
                    index=index,
                    frames=self._frames,
                    journal_id=self._journal.journal_id,
                    create=False,
                    read_only=True,
                )
            except FileNotFoundError:
                if attempt:
                    raise
        raise MmapOmsFactJournalRecoveryRequired("segment source is unavailable")

    @staticmethod
    def _frame(segment: _Segment, frame: int) -> bytes:
        offset = HEADER_BYTES + frame * FRAME_BYTES
        return segment._map[offset : offset + FRAME_BYTES]  # pyright: ignore[reportPrivateUsage]

    def _next_segment(self, index: int) -> _Segment:
        if not self._reuse_views:
            return self._open(index)
        if self._successor is None:
            self._successor = self._open(index)
            self._successor_identity = (
                self._successor.first_lsn,
                self._successor.first_batch_id,
                self._successor.predecessor,
            )
        else:
            segment = self._successor
            static = segment._static  # pyright: ignore[reportPrivateUsage]
            if (
                segment.index != index or segment._map[: len(static)] != static  # pyright: ignore[reportPrivateUsage]
            ):
                raise MmapOmsFactJournalRecoveryRequired("tail successor identity changed")
            # Keep the existing static/activation CRC/commit proof on EVERY use.
            # Only open/fstat/mmap/close is reused; no fact or cursor proof is skipped.
            segment._read_activation()  # pyright: ignore[reportPrivateUsage]
            if (
                segment.first_lsn,
                segment.first_batch_id,
                segment.predecessor,
            ) != self._successor_identity:
                raise MmapOmsFactJournalRecoveryRequired("tail successor activation changed")
        return self._successor

    def read_next(self) -> VerifiedJournalBatch | None:
        if self._closed or self._failed:
            raise MmapOmsFactJournalRecoveryRequired("tail is closed/failed")
        try:
            return self._read_next()
        except Exception:
            self._failed = True
            raise

    def _read_next(self) -> VerifiedJournalBatch | None:
        self._validate_cursor()
        bound = self._window_bound or self._journal.read_bound()
        cursor = self.cursor
        if cursor.next_lsn > bound.committed_lsn + 1 or cursor.segment > bound.active_segment:
            raise MmapOmsFactJournalRecoveryRequired("tail cursor is beyond committed Journal")
        if self._current is None:
            self._current = self._open(cursor.segment)
        segment = self._current
        if not segment.first_lsn or segment.first_lsn + cursor.frame != cursor.next_lsn:
            raise MmapOmsFactJournalRecoveryRequired("tail cursor does not match activation")
        if cursor.frame:
            previous_frame = self._frame(segment, cursor.frame - 1)
            if self._reuse_views and self._anchor_cursor == cursor:
                # Re-read and compare ALL bytes, not just cached metadata. The
                # previous returned member already passed full CRC/schema/commit.
                if previous_frame != self._anchor_frame:
                    raise MmapOmsFactJournalRecoveryRequired("tail verified resume anchor changed")
            else:
                previous = MmapOmsFactJournalV1.decode_committed_frame(
                    previous_frame, cursor.next_lsn - 1
                )
                if (
                    previous is None
                    or previous.ordinal != previous.batch_size - 1
                    or previous.batch_id + 1 != cursor.next_batch_id
                ):
                    raise MmapOmsFactJournalRecoveryRequired("tail cursor splits/changes a batch")
        elif segment.first_batch_id != cursor.next_batch_id:
            raise MmapOmsFactJournalRecoveryRequired("tail cursor batch anchor mismatch")
        if cursor.next_lsn == bound.committed_lsn + 1:
            return None
        if cursor.segment < bound.active_segment:
            successor = self._next_segment(cursor.segment + 1)
            try:
                count = successor.first_lsn - segment.first_lsn
                if successor.predecessor != cursor.segment or not 1 <= count <= self._frames:
                    raise MmapOmsFactJournalRecoveryRequired(
                        "tail segment activation chain is invalid"
                    )
                if cursor.frame == count:
                    segment.close()
                    self._current = successor
                    if self._reuse_views:
                        self._successor = None
                        self._successor_identity = None
                    segment = successor
                    successor = None
                    if (
                        segment.first_lsn != cursor.next_lsn
                        or segment.first_batch_id != cursor.next_batch_id
                    ):
                        raise MmapOmsFactJournalRecoveryRequired(
                            "tail successor skips facts/batches"
                        )
                    cursor = JournalTailCursor(
                        cursor.journal_id,
                        cursor.profile_hash,
                        cursor.segment + 1,
                        0,
                        cursor.next_lsn,
                        cursor.next_batch_id,
                    )
                    # The next call checks another segment if necessary, but
                    # never returns a fabricated partial/empty batch.
                    self.cursor = cursor
                    return self._read_next()
                if not cursor.frame < count:
                    raise MmapOmsFactJournalRecoveryRequired("tail cursor is past sealed content")
            finally:
                if successor is not None and not self._reuse_views:
                    successor.close()
        else:
            count = bound.active_frames
        first_frame = self._frame(segment, cursor.frame)
        first = MmapOmsFactJournalV1.decode_committed_frame(first_frame, cursor.next_lsn)
        if (
            first is None
            or first.ordinal != 0
            or first.batch_id != cursor.next_batch_id
            or first.batch_size > self._maximum
            or cursor.frame + first.batch_size > count
            or cursor.next_lsn + first.batch_size - 1 > bound.committed_lsn
        ):
            raise MmapOmsFactJournalRecoveryRequired("tail batch is incomplete/oversized/corrupt")
        rows: list[MmapOmsFactRecordV1] = []
        last_frame = first_frame
        for ordinal in range(first.batch_size):
            # `first` was decoded from an immutable bytes copy above with the
            # complete frame CRC/commit checks. Do not decode that same frame twice.
            if ordinal == 0 and self._reuse_views:
                row = first
            else:
                last_frame = self._frame(segment, cursor.frame + ordinal)
                row = MmapOmsFactJournalV1.decode_committed_frame(
                    last_frame, cursor.next_lsn + ordinal
                )
            if (
                row is None
                or row.ordinal != ordinal
                or row.batch_id != cursor.next_batch_id
                or row.batch_size != first.batch_size
            ):
                raise MmapOmsFactJournalRecoveryRequired("tail batch member is corrupt")
            rows.append(row)
        after = JournalTailCursor(
            cursor.journal_id,
            cursor.profile_hash,
            cursor.segment,
            cursor.frame + len(rows),
            cursor.next_lsn + len(rows),
            cursor.next_batch_id + 1,
        )
        records = tuple(rows)
        result = VerifiedJournalBatch(cursor, after, records, journal_batch_fingerprint(records))
        self.cursor = after
        if self._reuse_views:
            self._anchor_cursor, self._anchor_frame = after, last_frame
        return result

    def close(self) -> None:
        self._closed = True
        self._anchor_cursor = None
        self._anchor_frame = None
        if self._current is not None:
            self._current.close()
            self._current = None
        if self._successor is not None:
            self._successor.close()
            self._successor = None
            self._successor_identity = None
