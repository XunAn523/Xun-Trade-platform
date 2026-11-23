from __future__ import annotations

from threading import Event, Thread, current_thread
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.oms.journal_tail import JournalTailCursor, SegmentedJournalTail
from xuanling_tradecore.oms.mmap_fact_journal_v1 import (
    MmapOmsFactJournalRecoveryRequired,
    MmapOmsFactJournalV1,
)
from xuanling_tradecore.oms.segmented_fact_journal import _Segment

import test_segmented_oms_fact_journal as fixture


def create(tmp_path):
    return fixture.SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=4
    )


@pytest.mark.parametrize("reuse", (False, True))
def test_live_tail_rotates_resumes_and_never_scans_history(tmp_path, reuse):
    journal = create(tmp_path)
    tail = SegmentedJournalTail(journal, reuse_segment_views=reuse)
    try:
        assert tail.read_next() is None
        records = _Segment.records
        caller = current_thread()

        def checked(segment):
            assert current_thread() is not caller, "tail scanned history"
            return records(segment)

        with patch.object(_Segment, "records", new=checked):
            fixture.append(journal, 1, 2, 3)
            first = tail.read_next()
            assert [r.lsn for r in first.records] == [1, 2, 3]
            cursor = tail.cursor
            tail.close()
            tail = SegmentedJournalTail(journal, cursor=cursor, reuse_segment_views=reuse)
            fixture.append(journal, 4, 5)
            batch = tail.read_next()
            assert [r.lsn for r in batch.records] == [4, 5]
            assert batch.before.segment == 2
            assert tail.read_next() is None
    finally:
        tail.close()
        journal.close()


def test_partial_batch_cannot_escape_writer_committed_fence(tmp_path):
    journal = create(tmp_path)
    tail = SegmentedJournalTail(journal)
    entered, release, read_done = Event(), Event(), Event()
    rows, errors = [], []
    original = _Segment.append_prepared

    def pause(segment, prepared, batch):
        # Simulate the first commit word becoming visible while the writer
        # has not yet committed all members or published next_lsn.
        offset = 4096
        for ordinal, (position, body) in enumerate(prepared):
            start = 4096 + ordinal * offset
            segment._map[start : start + len(body)] = body
            if ordinal == 0:
                import struct

                segment._map[start + 4096 - 8 : start + 4096] = struct.pack(
                    "<Q", position ^ 0x71C43AF29B6E10D5
                )
        entered.set()
        release.wait(3)
        return original(segment, prepared, batch)

    def write():
        try:
            fixture.append(journal, 1, 2)
        except BaseException as exc:
            errors.append(exc)

    def read():
        try:
            rows.append(tail.read_next())
        except BaseException as exc:
            errors.append(exc)
        finally:
            read_done.set()

    writer, reader = Thread(target=write), Thread(target=read)
    try:
        with patch.object(_Segment, "append_prepared", new=pause):
            writer.start()
            assert entered.wait(2)
            reader.start()
            assert not read_done.wait(0.02)
            release.set()
            writer.join(3)
            reader.join(3)
        assert not errors
        assert len(rows[0].records) == 2
    finally:
        release.set()
        if writer.ident:
            writer.join(3)
        if reader.ident:
            reader.join(3)
        tail.close()
        journal.close()


def test_slow_reader_open_does_not_hold_writer_lock(tmp_path):
    journal = create(tmp_path)
    fixture.append(journal, 1)
    tail = SegmentedJournalTail(journal)
    entered, release = Event(), Event()
    original = _Segment.__init__

    def slow(segment, *args, **kwargs):
        if current_thread().name == "tail-reader":
            entered.set()
            release.wait(3)
        original(segment, *args, **kwargs)

    reader = Thread(target=tail.read_next, name="tail-reader")
    try:
        with patch.object(_Segment, "__init__", new=slow):
            reader.start()
            assert entered.wait(2)
            assert fixture.append(journal, 2) == (2, 2)
            release.set()
            reader.join(3)
        assert not reader.is_alive()
    finally:
        release.set()
        if reader.ident:
            reader.join(3)
        tail.close()
        journal.close()


@pytest.mark.parametrize("reuse", (False, True))
def test_corrupt_member_and_oversize_fail_before_returning_any_batch(tmp_path, reuse):
    journal = create(tmp_path)
    fixture.append(journal, 1, 2)
    too_small = SegmentedJournalTail(journal, maximum_batch_facts=1)
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        too_small.read_next()
    too_small.close()
    tail = SegmentedJournalTail(journal, reuse_segment_views=reuse)
    try:
        path = journal.path / "segment-000000000001.mmap"
        with path.open("r+b") as stream:
            stream.seek(4096 * 2 + 35)
            stream.write(b"BAD")
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            tail.read_next()
        assert tail.cursor.next_lsn == 1
        with pytest.raises(MmapOmsFactJournalRecoveryRequired, match="failed"):
            tail.read_next()
    finally:
        tail.close()
        journal.close()


@pytest.mark.parametrize(
    "change",
    [
        {"journal_id": "wrong"},
        {"frame": True},
        {"profile_hash": "wrong"},
        {"next_lsn": 99},
        {"frame": 1, "next_lsn": 2},
    ],
)
def test_bad_cursor_does_not_skip_or_split_facts(tmp_path, change):
    journal = create(tmp_path)
    fixture.append(journal, 1, 2)
    cursor = JournalTailCursor(journal.journal_id, journal.profile_hash)
    tail = None
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            tail = SegmentedJournalTail(journal, cursor=msgspec.structs.replace(cursor, **change))
            tail.read_next()
    finally:
        if tail:
            tail.close()
        journal.close()


def test_many_rotations_are_suffix_only_and_tail_maps_are_read_only(tmp_path):
    journal = create(tmp_path)
    tail = SegmentedJournalTail(journal)
    try:
        for n in range(200):
            fixture.append(journal, n * 3 + 1, n * 3 + 2, n * 3 + 3)
            batch = tail.read_next()
            assert batch.after.next_lsn == 3 * (n + 1) + 1
            with pytest.raises(TypeError):
                tail._current._map[0] = 0
        assert tail.read_next() is None
    finally:
        tail.close()
        journal.close()


def test_reused_tail_maps_open_once_per_segment_not_once_per_batch(tmp_path):
    journal = fixture.SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=128
    )
    tail = None
    try:
        for n in range(300):
            fixture.append(journal, n + 1)
        tail = SegmentedJournalTail(journal, reuse_segment_views=True)
        opened = []
        original = tail._open

        def open_view(index):
            result = original(index)
            opened.append(result)
            return result

        with patch.object(tail, "_open", side_effect=open_view):
            batches = tuple(iter(tail.read_next, None))
        assert len(batches) == 300
        assert [segment.index for segment in opened] == [1, 2, 3]
        assert all(segment._closed for segment in opened[:-1])
        tail.close()
        assert all(segment._closed for segment in opened)
    finally:
        if tail is not None:
            tail.close()
        journal.close()


@pytest.mark.parametrize("offset", (0, 128, 152, 156))
def test_cached_successor_rechecks_static_activation_crc_and_commit_word(tmp_path, offset):
    journal = create(tmp_path)
    tail = None
    try:
        fixture.append(journal, 1)
        fixture.append(journal, 2)
        fixture.append(journal, 3, 4)
        fixture.append(journal, 5)
        tail = SegmentedJournalTail(journal, reuse_segment_views=True)
        assert tail.read_next().after.next_lsn == 2
        assert tail._successor is not None
        with tail._successor.path.open("r+b") as stream:
            stream.seek(offset)
            old = stream.read(1)
            stream.seek(offset)
            stream.write(bytes([old[0] ^ 1]))
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            tail.read_next()
        assert tail.cursor.next_lsn == 2
        with pytest.raises(MmapOmsFactJournalRecoveryRequired, match="failed"):
            tail.read_next()
    finally:
        if tail is not None:
            tail.close()
        journal.close()


def test_segment_view_flag_rejects_truthy_non_bool(tmp_path):
    journal = create(tmp_path)
    try:
        with pytest.raises(ValueError, match="reuse flag"):
            SegmentedJournalTail(journal, reuse_segment_views=1)
    finally:
        journal.close()


def test_reused_anchor_is_exact_bytes_and_fresh_resume_still_decodes_predecessor(tmp_path):
    journal = create(tmp_path)
    tail = replacement = None
    try:
        fixture.append(journal, 1, 2)
        fixture.append(journal, 3, 4)
        tail = SegmentedJournalTail(journal, reuse_segment_views=True)
        seen, caller = [], current_thread()
        original = MmapOmsFactJournalV1.decode_committed_frame

        def decode(frame, lsn):
            if current_thread() is caller:
                seen.append(lsn)
            return original(frame, lsn)

        with patch.object(MmapOmsFactJournalV1, "decode_committed_frame", side_effect=decode):
            first = tail.read_next()
            assert seen == [1, 2]
            assert tail.read_next().after.next_lsn == 5
            assert seen == [1, 2, 3, 4]  # prior anchor was re-read and byte-compared
            replacement = SegmentedJournalTail(
                journal, cursor=first.after, reuse_segment_views=True
            )
            assert replacement.read_next().after.next_lsn == 5
            assert seen[-3:] == [2, 3, 4]  # new reader cannot trust another tail's cache
        path = journal.path / "segment-000000000001.mmap"
        with path.open("r+b") as stream:
            stream.seek(4096 * 4 + 35)
            stream.write(b"BAD")
        with pytest.raises(MmapOmsFactJournalRecoveryRequired, match="anchor changed"):
            tail.read_next()
        assert tail.cursor.next_lsn == 5
    finally:
        if tail is not None:
            tail.close()
        if replacement is not None:
            replacement.close()
        journal.close()
