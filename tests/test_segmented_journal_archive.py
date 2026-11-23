from __future__ import annotations

import os
import time
from unittest.mock import patch

import pytest
from xuanling_tradecore.oms import MmapOmsFactJournalError, MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal

from test_segmented_oms_fact_journal import append


def create(tmp_path):
    return SegmentedOmsFactJournal(
        root=tmp_path / "hot",
        archive_root=tmp_path / "archive",
        journal_id="stream",
        segment_frames=4,
        disk_budget_bytes=20 * 20480,
    )


def sealed(journal, index):
    end = time.monotonic() + 5
    while time.monotonic() < end:
        with journal._catalog_lock:
            if "sha256" in journal._catalog[index]:
                return
        time.sleep(0.001)
    pytest.fail("segment was not sealed")


def test_archive_preserves_complete_tail_and_restart_while_reclaiming_only_hot_copy(tmp_path):
    journal = create(tmp_path)
    tail = None
    try:
        for n in range(1, 13):
            append(journal, n)
        sealed(journal, 1)
        sealed(journal, 2)
        before = journal.stats()
        assert journal.archive_sealed(1)
        assert journal.archive_sealed(2)
        assert not journal.archive_sealed(1)
        assert not (tmp_path / "hot/segment-000000000001.mmap").exists()
        assert journal.segment_read_path(1) == tmp_path / "archive/segment-000000000001.mmap"
        assert journal.stats().archived_bytes == 2 * 20480
        assert journal.stats().allocated_bytes <= before.allocated_bytes
        tail = SegmentedJournalTail(journal)
        rows = []
        while (batch := tail.read_next()) is not None:
            rows.extend(batch.records)
        assert [r.lsn for r in rows] == list(range(1, 13))
    finally:
        if tail:
            tail.close()
        journal.close()
    reopened = create(tmp_path)
    try:
        assert append(reopened, 13) == (13, 13)
    finally:
        reopened.close()
    assert [r.fact.fact_id for r in reopened.records()] == [str(n) for n in range(1, 14)]
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        SegmentedOmsFactJournal(root=tmp_path / "hot", journal_id="stream", segment_frames=4)


def test_archive_failure_before_publication_preserves_hot_authority(tmp_path):
    journal = create(tmp_path)
    append(journal, 1, 2, 3, 4)
    append(journal, 5)
    sealed(journal, 1)
    try:
        with patch.object(journal, "_publish_catalog", side_effect=OSError("publication")):
            with pytest.raises(OSError):
                journal.archive_sealed(1)
        assert (tmp_path / "hot/segment-000000000001.mmap").exists()
        assert (tmp_path / "archive/segment-000000000001.mmap").exists()
    finally:
        journal.close()
    recovered = create(tmp_path)
    try:
        assert recovered.read_bound().committed_lsn == 5
        assert recovered.archive_sealed(1)  # adopts only a matching, verified copy
    finally:
        recovered.close()


def test_archive_never_moves_active_or_unsealed_segments_and_refuses_wrong_copy(tmp_path):
    journal = create(tmp_path)
    try:
        for index in (True, 0, 1, 99):
            with pytest.raises(MmapOmsFactJournalError):
                journal.archive_sealed(index)
        append(journal, 1, 2, 3, 4)
        append(journal, 5)
        sealed(journal, 1)
        target = tmp_path / "archive/segment-000000000001.mmap"
        target.write_bytes(b"wrong")
        target.chmod(0o600)
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            journal.archive_sealed(1)
        assert (tmp_path / "hot/segment-000000000001.mmap").exists()
        assert target.read_bytes() == b"wrong"
    finally:
        journal.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX open mmap survives hot-copy unlink")
def test_existing_mapping_and_reader_open_race_survive_verified_archive_handoff(tmp_path):
    journal = create(tmp_path)
    tail = SegmentedJournalTail(journal)
    other = None
    try:
        append(journal, 1, 2, 3, 4)
        append(journal, 5)
        sealed(journal, 1)
        assert tail.read_next().records[0].lsn == 1
        old = journal.segment_read_path(1)
        assert journal.archive_sealed(1)
        assert tail.read_next().records[0].lsn == 5
        other = SegmentedJournalTail(journal)
        actual = journal.segment_read_path
        calls = []

        def race(index):
            calls.append(index)
            return old if len(calls) == 1 else actual(index)

        with patch.object(journal, "segment_read_path", side_effect=race):
            assert other.read_next().records[0].lsn == 1
        assert calls[:2] == [1, 1]
        assert calls[2:] in ([], [2])  # bounded successor prefetch is legitimate
    finally:
        tail.close()
        if other:
            other.close()
        journal.close()
