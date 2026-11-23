from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from xuanling_tradecore.oms import (
    MmapOmsFactJournalError,
    MmapOmsFactJournalFull,
    MmapOmsFactJournalRecoveryRequired,
    OmsFactKind,
    OmsJournalFact,
)
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal


def fact(index: int) -> OmsJournalFact:
    return OmsJournalFact(
        str(index), OmsFactKind.ORDER_ATTRIBUTED, "account", "order", "execution", {}
    )


def append(journal: SegmentedOmsFactJournal, *indexes: int) -> tuple[int, int]:
    deadline = time.monotonic() + 5
    while True:
        try:
            commit = journal.append_batch(tuple(fact(index) for index in indexes))
            return commit.first_lsn, commit.final_lsn
        except MmapOmsFactJournalFull:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.001)


def test_uneven_batches_rotate_without_crossing_and_resume_global_lsn(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    assert append(journal, 1, 2, 3) == (1, 3)
    assert append(journal, 4, 5) == (4, 5)
    assert append(journal, 6) == (6, 6)
    journal.close()
    records = list(journal.records())
    assert [r.lsn for r in records] == list(range(1, 7))
    assert [r.batch_id for r in records] == [1, 1, 1, 2, 2, 3]
    reopened = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    assert append(reopened, 7, 8) == (7, 8)
    reopened.close()
    assert [r.fact.fact_id for r in reopened.records()] == [str(i) for i in range(1, 9)]


def test_append_does_not_open_or_fsync_even_at_rotation(tmp_path: Path) -> None:
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=4
    )
    real_open, real_sync = os.open, os.fsync
    caller = threading.get_ident()

    def checked_open(*args: object, **kwargs: object) -> int:
        assert threading.get_ident() != caller, "file opened on append thread"
        return real_open(*args, **kwargs)

    def checked_sync(*args: object, **kwargs: object) -> None:
        assert threading.get_ident() != caller, "fsync on append thread"
        real_sync(*args, **kwargs)

    try:
        with patch("os.open", checked_open), patch("os.fsync", checked_sync):
            assert append(journal, 1, 2, 3) == (1, 3)
            assert append(journal, 4, 5) == (4, 5)
    finally:
        journal.close()


def test_sealed_cache_advice_rejects_invalid_profile_before_creating_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT", "invalid")
    root = tmp_path / "journal"
    with pytest.raises(MmapOmsFactJournalError, match="cache diagnostic"):
        SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    assert not root.exists()


@pytest.mark.skipif(not hasattr(os, "posix_fadvise"), reason="Linux advisory API required")
@pytest.mark.parametrize("enabled", [False, True])
def test_sealed_cache_advice_is_off_hot_path_and_preserves_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT", "1" if enabled else "0")
    root = tmp_path / "journal"
    calls = []
    caller = threading.get_ident()

    def advise(fd: int, offset: int, length: int, advice: int) -> None:
        manifest = json.loads((root / "segments.json").read_text())
        assert threading.get_ident() != caller
        assert offset == length == 0 and advice == os.POSIX_FADV_DONTNEED
        assert any(row.get("sha256") for row in manifest["segments"])
        assert os.fstat(fd).st_size > 0
        calls.append(1)

    monkeypatch.setattr(os, "posix_fadvise", advise)
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    append(journal, 1, 2, 3, 4)
    append(journal, 5)
    journal.close()
    assert bool(calls) is enabled
    assert [r.fact.fact_id for r in journal.records()] == [str(i) for i in range(1, 6)]
    reopened = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    reopened.close()
    assert [r.fact.fact_id for r in reopened.records()] == [str(i) for i in range(1, 6)]


@pytest.mark.skipif(not hasattr(os, "posix_fadvise"), reason="Linux advisory API required")
def test_sealed_cache_advice_failure_closes_writer_without_losing_facts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT", "1")

    def fail_advice(*args: object) -> None:
        raise OSError("injected advisory failure")

    monkeypatch.setattr(os, "posix_fadvise", fail_advice)
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    append(journal, 1, 2, 3, 4)
    append(journal, 5)
    deadline = time.monotonic() + 5
    while not journal.stats().maintenance_failed and time.monotonic() < deadline:
        time.sleep(0.001)
    assert journal.stats().maintenance_failed
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        journal.append_batch((fact(6),))
    journal.close()
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT", "0")
    reopened = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    reopened.close()
    assert [r.fact.fact_id for r in reopened.records()] == [str(i) for i in range(1, 6)]


def test_thousand_rotations_have_complete_ordered_facts_and_bounded_mappings(
    tmp_path: Path,
) -> None:
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=2
    )
    try:
        for index in range(1, 2003, 2):
            append(journal, index, index + 1)
        stats = journal.stats()
        assert stats.rotations == 1000
        assert stats.prepared_segments <= 2 and stats.sealed_waiting <= 3
    finally:
        journal.close()
    records = list(journal.records())
    assert len(records) == 2002
    assert [r.lsn for r in records] == list(range(1, 2003))


def test_disk_budget_refuses_new_batches_without_silently_deleting(tmp_path: Path) -> None:
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal",
        journal_id="stream",
        segment_frames=2,
        disk_budget_bytes=3 * (4096 + 2 * 4096),
    )
    try:
        for index in (1, 3, 5):
            append(journal, index, index + 1)
        with pytest.raises(MmapOmsFactJournalFull):
            journal.append_batch((fact(7),))
        assert journal.stats().next_lsn == 7
    finally:
        journal.close()
    assert len(list(journal.records())) == 6
    assert len(list((tmp_path / "journal").glob("segment-*.mmap"))) == 3


def test_second_writer_and_changed_profile_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    try:
        with pytest.raises(OSError):
            SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    finally:
        journal.close()
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        SegmentedOmsFactJournal(root=root, journal_id="different", segment_frames=4)


@pytest.mark.parametrize("kind", ["commit", "activation", "manifest", "missing", "digest"])
def test_corrupt_tail_or_metadata_never_reopens_as_empty(tmp_path: Path, kind: str) -> None:
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    append(journal, 1, 2)
    if kind == "digest":
        append(journal, 3, 4, 5)  # seal first segment
    journal.close()
    path = root / "segment-000000000001.mmap"
    if kind == "missing":
        path.unlink()
    elif kind == "manifest":
        manifest = root / "segments.json"
        payload = json.loads(manifest.read_text())
        payload["segments"].append(payload["segments"][0])
        manifest.write_text(json.dumps(payload))
    else:
        with path.open("r+b") as output:
            offset = {"commit": 4096 + 2 * 4096 - 8, "activation": 128, "digest": 4000}[kind]
            output.seek(offset)
            output.write(b"\xff" * 8)
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)


def test_invalid_batch_does_not_consume_a_global_lsn(tmp_path: Path) -> None:
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=2
    )
    try:
        for batch in [(), (fact(1), fact(1)), (fact(1), fact(2), fact(3))]:
            with pytest.raises(MmapOmsFactJournalError):
                journal.append_batch(batch)
        assert append(journal, 1) == (1, 1)
    finally:
        journal.close()


def test_replay_never_exposes_first_member_of_a_torn_batch(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    append(journal, 1, 2)
    journal.close()
    with (root / "segment-000000000001.mmap").open("r+b") as output:
        output.seek(4096 + 2 * 4096 - 8)
        output.write(b"\0" * 8)
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        next(journal.records())


def test_process_exit_after_rotation_preserves_committed_batches(tmp_path: Path) -> None:
    root = tmp_path / "journal"
    script = """
import os,sys
from pathlib import Path
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal
from xuanling_tradecore.oms import OmsJournalFact,OmsFactKind
j=SegmentedOmsFactJournal(root=Path(sys.argv[1]),journal_id='stream',segment_frames=2)
for start in (1,3):
 j.append_batch(tuple(OmsJournalFact(str(i),OmsFactKind.ORDER_ATTRIBUTED,'a','o','e',{}) for i in (start,start+1)))
os._exit(23)
"""
    result = subprocess.run([sys.executable, "-c", script, str(root)], timeout=10, check=False)
    assert result.returncode == 23
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=2)
    try:
        assert append(journal, 5, 6) == (5, 6)
    finally:
        journal.close()
    assert len(list(journal.records())) == 6
