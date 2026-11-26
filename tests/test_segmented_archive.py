import json

import pytest
from emt_gateway.journal import FileJournal, JournalAppend, JournalFailure
from emt_gateway.journal_archive import AsyncFileJournalArchive
from emt_gateway.segmented_archive import SegmentedArchiveStore


def entry(index):
    return JournalAppend("OFFLINE_ARCHIVE", f"command-{index}", f"correlation-{index}", "a" * 64)


def test_journal_tail_cache_is_bounded_without_changing_disk_or_lsn(tmp_path):
    path = tmp_path / "tail.emtj"
    with FileJournal(path, retained_records=2) as journal:
        journal.append_batch(tuple(entry(i) for i in range(10)))
        assert journal.record_count == 10
        assert [row.lsn for row in journal.records()] == [9, 10]
    with FileJournal(path, retained_records=0) as journal:
        assert journal.record_count == 10
        assert journal.records() == ()
        assert journal.append_batch((entry(11),))[0].lsn == 11
    with FileJournal(path) as journal:
        assert len(journal.records()) == 11


def test_segments_rotate_and_explicit_retention_prunes_only_old_sealed_data(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_records=2, maximum_segments=3)
    store.append_batch(tuple(entry(i) for i in range(9)))
    assert store.snapshot().sealed_segments == 2
    assert store.snapshot().pruned_segments == 2
    assert store.snapshot().retained_record_objects == 0
    store.close()
    index = json.loads((store.directory / "index.json").read_text())
    assert [row["segment_id"] for row in index["sealed"]] == [3, 4]
    assert index["active"]["records"] == 1
    reopened = SegmentedArchiveStore(path, maximum_segment_records=2, maximum_segments=3)
    reopened.append_batch((entry(9), entry(10)))
    reopened.close()
    assert len(list(reopened.directory.glob("*.emtj"))) == 2


def test_default_policy_preserves_all_segments_and_rebuilds_missing_index(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_records=2)
    store.append_batch(tuple(entry(i) for i in range(9)))
    store.close()
    (store.directory / "index.json").unlink()
    reopened = SegmentedArchiveStore(path, maximum_segment_records=2)
    assert reopened.snapshot().sealed_segments == 4
    assert reopened.snapshot().pruned_segments == 0
    reopened.close()
    assert (store.directory / "index.json").is_file()


def test_corrupt_sealed_segment_is_not_truncated_or_silently_repaired(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_records=1)
    store.append_batch((entry(1), entry(2)))
    store.close()
    sealed = next(store.directory.glob("*.emtj"))
    damaged = sealed.read_bytes() + b"incomplete-tail"
    sealed.write_bytes(damaged)
    with pytest.raises(JournalFailure):
        SegmentedArchiveStore(path)
    assert sealed.read_bytes() == damaged


def test_byte_budget_rotates_without_exceeding_segment_size(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_bytes=8192)
    rows = tuple(JournalAppend("K", f"command-{i}", "c" * 3000, "a" * 64) for i in range(10))
    store.append_batch(rows)
    store.close()
    assert all(p.stat().st_size <= 8192 for p in store.directory.glob("*.emtj"))
    assert path.stat().st_size <= 8192


def test_index_rebuild_recovers_durable_active_records_after_process_exit(tmp_path):
    import subprocess
    import sys

    path = tmp_path / "crash.emtj"
    script = """
import os,sys
from emt_gateway.segmented_archive import SegmentedArchiveStore
from emt_gateway.journal import JournalAppend
store=SegmentedArchiveStore(sys.argv[1],maximum_segment_records=2)
store.append_batch(tuple(JournalAppend('K',str(i),'r','a'*64) for i in range(3)))
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", script, str(path)], check=True, timeout=20)
    restored = SegmentedArchiveStore(path, maximum_segment_records=2)
    restored.close()
    payload = json.loads((restored.directory / "index.json").read_text())
    assert sum(row["records"] for row in payload["sealed"]) + payload["active"]["records"] == 3


def test_sealed_index_checksum_detects_valid_crc_content_changes(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_records=1)
    store.append_batch((entry(1), entry(2)))
    store.close()
    sealed = next(store.directory.glob("*.emtj"))
    with FileJournal(sealed, exclusive=False) as writer:
        writer.append_batch((entry(3),))
    with pytest.raises(JournalFailure, match="indexed checksum"):
        SegmentedArchiveStore(path)


def test_missing_indexed_tail_segment_is_not_treated_as_retention(tmp_path):
    path = tmp_path / "archive.emtj"
    store = SegmentedArchiveStore(path, maximum_segment_records=1)
    store.append_batch((entry(1), entry(2), entry(3)))
    store.close()
    sorted(store.directory.glob("*.emtj"))[-1].unlink()
    with pytest.raises(JournalFailure, match="missing"):
        SegmentedArchiveStore(path)


def test_archive_owner_lease_survives_segment_rotation(tmp_path):
    path = tmp_path / "archive.emtj"
    first = SegmentedArchiveStore(path, maximum_segment_records=1)
    first.append_batch((entry(1), entry(2)))
    with pytest.raises(RuntimeError):
        SegmentedArchiveStore(path)
    first.close()


def test_async_archive_drains_all_accepted_records_and_exposes_retention(tmp_path):
    archive = AsyncFileJournalArchive(
        tmp_path / "async.emtj", capacity=100, maximum_segment_records=2, maximum_segments=3
    )
    for i in range(12):
        assert archive.offer(
            kind="K", command_id=f"c-{i}", correlation_id=f"r-{i}", payload_hash="a" * 64
        )
    archive.close()
    status = archive.snapshot()
    assert status.persisted == 12
    assert status.failed == 0
    assert status.retained_record_objects == 0
    assert status.sealed_segments == 2


def test_close_timeout_does_not_close_storage_under_a_live_writer(tmp_path, monkeypatch):
    from threading import Event

    archive = AsyncFileJournalArchive(tmp_path / "slow.emtj")
    started, release = Event(), Event()
    original = archive._journal.append_batch

    def blocked(entries):
        started.set()
        assert release.wait(5)
        return original(entries)

    monkeypatch.setattr(archive._journal, "append_batch", blocked)
    assert archive.offer(kind="K", command_id="c", correlation_id="r", payload_hash="a" * 64)
    assert started.wait(5)
    try:
        with pytest.raises(TimeoutError):
            archive.close(timeout_seconds=0.01)
        assert not archive._journal._closed
    finally:
        release.set()
        archive.close()
    assert archive.snapshot().persisted == 1


def test_failed_worker_start_releases_archive_ownership(tmp_path):
    from unittest.mock import patch

    path = tmp_path / "start-failure.emtj"
    with patch("emt_gateway.journal_archive.Thread.start", side_effect=RuntimeError("no thread")):
        with pytest.raises(RuntimeError, match="no thread"):
            AsyncFileJournalArchive(path)
    retry = AsyncFileJournalArchive(path)
    retry.close()
