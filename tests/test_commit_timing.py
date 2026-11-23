import json
from queue import Empty, Full
from unittest.mock import patch

import pytest
from xuanling_tradecore.oms import MmapOmsFactJournalError
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity
from xuanling_tradecore.persistence.commit_timing import (
    RECORD,
    CommitTimingWriter,
    _BatchDrainQueue,
    verify_commit_timing,
)

from test_segmented_oms_fact_journal import append


def test_clock_label_includes_boot_and_time_namespace_and_never_guesses_when_unknown():
    from pathlib import Path
    from types import SimpleNamespace

    with (
        patch("xuanling_tradecore.persistence.clock_identity.sys.platform", "linux"),
        patch.object(Path, "read_text", side_effect=["boot", "monotonic 0", "boot", "monotonic 0"]),
        patch.object(
            Path,
            "stat",
            side_effect=[SimpleNamespace(st_dev=4, st_ino=1), SimpleNamespace(st_dev=4, st_ino=2)],
        ),
    ):
        first, second = monotonic_clock_identity(), monotonic_clock_identity()
        assert first and second and first != second and len(first) == len(second) == 64
    with (
        patch("xuanling_tradecore.persistence.clock_identity.sys.platform", "linux"),
        patch.object(Path, "read_text", side_effect=OSError),
    ):
        assert monotonic_clock_identity() is None


@pytest.mark.parametrize("batch", ["0", "1"])
def test_rotating_fixed_evidence_is_complete_private_and_has_exact_time_ranges(
    tmp_path, monkeypatch, batch
):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", batch)
    root = tmp_path / "evidence"
    writer = CommitTimingWriter(
        root=root, journal_id="source", first_lsn=11, maximum_bytes=4096, segment_records=2
    )
    for n in range(5):
        assert writer.observe(11 + n * 2, 12 + n * 2, 2, 100 + n * 10, 102 + n * 10)
    writer.close()
    assert verify_commit_timing(root) == (5, 20)
    data = b"".join(p.read_bytes() for p in sorted(root.glob("*.bin")))
    assert len(data) == RECORD.size * 5
    assert RECORD.unpack(data[: RECORD.size]) == (1, 11, 12, 100, 102, 2)
    raw = json.loads((root / "manifest.json").read_text())
    assert raw["formal_qualification"] is False and len(raw["segments"]) == 3
    assert not writer.failed
    for path in root.iterdir():
        if __import__("os").name == "posix":
            assert not path.stat().st_mode & 0o077
    with pytest.raises(FileExistsError):
        CommitTimingWriter(root=root, journal_id="source", first_lsn=21, maximum_bytes=4096)


@pytest.mark.parametrize(
    "row", ((1, 2, 2, 9, 8), (2, 3, 2, 9, 10), (1, 1, 0, 9, 10), (1, 2, 2, True, 10))
)
def test_invalid_timing_or_lsn_never_becomes_complete_evidence(tmp_path, row):
    root = tmp_path / "evidence"
    writer = CommitTimingWriter(root=root, journal_id="source", first_lsn=1, maximum_bytes=4096)
    assert not writer.observe(*row)
    writer.close()
    with pytest.raises(ValueError, match="incomplete"):
        verify_commit_timing(root)


@pytest.mark.parametrize("batch", ["0", "1"])
def test_quota_or_corrupt_segment_never_passes_integrity(tmp_path, monkeypatch, batch):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", batch)
    root = tmp_path / "quota"
    writer = CommitTimingWriter(
        root=root, journal_id="source", first_lsn=1, maximum_bytes=RECORD.size
    )
    writer.observe(1, 2, 2, 1, 2)
    writer.observe(3, 4, 2, 3, 4)
    writer.close()
    assert writer.failed and writer.stats()["bytes"] == RECORD.size
    with pytest.raises(ValueError):
        verify_commit_timing(root)
    root = tmp_path / "corrupt"
    writer = CommitTimingWriter(root=root, journal_id="source", first_lsn=1, maximum_bytes=4096)
    writer.observe(1, 2, 2, 1, 2)
    writer.close()
    path = next(root.glob("*.bin"))
    with path.open("r+b") as target:
        target.seek(8)
        target.write(b"\xff")
    with pytest.raises(ValueError):
        verify_commit_timing(root)


@pytest.mark.parametrize("batch", ["0", "1"])
def test_full_queue_is_nonblocking_and_loss_remains_visible(tmp_path, monkeypatch, batch):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", batch)
    root = tmp_path / "evidence"
    with patch("xuanling_tradecore.persistence.commit_timing.Thread.start"):
        writer = CommitTimingWriter(
            root=root, journal_id="source", first_lsn=1, maximum_bytes=4096, capacity=1
        )
    assert writer.observe(1, 2, 2, 1, 2)
    assert not writer.observe(3, 4, 2, 3, 4)
    writer._stop.set()
    (writer._run_batched if batch == "1" else writer._run)()
    assert writer.stats()["dropped"] == 1 and writer.stats()["written"] == 1
    with pytest.raises(ValueError):
        verify_commit_timing(root)


def test_batch_queue_is_bounded_fifo_and_never_waits_to_fill():
    queue = _BatchDrainQueue(3)
    with pytest.raises(Empty):
        queue.get_available(3, 0)
    queue.put_nowait((1,))
    assert queue.get_available(3, 0) == ((1,),)
    for value in (2, 3, 4):
        queue.put_nowait((value,))
    with pytest.raises(Full):
        queue.put_nowait((5,))
    assert queue.get_available(2, 0) == ((2,), (3,))
    assert queue.get_available(3, 0) == ((4,),)
    with pytest.raises(ValueError):
        queue.get_available(65, 0)


def test_invalid_batch_drain_fails_before_creating_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", "invalid")
    root = tmp_path / "evidence"
    with pytest.raises(ValueError, match="batch diagnostic"):
        CommitTimingWriter(root=root, journal_id="source", first_lsn=1, maximum_bytes=4096)
    assert not root.exists()


@pytest.mark.parametrize("batch", ["0", "1"])
def test_batch_drain_preserves_exact_large_rotated_source_ledger(tmp_path, monkeypatch, batch):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", batch)
    root = tmp_path / "evidence"
    writer = CommitTimingWriter(
        root=root, journal_id="source", first_lsn=1, maximum_bytes=RECORD.size * 5000,
        capacity=5000, segment_records=257,
    )
    for index in range(5000):
        assert writer.observe(index + 1, index + 1, 1, index * 2 + 1, index * 2 + 2)
    writer.close()
    assert verify_commit_timing(root) == (5000, 5000)
    assert writer.stats()["dropped"] == 0


def test_batch_drain_matches_original_records_and_chain(tmp_path, monkeypatch):
    outputs = []
    for batch in ("0", "1"):
        monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", batch)
        root = tmp_path / batch
        writer = CommitTimingWriter(
            root=root, journal_id="source", first_lsn=1, maximum_bytes=RECORD.size * 1000,
            capacity=1000, segment_records=73,
        )
        for index in range(1000):
            assert writer.observe(index + 1, index + 1, 1, index * 2 + 1, index * 2 + 2)
        writer.close()
        assert verify_commit_timing(root) == (1000, 1000)
        manifest = json.loads((root / "manifest.json").read_text())
        outputs.append((
            [path.read_bytes() for path in sorted(root.glob("*.bin"))],
            manifest["chain"],
        ))
    assert outputs[0] == outputs[1]


def test_batch_drain_io_failure_never_produces_complete_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", "1")
    root = tmp_path / "evidence"
    with patch("xuanling_tradecore.persistence.commit_timing.Thread.start"):
        writer = CommitTimingWriter(root=root, journal_id="source", first_lsn=1, maximum_bytes=4096)
    assert writer.observe(1, 1, 1, 1, 2)
    assert writer.observe(2, 2, 1, 3, 4)
    writer._stop.set()
    with patch("xuanling_tradecore.persistence.commit_timing.open_private", side_effect=OSError):
        writer._run_batched()
    assert writer.failed and writer.stats()["written"] == 0
    with pytest.raises(ValueError):
        verify_commit_timing(root)


@pytest.mark.parametrize("change", ([], {"first_lsn": True}, {"written": "0"}))
def test_manifest_shape_and_numeric_types_are_not_coerced(tmp_path, change):
    root = tmp_path / "evidence"
    writer = CommitTimingWriter(root=root, journal_id="source", first_lsn=1, maximum_bytes=4096)
    writer.close()
    manifest = root / "manifest.json"
    original = json.loads(manifest.read_text())
    manifest.write_text(json.dumps(change if isinstance(change, list) else {**original, **change}))
    with pytest.raises(ValueError):
        verify_commit_timing(root)


@pytest.mark.parametrize("raises", (False, True))
def test_observer_failure_does_not_turn_committed_fact_into_absence_or_retry(tmp_path, raises):
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="source", segment_frames=4)
    calls = []

    def observer(*values):
        calls.append(values)
        if raises:
            raise RuntimeError("private diagnostic failure")
        return False

    journal.bind_commit_observer(observer)
    with pytest.raises(MmapOmsFactJournalError):
        journal.bind_commit_observer(observer)
    append(journal, 1, 2)
    assert journal.read_bound().committed_lsn == 2
    assert journal.stats().commit_timing_failed
    assert len(calls) == 1 and calls[0][:3] == (1, 2, 2)
    assert calls[0][3] <= calls[0][4]
    journal.close()
    assert [record.lsn for record in journal.records()] == [1, 2]


def test_journal_observer_writes_real_commit_ranges_without_changing_source_bytes(tmp_path):
    original = SegmentedOmsFactJournal(
        root=tmp_path / "original", journal_id="source", segment_frames=4
    )
    observed = SegmentedOmsFactJournal(
        root=tmp_path / "observed", journal_id="source", segment_frames=4
    )
    writer = CommitTimingWriter(
        root=tmp_path / "evidence", journal_id="source", first_lsn=1, maximum_bytes=4096
    )
    try:
        observed.bind_commit_observer(writer.observe)
        append(original, 1, 2)
        append(observed, 1, 2)
    finally:
        original.close()
        observed.close()
        writer.close()
    assert verify_commit_timing(tmp_path / "evidence") == (1, 2)
    assert (tmp_path / "original/segment-000000000001.mmap").read_bytes() == (
        tmp_path / "observed/segment-000000000001.mmap"
    ).read_bytes()
