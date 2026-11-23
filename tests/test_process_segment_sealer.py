from __future__ import annotations

import hashlib
import json
import os
import signal
import time
from unittest.mock import patch

import pytest
from tools.performance.analyze_segment_maintenance import analyze
from xuanling_tradecore.oms import MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal
from xuanling_tradecore.persistence.process_segment_sealer import ProcessSegmentSealer
from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace

from test_segmented_oms_fact_journal import append


def source(tmp_path):
    root = tmp_path / "journal"
    journal = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    append(journal, 1, 2, 3, 4)
    append(journal, 5)
    journal.close()
    return root


def test_child_verifies_exact_cut_and_full_digest_without_parent_record_scan(tmp_path):
    root = source(tmp_path)
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    try:
        with patch(
            "xuanling_tradecore.oms.segmented_fact_journal._Segment.records",
            side_effect=AssertionError("parent decoded sealed source"),
        ):
            digest = owner(1, (1, 1, 0, 4, 2))
        assert (
            digest == hashlib.sha256((root / "segment-000000000001.mmap").read_bytes()).hexdigest()
        )
    finally:
        owner.close()


def test_opt_in_maintenance_trace_has_matched_redacted_phases(tmp_path, monkeypatch):
    root = source(tmp_path)
    trace_root = tmp_path / "traces"
    trace_root.mkdir(mode=0o700)
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEGMENT_TRACE_DIR", str(trace_root))
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    try:
        owner.prepare(100, True)
        owner(1, (1, 1, 0, 4, 2))
    finally:
        owner.close()
    manifest = next(trace_root.glob("segment-maintenance-*.manifest.json"))
    count = verify_closed_request_trace(manifest)
    status_path = manifest.with_name(manifest.name.replace(".manifest.json", ".status.json"))
    status = json.loads(status_path.read_text())
    clock = status["clock_identity_sha256"]
    if clock is None:
        with pytest.raises(ValueError, match="invalid clock"):
            analyze(manifest, expected_clock=clock)
        # Windows has no Linux boot identity; use a fixture-only identity for
        # structural verification, never as a performance qualification.
        clock = "a" * 64
        status["clock_identity_sha256"] = clock
        status_path.write_text(json.dumps(status))
    report = analyze(manifest, expected_clock=clock)
    assert report["operations"] == 2 and report["failed_phases"] == 0
    assert report["records"] == count and report["formal_qualification"] is False
    with pytest.raises(ValueError, match="incompatible"):
        analyze(manifest, expected_clock="0" * 64)
    records = [
        json.loads(line)
        for path in trace_root.glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert count == len(records)
    assert {r["operation"] for r in records} == {"prepare", "seal"}
    for serial in (1, 2):
        group = [r for r in records if r["serial"] == serial]
        assert group[0]["phase"] == group[-1]["phase"] == "operation"
        assert group[0]["state"] == "BEGIN" and group[-1]["state"] == "END"
        for phase in {r["phase"] for r in group}:
            samples = [r for r in group if r["phase"] == phase]
            assert [r["state"] for r in samples] == ["BEGIN", "END"]
            assert samples[0]["at_ns"] <= samples[1]["at_ns"]
    assert {r["phase"] for r in records} >= {
        "ftruncate",
        "prefault",
        "prepare_flush",
        "prepare_fsync",
        "seal_validate",
        "seal_flush",
        "seal_fsync",
        "seal_digest",
    }
    assert "stream" not in repr(records) and str(root) not in repr(records)


def test_maintenance_trace_marks_failed_syscall_without_false_completion(tmp_path):
    from xuanling_tradecore.oms.segmented_fact_journal import _Segment

    events = []
    with patch(
        "xuanling_tradecore.oms.segmented_fact_journal.os.ftruncate",
        side_effect=OSError("private failure text"),
    ):
        with pytest.raises(OSError):
            _Segment(
                tmp_path / "segment.mmap",
                index=1,
                frames=4,
                journal_id="stream",
                create=True,
                stage_event=lambda phase, state, at: events.append((phase, state, at)),
            )
    assert [(phase, state) for phase, state, _ in events] == [
        ("ftruncate", "BEGIN"),
        ("ftruncate", "FAILED"),
    ]
    assert "private failure text" not in repr(events)


@pytest.mark.parametrize("pages,expected_pauses", [(64, 8), (256, 2)])
def test_paced_prefault_is_opt_in_and_preserves_complete_prepared_bytes(
    tmp_path, pages, expected_pauses
):
    from xuanling_tradecore.oms.segmented_fact_journal import _Segment

    baseline = tmp_path / "baseline.mmap"
    paced = tmp_path / "paced.mmap"
    with patch("xuanling_tradecore.oms.segmented_fact_journal.sleep") as pause:
        first = _Segment(baseline, index=1, frames=513, journal_id="stream", create=True)
        first.close()
        pause.assert_not_called()
        second = _Segment(
            paced,
            index=1,
            frames=513,
            journal_id="stream",
            create=True,
            diagnostic_paced_prefault_pages=pages,
        )
        second.close()
        assert pause.call_count == expected_pauses
        assert all(call.args == (0.001,) for call in pause.call_args_list)
    assert baseline.read_bytes() == paced.read_bytes()


@pytest.mark.parametrize("pacing", ["1", "64"])
def test_paced_child_prepares_complete_segment_before_success(tmp_path, monkeypatch, pacing):
    from xuanling_tradecore.oms.segmented_fact_journal import _Segment

    root = tmp_path / "journal"
    root.mkdir(mode=0o700)
    expected = tmp_path / "expected.mmap"
    baseline = _Segment(expected, index=1, frames=513, journal_id="stream", create=True)
    baseline.close()
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_PACED_PREFAULT", pacing)
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=513)
    try:
        owner.prepare(1, True)
    finally:
        owner.close()
    assert (root / "segment-000000000001.mmap").read_bytes() == expected.read_bytes()


def test_invalid_pacing_cannot_publish_a_prepared_segment(tmp_path, monkeypatch):
    root = tmp_path / "journal"
    root.mkdir(mode=0o700)
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_PACED_PREFAULT", "invalid")
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4, timeout_seconds=1)
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner.prepare(1, True)
        assert not (root / "segment-000000000001.mmap").exists()
    finally:
        owner.close()


def test_killed_maintenance_writer_is_not_a_closed_hash_chain(tmp_path, monkeypatch):
    root = source(tmp_path)
    trace_root = tmp_path / "traces"
    trace_root.mkdir(mode=0o700)
    monkeypatch.setenv("TRADECORE_DIAGNOSTIC_SEGMENT_TRACE_DIR", str(trace_root))
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    deadline = time.monotonic() + 5
    while not list(trace_root.glob("*.manifest.json")) and time.monotonic() < deadline:
        time.sleep(0.01)
    manifests = list(trace_root.glob("*.manifest.json"))
    assert len(manifests) == 1
    owner._process.terminate()
    owner._process.join(timeout=3)
    try:
        with pytest.raises(ValueError, match="unsealed"):
            verify_closed_request_trace(manifests[0])
    finally:
        owner.close()


@pytest.mark.parametrize("corrupt", (False, True))
def test_wrong_cut_or_torn_commit_never_get_a_seal_receipt(tmp_path, corrupt):
    root = source(tmp_path)
    target = root / "segment-000000000001.mmap"
    if corrupt:
        with target.open("r+b") as out:
            out.seek(8192 - 8)
            out.write(b"\0" * 8)
    before = target.read_bytes()
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner(1, (1, 1, 0, 4 if corrupt else 3, 2))
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner(1, (1, 1, 0, 4, 2))
        assert target.read_bytes() == before
    finally:
        owner.close()


def test_dead_child_cannot_publish_success(tmp_path):
    root = source(tmp_path)
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    owner._process.terminate()
    owner._process.join(timeout=3)
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner(1, (1, 1, 0, 4, 2))
    finally:
        owner.close()


@pytest.mark.skipif(os.name != "posix", reason="SIGSTOP requires POSIX")
def test_stopped_child_reply_has_absolute_deadline_and_bounded_close(tmp_path):
    root = source(tmp_path)
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4, timeout_seconds=0.2)
    os.kill(owner._process.pid, signal.SIGSTOP)
    started = time.monotonic()
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner(1, (1, 1, 0, 4, 2))
        assert time.monotonic() - started < 2
    finally:
        # A forced stop is not reported as a clean drain. A second close can
        # acknowledge the already-dead child and release the retained lease.
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner.close()
        owner.close()


def test_backend_failed_drain_retains_journal_writer_lease(tmp_path):
    class Backend:
        fail = True

        def __call__(self, index, cut):
            raise AssertionError("not rotating")

        def close(self):
            if self.fail:
                raise RuntimeError("undrained")

    root = tmp_path / "journal"
    backend = Backend()
    journal = SegmentedOmsFactJournal(
        root=root, journal_id="stream", segment_frames=4, segment_sealer=backend
    )
    with pytest.raises(RuntimeError):
        journal.close()
    with pytest.raises(OSError):
        SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    backend.fail = False
    journal.close()
    reopened = SegmentedOmsFactJournal(root=root, journal_id="stream", segment_frames=4)
    reopened.close()


def test_journal_rotations_use_real_process_sealer(tmp_path):
    root = tmp_path / "journal"
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    journal = SegmentedOmsFactJournal(
        root=root,
        journal_id="stream",
        segment_frames=4,
        segment_sealer=owner,
        segment_preparer=owner.prepare,
    )
    for n in range(1, 13):
        append(journal, n)
    journal.close()
    assert [record.lsn for record in journal.records()] == list(range(1, 13))
    assert journal.stats().sealed_segments == 2
    assert journal.stats().seal_work_ns >= journal.stats().maximum_seal_ns >= 0
    assert not owner._process.is_alive()
    assert journal.stats().process_prepared_segments >= 3
    assert journal.stats().prepare_work_ns >= journal.stats().maximum_prepare_ns > 0


def test_prepare_is_exclusive_and_never_overwrites_an_existing_source(tmp_path):
    root = source(tmp_path)
    original = (root / "segment-000000000001.mmap").read_bytes()
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner.prepare(1, True)
        assert (root / "segment-000000000001.mmap").read_bytes() == original
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            owner.prepare(100, True)
        assert not (root / "segment-000000000100.mmap").exists()
    finally:
        owner.close()


def test_parent_only_opens_prepared_mapping_without_bulk_prefault_or_source_creation(tmp_path):
    from xuanling_tradecore.oms.segmented_fact_journal import _Segment

    root = tmp_path / "journal"
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    original = _Segment.__init__
    creates = []

    def open_only(self, *args, **kwargs):
        creates.append(kwargs.get("create"))
        assert kwargs.get("create") is False
        return original(self, *args, **kwargs)

    try:
        with patch.object(_Segment, "__init__", new=open_only):
            journal = SegmentedOmsFactJournal(
                root=root,
                journal_id="stream",
                segment_frames=4,
                segment_sealer=owner,
                segment_preparer=owner.prepare,
            )
            try:
                append(journal, 1, 2, 3, 4)
                append(journal, 5)
            finally:
                journal.close()
        assert creates and not any(creates)
        assert [record.lsn for record in journal.records()] == [1, 2, 3, 4, 5]
    finally:
        owner.close()


def test_activated_prepared_file_cannot_enter_catalog_or_ready_queue(tmp_path):
    from xuanling_tradecore.oms.segmented_fact_journal import _Segment

    root = tmp_path / "journal"
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)

    def invalid_prepare(index, prefault):
        owner.prepare(index, prefault)
        segment = _Segment(
            root / f"segment-{index:012d}.mmap",
            index=index,
            frames=4,
            journal_id="stream",
            create=False,
        )
        try:
            segment.activate(1, 1, 0)
        finally:
            segment.close()

    try:
        with pytest.raises(MmapOmsFactJournalRecoveryRequired, match="already activated"):
            SegmentedOmsFactJournal(
                root=root,
                journal_id="stream",
                segment_frames=4,
                segment_sealer=owner,
                segment_preparer=invalid_prepare,
            )
        assert not (root / "segments.json").exists()
        assert (root / "segment-000000000001.mmap").is_file()
    finally:
        owner.close()


def test_sealer_death_marks_maintenance_failed_without_publishing_digest(tmp_path):
    root = tmp_path / "journal"
    owner = ProcessSegmentSealer(root=root, journal_id="stream", frames=4)
    journal = SegmentedOmsFactJournal(
        root=root, journal_id="stream", segment_frames=4, segment_sealer=owner
    )
    try:
        append(journal, 1, 2, 3, 4)
        owner._process.terminate()
        owner._process.join(timeout=3)
        append(journal, 5)
        deadline = time.monotonic() + 3
        while not journal.stats().maintenance_failed and time.monotonic() < deadline:
            time.sleep(0.001)
        assert journal.stats().maintenance_failed
        assert journal.stats().sealed_segments == 0
        assert (root / "segment-000000000001.mmap").is_file()
        import json

        assert "sha256" not in json.loads((root / "segments.json").read_text())["segments"][0]
    finally:
        journal.close()
