from __future__ import annotations

import json
from dataclasses import replace

import pytest
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail
from xuanling_tradecore.oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.oms.order_book import OmsFactKind, OmsJournalFact
from xuanling_tradecore.oms.read_only_journal_source import PublishedJournalSourceV1
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal


def fact(name: str) -> OmsJournalFact:
    return OmsJournalFact(
        name, OmsFactKind.ORDER_ATTRIBUTED, "account", "order", "execution", {}
    )


def test_published_source_reads_live_writer_without_second_lease(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path,
        journal_id="journal",
        segment_frames=8,
        spare_segments=2,
        disk_budget_bytes=4 * (4096 + 8 * 4096),
        prefault=False,
    )
    try:
        journal.append_batch((fact("a"), fact("b")))
        source = PublishedJournalSourceV1(
            root=tmp_path,
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            segment_frames=8,
            bound_for=journal.read_bound,
        )
        tail = SegmentedJournalTail(source, maximum_batch_facts=8)
        try:
            batch = tail.read_next()
            assert batch is not None
            assert tuple(row.fact.fact_id for row in batch.records) == ("a", "b")
            assert batch.after.next_lsn == 3
        finally:
            tail.close()
        # The writer remains live and authoritative; the view acquired no lease.
        assert journal.append_batch((fact("c"),)).final_lsn == 3
    finally:
        journal.close()


def test_published_source_rejects_foreign_fence_and_manifest(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path,
        journal_id="journal",
        segment_frames=8,
        spare_segments=2,
        disk_budget_bytes=4 * (4096 + 8 * 4096),
        prefault=False,
    )
    try:
        source = PublishedJournalSourceV1(
            root=tmp_path,
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            segment_frames=8,
            bound_for=lambda: replace(journal.read_bound(), journal_id="foreign"),
        )
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            source.read_bound()

        manifest = tmp_path / "segments.json"
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["journal_id"] = "foreign"
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            source.segment_read_path(1)
    finally:
        journal.close()
