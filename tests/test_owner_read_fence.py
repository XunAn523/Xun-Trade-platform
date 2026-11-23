from __future__ import annotations

import multiprocessing as mp
from time import monotonic_ns

import pytest
from xuanling_tradecore.consumer.owner_read_fence import (
    PublishedOwnerReadFenceV1,
    new_owner_read_fence,
    publish_owner_read_fence,
)
from xuanling_tradecore.oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedJournalReadBound
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth

PROFILE = "1" * 64


def publication(shared, *, state: str = "RUNNING", observed: int = 20) -> None:
    publish_owner_read_fence(
        shared,
        health=IncrementalProjectionHealth(
            state, min(20, observed), observed, 4, monotonic_ns()
        ),
        bound=SegmentedJournalReadBound("journal", PROFILE, 64, 2, 8, observed, 5),
    )


def test_owner_read_fence_publishes_one_consistent_cut():
    shared = new_owner_read_fence(mp.get_context("spawn"))
    publication(shared)
    fence = PublishedOwnerReadFenceV1(
        shared, journal_id="journal", profile_hash=PROFILE, segment_frames=64
    )
    snapshot = fence.snapshot()
    assert snapshot.health.projected_lsn == 20
    assert snapshot.bound.committed_lsn == 20
    assert snapshot.bound.active_segment == 2


def test_owner_read_fence_retries_busy_publisher_without_accepting_torn_cut():
    shared = new_owner_read_fence(mp.get_context("spawn"))
    publication(shared)

    class BrieflyBusy(list):
        reads = 0

        def __getitem__(self, index):
            if index == 0 and self.reads < 16:
                self.reads += 1
                return 1  # publication is in progress
            return super().__getitem__(index)

    transient = BrieflyBusy(shared)
    fence = PublishedOwnerReadFenceV1(
        transient, journal_id="journal", profile_hash=PROFILE, segment_frames=64
    )
    assert fence.snapshot().bound.committed_lsn == 20
    assert transient.reads == 16


def test_owner_read_fence_allows_monotonic_projection_lag_behind_journal():
    shared = new_owner_read_fence(mp.get_context("spawn"))
    publish_owner_read_fence(
        shared,
        health=IncrementalProjectionHealth("RUNNING", 16, 16, 3, monotonic_ns()),
        bound=SegmentedJournalReadBound("journal", PROFILE, 64, 2, 8, 20, 5),
    )
    snapshot = PublishedOwnerReadFenceV1(
        shared, journal_id="journal", profile_hash=PROFILE, segment_frames=64
    ).snapshot()
    assert snapshot.health.projected_lsn == snapshot.health.observed_committed_lsn == 16
    assert snapshot.bound.committed_lsn == 20


@pytest.mark.parametrize("mode", ("empty", "torn", "failed", "mismatch", "stale"))
def test_owner_read_fence_fails_closed(mode: str):
    shared = new_owner_read_fence(mp.get_context("spawn"))
    if mode == "empty":
        pass
    elif mode == "torn":
        shared[0] = 1
    elif mode == "failed":
        publication(shared, state="FAILED")
    else:
        publication(shared)
        if mode == "mismatch":
            shared[3] = 19
        elif mode == "stale":
            shared[5] = 1
    fence = PublishedOwnerReadFenceV1(
        shared,
        journal_id="journal",
        profile_hash=PROFILE,
        segment_frames=64,
        maximum_age_ns=1_000_000,
    )
    with pytest.raises(MmapOmsFactJournalRecoveryRequired):
        fence.snapshot()
