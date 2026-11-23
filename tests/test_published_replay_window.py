from time import monotonic, sleep

from xuanling_tradecore.oms import OrderBook
from xuanling_tradecore.oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.oms.published_replay_window import PublishedCommittedReplayBook
from xuanling_tradecore.oms.read_only_journal_source import PublishedJournalSourceV1
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal

from test_order_projection_store import SCOPE, admit


def wait_for(call):
    deadline = monotonic() + 3
    while monotonic() < deadline:
        result = call()
        if result:
            return result
        sleep(0.001)
    return call()


def test_published_replay_window_projects_live_commits_without_sql(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal",
        journal_id="stream",
        segment_frames=32,
        prefault=False,
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    source = PublishedJournalSourceV1(
        root=journal.path,
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        segment_frames=32,
        bound_for=journal.read_bound,
    )
    replay = PublishedCommittedReplayBook(
        source, event_capacity=8, checkpoint_capacity=8
    )
    try:
        replay.start()
        first = admit(book, "direct-replay-1")
        second = admit(book, "direct-replay-2")
        assert first.journal_commit is not None and second.journal_commit is not None
        def complete_bounds():
            result = replay.committed_replay_bounds(
                SCOPE,
                after=0,
                observed_head=journal.read_bound().committed_lsn,
                partitions=1,
            )
            return result if result and result[0][2] == 2 else None

        bounds = wait_for(complete_bounds)
        assert bounds is not None and bounds[0][2] == 2
        page = replay.committed_replay_page(
            SCOPE, after=0, high=bounds[0][1], limit=8
        )
        assert page is not None and not page.has_more
        assert [row.consumer_event().oms_order_id for row in page.entries] == [
            first.order.order.oms_order_id,
            second.order.order.oms_order_id,
        ]
        working = book.mark_working(oms_order_id=first.order.order.oms_order_id)
        assert working is not None
        successor = wait_for(
            lambda: replay.committed_replay_page(
                SCOPE,
                after=bounds[0][1],
                high=journal.read_bound().committed_lsn,
                limit=8,
            )
        )
        assert successor is not None
        assert [row.consumer_event().order_revision for row in successor.entries] == [1]
    finally:
        replay.close()
        journal.close()

def test_published_replay_window_starts_after_recovered_fence(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal",
        journal_id="stream",
        segment_frames=32,
        prefault=False,
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    first = admit(book, "before-read-plane")
    source = PublishedJournalSourceV1(
        root=journal.path,
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        segment_frames=32,
        bound_for=journal.read_bound,
    )
    replay = PublishedCommittedReplayBook(
        source, event_capacity=8, checkpoint_capacity=8
    )
    try:
        replay.start()
        assert first.journal_commit is not None
        floor = first.journal_commit.final_lsn
        second = admit(book, "after-read-plane")
        assert second.journal_commit is not None
        assert replay.committed_replay_page(
            SCOPE, after=0, high=second.journal_commit.final_lsn, limit=8
        ) is None
        page = wait_for(
            lambda: replay.committed_replay_page(
                SCOPE,
                after=floor,
                high=second.journal_commit.final_lsn,
                limit=8,
            )
        )
        assert page is not None
        assert [row.consumer_event().oms_order_id for row in page.entries] == [
            second.order.order.oms_order_id
        ]
    finally:
        replay.close()
        journal.close()


def test_evicted_predecessor_disables_optional_cache_without_fencing_reader(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=32, prefault=False,
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    source = PublishedJournalSourceV1(
        root=journal.path, journal_id=journal.journal_id,
        profile_hash=journal.profile_hash, segment_frames=32, bound_for=journal.read_bound,
    )
    replay = PublishedCommittedReplayBook(source, event_capacity=8, checkpoint_capacity=1)
    try:
        replay.start()
        first = admit(book, "evicted-predecessor")
        admit(book, "evict-first-admission")
        assert wait_for(lambda: replay.committed_replay_bounds(
            SCOPE, after=0, observed_head=journal.read_bound().committed_lsn, partitions=1,
        )) is not None
        book.reject(oms_order_id=first.order.order.oms_order_id)
        assert wait_for(lambda: not replay._covered)  # cache coverage is lost, not source authority
        assert not replay.failed
        assert replay.committed_replay_bounds(
            SCOPE, after=0, observed_head=journal.read_bound().committed_lsn, partitions=1,
        ) is None
        replay._failure = RuntimeError("fault injection")
        assert replay.failed  # real pump errors remain fail-closed even without coverage
        replay._failure = MmapOmsFactJournalRecoveryRequired("tail cursor is beyond committed Journal")
        assert replay.failure_kind == "MmapOmsFactJournalRecoveryRequired:CURSOR_BEYOND_COMMIT"
        replay._failure = MmapOmsFactJournalRecoveryRequired("owner read fence failed/stale")
        assert replay.failure_kind == "MmapOmsFactJournalRecoveryRequired:OWNER_FENCE_FAILED_OR_STALE"
        replay._failure = MmapOmsFactJournalRecoveryRequired("private path and arbitrary details")
        assert replay.failure_kind == "MmapOmsFactJournalRecoveryRequired:UNCLASSIFIED"
    finally:
        replay.close()
        journal.close()
