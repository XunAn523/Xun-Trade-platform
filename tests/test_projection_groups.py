from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from xuanling_tradecore.oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionError
from xuanling_tradecore.read_model.projection_groups import VerifiedProjectionGroups

from test_order_projection_store import admit, create


def test_one_frozen_prefix_per_bounded_group_and_low_pressure_does_not_wait(tmp_path):
    journal, book, store, tail = create(tmp_path, segment_frames=256)
    try:
        for n in range(300):
            admit(book, f"frozen-{n}")
        groups = VerifiedProjectionGroups(tail, 256, freeze_source_bound=True)
        with patch.object(journal, "read_bound", wraps=journal.read_bound) as bounds:
            first, second = groups.read_next(), groups.read_next()
            assert (len(first), len(second)) == (256, 44)
            assert not groups.read_next()
            assert bounds.call_count == 3
        assert tail._window_bound is None
        assert store.apply_many(first) == (True,) * 256
        assert store.apply_many(second) == (True,) * 44
        admit(book, "low-frozen")
        group = groups.read_next()
        assert len(group) == 1 and store.apply_many(group) == (True,)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_commits_after_frozen_prefix_wait_only_until_next_group_and_bad_fence_rejected(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book, "before-fence")
        with tail.source_read_window():
            admit(book, "after-fence")
            first = tail.read_next()
            assert first is not None and tail.read_next() is None
        assert tail.read_next() is not None
        bound = journal.read_bound()
        with patch.object(journal, "read_bound", return_value=replace(bound, journal_id="foreign")):
            with pytest.raises(MmapOmsFactJournalRecoveryRequired, match="namespace"):
                with tail.source_read_window():
                    pytest.fail("foreign source window entered")
        assert tail._window_bound is None and tail._failed
    finally:
        tail.close()
        store.close()
        journal.close()


def test_frozen_window_exception_does_not_leak_or_publish_partial_group(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book, "fence-fault")
        with pytest.raises(OSError):
            with tail.source_read_window():
                assert tail.read_next() is not None
                raise OSError("fixture CRC failure")
        assert tail._window_bound is None and tail._failed
        assert store.recovery_counts() == (0, 0, 0)
        with pytest.raises(MmapOmsFactJournalRecoveryRequired):
            tail.read_next()
    finally:
        tail.close()
        store.close()
        journal.close()


def test_frozen_group_still_rejects_corrupted_physical_frame(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book, "physical-crc")
        original = tail._frame

        def corrupt(segment, frame):
            data = bytearray(original(segment, frame))
            data[80] ^= 1
            return bytes(data)

        with patch.object(tail, "_frame", side_effect=corrupt):
            with pytest.raises(MmapOmsFactJournalRecoveryRequired):
                VerifiedProjectionGroups(tail, 256, freeze_source_bound=True).read_next()
        assert tail._window_bound is None and tail._failed
        assert store.recovery_counts() == (0, 0, 0)
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("limit", (0, 257, True, 1.5))
def test_invalid_group_budget_never_reads_source(limit):
    tail = Mock()
    with pytest.raises(OrderProjectionError):
        VerifiedProjectionGroups(tail, limit)
    tail.read_next.assert_not_called()


def test_actual_512_fact_boundary_no_loss_and_no_wait_for_future_batches(tmp_path):
    journal, book, store, tail = create(tmp_path, segment_frames=256)
    try:
        for n in range(300):
            admit(book, f"group-{n}")
        groups = VerifiedProjectionGroups(tail, 256)
        first, second = groups.read_next(), groups.read_next()
        assert (len(first), len(second)) == (256, 44)
        assert sum(len(batch.records) for batch in first) == 512
        assert first[-1].after.next_lsn == second[0].before.next_lsn == 513
        assert store.apply_many(first) == (True,) * 256
        assert store.apply_many(second) == (True,) * 44
        assert not groups.read_next()
        admit(book, "single-low-pressure")
        # Returns the one available batch now, rather than polling to fill 256.
        last = groups.read_next()
        assert len(last) == 1 and store.apply_many(last) == (True,)
        assert store.recovery_counts() == (602, 301, 301)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_lookahead_is_bounded_preserves_order_and_is_not_published_early():
    # Grouping-only test: production tail/SQL still validate the real CRC,
    # ordinal, fingerprint and cursor; these synthetic sizes do not bypass SQL.
    batches = [SimpleNamespace(records=(None,) * n) for n in (128, 128, 128, 127, 128, 2)]
    tail = Mock()
    tail.read_next.side_effect = [*batches, None, None]
    stopped = [False]
    groups = VerifiedProjectionGroups(tail, 256, lambda: stopped[0])
    assert groups.read_next() == tuple(batches[:4])  # 511, next 128 cannot fit
    assert tail.read_next.call_count == 5
    stopped[0] = True
    assert groups.read_next() == ()
    assert tail.read_next.call_count == 5
    stopped[0] = False
    assert groups.read_next() == tuple(batches[4:])
    assert groups.read_next() == ()
    assert groups._pending is None


def test_bad_tail_is_not_converted_into_partial_success():
    tail = Mock()
    tail.read_next.side_effect = [SimpleNamespace(records=(None,) * 2), ValueError("bad CRC")]
    groups = VerifiedProjectionGroups(tail, 256)
    with pytest.raises(ValueError, match="CRC"):
        groups.read_next()


@pytest.mark.parametrize("size", (0, 513))
def test_impossible_source_batch_cannot_expand_fact_limit(size):
    tail = Mock()
    tail.read_next.return_value = SimpleNamespace(records=(None,) * size)
    with pytest.raises(OrderProjectionError, match="budget"):
        VerifiedProjectionGroups(tail, 256).read_next()
