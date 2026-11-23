from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.projection_reads_v2 import (
    ConsumerProjectionReadErrorV2,
    projection_read_scope,
)
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1
from xuanling_tradecore.oms.committed_replay_window import CommittedReplayWindow
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint
from xuanling_tradecore.oms.retained_order_book import RetainedOrderBook
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex
from xuanling_tradecore.read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth

from test_consumer_order_projection_reader_v2 import replay_for
from test_order_projection_store import admit, create, drain, retry
from test_retained_order_book import retire
from test_tradecore_oms import _bindings


def pos(n):
    return ConsumerEventPositionV2(RedisStreamIdV1(n, 0), 0)


@pytest.fixture
def env(tmp_path):
    journal, _, store, tail = create(tmp_path)
    cold = ColdIntentIndex(root=tmp_path / "cold", journal_id="stream", initialize=True)
    book = RetainedOrderBook(journal=journal, cold_index=cold, replay_cache_events=8)
    book.enable_committed_consumer_replay()
    stream = [CursorStreamRefV2(journal.journal_id, 1)]
    health = [IncrementalProjectionHealth("RUNNING", 0, 0, 0, 100)]
    reader = ConsumerOrderProjectionReaderV2(
        root=tmp_path / "projection",
        journal=journal,
        stream=stream[0],
        current_stream=lambda: stream[0],
        health_for=lambda: health[0],
        clock_ns=lambda: 100,
        committed_replay_book=book,
    )
    try:
        yield journal, book, store, tail, reader, stream, health, cold
    finally:
        tail.close()
        store.close()
        cold.close()
        journal.close()


def test_signed_hot_pages_equal_cold_projection_even_after_terminal_retirement(env):
    journal, book, store, tail, reader, stream, health, cold = env
    account = _bindings()[0]
    for n in range(3):
        order = retry(lambda: admit(book, str(n)))
        oid = order.order.order.oms_order_id
        retry(lambda: book.reject(oms_order_id=oid))
        retire(book, cold, oid)
    assert book.order_count(account.account_id) == 0
    drain(store, tail)
    health[0] = replace(health[0], projected_lsn=store.cursor.next_lsn - 1)
    replay, _ = replay_for(reader, stream, [10])
    captured = []
    with patch(
        "xuanling_tradecore.read_model.consumer_order_reader_v2.OrderProjectionStore"
    ) as sql:
        with projection_read_scope("CreateReplayPartitions"):
            plan = replay.issue_partitions(
                binding=account, authorization_epoch=1, partition_count=4
            )
        assert sum(p.bounds.event_count for p in plan.partitions) == 6
        for part in plan.partitions:
            token = part.cursor_token
            with projection_read_scope("PollEvents"):
                while True:
                    page = replay.poll(binding=account, authorization_epoch=1, token=token, limit=1)
                    assert page.high_watermark_position == part.bounds.high_watermark_position
                    captured.extend(item.event for item in page.items)
                    token = page.next_cursor_token
                    if not page.has_more:
                        break
        sql.assert_not_called()
    assert len(captured) == len({e.journal_lsn for e in captured}) == 6
    drain(store, tail)
    expected = store.events_after(
        (account.consumer_id, account.consumer_account_id, account.account_id),
        0,
        through_lsn=journal.read_bound().committed_lsn,
    )
    assert tuple(captured) == expected.events


def test_evicted_or_unpublished_range_must_fall_back_not_claim_absence(env):
    journal, book, store, tail, reader, stream, _, _ = env
    account = _bindings()[0]
    for n in range(10):
        retry(lambda: admit(book, str(n)))
    with projection_read_scope("CreateReplayPartitions"):
        assert (
            reader.replay_partition_bounds(binding=account, stream=stream[0], partition_count=2)
            == ()
        )
    # The lagging SQL can only prove its empty prefix, not the evicted live range.
    high = pos(journal.read_bound().committed_lsn)
    with projection_read_scope("PollEvents"):
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.read_events(
                binding=account,
                stream=stream[0],
                after_position=None,
                high_watermark_position=high,
                limit=10,
            )
    drain(store, tail)
    with projection_read_scope("PollEvents"):
        page = reader.read_events(
            binding=account,
            stream=stream[0],
            after_position=None,
            high_watermark_position=high,
            limit=10,
        )
        assert len(page.items) == 10
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.read_events(
                binding=account,
                stream=stream[0],
                after_position=high,
                high_watermark_position=pos(high.stream_id.redis_ms + 1),
                limit=10,
            )
    with pytest.raises(ValueError, match="pre-live"):
        book.enable_committed_consumer_replay()


def test_scope_health_context_and_generation_guards_still_apply(env):
    journal, book, _, _, reader, stream, health, _ = env
    account = _bindings()[0]
    retry(lambda: admit(book))
    high = pos(journal.read_bound().committed_lsn)
    kwargs = dict(stream=stream[0], after_position=None, high_watermark_position=high, limit=100)
    with pytest.raises(ConsumerProjectionReadErrorV2):
        reader.read_events(binding=account, **kwargs)
    with projection_read_scope("PollEvents"):
        for foreign in (
            replace(account, consumer_id="foreign"),
            replace(account, consumer_account_id="foreign"),
        ):
            assert not reader.read_events(binding=foreign, **kwargs).items
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.read_events(binding=replace(account, active=False), **kwargs)
        health[0] = replace(health[0], state="PAUSED")
        assert len(reader.read_events(binding=account, **kwargs).items) == 1
    with projection_read_scope("CreateReplayPartitions"):
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.replay_partition_bounds(binding=account, stream=stream[0], partition_count=2)
    with projection_read_scope("PollEvents"):
        for state, timestamp in (("FAILED", 100), ("RUNNING", -3_000_000_000)):
            health[0] = replace(health[0], state=state, sampled_monotonic_ns=timestamp)
            with pytest.raises(ConsumerProjectionReadErrorV2):
                reader.read_events(binding=account, **kwargs)
        health[0] = replace(health[0], state="RUNNING", sampled_monotonic_ns=100)
        stream[0] = replace(stream[0], stream_generation=2)
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.read_events(binding=account, **kwargs)


def test_recovery_history_is_not_fabricated_from_a_new_empty_window(env):
    journal, book, store, tail, _, _, _, cold = env
    original = retry(lambda: admit(book))
    drain(store, tail)
    encoded = store.recovery_order_page("", watermark=journal.read_bound().committed_lsn)[0][1]
    state = decode_checkpoint(encoded)
    recovered = RetainedOrderBook(journal=journal, cold_index=cold, replay_cache_events=8)
    recovered.restore_cold_covered_checkpoint(
        encoded,
        expected_journal_id="stream",
        event_counts={"broker-account-1": 1},
        recent_events=state.events,
    )
    recovered.enable_committed_consumer_replay()
    retry(lambda: recovered.mark_working(oms_order_id=original.order.order.oms_order_id))
    scope = ("consumer-1", "consumer-account-1", "broker-account-1")
    high = journal.read_bound().committed_lsn
    assert recovered.committed_replay_page(scope, after=0, high=high, limit=100) is None
    page = recovered.committed_replay_page(scope, after=state.watermark, high=high, limit=100)
    assert page is not None and len(page.entries) == 1


def test_cache_failure_cannot_reclassify_committed_order_or_publish_partial_range(env):
    journal, book, store, tail, reader, stream, _, _ = env
    with patch.object(CommittedReplayWindow, "append", side_effect=RuntimeError("private")):
        accepted = retry(lambda: admit(book))
    assert accepted.event is not None and journal.read_bound().committed_lsn > 0
    retry(lambda: admit(book, "after-failure"))
    scope = ("consumer-1", "consumer-account-1", "broker-account-1")
    high = journal.read_bound().committed_lsn
    assert book.committed_replay_page(scope, after=0, high=high, limit=100) is None
    drain(store, tail)
    with projection_read_scope("PollEvents"):
        page = reader.read_events(
            binding=_bindings()[0],
            stream=stream[0],
            after_position=None,
            high_watermark_position=pos(high),
            limit=100,
        )
    assert len(page.items) == 2


def test_wraparound_sparse_positions_and_byte_budget(env):
    journal, book, _, _, reader, stream, _, _ = env
    for n in range(20):
        retry(lambda: admit(book, str(n)))
    scope = ("consumer-1", "consumer-account-1", "broker-account-1")
    high = journal.read_bound().committed_lsn
    bounds = book.committed_replay_bounds(scope, after=high - 8, observed_head=high, partitions=3)
    assert bounds is not None and sum(b[2] for b in bounds) == 4
    with projection_read_scope("PollEvents"):
        with patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.msgspec.json.encode",
            return_value=b"x" * 65536,
        ):
            page = reader.read_events(
                binding=_bindings()[0],
                stream=stream[0],
                after_position=pos(high - 12),
                high_watermark_position=pos(high),
                limit=100,
            )
            assert len(page.items) == 4 and page.has_more
    window = CommittedReplayWindow(capacity=1, floor=100)
    assert window.bounds(scope, after=100, observed_head=200, partitions=2) == ()
    assert window.page(scope, after=100, high=101, limit=1) is None
    for capacity in (0, True, 1_000_001):
        with pytest.raises(ValueError):
            CommittedReplayWindow(capacity=capacity, floor=0)


def test_default_book_does_not_allocate_a_consumer_window(env):
    journal, _, _, _, _, _, _, cold = env
    normal = RetainedOrderBook(journal=journal, cold_index=cold)
    with patch(
        "xuanling_tradecore.oms.retained_order_book.CommittedReplayWindow",
        side_effect=AssertionError("not opted in"),
    ):
        assert admit(normal).event is not None


def test_prefix_never_invents_unpublished_positions_and_window_rejects_mixed_journal(env):
    journal, book, _, _, reader, stream, _, _ = env
    account = _bindings()[0]
    first = admit(book)
    assert first.event is not None
    head = journal.read_bound().committed_lsn
    scope = (account.consumer_id, account.consumer_account_id, account.account_id)
    page = book.committed_replay_page(scope, after=0, high=head, limit=10)
    assert page is not None
    row = page.entries[0]
    window = CommittedReplayWindow(capacity=2, floor=0)
    window.append(row)
    assert window.bounds(scope, after=0, observed_head=head + 100, partitions=2) == ((0, head, 1),)
    with pytest.raises(ValueError):
        window.append(
            replace(row, event=replace(row.event, journal_id="foreign", journal_lsn=head + 1))
        )
    with projection_read_scope("PollEvents"):
        with patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.msgspec.json.encode",
            return_value=b"x" * 65537,
        ):
            with pytest.raises(ConsumerProjectionReadErrorV2):
                reader.read_events(
                    binding=account,
                    stream=stream[0],
                    after_position=None,
                    high_watermark_position=pos(head),
                    limit=10,
                )


def test_signed_rolling_suffix_uses_new_commits_without_replaying_old_prefix(env):
    _, book, store, tail, reader, stream, health, _ = env
    account = _bindings()[0]
    admit(book)
    drain(store, tail)
    health[0] = replace(health[0], projected_lsn=store.cursor.next_lsn - 1)
    replay, _ = replay_for(reader, stream, [10])
    with projection_read_scope("CreateReplayPartitions"):
        plan = replay.issue_partitions(binding=account, authorization_epoch=1, partition_count=2)
    with projection_read_scope("PollEvents"):
        page = replay.poll(
            binding=account, authorization_epoch=1, token=plan.partitions[0].cursor_token, limit=100
        )
    retry(lambda: admit(book, "new"))
    drain(store, tail)
    health[0] = replace(health[0], projected_lsn=store.cursor.next_lsn - 1)
    with projection_read_scope("CreateReplayPartitions"):
        suffix = replay.issue_partitions(
            binding=account,
            authorization_epoch=1,
            partition_count=2,
            after_cursor=page.next_cursor_token,
        )
    assert len(suffix.partitions) == 1
    assert suffix.partitions[0].bounds.event_count == 1
    assert suffix.partitions[0].bounds.after_position == page.high_watermark_position
