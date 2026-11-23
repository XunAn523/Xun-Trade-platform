from __future__ import annotations

from dataclasses import replace
from time import monotonic, sleep
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.base import ConsumerNotFoundError
from xuanling_tradecore.consumer.cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from xuanling_tradecore.consumer.oms_operations_v2 import OrderBookConsumerOperationsV2
from xuanling_tradecore.consumer.projection_reads_v2 import (
    ConsumerAuditEvidenceUnavailableV2,
    ConsumerProjectionReadErrorV2,
    projection_read_scope,
)
from xuanling_tradecore.consumer.replay_v2 import (
    ConsumerCursorReplayError,
    ConsumerCursorReplayServiceV2,
)
from xuanling_tradecore.consumer.request_trace_v2 import request_trace_scope
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1
from xuanling_tradecore.oms import InMemoryOmsFactJournal, OrderBook
from xuanling_tradecore.oms.published_replay_window import PublishedCommittedReplayBook
from xuanling_tradecore.read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth

from test_order_projection_store import admit, create, drain, retry
from test_tradecore_oms import _bindings


@pytest.fixture
def env(tmp_path):
    journal, book, store, tail = create(tmp_path)
    stream = [CursorStreamRefV2(journal.journal_id, 1)]
    health = [IncrementalProjectionHealth("RUNNING", 0, 0, 0, 100)]
    reader = ConsumerOrderProjectionReaderV2(
        root=tmp_path / "projection",
        journal=journal,
        stream=stream[0],
        current_stream=lambda: stream[0],
        health_for=lambda: health[0],
        clock_ns=lambda: 100,
    )
    try:
        yield journal, book, store, tail, reader, stream, health
    finally:
        tail.close()
        store.close()
        journal.close()


def test_only_reviewed_read_context_can_open_storage_and_context_does_not_leak(env):
    _, _, _, _, reader, _, _ = env
    account = _bindings()[0]
    for operation in (None, "CancelOrder", "SubscribeOrderEvents", "PollEvents"):
        with (
            projection_read_scope(operation or ""),
            patch(
                "xuanling_tradecore.read_model.consumer_order_reader_v2.OrderProjectionStore"
            ) as open_store,
        ):
            with pytest.raises(ConsumerProjectionReadErrorV2):
                reader.get_order(account_binding=account, oms_order_id="absent")
            open_store.assert_not_called()
    with projection_read_scope("GetOrder"):
        with pytest.raises(ConsumerNotFoundError):
            reader.get_order(account_binding=account, oms_order_id="absent")
    with pytest.raises(ConsumerProjectionReadErrorV2):
        reader.get_order(account_binding=account, oms_order_id="absent")


def test_lookup_never_turns_lag_into_absence_and_scopes_cold_rows(env):
    _, book, store, tail, reader, _, health = env
    account = _bindings()[0]
    order_id = admit(book).order.order.oms_order_id
    with projection_read_scope("GetOrder"):
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.get_order(account_binding=account, oms_order_id=order_id)
        drain(store, tail)
        row = reader.get_order(account_binding=account, oms_order_id=order_id)
        assert row.account_id == account.account_id
        for foreign in (
            replace(account, consumer_id="foreign"),
            replace(account, consumer_account_id="foreign"),
            replace(account, account_id="foreign"),
        ):
            with pytest.raises(ConsumerNotFoundError):
                reader.get_order(account_binding=foreign, oms_order_id=order_id)
        retry(lambda: book.mark_working(oms_order_id=order_id))
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.get_order(account_binding=account, oms_order_id=order_id)
        drain(store, tail)
        assert (
            reader.get_order(account_binding=account, oms_order_id=order_id).order_revision
            > row.order_revision
        )
        health[0] = replace(health[0], state="FAILED", error_code="PROJECTION_FAILED")
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.get_order(account_binding=account, oms_order_id="absent")


def test_hot_reads_remain_memory_only_cold_fallback_cannot_authorize_cancel(env):
    _, book, store, tail, reader, _, _ = env
    account = _bindings()[0]
    order_id = admit(book).order.order.oms_order_id
    drain(store, tail)
    hot = OrderBookConsumerOperationsV2(
        order_book=book, admission_context_for=lambda *_: None, cold_order_reader=reader
    )
    with patch.object(reader, "get_order", side_effect=AssertionError("cold access")):
        assert (
            hot.get_order(account_binding=account, oms_order_id=order_id).oms_order_id == order_id
        )
    # Simulate the public read after terminal eviction. Commands still use the
    # real hot book; the cold projection never grants a cancel side effect.
    cold = OrderBookConsumerOperationsV2(
        order_book=OrderBook(journal=InMemoryOmsFactJournal("other")),
        admission_context_for=lambda *_: None,
        cold_order_reader=reader,
    )
    with projection_read_scope("GetOrder"):
        assert (
            cold.get_order(account_binding=account, oms_order_id=order_id).oms_order_id == order_id
        )
    with projection_read_scope("CancelOrder"):
        with pytest.raises(ConsumerProjectionReadErrorV2):
            cold.get_order(account_binding=account, oms_order_id=order_id)


def replay_for(reader, stream, clock):
    ring = CursorKeyRingV2(current=CursorSigningKeyV2("test", b"t" * 32))
    service = ConsumerCursorReplayServiceV2(
        key_ring_for=lambda _: ring,
        registry_for=lambda _: CursorStreamRegistryV2(stream[0], (stream[0],)),
        event_reader=reader,
        partition_reader=reader,
        cursor_ttl_ns=1000,
        wall_clock_ns=lambda: clock[0],
    )
    return service, ring


def test_signed_partitions_cross_segments_preserve_unique_fixed_bounds(env):
    _, book, store, tail, reader, stream, _ = env
    account = _bindings()[0]
    for n in range(24):
        admit(book, str(n))
    drain(store, tail)
    service, _ = replay_for(reader, stream, [10])
    with projection_read_scope("CreateReplayPartitions"):
        plan = service.issue_partitions(binding=account, authorization_epoch=1, partition_count=5)
    admit(book, "later")  # later fact need not be projected to read old fixed ranges
    seen = []
    for part in plan.partitions:
        token = part.cursor_token
        positions = []
        with projection_read_scope("PollEvents"):
            while True:
                page = service.poll(binding=account, authorization_epoch=1, token=token, limit=2)
                assert page.high_watermark_position == part.bounds.high_watermark_position
                positions.extend(i.position for i in page.items)
                token = page.next_cursor_token
                if not page.has_more:
                    break
            assert not service.poll(
                binding=account, authorization_epoch=1, token=token, limit=2
            ).items
        assert len(positions) == part.bounds.event_count
        seen.extend(positions)
    assert len(seen) == len(set(seen)) == 24
    assert seen == sorted(seen)


def test_rolling_partitions_accept_only_exhausted_signed_cursor_and_do_not_replay_history(env):
    _, book, store, tail, reader, stream, _ = env
    account = _bindings()[0]
    admit(book, "first")
    drain(store, tail)
    clock = [10]
    service, _ = replay_for(reader, stream, clock)
    with projection_read_scope("CreateReplayPartitions"):
        original = service.issue_partitions(
            binding=account, authorization_epoch=1, partition_count=2
        )
        with pytest.raises(ConsumerCursorReplayError, match="exhausted"):
            service.issue_partitions(
                binding=account,
                authorization_epoch=1,
                partition_count=2,
                after_cursor=original.partitions[0].cursor_token,
            )
    with projection_read_scope("PollEvents"):
        page = service.poll(
            binding=account,
            authorization_epoch=1,
            token=original.partitions[0].cursor_token,
            limit=200,
        )
    token = page.next_cursor_token
    high = page.high_watermark_position
    with projection_read_scope("CreateReplayPartitions"):
        assert (
            service.issue_partitions(
                binding=account, authorization_epoch=1, partition_count=3, after_cursor=token
            ).partitions
            == ()
        )
    for n in range(5):
        admit(book, f"later-{n}")
    drain(store, tail)
    with projection_read_scope("CreateReplayPartitions"):
        following = service.issue_partitions(
            binding=account, authorization_epoch=1, partition_count=3, after_cursor=token
        )
        assert following.partitions[0].bounds.after_position == high
        assert sum(p.bounds.event_count for p in following.partitions) == 5
        for binding, epoch, value in (
            (account, 2, token),
            (replace(account, consumer_account_id="foreign"), 1, token),
            (account, 1, token + "x"),
        ):
            with pytest.raises(ConsumerCursorReplayError):
                service.issue_partitions(
                    binding=binding,
                    authorization_epoch=epoch,
                    partition_count=3,
                    after_cursor=value,
                )
        clock[0] = 1010
        with pytest.raises(ConsumerCursorReplayError) as error:
            service.issue_partitions(
                binding=account, authorization_epoch=1, partition_count=3, after_cursor=token
            )
        assert error.value.code == "CURSOR_EXPIRED"


def test_signed_planner_can_freeze_verified_prefix_while_live_source_keeps_advancing(env):
    journal, book, store, tail, reader, stream, health = env
    account = _bindings()[0]
    admit(book, "first")
    drain(store, tail)
    projected = store.cursor.next_lsn - 1
    admit(book, "not-yet-projected")
    assert journal.read_bound().committed_lsn > projected
    service, _ = replay_for(reader, stream, [10])
    with projection_read_scope("CreateReplayPartitions"):
        plan = service.issue_partitions(binding=account, authorization_epoch=1, partition_count=4)
    assert sum(p.bounds.event_count for p in plan.partitions) == 1
    assert plan.partitions[-1].bounds.high_watermark_position.stream_id.redis_ms == projected
    with projection_read_scope("PollEvents"):
        page = service.poll(
            binding=account,
            authorization_epoch=1,
            token=plan.partitions[-1].cursor_token,
            limit=100,
        )
    assert not page.has_more and len(page.items) == 1
    with projection_read_scope("CreateReplayPartitions"):
        empty = service.issue_partitions(
            binding=account,
            authorization_epoch=1,
            partition_count=4,
            after_cursor=page.next_cursor_token,
        )
    assert not empty.partitions  # Not an authoritative absence of newer source events.
    drain(store, tail)
    with projection_read_scope("CreateReplayPartitions"):
        suffix = service.issue_partitions(
            binding=account,
            authorization_epoch=1,
            partition_count=4,
            after_cursor=page.next_cursor_token,
        )
    assert sum(p.bounds.event_count for p in suffix.partitions) == 1
    assert suffix.partitions[0].bounds.after_position == page.high_watermark_position
    # Source advances during a healthy read: GetOrder still needs the live
    # minimum and cannot turn lag into absence or stale latest-order success.
    admit(book, "again-not-projected")
    with projection_read_scope("GetOrder"):
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.get_order(account_binding=account, oms_order_id="absent")
    health[0] = replace(health[0], state="FAILED", error_code="PROJECTION_FAILED")
    with projection_read_scope("CreateReplayPartitions"):
        with pytest.raises(ConsumerCursorReplayError):
            service.issue_partitions(binding=account, authorization_epoch=1, partition_count=4)
    health[0] = replace(health[0], state="PAUSED", error_code=None)
    with projection_read_scope("CreateReplayPartitions"):
        with pytest.raises(ConsumerCursorReplayError):
            service.issue_partitions(binding=account, authorization_epoch=1, partition_count=4)
    with projection_read_scope("PollEvents"):
        reader.validate_delivery()  # Existing signed/frozen prefixes remain readable.


def test_hot_signed_prefix_survives_eviction_while_projector_is_behind(env, tmp_path):
    journal, book, store, tail, _, stream, health = env
    cache = PublishedCommittedReplayBook(journal, event_capacity=2, checkpoint_capacity=8)
    reader = ConsumerOrderProjectionReaderV2(
        root=tmp_path / "projection", journal=journal, stream=stream[0],
        current_stream=lambda: stream[0], health_for=lambda: health[0],
        clock_ns=lambda: 100, committed_replay_book=cache,
    )
    account = _bindings()[0]
    service, _ = replay_for(reader, stream, [10])

    def wait_for_high(lsn):
        deadline = monotonic() + 5
        while monotonic() < deadline:
            if cache.committed_replay_page(
                (account.consumer_id, account.consumer_account_id, account.account_id),
                after=0, high=lsn, limit=2,
            ) is not None:
                return
            sleep(0.001)
        pytest.fail("committed replay cache did not reach the requested high watermark")

    try:
        cache.start()
        admit(book, "projected")
        drain(store, tail)
        projected = store.cursor.next_lsn - 1
        health[0] = replace(health[0], projected_lsn=projected, observed_committed_lsn=projected)
        admit(book, "not-projected")
        wait_for_high(journal.read_bound().committed_lsn)
        with projection_read_scope("CreateReplayPartitions"):
            plan = service.issue_partitions(binding=account, authorization_epoch=1, partition_count=1)
        assert len(plan.partitions) == 1
        assert plan.partitions[0].bounds.high_watermark_position.stream_id.redis_ms == projected
        admit(book, "evicts-first-event")
        deadline = monotonic() + 5
        while monotonic() < deadline:
            if cache.committed_replay_page(
                (account.consumer_id, account.consumer_account_id, account.account_id),
                after=0, high=projected, limit=2,
            ) is None:
                break
            sleep(0.001)
        else:
            pytest.fail("cache did not evict the signed prefix")
        with projection_read_scope("PollEvents"):
            page = service.poll(binding=account, authorization_epoch=1,
                                token=plan.partitions[0].cursor_token, limit=2)
        assert len(page.items) == 1 and not page.has_more
        assert page.high_watermark_position.stream_id.redis_ms == projected
    finally:
        cache.close()


def test_partition_cut_does_not_expand_if_projection_overtakes_captured_source_head(env):
    journal, book, store, tail, reader, stream, _ = env
    account = _bindings()[0]
    admit(book, "first")
    captured = journal.read_bound()
    admit(book, "later")
    drain(store, tail)
    with (
        projection_read_scope("CreateReplayPartitions"),
        patch.object(journal, "read_bound", return_value=captured),
    ):
        bounds = reader.replay_partition_bounds(
            binding=account, stream=stream[0], partition_count=4
        )
    assert sum(p.event_count for p in bounds) == 1
    assert bounds[-1].high_watermark_position.stream_id.redis_ms == captured.committed_lsn


def test_explicit_zero_watermark_and_large_limit_keep_bounded_replay_contract(env):
    _, _, _, _, reader, stream, _ = env
    account = _bindings()[0]
    zero = ConsumerEventPositionV2(RedisStreamIdV1(0, 0), 0)
    with (
        projection_read_scope("PollEvents"),
        patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.OrderProjectionStore.events_after",
            autospec=True,
            wraps=None,
        ) as read,
    ):
        from xuanling_tradecore.persistence.order_projection_store import ProjectionEventPage

        read.return_value = ProjectionEventPage((), 0, 0, False)
        page = reader.read_events(
            binding=account,
            stream=stream[0],
            after_position=None,
            high_watermark_position=zero,
            limit=10_000,
        )
        assert page.high_watermark_position == zero and not page.items
        assert read.call_args.kwargs["limit"] == 200
        assert read.call_args.kwargs["through_lsn"] == 0


def test_signed_replay_rejects_tamper_epoch_scope_expiry_and_bad_position(env):
    _, book, store, tail, reader, stream, _ = env
    account = _bindings()[0]
    admit(book)
    drain(store, tail)
    clock = [10]
    service, ring = replay_for(reader, stream, clock)
    token = service.issue_begin_cursor(binding=account, authorization_epoch=1)
    with projection_read_scope("PollEvents"):
        for binding, epoch, value in (
            (account, 2, token),
            (replace(account, consumer_id="other"), 1, token),
            (account, 1, token + "x"),
        ):
            with pytest.raises(ConsumerCursorReplayError):
                service.poll(binding=binding, authorization_epoch=epoch, token=value, limit=2)
        invalid = ring.issue(
            consumer_id=account.consumer_id,
            consumer_account_id=account.consumer_account_id,
            journal_id=stream[0].journal_id,
            stream_generation=1,
            authorization_epoch=1,
            after_position=ConsumerEventPositionV2(RedisStreamIdV1(1, 1), 0),
            issued_at_wall_ns=10,
            expires_at_wall_ns=1010,
        )
        with pytest.raises(ConsumerCursorReplayError):
            service.poll(binding=account, authorization_epoch=1, token=invalid, limit=2)
        clock[0] = 1010
        with pytest.raises(ConsumerCursorReplayError) as error:
            service.poll(binding=account, authorization_epoch=1, token=token, limit=2)
        assert error.value.code == "CURSOR_EXPIRED"


def test_delivery_guard_is_memory_only_and_refuses_changed_generation_or_stale_health(env):
    journal, _, _, _, reader, stream, health = env
    with (
        patch.object(journal, "read_bound", side_effect=AssertionError("writer lock")),
        patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.OrderProjectionStore",
            side_effect=AssertionError("SQL"),
        ),
    ):
        reader.validate_delivery()
        stream[0] = replace(stream[0], stream_generation=2)
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.validate_delivery()
        stream[0] = replace(stream[0], stream_generation=1)
        for state in ("FAILED", "STOPPED", "NEW"):
            health[0] = replace(health[0], state=state)
            with pytest.raises(ConsumerProjectionReadErrorV2):
                reader.validate_delivery()
        health[0] = replace(health[0], state="RUNNING", sampled_monotonic_ns=101)
        with pytest.raises(ConsumerProjectionReadErrorV2):
            reader.validate_delivery()
        health[0] = replace(health[0], sampled_monotonic_ns=0)
        with patch.object(reader, "_clock", return_value=2_000_000_001):
            with pytest.raises(ConsumerProjectionReadErrorV2):
                reader.validate_delivery()


def test_storage_exception_is_redacted_and_does_not_change_journal(env):
    journal, _, _, _, reader, _, _ = env
    before = journal.read_bound()
    with (
        projection_read_scope("GetOrder"),
        patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.OrderProjectionStore",
            side_effect=OSError("private path and password"),
        ),
    ):
        record = {}
        with request_trace_scope(record):
            with pytest.raises(ConsumerProjectionReadErrorV2) as error:
                reader.get_order(account_binding=_bindings()[0], oms_order_id="absent")
        assert "password" not in str(error.value)
        assert record["projection_error_kind"] == "STORAGE_ACCESS_UNAVAILABLE"
        assert "private" not in str(record) and "password" not in str(record)
    assert journal.read_bound() == before


def test_cold_audit_reads_original_committed_facts_not_just_latest_or_event_cache(env):
    journal, book, store, tail, reader, _, _ = env
    account = _bindings()[0]
    order_id = admit(book).order.order.oms_order_id
    retry(lambda: book.mark_working(oms_order_id=order_id))
    retry(lambda: book.reject(oms_order_id=order_id))
    drain(store, tail)
    hot = OrderBookConsumerOperationsV2(order_book=book, admission_context_for=lambda *_: None)
    expected = hot.get_order_audit_trail(account_binding=account, oms_order_id=order_id)
    with projection_read_scope("GetOrderAuditTrail"):
        record = {}
        with request_trace_scope(record):
            actual = reader.get_order_audit_trail(account_binding=account, oms_order_id=order_id)
        assert actual == expected and len(actual) == 3
        times = [
            record[key]
            for key in (
                "audit_index_started_ns",
                "audit_index_finished_ns",
                "audit_journal_verify_started_ns",
                "audit_journal_verify_finished_ns",
            )
        ]
        assert times == sorted(times)
        assert record["audit_verified_events"] == 3 and record["audit_verified_batches"] == 3
        # Raw facts are mandatory, even though the database is complete/healthy.
        with patch(
            "xuanling_tradecore.oms.journal_audit.SegmentedJournalTail.read_next",
            side_effect=OSError("private secret path"),
        ):
            with pytest.raises(ConsumerAuditEvidenceUnavailableV2) as error:
                reader.get_order_audit_trail(account_binding=account, oms_order_id=order_id)
            assert "secret" not in str(error.value)
        with pytest.raises(ConsumerNotFoundError):
            reader.get_order_audit_trail(
                account_binding=replace(account, consumer_id="foreign"), oms_order_id=order_id
            )
    with pytest.raises(ConsumerProjectionReadErrorV2):
        reader.get_order_audit_trail(account_binding=account, oms_order_id=order_id)


def test_audit_lag_is_not_missing_evidence_or_partial_success(env):
    _, book, _, _, reader, _, _ = env
    order_id = admit(book).order.order.oms_order_id
    with projection_read_scope("GetOrderAuditTrail"):
        with pytest.raises(ConsumerAuditEvidenceUnavailableV2):
            reader.get_order_audit_trail(account_binding=_bindings()[0], oms_order_id=order_id)
