import json
import sqlite3
import threading
import time
from unittest.mock import Mock

import pytest
from perfbench.consumer_event_checkpoint import (
    ConcurrentRollingConsumerEventProbe,
    ConsumerEventCheckpoint,
    RollingConsumerEventProbe,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1


def position(n):
    return ConsumerEventPositionV2(RedisStreamIdV1(n, 0), 0).to_external_id()


def plan(after=None, end=8, count=4):
    return {
        "journal_id": "test-journal", "stream_generation": 1, "event_count": count,
        "high_watermark_position": position(end),
        "partitions": [{"partition_id": 0, "after_position": after,
                        "high_watermark_position": position(end), "event_count": count,
                        "cursor": f"private-start-{end}"}],
    }


def page(numbers=(2, 4), *, end=8, more=True, token="private-next-4"):
    return {
        "events": [{"event_id": f"event-{n}", "position": position(n),
                    "payload": {"private-order": "not persisted"}} for n in numbers],
        "has_more": more, "next_cursor": token, "high_watermark_position": position(end),
    }


def open_ledger(tmp_path, initialize=True, scope="a" * 64):
    return ConsumerEventCheckpoint(root=tmp_path / "checkpoint", scope_sha256=scope, initialize=initialize)


def test_partial_pages_and_exhausted_cursor_survive_reopen_and_suffix(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    first = ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
    assert first.unique_events == 2 and first.checkpointed_ns > 0
    stages = ledger.last_page_stages
    assert stages["validation_started_ns"] <= stages["sql_started_ns"] <= stages["full_before_ns"] <= stages["full_after_ns"]
    assert stages["full_after_ns"] == first.checkpointed_ns and stages["full_committed"]
    assert ledger.frontier() == (None, None)
    ledger.close()
    ledger = open_ledger(tmp_path, False)
    try:
        assert ledger.pending() == ((0, "private-next-4"),)
        ledger.commit_page(partition_id=0, origin_cursor="private-next-4",
                           page=page((6, 8), more=False, token="private-exhausted-8"))
        assert ledger.frontier() == ("private-exhausted-8", position(8))
        ledger.begin_plan(plan(position(8), end=12, count=2))
        ledger.commit_page(partition_id=0, origin_cursor="private-start-12",
                           page=page((10, 12), end=12, more=False, token="private-exhausted-12"))
        assert ledger.stats()["unique_checkpointed_events"] == 6
        assert ledger.frontier() == ("private-exhausted-12", position(12))
        assert "private-" not in json.dumps(ledger.stats())
    finally:
        ledger.close()
    assert b"not persisted" not in (tmp_path / "checkpoint/consumer-progress.sqlite").read_bytes()


def test_exact_last_page_replay_is_not_counted_twice(tmp_path):
    ledger = open_ledger(tmp_path)
    try:
        ledger.begin_plan(plan())
        ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
        duplicate = ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
        assert duplicate.unique_events == 0 and duplicate.replayed_events == 2
        assert ledger.last_page_stages is None  # no FULL occurred for a duplicate
        assert ledger.stats()["unique_checkpointed_events"] == 2
        changed = page()
        changed["events"][0]["payload"] = {"changed": True}
        with pytest.raises(ValueError, match="stale"):
            ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=changed)
    finally:
        ledger.close()


@pytest.mark.parametrize("bad", [
    page((4, 2)), page((2, 2)), page((2, 10)), page((), more=False),
    page(more=False), page((2, 4, 6, 8), more=True),
    page(token="private-start-8"), page(end=9),
])
def test_bad_page_never_advances_progress(tmp_path, bad):
    ledger = open_ledger(tmp_path)
    try:
        ledger.begin_plan(plan())
        with pytest.raises(ValueError):
            ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=bad)
        assert ledger.pending() == ((0, "private-start-8"),)
        assert ledger.stats()["unique_checkpointed_events"] == 0
    finally:
        ledger.close()


def test_generation_scope_and_unfinished_interval_cannot_be_replaced(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    try:
        with pytest.raises(ValueError, match="not been durably"):
            ledger.begin_plan(plan())
        ledger.commit_page(partition_id=0, origin_cursor="private-start-8",
                           page=page((2, 4, 6, 8), more=False, token="private-exhausted-8"))
        wrong = plan(position(8), end=12, count=2)
        wrong["journal_id"] = "another-service-namespace"
        with pytest.raises(ValueError, match="generation"):
            ledger.begin_plan(wrong)
    finally:
        ledger.close()
    with pytest.raises(ValueError, match="scope"):
        open_ledger(tmp_path, False, scope="b" * 64)


def test_empty_suffix_preserves_previous_frontier(tmp_path):
    ledger = open_ledger(tmp_path)
    try:
        ledger.begin_plan(plan())
        ledger.commit_page(partition_id=0, origin_cursor="private-start-8",
                           page=page((2, 4, 6, 8), more=False, token="private-exhausted-8"))
        empty = plan()
        empty.update(partitions=[], event_count=0, high_watermark_position=None)
        assert ledger.begin_plan(empty) is False
        assert ledger.frontier() == ("private-exhausted-8", position(8))
    finally:
        ledger.close()


def test_duplicate_event_in_later_page_rolls_back_whole_page_and_requires_reopen(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
    bad = page((6, 8), more=False, token="private-exhausted-8")
    bad["events"][1]["event_id"] = "event-2"
    with pytest.raises(ValueError, match="outcome"):
        ledger.commit_page(partition_id=0, origin_cursor="private-next-4", page=bad)
    with pytest.raises(ValueError, match="failed"):
        ledger.stats()
    ledger.close()
    reopened = open_ledger(tmp_path, False)
    try:
        assert reopened.stats()["unique_checkpointed_events"] == 2
        assert reopened.pending() == ((0, "private-next-4"),)
    finally:
        reopened.close()


def test_driver_only_requests_signed_suffix_after_durable_exhaustion(tmp_path):
    client = Mock()
    ledger = open_ledger(tmp_path)
    body = page((2, 4, 6, 8), more=False, token="private-exhausted-8")
    responses = [plan(), body, dict(partitions=[], event_count=0, high_watermark_position=None,
                                   journal_id="test-journal", stream_generation=1)]

    def reply(request):
        return {"message_type": "response", "schema_version": 2, "status": "COMPLETED",
                "request_id": request["request_id"], "payload": responses.pop(0)}

    client.call.side_effect = reply
    try:
        probe = RollingConsumerEventProbe(client=client, checkpoint=ledger, consumer_account_id="test")
        assert probe.step() is None
        assert probe.step().unique_events == 4
        assert probe.step() is None
        requests = [call.args[0] for call in client.call.call_args_list]
        assert requests[1]["payload"] == {"cursor": "private-start-8", "limit": 100}
        assert requests[2]["payload"]["after_cursor"] == "private-exhausted-8"
        assert "start_lsn" not in json.dumps(requests)
    finally:
        ledger.close()


def test_sql_settings_and_scope_are_persisted_without_overwrite(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.close()
    path = tmp_path / "checkpoint/consumer-progress.sqlite"
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert db.execute("SELECT scope FROM meta").fetchone() == ("a" * 64,)
    with pytest.raises(FileExistsError):
        open_ledger(tmp_path)


def test_wal_full_profile_is_bounded_and_cleanly_checkpointed_on_close(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
    stats = ledger.stats()
    assert stats["checkpoint_profile"] == "SQLITE_WAL_FULL_BOUNDED_TRUNCATE_V2"
    assert 0 < stats["wal_bytes"] <= 64 * 1024**2
    ledger.close()
    assert not (tmp_path / "checkpoint/consumer-progress.sqlite-wal").exists()
    assert not (tmp_path / "checkpoint/consumer-progress.sqlite-shm").exists()
    reopened = open_ledger(tmp_path, False)
    try:
        assert reopened.stats()["unique_checkpointed_events"] == 2
    finally:
        reopened.close()


def test_periodic_wal_reclaim_truncates_physical_file_and_keeps_acknowledged_page(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    ledger._wal_limit_bytes = 1
    try:
        committed = ledger.commit_page(
            partition_id=0, origin_cursor="private-start-8", page=page()
        )
        assert committed.unique_events == 2
        assert ledger.last_page_stages["wal_truncated"] is True
        assert ledger.last_page_stages["wal_bytes"] == 0
        stats = ledger.stats()
        assert stats["wal_truncate_checkpoints"] == 1
        assert stats["unique_checkpointed_events"] == 2
    finally:
        ledger.close()
    reopened = open_ledger(tmp_path, False)
    try:
        assert reopened.stats()["unique_checkpointed_events"] == 2
    finally:
        reopened.close()


def test_global_frontier_waits_for_every_partition_even_if_last_finishes_first(tmp_path):
    ledger = open_ledger(tmp_path)
    cut = plan()
    cut["partitions"] = [
        {"partition_id": 0, "after_position": None, "high_watermark_position": position(4),
         "event_count": 2, "cursor": "private-part-0"},
        {"partition_id": 1, "after_position": position(4), "high_watermark_position": position(8),
         "event_count": 2, "cursor": "private-part-1"},
    ]
    try:
        ledger.begin_plan(cut)
        ledger.commit_page(partition_id=1, origin_cursor="private-part-1",
                           page=page((6, 8), more=False, token="private-high-8"))
        assert ledger.frontier() == (None, None)
        assert ledger.pending() == ((0, "private-part-0"),)
        ledger.commit_page(partition_id=0, origin_cursor="private-part-0",
                           page=page((2, 4), end=4, more=False, token="private-high-4"))
        assert ledger.frontier() == ("private-high-8", position(8))
    finally:
        ledger.close()


def test_distinct_arrived_partition_pages_share_one_full_and_publish_frontier(tmp_path):
    ledger = open_ledger(tmp_path)
    cut = plan()
    cut["partitions"] = [
        {
            "partition_id": index,
            "after_position": None if index == 0 else position(index * 2),
            "high_watermark_position": position((index + 1) * 2),
            "event_count": 1,
            "cursor": f"private-part-{index}",
        }
        for index in range(4)
    ]
    try:
        ledger.begin_plan(cut)
        committed = ledger.commit_pages(
            tuple(
                (
                    index,
                    f"private-part-{index}",
                    page(
                        ((index + 1) * 2,),
                        end=(index + 1) * 2,
                        more=False,
                        token=f"private-high-{(index + 1) * 2}",
                    ),
                )
                for index in range(4)
            )
        )
        assert [item.page_serial for item in committed] == [1, 2, 3, 4]
        assert {item.checkpointed_ns for item in committed} == {
            ledger.last_page_stages["full_after_ns"]
        }
        assert ledger.last_page_stages["pages"] == 4
        assert ledger.last_page_stages["unique_events"] == 4
        assert ledger.stats()["unique_checkpointed_events"] == 4
        assert ledger.frontier() == ("private-high-8", position(8))
    finally:
        ledger.close()


def test_checkpoint_batch_is_bounded_distinct_and_atomic(tmp_path):
    ledger = open_ledger(tmp_path)
    cut = plan()
    cut["partitions"] = [
        {
            "partition_id": index,
            "after_position": None if index == 0 else position(index * 2),
            "high_watermark_position": position((index + 1) * 2),
            "event_count": 1,
            "cursor": f"private-part-{index}",
        }
        for index in range(2)
    ]
    cut.update(event_count=2, high_watermark_position=position(4))
    try:
        ledger.begin_plan(cut)
        valid = page((2,), end=2, more=False, token="private-high-2")
        invalid = page((4,), end=4, more=False, token="private-part-1")
        with pytest.raises(ValueError):
            ledger.commit_pages(
                ((0, "private-part-0", valid), (1, "private-part-1", invalid))
            )
        assert ledger.stats()["unique_checkpointed_events"] == 0
        assert ledger.pending() == ((0, "private-part-0"), (1, "private-part-1"))
        with pytest.raises(ValueError, match="distinct"):
            ledger.commit_pages(
                ((0, "private-part-0", valid), (0, "private-part-0", valid))
            )
        with pytest.raises(ValueError, match="1..4"):
            ledger.commit_pages(())
    finally:
        ledger.close()


def test_duplicate_and_new_distinct_partition_pages_preserve_unique_count(tmp_path):
    ledger = open_ledger(tmp_path)
    cut = plan()
    cut["partitions"] = [
        {
            "partition_id": 0,
            "after_position": None,
            "high_watermark_position": position(2),
            "event_count": 1,
            "cursor": "private-part-0",
        },
        {
            "partition_id": 1,
            "after_position": position(2),
            "high_watermark_position": position(4),
            "event_count": 1,
            "cursor": "private-part-1",
        },
    ]
    cut.update(event_count=2, high_watermark_position=position(4))
    first = page((2,), end=2, more=False, token="private-high-2")
    second = page((4,), end=4, more=False, token="private-high-4")
    try:
        ledger.begin_plan(cut)
        ledger.commit_page(partition_id=0, origin_cursor="private-part-0", page=first)
        results = ledger.commit_pages(
            ((0, "private-part-0", first), (1, "private-part-1", second))
        )
        assert results[0].unique_events == 0 and results[0].replayed_events == 1
        assert results[1].unique_events == 1 and results[1].replayed_events == 0
        assert ledger.stats()["unique_checkpointed_events"] == 2
        assert ledger.frontier() == ("private-high-4", position(4))
    finally:
        ledger.close()


def test_concurrent_probe_uses_one_inflight_per_signed_partition(tmp_path):
    ledger = open_ledger(tmp_path)
    lock = threading.Lock()
    state = {"active": 0, "peak": 0}
    origins: list[str] = []

    class Client:
        def call(self, request):
            operation = request["operation"]
            if operation == "CreateReplayPartitions":
                payload = plan()
                payload["partitions"] = [
                    {
                        "partition_id": index,
                        "after_position": None if index == 0 else position(index * 2),
                        "high_watermark_position": position((index + 1) * 2),
                        "event_count": 1,
                        "cursor": f"private-part-{index}",
                    }
                    for index in range(4)
                ]
            else:
                origin = request["payload"]["cursor"]
                index = int(origin.rsplit("-", 1)[1])
                with lock:
                    origins.append(origin)
                    state["active"] += 1
                    state["peak"] = max(state["peak"], state["active"])
                time.sleep(0.02)
                with lock:
                    state["active"] -= 1
                payload = page(
                    ((index + 1) * 2,),
                    end=(index + 1) * 2,
                    more=False,
                    token=f"private-high-{(index + 1) * 2}",
                )
            return {
                "message_type": "response",
                "schema_version": 2,
                "status": "COMPLETED",
                "request_id": request["request_id"],
                "payload": payload,
            }

    probe = ConcurrentRollingConsumerEventProbe(
        client=Client(), checkpoint=ledger, consumer_account_id="test", maximum_inflight=4
    )
    try:
        assert probe.step() == ()
        while ledger.stats()["unique_checkpointed_events"] < 4:
            probe.step()
        assert state["peak"] == 4
        assert sorted(origins) == [f"private-part-{index}" for index in range(4)]
        assert ledger.stats()["pending_partitions"] == 0
        assert ledger.frontier() == ("private-high-8", position(8))
        assert all(call.operation == "PollEvents" for call in probe.last_calls)
    finally:
        probe.close()
        ledger.close()


def test_lost_full_commit_ack_never_returns_a_checkpoint_receipt(tmp_path):
    ledger = open_ledger(tmp_path)
    ledger.begin_plan(plan())
    database = ledger._db

    class LostAck:
        def __getattr__(self, name):
            return getattr(database, name)

        def __enter__(self):
            return database.__enter__()

        def __exit__(self, *args):
            database.__exit__(*args)
            raise sqlite3.OperationalError("commit ACK lost")

    ledger._db = LostAck()
    with pytest.raises(ValueError, match="outcome"):
        ledger.commit_page(partition_id=0, origin_cursor="private-start-8", page=page())
    with pytest.raises(ValueError, match="failed"):
        ledger.pending()
    ledger.close()
    reopened = open_ledger(tmp_path, False)
    try:
        assert reopened.stats()["unique_checkpointed_events"] == 2
        assert reopened.pending() == ((0, "private-next-4"),)
    finally:
        reopened.close()


def test_fixed_sql_page_budget_fails_without_advancing_partial_page(tmp_path):
    root = tmp_path / "checkpoint"
    ledger = ConsumerEventCheckpoint(
        root=root, scope_sha256="a" * 64, initialize=True, maximum_bytes=1024**2
    )
    ledger.begin_plan(plan(end=20_000, count=10_000))
    committed = 0
    try:
        with pytest.raises(ValueError, match="outcome"):
            for start in range(0, 10_000, 100):
                _, cursor = ledger.pending()[0]
                ledger.commit_page(
                    partition_id=0, origin_cursor=cursor,
                    page=page(tuple(range(start * 2 + 2, (start + 100) * 2 + 1, 2)),
                              end=20_000, more=start + 100 < 10_000, token=f"private-{start}"),
                )
                committed += 100
    finally:
        ledger.close()
    assert 0 < committed < 10_000
    reopened = ConsumerEventCheckpoint(
        root=root, scope_sha256="a" * 64, initialize=False, maximum_bytes=1024**2
    )
    try:
        # The FULL page transaction can succeed before the post-commit total
        # storage fence reports that no further page may be admitted.  Recovery
        # therefore accepts either the last acknowledged prefix or exactly one
        # additional complete page; a partial page is never observable.
        recovered = reopened.stats()["unique_checkpointed_events"]
        assert recovered in {committed, committed + 100}
        assert recovered % 100 == 0
        assert reopened.stats()["database_bytes"] <= 1024**2
    finally:
        reopened.close()
