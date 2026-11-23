from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread, current_thread
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.oms_operations_v2 import OrderBookConsumerOperationsV2
from xuanling_tradecore.oms import MmapOmsFactJournalFull, OrderBook, ReservationRequirement
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail, journal_batch_fingerprint
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal
from xuanling_tradecore.persistence.frozen_consumer_snapshot import FrozenConsumerSnapshot
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStale,
    OrderProjectionStore,
)
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalOrderProjector

from test_tradecore_oms import _bindings, _decision_context, _intent, _risk_snapshot

SCOPE = ("consumer-1", "consumer-account-1", "broker-account-1")


def retry(call):
    deadline = time.monotonic() + 5
    while True:
        try:
            return call()
        except MmapOmsFactJournalFull:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.001)


def admit(book, ref="ref", quantity=100):
    account, strategy, attribution = _bindings()
    return retry(
        lambda: book.admit(
            intent=_intent(strategy_order_id=ref, quantity=quantity),
            account_binding=account,
            strategy_binding=strategy,
            attribution_binding=attribution,
            risk_snapshot=_risk_snapshot(cash_minor=10**12),
            decision_context=_decision_context(),
            reservation_requirement=ReservationRequirement("SZ.000010", cash_minor=1000),
            now_monotonic_ns=20,
        )
    )


def create(tmp_path, *, segment_frames=8):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=segment_frames
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    store = OrderProjectionStore(
        tmp_path / "projection",
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        initialize=True,
    )
    tail = SegmentedJournalTail(journal)
    return journal, book, store, tail


def drain(store, tail):
    batches = []
    while (batch := tail.read_next()) is not None:
        assert store.apply(batch)
        batches.append(batch)
    return batches


def test_readonly_open_meta_and_batch_guard_share_one_statement_snapshot(tmp_path):
    journal, book, store, tail = create(tmp_path)
    opened, update, updated = Event(), Event(), Event()
    failures = []
    readonly = None
    worker = None
    try:
        admit(book, "prefix")
        drain(store, tail)
        admit(book, "new-commit")
        batch = tail.read_next()
        assert batch is not None
        store.close()

        def write_new_prefix():
            writer = None
            try:
                writer = OrderProjectionStore(
                    tmp_path / "projection",
                    journal_id=journal.journal_id,
                    profile_hash=journal.profile_hash,
                )
                opened.set()
                assert update.wait(3)
                writer.apply(batch)
            except BaseException as error:
                failures.append(error)
            finally:
                if writer is not None:
                    writer.close()
                updated.set()

        worker = Thread(target=write_new_prefix)
        worker.start()
        assert opened.wait(3)
        original_connect = sqlite3.connect
        triggered = [False]

        class Cursor:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            def fetchone(self):
                # The SELECT has returned its ONE complete snapshot row; now
                # let the real concurrent writer commit before Python verifies
                # the metadata. The former second SELECT saw the newer batch.
                result = self.wrapped.fetchone()
                if not triggered[0]:
                    triggered[0] = True
                    update.set()
                    assert updated.wait(3)
                return result

        class ReadConnection(sqlite3.Connection):
            def execute(self, sql, *args):
                result = super().execute(sql, *args)
                return Cursor(result) if "FROM META" in sql.upper() else result

        def connect(*args, **kwargs):
            if "mode=ro" in str(args[0]):
                kwargs["factory"] = ReadConnection
            return original_connect(*args, **kwargs)

        with patch(
            "xuanling_tradecore.persistence.order_projection_store.sqlite3.connect", new=connect
        ):
            readonly = OrderProjectionStore(
                tmp_path / "projection",
                journal_id=journal.journal_id,
                profile_hash=journal.profile_hash,
                read_only=True,
            )
        assert triggered[0] and not failures
        assert readonly.cursor == batch.after
    finally:
        update.set()
        if worker is not None:
            worker.join(timeout=4)
            assert not worker.is_alive()
        if readonly is not None:
            readonly.close()
        store.close()
        tail.close()
        journal.close()


def test_incremental_lifecycle_interleaving_matches_existing_consumer_projection(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book, "a")
        admit(book, "b")
        order_id = first.order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        retry(lambda: book.apply_trade(oms_order_id=order_id, trade_key="trade", quantity=40))
        retry(lambda: book.request_cancel(oms_order_id=order_id))
        retry(lambda: book.cancel(oms_order_id=order_id))
        admit(book, "a", quantity=200)  # conflict advances fact watermark, not event count
        batches = drain(store, tail)
        actual = store.lookup(SCOPE, order_id)
        current = book.order(order_id)
        expected = OrderBookConsumerOperationsV2._project_order(
            book.last_order_event(order_id), current.order.consumer_trace, current.order.trading_day
        )
        assert actual == expected
        events = store.events_after(SCOPE, 0, limit=2)
        all_events = list(events.events)
        while events.has_more:
            events = store.events_after(SCOPE, events.after_lsn, limit=2)
            all_events.extend(events.events)
        assert [event.journal_lsn for event in all_events] == [
            event.journal_lsn for event in book.events(SCOPE[2])
        ]
        assert store.cursor.next_lsn == journal.read_bound().committed_lsn + 1
        assert not store.apply(batches[0])
        assert not store.apply(batches[-1])
        assert len(store.events_after(SCOPE, 0).events) == len(all_events)
        assert store.lookup(("foreign", SCOPE[1], SCOPE[2]), order_id) is None
        assert store.events_after((SCOPE[0], "foreign", SCOPE[2]), 0).events == ()
        with pytest.raises(OrderProjectionStale):
            store.lookup(SCOPE, order_id, minimum_watermark=journal.read_bound().committed_lsn + 1)
        with pytest.raises(OrderProjectionStale):
            store.events_after(SCOPE, journal.read_bound().committed_lsn + 1)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_fixed_event_window_and_partition_bounds_share_verified_prefix(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book, "a")
        admit(book, "b")
        drain(store, tail)
        frozen = store.cursor.next_lsn - 1
        retry(lambda: book.mark_working(oms_order_id=first.order.order.oms_order_id))
        admit(book, "c")
        drain(store, tail)
        page = store.events_after(SCOPE, 0, limit=1, through_lsn=frozen)
        events = list(page.events)
        while page.has_more:
            page = store.events_after(SCOPE, page.after_lsn, limit=1, through_lsn=frozen)
            events.extend(page.events)
        assert len(events) == 2
        assert page.watermark == frozen
        assert all(e.journal_lsn <= frozen for e in events)
        bounds = store.partition_events(SCOPE, partition_count=3)
        assert len(bounds) == 3
        assert sum(count for _, _, count in bounds) == 4
        capped = store.partition_events(SCOPE, partition_count=3, through_lsn=frozen)
        assert sum(count for _, _, count in capped) == 2
        assert all(end <= frozen for _, end, _ in capped)
        assert store.partition_events(SCOPE, partition_count=3, through_lsn=0) == ()
        after = 0
        seen = []
        for start, end, count in bounds:
            assert start == after
            part = store.events_after(SCOPE, start, through_lsn=end)
            assert len(part.events) == count and not part.has_more
            seen.extend(e.journal_lsn for e in part.events)
            after = end
        assert seen == sorted(set(seen))
        assert store.partition_events(("foreign", *SCOPE[1:]), partition_count=3) == ()
        ahead = store.cursor.next_lsn
        with pytest.raises(OrderProjectionStale):
            store.events_after(SCOPE, 0, minimum_watermark=ahead)
        with pytest.raises(OrderProjectionStale):
            store.events_after(SCOPE, 0, through_lsn=ahead)
        with pytest.raises(OrderProjectionStale):
            store.partition_events(SCOPE, partition_count=2, minimum_watermark=ahead)
        for bad in (True, 0, 65):
            with pytest.raises(OrderProjectionError):
                store.partition_events(SCOPE, partition_count=bad)
        for bad in (True, -1):
            with pytest.raises(OrderProjectionError):
                store.partition_events(SCOPE, partition_count=2, through_lsn=bad)
        with pytest.raises(OrderProjectionStale):
            store.partition_events(SCOPE, partition_count=2, after_lsn=frozen, through_lsn=0)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_rows_and_watermark_roll_back_together_and_resume_from_last_commit(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book)
        batch = tail.read_next()
        with patch.object(store, "_write_meta", side_effect=OSError("before cursor publish")):
            with pytest.raises(OSError):
                store.apply(batch)
        store.close()
        store = OrderProjectionStore(
            tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
        )
        assert store.cursor.next_lsn == 1
        assert store.lookup(SCOPE, first.order.order.oms_order_id) is None
        assert store.events_after(SCOPE, 0).events == ()
        assert store.apply(batch)
        assert store.lookup(SCOPE, first.order.order.oms_order_id) is not None
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("tamper", ["gap", "partial", "digest", "state", "after"])
def test_bad_batch_never_advances_projection(tmp_path, tamper):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book)
        batch = tail.read_next()
        if tamper == "gap":
            import msgspec

            batch = replace(batch, before=msgspec.structs.replace(batch.before, next_lsn=5))
        elif tamper == "partial":
            batch = replace(batch, records=batch.records[:1])
        elif tamper == "digest":
            batch = replace(batch, sha256="0" * 64)
        elif tamper == "after":
            import msgspec

            batch = replace(batch, after=msgspec.structs.replace(batch.after, frame=99))
        else:
            rows = (
                batch.records[0],
                replace(
                    batch.records[1],
                    fact=replace(batch.records[1].fact, attributes={"cash_minor": "0"}),
                ),
            )
            batch = replace(batch, records=rows, sha256=journal_batch_fingerprint(rows))
        with pytest.raises((ValueError, OrderProjectionError)):
            store.apply(batch)
        store.close()
        store = OrderProjectionStore(
            tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
        )
        assert store.cursor.next_lsn == 1
        assert store.events_after(SCOPE, 0).events == ()
    finally:
        tail.close()
        store.close()
        journal.close()


def test_snapshot_streams_full_history_and_readonly_queries_are_scoped(tmp_path):
    journal, book, store, tail = create(tmp_path)
    reader = None
    try:
        ids = [admit(book, str(n)).order.order.oms_order_id for n in range(100)]
        batches = drain(store, tail)
        reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        watermark = reader.freeze_snapshot(tmp_path / "frozen", SCOPE)
        source = FrozenConsumerSnapshot(tmp_path / "frozen")
        try:
            assert source.order_count == 100
            assert source.snapshot_position.stream_id.redis_ms == watermark
            assert [row.oms_order_id for row in source.read_orders(0, 100)] == sorted(ids)
        finally:
            source.close()
        assert reader.lookup(SCOPE, ids[0]) == store.lookup(SCOPE, ids[0])
        with pytest.raises(OrderProjectionError, match="readonly"):
            reader.apply(batches[0])
    finally:
        if reader is not None:
            reader.close()
        tail.close()
        store.close()
        journal.close()


def test_missing_wrong_identity_second_writer_and_corrupt_rows_fail_closed(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        drain(store, tail)
        with pytest.raises((OSError, ValueError)):
            OrderProjectionStore(
                tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
            )
        with pytest.raises(OrderProjectionError):
            OrderProjectionStore(
                tmp_path / "missing", journal_id="stream", profile_hash=journal.profile_hash
            )
        with pytest.raises(OrderProjectionError):
            OrderProjectionStore(
                tmp_path / "projection",
                journal_id="wrong",
                profile_hash=journal.profile_hash,
                read_only=True,
            )
        with store._db:
            store._db.execute("UPDATE orders SET latest=?", (b"corrupt",))
        with pytest.raises(OrderProjectionError, match="checksum"):
            store.lookup(SCOPE, order_id)
    finally:
        tail.close()
        store.close()
        journal.close()


def wait(projector, predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        health = projector.health()
        if predicate(health):
            return health
        time.sleep(0.005)
    raise AssertionError(f"projector did not reach expected state: {projector.health()}")


def test_background_pause_catchup_restart_and_cached_health(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    projector = IncrementalOrderProjector(
        journal=journal, root=tmp_path / "projection", initialize=True
    )
    try:
        projector.start()
        projector.pause()
        wait(projector, lambda health: health.state == "PAUSED")
        ids = [admit(book, str(n)).order.order.oms_order_id for n in range(20)]
        paused = wait(projector, lambda health: health.lag_facts == 40)
        assert paused.projected_lsn == 0
        original_bound, caller = journal.read_bound, current_thread()

        def guarded_bound():
            assert current_thread() is not caller, "health touched writer"
            return original_bound()

        with patch.object(journal, "read_bound", side_effect=guarded_bound):
            assert projector.health() == paused
        projector.resume()
        wait(projector, lambda health: health.projected_lsn == 40)
        projector.close()
        assert projector.health().state == "STOPPED"
        projector = IncrementalOrderProjector(journal=journal, root=tmp_path / "projection")
        projector.start()
        assert projector.health().projected_lsn == 40
        assert projector.health().completed_batches == 0
        retry(lambda: book.mark_working(oms_order_id=ids[0]))
        wait(projector, lambda health: health.projected_lsn == 41)
        with_reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        try:
            assert with_reader.lookup(SCOPE, ids[0]).order_state.value == "WORKING"
        finally:
            with_reader.close()
    finally:
        projector.close()
        journal.close()


def test_projector_failure_leaves_source_and_checkpoint_unchanged(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    projector = IncrementalOrderProjector(
        journal=journal, root=tmp_path / "projection", initialize=True
    )
    try:
        projector.start()
        with patch(
            "xuanling_tradecore.persistence.order_projection_store.reduce_order_projection",
            side_effect=OSError("private failure"),
        ):
            admit(OrderBook(journal=journal, persist_recovery_seed=True))
            assert wait(projector, lambda health: health.state == "FAILED").projected_lsn == 0
        reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        try:
            assert reader.cursor.next_lsn == 1
        finally:
            reader.close()
        assert journal.read_bound().committed_lsn == 2
    finally:
        projector.close()
        journal.close()


def test_process_exit_after_projection_commit_preserves_resume_cursor(tmp_path):
    script = """
import os,sys
from pathlib import Path
from test_order_projection_store import create,admit,drain
root=Path(sys.argv[1])
journal,book,store,tail=create(root)
first=admit(book)
book.mark_working(oms_order_id=first.order.order.oms_order_id)
drain(store,tail)
os._exit(0)
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join((str(Path(__file__).parent), env.get("PYTHONPATH", "")))
    subprocess.run([sys.executable, "-c", script, str(tmp_path)], check=True, env=env, timeout=30)
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    store = OrderProjectionStore(
        tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
    )
    tail = SegmentedJournalTail(journal, cursor=store.cursor)
    try:
        assert store.cursor.next_lsn == 4
        assert tail.read_next() is None
        page = store.events_after(SCOPE, 0)
        assert len(page.events) == 2 and page.events[-1].order_state.value == "WORKING"
    finally:
        tail.close()
        store.close()
        journal.close()


def test_queries_refuse_foreign_thread_instead_of_using_shared_sql_connection(tmp_path):
    journal, book, store, tail = create(tmp_path)
    errors = []
    try:

        def query():
            try:
                store.lookup(SCOPE, "unknown")
            except Exception as exc:
                errors.append(exc)

        thread = Thread(target=query)
        thread.start()
        thread.join(2)
        assert len(errors) == 1 and isinstance(errors[0], OrderProjectionError)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_per_order_history_budget_rejects_before_cursor_advance(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        store._maximum_order_events = 1
        order_id = admit(book).order.order.oms_order_id
        drain(store, tail)
        retry(lambda: book.mark_working(oms_order_id=order_id))
        with pytest.raises(ValueError, match="bounded checkpoint"):
            store.apply(tail.read_next())
        store.close()
        store = OrderProjectionStore(
            tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
        )
        assert store.cursor.next_lsn == 3
        assert store.lookup(SCOPE, order_id).order_state.value == "ACCEPTED"
    finally:
        tail.close()
        store.close()
        journal.close()


def test_database_budget_full_retains_complete_prefix_and_all_source_facts(tmp_path):
    import sqlite3

    journal, book, store, tail = create(tmp_path)
    store.close()
    store = OrderProjectionStore(
        tmp_path / "projection",
        journal_id="stream",
        profile_hash=journal.profile_hash,
        maximum_database_bytes=1024**2,
    )
    previous = 0
    try:
        for n in range(500):
            order_id = admit(book, str(n)).order.order.oms_order_id
            batch = tail.read_next()
            try:
                store.apply(batch)
            except sqlite3.OperationalError:
                break
            previous = batch.after.next_lsn - 1
        else:
            raise AssertionError("bounded database never reached its page budget")
        assert journal.read_bound().committed_lsn == previous + 2
        assert store.path.stat().st_size <= 1024**2
        store.close()
        store = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=journal.profile_hash,
            maximum_database_bytes=1024**2,
        )
        assert store.cursor.next_lsn == previous + 1
        assert store.lookup(SCOPE, order_id) is None
    finally:
        tail.close()
        store.close()
        journal.close()


def test_projector_cleanup_failure_is_sticky_and_still_closes_store(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    projector = IncrementalOrderProjector(
        journal=journal, root=tmp_path / "projection", initialize=True
    )
    original = SegmentedJournalTail.close

    def failed_close(tail):
        original(tail)
        raise OSError("close failure")

    try:
        projector.start()
        with patch.object(SegmentedJournalTail, "close", new=failed_close):
            projector.close()
        assert projector.health().state == "FAILED"
        assert projector.health().error_code == "PROJECTION_CLOSE_FAILED"
        reopened = OrderProjectionStore(
            tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
        )
        reopened.close()
    finally:
        projector.close()
        journal.close()


def test_fixed_watermark_snapshot_excludes_concurrent_order_without_blocking_journal(tmp_path):
    import xuanling_tradecore.persistence.order_projection_store as projection_module

    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    projector = IncrementalOrderProjector(
        journal=journal, root=tmp_path / "projection", initialize=True
    )
    reader = None
    begin, appended = Event(), Event()
    results, errors = [], []

    def append_concurrently():
        try:
            assert begin.wait(3)
            results.append(admit(book, "after-snapshot").order.order.oms_order_id)
        except BaseException as exc:
            errors.append(exc)
        finally:
            appended.set()

    writer = Thread(target=append_concurrently)
    original = projection_module.build_frozen_consumer_snapshot

    def freeze(**kwargs):
        # The reader's BEGIN+watermark read already fixed its SQLite snapshot.
        begin.set()
        assert appended.wait(3), "Journal append waited for a read-model snapshot"
        return original(**kwargs)

    try:
        projector.start()
        old_ids = [admit(book, str(n)).order.order.oms_order_id for n in range(10)]
        wait(projector, lambda health: health.projected_lsn == 20)
        reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        writer.start()
        with patch.object(projection_module, "build_frozen_consumer_snapshot", side_effect=freeze):
            watermark = reader.freeze_snapshot(tmp_path / "fixed-view", SCOPE)
        assert not errors
        assert watermark == 20
        assert journal.read_bound().committed_lsn == 22
        source = FrozenConsumerSnapshot(tmp_path / "fixed-view")
        try:
            assert source.order_count == 10
            assert [row.oms_order_id for row in source.read_orders(0, 10)] == sorted(old_ids)
        finally:
            source.close()
        wait(projector, lambda health: health.projected_lsn == 22)
        assert reader.lookup(SCOPE, results[0], minimum_watermark=22) is not None
    finally:
        begin.set()
        if writer.ident is not None:
            writer.join(3)
        if reader is not None:
            reader.close()
        projector.close()
        journal.close()


def test_updates_restore_at_most_one_order_not_full_historical_account(tmp_path):
    from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint

    journal, book, store, tail = create(tmp_path)
    restored = []
    original = OrderBook.restore_recovery_checkpoint

    def restore(target, encoded, **kwargs):
        restored.append(len(decode_checkpoint(encoded).orders))
        return original(target, encoded, **kwargs)

    try:
        with patch.object(OrderBook, "restore_recovery_checkpoint", new=restore):
            for n in range(100):
                order_id = admit(book, str(n)).order.order.oms_order_id
                drain(store, tail)
                retry(lambda: book.mark_working(oms_order_id=order_id))
                drain(store, tail)
        assert max(restored) == 1
        assert store._meta().orders == 100
        assert store._meta().events == 200
        assert journal.read_bound().committed_lsn == 300
        from xuanling_tradecore.persistence.order_projection_store import _EVENT_PAGE_SQL

        plan = store._db.execute(
            "EXPLAIN QUERY PLAN " + _EVENT_PAGE_SQL, (*SCOPE, 0, 300, 101)
        ).fetchall()
        assert any("event_scope" in str(row[3]) for row in plan)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_closed_source_writer_cannot_report_healthy_caught_up_projection(tmp_path):
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=8
    )
    projector = IncrementalOrderProjector(
        journal=journal, root=tmp_path / "projection", initialize=True
    )
    try:
        projector.start()
        journal.close()
        assert (
            wait(projector, lambda health: health.state == "FAILED").error_code
            == "PROJECTION_FAILED"
        )
    finally:
        projector.close()
        journal.close()


def test_projection_ahead_of_available_source_fails_startup_without_resetting_index(tmp_path):
    journal, book, store, tail = create(tmp_path)
    admit(book)
    drain(store, tail)
    tail.close()
    store.close()
    journal.close()
    # Model a wrong/lost source prefix, not an actual power-loss experiment.
    replacement = SegmentedOmsFactJournal(
        root=tmp_path / "replacement", journal_id="stream", segment_frames=8
    )
    projector = IncrementalOrderProjector(journal=replacement, root=tmp_path / "projection")
    try:
        with pytest.raises(OrderProjectionError, match="projection failed"):
            projector.start()
        assert projector.health().state == "FAILED"
        reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id="stream",
            profile_hash=replacement.profile_hash,
            read_only=True,
        )
        try:
            assert reader.cursor.next_lsn == 3
        finally:
            reader.close()
    finally:
        projector.close()
        replacement.close()


def test_grouped_projection_commits_once_and_keeps_individual_batch_proofs(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book, "a")
        admit(book, "b")
        retry(lambda: book.reject(oms_order_id=first.order.order.oms_order_id))
        group = tuple(iter(lambda: tail.read_next(), None))
        statements = []
        store._db.set_trace_callback(statements.append)
        assert store.apply_many(group) == (True, True, True)
        assert statements.count("BEGIN IMMEDIATE") == 1
        assert statements.count("COMMIT") == 1
        assert store.recovery_counts() == (6, 2, 3)
        assert store.apply_many(group) == (False, False, False)
        assert store.cursor == group[-1].after
        assert store.lookup(SCOPE, first.order.order.oms_order_id).order_state.value == "REJECTED"
        assert store._db.execute("PRAGMA synchronous").fetchone() == (2,)
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("bad_suffix", [False, True])
def test_opt_in_phase_evidence_counts_only_real_full_commits(tmp_path, bad_suffix):
    journal, book, store, tail = create(tmp_path)
    rows = []
    store.close()
    store = OrderProjectionStore(
        tmp_path / "projection",
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        stage_observer=rows.append,
    )
    try:
        admit(book, "a")
        admit(book, "b")
        group = tuple(iter(lambda: tail.read_next(), None))
        if bad_suffix:
            with pytest.raises(OrderProjectionError):
                store.apply_many((group[0], replace(group[1], sha256="0" * 64)))
            assert rows[0]["values"]["committed"] is False
            assert rows[0]["values"]["committed_facts"] == 0
            assert rows[0]["calls"]["sql_rollback"] == 1
        else:
            assert store.apply_many(group) == (True, True)
            assert len(rows) == 1
            row = rows[0]
            assert row["values"]["committed_facts"] == 4
            assert row["values"]["committed_batches"] == 2
            assert row["values"]["first_lsn"] == 1 and row["values"]["final_lsn"] == 4
            assert row["calls"]["sql_commit"] == 1 and row["calls"]["reducer"] == 2
            assert row["calls"]["encoding"] > 2 and row["calls"]["hashing"] > 2
            assert store._db.execute("PRAGMA synchronous").fetchone() == (2,)
        assert store._stage is None and store._db.stage_timing is None
        store.close()
        reader = OrderProjectionStore(
            tmp_path / "projection",
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        try:
            assert reader.cursor.next_lsn == (1 if bad_suffix else 5)
        finally:
            reader.close()
    finally:
        store.close()
        tail.close()
        journal.close()


def test_grouped_projection_bad_suffix_rolls_back_the_entire_group(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book, "a")
        admit(book, "b")
        good, second = tail.read_next(), tail.read_next()
        bad = replace(second, sha256="0" * 64)
        with pytest.raises(OrderProjectionError):
            store.apply_many((good, bad))
        store.close()
        store = OrderProjectionStore(
            tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
        )
        assert store.cursor.next_lsn == 1
        assert store.recovery_counts() == (0, 0, 0)
        assert store.lookup(SCOPE, first.order.order.oms_order_id) is None
        assert store.apply_many((good, second)) == (True, True)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_grouped_projection_enforces_capacity_before_transaction(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book)
        batch = tail.read_next()
        oversized_facts = replace(batch, records=batch.records * 3)
        for group in ((), [batch], (batch,) * 257, (oversized_facts,) * 128):
            with pytest.raises(OrderProjectionError, match="budget"):
                store.apply_many(group)
            assert store.cursor.next_lsn == 1
        assert store.apply(batch)
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("bad_last", (False, True))
@pytest.mark.parametrize("group_size", (128, 256))
def test_larger_bounded_group_retains_full_atomicity_and_all_batch_proofs(
    tmp_path, bad_last, group_size
):
    journal, book, store, tail = create(tmp_path)
    try:
        for n in range(group_size):
            admit(book, f"bounded-group-{n}")
        group = tuple(iter(lambda: tail.read_next(), None))
        assert (
            len(group) == group_size
            and sum(len(batch.records) for batch in group) == group_size * 2
        )
        if bad_last:
            with pytest.raises(OrderProjectionError):
                store.apply_many((*group[:-1], replace(group[-1], sha256="0" * 64)))
            store.close()
            store = OrderProjectionStore(
                tmp_path / "projection", journal_id="stream", profile_hash=journal.profile_hash
            )
            assert store.recovery_counts() == (0, 0, 0)
            assert store.cursor.next_lsn == 1
        statements = []
        store._db.set_trace_callback(statements.append)
        assert store.apply_many(group) == (True,) * group_size
        assert statements.count("BEGIN IMMEDIATE") == statements.count("COMMIT") == 1
        assert store._db.execute("PRAGMA synchronous").fetchone() == (2,)
        assert store.recovery_counts() == (group_size * 2, group_size, group_size)
        assert store.cursor == group[-1].after
        assert store._db.execute("SELECT count(*) FROM batches").fetchone() == (group_size,)
    finally:
        tail.close()
        store.close()
        journal.close()
