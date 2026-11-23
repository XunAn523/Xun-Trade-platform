import multiprocessing as mp
import os
import sqlite3
import sys
from dataclasses import replace
from itertools import zip_longest
from time import monotonic_ns
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStore,
)

from test_order_projection_store import SCOPE, admit, create, retry


def open_store(root, journal, **kw):
    return OrderProjectionStore(
        root, journal_id=journal.journal_id, profile_hash=journal.profile_hash, **kw
    )


@pytest.mark.parametrize("group_size", [16, 32, 64, 128, 256])
def test_reuse_keeps_all_tables_identical_and_eliminates_only_transaction_local_work(
    tmp_path, group_size
):
    journal, book, legacy, tail = create(tmp_path)
    candidate = open_store(
        tmp_path / "optimized", journal, initialize=True, optimize_transactions=True
    )
    try:
        for index in range(300):
            order = admit(book, f"diff-{index}")
            if index % 2:
                retry(lambda: book.mark_working(oms_order_id=order.order.order.oms_order_id))
            else:
                retry(lambda: book.reject(oms_order_id=order.order.order.oms_order_id))
        batches = tuple(iter(lambda: tail.read_next(), None))
        for offset in range(0, len(batches), group_size):
            group = batches[offset : offset + group_size]
            assert candidate.apply_many(group) == legacy.apply_many(group)
            assert candidate._group_meta is None and candidate._group_checkpoints == {}
            assert candidate._group_reducers == {}
            assert candidate._group_snapshot_ids == candidate._group_snapshot_scopes == {}
            assert candidate._group_order_rows == {} and candidate._group_fact_rows == []
        for table in (
            "meta",
            "orders",
            "events",
            "facts",
            "batches",
            "snapshot_ids",
            "snapshot_scopes",
            "snapshot_versions",
        ):
            left = legacy._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")
            right = candidate._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")
            assert list(left) == list(right), table
        assert candidate.cursor == legacy.cursor
        assert candidate.snapshot_descriptor(SCOPE) == legacy.snapshot_descriptor(SCOPE)
        watermark, count = candidate.snapshot_descriptor(SCOPE)
        for start in range(0, count, 100):
            kw = dict(watermark=watermark, count=count, start=start, end=min(start + 100, count))
            assert candidate.snapshot_page(SCOPE, **kw) == legacy.snapshot_page(SCOPE, **kw)
        assert candidate._db.execute("PRAGMA synchronous").fetchone() == (2,)
        assert candidate.apply_many(batches[:group_size]) == (False,) * group_size
    finally:
        candidate.close()
        tail.close()
        legacy.close()
        journal.close()


def test_group_cache_is_bounded_cleared_and_does_not_skip_duplicate_proofs(tmp_path):
    journal, book, old, tail = create(tmp_path)
    old.close()
    store = open_store(tmp_path / "projection", journal, optimize_transactions=True)
    try:
        order = admit(book)
        retry(lambda: book.mark_working(oms_order_id=order.order.order.oms_order_id))
        first, second = tail.read_next(), tail.read_next()
        sql = []
        store._db.set_trace_callback(sql.append)
        with patch.object(store, "_checked", wraps=store._checked) as checked:
            assert store.apply_many((first, first, second)) == (True, False, True)
            assert checked.call_count == 0  # new order is already checked by reducer in this txn
        assert sum(statement.startswith("UPDATE meta") for statement in sql) == 1
        assert sum("FROM meta AS" in statement for statement in sql) == 1
        assert store._group_checkpoints == {} and store._group_meta is None
        assert store._group_reducers == {}
        assert store._group_snapshot_ids == store._group_snapshot_scopes == {}
        with pytest.raises(OrderProjectionError):
            store.apply_many((first, replace(first, sha256="0" * 64)))
        assert store._group_checkpoints == {} and store._group_meta is None
        assert store._group_reducers == {}
        assert store._group_snapshot_ids == store._group_snapshot_scopes == {}
        store.close()
        store = open_store(tmp_path / "projection", journal)
        assert store.cursor == second.after
    finally:
        store.close()
        tail.close()
        journal.close()


@pytest.mark.parametrize(
    "fault",
    [
        "hash",
        "gap",
        "partial",
        "database",
        "scope_write",
        "order_write",
        "fact_write",
        "event_write",
        "version_write",
    ],
)
@pytest.mark.parametrize("seed_cache", (False, True))
def test_bad_later_member_rolls_back_all_optimized_state(tmp_path, fault, seed_cache):
    journal, book, old, tail = create(tmp_path)
    old.close()
    store = open_store(
        tmp_path / "projection",
        journal,
        optimize_transactions=True,
        reuse_verified_admission_seeds=seed_cache,
    )
    try:
        admit(book, "a")
        admit(book, "b")
        first, second = tail.read_next(), tail.read_next()
        if fault == "hash":
            second = replace(second, sha256="0" * 64)
        elif fault == "gap":
            first, second = second, first
        elif fault == "partial":
            second = replace(second, records=second.records[:-1])
        if fault == "database":
            store._db.execute(
                "CREATE TRIGGER fail_later BEFORE INSERT ON batches WHEN NEW.first_lsn>1 BEGIN SELECT RAISE(ABORT, 'fixture'); END"
            )
            store._db.commit()
        elif fault == "scope_write":
            store._db.execute(
                "CREATE TRIGGER fail_scope BEFORE INSERT ON snapshot_scopes BEGIN SELECT RAISE(ABORT, 'fixture'); END"
            )
            store._db.commit()
        elif fault in ("order_write", "fact_write", "event_write", "version_write"):
            table = {
                "order_write": "orders",
                "fact_write": "facts",
                "event_write": "events",
                "version_write": "snapshot_versions",
            }[fault]
            store._db.execute(
                f"CREATE TRIGGER fail_flush BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'fixture'); END"
            )
            store._db.commit()
        with pytest.raises((OrderProjectionError, sqlite3.IntegrityError)):
            store.apply_many((first, second))
        assert store._group_checkpoints == {} and store._group_meta is None
        assert store._group_snapshot_ids == store._group_snapshot_scopes == {}
        assert store._group_order_rows == {} and store._group_fact_rows == []
        assert store._group_event_rows == store._group_version_rows == []
        if seed_cache:
            assert store._seed_cache.entries == 0 and store._seed_cache._active is None
        store.close()
        store = open_store(tmp_path / "projection", journal)
        assert store.cursor.next_lsn == 1 and store.recovery_counts() == (0, 0, 0)
        for table in ("orders", "events", "facts", "batches", "snapshot_ids", "snapshot_versions"):
            assert store._db.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
    finally:
        store.close()
        tail.close()
        journal.close()


def test_group_snapshot_counts_and_ordinals_reused_but_versions_kept(tmp_path):
    journal, book, legacy, tail = create(tmp_path)
    store = open_store(tmp_path / "optimized", journal, initialize=True, optimize_transactions=True)
    try:
        for index in range(5):
            order = admit(book, f"snapshot-reuse-{index}")
            retry(lambda: book.reject(oms_order_id=order.order.order.oms_order_id))
        group = tuple(iter(tail.read_next, None))
        statements = []
        store._db.set_trace_callback(statements.append)
        bulk_calls = []
        connection = store._db
        proxy = Mock(wraps=connection)

        def bulk(sql, rows):
            rows = tuple(rows)
            bulk_calls.append((sql, len(rows)))
            return connection.executemany(sql, rows)

        proxy.executemany.side_effect = bulk
        with patch.object(store, "_db", proxy):
            assert store.apply_many(group) == legacy.apply_many(group) == (True,) * 10
        assert len(bulk_calls) == 4
        assert next(count for sql, count in bulk_calls if sql.startswith("INSERT INTO orders")) == 5
        assert next(count for sql, count in bulk_calls if sql.startswith("INSERT INTO facts")) == 20
        assert (
            next(count for sql, count in bulk_calls if sql.startswith("INSERT INTO events")) == 10
        )
        assert (
            next(
                count
                for sql, count in bulk_calls
                if sql.startswith("INSERT INTO snapshot_versions")
            )
            == 10
        )
        assert sum(v.startswith("SELECT orders FROM snapshot_scopes") for v in statements) == 1
        assert sum(v.startswith("INSERT INTO snapshot_scopes") for v in statements) == 1
        assert not any(v.startswith("SELECT ordinal FROM snapshot_ids") for v in statements)
        assert sum(v.startswith("INSERT INTO snapshot_versions") for v in statements) == 10
        assert store.snapshot_descriptor(SCOPE) == legacy.snapshot_descriptor(SCOPE) == (20, 5)
        assert store._group_snapshot_scopes == store._group_snapshot_ids == {}
        for table in ("snapshot_scopes", "snapshot_ids", "snapshot_versions"):
            assert (
                store._db.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
                == legacy._db.execute(f"SELECT * FROM {table} ORDER BY 1,2").fetchall()
            )
    finally:
        store.close()
        legacy.close()
        tail.close()
        journal.close()


def killed_writer(channel, root, bound, paths, phase):
    source = SimpleNamespace(
        journal_id=bound.journal_id,
        profile_hash=bound.profile_hash,
        read_bound=lambda: bound,
        segment_read_path=lambda index: paths[index],
    )
    tail = SegmentedJournalTail(source)
    batches = tuple(iter(lambda: tail.read_next(), None))
    store = OrderProjectionStore(
        root,
        journal_id=bound.journal_id,
        profile_hash=bound.profile_hash,
        optimize_transactions=True,
        reuse_verified_admission_seeds=os.environ.get("TRADECORE_PROJECTION_SCALE_SEED_CACHE")
        == "1",
    )

    def stop_here():
        channel.send("BOUNDARY")
        channel.recv()  # parent must kill, never resume this deliberately interrupted fixture
        raise AssertionError("fixture unexpectedly resumed")

    if phase == "before":
        stop_here()
    if phase == "mid":
        original = store._write_meta

        def mid(meta):
            original(meta)
            stop_here()

        store._write_meta = mid
    if phase == "committed_before_cache":
        from xuanling_tradecore.oms.verified_seed_cache import AdmissionSeedTransaction

        AdmissionSeedTransaction.publish_after_full_commit = lambda _self: stop_here()
    store.apply_many(batches)
    stop_here()


@pytest.mark.skipif(sys.platform != "linux", reason="actual Linux process termination")
@pytest.mark.parametrize("phase", ["before", "mid", "after", "committed_before_cache"])
@pytest.mark.parametrize("group_size", (16, 256))
def test_real_child_kill_exposes_only_full_prefix_or_full_committed_group(
    tmp_path, phase, group_size
):
    journal, book, store, tail = create(tmp_path)
    process = None
    parent, child = mp.get_context("spawn").Pipe()
    try:
        for index in range(group_size):
            admit(book, f"kill-{index}")
        bound = journal.read_bound()
        paths = {
            index: journal.segment_read_path(index) for index in range(1, bound.active_segment + 1)
        }
        store.close()
        process = mp.get_context("spawn").Process(
            target=killed_writer,
            args=(
                child,
                tmp_path / "projection",
                bound,
                paths,
                phase,
            ),
        )
        process.start()
        child.close()
        assert parent.poll(10) and parent.recv() == "BOUNDARY"
        process.kill()
        process.join(5)
        assert process.exitcode != 0 and not process.is_alive()
        store = open_store(tmp_path / "projection", journal)
        count = group_size if phase in ("after", "committed_before_cache") else 0
        assert store.recovery_counts() == (count * 2, count, count)
        assert store.cursor.next_lsn == count * 2 + 1
        assert store._db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        if process is not None and process.is_alive():
            process.kill()
            process.join(5)
        parent.close()
        child.close()
        store.close()
        tail.close()
        journal.close()


def streaming_mock_diff(tmp_path, record_property, total_orders, group_size=64, ordering="paired"):
    """No SDK/UDS capacity claim: real source/SQL differential at fixed data size.

    Both sides use the same original reducer, physical tail and FULL commits.
    Verified batches are held only as a pending bounded group. The synthetic
    producer book retains the fixed test population, not a long-run memory claim.
    The outer operator MUST provide a finite CPU/memory/runtime cgroup.
    """
    assert type(group_size) is int and group_size in (16, 32, 64, 128, 256)
    assert ordering in ("paired", "deferred")
    journal, book, legacy, tail = create(tmp_path, segment_frames=4096)
    candidate = open_store(
        tmp_path / "optimized",
        journal,
        initialize=True,
        optimize_transactions=True,
        reuse_verified_admission_seeds=os.environ.get("TRADECORE_PROJECTION_SCALE_SEED_CACHE")
        == "1",
    )
    elapsed = [0, 0]
    candidate_tail = SegmentedJournalTail(journal, reuse_segment_views=True)
    group, applied = [], 0
    try:

        def flush():
            nonlocal applied
            batch_group = tuple(group)
            results = []
            for index, store in enumerate((legacy, candidate)):
                before = monotonic_ns()
                results.append(store.apply_many(batch_group))
                elapsed[index] += monotonic_ns() - before
            assert results[0] == results[1] == (True,) * len(batch_group)
            applied += len(batch_group)
            group.clear()

        def terminal(index, order_id):
            if index % 5 == 4:
                retry(lambda: book.reject(oms_order_id=order_id))
            else:
                retry(
                    lambda: book.apply_trade(
                        oms_order_id=order_id,
                        trade_key=f"mock-fill-{index}",
                        quantity=book.order(order_id).leaves_quantity,
                    )
                )

        def consume_available():
            while (batch := tail.read_next()) is not None:
                assert candidate_tail.read_next() == batch
                group.append(batch)
                if len(group) == group_size:
                    flush()

        pending_ids = []
        for index in range(total_orders):
            order_id = admit(book, f"LR-{index}").order.order.oms_order_id
            if ordering == "paired":
                terminal(index, order_id)
            else:
                pending_ids.append(order_id)
            consume_available()
        if ordering == "deferred":
            if group:
                flush()  # no admission/terminal pairing in one transaction
            for index, order_id in enumerate(pending_ids):
                terminal(index, order_id)
                consume_available()
        if group:
            flush()
        assert candidate_tail.read_next() is None
        assert applied == 2 * total_orders
        assert (
            candidate.recovery_counts()
            == legacy.recovery_counts()
            == (4 * total_orders, total_orders, 2 * total_orders)
        )
        assert (
            candidate.cursor == legacy.cursor and candidate.cursor.next_lsn == 4 * total_orders + 1
        )
        for table in (
            "meta",
            "orders",
            "events",
            "facts",
            "batches",
            "snapshot_scopes",
            "snapshot_ids",
            "snapshot_versions",
        ):
            sentinel = object()
            left = legacy._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")
            right = candidate._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")
            for a, b in zip_longest(left, right, fillvalue=sentinel):
                assert a == b, table
        assert candidate.snapshot_descriptor(SCOPE) == (4 * total_orders, total_orders)
        assert candidate._group_meta is None and candidate._group_checkpoints == {}
        record_property("orders", total_orders)
        record_property("physical_batches", applied)
        record_property("transaction_batch_limit", group_size)
        record_property("ordering", ordering)
        record_property("legacy_apply_ns", elapsed[0])
        record_property("candidate_apply_ns", elapsed[1])
        record_property("native_calls", 0)
        record_property("full_mixed_qualification", False)
        if candidate._seed_cache is not None:
            assert candidate._seed_cache.entries <= 1024
            assert candidate._seed_cache.encoded_bytes <= 16 * 1024**2
            record_property("seed_cache_hits", candidate._seed_cache.hits)
            record_property("seed_cache_misses", candidate._seed_cache.misses)
    finally:
        candidate.close()
        candidate_tail.close()
        tail.close()
        legacy.close()
        journal.close()


@pytest.mark.parametrize("ordering", ("paired", "deferred"))
def test_streaming_mock_diff_smoke(tmp_path, record_property, ordering):
    streaming_mock_diff(tmp_path, record_property, 100, ordering=ordering)


@pytest.mark.skipif(
    os.environ.get("TRADECORE_PROJECTION_SCALE_ORDERS") != "100000",
    reason="explicit finite-resource 100,000-order acceptance only",
)
def test_streaming_hundred_thousand_mock_orders_keep_every_table_and_replay_identical(
    tmp_path, record_property
):
    streaming_mock_diff(
        tmp_path,
        record_property,
        100_000,
        int(os.environ.get("TRADECORE_PROJECTION_SCALE_BATCHES", "64")),
        os.environ.get("TRADECORE_PROJECTION_SCALE_ORDERING", "paired"),
    )
