import sqlite3

import pytest
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStore,
)

from test_order_projection_store import admit, create, retry


@pytest.mark.parametrize("fault", (False, True))
def test_commit_only_evidence_keeps_full_ranges_without_per_operation_probes(tmp_path, fault):
    journal, book, legacy, tail = create(tmp_path)
    rows = []
    root = tmp_path / "commit-only"
    store = OrderProjectionStore(
        root,
        journal_id=journal.journal_id,
        profile_hash=journal.profile_hash,
        initialize=True,
        optimize_transactions=True,
        stage_observer=rows.append,
        commit_only_diagnostics=True,
    )
    try:
        assert type(store._db) is sqlite3.Connection  # no per-execute wrapper
        for n in range(10):
            order = admit(book, f"commit-{n}")
            retry(lambda: book.reject(oms_order_id=order.order.order.oms_order_id))
        group = tuple(iter(tail.read_next, None))
        if fault:
            store._db.execute(
                "CREATE TRIGGER fail_bulk BEFORE INSERT ON facts BEGIN SELECT RAISE(ABORT, 'fixture'); END"
            )
            store._db.commit()
            with pytest.raises(sqlite3.IntegrityError):
                store.apply_many(group)
        else:
            assert store.apply_many(group) == legacy.apply_many(group)
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
                assert list(store._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")) == list(
                    legacy._db.execute(f"SELECT * FROM {table} ORDER BY 1,2")
                )
        assert len(rows) == 1
        row = rows[0]
        assert row["calls"] == {"sql_rollback" if fault else "sql_commit": 1}
        assert row["values"]["commit_only"] is True
        assert row["values"]["committed"] is not fault
        assert row["values"]["committed_facts"] == (0 if fault else 40)
        assert row["values"]["attempted_batches"] == 20
        assert row["values"]["first_lsn"] == 1 and row["values"]["final_lsn"] == 40
        # Windows CPython 3.10 monotonic can quantize a short transaction to
        # equal endpoints. Preserve it rather than inventing positive duration.
        assert 0 < row["start_ns"] <= row["end_ns"]
        store.close()
        store = OrderProjectionStore(
            root, journal_id=journal.journal_id, profile_hash=journal.profile_hash
        )
        assert store.cursor.next_lsn == (1 if fault else 41)
    finally:
        store.close()
        legacy.close()
        tail.close()
        journal.close()


def test_commit_only_requires_a_real_sink_before_initialization(tmp_path):
    target = tmp_path / "forbidden"
    with pytest.raises(OrderProjectionError):
        OrderProjectionStore(
            target,
            journal_id="journal",
            profile_hash="a" * 64,
            initialize=True,
            commit_only_diagnostics=True,
        )
    assert not target.exists()
