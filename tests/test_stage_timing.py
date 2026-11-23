import sqlite3

import pytest
from xuanling_tradecore.persistence.stage_timing import StageTiming, TimedSqliteConnection


def test_fixed_stages_record_failure_time_without_exception_text():
    rows = []
    timing = StageTiming("reactor", rows.append)
    assert timing.call("decode", lambda n: n + 1, 1) == 2
    with pytest.raises(RuntimeError):
        timing.call("read", lambda: (_ for _ in ()).throw(RuntimeError("private password")))
    timing.finish({"accepted": 1, "succeeded": False})
    row = rows[0]
    assert row["calls"] == {"read": 1, "decode": 1}
    assert row["start_ns"] <= row["end_ns"]
    assert row["inclusive_stages"] is True
    assert "private" not in str(row)
    with pytest.raises(ValueError):
        timing.finish({})


def test_stage_names_and_values_cannot_export_payloads():
    timing = StageTiming("projector", lambda _: None)
    with pytest.raises(ValueError):
        timing.call("password", lambda: None)
    with pytest.raises(ValueError):
        timing.finish({"payload": "private"})


def test_sql_diagnostics_preserve_full_transactions_and_rollback():
    rows = []
    db = sqlite3.connect(":memory:", factory=TimedSqliteConnection)
    db.execute("CREATE TABLE example(x)")
    db.stage_timing = timing = StageTiming("projector", rows.append)
    try:
        db.execute("BEGIN IMMEDIATE")
        db.executemany("INSERT INTO example VALUES(?)", [(1,), (2,)])
        db.commit()
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO example VALUES(3)")
        db.rollback()
        assert db.execute("SELECT sum(x) FROM example").fetchone() == (3,)
        timing.finish({"committed": True})
        db.stage_timing = None
        assert rows[0]["calls"]["sql_commit"] == 1
        assert rows[0]["calls"]["sql_rollback"] == 1
        assert rows[0]["calls"]["sql_execute"] == 5
    finally:
        db.close()
