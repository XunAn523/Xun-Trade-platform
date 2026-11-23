import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStore,
)
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth
from xuanling_tradecore.read_model.process_order_projector import ProcessOrderProjector

from test_order_projection_store import SCOPE, admit, create


@pytest.mark.parametrize("capacity", (0, 257, True))
def test_invalid_transaction_budget_refused_before_child_start(tmp_path, capacity):
    journal, _, store, tail = create(tmp_path)
    try:
        with pytest.raises(OrderProjectionError, match="transaction batch"):
            ProcessOrderProjector(
                journal=journal,
                root=tmp_path / "projection",
                maximum_database_bytes=8 * 1024**3,
                maximum_transaction_batches=capacity,
            )
        assert journal.read_bound().committed_lsn == 0
    finally:
        tail.close()
        store.close()
        journal.close()


def wait(worker, predicate):
    end = time.monotonic() + 8
    while time.monotonic() < end:
        health = worker.health()
        if predicate(health):
            return health
        assert health.state != "FAILED", health
        time.sleep(0.01)
    pytest.fail("child projection made no expected progress")


def test_spawned_projection_rotates_pauses_and_resumes_exact_persisted_prefix(tmp_path):
    journal, book, store, tail = create(tmp_path)
    store.close()
    tail.close()
    worker = ProcessOrderProjector(
        journal=journal,
        root=tmp_path / "projection",
        maximum_database_bytes=8 * 1024**3,
        maximum_transaction_batches=64,
    )
    try:
        worker.start()
        worker.pause()
        wait(worker, lambda h: h.state == "PAUSED")
        with pytest.raises(OrderProjectionError, match="unavailable"):
            worker.drain(final_lsn=0)
        orders = [admit(book, f"process-{n}") for n in range(12)]
        assert worker.health().projected_lsn == 0
        worker.resume()
        cut = journal.read_bound().committed_lsn
        wait(worker, lambda h: h.state == "RUNNING")
        assert worker.drain(final_lsn=cut).projected_lsn == cut
        worker.close()
        assert worker.drain(final_lsn=cut).projected_lsn == cut
        assert worker._process.exitcode == 0
        store = OrderProjectionStore(
            tmp_path / "projection",
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            read_only=True,
        )
        try:
            assert store.cursor.next_lsn == cut + 1
            assert len(store.events_after(SCOPE, 0).events) == 12
            assert store.lookup(SCOPE, orders[-1].order.order.oms_order_id) is not None
        finally:
            store.close()
        # Journal writer/lease is still owned by parent after child drain.
        admit(book, "after-child")
        with pytest.raises(OrderProjectionError, match="quiescent"):
            worker.drain(final_lsn=cut)
        replacement = ProcessOrderProjector(
            journal=journal, root=tmp_path / "projection", maximum_database_bytes=8 * 1024**3
        )
        try:
            replacement.start()
            wait(replacement, lambda h: h.projected_lsn == journal.read_bound().committed_lsn)
        finally:
            replacement.close()
    finally:
        worker.close()
        journal.close()


@pytest.mark.parametrize("commit_only", (False, True))
def test_projection_phase_writer_closes_with_complete_real_commit_ranges(tmp_path, commit_only):
    journal, book, store, tail = create(tmp_path)
    store.close()
    tail.close()
    path = tmp_path / "stages.jsonl"
    worker = ProcessOrderProjector(
        journal=journal,
        root=tmp_path / "projection",
        maximum_database_bytes=8 * 1024**3,
        stage_trace_path=path,
        commit_only_diagnostics=commit_only,
    )
    try:
        for n in range(12):
            admit(book, f"stage-{n}")
        worker.start()
        cut = journal.read_bound().committed_lsn
        worker.drain(final_lsn=cut)
        worker.close()
        assert worker.shutdown_verified
        status = json.loads(path.with_suffix(".status.json").read_text())
        assert status["schema_version"] == "consumer-stage-trace-v1"
        assert status["closed"] and status["drain_complete"]
        assert status["queue_dropped"] == status["capacity_dropped"] == 0
        from xuanling_tradecore.persistence.rotating_request_trace import (
            verify_closed_request_trace,
        )

        assert verify_closed_request_trace(path.with_suffix(".manifest.json")) == status["written"]
        # Read the sealed names from the manifest rather than guessing their shape.
        manifest = json.loads(path.with_suffix(".manifest.json").read_text())
        rows = [
            json.loads(line)
            for part in manifest["segments"]
            for line in (path.parent / part["file"]).read_text().splitlines()
        ]
        assert sum(row["values"].get("committed_facts", 0) for row in rows) == cut
        assert all(row["inclusive_stages"] for row in rows)
        if commit_only:
            assert all(set(row["calls"]) <= {"sql_commit", "sql_rollback"} for row in rows)
            assert all(row["values"]["commit_only"] is True for row in rows)
        else:
            assert any("source_control" in row["calls"] for row in rows)
    finally:
        worker.close()
        journal.close()


def test_final_drain_rejects_deadline_changing_source_and_paused_owner():
    worker = object.__new__(ProcessOrderProjector)
    worker._started = True
    worker._closed = False
    worker._journal = SimpleNamespace(read_bound=lambda: SimpleNamespace(committed_lsn=10))
    behind = IncrementalProjectionHealth("RUNNING", 0, 10, 0, 1)
    with (
        patch.object(worker, "health", return_value=behind),
        patch(
            "xuanling_tradecore.read_model.process_order_projector.monotonic_ns",
            side_effect=[0, 1, 1_000_000_001],
        ),
        patch("xuanling_tradecore.read_model.process_order_projector.sleep"),
    ):
        with pytest.raises(OrderProjectionError, match="deadline"):
            worker.drain(final_lsn=10, timeout_seconds=1)
    calls = iter([10, 11])
    worker._journal = SimpleNamespace(read_bound=lambda: SimpleNamespace(committed_lsn=next(calls)))
    with (
        patch.object(worker, "health", return_value=behind),
        patch("xuanling_tradecore.read_model.process_order_projector.sleep"),
    ):
        with pytest.raises(OrderProjectionError, match="quiescent"):
            worker.drain(final_lsn=10)
    worker._journal = SimpleNamespace(read_bound=lambda: SimpleNamespace(committed_lsn=10))
    for state in ("PAUSED", "FAILED", "STOPPED"):
        health = IncrementalProjectionHealth(state, 10, 10, 5, 1)
        with (
            patch.object(worker, "health", return_value=health),
            patch.object(worker, "resume") as resume,
        ):
            with pytest.raises(OrderProjectionError, match="unavailable"):
                worker.drain(final_lsn=10)
            resume.assert_not_called()
    for cut, seconds in ((True, 1), (-1, 1), (10, 0), (10, 31), (10, True)):
        with pytest.raises(OrderProjectionError, match="invalid"):
            worker.drain(final_lsn=cut, timeout_seconds=seconds)


def test_unexpected_child_death_is_sticky_failed_not_automatic_restart(tmp_path):
    journal, _, store, tail = create(tmp_path)
    store.close()
    tail.close()
    worker = ProcessOrderProjector(
        journal=journal, root=tmp_path / "projection", maximum_database_bytes=8 * 1024**3
    )
    try:
        worker.start()
        worker._process.terminate()
        worker._process.join(3)
        end = time.monotonic() + 3
        while worker.health().state != "FAILED" and time.monotonic() < end:
            time.sleep(0.01)
        assert worker.health().state == "FAILED"
        worker.resume()
        assert worker.health().state == "FAILED"
        with pytest.raises(OrderProjectionError):
            worker.start()
        assert journal.read_bound().committed_lsn == 0
    finally:
        worker.close()
        journal.close()
