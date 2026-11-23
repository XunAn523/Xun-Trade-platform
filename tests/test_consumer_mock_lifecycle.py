from __future__ import annotations

import time
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.mock_lifecycle import (
    MockLifecycleError,
    MockOrderLifecycle,
    _lower_mock_thread_priority,
    mock_sequence,
)
from xuanling_tradecore.oms import IntentAdmissionOutcome, OmsOrderBookError
from xuanling_tradecore.oms.retained_order_book import ColdIntentLookupRequired

from test_retained_order_book import admit, setup


def wait(worker, predicate):
    end = time.monotonic() + 5
    while time.monotonic() < end:
        health = worker.health()
        if predicate(health):
            return health
        if health.state == "FAILED":
            pytest.fail(str(health))
        time.sleep(0.001)
    pytest.fail("mock worker made no expected progress")


@pytest.mark.parametrize("yield_every", [0, 1, 2])
def test_no_notification_required_and_terminal_archive_releases_every_reserve(tmp_path, yield_every):
    cold, journal, book = setup(tmp_path, maximum_hot_orders=10)
    worker = MockOrderLifecycle(
        book=book, cold=cold, batch_size=4, yield_every=yield_every,
        timing_diagnostics=bool(yield_every),
    )
    try:
        orders = [admit(book, f"LR-{n}") for n in range(10)]
        before = len(journal.batches)
        with patch("xuanling_tradecore.consumer.mock_lifecycle.monotonic_ns", return_value=30):
            worker.start()
            health = wait(worker, lambda h: h.retired == 10)
        assert (health.filled, health.rejected, health.native_calls) == (8, 2, 0)
        assert len(journal.batches) == before + 10
        assert book.reservations("broker-account-1") == ()
        assert book.order_count("broker-account-1") == 0
        assert book.maintenance_order_ids() == ()
        for n, previous in enumerate(orders):
            with pytest.raises(ColdIntentLookupRequired) as lookup:
                admit(book, f"LR-{n}")
            proof = cold.resolve(lookup.value.key).result(timeout=5)
            retry = admit(book, f"LR-{n}", resolution=proof)
            assert retry.outcome is IntentAdmissionOutcome.IDEMPOTENT
            assert retry.order.order.oms_order_id == previous.order.order.oms_order_id
            assert retry.replayed_event.order_state.value == (
                "REJECTED" if n % 5 == 4 else "FILLED"
            )
        worker.close()
        evidence = worker.timing_evidence()
        if yield_every:
            assert evidence is not None and evidence["complete"] is True
            assert evidence["total"] >= 2
            assert all(row[0] in {"terminal", "retire"} for row in evidence["intervals"])
        else:
            assert evidence is None
    finally:
        worker.close()
        cold.close()


def test_mock_cooperative_yield_budget_is_bounded(tmp_path):
    cold, _, book = setup(tmp_path)
    try:
        for invalid in (True, -1, 129, 1.5):
            with pytest.raises(MockLifecycleError):
                MockOrderLifecycle(book=book, cold=cold, yield_every=invalid)
        for invalid in (True, -1, 6, 1.5):
            with pytest.raises(MockLifecycleError):
                MockOrderLifecycle(book=book, cold=cold, nice_increment=invalid)
    finally:
        cold.close()


def test_mock_nice_adjustment_affects_only_current_thread_and_never_increases_priority():
    module = "xuanling_tradecore.consumer.mock_lifecycle"
    with (
        patch(module + ".sys.platform", "linux"),
        patch(module + ".os.PRIO_PROCESS", 0, create=True),
        patch(module + ".os.getpriority", side_effect=[0, 3], create=True),
        patch(module + ".os.setpriority", create=True) as set_priority,
    ):
        _lower_mock_thread_priority(3)
        set_priority.assert_called_once_with(0, 0, 3)


@pytest.mark.parametrize("bad", ["", "LR-", "LR--1", "LR-１", "123", "LR-" + "1" * 25])
def test_mock_sequence_contract_is_explicit_and_bounded(bad):
    with pytest.raises(MockLifecycleError):
        mock_sequence(bad)


def test_bad_profile_reference_poisoned_owner_never_evicts_active_order(tmp_path):
    cold, _, book = setup(tmp_path)
    worker = MockOrderLifecycle(book=book, cold=cold)
    try:
        order = admit(book, "not-mock")
        worker.start()
        end = time.monotonic() + 5
        while worker.health().state != "FAILED" and time.monotonic() < end:
            time.sleep(0.001)
        assert worker.health().state == "FAILED"
        assert book.order(order.order.order.oms_order_id).reservation.is_active
        assert book.order_count("broker-account-1") == 1
        with pytest.raises(MockLifecycleError):
            worker.start()
        for limit in (True, 0, 129):
            with pytest.raises(OmsOrderBookError):
                book.maintenance_order_ids(limit=limit)
    finally:
        worker.close()
        cold.close()


def test_archive_failure_retains_committed_terminal_for_recovery(tmp_path):
    cold, journal, book = setup(tmp_path)
    worker = MockOrderLifecycle(book=book, cold=cold)
    order = admit(book, "LR-4")
    try:
        with (
            patch("xuanling_tradecore.consumer.mock_lifecycle.monotonic_ns", return_value=30),
            patch.object(cold, "archive", side_effect=OSError("private failure")),
        ):
            worker.start()
            end = time.monotonic() + 5
            while worker.health().state != "FAILED" and time.monotonic() < end:
                time.sleep(0.001)
            assert worker.health().state == "FAILED"
        current = book.order(order.order.order.oms_order_id)
        assert current.order.state.value == "REJECTED"
        assert not current.reservation.is_active
        assert len(journal.batches) == 2
        assert "private" not in str(worker.health())
    finally:
        worker.close()
        cold.close()


def test_terminal_deadline_failure_retains_actual_elapsed_and_source_state(tmp_path):
    cold, journal, book = setup(tmp_path)
    worker = MockOrderLifecycle(book=book, cold=cold, terminal_deadline_ns=1)
    order = admit(book, "LR-4")
    try:
        with patch("xuanling_tradecore.consumer.mock_lifecycle.monotonic_ns", return_value=10000):
            worker.start()
            end = time.monotonic() + 3
            while worker.health().state != "FAILED" and time.monotonic() < end:
                time.sleep(0.001)
            health = worker.health()
            assert health.error_code == "MOCK_TERMINAL_DEADLINE"
            assert health.max_terminal_ns > 1
        assert book.order_count("broker-account-1") == 1
        assert book.order(order.order.order.oms_order_id).order.state.value == "REJECTED"
        assert len(journal.batches) == 2 and cold.stats().records == 0
    finally:
        worker.close()
        cold.close()


def test_archive_pipeline_is_bounded_and_never_retires_before_real_receipt(tmp_path):
    from concurrent.futures import Future

    cold, _, book = setup(tmp_path, maximum_hot_orders=8)
    worker = MockOrderLifecycle(book=book, cold=cold, batch_size=2, maximum_pending_archives=2)
    orders = [admit(book, f"LR-{n}") for n in range(8)]
    real_archive = cold.archive
    held = []

    def blocked(checkpoints):
        future = Future()
        held.append((future, real_archive(checkpoints)))
        return future

    try:
        with (
            patch("xuanling_tradecore.consumer.mock_lifecycle.monotonic_ns", return_value=30),
            patch.object(cold, "archive", side_effect=blocked),
        ):
            worker.start()
            health = wait(worker, lambda h: h.pending_archives == 2)
            assert health.retired == 0 and health.peak_pending_archives == 2
            assert health.filled + health.rejected == 4
            assert book.order_count("broker-account-1") == 8
            assert (
                sum(book.order(o.order.order.oms_order_id).reservation.is_active for o in orders)
                == 4
            )
            for _ in range(4):
                for outer, inner in list(held):
                    if not outer.done():
                        outer.set_result(inner.result(timeout=5))
                time.sleep(0.02)
            health = wait(worker, lambda h: h.retired == 8)
            assert health.pending_archives == 0 and health.peak_pending_archives <= 2
            assert book.order_count("broker-account-1") == 0
    finally:
        for outer, inner in list(held):
            if not outer.done():
                outer.set_result(inner.result(timeout=5))
        worker.close()
        cold.close()
