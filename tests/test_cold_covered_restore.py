from __future__ import annotations

import msgspec
import pytest
from xuanling_tradecore.oms import OmsOrderBookError, OrderBook
from xuanling_tradecore.oms.cold_intent import ColdIntentIndexError
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint
from xuanling_tradecore.oms.retained_order_book import OmsHotCapacityFull, RetainedOrderBook
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionError

from test_oms_committed_recovery import RecordingJournal, admit
from test_order_projection_store import admit as projected_admit
from test_order_projection_store import create, drain


def active_state():
    source = RecordingJournal()
    live = OrderBook(journal=source, persist_recovery_seed=True)
    order_id = admit(live).order.order.oms_order_id
    live.mark_working(oms_order_id=order_id)
    live.apply_trade(oms_order_id=order_id, trade_key="fill", quantity=40)
    replay = OrderBook(journal=source, persist_recovery_seed=True)
    for facts, commit in source.batches:
        replay.restore_committed_batch(facts, commit)
    encoded = replay.export_recovery_checkpoint()
    return source, live, order_id, encoded, decode_checkpoint(encoded)


def test_startup_import_restores_preexisting_reservation_history_and_window_without_writes(
    tmp_path,
):
    source, original, order_id, encoded, state = active_state()
    cold = ColdIntentIndex(
        root=tmp_path / "cold", journal_id="stream", initialize=True, filter_bytes=64
    )
    book = RetainedOrderBook(journal=source, cold_index=cold, replay_cache_events=2)
    try:
        count = len(source.batches)
        book.restore_cold_covered_checkpoint(
            encoded,
            expected_journal_id="stream",
            event_counts={"broker-account-1": 3},
            recent_events=state.events[-2:],
        )
        assert book.order(order_id) == original.order(order_id)
        assert book.reservations("broker-account-1") == original.reservations("broker-account-1")
        assert book.last_order_event(order_id) == original.last_order_event(order_id)
        assert book.event_count("broker-account-1") == 3
        assert book.events_from("broker-account-1", 1) == original.events("broker-account-1")[-2:]
        assert len(source.batches) == count
        assert admit(book).order.order.oms_order_id == order_id
        assert len(source.batches) == count
        with pytest.raises(OmsOrderBookError):
            book.restore_cold_covered_checkpoint(
                encoded, expected_journal_id="stream", event_counts={}, recent_events=()
            )
    finally:
        cold.close()


@pytest.mark.parametrize("bad", ["terminal", "window", "counts", "scope"])
def test_invalid_restore_does_not_install_partial_accounts(tmp_path, bad):
    source, _, _, encoded, state = active_state()
    cold = ColdIntentIndex(
        root=tmp_path / "cold", journal_id="stream", initialize=True, filter_bytes=64
    )
    book = RetainedOrderBook(journal=source, cold_index=cold, replay_cache_events=2)
    counts, recent, scope = {"broker-account-1": 3}, state.events[-2:], "stream"
    if bad == "terminal":
        encoded = msgspec.json.encode(
            msgspec.structs.replace(
                state, orders=(msgspec.structs.replace(state.orders[0], state="CANCELED"),)
            )
        )
    elif bad == "window":
        recent = tuple(reversed(recent))
    elif bad == "counts":
        counts = {"broker-account-1": 0}
    else:
        scope = "foreign"
    try:
        with pytest.raises(OmsOrderBookError):
            book.restore_cold_covered_checkpoint(
                encoded, expected_journal_id=scope, event_counts=counts, recent_events=recent
            )
        assert not book._accounts
    finally:
        cold.close()


def test_account_capacity_stops_new_account_before_facts(tmp_path):
    cold = ColdIntentIndex(
        root=tmp_path / "cold", journal_id="stream", initialize=True, filter_bytes=64
    )
    source = RecordingJournal()
    book = RetainedOrderBook(journal=source, cold_index=cold, maximum_accounts=1)
    try:
        book.event_count("one")
        with pytest.raises(OmsHotCapacityFull):
            book.event_count("two")
        assert source.batches == []
    finally:
        cold.close()


def test_projection_recovery_pages_are_keyset_bounded_and_verify_counts(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        for n in range(5):
            projected_admit(book, str(n))
        drain(store, tail)
        assert store.recovery_counts() == (10, 5, 5)
        seen, last = [], ""
        while page := store.recovery_order_page(last, watermark=10, limit=2):
            assert len(page) <= 2
            seen.extend(order_id for order_id, _ in page)
            last = page[-1][0]
        assert seen == sorted(set(seen)) and len(seen) == 5
        with pytest.raises(OrderProjectionError):
            store.recovery_order_page("", watermark=11)
        store._db.execute("DELETE FROM orders WHERE oms_id=?", (seen[0],))
        store._db.commit()
        with pytest.raises(OrderProjectionError, match="count"):
            store.recovery_counts()
    finally:
        tail.close()
        store.close()
        journal.close()


def test_cold_database_budget_rejects_existing_oversize_without_reset(tmp_path):
    root = tmp_path / "cold"
    cold = ColdIntentIndex(root=root, journal_id="stream", initialize=True, filter_bytes=64)
    cold.close()
    path = root / "intents.sqlite"
    with path.open("ab") as output:
        output.write(b"\0" * 1024**2)
    before = path.stat().st_size
    with pytest.raises(ColdIntentIndexError, match="budget"):
        ColdIntentIndex(
            root=root, journal_id="stream", filter_bytes=64, maximum_database_bytes=1024**2
        )
    assert path.stat().st_size == before
