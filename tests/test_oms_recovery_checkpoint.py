from __future__ import annotations

from pathlib import Path

import msgspec
import pytest
from xuanling_tradecore.oms import IntentAdmissionOutcome, OmsOrderBookError, OrderBook
from xuanling_tradecore.oms.recovery_checkpoint import JournalCheckpointStore, decode_checkpoint

from test_oms_committed_recovery import RecordingJournal, admit


def replay(journal: RecordingJournal, count: int | None = None) -> OrderBook:
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    for facts, commit in journal.batches[:count]:
        book.restore_committed_batch(facts, commit)
    return book


def history() -> tuple[RecordingJournal, OrderBook, str]:
    journal = RecordingJournal()
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    order_id = admit(book).order.order.oms_order_id
    book.mark_working(oms_order_id=order_id)
    book.apply_trade(oms_order_id=order_id, trade_key="t1", quantity=40)
    book.request_cancel(oms_order_id=order_id)
    book.cancel(oms_order_id=order_id)
    return journal, book, order_id


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5])
def test_checkpoint_plus_suffix_matches_full_replay(count: int) -> None:
    journal, original, order_id = history()
    checkpoint = replay(journal, count).export_recovery_checkpoint()
    target = OrderBook(journal=journal, persist_recovery_seed=True)
    target.restore_recovery_checkpoint(checkpoint, expected_journal_id="stream")
    for batch in journal.batches[count:]:
        target.restore_committed_batch(*batch)
    assert target.order(order_id) == original.order(order_id)
    assert target.events("broker-account-1") == original.events("broker-account-1")
    assert target.reservations("broker-account-1") == original.reservations("broker-account-1")
    before = len(journal.batches)
    assert admit(target).outcome is IntentAdmissionOutcome.IDEMPOTENT
    assert len(journal.batches) == before


def test_restart_then_new_write_then_restart_again() -> None:
    journal, _, order_id = history()
    # Use an unfinished checkpoint and the same persistent Journal writer.
    journal.batches = journal.batches[:1]
    target = OrderBook(journal=journal, persist_recovery_seed=True)
    target.restore_recovery_checkpoint(
        replay(journal).export_recovery_checkpoint(), expected_journal_id="stream"
    )
    target.mark_working(oms_order_id=order_id)
    recovered = replay(journal)
    assert recovered.order(order_id) == target.order(order_id)


@pytest.mark.parametrize(
    "field,value",
    [
        ("filled_quantity", 100),
        ("filled_quantity", -1),
        ("order_revision", 99),
        ("reservation_revision", 99),
        ("trade_keys", ["t1", "t1"]),
        ("state", "ACCEPTED"),
    ],
)
def test_semantic_corruption_never_installs_partial_state(field: str, value: object) -> None:
    journal, _, _ = history()
    raw = msgspec.json.decode(replay(journal).export_recovery_checkpoint())
    raw["orders"][0][field] = value
    target = OrderBook(journal=journal, persist_recovery_seed=True)
    with pytest.raises(ValueError):
        target.restore_recovery_checkpoint(msgspec.json.encode(raw), expected_journal_id="stream")
    assert target.order_count("broker-account-1") == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_lsn", 0),
        ("event_id", "fake"),
        ("account_id", "other"),
        ("order_revision", 2),
        ("source_key", "invalid"),
    ],
)
def test_event_corruption_is_rejected(field: str, value: object) -> None:
    journal, _, _ = history()
    raw = msgspec.json.decode(replay(journal).export_recovery_checkpoint())
    raw["events"][0][field] = value
    with pytest.raises(ValueError):
        OrderBook(journal=journal, persist_recovery_seed=True).restore_recovery_checkpoint(
            msgspec.json.encode(raw), expected_journal_id="stream"
        )


def test_wrong_identity_unknown_schema_and_live_export_are_rejected() -> None:
    journal, live, _ = history()
    with pytest.raises(OmsOrderBookError, match="quiescent"):
        live.export_recovery_checkpoint()
    encoded = replay(journal).export_recovery_checkpoint()
    with pytest.raises(OmsOrderBookError, match="identity"):
        OrderBook(journal=journal, persist_recovery_seed=True).restore_recovery_checkpoint(
            encoded, expected_journal_id="other"
        )
    with pytest.raises(OmsOrderBookError):
        decode_checkpoint(encoded, maximum_bytes=1)
    raw = msgspec.json.decode(encoded)
    raw["unexpected"] = True
    with pytest.raises(OmsOrderBookError):
        decode_checkpoint(msgspec.json.encode(raw))


def test_store_keeps_verified_fallback_and_requires_contiguous_suffix(tmp_path: Path) -> None:
    journal, original, order_id = history()
    store = JournalCheckpointStore(tmp_path / "checkpoints")
    old = replay(journal, 2).export_recovery_checkpoint()
    store.publish(old)
    newest = store.publish(replay(journal, 4).export_recovery_checkpoint())
    newest.write_bytes(b"corrupt")
    assert store.load_latest(expected_journal_id="stream") == old
    target = OrderBook(journal=journal, persist_recovery_seed=True)
    target.restore_recovery_checkpoint(old, expected_journal_id="stream")
    with pytest.raises(OmsOrderBookError, match="position"):
        target.restore_committed_batch(*journal.batches[3])
    for batch in journal.batches[2:]:
        target.restore_committed_batch(*batch)
    assert target.order(order_id) == original.order(order_id)
    with pytest.raises(OmsOrderBookError):
        store.publish(old)
    with pytest.raises(OmsOrderBookError):
        store.load_latest(expected_journal_id="other")


def test_failed_publication_keeps_previous_generation(tmp_path: Path, monkeypatch) -> None:
    import xuanling_tradecore.oms.recovery_checkpoint as module

    journal, _, _ = history()
    store = JournalCheckpointStore(tmp_path / "checkpoints")
    first = replay(journal, 1).export_recovery_checkpoint()
    store.publish(first)

    def fail(*args, **kwargs):
        raise OSError("injected replace failure")

    monkeypatch.setattr(module, "atomic_runtime_json", fail)
    with pytest.raises(OSError):
        store.publish(replay(journal).export_recovery_checkpoint())
    assert store.load_latest(expected_journal_id="stream") == first
