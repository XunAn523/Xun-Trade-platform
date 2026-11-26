from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from xuanling_tradecore.domain import OmsOrderState
from xuanling_tradecore.oms import IntentAdmissionOutcome, OmsOrderBookError, ReservationRequirement
from xuanling_tradecore.oms.retained_order_book import (
    ColdIntentLookupRequired,
    OmsHistoryRequiresColdReader,
    OmsHotCapacityFull,
    RetainedOrderBook,
)
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex

from test_oms_committed_recovery import RecordingJournal
from test_tradecore_oms import _bindings, _decision_context, _intent, _risk_snapshot


def admit(book, sequence="1", quantity=100, resolution=None):
    account, strategy, attribution = _bindings()
    return book.admit(
        intent=_intent(strategy_order_id=sequence, quantity=quantity),
        account_binding=account,
        strategy_binding=strategy,
        attribution_binding=attribution,
        risk_snapshot=_risk_snapshot(),
        decision_context=_decision_context(),
        reservation_requirement=ReservationRequirement("SZ.000010", cash_minor=1000),
        now_monotonic_ns=20,
        cold_resolution=resolution,
    )


def setup(tmp_path: Path, **kwargs):
    cold = ColdIntentIndex(
        root=tmp_path / "cold", journal_id="stream", initialize=True, filter_bytes=64
    )
    journal = RecordingJournal()
    book = RetainedOrderBook(journal=journal, cold_index=cold, **kwargs)
    return cold, journal, book


def retire(book, cold, order_id):
    candidate = book.prepare_terminal_archive(order_id)
    receipt = cold.archive((candidate.encode(),)).result(timeout=5)
    book.retire_terminal(candidate, receipt)
    return candidate, receipt


def test_terminal_retirement_preserves_cold_retry_ids_and_conflict(tmp_path: Path) -> None:
    cold, journal, book = setup(tmp_path)
    try:
        first = admit(book)
        order_id = first.order.order.oms_order_id
        book.reject(oms_order_id=order_id)
        candidate, _ = retire(book, cold, order_id)
        assert book.order_count("broker-account-1") == 0
        assert book.reservations("broker-account-1") == ()
        before = len(journal.batches)
        with pytest.raises(ColdIntentLookupRequired) as lookup:
            admit(book)
        assert len(journal.batches) == before
        proof = cold.resolve(lookup.value.key).result(timeout=5)
        result = admit(book, resolution=proof)
        assert result.outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert result.order.order.oms_order_id == order_id
        assert result.replayed_event == candidate.events[-1]
        assert len(journal.batches) == before
        conflict = admit(book, quantity=200, resolution=proof)
        assert conflict.outcome is IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT
        assert len(journal.batches) == before + 1
        assert book.order_count("broker-account-1") == 0
    finally:
        cold.close()


def test_unfinished_cap_backpressures_before_fact_and_never_evicts(tmp_path: Path) -> None:
    cold, journal, book = setup(tmp_path, maximum_hot_orders=1)
    try:
        first = admit(book)
        with pytest.raises(ValueError, match="active"):
            book.prepare_terminal_archive(first.order.order.oms_order_id)
        before = len(journal.batches)
        with pytest.raises(OmsHotCapacityFull):
            admit(book, "2")
        assert len(journal.batches) == before
        assert book.order_count("broker-account-1") == 1
        assert admit(book).outcome is IntentAdmissionOutcome.IDEMPOTENT
        book.reject(oms_order_id=first.order.order.oms_order_id)
        retire(book, cold, first.order.order.oms_order_id)
        assert admit(book, "2").outcome is IntentAdmissionOutcome.ACCEPTED
    finally:
        cold.close()


def test_tampered_unpublished_handoff_rejected_but_immutable_commit_survives_append(
    tmp_path: Path,
) -> None:
    cold, _, book = setup(tmp_path)
    try:
        first = admit(book)
        book.reject(oms_order_id=first.order.order.oms_order_id)
        candidate = book.prepare_terminal_archive(first.order.order.oms_order_id)
        receipt = cold.archive((candidate.encode(),)).result(timeout=5)
        with pytest.raises(ColdIntentLookupRequired):
            book.retire_terminal(candidate, replace(receipt, digests=(b"fake",)))
        with pytest.raises(ColdIntentLookupRequired):
            book.retire_terminal(candidate, replace(receipt, generation=receipt.generation + 1))
        assert book.order_count("broker-account-1") == 1
        cold.archive((candidate.encode(),)).result(timeout=5)
        # Later append-only FULL commits do not overwrite this exact row.
        book.retire_terminal(candidate, receipt)
        assert book.order_count("broker-account-1") == 0
    finally:
        cold.close()


def test_stale_negative_or_forged_lookup_cannot_admit(tmp_path: Path) -> None:
    cold, journal, book = setup(tmp_path)
    try:
        first = admit(book)
        book.reject(oms_order_id=first.order.order.oms_order_id)
        candidate, _ = retire(book, cold, first.order.order.oms_order_id)
        proof = cold.resolve(candidate.key).result(timeout=5)
        before = len(journal.batches)
        with pytest.raises(ColdIntentLookupRequired):
            admit(book, resolution=replace(proof, checkpoint=None))
        cold.archive((candidate.encode(),)).result(timeout=5)
        assert not cold.validate(proof)  # lookup semantics still demand CURRENT generation
        assert len(journal.batches) == before
    finally:
        cold.close()


def test_bulk_retirement_validates_immutable_receipt_once_and_all_payloads(tmp_path, monkeypatch):
    cold, _, book = setup(tmp_path)
    candidates = []
    for ref in ("bulk-a", "bulk-b", "bulk-c"):
        result = admit(book, ref)
        book.reject(oms_order_id=result.order.order.oms_order_id)
        candidates.append(book.prepare_terminal_archive(result.order.order.oms_order_id))
    values = tuple(candidates)
    try:
        receipt = cold.archive(tuple(c.encode() for c in values)).result(timeout=5)
        calls = []
        validate = cold.validate
        monkeypatch.setattr(cold, "validate", lambda r: calls.append(1) or validate(r))
        with pytest.raises(OmsOrderBookError):
            book.retire_terminals((values[0], values[0]), receipt)
        assert book.order_count("broker-account-1") == 3
        calls.clear()
        book.retire_terminals(values, receipt)
        assert len(calls) == 1 and book.order_count("broker-account-1") == 0
        for candidate in values:
            proof = cold.resolve(candidate.key).result(timeout=5)
            replay = admit(book, candidate.key.strategy_order_id, resolution=proof)
            assert replay.outcome is IntentAdmissionOutcome.IDEMPOTENT
            assert replay.order.order.oms_order_id == candidate.current.order.oms_order_id
    finally:
        cold.close()


def test_terminal_candidate_reuses_only_its_immutable_encoding(tmp_path):
    from unittest.mock import patch

    cold, _, book = setup(tmp_path)
    try:
        result = admit(book)
        book.reject(oms_order_id=result.order.order.oms_order_id)
        candidate = book.prepare_terminal_archive(result.order.order.oms_order_id)
        encoded = candidate.encode()
        copied = replace(candidate)
        assert copied._encoded is None
        with patch(
            "xuanling_tradecore.oms.retained_order_book.OmsRecoverySeedV1.build",
            side_effect=AssertionError("duplicate seed"),
        ):
            assert candidate.encode() is encoded
            with pytest.raises(AssertionError):
                copied.encode()
        receipt = cold.archive((encoded,)).result(timeout=5)
        book.retire_terminal(candidate, receipt)
        assert book.order_count("broker-account-1") == 0
    finally:
        cold.close()


def test_repeated_lifecycle_keeps_orders_reservations_histories_and_window_bounded(
    tmp_path: Path,
) -> None:
    cold, _, book = setup(tmp_path, maximum_hot_orders=4, replay_cache_events=8)
    try:
        for n in range(100):
            # A saturated filter intentionally exercises exact negative resolution.
            try:
                result = admit(book, str(n))
            except ColdIntentLookupRequired as lookup:
                result = admit(book, str(n), resolution=cold.resolve(lookup.key).result(timeout=5))
            order_id = result.order.order.oms_order_id
            book.reject(oms_order_id=order_id)
            retire(book, cold, order_id)
            assert book.order_count("broker-account-1") == 0
            assert book.reservations("broker-account-1") == ()
            assert len(book._histories) == 0
        assert book.event_count("broker-account-1") == 200
        assert len(book.events_from("broker-account-1", 192)) == 8
        with pytest.raises(OmsHistoryRequiresColdReader):
            book.events_from("broker-account-1", 191)
        assert cold.stats().records == 100
    finally:
        cold.close()


def test_order_event_cap_fails_before_commit(tmp_path: Path) -> None:
    cold, journal, book = setup(tmp_path, maximum_order_events=4)
    try:
        first = admit(book)
        book.mark_working(oms_order_id=first.order.order.oms_order_id)
        before = len(journal.batches)
        with pytest.raises(OmsHotCapacityFull):
            book.apply_trade(oms_order_id=first.order.order.oms_order_id, trade_key="t", quantity=1)
        assert len(journal.batches) == before
        assert book.order(first.order.order.oms_order_id).filled_quantity == 0
        book.request_cancel(oms_order_id=first.order.order.oms_order_id)
        book.cancel(oms_order_id=first.order.order.oms_order_id)
        retire(book, cold, first.order.order.oms_order_id)
        assert book.reservations("broker-account-1") == ()
    finally:
        cold.close()


def test_retained_cancel_preserves_request_identity_and_reservation_until_confirmation(tmp_path):
    cold, journal, book = setup(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        book.mark_working(oms_order_id=order_id)
        event = book.request_cancel(
            oms_order_id=order_id, cancel_request_id="cancel-1", reason="strategy requested"
        )
        assert event.order_state is OmsOrderState.CANCEL_PENDING
        assert book.order(order_id).reservation.is_active
        for fact in journal.batches[-1][0]:
            assert fact.attributes["cancel_request_id"] == "cancel-1"
            assert fact.attributes["reason"] == "strategy requested"
        before = len(journal.batches)
        assert (
            book.request_cancel(
                oms_order_id=order_id, cancel_request_id="cancel-1", reason="strategy requested"
            )
            is None
        )
        assert len(journal.batches) == before
        book.cancel(oms_order_id=order_id)
        assert not book.order(order_id).reservation.is_active
        retire(book, cold, order_id)
    finally:
        cold.close()


def test_concurrent_same_key_creates_one_order_and_one_reservation(tmp_path: Path) -> None:
    cold, journal, book = setup(tmp_path)
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(lambda _: admit(book), range(100)))
        assert sum(r.outcome is IntentAdmissionOutcome.ACCEPTED for r in results) == 1
        assert len({r.order.order.oms_order_id for r in results}) == 1
        assert len(journal.batches) == 1
        assert len(book.reservations("broker-account-1")) == 1
    finally:
        cold.close()
