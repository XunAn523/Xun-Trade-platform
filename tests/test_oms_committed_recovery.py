from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from xuanling_tradecore.oms import (
    IntentAdmissionOutcome,
    MmapOmsFactJournalError,
    MmapOmsFactJournalV1,
    OmsJournalCommit,
    OmsJournalFact,
    OmsOrderBookError,
    OrderBook,
    ReservationRequirement,
)
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal

from test_tradecore_oms import _bindings, _decision_context, _intent, _risk_snapshot


class RecordingJournal:
    def __init__(self) -> None:
        self.batches: list[tuple[tuple[OmsJournalFact, ...], OmsJournalCommit]] = []

    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        start = 1 + sum(commit.fact_count for _, commit in self.batches)
        commit = OmsJournalCommit("stream", start, start + len(facts) - 1, len(facts))
        self.batches.append((facts, commit))
        return commit


def admit(book: OrderBook, quantity: int = 100):
    account, strategy, attribution = _bindings()
    return book.admit(
        intent=_intent(quantity=quantity),
        account_binding=account,
        strategy_binding=strategy,
        attribution_binding=attribution,
        risk_snapshot=_risk_snapshot(),
        decision_context=_decision_context(),
        reservation_requirement=ReservationRequirement("SZ.000010", cash_minor=1000),
        now_monotonic_ns=20,
    )


def test_restart_restores_full_lifecycle_ids_and_reservations_without_writes() -> None:
    source = RecordingJournal()
    original = OrderBook(journal=source, persist_recovery_seed=True)
    first = admit(original)
    order_id = first.order.order.oms_order_id
    original.mark_working(oms_order_id=order_id)
    original.apply_trade(oms_order_id=order_id, trade_key="trade-1", quantity=40)
    original.request_cancel(oms_order_id=order_id)
    original.cancel(oms_order_id=order_id)
    assert admit(original, quantity=200).outcome is IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT
    target = RecordingJournal()
    recovered = OrderBook(journal=target, persist_recovery_seed=True)
    for facts, commit in source.batches:
        recovered.restore_committed_batch(facts, commit)
    assert recovered.order(order_id) == original.order(order_id)
    assert recovered.reservations("broker-account-1") == original.reservations("broker-account-1")
    assert recovered.events("broker-account-1") == original.events("broker-account-1")
    retry = admit(recovered)
    assert retry.outcome is IntentAdmissionOutcome.IDEMPOTENT
    assert retry.order.order.oms_order_id == order_id
    assert target.batches == []
    with pytest.raises(OmsOrderBookError, match="lifecycle"):
        recovered.restore_committed_batch(*source.batches[0])


@pytest.mark.parametrize("tamper", ["seed", "reservation", "scope", "gap"])
def test_invalid_recovery_batch_does_not_create_any_order(tamper: str) -> None:
    source = RecordingJournal()
    book = OrderBook(journal=source, persist_recovery_seed=True)
    admit(book)
    facts, commit = source.batches[0]
    if tamper == "seed":
        facts = (replace(facts[0], recovery_payload=None), facts[1])
    elif tamper == "reservation":
        facts = (facts[0], replace(facts[1], attributes={**facts[1].attributes, "cash_minor": "0"}))
    elif tamper == "scope":
        facts = (replace(facts[0], account_id="other"), facts[1])
    else:
        commit = OmsJournalCommit("stream", 2, 3, 2)
    target = RecordingJournal()
    recovered = OrderBook(journal=target, persist_recovery_seed=True)
    with pytest.raises(OmsOrderBookError):
        recovered.restore_committed_batch(facts, commit)
    assert recovered.order_count("broker-account-1") == 0
    assert recovered.reservations("broker-account-1") == ()
    assert target.batches == []


def test_trade_replay_checks_resource_facts_before_state_mutation() -> None:
    source = RecordingJournal()
    original = OrderBook(journal=source, persist_recovery_seed=True)
    first = admit(original)
    order_id = first.order.order.oms_order_id
    original.apply_trade(oms_order_id=order_id, trade_key="trade", quantity=50)
    recovered = OrderBook(journal=RecordingJournal(), persist_recovery_seed=True)
    recovered.restore_committed_batch(*source.batches[0])
    before = recovered.order(order_id)
    facts, commit = source.batches[1]
    damaged = (facts[0], replace(facts[1], attributes={**facts[1].attributes, "cash_minor": "1"}))
    with pytest.raises(OmsOrderBookError, match="reservation"):
        recovered.restore_committed_batch(damaged, commit)
    assert recovered.order(order_id) == before
    recovered.restore_committed_batch(facts, commit)
    assert recovered.order(order_id) == original.order(order_id)


def test_cancel_request_metadata_survives_replay_and_incomplete_metadata_is_rejected() -> None:
    source = RecordingJournal()
    original = OrderBook(journal=source, persist_recovery_seed=True)
    order_id = admit(original).order.order.oms_order_id
    original.request_cancel(
        oms_order_id=order_id, cancel_request_id="cancel-1", reason="strategy cleanup"
    )
    facts, receipt = source.batches[1]
    assert facts[0].attributes["cancel_request_id"] == "cancel-1"
    assert facts[0].attributes["reason"] == "strategy cleanup"
    recovered = OrderBook(journal=RecordingJournal(), persist_recovery_seed=True)
    recovered.restore_committed_batch(*source.batches[0])
    before = recovered.order(order_id)
    malformed_attrs = {"from": facts[0].attributes["from"], "to": "CANCEL_PENDING", "reason": "x"}
    malformed = tuple(replace(fact, attributes=malformed_attrs) for fact in facts)
    with pytest.raises(OmsOrderBookError, match="metadata"):
        recovered.restore_committed_batch(malformed, receipt)
    assert recovered.order(order_id) == before
    recovered.restore_committed_batch(facts, receipt)
    assert recovered.order(order_id) == original.order(order_id)


def test_canonical_seed_fits_existing_frame_and_survives_disk_restart(tmp_path: Path) -> None:
    path = tmp_path / "facts.mmap"
    journal = MmapOmsFactJournalV1(path=path, journal_id="stream", capacity=8)
    book = OrderBook(journal=journal, persist_recovery_seed=True)
    first = admit(book)
    journal.close()
    reopened = MmapOmsFactJournalV1(path=path, journal_id="stream", capacity=8)
    try:
        records = reopened.scan().records
        assert records[0].fact.recovery_payload is not None
        recovered = OrderBook(journal=reopened, persist_recovery_seed=True)
        recovered.restore_committed_batch(
            tuple(r.fact for r in records), OmsJournalCommit("stream", 1, 2, 2)
        )
        assert admit(recovered).order.order.oms_order_id == first.order.order.oms_order_id
        assert len(reopened.scan().records) == 2
    finally:
        reopened.close()


def test_bad_recovery_payload_does_not_poison_segmented_writer(tmp_path: Path) -> None:
    journal = SegmentedOmsFactJournal(
        root=tmp_path / "journal", journal_id="stream", segment_frames=4
    )
    source = RecordingJournal()
    admit(OrderBook(journal=source, persist_recovery_seed=True))
    fact = source.batches[0][0][0]
    try:
        for payload in [b"{", b"[]", b'{"too_large":"' + b"x" * 5000 + b'"}']:
            with pytest.raises(MmapOmsFactJournalError):
                journal.append_batch((replace(fact, recovery_payload=payload),))
        assert not journal.stats().maintenance_failed
        assert journal.append_batch((fact,)).first_lsn == 1
    finally:
        journal.close()
