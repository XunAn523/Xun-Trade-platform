from dataclasses import replace

import pytest
from xuanling_tradecore.consumer import OrderBookConsumerOperationsV2
from xuanling_tradecore.consumer.emt_committed_admission_v1 import (
    EmtCommittedAdmissionErrorV1,
    verify_emt_committed_admission_v1,
)
from xuanling_tradecore.consumer.gateway_v2 import ConsumerGatewayConfigurationError
from xuanling_tradecore.oms import MmapOmsFactJournalV1, OmsJournalCommit, OrderBook

from test_consumer_oms_operations_v2 import ConsumerOmsOperationsV2Tests
from test_mmap_oms_fact_journal_v1 import _fact
from test_oms_committed_recovery import admit


def test_revalidates_real_committed_batch_and_keeps_original_positions(tmp_path):
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        journal.append_batch((_fact("prior"),))
        admission = admit(OrderBook(journal=journal, persist_recovery_seed=True))
        assert admission.journal_commit is not None
        evidence = verify_emt_committed_admission_v1(
            journal=journal, commit=admission.journal_commit, expected=admission.order
        )
        assert evidence.commit.first_lsn == 2
        assert evidence.commit.final_lsn == 3
        assert evidence.seed.order == admission.order
        assert evidence.facts[0].recovery_payload == evidence.seed.encoded
    finally:
        journal.close()


def test_rejects_benchmark_journal_without_recovery_seed(tmp_path):
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        admission = admit(OrderBook(journal=journal, persist_recovery_seed=False))
        assert admission.journal_commit is not None
        with pytest.raises(EmtCommittedAdmissionErrorV1, match="recovery evidence"):
            verify_emt_committed_admission_v1(
                journal=journal, commit=admission.journal_commit, expected=admission.order
            )
    finally:
        journal.close()


def test_rejects_foreign_receipt_and_different_in_memory_order(tmp_path):
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        admission = admit(OrderBook(journal=journal, persist_recovery_seed=True))
        assert admission.journal_commit is not None
        with pytest.raises(EmtCommittedAdmissionErrorV1, match="verification failed"):
            verify_emt_committed_admission_v1(
                journal=journal,
                commit=OmsJournalCommit("foreign", 1, 2, 2),
                expected=admission.order,
            )
        with pytest.raises(EmtCommittedAdmissionErrorV1, match="does not match"):
            verify_emt_committed_admission_v1(
                journal=journal,
                commit=admission.journal_commit,
                expected=replace(admission.order, filled_quantity=1),
            )
    finally:
        journal.close()


def test_valid_crc_cannot_hide_inconsistent_reservation_fact(tmp_path):
    source = MmapOmsFactJournalV1(path=tmp_path / "source", journal_id="source", capacity=8)
    altered = MmapOmsFactJournalV1(path=tmp_path / "altered", journal_id="altered", capacity=8)
    try:
        admission = admit(OrderBook(journal=source, persist_recovery_seed=True))
        assert admission.journal_commit is not None
        first, second = (row.fact for row in source.read_committed_batch(admission.journal_commit))
        damaged = replace(second, attributes={**second.attributes, "cash_minor": "0"})
        receipt = altered.append_batch((first, damaged))
        with pytest.raises(EmtCommittedAdmissionErrorV1, match="verification failed"):
            verify_emt_committed_admission_v1(
                journal=altered, commit=receipt, expected=admission.order
            )
    finally:
        source.close()
        altered.close()


def test_post_commit_wakeup_contains_original_receipt_and_runs_once(tmp_path):
    fixture = ConsumerOmsOperationsV2Tests()
    fixture.setUp()
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        wakes, fences = [], []
        operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=journal, persist_recovery_seed=True),
            admission_context_for=lambda *_: fixture._context(),
            now_monotonic_ns=lambda: 20,
            on_execution_commit_evidence=lambda order, commit: wakes.append((order, commit)),
            on_execution_wakeup_failed=lambda: fences.append(True),
        )
        receipt = operations.submit_signal(
            session=fixture.session,
            account_binding=fixture.account,
            strategy_binding_id="strategy-bind",
            signal=fixture._signal(),
        )
        operations.submit_signal(
            session=fixture.session,
            account_binding=fixture.account,
            strategy_binding_id="strategy-bind",
            signal=fixture._signal("retry"),
        )
        assert len(wakes) == 1 and fences == []
        order, commit = wakes[0]
        assert (receipt.journal_id, receipt.journal_lsn) == (commit.journal_id, commit.final_lsn)
        assert (
            verify_emt_committed_admission_v1(
                journal=journal, commit=commit, expected=order
            ).seed.order
            == order
        )
    finally:
        journal.close()


def test_failed_wakeup_requests_fence_without_erasing_committed_order(tmp_path):
    fixture = ConsumerOmsOperationsV2Tests()
    fixture.setUp()
    journal = MmapOmsFactJournalV1(path=tmp_path / "journal", journal_id="j", capacity=8)
    try:
        fences = []

        def broken_wakeup(_order, _commit):
            raise OSError("injected")

        operations = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=journal, persist_recovery_seed=True),
            admission_context_for=lambda *_: fixture._context(),
            now_monotonic_ns=lambda: 20,
            on_execution_commit_evidence=broken_wakeup,
            on_execution_wakeup_failed=lambda: fences.append(True),
        )
        receipt = operations.submit_signal(
            session=fixture.session,
            account_binding=fixture.account,
            strategy_binding_id="strategy-bind",
            signal=fixture._signal(),
        )
        assert fences == [True]
        assert operations.get_order(
            account_binding=fixture.account, oms_order_id=receipt.oms_order_id
        )
        assert journal.scan().clean

        def broken_fence():
            raise OSError("fence failed")

        broken = OrderBookConsumerOperationsV2(
            order_book=OrderBook(journal=journal, persist_recovery_seed=True),
            admission_context_for=lambda *_: fixture._context(),
            now_monotonic_ns=lambda: 20,
            on_execution_commit_evidence=broken_wakeup,
            on_execution_wakeup_failed=broken_fence,
        )
        with pytest.raises(ConsumerGatewayConfigurationError, match="closure is unconfirmed"):
            broken.submit_signal(
                session=fixture.session,
                account_binding=fixture.account,
                strategy_binding_id="strategy-bind",
                signal=fixture._signal(),
            )
    finally:
        journal.close()


def test_execution_evidence_wakeup_requires_exactly_one_fenced_receiver():
    fixture = ConsumerOmsOperationsV2Tests()
    fixture.setUp()
    options = {
        "order_book": fixture.operations._order_book,
        "admission_context_for": lambda *_: fixture._context(),
    }
    with pytest.raises(ConsumerGatewayConfigurationError, match="failure fence"):
        OrderBookConsumerOperationsV2(
            **options, on_execution_commit_evidence=lambda _order, _commit: None
        )
    with pytest.raises(ConsumerGatewayConfigurationError, match="one wake-up"):
        OrderBookConsumerOperationsV2(
            **options,
            on_execution_committed=lambda _order: None,
            on_execution_commit_evidence=lambda _order, _commit: None,
            on_execution_wakeup_failed=lambda: None,
        )
    with pytest.raises(ConsumerGatewayConfigurationError, match="no evidence"):
        OrderBookConsumerOperationsV2(**options, on_execution_wakeup_failed=lambda: None)
