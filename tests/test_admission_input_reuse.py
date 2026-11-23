from dataclasses import replace
from itertools import count
from unittest.mock import patch

import pytest
from xuanling_tradecore.domain._validation import DomainValidationError
from xuanling_tradecore.oms import OrderBook, ReservationRequirement
from xuanling_tradecore.oms import retained_order_book as retained_module

from test_oms_committed_recovery import RecordingJournal
from test_retained_order_book import admit, retire, setup
from test_tradecore_oms import _bindings, _decision_context, _intent, _risk_snapshot


def test_retained_validates_once_and_hashes_exact_intent_before_account_lock(tmp_path):
    cold, _, book = setup(tmp_path)
    account = book._account("broker-account-1")
    original_hash = retained_module.intent_hash_for

    def outside_lock(intent):
        assert not account.lock._is_owned()
        return original_hash(intent)

    try:
        with (
            patch.object(book, "_validate_binding", wraps=book._validate_binding) as binding,
            patch.object(book, "_validate_risk_context", wraps=book._validate_risk_context) as risk,
            patch.object(retained_module, "intent_hash_for", side_effect=outside_lock) as hashing,
        ):
            for n in range(1, 4):
                admit(book)
                assert binding.call_count == risk.call_count == hashing.call_count == n
    finally:
        cold.close()


def test_reference_and_retained_emit_identical_accept_conflict_terminal_facts(tmp_path):
    cold, retained_journal, book = setup(tmp_path)
    source = RecordingJournal()
    reference = OrderBook(journal=source, persist_recovery_seed=True)
    for target in (reference, book):
        identifiers = count(1)
        target._new_id = lambda ids=identifiers: f"local-id-{next(ids)}"
    try:
        for quantity in (100, 100, 200):
            account, strategy, attribution = _bindings()
            expected = reference.admit(
                intent=_intent(strategy_order_id="1", quantity=quantity),
                account_binding=account,
                strategy_binding=strategy,
                attribution_binding=attribution,
                risk_snapshot=_risk_snapshot(),
                decision_context=_decision_context(),
                reservation_requirement=ReservationRequirement("SZ.000010", cash_minor=1000),
                now_monotonic_ns=20,
            )
            actual = admit(book, quantity=quantity)
            assert actual == expected
        order_id = expected.order.order.oms_order_id
        for target in (reference, book):
            target.apply_trade(oms_order_id=order_id, trade_key="first", quantity=40)
            target.request_cancel(oms_order_id=order_id)
            target.cancel(oms_order_id=order_id)
        assert source.batches == retained_journal.batches
        assert reference.order(order_id) == book.order(order_id)
        assert reference.reservations("broker-account-1") == book.reservations("broker-account-1")
    finally:
        cold.close()


def test_failed_journal_never_installs_hold_or_hot_identity(tmp_path):
    cold, journal, book = setup(tmp_path)
    try:
        with patch.object(journal, "append_batch", side_effect=OSError("test storage failure")):
            with pytest.raises(OSError):
                admit(book)
        assert journal.batches == []
        assert book.order_count("broker-account-1") == 0
        assert book.reservations("broker-account-1") == ()
        assert book._hot_accounts == book._histories == {}
    finally:
        cold.close()


@pytest.mark.parametrize("case", ("binding", "stale_risk", "decision", "symbol", "empty", "time"))
def test_cold_retry_still_requires_fresh_full_inputs_before_any_fact(tmp_path, case):
    cold, journal, book = setup(tmp_path)
    try:
        first = admit(book)
        book.reject(oms_order_id=first.order.order.oms_order_id)
        candidate, _ = retire(book, cold, first.order.order.oms_order_id)
        proof = cold.resolve(candidate.key).result(3)
        account, strategy, attribution = _bindings()
        arguments = dict(
            intent=_intent(strategy_order_id="1"),
            account_binding=account,
            strategy_binding=strategy,
            attribution_binding=attribution,
            risk_snapshot=_risk_snapshot(),
            decision_context=_decision_context(),
            reservation_requirement=ReservationRequirement("SZ.000010", cash_minor=1000),
            now_monotonic_ns=20,
            cold_resolution=proof,
        )
        if case == "binding":
            arguments["strategy_binding"] = replace(strategy, active=False)
        elif case == "stale_risk":
            arguments["now_monotonic_ns"] = 10**18
        elif case == "decision":
            arguments["decision_context"] = replace(_decision_context(), risk_snapshot_id="other")
        elif case == "symbol":
            arguments["reservation_requirement"] = ReservationRequirement("OTHER", cash_minor=1000)
        elif case == "empty":
            arguments["reservation_requirement"] = ReservationRequirement("SZ.000010")
        else:
            arguments["now_monotonic_ns"] = -1
        before = list(journal.batches)
        with pytest.raises(DomainValidationError):
            book.admit(**arguments)
        assert journal.batches == before
        assert book.reservations("broker-account-1") == ()
    finally:
        cold.close()
