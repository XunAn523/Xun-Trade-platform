from dataclasses import replace
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.oms.order_book import OmsOrderBookError
from xuanling_tradecore.oms.order_projection import (
    TransactionOrderProjection,
    reduce_order_projection,
)
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint

from test_order_projection_store import admit, create, retry


def test_transaction_book_matches_fresh_reducer_for_interleaved_lifecycle_and_conflict(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        first = admit(book, "a").order.order.oms_order_id
        second = admit(book, "b").order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=first))
        retry(lambda: book.apply_trade(oms_order_id=first, trade_key="partial", quantity=40))
        retry(lambda: book.reject(oms_order_id=second))
        retry(lambda: book.request_cancel(oms_order_id=first))
        retry(lambda: book.cancel(oms_order_id=first))
        admit(book, "a", quantity=200)  # no event, but complete conflict facts remain
        previous, reducers = {}, {}
        while (batch := tail.read_next()) is not None:
            facts = tuple(row.fact for row in batch.records)
            key = facts[0].oms_order_id
            expected = reduce_order_projection(facts, batch.commit, previous=previous.get(key))
            reducer = reducers.setdefault(
                key, TransactionOrderProjection(previous=None, maximum_order_events=64)
            )
            actual = reducer.reduce(facts, batch.commit)
            assert actual == expected
            previous[key] = expected.checkpoint
        assert len(reducers) == 2
    finally:
        tail.close()
        store.close()
        journal.close()


def test_same_transaction_does_not_reparse_seed_for_its_own_verified_state(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        retry(lambda: book.reject(oms_order_id=order_id))
        batches = tuple(iter(lambda: tail.read_next(), None))
        reducer = TransactionOrderProjection(previous=None, maximum_order_events=64)
        with patch.object(
            OmsRecoverySeedV1, "decode_and_verify", wraps=OmsRecoverySeedV1.decode_and_verify
        ) as checked:
            for batch in batches:
                reducer.reduce(tuple(row.fact for row in batch.records), batch.commit)
            assert checked.call_count == 1
        previous = None
        with patch.object(
            OmsRecoverySeedV1, "decode_and_verify", wraps=OmsRecoverySeedV1.decode_and_verify
        ) as checked:
            for batch in batches:
                previous = reduce_order_projection(
                    tuple(row.fact for row in batch.records), batch.commit, previous=previous
                ).checkpoint
            assert checked.call_count == 5
    finally:
        tail.close()
        store.close()
        journal.close()


def test_immutable_seed_encoding_reused_only_inside_one_transaction_book(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        retry(lambda: book.reject(oms_order_id=order_id))
        batches = tuple(iter(lambda: tail.read_next(), None))
        reducer = TransactionOrderProjection(previous=None, maximum_order_events=64)
        expected, previous = [], None
        for batch in batches:
            result = reduce_order_projection(
                tuple(row.fact for row in batch.records), batch.commit, previous=previous
            )
            expected.append(result)
            previous = result.checkpoint
        with patch.object(OmsRecoverySeedV1, "build", wraps=OmsRecoverySeedV1.build) as built:
            for batch, reference in zip(batches, expected):
                assert (
                    reducer.reduce(tuple(row.fact for row in batch.records), batch.commit)
                    == reference
                )
            # Original canonical re-build/comparison remains; checkpoint export
            # reuses those verified bytes rather than building them a second time.
            assert built.call_count == 1
        # A new transaction must encode again; there is no process/history cache.
        replacement = TransactionOrderProjection(previous=None, maximum_order_events=64)
        with patch.object(OmsRecoverySeedV1, "build", wraps=OmsRecoverySeedV1.build) as built:
            replacement.reduce(tuple(row.fact for row in batches[0].records), batches[0].commit)
            assert built.call_count == 1
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("wrong_scope", (None, "account", "order"))
def test_persisted_seed_verified_once_by_original_restore_then_bound_to_source(
    tmp_path, wrong_scope
):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        first, second = tail.read_next(), tail.read_next()
        previous = reduce_order_projection(
            tuple(row.fact for row in first.records), first.commit, previous=None
        ).checkpoint
        facts = tuple(row.fact for row in second.records)
        expected = reduce_order_projection(facts, second.commit, previous=previous)
        if wrong_scope is not None:
            field = "account_id" if wrong_scope == "account" else "oms_order_id"
            facts = tuple(replace(fact, **{field: "foreign"}) for fact in facts)
        candidate = TransactionOrderProjection(previous=previous, maximum_order_events=64)
        with patch.object(
            OmsRecoverySeedV1, "decode_and_verify", wraps=OmsRecoverySeedV1.decode_and_verify
        ) as checked:
            if wrong_scope is None:
                assert candidate.reduce(facts, second.commit) == expected
            else:
                with pytest.raises(OmsOrderBookError):
                    candidate.reduce(facts, second.commit)
                with pytest.raises(OmsOrderBookError, match="previously failed"):
                    candidate.reduce(tuple(row.fact for row in second.records), second.commit)
            assert checked.call_count == 1
        with patch.object(
            OmsRecoverySeedV1, "decode_and_verify", wraps=OmsRecoverySeedV1.decode_and_verify
        ) as checked:
            reduce_order_projection(
                tuple(row.fact for row in second.records), second.commit, previous=previous
            )
            assert checked.call_count == 2  # frozen reference, not a weaker restore
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("predecessor", (False, True))
def test_checkpoint_byte_reuse_never_primes_cache_before_original_seed_validation(
    tmp_path, predecessor
):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        first, second = tail.read_next(), tail.read_next()
        if predecessor:
            previous = reduce_order_projection(
                tuple(row.fact for row in first.records), first.commit, previous=None
            ).checkpoint
            state = decode_checkpoint(previous)
            state = msgspec.structs.replace(
                state, orders=(msgspec.structs.replace(state.orders[0], admission_seed=b"{}"),)
            )
            candidate = TransactionOrderProjection(
                previous=msgspec.json.encode(state), maximum_order_events=64
            )
            facts, commit = tuple(row.fact for row in second.records), second.commit
        else:
            candidate = TransactionOrderProjection(previous=None, maximum_order_events=64)
            facts = (replace(first.records[0].fact, recovery_payload=b"{}"), first.records[1].fact)
            commit = first.commit
        with pytest.raises(ValueError):
            candidate.reduce(facts, commit)
        assert candidate._book is None
        with pytest.raises(OmsOrderBookError, match="previously failed"):
            candidate.reduce(tuple(row.fact for row in first.records), first.commit)
    finally:
        tail.close()
        store.close()
        journal.close()


@pytest.mark.parametrize("fault", ["journal", "account", "execution", "repeated", "capacity"])
def test_bad_suffix_poisoned_without_cross_order_or_lifecycle_escape(tmp_path, fault):
    journal, book, store, tail = create(tmp_path)
    try:
        order_id = admit(book).order.order.oms_order_id
        retry(lambda: book.mark_working(oms_order_id=order_id))
        first, second = tail.read_next(), tail.read_next()
        reducer = TransactionOrderProjection(
            previous=None, maximum_order_events=1 if fault == "capacity" else 64
        )
        reducer.reduce(tuple(row.fact for row in first.records), first.commit)
        facts, commit = tuple(row.fact for row in second.records), second.commit
        if fault == "journal":
            commit = replace(commit, journal_id="wrong-journal")
        elif fault in ("account", "execution"):
            field = "account_id" if fault == "account" else "execution_id"
            facts = tuple(replace(fact, **{field: "wrong"}) for fact in facts)
        elif fault == "repeated":
            facts, commit = tuple(row.fact for row in first.records), first.commit
        with pytest.raises(OmsOrderBookError):
            reducer.reduce(facts, commit)
        with pytest.raises(OmsOrderBookError, match="previously failed"):
            reducer.reduce(tuple(row.fact for row in second.records), second.commit)
    finally:
        tail.close()
        store.close()
        journal.close()
