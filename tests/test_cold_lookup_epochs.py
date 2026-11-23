import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from unittest.mock import patch

import pytest
from xuanling_tradecore.oms import IntentAdmissionOutcome
from xuanling_tradecore.oms.cold_intent import ColdIntentIndexError, ColdIntentKey
from xuanling_tradecore.oms.retained_order_book import ColdIntentLookupRequired, RetainedOrderBook

from test_cold_intent_index import checkpoint, index
from test_oms_committed_recovery import RecordingJournal
from test_retained_order_book import admit


def stripe(store, key):
    return store._bits(key)[0] % 65_536


def other_key(store, archived, *, collision=False):
    return next(
        ColdIntentKey(archived.account_id, archived.strategy_id, f"new-{n}")
        for n in range(20_000)
        if (
            stripe(store, ColdIntentKey(archived.account_id, archived.strategy_id, f"new-{n}"))
            == stripe(store, archived)
        )
        is collision
    )


def test_unrelated_generations_do_not_revoke_exact_miss_but_collision_does(tmp_path):
    key, payload = checkpoint()
    store = index(tmp_path / "index", scoped_lookup_epochs=True)
    try:
        absent, colliding = other_key(store, key), other_key(store, key, collision=True)
        store._filter[:] = b"\xff" * 64  # require real SQL even for an absent key
        proof = store.resolve(absent).result(3)
        collision = store.resolve(colliding).result(3)
        assert proof.checkpoint is collision.checkpoint is None
        for _ in range(20):
            store.archive((payload,)).result(3)
            assert store.validate(proof)
        assert proof.generation == 0 and store.stats().generation == 20
        assert not store.validate(collision)
        assert len(store._lookup_epochs) == 65_536
        assert store.stats().lookup_epoch_bytes == 524_288
        assert store.stats().stale_lookup_proofs == 1
    finally:
        store.close()


def test_same_key_and_every_signed_field_cannot_be_forged_or_reused_after_restart(tmp_path):
    key, payload = checkpoint()
    root = tmp_path / "index"
    store = index(root, scoped_lookup_epochs=True)
    try:
        negative = store.probe(key)
        assert negative is not None and store.validate(negative)
        receipt = store.archive((payload,)).result(3)
        assert not store.validate(negative)
        found = store.resolve(key).result(3)
        assert found.checkpoint == payload and store.validate(found)
        for forged in (
            replace(found, generation=store.stats().generation + 1),
            replace(found, generation=0),
            replace(found, lookup_epoch=None),
            replace(found, lookup_epoch=True),
            replace(found, lookup_epoch=found.lookup_epoch + 1),
            replace(found, checkpoint=None),
            replace(found, checkpoint=b"{}"),
            replace(found, key=other_key(store, key)),
            replace(found, owner_nonce=b"0" * 32),
        ):
            assert not store.validate(forged)
        with patch.object(store, "_journal_id", "foreign"):
            assert not store.validate(found)
        store.close()
        store = index(root, scoped_lookup_epochs=True)
        assert not store.validate(found) and not store.validate(receipt)
        assert store.resolve(key).result(3).checkpoint == payload
    finally:
        store.close()


def test_invalid_archive_or_rolled_back_sql_cannot_advance_epochs(tmp_path):
    key, active = checkpoint(False)
    _, terminal = checkpoint()
    store = index(tmp_path / "index", scoped_lookup_epochs=True)
    try:
        proof = store.probe(key)
        before = store._lookup_epochs.tobytes()
        with pytest.raises(ColdIntentIndexError):
            store.archive((active,)).result(3)
        assert store._lookup_epochs.tobytes() == before and store.validate(proof)
        # A real SQLite transaction failure must leave no row/epoch publication.
        with sqlite3.connect(store._path) as db:
            db.execute(
                "CREATE TRIGGER fail_insert BEFORE INSERT ON intents BEGIN SELECT RAISE(ABORT,'fixture'); END"
            )
        with pytest.raises(ColdIntentIndexError):
            store.archive((terminal,)).result(3)
        assert store._lookup_epochs.tobytes() == before and store.stats().failed
        with pytest.raises(ColdIntentIndexError):
            store.validate(proof)
    finally:
        store.close()


def test_full_commit_then_publication_failure_poisons_every_proof_and_rebuilds(tmp_path):
    key, terminal = checkpoint()
    root = tmp_path / "index"
    store = index(root, scoped_lookup_epochs=True)
    try:
        proof = store.probe(key)
        with patch.object(store, "_add_filter", side_effect=ColdIntentIndexError("publication")):
            with pytest.raises(ColdIntentIndexError):
                store.archive((terminal,)).result(3)
        assert store.stats().failed
        with pytest.raises(ColdIntentIndexError):
            store.validate(proof)
        store.close()
        store = index(root, scoped_lookup_epochs=True)
        assert store.resolve(key).result(3).checkpoint == terminal
        assert not store.validate(proof)
    finally:
        store.close()


def test_uint64_exhaustion_fails_closed_before_local_commit_never_wraps(tmp_path):
    key, terminal = checkpoint()
    store = index(tmp_path / "index", scoped_lookup_epochs=True)
    try:
        bucket = stripe(store, key)
        store._lookup_epochs[bucket] = 2**64 - 1
        proof = store.probe(key)
        with pytest.raises(ColdIntentIndexError):
            store.archive((terminal,)).result(3)
        assert store.stats().failed and store._lookup_epochs[bucket] == 2**64 - 1
        with pytest.raises(ColdIntentIndexError):
            store.validate(proof)
        with sqlite3.connect(store._path) as db:
            assert db.execute("SELECT count(*) FROM intents").fetchone() == (0,)
    finally:
        store.close()


def test_old_negative_never_overrides_hot_tail_or_same_key_retirement(tmp_path):
    store = index(tmp_path / "index", scoped_lookup_epochs=True)
    journal = RecordingJournal()
    book = RetainedOrderBook(journal=journal, cold_index=store)
    key = ColdIntentKey("broker-account-1", "strategy-1", "1")
    try:
        # Derive the fixture's actual strategy scope, not a guessed cache key.
        first = admit(book)
        t = first.order.order.consumer_trace
        key = ColdIntentKey(first.order.order.account_id, t.strategy_id, t.strategy_order_id)
        old = store.probe(key)
        before = len(journal.batches)
        with ThreadPoolExecutor(8) as pool:
            results = list(pool.map(lambda _: admit(book, resolution=old), range(8)))
        assert all(r.outcome is IntentAdmissionOutcome.IDEMPOTENT for r in results)
        assert len(journal.batches) == before
        assert len(book.reservations(key.account_id)) == 1
        order_id = first.order.order.oms_order_id
        book.reject(oms_order_id=order_id)
        candidate = book.prepare_terminal_archive(order_id)
        receipt = store.archive((candidate.encode(),)).result(3)
        assert not store.validate(old)
        assert admit(book, resolution=old).outcome is IntentAdmissionOutcome.IDEMPOTENT
        book.retire_terminal(candidate, receipt)
        before = len(journal.batches)
        with pytest.raises(ColdIntentLookupRequired):
            admit(book, resolution=old)
        found = store.resolve(key).result(3)
        assert admit(book, resolution=found).order.order.oms_order_id == order_id
        assert len(journal.batches) == before and book.reservations(key.account_id) == ()
    finally:
        store.close()


def test_sql_full_boundary_keeps_hot_coverage_until_publication(tmp_path):
    store = index(tmp_path / "index", scoped_lookup_epochs=True)
    journal = RecordingJournal()
    book = RetainedOrderBook(journal=journal, cold_index=store)
    entered, release = Event(), Event()
    try:
        first = admit(book)
        trace = first.order.order.consumer_trace
        key = ColdIntentKey(
            first.order.order.account_id, trace.strategy_id, trace.strategy_order_id
        )
        old = store.probe(key)
        book.reject(oms_order_id=first.order.order.oms_order_id)
        candidate = book.prepare_terminal_archive(first.order.order.oms_order_id)
        from xuanling_tradecore.persistence.cold_intent_index import commit_cold_rows

        def full_then_pause(db, rows):
            result = commit_cold_rows(db, rows)
            entered.set()
            assert release.wait(4)
            return result

        with patch(
            "xuanling_tradecore.persistence.cold_intent_index.commit_cold_rows",
            side_effect=full_then_pause,
        ):
            future = store.archive((candidate.encode(),))
            assert entered.wait(3) and not future.done()
            assert store.validate(old)
            before = len(journal.batches)
            assert admit(book, resolution=old).outcome is IntentAdmissionOutcome.IDEMPOTENT
            assert len(journal.batches) == before
            release.set()
            receipt = future.result(3)
        assert not store.validate(old)
        book.retire_terminal(candidate, receipt)
    finally:
        release.set()
        store.close()


@pytest.mark.parametrize("process_owner", (False, True))
def test_runtime_composition_and_recovery_never_reuse_previous_owner_proofs(
    tmp_path, process_owner
):
    if process_owner and sys.platform != "linux":
        pytest.skip("real process cold/segment owners require Linux")
    from test_cold_aware_oms_runtime import admit as runtime_admit
    from test_cold_aware_oms_runtime import runtime
    from test_order_projection_store import retry

    root = tmp_path / "runtime"
    r = runtime(root, True, scoped_cold_lookup_epochs=True, process_cold_validation=process_owner)
    try:
        first = runtime_admit(r, "old")
        order_id = first.order.order.oms_order_id
        retry(lambda: r.order_book.reject(oms_order_id=order_id))
        candidate = r.order_book.prepare_terminal_archive(order_id)
        receipt = r.cold_index.archive((candidate.encode(),)).result(8)
        r.order_book.retire_terminal(candidate, receipt)
        trace = first.order.order.consumer_trace
        key = ColdIntentKey(
            first.order.order.account_id, trace.strategy_id, trace.strategy_order_id
        )
        proof = r.cold_index.resolve(key).result(5)
        cut = r.journal.read_bound().committed_lsn
        assert r.cold_index.stats().lookup_epoch_bytes == 524_288
        r.close()
        r = runtime(root, scoped_cold_lookup_epochs=True, process_cold_validation=process_owner)
        assert not r.cold_index.validate(proof) and not r.cold_index.validate(receipt)
        assert runtime_admit(r, "old").order.order.oms_order_id == order_id
        assert r.journal.read_bound().committed_lsn == cut
    finally:
        r.close()
