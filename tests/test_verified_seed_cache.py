from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from hashlib import sha256
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.oms.order_book import OmsOrderBookError
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1
from xuanling_tradecore.oms.verified_seed_cache import VerifiedAdmissionSeedCache

from test_order_projection_store import admit, create, retry
from test_projection_transaction_reuse import open_store, streaming_mock_diff

CONTEXT = ("journal", "a" * 64, "b" * 64)


def test_cache_enabled_cross_transaction_full_table_differential(
    tmp_path, record_property, monkeypatch
):
    monkeypatch.setenv("TRADECORE_PROJECTION_SCALE_SEED_CACHE", "1")
    streaming_mock_diff(tmp_path, record_property, 300, group_size=16, ordering="deferred")


def resolve(tx, seed):
    return tx.resolve(
        seed.encoded,
        account_id=seed.order.order.account_id,
        oms_order_id=seed.order.order.oms_order_id,
    )


def test_cache_namespace_thread_hash_bytes_canonical_abort_and_close(tmp_path):
    journal, book, store, tail = create(tmp_path)
    cache = VerifiedAdmissionSeedCache(CONTEXT)
    try:
        seed = OmsRecoverySeedV1.build(admit(book).order)
        tx = cache.begin(CONTEXT)
        assert resolve(tx, seed) == seed
        assert cache.entries == 0
        tx.abort()
        assert cache.entries == 0
        tx = cache.begin(CONTEXT)
        resolve(tx, seed)
        tx.publish_after_full_commit()
        assert cache.entries == 1 and cache.encoded_bytes == len(seed.encoded)
        for index in range(3):
            wrong = list(CONTEXT)
            wrong[index] = "z" * 64
            with pytest.raises(OmsOrderBookError):
                cache.begin(tuple(wrong))
        with ThreadPoolExecutor(1) as pool:
            with pytest.raises(OmsOrderBookError):
                pool.submit(cache.begin, CONTEXT).result()
        tx = cache.begin(CONTEXT)
        with patch.object(OmsRecoverySeedV1, "decode_and_verify", side_effect=AssertionError):
            assert resolve(tx, seed) == seed
        changed = seed.encoded + b" "
        # Even a forced same-hash result must not bypass full byte equality.
        with patch("xuanling_tradecore.oms.verified_seed_cache.sha256") as digest:
            digest.return_value.hexdigest.return_value = seed.seed_hash
            with pytest.raises(OmsOrderBookError):
                tx.resolve(
                    changed,
                    account_id=seed.order.order.account_id,
                    oms_order_id=seed.order.order.oms_order_id,
                )
        tx.abort()
        tx = cache.begin(CONTEXT)
        with pytest.raises(ValueError):
            tx.resolve(b"{}", account_id="other", oms_order_id="other")
        tx.abort()
        assert cache.entries == 1
        cache.close()
        assert not cache._entries
        with pytest.raises(OmsOrderBookError):
            cache.begin(CONTEXT)
    finally:
        store.close()
        tail.close()
        journal.close()


def test_cache_1024_orders_and_bytes_budget_lru(tmp_path):
    journal, book, store, tail = create(tmp_path, segment_frames=4096)
    cache = VerifiedAdmissionSeedCache(CONTEXT)
    try:
        first = None
        for index in range(1025):
            seed = OmsRecoverySeedV1.build(admit(book, f"bounded-{index}").order)
            first = first or seed
            tx = cache.begin(CONTEXT)
            resolve(tx, seed)
            tx.publish_after_full_commit()
            assert cache.entries <= 1024 and cache.encoded_bytes <= 16 * 1024**2
        assert first is not None and cache.evictions == 1
        assert (first.order.order.account_id, first.order.order.oms_order_id) not in cache._entries
        cache.MAXIMUM_BYTES = 1
        tx = cache.begin(CONTEXT)
        resolve(tx, seed)
        tx.publish_after_full_commit()
        assert cache.entries == cache.encoded_bytes == 0
    finally:
        cache.close()
        store.close()
        tail.close()
        journal.close()


def test_cache_cross_transaction_hit_still_checks_mutable_checkpoint_and_rollback(tmp_path):
    journal, book, old, tail = create(tmp_path)
    old.close()
    store = open_store(
        tmp_path / "projection",
        journal,
        optimize_transactions=True,
        reuse_verified_admission_seeds=True,
    )
    try:
        order = admit(book)
        retry(lambda: book.mark_working(oms_order_id=order.order.order.oms_order_id))
        first, second = tail.read_next(), tail.read_next()
        assert store.apply_many((first,)) == (True,)
        cache = store._seed_cache
        assert cache is not None and cache.entries == 1
        with patch.object(OmsRecoverySeedV1, "decode_and_verify", side_effect=AssertionError):
            assert store.apply_many((second,)) == (True,)
        assert cache.hits > 0
        # Checkpoint checksum is valid, but its mutable quantities are impossible.
        checkpoint = store._db.execute("SELECT checkpoint FROM orders").fetchone()[0]
        state = msgspec.json.decode(checkpoint)
        state["orders"][0]["filled_quantity"] = -1
        bad = msgspec.json.encode(state)
        store._db.execute(
            "UPDATE orders SET checkpoint=?,checkpoint_hash=?", (bad, sha256(bad).digest())
        )
        store._db.commit()
        retry(lambda: book.reject(oms_order_id=order.order.order.oms_order_id))
        third = tail.read_next()
        hits_before_bad_state = cache.hits
        with pytest.raises(ValueError):
            store.apply_many((third,))
        assert cache.hits > hits_before_bad_state
        assert cache.entries == 1 and cache._active is None
    finally:
        store.close()
        tail.close()
        journal.close()


def test_failed_group_never_publishes_new_proofs(tmp_path):
    journal, book, old, tail = create(tmp_path)
    old.close()
    store = open_store(
        tmp_path / "projection",
        journal,
        optimize_transactions=True,
        reuse_verified_admission_seeds=True,
    )
    try:
        admit(book, "one")
        admit(book, "two")
        first, second = tail.read_next(), tail.read_next()
        with pytest.raises(Exception):
            store.apply_many((first, replace(second, sha256="0" * 64)))
        assert store._seed_cache.entries == 0 and store._seed_transaction is None
        assert store._db.execute("SELECT COUNT(*) FROM orders").fetchone() == (0,)
    finally:
        store.close()
        tail.close()
        journal.close()
