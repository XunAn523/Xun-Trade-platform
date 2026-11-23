from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Event, get_ident

import pytest
from xuanling_tradecore.oms import OrderBook
from xuanling_tradecore.oms.cold_intent import (
    ColdIntentIndexBusy,
    ColdIntentIndexError,
    ColdIntentKey,
)
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex

from test_oms_committed_recovery import RecordingJournal, admit


def checkpoint(terminal: bool = True) -> tuple[ColdIntentKey, bytes]:
    source = RecordingJournal()
    original = OrderBook(journal=source, persist_recovery_seed=True)
    first = admit(original)
    if terminal:
        original.reject(oms_order_id=first.order.order.oms_order_id)
    book = OrderBook(journal=source, persist_recovery_seed=True)
    for batch in source.batches:
        book.restore_committed_batch(*batch)
    trace = first.order.order.consumer_trace
    return ColdIntentKey(
        first.order.order.account_id, trace.strategy_id or "", trace.strategy_order_id or ""
    ), book.export_recovery_checkpoint()


def index(root: Path, **kwargs) -> ColdIntentIndex:
    return ColdIntentIndex(
        root=root, journal_id="stream", filter_bytes=64, initialize=not root.exists(), **kwargs
    )


def test_negative_probe_commit_handoff_exact_lookup_and_restart(tmp_path: Path) -> None:
    key, payload = checkpoint()
    store = index(tmp_path / "index")
    negative = store.probe(key)
    assert negative is not None and store.validate(negative)
    try:
        receipt = store.archive((payload,)).result(timeout=5)
        assert receipt.keys == (key,) and store.validate(receipt)
        assert not store.validate(negative)
        assert store.probe(key) is None
        found = store.resolve(key).result(timeout=5)
        assert found.checkpoint == payload and store.validate(found)
        seed = decode_checkpoint(found.checkpoint).orders[0].admission_seed
        assert OmsRecoverySeedV1.decode_and_verify(seed).order.order.state.value == "ACCEPTED"
        assert store.stats().records == 1
        timings = store.stats()
        assert timings.archive_batches == 1
        assert timings.validation_ns >= timings.max_validation_ns >= 0
        assert timings.sql_commit_ns >= timings.max_sql_commit_ns >= 0
        assert 0 < store.stats().filter_occupancy_ppm <= 1_000_000
    finally:
        store.close()
    restarted = index(tmp_path / "index")
    try:
        assert not restarted.validate(receipt)  # a previous owner's proof is never reusable
        assert restarted.resolve(key).result(timeout=5).checkpoint == payload
        assert restarted.stats().records == 1
    finally:
        restarted.close()


def test_false_positive_requires_exact_bounded_lookup(tmp_path: Path) -> None:
    store = index(tmp_path / "index")
    try:
        key = ColdIntentKey("a", "s", "new")
        store._filter[:] = b"\xff" * 64
        assert store.probe(key) is None
        result = store.resolve(key).result(timeout=5)
        assert result.checkpoint is None and store.validate(result)
    finally:
        store.close()


def test_positive_receipts_survive_append_only_generations_but_not_forgery(tmp_path):
    from dataclasses import replace

    key, payload = checkpoint()
    store = index(tmp_path / "index")
    try:
        negative = store.probe(ColdIntentKey("a", "s", "absent"))
        first = store.archive((payload,)).result(timeout=5)
        second = store.archive((payload,)).result(timeout=5)
        assert second.generation > first.generation
        assert store.validate(first) and store.validate(second)
        assert negative is not None and not store.validate(negative)
        assert not store.validate(replace(first, generation=0))
        assert not store.validate(replace(first, generation=second.generation + 1))
        assert not store.validate(replace(first, generation=second.generation))
        assert not store.validate(replace(first, owner_nonce=b"x" * 32))
        assert not store.validate(replace(first, digests=(b"x" * 32,)))
        assert store.resolve(key).result(timeout=5).checkpoint == payload
    finally:
        store.close()


def test_archive_phase_counters_follow_real_boundaries_not_guessed_split(tmp_path):
    from unittest.mock import patch

    store = index(tmp_path / "phase-index")
    _, payload = checkpoint()
    try:
        with patch(
            "xuanling_tradecore.persistence.cold_intent_index.monotonic_ns",
            side_effect=(100, 120, 180),
        ):
            store.archive((payload,)).result(timeout=5)
        stats = store.stats()
        assert stats.archive_batches == 1
        assert (stats.validation_ns, stats.max_validation_ns) == (20, 20)
        assert (stats.sql_commit_ns, stats.max_sql_commit_ns) == (60, 60)
    finally:
        store.close()


def test_external_sql_owner_keeps_lease_until_backend_drain_retries_successfully(tmp_path):
    from xuanling_tradecore.persistence.cold_intent_index import ColdArchiveOutcomeUnknown
    from xuanling_tradecore.persistence.runtime_files import ExclusiveRuntimeLease

    drained = Event()
    attempts = []

    def unknown(_payloads):
        raise ColdArchiveOutcomeUnknown("lost commit reply")

    def close_backend():
        attempts.append(1)
        if len(attempts) == 1:
            drained.set()
            raise ColdIntentIndexError("backend not stopped")

    root = tmp_path / "owned-backend"
    store = index(root, archive_committer=unknown, archive_close=close_backend)
    _, payload = checkpoint()
    try:
        with pytest.raises(ColdIntentIndexError):
            store.archive((payload,)).result(timeout=5)
        assert drained.wait(3)
        with pytest.raises(OSError):
            ExclusiveRuntimeLease(root / "cold-owner.lock")
        store.close()
        assert len(attempts) == 2 and store._backend_drained
        lease = ExclusiveRuntimeLease(root / "cold-owner.lock")
        lease.close()
    finally:
        store.close()


@pytest.mark.parametrize("invalid_member", [False, True])
def test_available_archive_group_has_one_full_commit_or_atomic_rollback(
    tmp_path, monkeypatch, invalid_member
):
    from test_retained_order_book import admit as admit_retained
    from test_retained_order_book import setup

    fixture = tmp_path / "fixtures"
    fixture.mkdir()
    owner, _, book = setup(fixture)
    payloads = []
    for ref in ("a", "b", "c"):
        order = admit_retained(book, ref)
        book.reject(oms_order_id=order.order.order.oms_order_id)
        payloads.append(book.prepare_terminal_archive(order.order.order.oms_order_id).encode())
    owner.close()
    if invalid_member:
        payloads[2] = checkpoint(False)[1]
    store = index(
        tmp_path / "grouped", maximum_transaction_archives=4, maximum_transaction_checkpoints=256
    )
    entered, release = Event(), Event()
    original = store._write
    calls = []

    def blocked(db, work):
        calls.append(len(work.checkpoints))
        if len(calls) == 1:
            entered.set()
            assert release.wait(5)
        return original(db, work)

    monkeypatch.setattr(store, "_write", blocked)
    try:
        first = store.archive((payloads[0],))
        assert entered.wait(3)
        second = store.archive((payloads[1],))
        third = store.archive((payloads[2],))
        assert not second.done() and not third.done()
        release.set()
        assert store.validate(first.result(timeout=5))
        if invalid_member:
            for future in (second, third):
                with pytest.raises(ColdIntentIndexError):
                    future.result(timeout=5)
            assert store.stats().records == 1 and store.stats().generation == 1
        else:
            receipts = [future.result(timeout=5) for future in (second, third)]
            assert receipts[0].generation == receipts[1].generation == 2
            assert all(store.validate(receipt) and len(receipt.keys) == 1 for receipt in receipts)
            assert store.stats().records == 3 and store.stats().archive_batches == 2
        assert calls == [1, 2]
    finally:
        release.set()
        store.close()


def test_hot_probe_and_generation_check_never_execute_sql(tmp_path: Path, monkeypatch) -> None:
    threads = []
    original = ColdIntentIndex._connect

    def connect(self):
        threads.append(get_ident())
        return original(self)

    monkeypatch.setattr(ColdIntentIndex, "_connect", connect)
    store = index(tmp_path / "index")
    try:
        key = ColdIntentKey("a", "s", "new")
        for _ in range(1000):
            result = store.probe(key)
            assert result is not None and store.validate(result)
        assert len(threads) == 1 and threads[0] != get_ident()
    finally:
        store.close()


def test_lookup_coalescing_is_bounded_and_does_not_block_submitter(
    tmp_path: Path, monkeypatch
) -> None:
    store = index(tmp_path / "index", capacity=2)
    entered, release = Event(), Event()
    original = store._lookup
    calls = []

    def lookup(db, key):
        calls.append(key)
        entered.set()
        assert release.wait(5)
        return original(db, key)

    monkeypatch.setattr(store, "_lookup", lookup)
    key = ColdIntentKey("a", "s", "new")
    try:
        a = store.resolve(key)
        assert entered.wait(5)
        b = store.resolve(key)
        assert not a.cancel()  # abandoning a waiter cannot cancel the shared operation
        with pytest.raises(ColdIntentIndexBusy):
            store.resolve(key)
        assert store.stats().pending_lookups == 2
        release.set()
        assert a.result(timeout=5) == b.result(timeout=5)
        assert calls == [key]
    finally:
        release.set()
        store.close()


def test_active_checkpoint_is_rejected_without_poisoning_worker(tmp_path: Path) -> None:
    store = index(tmp_path / "index")
    try:
        key, active = checkpoint(False)
        with pytest.raises(ColdIntentIndexError):
            store.archive((active,)).result(timeout=5)
        assert store.stats().generation == 0 and not store.stats().failed
        _, terminal = checkpoint()
        store.archive((terminal,)).result(timeout=5)
        assert store.resolve(key).result(timeout=5).checkpoint == terminal
    finally:
        store.close()


def test_changed_result_cannot_overwrite_original_and_duplicate_is_idempotent(
    tmp_path: Path,
) -> None:
    store = index(tmp_path / "index")
    key, payload = checkpoint()
    try:
        store.archive((payload,)).result(timeout=5)
        store.archive((payload,)).result(timeout=5)
        assert store.stats().records == 1
        _, different_order_same_key = checkpoint()
        with pytest.raises(ColdIntentIndexError, match="overwrite"):
            store.archive((different_order_same_key,)).result(timeout=5)
        assert store.resolve(key).result(timeout=5).checkpoint == payload
    finally:
        store.close()


def test_storage_failure_is_sticky_and_never_means_not_found(tmp_path: Path, monkeypatch) -> None:
    store = index(tmp_path / "index")

    def broken(db, key):
        raise sqlite3.OperationalError("injected IO error")

    monkeypatch.setattr(store, "_lookup", broken)
    key = ColdIntentKey("a", "s", "new")
    try:
        with pytest.raises(ColdIntentIndexError):
            store.resolve(key).result(timeout=5)
        assert store.stats().failed
        with pytest.raises(ColdIntentIndexError, match="unknown"):
            store.probe(key)
    finally:
        store.close()


def test_exclusive_owner_and_wrong_scope_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "index"
    store = index(root)
    with pytest.raises(OSError):
        index(root)
    store.close()
    with pytest.raises(ColdIntentIndexError):
        ColdIntentIndex(root=root, journal_id="other", filter_bytes=64)


def test_corrupt_payload_is_detected_on_open(tmp_path: Path) -> None:
    root = tmp_path / "index"
    store = index(root)
    _, payload = checkpoint()
    store.archive((payload,)).result(timeout=5)
    store.close()
    with sqlite3.connect(str(root / "intents.sqlite")) as db:
        db.execute("UPDATE intents SET payload=?", (b"corrupt",))
    with pytest.raises(ColdIntentIndexError):
        index(root)


def test_missing_database_is_never_implicitly_recreated(tmp_path: Path) -> None:
    root = tmp_path / "index"
    index(root).close()
    (root / "intents.sqlite").unlink()
    with pytest.raises(OSError):
        index(root)
    assert not (root / "intents.sqlite").exists()


def test_malformed_input_does_not_poison_worker(tmp_path: Path) -> None:
    store = index(tmp_path / "index")
    try:
        with pytest.raises(ColdIntentIndexError):
            store.archive((b"{}",)).result(timeout=5)
        _, payload = checkpoint()
        store.archive((payload,)).result(timeout=5)
        assert not store.stats().failed
    finally:
        store.close()
