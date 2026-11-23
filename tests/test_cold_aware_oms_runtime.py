from __future__ import annotations

import json
import os
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.consumer.cold_aware_oms_runtime import ColdAwareOmsRuntime
from xuanling_tradecore.consumer.runtime_profile import ConsumerLongRunningPolicy
from xuanling_tradecore.domain import OmsOrderState
from xuanling_tradecore.oms import IntentAdmissionOutcome, OmsOrderBookError, OrderBook
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail
from xuanling_tradecore.oms.order_book import OmsFactKind, OmsJournalFact
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint
from xuanling_tradecore.oms.retained_order_book import ColdIntentLookupRequired
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionStore

from test_order_projection_store import retry
from test_retained_order_book import admit as retained_admit
from test_retained_order_book import retire


def runtime(root: Path, initialize=False, **kwargs):
    policy = kwargs.pop(
        "policy",
        ConsumerLongRunningPolicy(
            segment_frames=8,
            maximum_active_orders=4,
            replay_cache_events=3,
            cold_filter_bytes=64,
        ),
    )
    return ColdAwareOmsRuntime(
        root=root, scope_sha256="a" * 64, policy=policy, initialize=initialize, **kwargs
    )


def admit(r, ref="a", quantity=100):
    try:
        return retry(lambda: retained_admit(r.order_book, ref, quantity))
    except ColdIntentLookupRequired as lookup:
        proof = r.cold_index.resolve(lookup.key).result(timeout=5)
        return retry(lambda: retained_admit(r.order_book, ref, quantity, proof))


def test_owner_recovery_digest_binds_actual_verified_prefix_and_is_restart_stable(tmp_path):
    root = tmp_path / "runtime"
    first = runtime(root, True)
    empty = first.recovered_source_sha256
    admit(first)
    first.close()
    second = runtime(root)
    try:
        cut = second.recovered_watermark
        expected = sha256(
            msgspec.json.encode(
                (
                    "oms-owner-recovered-prefix-v1",
                    second.journal.journal_id,
                    second.journal.profile_hash,
                    cut,
                )
            )
        )
        tail = SegmentedJournalTail(second.journal)
        try:
            while tail.cursor.next_lsn <= cut:
                batch = tail.read_next()
                assert batch is not None
                expected.update(bytes.fromhex(batch.sha256))
        finally:
            tail.close()
        prefix = second.recovered_source_sha256
        assert cut == 2 and prefix == expected.hexdigest() and prefix != empty
        assert admit(second).outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert second.journal.read_bound().committed_lsn == cut
    finally:
        second.close()
    third = runtime(root)
    try:
        assert third.recovered_source_sha256 == prefix
    finally:
        third.close()


def test_restart_partitions_hot_and_cold_at_same_watermark_and_preserves_idempotency(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    first, second, third = admit(r, "a"), admit(r, "b"), admit(r, "c")
    a, b, c = (item.order.order.oms_order_id for item in (first, second, third))
    retry(lambda: r.order_book.mark_working(oms_order_id=a))
    retry(lambda: r.order_book.apply_trade(oms_order_id=a, trade_key="fill", quantity=40))
    retry(lambda: r.order_book.reject(oms_order_id=b))
    retry(lambda: r.order_book.mark_working(oms_order_id=c))
    retry(lambda: r.order_book.request_cancel(oms_order_id=c))
    retry(lambda: r.order_book.cancel(oms_order_id=c))
    expected = r.order_book.order(a)
    count = r.order_book.event_count("broker-account-1")
    recent = r.order_book.events_from("broker-account-1", count - 3)
    cut = r.journal.read_bound().committed_lsn
    original_key = r.identity.event_key
    r.close()
    recovered = runtime(root)
    try:
        assert recovered.identity.event_key == original_key
        assert recovered.recovered_watermark == cut
        assert recovered.stats.active_orders == 1 and recovered.stats.cold_orders == 2
        assert recovered.order_book.order_count("broker-account-1") == 1
        assert recovered.order_book.order(a) == expected
        assert recovered.order_book.event_count("broker-account-1") == count
        assert recovered.order_book.events_from("broker-account-1", count - 3) == recent
        snapshot = decode_checkpoint(recovered.checkpoint_recovered_state().read_bytes())
        assert snapshot.watermark == cut and len(snapshot.orders) == 1
        assert admit(recovered, "a").outcome is IntentAdmissionOutcome.IDEMPOTENT
        cold_retry = admit(recovered, "b")
        assert cold_retry.outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert cold_retry.order.order.oms_order_id == b
        assert cold_retry.replayed_event.order_state.value == "REJECTED"
        assert recovered.journal.read_bound().committed_lsn == cut
        assert len(recovered.order_book.reservations("broker-account-1")) == 1
        assert admit(recovered, "b", 200).outcome is IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT
        assert recovered.journal.read_bound().committed_lsn == cut + 1
        with pytest.raises(OmsOrderBookError):
            recovered.checkpoint_recovered_state()
    finally:
        recovered.close()


@pytest.mark.parametrize("stage", ["accepted", "terminal", "partial-cold", "pre-publication"])
def test_process_death_never_reuses_incomplete_cold_coverage_as_absence(tmp_path, stage):
    root = tmp_path / "runtime"
    code = """
import os,sys
from pathlib import Path
from test_cold_aware_oms_runtime import runtime,admit
import xuanling_tradecore.consumer.cold_aware_oms_runtime as module
r=runtime(Path(sys.argv[1]),True)
first=admit(r,'a')
if sys.argv[2]=='accepted': os._exit(27)
r.order_book.reject(oms_order_id=first.order.order.oms_order_id)
if sys.argv[2]=='terminal': os._exit(27)
second=admit(r,'b')
r.order_book.reject(oms_order_id=second.order.order.oms_order_id)
r.close()
if sys.argv[2]=='partial-cold':
    original=module.ColdIntentIndex.archive
    def archive(self,payloads):
        result=original(self,payloads)
        result.result(timeout=30)
        os._exit(27)
    module.ColdIntentIndex.archive=archive
else:
    module.atomic_runtime_json=lambda *_: os._exit(27)
runtime(Path(sys.argv[1]))
"""
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [str(Path.cwd()), str(Path.cwd() / "tests"), os.environ.get("PYTHONPATH", "")]
        ),
    }
    process = subprocess.run(
        [sys.executable, "-c", code, str(root), stage], env=env, timeout=60, check=False
    )
    assert process.returncode == 27
    r = runtime(root)
    try:
        cut = r.recovered_watermark
        assert r.stats.active_orders == (1 if stage == "accepted" else 0)
        assert r.stats.cold_orders == (
            0 if stage == "accepted" else 1 if stage == "terminal" else 2
        )
        result = admit(r, "a")
        assert result.outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert r.journal.read_bound().committed_lsn == cut
        assert r.order_book.order_count("broker-account-1") == (1 if stage == "accepted" else 0)
    finally:
        r.close()


def test_many_historical_terminals_are_never_loaded_into_the_hot_book(tmp_path):
    root = tmp_path / "runtime"
    policy = ConsumerLongRunningPolicy(
        segment_frames=8, maximum_active_orders=1, replay_cache_events=3, cold_filter_bytes=64
    )
    r = runtime(root, True, policy=policy)
    for n in range(40):
        order_id = admit(r, str(n)).order.order.oms_order_id
        retry(lambda: r.order_book.reject(oms_order_id=order_id))
        retire(r.order_book, r.cold_index, order_id)
    live_id = admit(r, "live").order.order.oms_order_id
    r.close()
    sizes = []
    original = OrderBook.restore_recovery_checkpoint

    def restore(book, encoded, **kwargs):
        sizes.append(len(decode_checkpoint(encoded).orders))
        return original(book, encoded, **kwargs)

    with patch.object(OrderBook, "restore_recovery_checkpoint", new=restore):
        r = runtime(root, policy=policy)
    try:
        assert max(sizes) == 1
        assert r.stats.active_orders == 1 and r.stats.cold_orders == 40
        assert r.order_book.order(live_id).order.oms_order_id == live_id
        assert r.order_book.event_count("broker-account-1") == 81
        assert r.order_book.order_count("broker-account-1") == 1
    finally:
        r.close()


def test_publication_failure_preserves_previous_cut_and_releases_leases(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    admit(r)
    r.close()
    previous = (root / "coverage.json").read_bytes()
    with patch(
        "xuanling_tradecore.consumer.cold_aware_oms_runtime.atomic_runtime_json",
        side_effect=OSError("publish failure"),
    ):
        with pytest.raises(OSError):
            runtime(root)
    assert (root / "coverage.json").read_bytes() == previous
    r = runtime(root)
    try:
        assert r.stats.active_orders == 1
    finally:
        r.close()


def test_disposable_index_loss_rebuilds_from_source_without_losing_cold_identity(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    order_id = admit(r).order.order.oms_order_id
    retry(lambda: r.order_book.reject(oms_order_id=order_id))
    old_root = r.generation_root
    r.close()
    for name in ("projection/orders.sqlite", "cold/intents.sqlite"):
        path = old_root / name
        path.rename(path.with_suffix(".missing"))
    r = runtime(root)
    try:
        assert r.stats.cold_orders == 1
        assert admit(r).order.order.oms_order_id == order_id
    finally:
        r.close()


def test_active_and_generation_capacity_refuse_before_returning_a_live_book(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    admit(r, "a")
    admit(r, "b")
    r.close()
    small = ConsumerLongRunningPolicy(
        segment_frames=8, maximum_active_orders=1, replay_cache_events=3, cold_filter_bytes=64
    )
    with pytest.raises(OmsOrderBookError, match="capacity"):
        runtime(root, policy=small)
    r = runtime(root)
    r.close()
    with pytest.raises(OmsOrderBookError, match="generation"):
        runtime(root, maximum_generations=3)


def test_missing_source_wrong_scope_and_second_owner_fail_closed(tmp_path):
    root = tmp_path / "runtime"
    with pytest.raises(OmsOrderBookError):
        runtime(root)
    r = runtime(root, True)
    with pytest.raises(OSError):
        runtime(root)
    r.close()
    with pytest.raises(Exception):
        ColdAwareOmsRuntime(
            root=root, scope_sha256="b" * 64, policy=ConsumerLongRunningPolicy(segment_frames=8)
        )
    manifest = root / "journal" / "segments.json"
    manifest.rename(manifest.with_suffix(".missing"))
    with pytest.raises(OmsOrderBookError, match="manifest"):
        runtime(root)


def test_corrupt_source_is_not_hidden_by_previous_published_coverage(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    admit(r)
    r.close()
    path = root / "journal" / "segment-000000000001.mmap"
    with path.open("r+b", buffering=0) as file:
        file.seek(4096 + 100)
        original = file.read(1)
        file.seek(4096 + 100)
        file.write(bytes([original[0] ^ 1]))
        try:
            with pytest.raises(Exception):
                runtime(root)
        finally:
            file.seek(4096 + 100)
            file.write(original)
    r = runtime(root)
    try:
        assert r.stats.active_orders == 1
    finally:
        r.close()


@pytest.mark.parametrize(
    "field,value", [("watermark", 10_000), ("watermark", True), ("active_sha256", "bad")]
)
def test_previous_publication_mismatch_is_not_silently_reset(tmp_path, field, value):
    root = tmp_path / "runtime"
    runtime(root, True).close()
    path = root / "coverage.json"
    original = path.read_bytes()
    payload = json.loads(original)
    payload[field] = value
    # Isolated fault fixture, not a deployment metadata writer.
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(OmsOrderBookError):
        runtime(root)
    path.write_bytes(original)
    runtime(root).close()


def test_source_advancing_during_recovery_rejects_the_fixed_cut_and_can_retry(tmp_path):
    import xuanling_tradecore.consumer.cold_aware_oms_runtime as module

    root = tmp_path / "runtime"
    r = runtime(root, True)
    admit(r)
    r.close()
    constructor, apply = module.SegmentedOmsFactJournal, OrderProjectionStore.apply
    opened, changed = [], []

    def create_journal(**kwargs):
        journal = constructor(**kwargs)
        opened.append(journal)
        return journal

    def project(store, batch):
        result = apply(store, batch)
        if not changed:
            initial = OmsRecoverySeedV1.decode_and_verify(
                batch.records[0].fact.recovery_payload
            ).order
            trace = initial.order.consumer_trace
            opened[0].append_batch(
                (
                    OmsJournalFact(
                        "injected-conflict",
                        OmsFactKind.IDEMPOTENCY_CONFLICT,
                        initial.order.account_id,
                        initial.order.oms_order_id,
                        initial.execution.execution_id,
                        {
                            "existing_intent_hash": initial.order.intent_hash,
                            "incoming_intent_hash": "0" * 64,
                            "strategy_id": trace.strategy_id,
                            "strategy_order_id": trace.strategy_order_id,
                        },
                    ),
                )
            )
            changed.append(True)
        return result

    with (
        patch.object(module, "SegmentedOmsFactJournal", side_effect=create_journal),
        patch.object(OrderProjectionStore, "apply", new=project),
    ):
        with pytest.raises(OmsOrderBookError, match="changed"):
            runtime(root)
    r = runtime(root)
    try:
        assert r.recovered_watermark == 3 and r.stats.active_orders == 1
    finally:
        r.close()


def test_normalized_unknown_remains_hot_and_reserved_without_redispatch(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    order_id = admit(r).order.order.oms_order_id
    retry(
        lambda: r.order_book._transition(oms_order_id=order_id, target=OmsOrderState.SUBMIT_UNKNOWN)
    )
    r.close()
    r = runtime(root)
    try:
        cut = r.recovered_watermark
        assert r.stats.active_orders == 1 and r.stats.cold_orders == 0
        assert r.order_book.order(order_id).order.state is OmsOrderState.SUBMIT_UNKNOWN
        assert r.order_book.order(order_id).reservation.is_active
        assert admit(r).outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert r.journal.read_bound().committed_lsn == cut
    finally:
        r.close()


def test_native_dispatch_facts_without_recovery_semantics_block_bootstrap(tmp_path):
    root = tmp_path / "runtime"
    r = runtime(root, True)
    order = admit(r).order
    retry(
        lambda: r.journal.append_batch(
            (
                OmsJournalFact(
                    "unresolved-native",
                    OmsFactKind.NATIVE_CALL_ENTERED,
                    order.order.account_id,
                    order.order.oms_order_id,
                    order.execution.execution_id,
                ),
            )
        )
    )
    r.close()
    with pytest.raises(OmsOrderBookError, match="not supported"):
        runtime(root)
