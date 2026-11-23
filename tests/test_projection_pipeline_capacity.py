"""Explicit finite-resource S4 stage benchmark, never a UDS/consumer TPS claim."""

import gc
import json
import os
import sys
from time import monotonic_ns, sleep
from unittest.mock import patch

import pytest
from xuanling_tradecore.oms.journal_tail import SegmentedJournalTail
from xuanling_tradecore.persistence.clock_identity import monotonic_clock_identity
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionStore
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json
from xuanling_tradecore.read_model.projection_groups import VerifiedProjectionGroups

from test_order_projection_store import admit, create, retry


def pipeline_capacity(
    root, *, orders, batches, optimized, retain_producer_book=False, reuse_verified_seeds=False
):
    if (
        type(orders) is not int
        or not 1 <= orders <= 100_000
        or batches not in (16, 32, 64, 128, 256)
        or type(optimized) is not bool
        or type(retain_producer_book) is not bool
        or type(reuse_verified_seeds) is not bool
        or (reuse_verified_seeds and not optimized)
    ):
        raise ValueError("invalid fixed S4 stage benchmark size")
    journal, book, store, tail = create(root, segment_frames=4096)
    rows = []
    gc_rows, gc_started = [], None
    gc_overflow = False

    def observe_gc(phase, info):
        nonlocal gc_started, gc_overflow
        if info["generation"] != 2:
            return
        if phase == "start":
            gc_started = monotonic_ns()
        elif gc_started is not None:
            if len(gc_rows) < 128:
                gc_rows.append(dict(before_ns=gc_started, after_ns=monotonic_ns()))
            else:
                gc_overflow = True
            gc_started = None

    try:
        store.close()
        for n in range(orders):
            order = admit(book, f"LR-{n}").order
            if n % 5 == 4:
                retry(lambda: book.reject(oms_order_id=order.order.oms_order_id))
            else:
                retry(
                    lambda: book.apply_trade(
                        oms_order_id=order.order.oms_order_id,
                        trade_key=f"mock-fill-{n}",
                        quantity=order.leaves_quantity,
                    )
                )
        # No producer write or wait-to-fill occurs inside the timed replay. The
        # finite fixture's prepared source has exactly the asserted final H.
        deadline = monotonic_ns() + 5_000_000_000
        while journal.stats().sealed_waiting and monotonic_ns() < deadline:
            sleep(0.001)
        assert not journal.stats().maintenance_failed
        high = journal.read_bound().committed_lsn
        assert high == 4 * orders
        store = OrderProjectionStore(
            root / "projection",
            journal_id=journal.journal_id,
            profile_hash=journal.profile_hash,
            optimize_transactions=optimized,
            reuse_verified_admission_seeds=reuse_verified_seeds,
        )
        tail.close()
        tail = SegmentedJournalTail(journal, reuse_segment_views=optimized)
        # Production projection is a separate process: it does NOT retain the
        # producer's 100K-order Python graph. Release that PREPARATION fixture,
        # not the physical source or SQL work. Retained mode is an explicit A/B
        # diagnostic. GC remains enabled/unchanged throughout the replay.
        if not retain_producer_book:
            book = None
        gc.collect()
        assert gc.isenabled(), "capacity probe must not suppress GC"
        thresholds = gc.get_threshold()
        gc.callbacks.append(observe_gc)
        started = monotonic_ns()
        completed = 0
        groups = VerifiedProjectionGroups(tail, batches, freeze_source_bound=optimized)

        def commit_group():
            nonlocal completed
            before = monotonic_ns()
            assert store.apply_many(tuple(group)) == (True,) * len(group)
            after = monotonic_ns()
            facts = sum(len(batch.records) for batch in group)
            rows.append(
                dict(
                    before_ns=before,
                    full_after_ns=after,
                    batches=len(group),
                    facts=facts,
                    first_lsn=group[0].before.next_lsn,
                    final_lsn=group[-1].after.next_lsn - 1,
                )
            )
            completed += facts

        while group := groups.read_next():
            commit_group()
        ended = monotonic_ns()
        gc.callbacks.remove(observe_gc)
        assert not gc_overflow and gc_started is None
        assert gc.isenabled() and gc.get_threshold() == thresholds
        assert completed == high and store.cursor.next_lsn == high + 1
        assert journal.read_bound().committed_lsn == high
        assert store.recovery_counts() == (high, orders, orders * 2)
        assert store._db.execute("PRAGMA synchronous").fetchone() == (2,)
        assert store._db.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        duration = (ended - started) / 1e9
        buckets = [0] * int(duration)
        for row in rows:
            slot = (row["full_after_ns"] - started) // 1_000_000_000
            if slot < len(buckets):
                buckets[slot] += row["facts"]
        rss = None
        if sys.platform == "linux":
            import resource

            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        report = dict(
            schema="projection-pipeline-capacity-v1",
            clock="time.monotonic_ns",
            clock_identity_sha256=monotonic_clock_identity(),
            cpu_affinity=sorted(os.sched_getaffinity(0)) if sys.platform == "linux" else None,
            native_calls=0,
            qualification=False,
            uds_capacity_claimed=False,
            source_prepared_before_window=True,
            t0=started,
            t1=ended,
            orders=orders,
            source_lsn=high,
            facts=completed,
            events=orders * 2,
            transaction_batch_limit=batches,
            transaction_fact_limit=512,
            optimized=optimized,
            reuse_verified_admission_seeds=reuse_verified_seeds,
            seed_cache_hits=store._seed_cache.hits if store._seed_cache else 0,
            seed_cache_misses=store._seed_cache.misses if store._seed_cache else 0,
            seed_cache_orders=store._seed_cache.entries if store._seed_cache else 0,
            seed_cache_encoded_bytes=store._seed_cache.encoded_bytes if store._seed_cache else 0,
            producer_book_retained=retain_producer_book,
            measurement_profile="PROJECTOR_WITH_RETAINED_PRODUCER_GRAPH"
            if retain_producer_book
            else "PREPARED_SOURCE_PROJECTOR_ONLY",
            gc_enabled=gc.isenabled(),
            gc_thresholds=list(gc.get_threshold()),
            generation_2_pauses=gc_rows,
            duration_seconds=duration,
            facts_per_second=completed / duration,
            projected_events_per_second=orders * 2 / duration,
            full_second_fact_buckets=buckets,
            process_peak_rss_bytes=rss,
            minimum_30_seconds_met=duration >= 30,
            stage_average_capacity_met=completed / duration >= 8000,
            journal_mode="DELETE",
            synchronous="FULL",
        )
        atomic_runtime_json(root / "capacity.json", report)
        atomic_runtime_json(root / "transactions.json", dict(rows=rows))
        assert (root / "transactions.json").stat().st_size <= 32 * 1024**2
        return report
    finally:
        if observe_gc in gc.callbacks:
            gc.callbacks.remove(observe_gc)
        store.close()
        tail.close()
        journal.close()


@pytest.mark.parametrize("batches", (128, 256))
@pytest.mark.parametrize("seed_cache", (False, True))
def test_stage_benchmark_smoke_has_actual_full_ranges_and_counts(tmp_path, batches, seed_cache):
    report = pipeline_capacity(
        tmp_path, orders=100, batches=batches, optimized=True, reuse_verified_seeds=seed_cache
    )
    assert report["facts"] == 400 and report["events"] == 200
    assert report["qualification"] is False and report["uds_capacity_claimed"] is False
    raw = json.loads((tmp_path / "transactions.json").read_text())
    assert raw["rows"][0]["first_lsn"] == 1 and raw["rows"][-1]["final_lsn"] == 400
    assert all(row["before_ns"] <= row["full_after_ns"] for row in raw["rows"])
    assert report["producer_book_retained"] is False and report["gc_enabled"] is True


def test_retained_producer_is_explicit_and_gc_callback_does_not_leak(tmp_path):
    before = tuple(gc.callbacks)
    report = pipeline_capacity(
        tmp_path, orders=50, batches=256, optimized=True, retain_producer_book=True
    )
    assert report["producer_book_retained"] is True and report["gc_enabled"] is True
    assert tuple(gc.callbacks) == before


def test_gc_observer_is_removed_if_full_transaction_fails(tmp_path):
    before = tuple(gc.callbacks)
    with patch.object(OrderProjectionStore, "apply_many", side_effect=OSError("fixture")):
        with pytest.raises(OSError):
            pipeline_capacity(tmp_path, orders=50, batches=256, optimized=True)
    assert tuple(gc.callbacks) == before


@pytest.mark.skipif(
    not os.environ.get("TRADECORE_PROJECTION_CAPACITY_ORDERS"),
    reason="explicit finite-resource S4 stage benchmark only",
)
def test_explicit_projection_pipeline_capacity(tmp_path, record_property):
    orders = int(os.environ["TRADECORE_PROJECTION_CAPACITY_ORDERS"])
    batches = int(os.environ["TRADECORE_PROJECTION_CAPACITY_BATCHES"])
    flag = os.environ["TRADECORE_PROJECTION_CAPACITY_OPTIMIZED"]
    if flag not in ("0", "1"):
        raise ValueError("stage optimized flag must be explicitly 0 or 1")
    optimized = flag == "1"
    retain = os.environ.get("TRADECORE_PROJECTION_CAPACITY_RETAIN_PRODUCER_BOOK", "0")
    if retain not in ("0", "1"):
        raise ValueError("producer-retention flag must be explicitly 0 or 1")
    seeds = os.environ.get("TRADECORE_PROJECTION_CAPACITY_SEED_CACHE", "0")
    if seeds not in ("0", "1"):
        raise ValueError("immutable-seed flag must be explicitly 0 or 1")
    report = pipeline_capacity(
        tmp_path,
        orders=orders,
        batches=batches,
        optimized=optimized,
        retain_producer_book=retain == "1",
        reuse_verified_seeds=seeds == "1",
    )
    for key, value in report.items():
        if not isinstance(value, list):
            record_property(key, value)
    if os.environ.get("TRADECORE_REQUIRE_PROJECTION_CAPACITY") == "1":
        assert report["minimum_30_seconds_met"] and report["stage_average_capacity_met"]
