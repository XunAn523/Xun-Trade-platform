from __future__ import annotations

import time
from dataclasses import replace
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionError
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalProjectionHealth
from xuanling_tradecore.read_model.versioned_snapshot_reader import VersionedConsumerSnapshotReader

from test_order_projection_store import SCOPE, admit, create, drain, retry
from test_tradecore_oms import _bindings


def test_shared_versions_keep_old_membership_and_state_without_full_history_copy(tmp_path):
    journal, book, store, tail = create(tmp_path)
    source = later = None
    try:
        first = admit(book, "a")
        second = admit(book, "b")
        drain(store, tail)
        stream = CursorStreamRefV2(journal.journal_id, 1)
        reader = VersionedConsumerSnapshotReader(
            root=tmp_path / "projection",
            stream=stream,
            profile_hash=journal.profile_hash,
            health_for=lambda: IncrementalProjectionHealth("RUNNING", 4, 4, 2, time.monotonic_ns()),
        )
        with patch.object(store, "freeze_snapshot", side_effect=AssertionError("full copy")):
            frozen = reader.capture_snapshot_version(binding=_bindings()[0], stream=stream)
        source = frozen.order_source
        assert frozen.orders == () and source.order_count == 2
        assert source.retained_bytes == 4096
        old = source.read_orders(0, 2)
        retry(lambda: book.reject(oms_order_id=first.order.order.oms_order_id))
        admit(book, "c")
        drain(store, tail)
        later = reader.capture_snapshot_version(binding=_bindings()[0], stream=stream).order_source
        assert later.order_count == 3
        assert source.read_orders(0, 2) == old
        assert [e.oms_order_id for e in old] == [
            first.order.order.oms_order_id,
            second.order.order.oms_order_id,
        ]
        assert later.read_orders(0, 1)[0].order_state.value == "REJECTED"
        assert source.read_orders(1, 2) == (old[1],)
        for start, end in ((-1, 1), (0, 3), (1, 0), (True, 1)):
            with pytest.raises(OrderProjectionError):
                source.read_orders(start, end)
        source.close()
        with pytest.raises(OrderProjectionError):
            source.read_orders(0, 1)
        assert store.snapshot_descriptor(("foreign", *SCOPE[1:]))[1] == 0
    finally:
        for s in (source, later):
            if s is not None:
                s.close()
        tail.close()
        store.close()
        journal.close()


def test_shared_snapshot_gap_and_bad_checksum_are_not_partial_pages(tmp_path):
    journal, book, store, tail = create(tmp_path)
    try:
        admit(book)
        drain(store, tail)
        watermark, count = store.snapshot_descriptor(SCOPE)
        store._db.execute("UPDATE snapshot_versions SET digest=?", (b"x" * 32,))
        store._db.commit()
        with pytest.raises(OrderProjectionError):
            store.snapshot_page(SCOPE, watermark=watermark, count=count, start=0, end=1)
        store._db.execute("DELETE FROM snapshot_versions")
        store._db.commit()
        with pytest.raises(OrderProjectionError, match="incomplete"):
            store.snapshot_page(SCOPE, watermark=watermark, count=count, start=0, end=1)
    finally:
        tail.close()
        store.close()
        journal.close()


def test_snapshot_capture_requires_live_health_and_correct_stream(tmp_path):
    journal, _, store, tail = create(tmp_path)
    try:
        stream = CursorStreamRefV2(journal.journal_id, 1)
        health = [IncrementalProjectionHealth("RUNNING", 0, 0, 0, time.monotonic_ns())]
        reader = VersionedConsumerSnapshotReader(
            root=tmp_path / "projection",
            stream=stream,
            profile_hash=journal.profile_hash,
            health_for=lambda: health[0],
        )
        for state in ("PAUSED", "FAILED", "STOPPED", "NEW"):
            health[0] = replace(health[0], state=state)
            with pytest.raises(OrderProjectionError):
                reader.capture_snapshot_version(binding=_bindings()[0], stream=stream)
        health[0] = replace(health[0], state="RUNNING", sampled_monotonic_ns=0)
        with pytest.raises(OrderProjectionError):
            reader.capture_snapshot_version(binding=_bindings()[0], stream=stream)
    finally:
        tail.close()
        store.close()
        journal.close()
