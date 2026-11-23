from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from xuanling_tradecore.consumer.snapshot_cursor_v2 import (
    SnapshotCursorCodecV2,
    SnapshotCursorSigningKeyV2,
)
from xuanling_tradecore.consumer.snapshot_index_v2 import (
    ConsumerPagedSnapshotErrorV2,
    ConsumerSnapshotIndexV2,
    ConsumerSnapshotVersionV2,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1
from xuanling_tradecore.persistence.frozen_consumer_snapshot import (
    FrozenConsumerSnapshot,
    build_frozen_consumer_snapshot,
)

import test_consumer_snapshot_index_v2 as snapshot_fixture


def fixture():
    context = snapshot_fixture.ConsumerSnapshotIndexV2Tests()
    context.setUp()
    return context


def build(root: Path, count: int = 500):
    ctx = fixture()
    position = ConsumerEventPositionV2(RedisStreamIdV1(count, 0), 0)
    build_frozen_consumer_snapshot(
        root=root,
        scope=("consumer-1", "ca-1", "journal-1"),
        account_id="account-1",
        position=position,
        orders=(replace(ctx._order(n), oms_order_id=f"oms-{n:09d}") for n in range(1, count + 1)),
    )
    return position


class Reader:
    def __init__(self, root, position):
        self.root, self.position = root, position
        self.sources = []

    def capture_snapshot_version(self, **kwargs):
        source = FrozenConsumerSnapshot(self.root)
        self.sources.append(source)
        return ConsumerSnapshotVersionV2(self.position, (), order_source=source)


def make_index(root: Path, **kwargs):
    ctx = fixture()
    position = build(root)
    reader = Reader(root, position)
    index = ConsumerSnapshotIndexV2(
        registry_for=lambda _: ctx.registry,
        reader=reader,
        cursor_codec=SnapshotCursorCodecV2(SnapshotCursorSigningKeyV2("k", b"x" * 32)),
        wall_clock_ns=lambda: ctx.now,
        ttl_ns=100,
        maximum_retained_bytes=32769,
        maximum_page_cache_bytes=1,
        **kwargs,
    )
    return ctx, reader, index


def test_frozen_source_pages_full_history_without_materializing_all_orders(tmp_path: Path) -> None:
    ctx, reader, index = make_index(tmp_path / "snapshot")
    try:
        plan = index.create(session=ctx.session, binding=ctx.binding, page_size=100)
        assert plan.order_count == 500
        assert index.retained_storage_bytes() == (32768, 0)
        cursor = plan.page_cursor
        ids = []
        while cursor:
            page = index.read_page(session=ctx.session, binding=ctx.binding, page_cursor=cursor)
            ids.extend(event.oms_order_id for event in page.orders)
            cursor = page.next_page_cursor
        assert ids == [f"oms-{n:09d}" for n in range(1, 501)]
        assert index.retained_storage_bytes() == (32768, 0)
        with pytest.raises(ConsumerPagedSnapshotErrorV2, match="capacity"):
            index.create(session=ctx.session, binding=ctx.binding, page_size=100)
        assert reader.sources[-1]._closed
        ctx.now += 101
        assert index.retained_storage_bytes() == (0, 0)
        assert reader.sources[0]._closed
    finally:
        index.close()


def test_cache_byte_budget_and_pin_reclamation(tmp_path: Path) -> None:
    ctx, reader, index = make_index(tmp_path / "snapshot")
    index._maximum_page_cache_bytes = 1024 * 1024
    try:
        plan = index.create(session=ctx.session, binding=ctx.binding, page_size=100)
        page = index.read_page(
            session=ctx.session, binding=ctx.binding, page_cursor=plan.page_cursor
        )
        _, charged = index.retained_storage_bytes()
        assert 0 < charged <= 1024 * 1024
        again = index.read_page(
            session=ctx.session, binding=ctx.binding, page_cursor=plan.page_cursor
        )
        assert again.response_payload_bytes == page.response_payload_bytes
        assert index.retained_storage_bytes()[1] == charged
        ctx.now += 101
        assert index.retained_storage_bytes() == (0, 0)
    finally:
        index.close()
    assert reader.sources[0]._closed
    with pytest.raises(ConsumerPagedSnapshotErrorV2):
        index.create(session=ctx.session, binding=ctx.binding, page_size=100)


def test_deferred_delivery_rechecks_expiry_generation_and_scope_without_file_io(
    tmp_path: Path,
) -> None:
    from unittest.mock import patch

    from xuanling_tradecore.consumer.stream_registry_v2 import (
        CursorStreamRefV2,
        CursorStreamRegistryV2,
    )

    ctx, reader, index = make_index(tmp_path / "snapshot")
    try:
        plan = index.create(session=ctx.session, binding=ctx.binding, page_size=100)
        source = reader.sources[0]
        with (
            patch.object(
                source, "read_orders", side_effect=AssertionError("delivery fence read a file")
            ),
            patch.object(
                source, "close", side_effect=AssertionError("delivery fence closed a file")
            ),
        ):
            index.validate_page_delivery(
                session=ctx.session, binding=ctx.binding, page_cursor=plan.page_cursor
            )
            other = replace(ctx.binding, consumer_account_id="other")
            with pytest.raises(ConsumerPagedSnapshotErrorV2, match="scope"):
                index.validate_page_delivery(
                    session=ctx.session, binding=other, page_cursor=plan.page_cursor
                )
            stream = CursorStreamRefV2("other-journal", 2)
            with patch.object(
                index, "_registry_for", return_value=CursorStreamRegistryV2(stream, (stream,))
            ):
                with pytest.raises(ConsumerPagedSnapshotErrorV2, match="generation"):
                    index.validate_page_delivery(
                        session=ctx.session, binding=ctx.binding, page_cursor=plan.page_cursor
                    )
            ctx.now = plan.expires_at_wall_ns + 1
            with pytest.raises(ConsumerPagedSnapshotErrorV2):
                index.validate_page_delivery(
                    session=ctx.session, binding=ctx.binding, page_cursor=plan.page_cursor
                )
        assert not source._closed  # maintenance, not delivery, releases expired pins
    finally:
        index.close()


def test_expired_pin_close_cannot_hold_delivery_fence_index_lock(tmp_path: Path) -> None:
    from threading import Event, Thread
    from unittest.mock import patch

    ctx, reader, index = make_index(tmp_path / "snapshot")
    index._maximum_retained_bytes = 65536
    first = index.create(session=ctx.session, binding=ctx.binding, page_size=100)
    ctx.now += 50
    second = index.create(session=ctx.session, binding=ctx.binding, page_size=100)
    ctx.now = first.expires_at_wall_ns + 1
    entered, release, delivered = Event(), Event(), Event()
    errors = []
    original_close = reader.sources[0].close

    def close():
        entered.set()
        release.wait(3)
        original_close()

    def fence():
        try:
            index.validate_page_delivery(
                session=ctx.session, binding=ctx.binding, page_cursor=second.page_cursor
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            delivered.set()

    cleanup = Thread(target=index.retained_storage_bytes)
    delivery = Thread(target=fence)
    try:
        with patch.object(reader.sources[0], "close", side_effect=close):
            cleanup.start()
            assert entered.wait(2)
            delivery.start()
            assert delivered.wait(1), "delivery fence waited for a cold pin close"
            assert not errors
    finally:
        release.set()
        cleanup.join(3)
        if delivery.ident is not None:
            delivery.join(3)
        index.close()


@pytest.mark.parametrize("name", ["orders.bin", "positions.bin"])
def test_corrupt_file_is_rejected_on_open(tmp_path: Path, name: str) -> None:
    root = tmp_path / "snapshot"
    build(root)
    with (root / name).open("r+b") as f:
        f.seek(3)
        f.write(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        FrozenConsumerSnapshot(root)


def test_corruption_after_pin_is_detected_per_page(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    build(root)
    source = FrozenConsumerSnapshot(root)
    try:
        with (root / "positions.bin").open("r+b") as f:
            f.seek(0)
            f.write(b"corrupt")
        with pytest.raises(ValueError, match="corrupt"):
            source.read_orders(0, 1)
    finally:
        source.close()


def test_failed_build_never_publishes_a_manifest(tmp_path: Path) -> None:
    ctx = fixture()
    root = tmp_path / "snapshot"
    with pytest.raises(ValueError):
        build_frozen_consumer_snapshot(
            root=root,
            scope=("consumer-1", "ca-1", "journal-1"),
            account_id="account-1",
            position=ConsumerEventPositionV2(RedisStreamIdV1(99, 0), 0),
            orders=[ctx._order(2), ctx._order(1)],
        )
    assert not (root / "snapshot.json").exists()


def test_stream_build_large_dataset_and_bounded_read_ranges(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    build(root, 20000)
    source = FrozenConsumerSnapshot(root)
    try:
        assert source.order_count == 20000 and source.retained_bytes == 32768
        assert len(source.read_orders(19800, 20000)) == 200
        for start, end in [(-1, 1), (0, 201), (20000, 20001)]:
            with pytest.raises(ValueError):
                source.read_orders(start, end)
    finally:
        source.close()


def test_snapshot_watermark_mismatch_releases_pin(tmp_path: Path) -> None:
    ctx, reader, index = make_index(tmp_path / "snapshot")
    reader.position = ConsumerEventPositionV2(RedisStreamIdV1(999, 0), 0)
    with pytest.raises(ConsumerPagedSnapshotErrorV2):
        index.create(session=ctx.session, binding=ctx.binding, page_size=100)
    assert reader.sources[-1]._closed
    index.close()
