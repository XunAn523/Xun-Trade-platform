"""Fixed-watermark snapshots sharing immutable projection version rows.

Capture and cold page reads belong to snapshot read workers. Pins retain only
small descriptors, not all historical Python objects or long SQLite readers.
The backing database is retained while these pins exist; archive/reclaim must
not remove version rows before their independently tracked retention boundary.
"""

from __future__ import annotations

from pathlib import Path
from threading import Lock
from time import monotonic_ns
from typing import Callable

from ..consumer.snapshot_index_v2 import ConsumerSnapshotVersionV2
from ..consumer.stream_registry_v2 import CursorStreamRefV2
from ..domain import (
    ConsumerAccountBinding,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    RedisStreamIdV1,
)
from ..persistence.order_projection_store import OrderProjectionError, OrderProjectionStore
from .incremental_order_projector import IncrementalProjectionHealth


class VersionedSnapshotSource:
    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        profile_hash: str,
        scope: tuple[str, str, str],
        watermark: int,
        count: int,
        maximum_database_bytes: int,
    ) -> None:
        self._root, self._journal_id, self._profile_hash = root, journal_id, profile_hash
        self._db_scope, self._watermark = scope, watermark
        self._database_bytes = maximum_database_bytes
        self.scope = (scope[0], scope[1], journal_id)
        self.account_id, self.order_count = scope[2], count
        self.snapshot_position = ConsumerEventPositionV2(RedisStreamIdV1(watermark, 0), 0)
        self.retained_bytes = 4096  # descriptor upper bound; SQL bytes budgeted separately
        self._closed, self._lock = False, Lock()

    def read_orders(self, start: int, end: int) -> tuple[ConsumerOrderEvent, ...]:
        with self._lock:
            if self._closed:
                raise OrderProjectionError("snapshot pin is closed")
        store = OrderProjectionStore(
            self._root,
            journal_id=self._journal_id,
            profile_hash=self._profile_hash,
            read_only=True,
            maximum_database_bytes=self._database_bytes,
        )
        try:
            result = store.snapshot_page(
                self._db_scope,
                watermark=self._watermark,
                count=self.order_count,
                start=start,
                end=end,
            )
        finally:
            store.close()
        with self._lock:
            if self._closed:
                raise OrderProjectionError("snapshot pin closed during read")
        return result

    def close(self) -> None:
        with self._lock:
            self._closed = True


class VersionedConsumerSnapshotReader:
    def __init__(
        self,
        *,
        root: Path,
        stream: CursorStreamRefV2,
        profile_hash: str,
        health_for: Callable[[], IncrementalProjectionHealth],
        maximum_database_bytes: int = 8 * 1024**3,
        maximum_health_age_ns: int = 2_000_000_000,
    ) -> None:
        self._root, self._stream, self._profile_hash = root, stream, profile_hash
        self._health, self._budget, self._age = (
            health_for,
            maximum_database_bytes,
            maximum_health_age_ns,
        )

    def capture_snapshot_version(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2
    ) -> ConsumerSnapshotVersionV2:
        health = self._health()
        if (
            stream != self._stream
            or not binding.active
            or health.state != "RUNNING"
            or health.error_code is not None
            or not 0 <= monotonic_ns() - health.sampled_monotonic_ns <= self._age
        ):
            raise OrderProjectionError("snapshot projection is unavailable")
        scope = (binding.consumer_id, binding.consumer_account_id, binding.account_id)
        store = OrderProjectionStore(
            self._root,
            journal_id=stream.journal_id,
            profile_hash=self._profile_hash,
            read_only=True,
            maximum_database_bytes=self._budget,
        )
        try:
            watermark, count = store.snapshot_descriptor(scope)
        finally:
            store.close()
        source = VersionedSnapshotSource(
            root=self._root,
            journal_id=stream.journal_id,
            profile_hash=self._profile_hash,
            scope=scope,
            watermark=watermark,
            count=count,
            maximum_database_bytes=self._budget,
        )
        return ConsumerSnapshotVersionV2(
            snapshot_position=source.snapshot_position,
            orders=(),
            projection_lag=max(0, health.observed_committed_lsn - watermark),
            stale=health.observed_committed_lsn > watermark,
            order_source=source,
            retained_bytes=source.retained_bytes,
        )
