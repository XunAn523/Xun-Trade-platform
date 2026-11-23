"""Explicit Consumer read adapter for the reconstructible Journal projection.

No default runtime switch. SQL connections are request-owned on bounded read
workers, not shared with the projector or retained on arbitrary worker threads.
The public signed-cursor service remains responsible for token authentication.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from time import monotonic_ns
from typing import Callable, Generator, Protocol, cast

import msgspec

from ..consumer.base import ConsumerNotFoundError, PositionedConsumerEventV2
from ..consumer.projection_reads_v2 import (
    ConsumerAuditEvidenceUnavailableV2,
    ConsumerProjectionReadErrorV2,
    projection_read_operation,
    require_projection_read,
)
from ..consumer.replay_v2 import ConsumerReplayPartitionBoundsV2, ConsumerReplaySliceV2
from ..consumer.request_trace_v2 import mark_request_stage, mark_request_value
from ..consumer.stream_registry_v2 import CursorStreamRefV2
from ..domain import (
    ConsumerAccountBinding,
    ConsumerAuditTrail,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    RedisStreamIdV1,
)
from ..oms.committed_replay_window import CommittedReplayPage
from ..oms.journal_audit import verify_order_audit
from ..oms.journal_tail import JournalTailSource
from ..oms.order_book import OmsFactJournal
from ..persistence.order_projection_store import OrderProjectionStore
from .incremental_order_projector import IncrementalProjectionHealth
from .process_projection_reads import ProcessProjectionReads


def _position(lsn: int) -> ConsumerEventPositionV2:
    return ConsumerEventPositionV2(RedisStreamIdV1(lsn, 0), 0)


def _lsn(position: ConsumerEventPositionV2 | None) -> int:
    if position is None:
        return 0
    if position.stream_id.redis_seq or position.fact_ordinal:
        raise ConsumerProjectionReadErrorV2("projection position is not canonical")
    value = position.stream_id.redis_ms
    if value > 2**63 - 1:
        raise ConsumerProjectionReadErrorV2("projection position exceeds storage bounds")
    return value


class CommittedReplayBook(Protocol):
    def committed_replay_uses_journal(self, journal: OmsFactJournal) -> bool: ...

    def committed_replay_page(
        self, scope: tuple[str, str, str], *, after: int, high: int, limit: int
    ) -> CommittedReplayPage | None: ...

    def committed_replay_bounds(
        self,
        scope: tuple[str, str, str],
        *,
        after: int,
        observed_head: int,
        partitions: int,
    ) -> tuple[tuple[int, int, int], ...] | None: ...


class ConsumerOrderProjectionReaderV2:
    def __init__(
        self,
        *,
        root: Path,
        journal: JournalTailSource,
        stream: CursorStreamRefV2,
        current_stream: Callable[[], CursorStreamRefV2],
        health_for: Callable[[], IncrementalProjectionHealth],
        maximum_health_age_ns: int = 2_000_000_000,
        maximum_database_bytes: int = 8 * 1024**3,
        clock_ns: Callable[[], int] = monotonic_ns,
        process_reads: ProcessProjectionReads | None = None,
        committed_replay_book: CommittedReplayBook | None = None,
    ) -> None:
        if (
            stream.journal_id != journal.journal_id
            or type(maximum_health_age_ns) is not int
            or not 1 <= maximum_health_age_ns <= 60_000_000_000
            or type(maximum_database_bytes) is not int
            or not 1024**2 <= maximum_database_bytes <= 1024**4
            or (
                committed_replay_book is not None
                and not committed_replay_book.committed_replay_uses_journal(
                    cast(OmsFactJournal, journal)
                )
            )
        ):
            raise ValueError("invalid projection reader scope/freshness budget")
        self._root, self._journal, self._stream = root, journal, stream
        self._current_stream, self._health_for = current_stream, health_for
        self._age, self._clock = maximum_health_age_ns, clock_ns
        self._database_bytes = maximum_database_bytes
        self._process_reads = process_reads
        self._committed_replay_book = committed_replay_book

    def validate_delivery(self) -> None:
        """Memory-only final guard; never open SQL/files or acquire writer lock."""
        self._delivery_health()

    def _delivery_health(self) -> IncrementalProjectionHealth:
        try:
            return self._validate_delivery()
        except ConsumerProjectionReadErrorV2:
            raise
        except Exception:
            raise ConsumerProjectionReadErrorV2("projection delivery is unavailable") from None

    def _validate_delivery(self) -> IncrementalProjectionHealth:
        health = self._health_for()
        age = self._clock() - health.sampled_monotonic_ns
        if (
            health.state not in {"RUNNING", "PAUSED"}
            or (
                projection_read_operation() == "CreateReplayPartitions"
                and health.state != "RUNNING"
            )
            or health.error_code is not None
            or not 0 <= age <= self._age
            or self._current_stream() != self._stream
        ):
            raise ConsumerProjectionReadErrorV2("projection delivery is unavailable")
        return health

    @staticmethod
    def _scope(binding: ConsumerAccountBinding) -> tuple[str, str, str]:
        if not binding.active:
            raise ConsumerProjectionReadErrorV2("projection binding is unavailable")
        return binding.consumer_id, binding.consumer_account_id, binding.account_id

    @contextmanager
    def _read(
        self, operation: str
    ) -> Generator[tuple[OrderProjectionStore | ProcessProjectionReads, int], None, None]:
        require_projection_read(operation)
        store = None
        try:
            self.validate_delivery()
            minimum = self._journal.read_bound().committed_lsn
            store = self._process_reads or OrderProjectionStore(
                self._root,
                journal_id=self._journal.journal_id,
                profile_hash=self._journal.profile_hash,
                read_only=True,
                maximum_database_bytes=self._database_bytes,
            )
            yield store, minimum
            self.validate_delivery()
        except ConsumerNotFoundError:
            raise
        except Exception as error:
            # No private SQLite path/error/traceback crosses the Consumer seam.
            mark_request_value(
                "projection_error_kind",
                {
                    "OrderProjectionStale": "PROJECTION_BEHIND_FENCE",
                    "OrderProjectionError": "DERIVED_STORE_CHECK_FAILED",
                    "ConsumerProjectionReadErrorV2": "DELIVERY_GUARD_UNAVAILABLE",
                    "OperationalError": "SQL_OPERATION_UNAVAILABLE",
                    "FileNotFoundError": "STORAGE_ACCESS_UNAVAILABLE",
                    "PermissionError": "STORAGE_ACCESS_UNAVAILABLE",
                    "OSError": "STORAGE_ACCESS_UNAVAILABLE",
                }.get(type(error).__name__, "UNAVAILABLE"),
            )
            if operation == "GetOrderAuditTrail":
                raise ConsumerAuditEvidenceUnavailableV2("audit evidence is unavailable") from None
            raise ConsumerProjectionReadErrorV2("projection read is unavailable") from None
        finally:
            if store is not None and store is not self._process_reads:
                try:
                    store.close()
                except Exception:
                    if operation == "GetOrderAuditTrail":
                        raise ConsumerAuditEvidenceUnavailableV2(
                            "audit evidence is unavailable"
                        ) from None
                    raise ConsumerProjectionReadErrorV2("projection close is unavailable") from None

    def get_order(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> ConsumerOrderEvent:
        operation = projection_read_operation()
        with self._read(
            "GetOrderAuditTrail" if operation == "GetOrderAuditTrail" else "GetOrder"
        ) as (store, minimum):
            result = store.lookup(
                self._scope(account_binding), oms_order_id, minimum_watermark=minimum
            )
        if result is None:
            raise ConsumerNotFoundError("consumer order was not found")
        return result

    def get_order_audit_trail(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]:
        mark_request_stage("audit_index_started_ns")
        with self._read("GetOrderAuditTrail") as (store, minimum):
            plan = store.order_audit_plan(
                self._scope(account_binding), oms_order_id, minimum_watermark=minimum
            )
        mark_request_stage("audit_index_finished_ns")
        if plan is None:
            raise ConsumerNotFoundError("consumer order was not found")
        # The SQL snapshot/connection is already released before Journal I/O.
        try:
            mark_request_stage("audit_journal_verify_started_ns")
            result = verify_order_audit(
                self._journal, plan, scope=self._scope(account_binding), oms_order_id=oms_order_id
            )
            mark_request_stage("audit_journal_verify_finished_ns")
            mark_request_value("audit_verified_batches", len(plan.anchors))
            mark_request_value("audit_verified_events", len(result))
            self.validate_delivery()
            return result
        except Exception:
            raise ConsumerAuditEvidenceUnavailableV2("audit evidence is unavailable") from None

    def read_events(
        self,
        *,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        after_position: ConsumerEventPositionV2 | None,
        high_watermark_position: ConsumerEventPositionV2 | None,
        limit: int,
    ) -> ConsumerReplaySliceV2:
        if stream != self._stream or type(limit) is not int or not 1 <= limit <= 10_000:
            raise ConsumerProjectionReadErrorV2("projection replay scope/limit is unavailable")
        # limit is a ceiling, not a requirement to allocate that many events.
        after = _lsn(after_position)
        high = None if high_watermark_position is None else _lsn(high_watermark_position)
        if self._committed_replay_book is not None and high is not None:
            require_projection_read("PollEvents")
            self.validate_delivery()
            hot = self._committed_replay_book.committed_replay_page(
                self._scope(binding), after=after, high=high, limit=min(limit, 200)
            )
            if hot is not None:
                events: list[PositionedConsumerEventV2] = []
                used = 0
                more = hot.has_more
                for row in hot.entries:
                    event = row.consumer_event()
                    size = len(msgspec.json.encode(event))
                    if size > 65536:
                        raise ConsumerProjectionReadErrorV2("replay event exceeds its budget")
                    if used + size > 262144:
                        more = True
                        break
                    used += size
                    events.append(PositionedConsumerEventV2(_position(event.journal_lsn), event))
                self.validate_delivery()
                mark_request_value("event_read_path", "COMMITTED_WINDOW")
                return ConsumerReplaySliceV2(tuple(events), _position(hot.high), more)
        with self._read("PollEvents") as (store, minimum):
            page = store.events_after(
                self._scope(binding),
                after,
                limit=min(limit, 200),
                through_lsn=high,
                minimum_watermark=minimum if high is None else high,
            )
        return ConsumerReplaySliceV2(
            tuple(PositionedConsumerEventV2(_position(e.journal_lsn), e) for e in page.events),
            _position(page.watermark) if page.watermark or high is not None else None,
            page.has_more,
        )

    def replay_partition_bounds(
        self, *, binding: ConsumerAccountBinding, stream: CursorStreamRefV2, partition_count: int
    ) -> tuple[ConsumerReplayPartitionBoundsV2, ...]:
        if stream != self._stream:
            raise ConsumerProjectionReadErrorV2("projection stream is unavailable")
        hot = self._hot_bounds(binding, after=0, partitions=partition_count)
        if hot is not None:
            return tuple(
                ConsumerReplayPartitionBoundsV2(_position(a) if a else None, _position(b), count)
                for a, b, count in hot
            )
        with self._read("CreateReplayPartitions") as (store, observed_source_head):
            bounds = store.partition_events(
                self._scope(binding),
                partition_count=partition_count,
                through_lsn=observed_source_head,
            )
        return tuple(
            ConsumerReplayPartitionBoundsV2(_position(a) if a else None, _position(b), count)
            for a, b, count in bounds
        )

    def replay_suffix_partition_bounds(
        self,
        *,
        binding: ConsumerAccountBinding,
        stream: CursorStreamRefV2,
        partition_count: int,
        after_position: ConsumerEventPositionV2,
    ) -> tuple[ConsumerReplayPartitionBoundsV2, ...]:
        if stream != self._stream:
            raise ConsumerProjectionReadErrorV2("projection suffix stream is unavailable")
        after = _lsn(after_position)
        hot = self._hot_bounds(binding, after=after, partitions=partition_count)
        if hot is not None:
            return tuple(
                ConsumerReplayPartitionBoundsV2(_position(a), _position(b), count)
                for a, b, count in hot
            )
        with self._read("CreateReplayPartitions") as (store, observed_source_head):
            bounds = store.partition_events(
                self._scope(binding),
                partition_count=partition_count,
                minimum_watermark=after,
                after_lsn=after,
                through_lsn=observed_source_head,
            )
        return tuple(
            ConsumerReplayPartitionBoundsV2(_position(a), _position(b), count)
            for a, b, count in bounds
        )

    def _hot_bounds(
        self, binding: ConsumerAccountBinding, *, after: int, partitions: int
    ) -> tuple[tuple[int, int, int], ...] | None:
        if self._committed_replay_book is None:
            return None
        require_projection_read("CreateReplayPartitions")
        health = self._delivery_health()
        if type(health.projected_lsn) is not int or health.projected_lsn < 0:
            raise ConsumerProjectionReadErrorV2("projection delivery is unavailable")
        # A signed range may outlive this bounded cache.  Only sign a prefix
        # already committed to the canonical SQL projection, so eviction can
        # safely fall back without requiring an unprojected future watermark.
        durable_head = min(self._journal.read_bound().committed_lsn, health.projected_lsn)
        result = self._committed_replay_book.committed_replay_bounds(
            self._scope(binding),
            after=after,
            observed_head=durable_head,
            partitions=partitions,
        )
        self.validate_delivery()
        if result is not None:
            mark_request_value("event_read_path", "COMMITTED_WINDOW")
        return result
