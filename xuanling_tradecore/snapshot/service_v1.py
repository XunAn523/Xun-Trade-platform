"""Serial, read-only M3 snapshot collection with explicit completion semantics."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Protocol

from ..adapters import BrokerQuery
from ..domain import TradingDayEpoch
from ..domain._validation import (
    DomainValidationError,
    require_nonnegative,
    require_positive,
    require_text,
)


class SnapshotCollectionErrorV1(DomainValidationError):
    """A query series cannot prove a complete, authoritative snapshot."""


class SnapshotQueryKindV1(str, Enum):
    ASSETS = "assets"
    FUNDS = "funds"
    POSITIONS = "positions"
    ORDERS = "orders"
    TRADES = "trades"
    WORKING_ORDERS = "working_orders"
    CREDIT = "credit"


class QueryCompletionModeV1(str, Enum):
    """The sole registered terminal rule for an Adapter query family."""

    SINGLE = "SINGLE"
    CURSOR_PAGED = "CURSOR_PAGED"


@dataclass(frozen=True)
class SnapshotQuerySpecV1:
    query_kind: SnapshotQueryKindV1
    completion_mode: QueryCompletionModeV1
    max_pages: int = 128

    def __post_init__(self) -> None:
        require_positive(self.max_pages, "snapshot query max_pages")


@dataclass(frozen=True)
class SnapshotQueryPageV1:
    """A normalized page summary; raw broker rows remain at the adapter boundary."""

    request_id: str
    query_kind: SnapshotQueryKindV1
    request_cursor: str | None
    next_cursor: str | None
    row_count: int
    rows_digest: str
    succeeded: bool
    is_last: bool | None
    received_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.request_id, "snapshot query request_id", max_length=128)
        if self.request_cursor is not None:
            require_text(self.request_cursor, "snapshot query request_cursor", max_length=256)
        if self.next_cursor is not None:
            require_text(self.next_cursor, "snapshot query next_cursor", max_length=256)
        require_nonnegative(self.row_count, "snapshot query row_count")
        if len(self.rows_digest) != 64 or any(
            char not in "0123456789abcdef" for char in self.rows_digest
        ):
            raise SnapshotCollectionErrorV1("snapshot query rows_digest is invalid")
        require_nonnegative(self.received_monotonic_ns, "snapshot query received_monotonic_ns")


class SnapshotQueryExecutorV1(Protocol):
    """Adapter-owned bridge from one BrokerQuery to a normalized terminal page."""

    def execute(
        self,
        request: BrokerQuery,
        *,
        cursor: str | None,
        completion: SnapshotQuerySpecV1,
    ) -> SnapshotQueryPageV1:
        """Run no broker mutation and return the one page/terminal callback summary."""

        ...


@dataclass(frozen=True)
class AccountSnapshotRequestV1:
    snapshot_id: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    trading_day: TradingDayEpoch
    query_specs: tuple[SnapshotQuerySpecV1, ...]
    page_size: int = 100

    def __post_init__(self) -> None:
        for value, label in (
            (self.snapshot_id, "snapshot_id"),
            (self.account_id, "snapshot account_id"),
            (self.venue_id, "snapshot venue_id"),
            (self.adapter_instance_id, "snapshot adapter_instance_id"),
        ):
            require_text(value, label, max_length=128)
        specs = tuple(self.query_specs)
        if not specs:
            raise SnapshotCollectionErrorV1("snapshot requires at least one query spec")
        kinds = tuple(spec.query_kind for spec in specs)
        if len(kinds) != len(set(kinds)):
            raise SnapshotCollectionErrorV1("snapshot repeats a query kind")
        require_positive(self.page_size, "snapshot page_size")
        object.__setattr__(self, "query_specs", specs)


@dataclass(frozen=True)
class SnapshotQuerySeriesV1:
    query_kind: SnapshotQueryKindV1
    page_count: int
    row_count: int
    series_digest: str
    first_received_monotonic_ns: int
    completed_monotonic_ns: int


@dataclass(frozen=True)
class AccountSnapshotV1:
    """A committed generation only after every required query series is complete."""

    snapshot_id: str
    snapshot_generation: int
    account_id: str
    venue_id: str
    adapter_instance_id: str
    trading_day: TradingDayEpoch
    query_series: tuple[SnapshotQuerySeriesV1, ...]
    captured_monotonic_ns: int
    completed_monotonic_ns: int

    def __post_init__(self) -> None:
        require_text(self.snapshot_id, "committed snapshot_id", max_length=128)
        require_positive(self.snapshot_generation, "snapshot_generation")
        if self.completed_monotonic_ns < self.captured_monotonic_ns:
            raise SnapshotCollectionErrorV1("snapshot completion precedes capture")
        series = tuple(self.query_series)
        if not series or len({item.query_kind for item in series}) != len(series):
            raise SnapshotCollectionErrorV1("snapshot query series are incomplete")
        object.__setattr__(self, "query_series", series)

    @property
    def content_sha256(self) -> str:
        payload = "|".join(
            (
                self.snapshot_id,
                str(self.snapshot_generation),
                self.account_id,
                self.venue_id,
                self.adapter_instance_id,
                self.trading_day.value,
                *(f"{item.query_kind.value}:{item.series_digest}" for item in self.query_series),
            )
        )
        return sha256(payload.encode("utf-8")).hexdigest()


class InMemoryAccountSnapshotStoreV1:
    """Test-only generation store; production binds the same commit seam to Journal evidence."""

    def __init__(self) -> None:
        self._generations: dict[str, int] = {}
        self._snapshots: dict[tuple[str, int], AccountSnapshotV1] = {}

    def next_generation(self, account_id: str) -> int:
        return self._generations.get(account_id, 0) + 1

    def commit(self, snapshot: AccountSnapshotV1) -> None:
        current = self._generations.get(snapshot.account_id, 0)
        if snapshot.snapshot_generation != current + 1:
            raise SnapshotCollectionErrorV1("snapshot generation is not monotonic")
        self._generations[snapshot.account_id] = snapshot.snapshot_generation
        self._snapshots[(snapshot.account_id, snapshot.snapshot_generation)] = snapshot

    def latest(self, account_id: str) -> AccountSnapshotV1 | None:
        generation = self._generations.get(account_id)
        return None if generation is None else self._snapshots[(account_id, generation)]


class AccountSnapshotServiceV1:
    """Capture required query families serially and commit no partial generation."""

    def __init__(
        self, *, executor: SnapshotQueryExecutorV1, store: InMemoryAccountSnapshotStoreV1
    ) -> None:
        self._executor = executor
        self._store = store

    def capture(self, request: AccountSnapshotRequestV1) -> AccountSnapshotV1:
        captured = time.monotonic_ns()
        series = tuple(self._capture_series(request, spec) for spec in request.query_specs)
        completed = time.monotonic_ns()
        snapshot = AccountSnapshotV1(
            snapshot_id=request.snapshot_id,
            snapshot_generation=self._store.next_generation(request.account_id),
            account_id=request.account_id,
            venue_id=request.venue_id,
            adapter_instance_id=request.adapter_instance_id,
            trading_day=request.trading_day,
            query_series=series,
            captured_monotonic_ns=captured,
            completed_monotonic_ns=completed,
        )
        self._store.commit(snapshot)
        return snapshot

    def _capture_series(
        self, request: AccountSnapshotRequestV1, spec: SnapshotQuerySpecV1
    ) -> SnapshotQuerySeriesV1:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages: list[SnapshotQueryPageV1] = []
        for ordinal in range(spec.max_pages):
            request_id = f"{request.snapshot_id}:{spec.query_kind.value}:{ordinal}"
            page = self._executor.execute(
                BrokerQuery(
                    request_id=request_id,
                    account_id=request.account_id,
                    venue_id=request.venue_id,
                    adapter_instance_id=request.adapter_instance_id,
                    query_kind=spec.query_kind.value,
                    page_size=request.page_size,
                ),
                cursor=cursor,
                completion=spec,
            )
            self._validate_page(page, request_id=request_id, spec=spec, cursor=cursor)
            pages.append(page)
            if spec.completion_mode is QueryCompletionModeV1.SINGLE:
                return _series_from_pages(spec.query_kind, pages)
            if page.is_last is True:
                return _series_from_pages(spec.query_kind, pages)
            assert page.next_cursor is not None
            if page.next_cursor in seen_cursors:
                raise SnapshotCollectionErrorV1("cursor-paged query repeated a cursor")
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor
        raise SnapshotCollectionErrorV1(
            "cursor-paged query exceeded max_pages without terminal page"
        )

    @staticmethod
    def _validate_page(
        page: SnapshotQueryPageV1,
        *,
        request_id: str,
        spec: SnapshotQuerySpecV1,
        cursor: str | None,
    ) -> None:
        if page.request_id != request_id or page.query_kind is not spec.query_kind:
            raise SnapshotCollectionErrorV1("snapshot query callback identity is ambiguous")
        if page.request_cursor != cursor or not page.succeeded:
            raise SnapshotCollectionErrorV1("snapshot query did not complete successfully")
        if spec.completion_mode is QueryCompletionModeV1.SINGLE:
            if page.next_cursor is not None:
                raise SnapshotCollectionErrorV1("SINGLE query cannot publish a cursor")
            return
        if page.is_last is True:
            if page.next_cursor is not None:
                raise SnapshotCollectionErrorV1("terminal cursor-paged query has a next cursor")
        elif page.is_last is False:
            if page.next_cursor is None:
                raise SnapshotCollectionErrorV1("non-terminal cursor-paged query lacks next cursor")
        else:
            raise SnapshotCollectionErrorV1("cursor-paged query lacks native terminal evidence")


def _series_from_pages(
    query_kind: SnapshotQueryKindV1, pages: list[SnapshotQueryPageV1]
) -> SnapshotQuerySeriesV1:
    if not pages:
        raise SnapshotCollectionErrorV1("snapshot query series has no pages")
    payload = "|".join(page.rows_digest for page in pages).encode("ascii")
    return SnapshotQuerySeriesV1(
        query_kind=query_kind,
        page_count=len(pages),
        row_count=sum(page.row_count for page in pages),
        series_digest=sha256(payload).hexdigest(),
        first_received_monotonic_ns=pages[0].received_monotonic_ns,
        completed_monotonic_ns=pages[-1].received_monotonic_ns,
    )
