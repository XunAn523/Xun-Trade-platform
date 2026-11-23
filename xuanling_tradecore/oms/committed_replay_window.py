"""Bounded post-commit event references, NOT history or recovery authority.

The owning account lock serializes every call. No encoding, hashing, file I/O,
Consumer DTO construction or SQL is performed when publishing a reference.
An uncovered range returns None: callers MUST use the verified cold reader.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..domain import ConsumerEventKind, ConsumerOrderEvent, ConsumerTrace, TradingDayEpoch
from .order_book import OmsOrderEvent


@dataclass(frozen=True, slots=True)
class CommittedReplayEntry:
    event: OmsOrderEvent
    trace: ConsumerTrace
    trading_day: TradingDayEpoch

    def consumer_event(self) -> ConsumerOrderEvent:
        """Read-side construction only; immutable fields match Journal projection."""
        e = self.event
        return ConsumerOrderEvent(
            consumer_event_id=f"order:{e.event_id}",
            event_kind=ConsumerEventKind.ORDER,
            schema_version=2,
            account_id=e.account_id,
            trading_day=self.trading_day,
            journal_id=e.journal_id,
            journal_lsn=e.journal_lsn,
            consumer_trace=self.trace,
            oms_order_id=e.oms_order_id,
            execution_id=e.execution_id,
            order_state=e.order_state,
            order_revision=e.order_revision,
        )

    def scoped(self, scope: tuple[str, str, str]) -> bool:
        return (
            self.trace.consumer_id == scope[0]
            and self.trace.consumer_account_id == scope[1]
            and self.event.account_id == scope[2]
        )


@dataclass(frozen=True, slots=True)
class CommittedReplayPage:
    entries: tuple[CommittedReplayEntry, ...]
    high: int
    has_more: bool


class CommittedReplayWindow:
    def __init__(self, *, capacity: int, floor: int) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 1_000_000:
            raise ValueError("invalid replay window capacity")
        if type(floor) is not int or floor < 0:
            raise ValueError("invalid replay window floor")
        self._slots: list[CommittedReplayEntry | None] = [None] * capacity
        self._capacity, self._start, self._size = capacity, 0, 0
        self._floor = self._high = floor
        self._journal_id: str | None = None

    @property
    def high(self) -> int:
        return self._high

    def advance(self, high: int) -> None:
        """Cover a committed fact range which emitted no Consumer event."""
        if type(high) is not int or high < self._high:
            raise ValueError("replay publication regressed")
        self._high = high

    def _at(self, offset: int) -> CommittedReplayEntry:
        row = self._slots[(self._start + offset) % self._capacity]
        assert row is not None
        return row

    def append(self, row: CommittedReplayEntry) -> None:
        if (
            row.event.journal_lsn <= self._high
            or row.event.account_id != row.trace.account_id
            or (self._journal_id is not None and row.event.journal_id != self._journal_id)
        ):
            raise ValueError("replay publication is not monotonic")
        if self._size == self._capacity:
            self._floor = self._at(0).event.journal_lsn
            self._slots[self._start] = row
            self._start = (self._start + 1) % self._capacity
        else:
            self._slots[(self._start + self._size) % self._capacity] = row
            self._size += 1
        self._high = row.event.journal_lsn
        self._journal_id = row.event.journal_id

    def _right(self, lsn: int) -> int:
        left, right = 0, self._size
        while left < right:
            middle = (left + right) // 2
            if self._at(middle).event.journal_lsn <= lsn:
                left = middle + 1
            else:
                right = middle
        return left

    def page(
        self, scope: tuple[str, str, str], *, after: int, high: int, limit: int
    ) -> CommittedReplayPage | None:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("invalid replay page capacity")
        if not self._floor <= after <= high <= self._high:
            return None
        rows: list[CommittedReplayEntry] = []
        for offset in range(self._right(after), self._right(high)):
            row = self._at(offset)
            if row.scoped(scope):
                if len(rows) == limit:
                    return CommittedReplayPage(tuple(rows), high, True)
                rows.append(row)
        return CommittedReplayPage(tuple(rows), high, False)

    def bounds(
        self, scope: tuple[str, str, str], *, after: int, observed_head: int, partitions: int
    ) -> tuple[tuple[int, int, int], ...] | None:
        if type(partitions) is not int or not 1 <= partitions <= 64:
            raise ValueError("invalid replay partition capacity")
        high = min(observed_head, self._high)
        if not self._floor <= after <= high:
            return None
        # At most the configured window, never all historical orders/events.
        positions = [
            self._at(n).event.journal_lsn
            for n in range(self._right(after), self._right(high))
            if self._at(n).scoped(scope)
        ]
        count = len(positions)
        size = min(partitions, count)
        result: list[tuple[int, int, int]] = []
        for n in range(size):
            start, end = count * n // size, count * (n + 1) // size
            last = positions[end - 1]
            result.append((after, last, end - start))
            after = last
        return tuple(result)
