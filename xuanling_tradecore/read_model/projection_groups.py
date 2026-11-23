"""Bounded, non-waiting grouping of physically verified projection input."""

from typing import Callable

from ..oms.journal_tail import SegmentedJournalTail, VerifiedJournalBatch
from ..persistence.order_projection_store import OrderProjectionError


class VerifiedProjectionGroups:
    """Keep at most one complete lookahead batch, never split or drop a batch.

    The lookahead is bounded by the tail's existing maximum_batch_facts. It is
    not part of the current SQL transaction's unchanged 512-fact allowance.
    Neither this reader nor the tail waits for future source commits.
    """

    def __init__(
        self,
        tail: SegmentedJournalTail,
        maximum_batches: int,
        stopped: Callable[[], bool] = lambda: False,
        *,
        freeze_source_bound: bool = False,
    ) -> None:
        if (
            type(maximum_batches) is not int
            or not 1 <= maximum_batches <= 256
            or type(freeze_source_bound) is not bool
        ):
            raise OrderProjectionError("invalid projection transaction batch budget")
        self._tail, self._maximum, self._stopped = tail, maximum_batches, stopped
        self._pending: VerifiedJournalBatch | None = None
        self._freeze_source_bound = freeze_source_bound

    def read_next(self) -> tuple[VerifiedJournalBatch, ...]:
        if self._stopped():
            return ()
        if self._freeze_source_bound:
            with self._tail.source_read_window():
                return self._read_next()
        return self._read_next()

    def _read_next(self) -> tuple[VerifiedJournalBatch, ...]:
        if self._stopped():
            return ()
        first = self._pending if self._pending is not None else self._tail.read_next()
        self._pending = None
        if first is None:
            return ()
        group, facts = [first], len(first.records)
        if not 1 <= facts <= 512:
            raise OrderProjectionError("projection source batch exceeds transaction budget")
        while len(group) < self._maximum and facts < 512 and not self._stopped():
            following = self._tail.read_next()
            if following is None:
                break
            count = len(following.records)
            if not 1 <= count <= 512:
                raise OrderProjectionError("projection source batch exceeds transaction budget")
            if facts + count > 512:
                self._pending = following
                break
            group.append(following)
            facts += count
        return tuple(group)
