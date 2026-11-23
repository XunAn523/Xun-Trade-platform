"""Lock-free, fixed-size owner publication for a separate read process.

There is exactly one writer (the unique OMS owner) and one read-plane process.
The odd/even sequence prevents a reader from combining fields from two cuts.
This publication is a delivery fence only: it is not a queue, recovery proof,
writer lease, or permission to synthesize an OMS receipt.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns, sleep
from typing import MutableSequence, cast

from ..oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from ..oms.segmented_fact_journal import SegmentedJournalReadBound
from ..read_model.incremental_order_projector import IncrementalProjectionHealth

_WORDS = 10
_STATE = {"NEW": 0, "RUNNING": 1, "PAUSED": 2, "FAILED": 3, "CLOSED": 4}
_STATE_NAME = {value: key for key, value in _STATE.items()}
_FENCE_READ_ATTEMPTS = 4096
_FENCE_RETRY_BUDGET_NS = 2_000_000
_FENCE_YIELD_INTERVAL = 16


@dataclass(frozen=True)
class OwnerReadFenceSnapshotV1:
    health: IncrementalProjectionHealth
    bound: SegmentedJournalReadBound


def new_owner_read_fence(context: object) -> MutableSequence[int]:
    array = getattr(context, "Array", None)
    if not callable(array):
        raise TypeError("multiprocessing context cannot create a shared fence")
    # Raw shared ctypes; no semaphore, named shared-memory segment or resource
    # tracker ownership.  Lifetime belongs to the exact spawn tree.
    return cast(MutableSequence[int], array("Q", [0] * _WORDS, lock=False))


def publish_owner_read_fence(
    shared: MutableSequence[int],
    *,
    health: IncrementalProjectionHealth,
    bound: SegmentedJournalReadBound,
) -> None:
    if len(shared) != _WORDS or health.state not in _STATE:
        raise MmapOmsFactJournalRecoveryRequired("owner read fence publication is invalid")
    values = (
        _STATE[health.state],
        health.projected_lsn,
        health.observed_committed_lsn,
        health.completed_batches,
        health.sampled_monotonic_ns,
        bound.active_segment,
        bound.active_frames,
        bound.committed_lsn,
        bound.next_batch_id,
    )
    if any(type(value) is not int or value < 0 or value >= 1 << 64 for value in values):
        raise MmapOmsFactJournalRecoveryRequired("owner read fence value is out of range")
    sequence = int(shared[0])
    if sequence & 1 or sequence >= (1 << 64) - 2:
        raise MmapOmsFactJournalRecoveryRequired("owner read fence sequence is invalid/exhausted")
    shared[0] = sequence + 1
    for index, value in enumerate(values, 1):
        shared[index] = value
    shared[0] = sequence + 2


class PublishedOwnerReadFenceV1:
    def __init__(
        self,
        shared: MutableSequence[int],
        *,
        journal_id: str,
        profile_hash: str,
        segment_frames: int,
        maximum_age_ns: int = 2_000_000_000,
    ) -> None:
        if (
            len(shared) != _WORDS
            or not journal_id
            or len(journal_id) > 128
            or len(profile_hash) != 64
            or any(value not in "0123456789abcdef" for value in profile_hash)
            or type(segment_frames) is not int
            or segment_frames < 2
            or type(maximum_age_ns) is not int
            or not 1_000_000 <= maximum_age_ns <= 60_000_000_000
        ):
            raise MmapOmsFactJournalRecoveryRequired("owner read fence scope is invalid")
        self._shared = shared
        self._journal_id, self._profile_hash = journal_id, profile_hash
        self._frames, self._maximum_age = segment_frames, maximum_age_ns

    def snapshot(self) -> OwnerReadFenceSnapshotV1:
        values: tuple[int, ...] | None = None
        deadline = monotonic_ns() + _FENCE_RETRY_BUDGET_NS
        for attempt in range(_FENCE_READ_ATTEMPTS):
            before = int(self._shared[0])
            if before and not before & 1:
                candidate = tuple(int(self._shared[index]) for index in range(1, _WORDS))
                after = int(self._shared[0])
                if before == after and not after & 1:
                    values = candidate
                    break
            # A live publisher may be descheduled with an odd sequence. Let
            # it finish, but never deliver a torn value or wait indefinitely.
            if (attempt + 1) % _FENCE_YIELD_INTERVAL == 0:
                if monotonic_ns() >= deadline:
                    break
                sleep(0)
                if monotonic_ns() >= deadline:
                    break
        if values is None:
            raise MmapOmsFactJournalRecoveryRequired("owner read fence is unavailable/torn")
        state_id, projected, observed, batches, sampled, segment, frames, committed, next_batch = (
            values
        )
        state = _STATE_NAME.get(state_id)
        health = IncrementalProjectionHealth(
            state or "FAILED",
            projected,
            observed,
            batches,
            sampled,
            None if state in {"RUNNING", "PAUSED"} else "OWNER_READ_FENCE_UNAVAILABLE",
        )
        bound = SegmentedJournalReadBound(
            self._journal_id,
            self._profile_hash,
            self._frames,
            segment,
            frames,
            committed,
            next_batch,
        )
        if (
            state not in {"RUNNING", "PAUSED"}
            or health.error_code is not None
            or not 0 <= monotonic_ns() - sampled <= self._maximum_age
            # The Journal may advance between the projector's last observed
            # cut and this publication.  That is ordinary bounded lag, not a
            # torn fence.  Reads still use the exact newer Journal bound while
            # SQL operations fail closed against the watermark they require.
            or not projected <= observed <= committed
            or segment < 1
            or not 0 <= frames <= self._frames
            or next_batch < 1
        ):
            raise MmapOmsFactJournalRecoveryRequired("owner read fence failed/stale")
        return OwnerReadFenceSnapshotV1(health, bound)

    def health(self) -> IncrementalProjectionHealth:
        return self.snapshot().health

    def read_bound(self) -> SegmentedJournalReadBound:
        return self.snapshot().bound
