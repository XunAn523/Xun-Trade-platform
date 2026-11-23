"""Bounded weighted lanes for the Consumer V2 request reactor."""

from __future__ import annotations

from collections import deque
from enum import Enum
from typing import Generic, Mapping, TypeVar


class ConsumerOperationLaneV2(str, Enum):
    WRITE = "WRITE"
    REPLAY = "REPLAY"
    SNAPSHOT = "SNAPSHOT"
    OTHER = "OTHER"


_WRITE_OPERATIONS = frozenset({"SubmitSignal", "CancelOrder"})
_REPLAY_OPERATIONS = frozenset({"PollEvents", "CreateReplayPartitions"})
_SNAPSHOT_OPERATIONS = frozenset(
    {"GetAccountOrderSnapshot", "CreateOrderSnapshot", "ReadOrderSnapshotPage"}
)


def consumer_operation_lane_v2(operation: object) -> ConsumerOperationLaneV2:
    if not isinstance(operation, str):
        return ConsumerOperationLaneV2.OTHER
    if operation in _WRITE_OPERATIONS:
        return ConsumerOperationLaneV2.WRITE
    if operation in _REPLAY_OPERATIONS:
        return ConsumerOperationLaneV2.REPLAY
    if operation in _SNAPSHOT_OPERATIONS:
        return ConsumerOperationLaneV2.SNAPSHOT
    return ConsumerOperationLaneV2.OTHER


_T = TypeVar("_T")


class BoundedFairOperationQueueV2(Generic[_T]):
    """FIFO within a lane, weighted round-robin across non-empty lanes."""

    def __init__(
        self,
        *,
        total_capacity: int,
        lane_capacities: Mapping[ConsumerOperationLaneV2, int],
        schedule: tuple[ConsumerOperationLaneV2, ...] = (
            ConsumerOperationLaneV2.WRITE,
            ConsumerOperationLaneV2.SNAPSHOT,
            ConsumerOperationLaneV2.SNAPSHOT,
            ConsumerOperationLaneV2.WRITE,
            ConsumerOperationLaneV2.SNAPSHOT,
            ConsumerOperationLaneV2.REPLAY,
            ConsumerOperationLaneV2.SNAPSHOT,
            ConsumerOperationLaneV2.SNAPSHOT,
            ConsumerOperationLaneV2.OTHER,
        ),
    ) -> None:
        if total_capacity <= 0 or not schedule:
            raise ValueError("fair operation queue limits must be positive")
        if set(lane_capacities) != set(ConsumerOperationLaneV2) or any(
            value <= 0 for value in lane_capacities.values()
        ):
            raise ValueError("fair operation queue requires every positive lane capacity")
        if any(lane not in lane_capacities for lane in schedule):
            raise ValueError("fair operation queue schedule contains an unknown lane")
        self._total_capacity = total_capacity
        self._capacities = dict(lane_capacities)
        self._schedule = schedule
        self._cursor = 0
        self._size = 0
        self._reserved = {lane: 0 for lane in ConsumerOperationLaneV2}
        self._reserved_total = 0
        self._queues: dict[ConsumerOperationLaneV2, deque[_T]] = {
            lane: deque() for lane in ConsumerOperationLaneV2
        }

    def offer(self, item: _T, lane: ConsumerOperationLaneV2) -> bool:
        target = self._queues[lane]
        if self.used >= self._total_capacity or (
            len(target) + self._reserved[lane] >= self._capacities[lane]
        ):
            return False
        target.append(item)
        self._size += 1
        return True

    def reserve(self, lane: ConsumerOperationLaneV2) -> bool:
        """Move a popped job's credit to off-reactor execution, not new capacity."""
        if self.used >= self._total_capacity or (
            self.depth(lane) + self._reserved[lane] >= self._capacities[lane]
        ):
            return False
        self._reserved[lane] += 1
        self._reserved_total += 1
        return True

    def release(self, lane: ConsumerOperationLaneV2) -> None:
        if not self._reserved[lane]:
            raise ValueError("operation execution credit underflow")
        self._reserved[lane] -= 1
        self._reserved_total -= 1

    @property
    def used(self) -> int:
        return self._size + self._reserved_total

    def has_eligible(self, excluded: frozenset[ConsumerOperationLaneV2] = frozenset()) -> bool:
        return any(queue and lane not in excluded for lane, queue in self._queues.items())

    def pop(
        self, *, excluded: frozenset[ConsumerOperationLaneV2] = frozenset()
    ) -> tuple[_T, ConsumerOperationLaneV2]:
        if not self._size:
            raise IndexError("fair operation queue is empty")
        for _ in range(len(self._schedule)):
            lane = self._schedule[self._cursor]
            self._cursor = (self._cursor + 1) % len(self._schedule)
            queue = self._queues[lane]
            if queue and lane not in excluded:
                self._size -= 1
                return queue.popleft(), lane
        # A lane may intentionally be omitted from a custom schedule. Preserve
        # liveness while keeping the default schedule fully weighted.
        for lane, queue in self._queues.items():
            if queue and lane not in excluded:
                self._size -= 1
                return queue.popleft(), lane
        raise IndexError("fair operation queue has no eligible job")

    def discard(self, item: _T, lane: ConsumerOperationLaneV2) -> bool:
        queue = self._queues[lane]
        try:
            queue.remove(item)
        except ValueError:
            return False
        self._size -= 1
        return True

    def depth(self, lane: ConsumerOperationLaneV2) -> int:
        return len(self._queues[lane])

    def __len__(self) -> int:
        return self._size
