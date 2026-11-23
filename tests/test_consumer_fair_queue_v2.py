from __future__ import annotations

import unittest

from xuanling_tradecore.consumer import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
    consumer_operation_lane_v2,
)


class ConsumerFairOperationQueueV2Tests(unittest.TestCase):
    def _queue(self) -> BoundedFairOperationQueueV2[str]:
        return BoundedFairOperationQueueV2(
            total_capacity=8,
            lane_capacities={
                ConsumerOperationLaneV2.WRITE: 8,
                ConsumerOperationLaneV2.REPLAY: 8,
                ConsumerOperationLaneV2.SNAPSHOT: 2,
                ConsumerOperationLaneV2.OTHER: 1,
            },
        )

    def test_classifies_only_known_text_operations(self) -> None:
        self.assertEqual(
            ConsumerOperationLaneV2.WRITE, consumer_operation_lane_v2("SubmitSignal")
        )
        self.assertEqual(
            ConsumerOperationLaneV2.REPLAY, consumer_operation_lane_v2("PollEvents")
        )
        self.assertEqual(
            ConsumerOperationLaneV2.SNAPSHOT,
            consumer_operation_lane_v2("ReadOrderSnapshotPage"),
        )
        self.assertEqual(ConsumerOperationLaneV2.OTHER, consumer_operation_lane_v2({}))

    def test_bounds_each_lane_and_total_capacity(self) -> None:
        queue = self._queue()
        self.assertTrue(queue.offer("s1", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertTrue(queue.offer("s2", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertFalse(queue.offer("s3", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertTrue(queue.offer("o1", ConsumerOperationLaneV2.OTHER))
        self.assertFalse(queue.offer("o2", ConsumerOperationLaneV2.OTHER))
        for index in range(5):
            self.assertTrue(queue.offer(f"w{index}", ConsumerOperationLaneV2.WRITE))
        self.assertEqual(8, len(queue))
        self.assertFalse(queue.offer("overflow", ConsumerOperationLaneV2.REPLAY))

    def test_weighted_schedule_preserves_fifo_and_all_lanes_progress(self) -> None:
        queue = self._queue()
        for value, lane in (
            ("w1", ConsumerOperationLaneV2.WRITE),
            ("w2", ConsumerOperationLaneV2.WRITE),
            ("r1", ConsumerOperationLaneV2.REPLAY),
            ("r2", ConsumerOperationLaneV2.REPLAY),
            ("s1", ConsumerOperationLaneV2.SNAPSHOT),
            ("o1", ConsumerOperationLaneV2.OTHER),
        ):
            self.assertTrue(queue.offer(value, lane))
        drained = tuple(queue.pop() for _ in range(6))
        values = tuple(value for value, _lane in drained)
        self.assertLess(values.index("w1"), values.index("w2"))
        self.assertLess(values.index("r1"), values.index("r2"))
        self.assertEqual({"w1", "w2", "r1", "r2", "s1", "o1"}, set(values))
        self.assertEqual(0, len(queue))

    def test_discard_releases_lane_capacity(self) -> None:
        queue = self._queue()
        self.assertTrue(queue.offer("s1", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertTrue(queue.discard("s1", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertFalse(queue.discard("s1", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertTrue(queue.offer("s2", ConsumerOperationLaneV2.SNAPSHOT))

    def test_off_reactor_reservations_share_original_lane_and_total_capacity(self) -> None:
        queue = self._queue()
        self.assertTrue(queue.reserve(ConsumerOperationLaneV2.SNAPSHOT))
        self.assertTrue(queue.offer("s1", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertFalse(queue.reserve(ConsumerOperationLaneV2.SNAPSHOT))
        self.assertFalse(queue.offer("s2", ConsumerOperationLaneV2.SNAPSHOT))
        for _ in range(6):
            self.assertTrue(queue.reserve(ConsumerOperationLaneV2.WRITE))
        self.assertEqual(8, queue.used)
        self.assertEqual(1, len(queue))
        self.assertFalse(queue.offer("extra", ConsumerOperationLaneV2.REPLAY))
        queue.release(ConsumerOperationLaneV2.SNAPSHOT)
        self.assertEqual(7, queue.used)
        self.assertTrue(queue.offer("extra", ConsumerOperationLaneV2.REPLAY))
        with self.assertRaises(ValueError):
            queue.release(ConsumerOperationLaneV2.SNAPSHOT)

    def test_blocked_write_owner_does_not_block_eligible_read_or_reorder_writes(self) -> None:
        queue = self._queue()
        writes = frozenset({ConsumerOperationLaneV2.WRITE})
        for value in ("w1", "w2"):
            self.assertTrue(queue.offer(value, ConsumerOperationLaneV2.WRITE))
        self.assertTrue(queue.offer("read", ConsumerOperationLaneV2.SNAPSHOT))
        self.assertEqual("read", queue.pop(excluded=writes)[0])
        self.assertFalse(queue.has_eligible(writes))
        with self.assertRaises(IndexError):
            queue.pop(excluded=writes)
        self.assertEqual(["w1", "w2"], [queue.pop()[0], queue.pop()[0]])

    def test_cost_weighted_schedule_gives_snapshot_five_of_seven_busy_slots(self) -> None:
        queue = BoundedFairOperationQueueV2(
            total_capacity=100,
            lane_capacities={lane: 100 for lane in ConsumerOperationLaneV2},
        )
        for index in range(20):
            self.assertTrue(queue.offer(f"w{index}", ConsumerOperationLaneV2.WRITE))
        for index in range(50):
            self.assertTrue(queue.offer(f"s{index}", ConsumerOperationLaneV2.SNAPSHOT))
        lanes = [queue.pop()[1] for _ in range(70)]
        self.assertEqual(20, lanes.count(ConsumerOperationLaneV2.WRITE))
        self.assertEqual(50, lanes.count(ConsumerOperationLaneV2.SNAPSHOT))


if __name__ == "__main__":
    unittest.main()
