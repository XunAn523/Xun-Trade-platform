from __future__ import annotations

import unittest

from xuanling_tradecore.runtime.fault_monitor import (
    RUNTIME_FAULT_GAP_PAGE_V1_BYTES,
    OperationalFaultQueueV1,
    OperationalFaultSeverityV1,
    OperationalFaultV1,
    RuntimeFaultGapPageV1,
)


def _fault(sequence: int, severity: OperationalFaultSeverityV1) -> OperationalFaultV1:
    return OperationalFaultV1(sequence, "JOURNAL", severity, 1, "account-1")


class RuntimeFaultMonitorTests(unittest.TestCase):
    def test_gap_page_has_fixed_payload_free_round_trip_layout(self) -> None:
        page = RuntimeFaultGapPageV1(7, True, 11, 19, 3, 1 << 50, 23)
        self.assertEqual(RUNTIME_FAULT_GAP_PAGE_V1_BYTES, len(page.encode()))
        self.assertEqual(page, RuntimeFaultGapPageV1.decode(page.encode()))
        with self.assertRaises(ValueError):
            RuntimeFaultGapPageV1.decode(page.encode()[:-1])

    def test_critical_reserve_and_loss_latch_are_bounded_and_explicit(self) -> None:
        queue = OperationalFaultQueueV1(capacity=3, critical_reserve=1)
        self.assertTrue(queue.try_publish(_fault(1, OperationalFaultSeverityV1.WARNING)))
        self.assertTrue(queue.try_publish(_fault(2, OperationalFaultSeverityV1.ERROR)))
        self.assertFalse(queue.try_publish(_fault(3, OperationalFaultSeverityV1.WARNING)))
        self.assertTrue(queue.try_publish(_fault(4, OperationalFaultSeverityV1.CRITICAL)))
        self.assertFalse(queue.try_publish(_fault(5, OperationalFaultSeverityV1.CRITICAL)))
        latch = queue.monitoring_gap()
        assert latch is not None
        self.assertEqual(
            (3, 5, 2), (latch.first_sequence, latch.last_sequence, latch.dropped_count)
        )
        self.assertEqual((1, 2, 4), tuple(item.sequence for item in queue.drain(limit=9)))
        queue.acknowledge_gap_exported(latch)
        self.assertIsNone(queue.monitoring_gap())


if __name__ == "__main__":
    unittest.main()
