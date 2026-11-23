from __future__ import annotations

import unittest
from dataclasses import dataclass

from emt_gateway.fastspi import FastSpiMpscRing, FastSpiUnavailable


@dataclass(frozen=True)
class Record:
    value: int
    ring_enqueue_ns: int = 0


class FastSpiTests(unittest.TestCase):
    def test_ring_is_bounded_and_preserves_minimal_record_order(self) -> None:
        ring = FastSpiMpscRing(2)
        self.assertIsNotNone(ring.try_push(Record(1)))
        self.assertIsNotNone(ring.try_push(Record(2)))
        self.assertIsNone(ring.try_push(Record(3)))
        first = ring.pop()
        self.assertEqual(1, first.value)
        self.assertGreater(first.ring_enqueue_ns, 0)
        self.assertEqual(1, ring.depth)

    def test_native_requirement_is_explicit(self) -> None:
        candidate = FastSpiMpscRing(1)
        if candidate.native_available:
            self.assertTrue(FastSpiMpscRing(1, require_native=True).native_available)
        else:
            with self.assertRaises(FastSpiUnavailable):
                FastSpiMpscRing(1, require_native=True)
