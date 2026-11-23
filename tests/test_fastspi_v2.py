"""Contract tests for the fixed-record M2-N fastspi v2 boundary."""

from __future__ import annotations

import unittest

from emt_gateway.fastspi_v2_equivalence import legacy_emt_fastspi_v2_equivalence_matrix
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiRuntimeSelection,
    FastSpiV2ActivationError,
    FastSpiV2CallbackIngress,
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiV2Consumer,
    FastSpiV2IntegrityError,
    FastSpiV2Record,
    FastSpiV2Ring,
    FastSpiV2SequenceAllocator,
    FastSpiV2Unavailable,
    FastSpiVersion,
)


class FastSpiV2Tests(unittest.TestCase):
    def _record(self, sequence: int) -> FastSpiV2Record:
        return FastSpiV2Record(
            callback_kind=7,
            producer_id=3,
            callback_seq=sequence,
            observed_monotonic_ns=1_000 + sequence,
            order_emt_id=99,
            quantity=100,
            error_bytes=b"bounded",
        )

    def test_fixed_record_is_bounded_and_ring_preserves_order(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        first = self._record(1)
        second = self._record(2)
        self.assertTrue(ring.try_push(first))
        self.assertTrue(ring.try_push(second))
        self.assertFalse(ring.try_push(self._record(3)))
        self.assertTrue(ring.wait_for_items(0))
        self.assertEqual(1, ring.drain_wakeup())
        self.assertEqual(first, ring.pop())
        self.assertEqual(second, ring.pop())
        self.assertIsNone(ring.pop())
        snapshot = ring.snapshot()
        self.assertEqual(2, snapshot.high_watermark)
        self.assertEqual(1, snapshot.overflow_count)
        self.assertFalse(snapshot.native_available)

    def test_sequence_allocator_never_reuses_a_rejected_sequence(self) -> None:
        allocator = FastSpiV2SequenceAllocator(maximum_producers=2)
        self.assertEqual(1, allocator.reserve(1))
        self.assertEqual(2, allocator.reserve(1))
        self.assertEqual(1, allocator.reserve(2))

    def test_rejects_unbounded_records_and_invalid_capacity(self) -> None:
        with self.assertRaisesRegex(ValueError, "power of two"):
            FastSpiV2Ring(3, prefer_native=False)
        with self.assertRaisesRegex(ValueError, "error_bytes"):
            FastSpiV2Record(
                callback_kind=1,
                producer_id=1,
                callback_seq=1,
                observed_monotonic_ns=1,
                error_bytes=b"x" * 193,
            )
        with self.assertRaisesRegex(ValueError, "valid UTF-8"):
            FastSpiV2Record(
                callback_kind=1,
                producer_id=1,
                callback_seq=1,
                observed_monotonic_ns=1,
                error_bytes=b"\xff",
            )

    def test_native_requirement_is_explicit(self) -> None:
        candidate = FastSpiV2Ring(2)
        if candidate.native_available:
            self.assertTrue(FastSpiV2Ring(2, require_native=True).native_available)
        else:
            with self.assertRaises(FastSpiV2Unavailable):
                FastSpiV2Ring(2, require_native=True)

    def test_overflow_records_a_gap_without_retrying_or_reusing_sequence(self) -> None:
        latch = _GapLatch()
        ring = FastSpiV2Ring(2, prefer_native=False)
        ingress = FastSpiV2CallbackIngress(
            ring=ring,
            sequence_allocator=FastSpiV2SequenceAllocator(),
            callback_gap_latch=latch,
            producer_id=3,
            producer_label="emt-spi-3",
        )
        self.assertEqual(
            1,
            ingress.offer(
                callback_kind=7,
                callback_kind_label="OnOrderEvent",
                observed_monotonic_ns=100,
            ),
        )
        self.assertEqual(
            2,
            ingress.offer(
                callback_kind=7,
                callback_kind_label="OnOrderEvent",
                observed_monotonic_ns=101,
            ),
        )
        self.assertIsNone(
            ingress.offer(
                callback_kind=7,
                callback_kind_label="OnOrderEvent",
                observed_monotonic_ns=102,
            )
        )
        snapshot = ingress.snapshot()
        self.assertTrue(snapshot.disarmed)
        self.assertTrue(snapshot.callback_gap_pending)
        self.assertEqual(("emt-spi-3", 3, "OnOrderEvent", 102), latch.rejections[0])
        self.assertIsNotNone(ring.pop())
        self.assertIsNone(
            ingress.offer(
                callback_kind=7,
                callback_kind_label="OnOrderEvent",
                observed_monotonic_ns=103,
            )
        )
        self.assertEqual(
            (
                ("emt-spi-3", 3, "OnOrderEvent", 102),
                ("emt-spi-3", 4, "OnOrderEvent", 103),
            ),
            tuple(latch.rejections),
        )
        remaining = ring.pop()
        assert remaining is not None
        self.assertEqual(2, remaining.callback_seq)
        self.assertIsNone(ring.pop())

    def test_consumer_drains_bounded_records_and_detects_sequence_regression(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        consumer = FastSpiV2Consumer(ring=ring)
        observed: list[int] = []
        self.assertTrue(ring.try_push(self._record(1)))
        self.assertTrue(ring.try_push(self._record(2)))
        first = consumer.drain(
            maximum_records=1, handle_record=lambda record: observed.append(record.callback_seq)
        )
        self.assertEqual(1, first.records_drained)
        self.assertTrue(first.more_available)
        second = consumer.drain(
            maximum_records=2, handle_record=lambda record: observed.append(record.callback_seq)
        )
        self.assertEqual(1, second.records_drained)
        self.assertFalse(second.more_available)
        self.assertEqual([1, 2], observed)
        self.assertTrue(ring.try_push(self._record(2)))
        with self.assertRaisesRegex(FastSpiV2IntegrityError, "did not advance"):
            consumer.drain(maximum_records=1, handle_record=lambda record: None)

    def test_callback_kind_registry_is_immutable_and_fail_closed(self) -> None:
        registry = FastSpiV2CallbackKindRegistry(
            (FastSpiV2CallbackKind(code=7, name="OnOrderEvent"),)
        )
        self.assertEqual("OnOrderEvent", registry.name_for(7))
        with self.assertRaisesRegex(FastSpiV2IntegrityError, "not registered"):
            registry.name_for(8)

    def test_v2_runtime_selection_requires_disarmed_native_and_equivalence_evidence(self) -> None:
        v1 = FastSpiRuntimeSelection(version=FastSpiVersion.V1)
        v2 = FastSpiRuntimeSelection(
            version=FastSpiVersion.V2,
            equivalence_certificate_sha256="a" * 64,
        )
        v2.validate_disarmed_transition(
            current=v1,
            account_lifecycle="DISARMED",
            native_v2_available=True,
        )
        with self.assertRaisesRegex(FastSpiV2ActivationError, "DISARMED"):
            v2.validate_disarmed_transition(
                current=v1,
                account_lifecycle="READY",
                native_v2_available=True,
            )
        with self.assertRaisesRegex(FastSpiV2ActivationError, "native backend"):
            v2.validate_disarmed_transition(
                current=v1,
                account_lifecycle="DISARMED",
                native_v2_available=False,
            )
        with self.assertRaisesRegex(ValueError, "equivalence certificate"):
            FastSpiRuntimeSelection(version=FastSpiVersion.V2)

    def test_v2_runtime_binding_requires_matching_executable_evidence(self) -> None:
        matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        current = FastSpiRuntimeSelection(version=FastSpiVersion.V1)
        selection = FastSpiRuntimeSelection(
            version=FastSpiVersion.V2,
            equivalence_certificate_sha256=matrix.certificate_sha256,
        )
        selection.validate_runtime_binding(
            current=current,
            account_lifecycle="DISARMED",
            native_v2_available=True,
            equivalence_evidence=matrix,
        )
        with self.assertRaisesRegex(FastSpiV2ActivationError, "does not match"):
            FastSpiRuntimeSelection(
                version=FastSpiVersion.V2,
                equivalence_certificate_sha256="a" * 64,
            ).validate_runtime_binding(
                current=current,
                account_lifecycle="DISARMED",
                native_v2_available=True,
                equivalence_evidence=matrix,
            )


class _GapLatch:
    def __init__(self) -> None:
        self.rejections: list[tuple[str, int, str, int]] = []

    @property
    def active(self) -> bool:
        return bool(self.rejections)

    def record_rejection(
        self,
        *,
        producer_id: str,
        callback_seq: int,
        callback_kind: str,
        observed_monotonic_ns: int,
    ) -> None:
        self.rejections.append((producer_id, callback_seq, callback_kind, observed_monotonic_ns))


if __name__ == "__main__":
    unittest.main()
