"""DISARMED-only construction tests for the V2 native callback runtime."""

from __future__ import annotations

import unittest
from dataclasses import dataclass, field

from emt_gateway.fastspi_v2_equivalence import legacy_emt_fastspi_v2_equivalence_matrix
from emt_gateway.fastspi_v2_runtime import (
    FastSpiV2RuntimeBinding,
    FastSpiV2RuntimeBuildError,
    FastSpiV2RuntimeConfig,
    build_fastspi_v2_runtime_binding,
)
from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import InMemoryJournal
from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI
from emt_gateway.telemetry import NullPerformanceTrace
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiRuntimeSelection,
    FastSpiV2ActivationError,
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiV2IntegrityError,
    FastSpiV2Record,
    FastSpiV2Ring,
    FastSpiVersion,
)


class _TraderApiBase:
    instances = 0

    def __init__(self) -> None:
        _TraderApiBase.instances += 1


@dataclass
class _Ring:
    native_available: bool = True


@dataclass
class _GapLatch:
    active: bool = False
    producers: list[tuple[int, str]] = field(default_factory=list)
    callback_kinds: list[tuple[int, str]] = field(default_factory=list)
    rejections: list[tuple[int, int, int, int]] = field(default_factory=list)

    def register_producer(self, *, producer_id: int, label: str) -> None:
        self.producers.append((producer_id, label))

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None:
        self.callback_kinds.append((callback_kind, label))

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool:
        self.rejections.append((producer_id, callback_seq, callback_kind, observed_monotonic_ns))
        return True


class _Ingress:
    def __init__(self) -> None:
        self.offers: list[dict[str, object]] = []
        self.gaps: list[tuple[int, int]] = []

    def offer(self, **kwargs: object) -> bool:
        self.offers.append(kwargs)
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        self.gaps.append((callback_kind, observed_monotonic_ns))


class FastSpiV2RuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        _TraderApiBase.instances = 0
        self.matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        self.selection = FastSpiRuntimeSelection(
            version=FastSpiVersion.V2,
            equivalence_certificate_sha256=self.matrix.certificate_sha256,
        )
        self.current = FastSpiRuntimeSelection(version=FastSpiVersion.V1)

    @staticmethod
    def _config(account_lifecycle: str = "DISARMED") -> FastSpiV2RuntimeConfig:
        return FastSpiV2RuntimeConfig(
            account_id="emt_sim_03",
            account_profile="ordinary",
            account_lifecycle=account_lifecycle,
            producer_id=7,
            producer_label="emt-spi-7",
            ring_capacity=2,
            maximum_producers=1,
        )

    def test_binds_executable_evidence_before_building_the_official_bridge(self) -> None:
        ring = _Ring()
        latch = _GapLatch()
        ingress = _Ingress()
        binding = build_fastspi_v2_runtime_binding(
            trader_api_base=_TraderApiBase,
            config=self._config(),
            current_selection=self.current,
            selection=self.selection,
            equivalence_evidence=self.matrix,
            ring_factory=lambda capacity: ring,
            gap_latch_factory=lambda maximum_producers, maximum_callback_kinds: latch,
            ingress_factory=lambda supplied_ring, supplied_latch, producer_id: ingress,
        )
        self.assertIs(ring, binding.ring)
        self.assertIs(latch, binding.gap_latch)
        self.assertIs(ingress, binding.ingress)
        self.assertEqual([(7, "emt-spi-7")], latch.producers)
        self.assertEqual(
            tuple(enumerate(OFFICIAL_TRADER_SPI, start=1)),
            tuple(latch.callback_kinds),
        )
        self.assertEqual("OnOrderEvent", binding.callback_kinds.name_for(4))
        self.assertEqual(0, _TraderApiBase.instances)
        binding.trader_spi_type().onOrderEvent({"order_emt_id": 9})
        self.assertEqual(1, _TraderApiBase.instances)
        self.assertEqual(4, ingress.offers[0]["callback_kind"])

    def test_rejects_a_non_disarmed_build_before_any_native_factory_runs(self) -> None:
        invoked = False

        def ring_factory(capacity: int) -> _Ring:
            nonlocal invoked
            invoked = True
            return _Ring()

        with self.assertRaisesRegex(FastSpiV2ActivationError, "DISARMED"):
            build_fastspi_v2_runtime_binding(
                trader_api_base=_TraderApiBase,
                config=self._config(account_lifecycle="READY"),
                current_selection=self.current,
                selection=self.selection,
                equivalence_evidence=self.matrix,
                ring_factory=ring_factory,
            )
        self.assertFalse(invoked)

    def test_rejects_a_non_native_ring_before_registering_or_creating_an_ingress(self) -> None:
        latch_factory_called = False
        ingress_factory_called = False

        def gap_latch_factory(maximum_producers: int, maximum_callback_kinds: int) -> _GapLatch:
            nonlocal latch_factory_called
            latch_factory_called = True
            return _GapLatch()

        def ingress_factory(ring: object, latch: object, producer_id: int) -> _Ingress:
            nonlocal ingress_factory_called
            ingress_factory_called = True
            return _Ingress()

        with self.assertRaisesRegex(FastSpiV2ActivationError, "native backend"):
            build_fastspi_v2_runtime_binding(
                trader_api_base=_TraderApiBase,
                config=self._config(),
                current_selection=self.current,
                selection=self.selection,
                equivalence_evidence=self.matrix,
                ring_factory=lambda capacity: _Ring(native_available=False),
                gap_latch_factory=gap_latch_factory,
                ingress_factory=ingress_factory,
            )
        self.assertFalse(latch_factory_called)
        self.assertFalse(ingress_factory_called)

    def test_rejects_a_factory_latch_that_is_already_active(self) -> None:
        with self.assertRaisesRegex(FastSpiV2RuntimeBuildError, "fresh inactive"):
            build_fastspi_v2_runtime_binding(
                trader_api_base=_TraderApiBase,
                config=self._config(),
                current_selection=self.current,
                selection=self.selection,
                equivalence_evidence=self.matrix,
                ring_factory=lambda capacity: _Ring(),
                gap_latch_factory=lambda maximum_producers, maximum_callback_kinds: _GapLatch(
                    active=True
                ),
            )

    def test_rejects_v1_metadata_for_the_v2_builder_before_allocating_native_state(self) -> None:
        with self.assertRaisesRegex(FastSpiV2RuntimeBuildError, "V2 selection"):
            build_fastspi_v2_runtime_binding(
                trader_api_base=_TraderApiBase,
                config=self._config(),
                current_selection=self.current,
                selection=FastSpiRuntimeSelection(version=FastSpiVersion.V1),
                equivalence_evidence=self.matrix,
            )

    def test_binding_builds_worker_with_its_own_required_integrity_latch(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        latch = _GapLatch()
        binding = FastSpiV2RuntimeBinding(
            config=self._config(),
            selection=self.selection,
            ring=ring,
            gap_latch=latch,
            ingress=_Ingress(),
            callback_kinds=FastSpiV2CallbackKindRegistry(
                (FastSpiV2CallbackKind(code=4, name="OnOrderEvent"),)
            ),
            trader_spi_type=_TraderApiBase,
        )
        pipeline = CallbackPipeline(
            journal=InMemoryJournal(),
            trace=NullPerformanceTrace(),
            lookup_source=lambda record: None,
            capacity=2,
        )
        worker = binding.build_callback_worker(
            callback_pipeline=pipeline,
            monotonic_clock_ns=lambda: 200,
        )
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=99,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                )
            )
        )
        with self.assertRaisesRegex(FastSpiV2IntegrityError, "not registered"):
            worker.drain(maximum_records=1)
        self.assertEqual([(7, 1, 99, 100)], latch.rejections)


if __name__ == "__main__":
    unittest.main()
