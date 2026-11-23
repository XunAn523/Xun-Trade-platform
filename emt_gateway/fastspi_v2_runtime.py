"""DISARMED-only construction of the native fastspi V2 callback runtime.

The builder intentionally stops before SDK loading, ``createTraderApi`` or
login.  Its only job is to bind the executable V2-equivalence gate to the
native ring/latch/ingress and the official Python TraderSpi subclass.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol

from xuanling_tradecore.native.fastspi_v2 import (
    MAX_PRODUCER_ID,
    FastSpiRuntimeSelection,
    FastSpiV2ActivationError,
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiV2EquivalenceEvidencePort,
    FastSpiV2NativeGapLatch,
    FastSpiV2NativeIngress,
    FastSpiV2Ring,
    FastSpiVersion,
)

from .fastspi_v2_worker import (
    FastSpiV2BrokerEventNormalizerPort,
    FastSpiV2BrokerEventObserverPort,
    FastSpiV2CallbackPipelineWorker,
)
from .gateway import CallbackPipeline
from .spi_bridge import OFFICIAL_TRADER_SPI
from .spi_bridge_v2 import (
    FastSpiV2NativeIngressPort,
    build_trader_spi_v2_bridge,
    register_official_trader_spi_v2_kinds,
)


class FastSpiV2RuntimeBuildError(RuntimeError):
    """A V2 callback runtime could not be safely bound while DISARMED."""


class FastSpiV2RuntimeRingPort(Protocol):
    @property
    def native_available(self) -> bool: ...


class FastSpiV2RuntimeGapLatchPort(Protocol):
    @property
    def active(self) -> bool: ...

    def register_producer(self, *, producer_id: int, label: str) -> None: ...

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None: ...

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool: ...


@dataclass(frozen=True)
class FastSpiV2RuntimeConfig:
    """Non-secret local inputs for one account's V2 callback runtime."""

    account_id: str
    account_profile: str
    account_lifecycle: str
    producer_id: int
    producer_label: str
    ring_capacity: int = 1024
    maximum_producers: int = 1

    def validate(self) -> None:
        if not self.account_id or not self.account_id.isascii() or len(self.account_id) > 128:
            raise FastSpiV2RuntimeBuildError("fastspi v2 runtime account identity is invalid")
        if self.account_profile not in {"ordinary", "credit", "option"}:
            raise FastSpiV2RuntimeBuildError("fastspi v2 runtime account profile is invalid")
        if self.account_lifecycle != "DISARMED":
            raise FastSpiV2ActivationError(
                "fastspi v2 runtime can be built only while the account is DISARMED"
            )
        if not 1 <= self.producer_id <= MAX_PRODUCER_ID:
            raise FastSpiV2RuntimeBuildError("fastspi v2 runtime producer id is invalid")
        if (
            not self.producer_label
            or not self.producer_label.isascii()
            or len(self.producer_label) > 128
        ):
            raise FastSpiV2RuntimeBuildError("fastspi v2 runtime producer label is invalid")
        if not 1 <= self.maximum_producers <= 16:
            raise FastSpiV2RuntimeBuildError("fastspi v2 runtime producer bound is invalid")


@dataclass(frozen=True)
class FastSpiV2RuntimeBinding:
    """Prepared native callback components; this value cannot open an SDK session."""

    config: FastSpiV2RuntimeConfig
    selection: FastSpiRuntimeSelection
    ring: FastSpiV2RuntimeRingPort
    gap_latch: FastSpiV2RuntimeGapLatchPort
    ingress: FastSpiV2NativeIngressPort
    callback_kinds: FastSpiV2CallbackKindRegistry
    trader_spi_type: type[Any]

    def build_callback_worker(
        self,
        *,
        callback_pipeline: CallbackPipeline,
        broker_event_normalizer: FastSpiV2BrokerEventNormalizerPort | None = None,
        broker_event_observer: FastSpiV2BrokerEventObserverPort | None = None,
        monotonic_clock_ns: Callable[[], int] | None = None,
    ) -> FastSpiV2CallbackPipelineWorker:
        """Create the post-ring worker with this runtime's required gap latch."""

        if not isinstance(self.ring, FastSpiV2Ring):
            raise FastSpiV2RuntimeBuildError(
                "fastspi v2 runtime binding cannot build a worker from a foreign ring"
            )
        if monotonic_clock_ns is not None:
            return FastSpiV2CallbackPipelineWorker(
                ring=self.ring,
                callback_kinds=self.callback_kinds,
                account_profile=self.config.account_profile,
                callback_pipeline=callback_pipeline,
                integrity_gap_latch=self.gap_latch,
                broker_event_normalizer=broker_event_normalizer,
                broker_event_observer=broker_event_observer,
                monotonic_clock_ns=monotonic_clock_ns,
            )
        return FastSpiV2CallbackPipelineWorker(
            ring=self.ring,
            callback_kinds=self.callback_kinds,
            account_profile=self.config.account_profile,
            callback_pipeline=callback_pipeline,
            integrity_gap_latch=self.gap_latch,
            broker_event_normalizer=broker_event_normalizer,
            broker_event_observer=broker_event_observer,
        )


FastSpiV2RingFactory = Callable[[int], FastSpiV2RuntimeRingPort]
FastSpiV2GapLatchFactory = Callable[[int, int], FastSpiV2RuntimeGapLatchPort]
FastSpiV2IngressFactory = Callable[
    [FastSpiV2RuntimeRingPort, FastSpiV2RuntimeGapLatchPort, int], FastSpiV2NativeIngressPort
]


def build_fastspi_v2_runtime_binding(
    *,
    trader_api_base: type[Any],
    config: FastSpiV2RuntimeConfig,
    current_selection: FastSpiRuntimeSelection,
    selection: FastSpiRuntimeSelection,
    equivalence_evidence: FastSpiV2EquivalenceEvidencePort,
    ring_factory: FastSpiV2RingFactory | None = None,
    gap_latch_factory: FastSpiV2GapLatchFactory | None = None,
    ingress_factory: FastSpiV2IngressFactory | None = None,
) -> FastSpiV2RuntimeBinding:
    """Bind a native V2 callback runtime after every fail-closed prerequisite.

    The builder allocates only local native callback structures and creates a
    Python subclass. It never imports an EMT SDK, constructs a vendor API,
    makes a directory, or invokes any native session method.
    """

    config.validate()
    if selection.version is not FastSpiVersion.V2:
        raise FastSpiV2RuntimeBuildError("fastspi v2 runtime requires a V2 selection")
    actual_ring_factory = ring_factory or _native_ring
    ring = actual_ring_factory(config.ring_capacity)
    selection.validate_runtime_binding(
        current=current_selection,
        account_lifecycle=config.account_lifecycle,
        native_v2_available=ring.native_available,
        equivalence_evidence=equivalence_evidence,
    )
    actual_gap_latch_factory = gap_latch_factory or _native_gap_latch
    gap_latch = actual_gap_latch_factory(config.maximum_producers, len(OFFICIAL_TRADER_SPI))
    if gap_latch.active:
        raise FastSpiV2RuntimeBuildError(
            "fastspi v2 runtime requires a fresh inactive callback-gap latch"
        )
    gap_latch.register_producer(producer_id=config.producer_id, label=config.producer_label)
    register_official_trader_spi_v2_kinds(gap_latch)
    actual_ingress_factory = ingress_factory or _native_ingress
    ingress = actual_ingress_factory(ring, gap_latch, config.producer_id)
    callback_kinds = FastSpiV2CallbackKindRegistry(
        tuple(
            FastSpiV2CallbackKind(code=callback_kind, name=official_name)
            for callback_kind, official_name in enumerate(OFFICIAL_TRADER_SPI, start=1)
        )
    )
    return FastSpiV2RuntimeBinding(
        config=config,
        selection=selection,
        ring=ring,
        gap_latch=gap_latch,
        ingress=ingress,
        callback_kinds=callback_kinds,
        trader_spi_type=build_trader_spi_v2_bridge(trader_api_base, callback_ingress=ingress),
    )


def _native_ring(capacity: int) -> FastSpiV2Ring:
    return FastSpiV2Ring(capacity, require_native=True)


def _native_gap_latch(
    maximum_producers: int,
    maximum_callback_kinds: int,
) -> FastSpiV2NativeGapLatch:
    return FastSpiV2NativeGapLatch(
        maximum_producers=maximum_producers,
        maximum_callback_kinds=maximum_callback_kinds,
    )


def _native_ingress(
    ring: FastSpiV2RuntimeRingPort,
    gap_latch: FastSpiV2RuntimeGapLatchPort,
    producer_id: int,
) -> FastSpiV2NativeIngress:
    if not isinstance(ring, FastSpiV2Ring) or not isinstance(gap_latch, FastSpiV2NativeGapLatch):
        raise FastSpiV2RuntimeBuildError(
            "fastspi v2 native runtime factories returned invalid types"
        )
    return FastSpiV2NativeIngress(ring=ring, gap_latch=gap_latch, producer_id=producer_id)
