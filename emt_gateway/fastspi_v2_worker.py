"""Post-ring materialization worker for fixed fastspi v2 callback records.

The module is intentionally not imported by the V2 TraderSpi bridge.  All
object allocation, UUID creation, trace lifecycle and durable Journal work
begin only after :class:`FastSpiV2Consumer` has popped a native record.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from typing import Callable, Protocol
from uuid import uuid4

from xuanling_tradecore.domain import BrokerEvent
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiV2CallbackKindRegistry,
    FastSpiV2Consumer,
    FastSpiV2DrainResult,
    FastSpiV2IntegrityError,
    FastSpiV2Record,
    FastSpiV2Ring,
)

from .gateway import CallbackDelivery, CallbackPipeline, CallbackRecord
from .order_ledger import LedgerCallbackFact
from .spi_bridge import (
    EmtLedgerCallbackScalars,
    ledger_callback_fact_from_scalars,
    profile_for_callback,
    workload_for_callback,
)
from .spi_bridge_v2 import (
    FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT,
    FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT,
    FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT,
    FASTSPI_V2_FLAG_QUERY,
    FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT,
    FASTSPI_V2_FLAG_QUERY_LAST,
    FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT,
    FASTSPI_V2_FLAG_TRADE_EXECUTION_ID_PRESENT,
    FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT,
    FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT,
    FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT,
)


@dataclass(frozen=True)
class FastSpiV2CallbackWorkerResult:
    """One bounded post-ring materialization result with durable deliveries."""

    drain: FastSpiV2DrainResult
    deliveries: tuple[CallbackDelivery, ...]
    broker_events: tuple[BrokerEvent, ...] = ()


class FastSpiV2WorkerIntegrityGapLatchPort(Protocol):
    """Post-C5 recording seam for an impossible consumed slot."""

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool: ...


class FastSpiV2BrokerEventNormalizerPort(Protocol):
    def normalize(self, record: CallbackRecord) -> BrokerEvent: ...


class FastSpiV2BrokerEventObserverPort(Protocol):
    def on_broker_event(self, event: BrokerEvent) -> None: ...


class FastSpiV2CallbackPipelineWorker:
    """Convert popped V2 scalar slots into the existing callback fact pipeline.

    The slot intentionally does not carry a raw EMT payload.  Its bounded
    scalar extension is instead reconstructed through the same legacy ledger
    fact constructor as V1, after consumer pop.  A missing presence flag never
    becomes a guessed source-key field.
    """

    def __init__(
        self,
        *,
        ring: FastSpiV2Ring,
        callback_kinds: FastSpiV2CallbackKindRegistry,
        account_profile: str,
        callback_pipeline: CallbackPipeline,
        integrity_gap_latch: FastSpiV2WorkerIntegrityGapLatchPort,
        broker_event_normalizer: FastSpiV2BrokerEventNormalizerPort | None = None,
        broker_event_observer: FastSpiV2BrokerEventObserverPort | None = None,
        monotonic_clock_ns: Callable[[], int] = perf_counter_ns,
    ) -> None:
        if not account_profile:
            raise ValueError("fastspi v2 worker account_profile is required")
        self._consumer = FastSpiV2Consumer(ring=ring)
        self._callback_kinds = callback_kinds
        self._account_profile = account_profile
        self._callback_pipeline = callback_pipeline
        self._monotonic_clock_ns = monotonic_clock_ns
        self._integrity_gap_latch = integrity_gap_latch
        if (broker_event_normalizer is None) != (broker_event_observer is None):
            raise ValueError("fastspi v2 target-domain normalizer and observer must be paired")
        self._broker_event_normalizer = broker_event_normalizer
        self._broker_event_observer = broker_event_observer

    def drain(self, *, maximum_records: int) -> FastSpiV2CallbackWorkerResult:
        """Materialize and durably normalize at most ``maximum_records`` slots."""

        deliveries: list[CallbackDelivery] = []
        broker_events: list[BrokerEvent] = []

        def handle(record: FastSpiV2Record) -> None:
            callback_record = self._materialize(record)
            deliveries.append(self._callback_pipeline.normalize_post_ring_record(callback_record))
            if self._broker_event_normalizer is None or self._broker_event_observer is None:
                return
            try:
                event = self._broker_event_normalizer.normalize(callback_record)
                self._broker_event_observer.on_broker_event(event)
            except Exception as error:
                raise FastSpiV2IntegrityError(
                    "fastspi v2 target-domain callback normalization failed", record=record
                ) from error
            broker_events.append(event)

        try:
            drain = self._consumer.drain(maximum_records=maximum_records, handle_record=handle)
        except FastSpiV2IntegrityError as error:
            self._record_integrity_gap(error)
            raise
        return FastSpiV2CallbackWorkerResult(
            drain=drain,
            deliveries=tuple(deliveries),
            broker_events=tuple(broker_events),
        )

    def _materialize(self, record: FastSpiV2Record) -> CallbackRecord:
        try:
            callback_name = self._callback_kinds.name_for(record.callback_kind)
        except FastSpiV2IntegrityError as error:
            raise FastSpiV2IntegrityError(str(error), record=record) from error
        is_query = bool(record.flags & FASTSPI_V2_FLAG_QUERY)
        return CallbackRecord(
            callback_correlation_id=str(uuid4()),
            callback_name=callback_name,
            profile=profile_for_callback(callback_name, self._account_profile),
            workload=workload_for_callback(callback_name),
            callback_entry_ns=record.observed_monotonic_ns,
            ring_enqueue_ns=max(record.observed_monotonic_ns, self._monotonic_clock_ns()),
            order_emt_id=record.order_emt_id or None,
            operation_native_id=record.order_cancel_emt_id or None,
            producer_id=str(record.producer_id),
            callback_seq=record.callback_seq,
            request_id=record.client_id if is_query and record.client_id else None,
            is_last=bool(record.flags & FASTSPI_V2_FLAG_QUERY_LAST) if is_query else None,
            query_item_present=(
                bool(record.flags & FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT) if is_query else None
            ),
            query_reference=(
                record.query_reference
                if is_query and record.flags & FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT
                else None
            ),
            ledger_fact=self._ledger_callback_fact(callback_name, record),
        )

    def _record_integrity_gap(self, error: FastSpiV2IntegrityError) -> None:
        """Latch an impossible consumed POD record without hiding the failure."""

        record = error.record
        if record is None:
            return
        self._integrity_gap_latch.record_native_rejection(
            producer_id=record.producer_id,
            callback_seq=record.callback_seq,
            callback_kind=record.callback_kind,
            observed_monotonic_ns=record.observed_monotonic_ns,
        )

    @staticmethod
    def _ledger_callback_fact(
        callback_name: str, record: FastSpiV2Record
    ) -> LedgerCallbackFact | None:
        """Build a V1-identical legacy fact only from presence-marked scalars."""

        return ledger_callback_fact_from_scalars(
            callback_name,
            EmtLedgerCallbackScalars(
                order_emt_id=record.order_emt_id or None,
                client_order_id=str(record.order_client_id) if record.order_client_id else None,
                status=record.status,
                cancel_emt_id=record.order_cancel_emt_id or None,
                update_time=(
                    str(record.order_update_time)
                    if record.flags & FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT
                    else None
                ),
                qty_traded=(
                    record.qty_traded
                    if record.flags & FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT
                    else None
                ),
                qty_left=(
                    record.qty_left
                    if record.flags & FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT
                    else None
                ),
                market=(
                    str(record.trade_market)
                    if record.flags & FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT
                    else None
                ),
                quantity=record.quantity if record.quantity > 0 else None,
                price=(
                    repr(record.trade_price)
                    if record.flags & FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT
                    else None
                ),
                report_index=(
                    record.trade_report_index
                    if record.flags & FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT
                    else None
                ),
                exec_id=(
                    record.trade_execution_id.decode("utf-8")
                    if record.flags & FASTSPI_V2_FLAG_TRADE_EXECUTION_ID_PRESENT
                    else None
                ),
            ),
        )
