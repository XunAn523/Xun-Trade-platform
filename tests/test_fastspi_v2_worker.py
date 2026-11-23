"""Post-ring materialization tests for the fastspi v2 callback worker."""

from __future__ import annotations

import unittest

from emt_gateway.fastspi_v2_worker import FastSpiV2CallbackPipelineWorker
from emt_gateway.gateway import CallbackPipeline, CallbackRecord
from emt_gateway.journal import InMemoryJournal
from emt_gateway.spi_bridge import _ledger_callback_fact
from emt_gateway.spi_bridge_v2 import (
    FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT,
    FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT,
    FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT,
    FASTSPI_V2_FLAG_QUERY,
    FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT,
    FASTSPI_V2_FLAG_QUERY_LAST,
    FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT,
    FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT,
    FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT,
)
from emt_gateway.telemetry import NullPerformanceTrace
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiV2IntegrityError,
    FastSpiV2Record,
    FastSpiV2Ring,
)


class _BrokerEventObserver:
    def __init__(self) -> None:
        self.events: list[object] = []

    def on_broker_event(self, event: object) -> None:
        self.events.append(event)


class _BrokerEventNormalizer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def normalize(self, record: CallbackRecord) -> object:
        if self.fail:
            raise RuntimeError("unproven identity")
        return (record.callback_name, record.order_emt_id)


class _IntegrityGapLatch:
    def __init__(self) -> None:
        self.rejections: list[tuple[int, int, int, int]] = []

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


class FastSpiV2CallbackWorkerTests(unittest.TestCase):
    def _pipeline(self) -> tuple[CallbackPipeline, InMemoryJournal]:
        journal = InMemoryJournal()
        return (
            CallbackPipeline(
                journal=journal,
                trace=NullPerformanceTrace(),
                lookup_source=lambda record: None,
                capacity=2,
            ),
            journal,
        )

    def _worker(
        self,
        ring: FastSpiV2Ring,
        pipeline: CallbackPipeline,
        *,
        integrity_gap_latch: _IntegrityGapLatch | None = None,
        broker_event_normalizer: _BrokerEventNormalizer | None = None,
        broker_event_observer: _BrokerEventObserver | None = None,
    ) -> FastSpiV2CallbackPipelineWorker:
        return FastSpiV2CallbackPipelineWorker(
            ring=ring,
            callback_kinds=FastSpiV2CallbackKindRegistry(
                (
                    FastSpiV2CallbackKind(code=4, name="OnOrderEvent"),
                    FastSpiV2CallbackKind(code=17, name="OnQueryOrder"),
                )
            ),
            account_profile="ordinary",
            callback_pipeline=pipeline,
            monotonic_clock_ns=lambda: 1_000,
            integrity_gap_latch=integrity_gap_latch or _IntegrityGapLatch(),
            broker_event_normalizer=broker_event_normalizer,
            broker_event_observer=broker_event_observer,
        )

    def test_materializes_only_after_pop_and_uses_existing_durable_callback_fact_path(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, journal = self._pipeline()
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=4,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                    order_emt_id=71,
                )
            )
        )
        result = self._worker(ring, pipeline).drain(maximum_records=1)
        self.assertEqual(1, result.drain.records_drained)
        self.assertEqual(1, len(result.deliveries))
        self.assertEqual("OnOrderEvent", result.deliveries[0].callback_name)
        self.assertEqual(["NATIVE_CALLBACK"], [record.kind for record in journal.records()])

    def test_query_flags_materialize_request_and_terminal_metadata_after_c5(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        observer = _Observer()
        journal = InMemoryJournal()
        pipeline = CallbackPipeline(
            journal=journal,
            trace=NullPerformanceTrace(),
            lookup_source=lambda record: None,
            capacity=2,
            lifecycle_observer=observer,
        )
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=17,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                    client_id=123,
                    flags=(
                        FASTSPI_V2_FLAG_QUERY
                        | FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT
                        | FASTSPI_V2_FLAG_QUERY_LAST
                    ),
                )
            )
        )
        result = self._worker(ring, pipeline).drain(maximum_records=1)
        delivery = result.deliveries[0]
        pipeline.ack(delivery.delivery_id)
        self.assertEqual("OnQueryOrder", delivery.callback_name)
        materialized = observer.enqueued[0]
        self.assertEqual(123, materialized.request_id)
        self.assertTrue(materialized.is_last)
        self.assertTrue(materialized.query_item_present)

    def test_unknown_callback_kind_fails_closed_before_journal_append(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, journal = self._pipeline()
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
        with self.assertRaisesRegex(RuntimeError, "not registered"):
            self._worker(ring, pipeline).drain(maximum_records=1)
        self.assertEqual((), journal.records())

    def test_sequence_regression_latches_fixed_gap_evidence_and_rethrows(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, _ = self._pipeline()
        latch = _IntegrityGapLatch()
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=4,
                    producer_id=7,
                    callback_seq=2,
                    observed_monotonic_ns=200,
                    order_emt_id=71,
                )
            )
        )
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=4,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                    order_emt_id=71,
                )
            )
        )
        with self.assertRaisesRegex(FastSpiV2IntegrityError, "did not advance"):
            self._worker(ring, pipeline, integrity_gap_latch=latch).drain(maximum_records=2)
        self.assertEqual([(7, 1, 4, 100)], latch.rejections)

    def test_unknown_callback_kind_latches_fixed_gap_evidence(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, _ = self._pipeline()
        latch = _IntegrityGapLatch()
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
            self._worker(ring, pipeline, integrity_gap_latch=latch).drain(maximum_records=1)
        self.assertEqual([(7, 1, 99, 100)], latch.rejections)

    def test_post_ring_domain_event_is_emitted_only_after_callback_delivery(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, journal = self._pipeline()
        observer = _BrokerEventObserver()
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=4,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                    order_emt_id=71,
                )
            )
        )
        result = self._worker(
            ring,
            pipeline,
            broker_event_normalizer=_BrokerEventNormalizer(),
            broker_event_observer=observer,
        ).drain(maximum_records=1)
        self.assertEqual(1, len(result.deliveries))
        self.assertEqual((("OnOrderEvent", 71),), result.broker_events)
        self.assertEqual(result.broker_events, tuple(observer.events))
        self.assertEqual(["NATIVE_CALLBACK"], [item.kind for item in journal.records()])

    def test_domain_normalization_failure_latches_and_rethrows(self) -> None:
        ring = FastSpiV2Ring(2, prefer_native=False)
        pipeline, _ = self._pipeline()
        latch = _IntegrityGapLatch()
        self.assertTrue(
            ring.try_push(
                FastSpiV2Record(
                    callback_kind=4,
                    producer_id=7,
                    callback_seq=1,
                    observed_monotonic_ns=100,
                    order_emt_id=71,
                )
            )
        )
        with self.assertRaisesRegex(FastSpiV2IntegrityError, "target-domain"):
            self._worker(
                ring,
                pipeline,
                integrity_gap_latch=latch,
                broker_event_normalizer=_BrokerEventNormalizer(fail=True),
                broker_event_observer=_BrokerEventObserver(),
            ).drain(maximum_records=1)
        self.assertEqual([(7, 1, 4, 100)], latch.rejections)

    def test_order_ledger_fact_matches_v1_from_presence_marked_slot_scalars(self) -> None:
        data = {
            "order_emt_id": 101,
            "order_client_id": 4,
            "order_cancel_emt_id": 0,
            "order_status": 2,
            "update_time": 20260827100000000,
            "qty_traded": 0,
            "qty_left": 100,
        }
        v1 = _ledger_callback_fact("OnOrderEvent", (data,))
        v2 = FastSpiV2CallbackPipelineWorker._ledger_callback_fact(
            "OnOrderEvent",
            FastSpiV2Record(
                callback_kind=4,
                producer_id=7,
                callback_seq=1,
                observed_monotonic_ns=100,
                order_emt_id=101,
                order_client_id=4,
                status=2,
                order_update_time=20260827100000000,
                qty_traded=0,
                qty_left=100,
                flags=(
                    FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT
                    | FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT
                    | FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT
                ),
            ),
        )
        self.assertEqual(v1, v2)

    def test_trade_ledger_fact_matches_v1_from_presence_marked_slot_scalars(self) -> None:
        data = {
            "order_emt_id": 101,
            "order_client_id": 4,
            "market": 2,
            "quantity": 100,
            "price": 1.61,
            "report_index": 99,
        }
        v1 = _ledger_callback_fact("OnTradeEvent", (data,))
        v2 = FastSpiV2CallbackPipelineWorker._ledger_callback_fact(
            "OnTradeEvent",
            FastSpiV2Record(
                callback_kind=5,
                producer_id=7,
                callback_seq=1,
                observed_monotonic_ns=100,
                order_emt_id=101,
                order_client_id=4,
                quantity=100,
                trade_market=2,
                trade_price=1.61,
                trade_report_index=99,
                flags=(
                    FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT
                    | FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT
                    | FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT
                ),
            ),
        )
        self.assertEqual(v1, v2)


if __name__ == "__main__":
    unittest.main()


class _Observer:
    def __init__(self) -> None:
        self.enqueued: list[CallbackRecord] = []

    def on_enqueued(self, record: CallbackRecord) -> None:
        self.enqueued.append(record)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        return None

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        return None
