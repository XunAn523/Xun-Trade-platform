"""End-to-end public P0 callback parity vectors for fastspi V1 and V2."""

from __future__ import annotations

import unittest
from typing import cast

from emt_gateway.fastspi_v2_audit import assert_fastspi_v2_c5_source_constrained
from emt_gateway.fastspi_v2_worker import FastSpiV2CallbackPipelineWorker
from emt_gateway.gateway import CallbackDelivery, CallbackPipeline, CallbackRecord
from emt_gateway.journal import InMemoryJournal
from emt_gateway.spi_bridge import build_trader_spi_bridge, python_callback_name
from emt_gateway.spi_bridge_v2 import build_trader_spi_v2_bridge
from emt_gateway.telemetry import NullPerformanceTrace
from xuanling_tradecore.native.fastspi_v2 import (
    FastSpiV2CallbackKind,
    FastSpiV2CallbackKindRegistry,
    FastSpiV2Record,
    FastSpiV2Ring,
)


class _TraderApiBase:
    pass


class _Observer:
    def __init__(self) -> None:
        self.enqueued: list[CallbackRecord] = []

    def on_enqueued(self, record: CallbackRecord) -> None:
        self.enqueued.append(record)

    def on_durable(self, record: CallbackRecord, durable_ns: int) -> None:
        return None

    def on_ack(self, record: CallbackRecord, acknowledged_ns: int) -> None:
        return None


class _Ingress:
    def __init__(self) -> None:
        self.offer_values: dict[str, object] | None = None
        self.gaps: list[tuple[int, int]] = []

    def offer(self, **kwargs: object) -> bool:
        self.offer_values = kwargs
        return True

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None:
        self.gaps.append((callback_kind, observed_monotonic_ns))

    def record_native_rejection(
        self,
        *,
        producer_id: int,
        callback_seq: int,
        callback_kind: int,
        observed_monotonic_ns: int,
    ) -> bool:
        self.gaps.append((callback_kind, observed_monotonic_ns))
        return True


_CALLBACK_KINDS = {
    "OnOrderEvent": 4,
    "OnTradeEvent": 5,
    "OnCancelOrderError": 6,
    "OnQueryOrderByPage": 18,
}


class FastSpiV2ParityTests(unittest.TestCase):
    def _pipeline(self, observer: _Observer) -> CallbackPipeline:
        return CallbackPipeline(
            journal=InMemoryJournal(),
            trace=NullPerformanceTrace(),
            lookup_source=lambda record: None,
            capacity=2,
            lifecycle_observer=observer,
        )

    def _v1(
        self,
        callback_name: str,
        args: tuple[object, ...],
    ) -> tuple[CallbackDelivery, CallbackRecord]:
        observer = _Observer()
        pipeline = self._pipeline(observer)
        bridge = build_trader_spi_bridge(
            _TraderApiBase,
            callback_ingress=pipeline,
            account_profile="ordinary",
        )()
        getattr(bridge, python_callback_name(callback_name))(*args)
        delivery = pipeline.normalize_one()
        assert delivery is not None
        return delivery, observer.enqueued[0]

    def _v2(
        self,
        callback_name: str,
        args: tuple[object, ...],
    ) -> tuple[CallbackDelivery, CallbackRecord]:
        ingress = _Ingress()
        bridge = build_trader_spi_v2_bridge(_TraderApiBase, callback_ingress=ingress)()
        getattr(bridge, python_callback_name(callback_name))(*args)
        assert ingress.offer_values is not None
        assert not ingress.gaps
        offered = ingress.offer_values
        expected_error = _error_bytes_from_args(args)
        if expected_error is not None:
            self.assertEqual(_bytes(offered, "error_bytes"), expected_error)
        observer = _Observer()
        pipeline = self._pipeline(observer)
        ring = FastSpiV2Ring(2, prefer_native=False)
        record = FastSpiV2Record(
            callback_kind=_CALLBACK_KINDS[callback_name],
            producer_id=7,
            callback_seq=1,
            observed_monotonic_ns=100,
            client_id=_int(offered, "client_id"),
            order_client_id=_int(offered, "order_client_id"),
            order_emt_id=_int(offered, "order_emt_id"),
            order_cancel_emt_id=_int(offered, "order_cancel_emt_id"),
            status=_int(offered, "status"),
            quantity=_int(offered, "quantity"),
            trade_id_hash=_int(offered, "trade_id_hash"),
            flags=_int(offered, "flags"),
            error_bytes=_bytes(offered, "error_bytes"),
            order_update_time=_int(offered, "order_update_time"),
            qty_traded=_int(offered, "qty_traded"),
            qty_left=_int(offered, "qty_left"),
            trade_market=_int(offered, "trade_market"),
            trade_price=_float(offered, "trade_price"),
            trade_report_index=_int(offered, "trade_report_index"),
            trade_execution_id=_bytes(offered, "trade_execution_id"),
            query_reference=_int(offered, "query_reference"),
        )
        self.assertTrue(ring.try_push(record))
        worker = FastSpiV2CallbackPipelineWorker(
            ring=ring,
            callback_kinds=FastSpiV2CallbackKindRegistry(
                tuple(
                    FastSpiV2CallbackKind(code=code, name=name)
                    for name, code in _CALLBACK_KINDS.items()
                )
            ),
            account_profile="ordinary",
            callback_pipeline=pipeline,
            monotonic_clock_ns=lambda: 100,
            integrity_gap_latch=ingress,
        )
        delivery = worker.drain(maximum_records=1).deliveries[0]
        return delivery, observer.enqueued[0]

    def test_p0_callback_public_delivery_and_ledger_fact_are_equal(self) -> None:
        vectors: tuple[tuple[str, tuple[object, ...]], ...] = (
            (
                "OnOrderEvent",
                (
                    {
                        "order_emt_id": 101,
                        "order_client_id": 4,
                        "order_cancel_emt_id": 0,
                        "order_status": 2,
                        "update_time": 20260827100000000,
                        "qty_traded": 0,
                        "qty_left": 100,
                        "quantity": 100,
                    },
                    {"error_id": 0, "error_msg": ""},
                    9,
                ),
            ),
            (
                "OnOrderEvent",
                (
                    {
                        "order_emt_id": 101,
                        "order_client_id": 4,
                        "order_cancel_emt_id": 55,
                        "order_status": 3,
                        "update_time": None,
                        "cancel_time": 20260827100000001,
                        "qty_traded": 40,
                        "qty_left": 60,
                        "quantity": 100,
                    },
                    {"error_id": 0, "error_msg": ""},
                    9,
                ),
            ),
            (
                "OnTradeEvent",
                (
                    {
                        "order_emt_id": 101,
                        "order_client_id": 4,
                        "market": 2,
                        "quantity": 100,
                        "price": 1.61,
                        "report_index": 99,
                    },
                    9,
                ),
            ),
            (
                "OnCancelOrderError",
                (
                    {"order_emt_id": 101, "order_cancel_emt_id": 55},
                    {"error_id": 7, "error_msg": "撤单被拒绝"},
                    9,
                ),
            ),
            (
                "OnQueryOrderByPage",
                ({}, {}, 17, 99, True, 9),
            ),
        )
        for callback_name, args in vectors:
            with self.subTest(callback_name=callback_name):
                self._assert_public_parity(callback_name, args)

    def _assert_public_parity(
        self,
        callback_name: str,
        args: tuple[object, ...],
    ) -> None:
        v1_delivery, v1_record = self._v1(callback_name, args)
        v2_delivery, v2_record = self._v2(callback_name, args)
        self.assertEqual(
            (v1_delivery.callback_name, v1_delivery.source_command_id, v1_delivery.event_lsn),
            (v2_delivery.callback_name, v2_delivery.source_command_id, v2_delivery.event_lsn),
        )
        self.assertEqual(
            (
                v1_record.callback_name,
                v1_record.profile,
                v1_record.workload,
                v1_record.order_emt_id,
                v1_record.operation_native_id,
                v1_record.request_id,
                v1_record.is_last,
                v1_record.query_item_present,
                v1_record.query_reference,
                v1_record.ledger_fact,
            ),
            (
                v2_record.callback_name,
                v2_record.profile,
                v2_record.workload,
                v2_record.order_emt_id,
                v2_record.operation_native_id,
                v2_record.request_id,
                v2_record.is_last,
                v2_record.query_item_present,
                v2_record.query_reference,
                v2_record.ledger_fact,
            ),
        )

    def test_c5_static_audit_rejects_no_current_forbidden_operation(self) -> None:
        assert_fastspi_v2_c5_source_constrained()


def _int(values: dict[str, object], field_name: str) -> int:
    value = values[field_name]
    assert type(value) is int
    return value


def _float(values: dict[str, object], field_name: str) -> float:
    value = values[field_name]
    if type(value) is float:
        return value
    assert type(value) is int
    return float(cast(int, value))


def _bytes(values: dict[str, object], field_name: str) -> bytes:
    value = values[field_name]
    if isinstance(value, str):
        return value.encode("utf-8")
    assert isinstance(value, bytes)
    return value


def _error_bytes_from_args(args: tuple[object, ...]) -> bytes | None:
    for value in args:
        if not isinstance(value, dict):
            continue
        error_value = value.get("error_msg", value.get("error_message"))
        if isinstance(error_value, str):
            return error_value.encode("utf-8")
        if isinstance(error_value, bytes):
            return error_value
    return None


if __name__ == "__main__":
    unittest.main()
