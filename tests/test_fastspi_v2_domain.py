"""Target-domain normalization tests for post-ring fastspi V2 callbacks."""

from __future__ import annotations

import unittest

from emt_gateway.fastspi_v2_domain import (
    FastSpiV2BrokerEventNormalizer,
    FastSpiV2CallbackDomainIdentity,
    FastSpiV2DomainEventConfig,
    FastSpiV2DomainNormalizationError,
)
from emt_gateway.gateway import CallbackRecord
from emt_gateway.order_ledger import LedgerCallbackFact, LedgerCallbackKind, OrderState
from xuanling_tradecore.domain import BrokerEventKind, TradingDayEpoch, VenueOrderIdentity


class FastSpiV2DomainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.identity = FastSpiV2CallbackDomainIdentity(
            venue_order_identity=VenueOrderIdentity(
                venue_id="EMT",
                adapter_instance_id="emt-sim-instance-1",
                adapter_client_scope="7",
                venue_order_id="order-9",
            ),
            execution_id="execution-9",
            cancel_request_id="cancel-9",
        )
        self.config = FastSpiV2DomainEventConfig(
            account_id="account-1",
            adapter_instance_id="emt-sim-instance-1",
            adapter_client_scope="7",
            trading_day=TradingDayEpoch("2026-09-02"),
            session_epoch=4,
        )
        self.normalizer = FastSpiV2BrokerEventNormalizer(
            config=self.config,
            identity_resolver=lambda record: self.identity,
        )

    @staticmethod
    def _record(
        callback_name: str,
        *,
        ledger_fact: LedgerCallbackFact | None = None,
        request_id: int | None = None,
        is_last: bool | None = None,
    ) -> CallbackRecord:
        return CallbackRecord(
            callback_correlation_id="callback-1",
            callback_name=callback_name,
            profile="ordinary",
            workload="steady",
            callback_entry_ns=100,
            ring_enqueue_ns=101,
            order_emt_id=9,
            operation_native_id=4,
            producer_id="7",
            callback_seq=8,
            request_id=request_id,
            is_last=is_last,
            query_item_present=False if request_id is not None else None,
            query_reference=2 if request_id is not None else None,
            ledger_fact=ledger_fact,
        )

    def test_normalizes_proven_order_and_trade_facts(self) -> None:
        order = self.normalizer.normalize(
            self._record(
                "OnOrderEvent",
                ledger_fact=LedgerCallbackFact(
                    kind=LedgerCallbackKind.ORDER_UPDATE,
                    source_key="order:9:LIVE:10:2",
                    order_emt_id=9,
                    state=OrderState.LIVE,
                    leaves_qty=8,
                ),
            )
        )
        self.assertEqual(BrokerEventKind.ORDER, order.event_kind)
        self.assertEqual(8, order.leaves_qty)
        self.assertEqual("execution-9", order.execution_id)

        trade = self.normalizer.normalize(
            self._record(
                "OnTradeEvent",
                ledger_fact=LedgerCallbackFact(
                    kind=LedgerCallbackKind.TRADE,
                    source_key="trade:1:execution-9",
                    order_emt_id=9,
                    market="1",
                    execution_key="execution-9",
                    qty=2,
                ),
            )
        )
        self.assertEqual(BrokerEventKind.TRADE, trade.event_kind)
        self.assertEqual("execution-9", trade.trade_key)
        self.assertEqual(2, trade.filled_qty)

    def test_rejects_unproven_identity_and_unregistered_query(self) -> None:
        unproven = FastSpiV2BrokerEventNormalizer(
            config=self.config,
            identity_resolver=lambda record: None,
        )
        with self.assertRaisesRegex(FastSpiV2DomainNormalizationError, "no proven"):
            unproven.normalize(
                self._record(
                    "OnOrderEvent",
                    ledger_fact=LedgerCallbackFact(
                        kind=LedgerCallbackKind.ORDER_UPDATE,
                        source_key="order:9:LIVE:10:2",
                        order_emt_id=9,
                        state=OrderState.LIVE,
                    ),
                )
            )
        with self.assertRaisesRegex(FastSpiV2DomainNormalizationError, "registered request"):
            self.normalizer.normalize(self._record("OnQueryOrderByPage"))

    def test_query_event_is_source_keyed_without_native_payload(self) -> None:
        event = self.normalizer.normalize(
            self._record("OnQueryOrderByPage", request_id=22, is_last=True)
        )
        self.assertEqual(BrokerEventKind.QUERY, event.event_kind)
        self.assertEqual("22", event.normalized_summary["request_id"])
        self.assertNotIn("callback-1", event.source_key)


if __name__ == "__main__":
    unittest.main()
