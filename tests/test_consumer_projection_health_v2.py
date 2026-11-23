from __future__ import annotations

import unittest

from xuanling_tradecore.consumer import (
    ConsumerProjectionHealthErrorV2,
    ConsumerProjectionHealthTrackerV2,
)
from xuanling_tradecore.consumer.base import PositionedConsumerEventV2
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerOrderEvent,
    ConsumerTrace,
    OmsOrderState,
    OrderOrigin,
    RedisStreamIdV1,
    TradingDayEpoch,
)


class ConsumerProjectionHealthV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.binding = ConsumerAccountBinding(
            "binding-1",
            1,
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "SIM",
            "EMT",
            "emt-instance-1",
        )
        trace = ConsumerTrace(
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "strategy-1",
            "strategy-order-1",
            "attribution-1",
            1,
            OrderOrigin.STRATEGY,
        )
        self.item = PositionedConsumerEventV2(
            ConsumerEventPositionV2(RedisStreamIdV1(1, 0), 0),
            ConsumerOrderEvent(
                "event-1",
                ConsumerEventKind.ORDER,
                2,
                "account-1",
                TradingDayEpoch("2026-09-18"),
                "journal-1",
                1,
                trace,
                "oms-1",
                "execution-1",
                OmsOrderState.WORKING,
                1,
            ),
        )

    def test_stale_health_latches_and_generation_cannot_cross(self) -> None:
        tracker = ConsumerProjectionHealthTrackerV2()
        self.assertTrue(tracker.health_for(binding=self.binding).stale)
        observed = tracker.observe_event(binding=self.binding, item=self.item)
        self.assertFalse(observed.stale)
        self.assertEqual(self.item.position, observed.source_watermark)
        stale = tracker.mark_stale(binding=self.binding, reason_code="PROJECTOR_PAUSED")
        self.assertTrue(stale.stale)
        self.assertEqual("PROJECTOR_PAUSED", stale.reason_code)
        self.assertEqual(stale, tracker.observe_event(binding=self.binding, item=self.item))
        newer = ConsumerAccountBinding(
            "binding-1",
            2,
            "consumer-1",
            "consumer-account-1",
            "account-1",
            "SIM",
            "EMT",
            "emt-instance-1",
        )
        with self.assertRaises(ConsumerProjectionHealthErrorV2):
            tracker.health_for(binding=newer)


if __name__ == "__main__":
    unittest.main()
