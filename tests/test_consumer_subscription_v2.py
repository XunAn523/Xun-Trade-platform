from __future__ import annotations

import unittest

from xuanling_tradecore.consumer import (
    ConsumerSubscriptionClosedV2,
    ConsumerSubscriptionLimitsV2,
    ConsumerSubscriptionRegistryV2,
    PositionedConsumerEventV2,
)
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


def _item() -> PositionedConsumerEventV2:
    trace = ConsumerTrace(
        "consumer-1", "ca-1", "account-1", "strategy-1", "so-1", "attr-1", 1, OrderOrigin.STRATEGY
    )
    event = ConsumerOrderEvent(
        "event-1",
        ConsumerEventKind.ORDER,
        2,
        "account-1",
        TradingDayEpoch("2026-09-03"),
        "journal-1",
        1,
        trace,
        "order-1",
        "execution-1",
        OmsOrderState.WORKING,
        1,
    )
    return PositionedConsumerEventV2(ConsumerEventPositionV2(RedisStreamIdV1(1, 0), 0), event)


class ConsumerSubscriptionV2Tests(unittest.TestCase):
    def test_overflow_closes_only_slow_subscription_and_requires_replay(self) -> None:
        binding = ConsumerAccountBinding(
            "bind-1", 1, "consumer-1", "ca-1", "account-1", "sim", "EMT", "emt-1"
        )
        registry = ConsumerSubscriptionRegistryV2()
        subscription = registry.register(
            subscription_id="s-1", binding=binding, limits=ConsumerSubscriptionLimitsV2(1, 10)
        )
        self.assertEqual(1, registry.publish(_item(), encoded_bytes=10))
        self.assertEqual(0, registry.publish(_item(), encoded_bytes=10))
        self.assertTrue(subscription.closed)
        with self.assertRaises(ConsumerSubscriptionClosedV2):
            subscription.drain(limit=1)


if __name__ == "__main__":
    unittest.main()
