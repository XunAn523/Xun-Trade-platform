from __future__ import annotations

import unittest

from xuanling_tradecore.consumer.bindings_v2 import ConsumerBindingError, ConsumerBindingSnapshotV2
from xuanling_tradecore.domain import (
    ConsumerAccountBinding,
    ConsumerPrincipal,
    ConsumerStrategyBinding,
)


class ConsumerBindingsV2Tests(unittest.TestCase):
    def test_authorizes_only_snapshot_bound_principal_account_and_strategy(self) -> None:
        account = ConsumerAccountBinding(
            "account-bind", 1, "consumer-1", "ca-1", "account-1", "sim", "EMT", "instance-1"
        )
        strategy = ConsumerStrategyBinding(
            "strategy-bind", 1, "consumer-1", "ca-1", "account-bind", "strategy-1"
        )
        snapshot = ConsumerBindingSnapshotV2(
            authorization_epoch=3, accounts=(account,), strategies=(strategy,)
        )
        snapshot.require_principal(ConsumerPrincipal("consumer-1", "login", "name"))
        self.assertEqual(
            "strategy-bind",
            snapshot.require_strategy(
                consumer_id="consumer-1", consumer_account_id="ca-1", strategy_id="strategy-1"
            ).binding_id,
        )
        with self.assertRaisesRegex(ConsumerBindingError, "not authorized"):
            snapshot.require_strategy(
                consumer_id="consumer-1", consumer_account_id="ca-1", strategy_id="other"
            )


if __name__ == "__main__":
    unittest.main()
