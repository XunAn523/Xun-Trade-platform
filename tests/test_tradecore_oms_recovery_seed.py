"""M1-08 canonical complete OMS recovery seed tests."""

from __future__ import annotations

import json
import unittest

from xuanling_tradecore.domain import OmsOrderState
from xuanling_tradecore.oms import (
    BrokerOrderObservationV1,
    OmsFactKind,
    OmsRecoverySeedError,
    OmsRecoverySeedV1,
)

from route_seed_fixture import recovery_seed


class TradeCoreOmsRecoverySeedTests(unittest.TestCase):
    def test_broker_observation_conserves_quantity_and_declares_reservation_fact(self) -> None:
        observation = BrokerOrderObservationV1(
            broker_event_id="event-1",
            source_key="source-1",
            order_state=OmsOrderState.PARTIALLY_FILLED,
            cumulative_filled_quantity=25,
            leaves_quantity=75,
            trade_keys=("trade-1",),
        )
        observation.validate_total_quantity(100)
        self.assertEqual(OmsFactKind.RESERVATION_ADJUSTED, observation.reservation_fact_kind)
        self.assertEqual(
            observation,
            BrokerOrderObservationV1.from_payload(observation.to_payload(), total_quantity=100),
        )
        with self.assertRaisesRegex(OmsRecoverySeedError, "conserve"):
            observation.validate_total_quantity(101)

        canceled = BrokerOrderObservationV1(
            broker_event_id="event-2",
            source_key="source-2",
            order_state=OmsOrderState.CANCELED,
            cumulative_filled_quantity=25,
            leaves_quantity=75,
            trade_keys=("trade-1",),
        )
        canceled.validate_total_quantity(100)
        self.assertEqual(OmsFactKind.RESERVATION_RELEASED, canceled.reservation_fact_kind)
        self.assertEqual(0, canceled.reservation_leaves_quantity)

        filled = BrokerOrderObservationV1(
            broker_event_id="event-3",
            source_key="source-3",
            order_state=OmsOrderState.FILLED,
            cumulative_filled_quantity=100,
            leaves_quantity=0,
            trade_keys=("trade-1", "trade-2"),
        )
        filled.validate_successor(observation, total_quantity=100)
        with self.assertRaisesRegex(OmsRecoverySeedError, "terminal"):
            observation.validate_successor(filled, total_quantity=100)

    def test_complete_seed_round_trips_to_identical_order(self) -> None:
        seed = recovery_seed(
            account_id="emt_03",
            oms_order_id="order-1",
            execution_id="execution-1",
            decision_context_id="decision-1",
            intent_hash="a" * 64,
        )
        decoded = OmsRecoverySeedV1.decode_and_verify(seed.encoded)
        self.assertEqual(seed, decoded)
        self.assertEqual("consumer-emt_03", decoded.order.order.consumer_trace.consumer_account_id)
        self.assertEqual(100_000, decoded.order.reservation.active_requirement.cash_minor)

    def test_missing_changed_or_noncanonical_seed_fails_closed(self) -> None:
        seed = recovery_seed(
            account_id="emt_03",
            oms_order_id="order-1",
            execution_id="execution-1",
            decision_context_id="decision-1",
            intent_hash="a" * 64,
        )
        payload = json.loads(seed.encoded)
        del payload["execution"]["quantity"]
        malformed = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        with self.assertRaises(OmsRecoverySeedError):
            OmsRecoverySeedV1.decode_and_verify(malformed)
        with self.assertRaisesRegex(OmsRecoverySeedError, "canonical"):
            OmsRecoverySeedV1.decode_and_verify(b'{ "schema_version": 1 }')


if __name__ == "__main__":
    unittest.main()
