from __future__ import annotations

import unittest
from unittest.mock import Mock

from xuanling_tradecore.consumer import ConsumerAccountView, ConsumerSession
from xuanling_tradecore.consumer.dispatch_v2 import (
    ConsumerWireDispatchError,
    ConsumerWireDispatcherV2,
)
from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayError
from xuanling_tradecore.consumer.request_trace_v2 import request_trace_scope
from xuanling_tradecore.domain import AccountLifecycle, ConsumerApiVersion, ReconciliationStatus


class _Gateway:
    def list_accounts(self, session: object) -> tuple[ConsumerAccountView, ...]:
        return (
            ConsumerAccountView(
                "ca-1",
                "sim",
                "EMT",
                "emt-sim-v1",
                AccountLifecycle.READY,
                ReconciliationStatus.VALIDATED,
            ),
        )


class ConsumerDispatchV2Tests(unittest.TestCase):
    def test_cursor_failure_trace_has_only_stable_code_not_private_error(self) -> None:
        for code, expected in (
            ("JOURNAL_GAP", "JOURNAL_GAP"),
            ("private-token", "CURSOR_REPLAY_FAILED"),
        ):
            gateway = Mock()
            gateway.create_replay_partitions.side_effect = ConsumerCursorReplayError(
                code, "private path password cursor"
            )
            dispatcher = ConsumerWireDispatcherV2(gateway)
            session = ConsumerSession("session-1", "consumer-1", ConsumerApiVersion(2, 0), 1, 2)
            record: dict[str, object] = {}
            with request_trace_scope(record), self.assertRaises(ConsumerWireDispatchError):
                dispatcher(
                    session,
                    {
                        "operation": "CreateReplayPartitions",
                        "request_id": "r",
                        "payload": {"consumer_account_id": "ca-1", "partition_count": 4},
                    },
                )
            self.assertEqual({"failure_stage": "cursor_replay", "failure_code": expected}, record)
            self.assertNotIn("private", str(record))

    def test_lists_bound_accounts_and_rejects_malformed_operation(self) -> None:
        dispatcher = ConsumerWireDispatcherV2(_Gateway())  # type: ignore[arg-type]
        session = ConsumerSession("session-1", "consumer-1", ConsumerApiVersion(2, 0), 1, 2)
        response = dispatcher(
            session, {"operation": "ListAccounts", "request_id": "r", "payload": {}}
        )
        self.assertEqual("ca-1", response["payload"]["accounts"][0]["consumer_account_id"])  # type: ignore[index]
        with self.assertRaisesRegex(ConsumerWireDispatchError, "SCHEMA_VALIDATION_FAILED"):
            dispatcher(session, {"operation": "SubmitSignal", "request_id": "r", "payload": {}})
