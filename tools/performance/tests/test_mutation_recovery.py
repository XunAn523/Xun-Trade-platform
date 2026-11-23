from __future__ import annotations

import unittest
from pathlib import Path

from emt_gateway.gateway import CallbackRecord
from perfbench.mutation_recovery import (
    MutationRecoveryError,
    MutationRecoverySettings,
    _OutstandingOrderIds,
)
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def _accounts() -> SimulatedAccounts:
    account = SimulatedAccount(account="simulated", password="local-only")
    return SimulatedAccounts("local-counter", 19088, account, account, account)


class MutationRecoveryTests(unittest.TestCase):
    def test_settings_bound_callback_timeout(self) -> None:
        settings = MutationRecoverySettings(Path("sdk"), Path("runtime"), _accounts())
        settings.validate()
        self.assertTrue(
            MutationRecoverySettings(
                Path("sdk"), Path("runtime"), _accounts(), isolate_after_report=True
            ).isolate_after_report
        )
        with self.assertRaisesRegex(MutationRecoveryError, "callback_timeout_seconds"):
            MutationRecoverySettings(
                Path("sdk"), Path("runtime"), _accounts(), callback_timeout_seconds=2.0
            ).validate()
        with self.assertRaisesRegex(MutationRecoveryError, "cancellation_interval_seconds"):
            MutationRecoverySettings(
                Path("sdk"), Path("runtime"), _accounts(), cancellation_interval_seconds=0.001
            ).validate()

    def test_outstanding_collector_keeps_only_query_order_native_ids(self) -> None:
        collector = _OutstandingOrderIds()
        collector.on_enqueued(
            CallbackRecord(
                callback_correlation_id="query",
                callback_name="OnQueryOrder",
                profile="ordinary",
                workload="steady",
                callback_entry_ns=1,
                ring_enqueue_ns=2,
                producer_id=1,
                callback_seq=1,
                order_emt_id=101,
                operation_native_id=None,
            )
        )
        collector.on_enqueued(
            CallbackRecord(
                callback_correlation_id="event",
                callback_name="OnOrderEvent",
                profile="ordinary",
                workload="steady",
                callback_entry_ns=1,
                ring_enqueue_ns=2,
                producer_id=1,
                callback_seq=2,
                order_emt_id=202,
                operation_native_id=None,
            )
        )
        self.assertEqual((101,), collector.ids())
