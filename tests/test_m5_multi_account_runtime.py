from __future__ import annotations

import unittest

from xuanling_tradecore.runtime import (
    AccountRuntimeRegistrationV1,
    AccountRuntimeStateV1,
    MultiAccountRuntimeErrorV1,
    MultiAccountRuntimeSupervisorV1,
)


class _Runtime:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start, self.starts, self.stops = fail_start, 0, 0

    def start_disarmed(self) -> None:
        self.starts += 1
        if self.fail_start:
            raise RuntimeError("injected")

    def stop_disarmed(self) -> None:
        self.stops += 1


class MultiAccountRuntimeTests(unittest.TestCase):
    def test_failure_isolated_to_one_account_and_other_account_stays_disarmed(self) -> None:
        bad, good = _Runtime(fail_start=True), _Runtime()
        supervisor = MultiAccountRuntimeSupervisorV1(
            {
                "account-1": AccountRuntimeRegistrationV1("account-1", "EMT", "emt-1", bad),
                "account-2": AccountRuntimeRegistrationV1("account-2", "QMT", "qmt-1", good),
            }
        )
        with self.assertRaisesRegex(MultiAccountRuntimeErrorV1, "remains DISARMED"):
            supervisor.start_account("account-1")
        self.assertEqual(
            AccountRuntimeStateV1.FAILED_DISARMED, supervisor.snapshot("account-1").state
        )
        self.assertEqual(
            AccountRuntimeStateV1.REGISTERED_DISARMED, supervisor.snapshot("account-2").state
        )
        self.assertEqual(
            AccountRuntimeStateV1.RUNNING_DISARMED, supervisor.start_account("account-2").state
        )
        self.assertEqual(0, bad.stops)
        self.assertEqual(
            AccountRuntimeStateV1.STOPPED_DISARMED, supervisor.stop_account("account-2").state
        )

    def test_duplicate_adapter_scope_and_repeat_start_fail_closed(self) -> None:
        runtime = _Runtime()
        with self.assertRaisesRegex(MultiAccountRuntimeErrorV1, "share one active"):
            MultiAccountRuntimeSupervisorV1(
                {
                    "account-1": AccountRuntimeRegistrationV1("account-1", "EMT", "emt-1", runtime),
                    "account-2": AccountRuntimeRegistrationV1(
                        "account-2", "EMT", "emt-1", _Runtime()
                    ),
                }
            )
        supervisor = MultiAccountRuntimeSupervisorV1(
            {"account-1": AccountRuntimeRegistrationV1("account-1", "EMT", "emt-1", runtime)}
        )
        supervisor.start_account("account-1")
        with self.assertRaisesRegex(MultiAccountRuntimeErrorV1, "not startable"):
            supervisor.start_account("account-1")


if __name__ == "__main__":
    unittest.main()
