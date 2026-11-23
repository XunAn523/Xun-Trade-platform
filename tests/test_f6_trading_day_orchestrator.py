"""F6 day rollover is per-account, durable, and remains DISARMED."""

from __future__ import annotations

import unittest

from xuanling_tradecore.domain import AccountLifecycle, TradingDayEpoch
from xuanling_tradecore.runtime import (
    AccountTradingDayStateV1,
    TradingDayOrchestrationErrorV1,
    TradingDayOrchestratorV1,
    TradingDayRolloverEvidenceV1,
)


class _FactPort:
    def __init__(self) -> None:
        self.evidence: list[TradingDayRolloverEvidenceV1] = []

    def append_rollover(self, evidence: TradingDayRolloverEvidenceV1) -> None:
        self.evidence.append(evidence)


def _evidence(account_id: str = "account-a") -> TradingDayRolloverEvidenceV1:
    return TradingDayRolloverEvidenceV1(
        account_id=account_id,
        previous_day=TradingDayEpoch("20260910"),
        next_day=TradingDayEpoch("20260911"),
        calendar_evidence_hash="a" * 64,
        adapter_evidence_hash="b" * 64,
        recovery_certificate_hash="c" * 64,
        observed_wall_ns=1,
    )


def _state(
    *,
    account_id: str = "account-a",
    lifecycle: AccountLifecycle = AccountLifecycle.DISARMED,
    session_active: bool = False,
    unresolved: int = 0,
) -> AccountTradingDayStateV1:
    return AccountTradingDayStateV1(
        account_id=account_id,
        trading_day=TradingDayEpoch("20260910"),
        lifecycle=lifecycle,
        adapter_session_active=session_active,
        unresolved_work_count=unresolved,
    )


class F6TradingDayOrchestratorTests(unittest.TestCase):
    def test_rollover_is_durable_per_account_and_stays_disarmed(self) -> None:
        fact_port = _FactPort()
        orchestrator = TradingDayOrchestratorV1(fact_port)
        orchestrator.register(_state())
        orchestrator.register(_state(account_id="account-b"))
        next_state = orchestrator.rollover(_evidence())
        self.assertEqual(next_state.trading_day.value, "20260911")
        self.assertIs(next_state.lifecycle, AccountLifecycle.DISARMED)
        self.assertEqual(len(fact_port.evidence), 1)
        self.assertEqual(orchestrator.state_for("account-b").trading_day.value, "20260910")

    def test_session_unresolved_or_non_disarmed_state_blocks_rollover_before_fact_append(
        self,
    ) -> None:
        for state in (
            _state(lifecycle=AccountLifecycle.READY),
            _state(session_active=True),
            _state(unresolved=1),
        ):
            with self.subTest(state=state):
                fact_port = _FactPort()
                orchestrator = TradingDayOrchestratorV1(fact_port)
                orchestrator.register(state)
                with self.assertRaises(TradingDayOrchestrationErrorV1):
                    orchestrator.rollover(_evidence())
                self.assertEqual(fact_port.evidence, [])


if __name__ == "__main__":
    unittest.main()
