from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)
from xuanling_tradecore.ems import (
    ExecutionGuard,
    ExecutionGuardAdmissionGate,
    ExecutionGuardError,
    ExecutionGuardPolicy,
    FrequencyLimitScope,
    GuardDecision,
    GuardDecisionCode,
    GuardMarketSnapshot,
    KillSwitchAction,
    KillSwitchLevel,
    KillSwitchRule,
    KillSwitchSnapshot,
)

SHA_A = "a" * 64


def _policy(
    *,
    account_limit: int = 3,
    strategy_limit: int = 2,
    symbol_limit: int = 2,
    side_limit: int = 2,
    deviation_bps: int = 1_000,
    max_quantity: int = 1_000,
    max_notional_ticks: int = 1_000_000,
) -> ExecutionGuardPolicy:
    return ExecutionGuardPolicy(
        policy_id="guard-v1",
        version=1,
        policy_hash=SHA_A,
        account_id="account-1",
        frequency_window_ns=1_000,
        account_order_limit=account_limit,
        strategy_order_limits={"strategy-1": strategy_limit, "strategy-2": strategy_limit},
        symbol_order_limits={"000001": symbol_limit, "000002": symbol_limit},
        side_order_limits={
            "BUY": side_limit,
            "SELL": side_limit,
            "MARGIN_BUY": side_limit,
            "SHORT_SELL": side_limit,
        },
        max_price_deviation_bps=deviation_bps,
        max_order_quantity=max_quantity,
        max_order_notional_ticks=max_notional_ticks,
    )


def _instruction(
    execution_id: str = "execution-1",
    *,
    account_id: str = "account-1",
    strategy_id: str | None = "strategy-1",
    symbol: str = "000001",
    market: str = "SZ",
    side: str = "BUY",
    protection: int = 100,
    limit: int | None = 100,
    quantity: int = 100,
) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=execution_id,
        oms_order_id=f"oms-{execution_id}",
        account_id=account_id,
        trading_day=TradingDayEpoch("2026-09-02"),
        consumer_trace=ConsumerTrace(
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            account_id=account_id,
            strategy_id=strategy_id,
            strategy_order_id=execution_id if strategy_id is not None else None,
            attribution_binding_id="attribution-1",
            binding_version=1,
            order_origin=OrderOrigin.STRATEGY if strategy_id else OrderOrigin.MANUAL,
        ),
        symbol=symbol,
        market=market,
        side=side,
        price_type="LIMIT",
        business_type="CASH",
        quantity=quantity,
        protection_price_ticks=protection,
        limit_price_ticks=limit,
    )


def _market(**changes: object) -> GuardMarketSnapshot:
    values: dict[str, object] = {
        "snapshot_id": "market-1",
        "symbol": "000001",
        "market": "SZ",
        "reference_price_ticks": 100,
        "minimum_price_ticks": 90,
        "maximum_price_ticks": 110,
        "captured_monotonic_ns": 0,
        "expires_monotonic_ns": 10_000,
    }
    values.update(changes)
    return GuardMarketSnapshot(  # type: ignore[reportArgumentType]  # Dynamic overrides exercise validation.
        **values
    )


def _switch(
    level: KillSwitchLevel,
    selector: str,
    *,
    switch_id: str = "switch-1",
    account_id: str = "account-1",
    action: KillSwitchAction = KillSwitchAction.BLOCK_NEW,
) -> KillSwitchRule:
    return KillSwitchRule(
        switch_id=switch_id,
        account_id=account_id,
        level=level,
        selector=selector,
        action=action,
        control_command_id=f"control-{switch_id}",
        control_version=1,
        reason_code="RISK_STOP",
        operator_id="risk-operator-1",
        approval_reference="approval-1",
        durable_fact_reference="journal:control:1",
    )


EMPTY_SWITCHES = KillSwitchSnapshot(control_version=0)


class ExecutionGuardTests(unittest.TestCase):
    def test_policy_is_immutable_and_rejects_invalid_bounds(self) -> None:
        policy = _policy()
        with self.assertRaises(TypeError):
            policy.strategy_order_limits[  # type: ignore[reportIndexIssue]  # Mutation must fail at runtime.
                "strategy-1"
            ] = 99
        with self.assertRaises(ExecutionGuardError):
            replace(policy, max_price_deviation_bps=10_001)
        with self.assertRaises(ExecutionGuardError):
            replace(policy, symbol_order_limits={})

    def test_each_block_new_scope_matches_only_its_selector(self) -> None:
        instruction = _instruction()
        cases = (
            (KillSwitchLevel.ACCOUNT, "account-1"),
            (KillSwitchLevel.STRATEGY, "strategy-1"),
            (KillSwitchLevel.SYMBOL, "000001"),
            (KillSwitchLevel.SIDE, "BUY"),
        )
        for index, (level, selector) in enumerate(cases):
            with self.subTest(level=level):
                guard = ExecutionGuard(_policy())
                rule = _switch(level, selector, switch_id=f"switch-{index}")
                blocked = guard.evaluate(
                    instruction,
                    market=_market(),
                    kill_switches=KillSwitchSnapshot(control_version=1, rules=(rule,)),
                    evaluated_monotonic_ns=10,
                )
                self.assertFalse(blocked.accepted)
                self.assertEqual(blocked.code, GuardDecisionCode.KILL_SWITCH_BLOCK_NEW)
                self.assertEqual(blocked.matched_kill_switch_ids, (f"switch-{index}",))
                allowed = guard.evaluate(
                    instruction,
                    market=_market(),
                    kill_switches=EMPTY_SWITCHES,
                    evaluated_monotonic_ns=11,
                )
                self.assertTrue(allowed.accepted)

    def test_unrelated_and_cancel_owned_working_rules_do_not_block_new(self) -> None:
        rules = (
            _switch(KillSwitchLevel.SYMBOL, "000002", switch_id="unrelated"),
            _switch(
                KillSwitchLevel.ACCOUNT,
                "account-1",
                switch_id="cancel-command",
                action=KillSwitchAction.CANCEL_OWNED_WORKING,
            ),
        )
        decision = ExecutionGuard(_policy()).evaluate(
            _instruction(),
            market=_market(),
            kill_switches=KillSwitchSnapshot(control_version=1, rules=rules),
            evaluated_monotonic_ns=10,
        )
        self.assertTrue(decision.accepted)

    def test_price_rejections_are_structured_and_do_not_consume_quota(self) -> None:
        cases = (
            (
                _instruction(protection=100),
                _market(captured_monotonic_ns=11),
                10,
                GuardDecisionCode.PRICE_SNAPSHOT_NOT_YET_VALID,
            ),
            (
                _instruction(protection=100),
                _market(expires_monotonic_ns=10),
                10,
                GuardDecisionCode.PRICE_SNAPSHOT_STALE,
            ),
            (
                _instruction(protection=111, limit=None),
                _market(),
                10,
                GuardDecisionCode.PRICE_OUTSIDE_BAND,
            ),
            (
                _instruction(protection=106, limit=None),
                _market(),
                10,
                GuardDecisionCode.PRICE_DEVIATION_EXCEEDED,
            ),
            (
                _instruction(protection=100, limit=101),
                _market(),
                10,
                GuardDecisionCode.LIMIT_PRICE_EXCEEDS_PROTECTION,
            ),
            (
                _instruction(side="SELL", protection=100, limit=99),
                _market(),
                10,
                GuardDecisionCode.LIMIT_PRICE_EXCEEDS_PROTECTION,
            ),
        )
        for instruction, market, now_ns, code in cases:
            with self.subTest(code=code):
                guard = ExecutionGuard(_policy(deviation_bps=500))
                rejected = guard.evaluate(
                    instruction,
                    market=market,
                    kill_switches=EMPTY_SWITCHES,
                    evaluated_monotonic_ns=now_ns,
                )
                self.assertEqual(rejected.code, code)
                self.assertEqual(
                    guard.snapshot(evaluated_monotonic_ns=now_ns).account_window_depth, 0
                )

    def test_quantity_and_notional_limits_reject_before_side_effect_quota(self) -> None:
        cases = (
            (
                _policy(max_quantity=99),
                _instruction(quantity=100),
                GuardDecisionCode.ORDER_QUANTITY_LIMIT_EXCEEDED,
            ),
            (
                _policy(max_notional_ticks=9_999),
                _instruction(quantity=100, protection=100),
                GuardDecisionCode.ORDER_NOTIONAL_LIMIT_EXCEEDED,
            ),
        )
        for policy, instruction, expected in cases:
            with self.subTest(expected=expected):
                guard = ExecutionGuard(policy)
                decision = guard.evaluate(
                    instruction,
                    market=_market(),
                    kill_switches=EMPTY_SWITCHES,
                    evaluated_monotonic_ns=10,
                )
                self.assertEqual(decision.code, expected)
                self.assertEqual(guard.snapshot(evaluated_monotonic_ns=10).account_window_depth, 0)

    def test_frequency_limit_is_atomic_idempotent_and_expires_at_boundary(self) -> None:
        guard = ExecutionGuard(_policy(account_limit=2, strategy_limit=2, symbol_limit=2))
        first = guard.evaluate(
            _instruction("execution-1"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=0,
        )
        replay = guard.evaluate(
            _instruction("execution-1"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=1,
        )
        second = guard.evaluate(
            _instruction("execution-2"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=1,
        )
        rejected = guard.evaluate(
            _instruction("execution-3"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=999,
        )
        expired = guard.evaluate(
            _instruction("execution-3"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=1_000,
        )
        self.assertIs(first, replay)
        self.assertTrue(second.accepted)
        self.assertEqual(rejected.code, GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED)
        self.assertEqual(
            rejected.exceeded_frequency_scopes,
            (
                FrequencyLimitScope.ACCOUNT,
                FrequencyLimitScope.STRATEGY,
                FrequencyLimitScope.SYMBOL,
                FrequencyLimitScope.SIDE,
            ),
        )
        self.assertTrue(expired.accepted)

    def test_strategy_symbol_and_side_limits_are_independent(self) -> None:
        guard = ExecutionGuard(
            _policy(account_limit=20, strategy_limit=1, symbol_limit=20, side_limit=20)
        )
        self.assertTrue(
            guard.evaluate(
                _instruction("s1"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=1,
            ).accepted
        )
        self.assertEqual(
            guard.evaluate(
                _instruction("s2"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=2,
            ).code,
            GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED,
        )
        self.assertTrue(
            guard.evaluate(
                _instruction("s3", strategy_id="strategy-2"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=3,
            ).accepted
        )

        symbol_guard = ExecutionGuard(
            _policy(account_limit=20, strategy_limit=20, symbol_limit=1, side_limit=20)
        )
        symbol_guard.evaluate(
            _instruction("y1"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=1,
        )
        self.assertEqual(
            symbol_guard.evaluate(
                _instruction("y2"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=2,
            ).code,
            GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED,
        )
        self.assertTrue(
            symbol_guard.evaluate(
                _instruction("y3", symbol="000002"),
                market=_market(symbol="000002"),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=3,
            ).accepted
        )

        side_guard = ExecutionGuard(
            _policy(account_limit=20, strategy_limit=20, symbol_limit=20, side_limit=1)
        )
        side_guard.evaluate(
            _instruction("d1"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=1,
        )
        self.assertEqual(
            side_guard.evaluate(
                _instruction("d2"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=2,
            ).code,
            GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED,
        )
        self.assertTrue(
            side_guard.evaluate(
                _instruction("d3", side="SELL"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=3,
            ).accepted
        )

    def test_concurrent_admission_never_exceeds_capacity(self) -> None:
        capacity = 8
        guard = ExecutionGuard(
            _policy(
                account_limit=capacity,
                strategy_limit=capacity,
                symbol_limit=capacity,
                side_limit=capacity,
            )
        )

        def evaluate(index: int) -> bool:
            return guard.evaluate(
                _instruction(f"execution-{index}"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=10,
            ).accepted

        with ThreadPoolExecutor(max_workers=16) as executor:
            accepted = list(executor.map(evaluate, range(64)))
        self.assertEqual(sum(accepted), capacity)
        snapshot = guard.snapshot(evaluated_monotonic_ns=10)
        self.assertEqual(snapshot.account_window_depth, capacity)
        self.assertEqual(snapshot.decision_counts[GuardDecisionCode.ALLOWED], capacity)
        self.assertEqual(
            snapshot.decision_counts[GuardDecisionCode.FREQUENCY_LIMIT_EXCEEDED],
            64 - capacity,
        )
        with self.assertRaises(TypeError):
            snapshot.decision_counts[  # type: ignore[reportIndexIssue]  # Snapshot must reject mutation.
                GuardDecisionCode.ALLOWED
            ] = 0

    def test_unknown_universe_and_identity_mismatch_fail_closed(self) -> None:
        guard = ExecutionGuard(_policy())
        cases = (
            (_instruction(strategy_id=None), GuardDecisionCode.UNAPPROVED_STRATEGY),
            (_instruction(symbol="999999"), GuardDecisionCode.UNAPPROVED_SYMBOL),
            (_instruction(side="UNKNOWN"), GuardDecisionCode.UNAPPROVED_SIDE),
        )
        for instruction, code in cases:
            with self.subTest(code=code):
                market = _market(symbol=instruction.symbol)
                self.assertEqual(
                    guard.evaluate(
                        instruction,
                        market=market,
                        kill_switches=EMPTY_SWITCHES,
                        evaluated_monotonic_ns=1,
                    ).code,
                    code,
                )
        with self.assertRaises(ExecutionGuardError):
            guard.evaluate(
                _instruction(account_id="account-2"),
                market=_market(),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=1,
            )
        with self.assertRaises(ExecutionGuardError):
            guard.evaluate(
                _instruction(),
                market=_market(market="SH"),
                kill_switches=EMPTY_SWITCHES,
                evaluated_monotonic_ns=1,
            )

    def test_admission_gate_rejection_has_only_audit_side_effect(self) -> None:
        class AuditSink:
            def __init__(self) -> None:
                self.rejections: list[GuardDecisionCode] = []

            def record_execution_rejected(
                self, instruction: ExecutionInstruction, decision: GuardDecision
            ) -> None:
                del instruction
                self.rejections.append(decision.code)

        audit = AuditSink()
        gate = ExecutionGuardAdmissionGate(ExecutionGuard(_policy()), audit)
        side_effects = {"reservation": 0, "call_started": 0, "adapter_submit": 0}

        def authorized(_instruction: ExecutionInstruction, _decision: GuardDecision) -> str:
            side_effects["reservation"] += 1
            side_effects["call_started"] += 1
            side_effects["adapter_submit"] += 1
            return "submitted"

        blocked = gate.admit(
            _instruction("blocked", protection=111),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=10,
            on_authorized=authorized,
        )
        self.assertFalse(blocked.decision.accepted)
        self.assertIsNone(blocked.authorized_result)
        self.assertEqual(side_effects, {"reservation": 0, "call_started": 0, "adapter_submit": 0})
        self.assertEqual(audit.rejections, [GuardDecisionCode.PRICE_OUTSIDE_BAND])

        allowed = gate.admit(
            _instruction("allowed"),
            market=_market(),
            kill_switches=EMPTY_SWITCHES,
            evaluated_monotonic_ns=11,
            on_authorized=authorized,
        )
        self.assertTrue(allowed.decision.accepted)
        self.assertEqual(allowed.authorized_result, "submitted")
        self.assertEqual(side_effects, {"reservation": 1, "call_started": 1, "adapter_submit": 1})


if __name__ == "__main__":
    unittest.main()
