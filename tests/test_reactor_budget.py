from dataclasses import FrozenInstanceError

import pytest
from xuanling_tradecore.consumer.reactor_budget import FixedReactorBudgets, ReactorIoTurn


@pytest.mark.parametrize("value", [True, 0, -1, 1.5, 129])
def test_invalid_policy_refused_before_service_start(value):
    with pytest.raises(ValueError):
        FixedReactorBudgets(accepts=value)


def test_budgets_are_frozen_and_reset_only_with_a_new_turn():
    policy = FixedReactorBudgets(read_bytes=100, write_bytes=100)
    with pytest.raises(FrozenInstanceError):
        policy.accepts = 2
    turn = ReactorIoTurn(policy)
    assert turn.receive_capacity() == 100 and turn.send_capacity() == 100
    turn.read_bytes = 100
    turn.write_bytes = 100
    turn.accept_ns = policy.phase_ns
    assert turn.receive_capacity() == turn.send_capacity() == 0 and not turn.may_accept()
    fresh = ReactorIoTurn(policy)
    assert fresh.receive_capacity() == 100 and fresh.may_accept()
    fresh.reads = policy.reads
    fresh.writes = policy.writes
    assert fresh.receive_capacity() == fresh.send_capacity() == 0
