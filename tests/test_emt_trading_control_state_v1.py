from __future__ import annotations

import pytest
from emt_gateway.facade import SideEffectPermit
from xuanling_tradecore.adapters.emt.trading_catalog_v1 import EmtOperationLane
from xuanling_tradecore.adapters.emt.trading_control_state_v1 import (
    EmtControlOperationProjectorV1,
    EmtControlOperationState,
    EmtControlOperationStateError,
)
from xuanling_tradecore.adapters.emt.trading_module_v1 import (
    EmtOperationCommandV1,
    EmtOperationReceiptV1,
)


def _fund_command() -> EmtOperationCommandV1:
    return EmtOperationCommandV1(
        operation_id="fund-1",
        official_name="FundTransfer",
        account_id="sim-account",
        scope_id="scope-a",
        session_epoch=3,
        native_arguments=(),
        approval_reference="fixture-approved",
    )


def _accepted_receipt() -> EmtOperationReceiptV1:
    return EmtOperationReceiptV1(
        operation_id="fund-1",
        official_name="FundTransfer",
        lane=EmtOperationLane.CONTROL_MUTATION,
        accepted=True,
        native_id=77,
        accepted_monotonic_ns=1,
        rejection_code=None,
    )


def test_t2_fund_transfer_requires_callback_then_authoritative_reconciliation() -> None:
    projector = EmtControlOperationProjectorV1()
    projector.record_pre_native(_fund_command())
    projector.record_call_started("fund-1")
    accepted = projector.record_receipt(_accepted_receipt())
    assert accepted.state is EmtControlOperationState.NATIVE_ACCEPTED
    callback = projector.observe_callback(
        scope_id="scope-a",
        session_epoch=3,
        native_id=77,
        callback_name="OnFundTransfer",
    )
    assert callback.state is EmtControlOperationState.CALLBACK_OBSERVED
    terminal = projector.reconcile(
        "fund-1", authoritative_complete=True, result_code="fund-transfer-settled"
    )
    assert terminal.state is EmtControlOperationState.AUTHORITATIVE_TERMINAL
    assert terminal.terminal


def test_t2_wrong_callback_or_native_serial_never_becomes_terminal() -> None:
    projector = EmtControlOperationProjectorV1()
    projector.record_pre_native(_fund_command())
    projector.record_call_started("fund-1")
    projector.record_receipt(_accepted_receipt())
    with pytest.raises(EmtControlOperationStateError, match="unowned"):
        projector.observe_callback(
            scope_id="scope-a", session_epoch=3, native_id=88, callback_name="OnFundTransfer"
        )
    with pytest.raises(EmtControlOperationStateError, match="does not match"):
        projector.observe_callback(
            scope_id="scope-a", session_epoch=3, native_id=77, callback_name="OnCreditCashRepay"
        )


def test_t2_native_rejection_is_terminal_without_retry() -> None:
    projector = EmtControlOperationProjectorV1()
    projector.record_pre_native(_fund_command())
    projector.record_call_started("fund-1")
    result = projector.record_receipt(
        EmtOperationReceiptV1(
            operation_id="fund-1",
            official_name="FundTransfer",
            lane=EmtOperationLane.CONTROL_MUTATION,
            accepted=False,
            native_id=None,
            accepted_monotonic_ns=None,
            rejection_code=123,
        )
    )
    assert result.state is EmtControlOperationState.NATIVE_REJECTED
    assert result.terminal_reason == "native_rejected:123"


def test_t2_non_control_order_is_rejected_from_control_projector() -> None:
    projector = EmtControlOperationProjectorV1()
    command = EmtOperationCommandV1(
        operation_id="order-1",
        official_name="InsertOrder",
        account_id="sim-account",
        scope_id="scope-a",
        session_epoch=3,
        native_arguments=(),
        approval_reference=SideEffectPermit.simulated_fixture().scope,
    )
    with pytest.raises(EmtControlOperationStateError, match="not a control"):
        projector.record_pre_native(command)
