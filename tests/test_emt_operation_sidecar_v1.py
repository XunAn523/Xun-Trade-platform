from __future__ import annotations

import pytest

from xuanling_tradecore.adapters.emt.trading_permissions_v1 import (
    EmtTradingPermissionError,
    EmtTradingPermissionSetV1,
    required_permission_for_trader_api_v1,
)
from xuanling_tradecore.oms.emt_operation_sidecar_v1 import (
    EmtOperationCommittedFactV1,
    EmtOperationFactKindV1,
    EmtOperationOmsSidecarV1,
    EmtOperationOmsStateV1,
    EmtOperationSidecarError,
)


def _fact(position: int, kind: EmtOperationFactKindV1, *, native_id: int | None = None) -> EmtOperationCommittedFactV1:
    return EmtOperationCommittedFactV1(
        scope_id="scope-a",
        session_epoch=7,
        position=position,
        operation_id="credit-repay-1",
        official_name="CreditCashRepay",
        kind=kind,
        payload_hash="a" * 64,
        native_id=native_id,
        terminal_reason="verified" if kind is EmtOperationFactKindV1.AUTHORITATIVE_TERMINAL else None,
    )


def test_t5_control_operation_projection_is_committed_fact_only_and_replay_safe() -> None:
    sidecar = EmtOperationOmsSidecarV1()
    sidecar.apply(_fact(10, EmtOperationFactKindV1.INTENT))
    sidecar.apply(_fact(11, EmtOperationFactKindV1.PRE_NATIVE))
    sidecar.apply(_fact(12, EmtOperationFactKindV1.CALL_STARTED))
    sidecar.apply(_fact(13, EmtOperationFactKindV1.NATIVE_ACCEPTED, native_id=99))
    sidecar.apply(_fact(14, EmtOperationFactKindV1.CALLBACK_OBSERVED, native_id=99))
    terminal = sidecar.apply(_fact(15, EmtOperationFactKindV1.AUTHORITATIVE_TERMINAL, native_id=99))
    assert terminal.state is EmtOperationOmsStateV1.AUTHORITATIVE_TERMINAL
    assert terminal.terminal
    assert sidecar.apply(_fact(15, EmtOperationFactKindV1.AUTHORITATIVE_TERMINAL, native_id=99)) == terminal


def test_t5_projector_rejects_missing_predecessor_and_native_serial_rewrite() -> None:
    sidecar = EmtOperationOmsSidecarV1()
    with pytest.raises(EmtOperationSidecarError, match="no committed predecessor"):
        sidecar.apply(_fact(10, EmtOperationFactKindV1.CALL_STARTED))
    sidecar.apply(_fact(10, EmtOperationFactKindV1.INTENT))
    sidecar.apply(_fact(11, EmtOperationFactKindV1.PRE_NATIVE))
    sidecar.apply(_fact(12, EmtOperationFactKindV1.CALL_STARTED))
    sidecar.apply(_fact(13, EmtOperationFactKindV1.NATIVE_ACCEPTED, native_id=99))
    with pytest.raises(EmtOperationSidecarError, match="native serial changed"):
        sidecar.apply(_fact(14, EmtOperationFactKindV1.CALLBACK_OBSERVED, native_id=100))


def test_t5_permissions_are_least_privilege_and_never_allow_a_wildcard() -> None:
    grants = EmtTradingPermissionSetV1(frozenset({"trade.order", "native.callback.read"}))
    assert grants.require_api("InsertOrder") == "trade.order"
    grants.require_callback_read()
    with pytest.raises(EmtTradingPermissionError, match="account.transfer"):
        grants.require_api("FundTransfer")
    assert required_permission_for_trader_api_v1("QueryCreditDebtInfo") == "credit.query.read"
    with pytest.raises(EmtTradingPermissionError, match="session lifecycle"):
        required_permission_for_trader_api_v1("Login")
