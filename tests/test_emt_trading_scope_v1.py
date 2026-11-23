from __future__ import annotations

import pytest
from emt_gateway.facade import SideEffectPermit
from emt_gateway.query_registry import QueryRegistry
from xuanling_tradecore.adapters.emt.trading_module_v1 import (
    EmtControlMutationDispatcherV1,
    EmtFullTradingModuleV1,
    EmtOperationCommandV1,
    EmtOperationReceiptV1,
    EmtTradingQueryServiceV1,
)
from xuanling_tradecore.adapters.emt.trading_scope_v1 import (
    ALL_TRADING_RELEASE_SCOPE_V1,
    ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    EmtTradingReleaseScopeError,
)


class _Api:
    def queryOrders(self, request: object, session_id: int, request_id: int) -> int:
        return 0


class _FastPort:
    def dispatch_fast(self, command: EmtOperationCommandV1) -> EmtOperationReceiptV1:
        raise AssertionError("disabled release scope must reject before the fast owner")


class _Control:
    def dispatch(
        self, command: EmtOperationCommandV1, *, permit: SideEffectPermit
    ) -> EmtOperationReceiptV1:
        raise AssertionError("disabled release scope must reject before the control owner")


def _option_command() -> EmtOperationCommandV1:
    return EmtOperationCommandV1(
        operation_id="scope-test",
        official_name="InsertOptionCombinedOrder",
        account_id="sim-account",
        scope_id="scope-a",
        session_epoch=1,
        native_arguments=(),
        approval_reference="approved-test",
    )


def test_ordinary_credit_scope_allows_normal_and_credit_but_denies_option_api() -> None:
    assert (
        ORDINARY_CREDIT_RELEASE_SCOPE_V1.require_api("InsertOrder").official_name == "InsertOrder"
    )
    assert (
        ORDINARY_CREDIT_RELEASE_SCOPE_V1.require_api("CreditCashRepay").official_name
        == "CreditCashRepay"
    )
    with pytest.raises(EmtTradingReleaseScopeError, match="CAPABILITY_UNAVAILABLE"):
        ORDINARY_CREDIT_RELEASE_SCOPE_V1.require_api("InsertOptionCombinedOrder")


def test_ordinary_credit_module_rejects_option_before_fast_dispatch() -> None:
    from emt_gateway.facade import EmtTraderFacade

    query = EmtTradingQueryServiceV1(
        facade=EmtTraderFacade(_Api(), session_id=1, query_registry=QueryRegistry()),
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    )
    control = _Control()
    control.release_scope = ORDINARY_CREDIT_RELEASE_SCOPE_V1  # type: ignore[attr-defined]
    module = EmtFullTradingModuleV1(
        query_service=query,
        control_dispatcher=control,  # type: ignore[arg-type]
        fast_operation_port=_FastPort(),  # type: ignore[arg-type]
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    )
    with pytest.raises(EmtTradingReleaseScopeError, match="CAPABILITY_UNAVAILABLE"):
        module.dispatch_mutation(_option_command(), permit=SideEffectPermit.simulated_fixture())
    with pytest.raises(EmtTradingReleaseScopeError, match="CAPABILITY_UNAVAILABLE"):
        query.begin("QueryOptionCombinedOrders")


def test_scoped_control_dispatcher_rejects_option_before_journal_or_sdk() -> None:
    from emt_gateway.facade import EmtTraderFacade

    class _Journal:
        def pre_native(self, command: EmtOperationCommandV1) -> str:
            raise AssertionError("scope must reject before journal write")

        def call_started(self, command: EmtOperationCommandV1, *, pre_native_reference: str) -> str:
            raise AssertionError("scope must reject before journal write")

    dispatcher = EmtControlMutationDispatcherV1(
        facade=EmtTraderFacade(_Api(), session_id=1, query_registry=QueryRegistry()),
        journal=_Journal(),  # type: ignore[arg-type]
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    )
    command = EmtOperationCommandV1(
        operation_id="scope-test-control",
        official_name="InsertOptionExerciseOrder",
        account_id="sim-account",
        scope_id="scope-a",
        session_epoch=1,
        native_arguments=(),
        approval_reference="approved-test",
    )
    with pytest.raises(EmtTradingReleaseScopeError, match="CAPABILITY_UNAVAILABLE"):
        dispatcher.dispatch(command, permit=SideEffectPermit.simulated_fixture())


def test_full_module_rejects_mixed_release_scope_composition() -> None:
    from emt_gateway.facade import EmtTraderFacade
    from xuanling_tradecore.adapters.emt.trading_module_v1 import EmtTradingModuleError

    query = EmtTradingQueryServiceV1(
        facade=EmtTraderFacade(_Api(), session_id=1, query_registry=QueryRegistry()),
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    )
    control = _Control()
    control.release_scope = ALL_TRADING_RELEASE_SCOPE_V1  # type: ignore[attr-defined]
    with pytest.raises(EmtTradingModuleError, match="release scope does not match"):
        EmtFullTradingModuleV1(
            query_service=query,
            control_dispatcher=control,  # type: ignore[arg-type]
            fast_operation_port=_FastPort(),  # type: ignore[arg-type]
            release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
        )
