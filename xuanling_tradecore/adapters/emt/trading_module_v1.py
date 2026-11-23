"""OMS/EMS composition for EMT's complete trading-only SDK surface.

The module deliberately keeps direct vendor invocation at the platform edge:
the existing fast ``EmtBrokerAdapter`` retains ownership of C0--C7 order and
cancel dispatch, while this module supplies typed catalog admission, query
dispatch and durable control-mutation sequencing for every other TraderApi
operation.  It never imports a vendor SDK or exposes generic method reflection.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns
from typing import Any, Mapping, Protocol, Sequence

from ...compatibility.emt_trading_platform import (
    EmtTraderFacade,
    NativeInvocation,
    QueryCompletion,
    QueryHandle,
    SideEffectPermit,
)
from .trading_catalog_v1 import (
    EmtOperationLane,
    EmtTradingCatalogError,
    TraderApiSpecV1,
    trader_api_spec_v1,
)
from .trading_scope_v1 import ALL_TRADING_RELEASE_SCOPE_V1, EmtTradingReleaseScopeV1


class EmtTradingModuleError(RuntimeError):
    """A complete-EMT operation would violate an OMS/EMS safety boundary."""


@dataclass(frozen=True)
class EmtOperationCommandV1:
    """Payload-free operation identity plus platform-owned native arguments.

    ``native_arguments`` are passed only to the already-loaded facade and must
    never be written to a journal, log, trace, exception, or OMS fact.  Their
    vendor-structure construction stays in a per-interface platform factory.
    """

    operation_id: str
    official_name: str
    account_id: str
    scope_id: str
    session_epoch: int
    native_arguments: Sequence[Any]
    approval_reference: str

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (
                self.operation_id,
                self.official_name,
                self.account_id,
                self.scope_id,
                self.approval_reference,
            )
        ):
            raise EmtTradingModuleError("operation identity and approval reference are required")
        if self.session_epoch <= 0:
            raise EmtTradingModuleError("operation session epoch must be positive")
        spec = trader_api_spec_v1(self.official_name)
        if not spec.mutating:
            raise EmtTradingModuleError("operation command requires a catalogued mutation")

    @property
    def spec(self) -> TraderApiSpecV1:
        return trader_api_spec_v1(self.official_name)


@dataclass(frozen=True)
class EmtOperationReceiptV1:
    operation_id: str
    official_name: str
    lane: EmtOperationLane
    accepted: bool
    native_id: int | None
    accepted_monotonic_ns: int | None
    rejection_code: int | None


class EmtOperationJournalPortV1(Protocol):
    """Durable operation-fact boundary; implementations must be Journal-first."""

    def pre_native(self, command: EmtOperationCommandV1) -> str:
        """Persist the complete intent and return its durable reference."""

    def call_started(self, command: EmtOperationCommandV1, *, pre_native_reference: str) -> str:
        """Persist CALL_STARTED before the native invocation."""

    def native_accepted(
        self,
        command: EmtOperationCommandV1,
        *,
        call_started_reference: str,
        native_id: int,
        accepted_monotonic_ns: int,
    ) -> None:
        """Persist a native receipt; it is not yet a business terminal state."""

    def native_rejected(
        self,
        command: EmtOperationCommandV1,
        *,
        call_started_reference: str,
        rejection_code: int | None,
    ) -> None:
        """Persist a local native rejection without creating a retry."""

    def native_outcome_unknown(
        self,
        command: EmtOperationCommandV1,
        *,
        call_started_reference: str,
        reason: str,
    ) -> None:
        """Persist an unknown-outcome fact and force later authoritative reconciliation."""


class EmtFastOperationPortV1(Protocol):
    """Existing C0--C7 owner for catalogued fast mutation kinds."""

    def dispatch_fast(self, command: EmtOperationCommandV1) -> EmtOperationReceiptV1:
        """Dispatch only after the existing fast Journal/route/guard fences."""


class EmtControlMutationDispatcherV1:
    """Journal-first dispatcher for low-frequency money, credit and exercise operations."""

    def __init__(
        self,
        *,
        facade: EmtTraderFacade,
        journal: EmtOperationJournalPortV1,
        release_scope: EmtTradingReleaseScopeV1 = ALL_TRADING_RELEASE_SCOPE_V1,
    ) -> None:
        self._facade = facade
        self._journal = journal
        self._release_scope = release_scope

    @property
    def release_scope(self) -> EmtTradingReleaseScopeV1:
        return self._release_scope

    def dispatch(
        self,
        command: EmtOperationCommandV1,
        *,
        permit: SideEffectPermit,
    ) -> EmtOperationReceiptV1:
        spec = self._release_scope.require_api(command.official_name)
        if spec.lane is not EmtOperationLane.CONTROL_MUTATION:
            raise EmtTradingModuleError(
                f"{command.official_name} must not bypass its {spec.lane.value} owner"
            )
        pre_native_reference = self._journal.pre_native(command)
        call_started_reference = self._journal.call_started(
            command,
            pre_native_reference=pre_native_reference,
        )
        try:
            invocation = self._facade.invoke(
                command.official_name,
                business_args=command.native_arguments,
                side_effect_permit=permit,
            )
        except BaseException as exc:
            self._journal.native_outcome_unknown(
                command,
                call_started_reference=call_started_reference,
                reason=type(exc).__name__,
            )
            raise
        native_id = _positive_native_id(invocation)
        if native_id is None:
            rejection = _last_error_code(self._facade)
            self._journal.native_rejected(
                command,
                call_started_reference=call_started_reference,
                rejection_code=rejection,
            )
            return EmtOperationReceiptV1(
                operation_id=command.operation_id,
                official_name=command.official_name,
                lane=spec.lane,
                accepted=False,
                native_id=None,
                accepted_monotonic_ns=None,
                rejection_code=rejection,
            )
        accepted = monotonic_ns()
        self._journal.native_accepted(
            command,
            call_started_reference=call_started_reference,
            native_id=native_id,
            accepted_monotonic_ns=accepted,
        )
        return EmtOperationReceiptV1(
            operation_id=command.operation_id,
            official_name=command.official_name,
            lane=spec.lane,
            accepted=True,
            native_id=native_id,
            accepted_monotonic_ns=accepted,
            rejection_code=None,
        )


class EmtTradingQueryServiceV1:
    """Catalogue-gated dispatcher for all TraderApi Query* operations."""

    def __init__(
        self,
        *,
        facade: EmtTraderFacade,
        release_scope: EmtTradingReleaseScopeV1 = ALL_TRADING_RELEASE_SCOPE_V1,
    ) -> None:
        self._facade = facade
        self._release_scope = release_scope

    @property
    def release_scope(self) -> EmtTradingReleaseScopeV1:
        return self._release_scope

    def begin(
        self,
        official_name: str,
        *,
        native_arguments: Sequence[Any] = (),
        correlation_id: str | None = None,
        profile: str = "ordinary",
        workload: str | None = None,
        completion: QueryCompletion | None = None,
        timeout_ns: int = 8_000_000_000,
    ) -> QueryHandle:
        spec = self._release_scope.require_api(official_name)
        if spec.lane is not EmtOperationLane.QUERY:
            raise EmtTradingModuleError(f"{official_name} is not a query-lane operation")
        return self._facade.invoke_query(
            official_name,
            business_args=native_arguments,
            correlation_id=correlation_id,
            profile=profile,
            workload=workload,
            completion=completion,
            timeout_ns=timeout_ns,
        )


class EmtFullTradingModuleV1:
    """Single strong-typed entry point for the complete EMT trading surface."""

    def __init__(
        self,
        *,
        query_service: EmtTradingQueryServiceV1,
        control_dispatcher: EmtControlMutationDispatcherV1,
        fast_operation_port: EmtFastOperationPortV1,
        release_scope: EmtTradingReleaseScopeV1 = ALL_TRADING_RELEASE_SCOPE_V1,
    ) -> None:
        self.query_service = query_service
        self._control_dispatcher = control_dispatcher
        self._fast_operation_port = fast_operation_port
        self._release_scope = release_scope
        if query_service.release_scope != release_scope:
            raise EmtTradingModuleError("query service release scope does not match full trading module")
        if control_dispatcher.release_scope != release_scope:
            raise EmtTradingModuleError("control dispatcher release scope does not match full trading module")

    def dispatch_mutation(
        self,
        command: EmtOperationCommandV1,
        *,
        permit: SideEffectPermit,
    ) -> EmtOperationReceiptV1:
        lane = self._release_scope.require_api(command.official_name).lane
        if lane is EmtOperationLane.FAST_EXECUTION:
            return self._fast_operation_port.dispatch_fast(command)
        if lane is EmtOperationLane.CONTROL_MUTATION:
            return self._control_dispatcher.dispatch(command, permit=permit)
        raise EmtTradingCatalogError(f"mutation {command.official_name} has no executable lane")


def _positive_native_id(invocation: NativeInvocation) -> int | None:
    value = invocation.return_value
    return value if type(value) is int and value > 0 else None


def _last_error_code(facade: EmtTraderFacade) -> int | None:
    """Read only a bounded numeric native error immediately after return zero."""

    try:
        error = facade.invoke("GetApiLastError").return_value
    except BaseException:
        return None
    if not isinstance(error, Mapping):
        return None
    code = error.get("error_id")
    return code if type(code) is int else None
