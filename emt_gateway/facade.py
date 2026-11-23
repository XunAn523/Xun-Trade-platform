"""Capability-gated facade over the official EMT TraderApi surface.

This module owns the one explicit route table from official 2.27.0 names to
the lower-camel Python binding. It never imports the vendor SDK and does not
retain request payloads. Session lifecycle remains owned by :mod:`session`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence
from uuid import uuid4

from .capabilities import (
    OFFICIAL_TRADER_API,
    CapabilityReport,
    CapabilityStatus,
    inspect_trader_api,
    python_api_name,
)
from .query_registry import QueryCompletion, QueryHandle, QueryRegistry
from .telemetry import NullPerformanceTrace, PerformanceTracePort


class FacadeError(RuntimeError):
    """The gateway cannot safely dispatch an EMT operation."""


class UnsupportedInterfaceError(FacadeError):
    """The loaded binding does not provide an official direct operation."""


class LifecycleOwnedError(FacadeError):
    """A caller tried to bypass the controlled session lifecycle."""


class SideEffectPermitError(FacadeError):
    """A mutating operation lacks its explicit simulated-fixture permit."""


class OperationKind(str, Enum):
    LIFECYCLE = "lifecycle"
    QUERY = "query"
    MUTATION = "mutation"
    LOOKUP = "lookup"


class ArgumentMode(str, Enum):
    EXPLICIT = "explicit"
    SESSION = "session"
    SESSION_REQUEST = "session_request"


@dataclass(frozen=True)
class OperationSpec:
    """Safe dispatch metadata for one official TraderApi method."""

    official_name: str
    binding_name: str
    kind: OperationKind
    argument_mode: ArgumentMode
    callback_name: str | None = None


@dataclass(frozen=True)
class SideEffectPermit:
    """Explicit capability required for a fixture to emit a mutation."""

    scope: str

    @classmethod
    def simulated_fixture(cls) -> SideEffectPermit:
        return cls(scope="simulated-fixture")


@dataclass(frozen=True)
class NativeInvocation:
    official_name: str
    return_value: Any
    correlation_id: str | None


_SESSION_OWNED = frozenset(
    {
        "CreateTraderApi",
        "SetCpuAffinity",
        "SetSoftwareVersion",
        "SetHeartBeatInterval",
        "SubscribePublicTopic",
        "RegisterSpi",
        "Login",
        "Logout",
        "Release",
    }
)
_MUTATIONS = frozenset(
    {
        "InsertOrder",
        "CancelOrder",
        "FundTransfer",
        "CreditCashRepay",
        "CreditCashRepaySpecifiedDebt",
        "CreditSellStockRepayDebtInterestFee",
        "CreditExtendDebtDate",
        "CreditQuotaTransfer",
        "CreditMarginAvlTransfer",
        "InsertOptionExerciseOrder",
        "InsertOptionCombinedOrder",
    }
)
_NO_SESSION = frozenset(
    {
        "SetCpuAffinity",
        "GetTradingDay",
        "GetApiVersion",
        "GetApiLastError",
        "GetClientIDByEMTID",
        "GetAccountByEMTID",
    }
)
_QUERY_CALLBACK_OVERRIDES = {
    "QueryOrderByEMTID": "OnQueryOrder",
    "QueryOrders": "OnQueryOrder",
    "QueryOrdersByPage": "OnQueryOrderByPage",
    "QueryUnfinishedOrders": "OnQueryOrder",
    "QueryTradesByEMTID": "OnQueryTrade",
    "QueryTrades": "OnQueryTrade",
    "QueryTradesByPage": "OnQueryTradeByPage",
    "QueryETFTickerBasket": "OnQueryETFBasket",
    "QueryOptionCombinedUnfinishedOrders": "OnQueryOptionCombinedOrders",
    "QueryOptionCombinedOrderByEMTID": "OnQueryOptionCombinedOrders",
    "QueryOptionCombinedTradesByEMTID": "OnQueryOptionCombinedTrades",
}
# These callbacks are documented by the supplied Python binding as one-shot
# replies without an ``is_last`` argument.  The list is intentionally
# explicit: every other query, including ``QueryYesterdayAsset``, must wait
# for the binding's terminal ``is_last=True`` callback.  In particular, the
# current Linux 2.27 binding exposes ``onQueryYesterdayAsset(data, error,
# request_id, is_last, session_id)``; treating its first response as terminal
# would hide a missing final response from recovery acceptance.
_SINGLE_RESPONSE_QUERIES = frozenset(
    {
        "QueryFund",
        "QueryOtherServerFund",
        "QueryCreditFundInfo",
        "QueryCreditAssetDebtInfo",
        "QueryCreditExcessStock",
        "QueryMulCreditExcessStock",
        "QueryCreditFundExtraInfo",
        "QueryOptionCombinedExecPosition",
        "QueryHKConnectIndicativeExchangeRateInfo",
    }
)


def operation_specs() -> tuple[OperationSpec, ...]:
    """Return all 86 official API entries, including controlled rejections."""

    specs: list[OperationSpec] = []
    for official_name in OFFICIAL_TRADER_API:
        if official_name in _SESSION_OWNED:
            kind, mode, callback = OperationKind.LIFECYCLE, ArgumentMode.EXPLICIT, None
        elif official_name.startswith("Query"):
            kind, mode = OperationKind.QUERY, ArgumentMode.SESSION_REQUEST
            callback = _QUERY_CALLBACK_OVERRIDES.get(official_name, f"On{official_name}")
        elif official_name in _MUTATIONS:
            kind, mode, callback = OperationKind.MUTATION, ArgumentMode.SESSION, None
        else:
            kind, mode, callback = OperationKind.LOOKUP, ArgumentMode.SESSION, None
            if official_name in _NO_SESSION:
                mode = ArgumentMode.EXPLICIT
        specs.append(
            OperationSpec(official_name, python_api_name(official_name), kind, mode, callback)
        )
    return tuple(specs)


_SPECS = {spec.official_name: spec for spec in operation_specs()}


class EmtTraderFacade:
    """Session-bound, capability-gated dispatcher for all TraderApi entries.

    ``business_args`` pass directly to the binding and are never logged,
    retained or added to trace events. Query request IDs are allocated by the
    supplied registry; successful query completion remains callback-driven.
    """

    def __init__(
        self,
        api: Any,
        *,
        session_id: int,
        capability_report: CapabilityReport | None = None,
        query_registry: QueryRegistry | None = None,
        trace: PerformanceTracePort | None = None,
    ) -> None:
        if session_id == 0:
            raise FacadeError("a non-zero logged-in session_id is required")
        self._api = api
        self._session_id = session_id
        self._report = capability_report or inspect_trader_api(api)
        self._queries = query_registry
        self._trace = trace or NullPerformanceTrace()

    @property
    def capabilities(self) -> CapabilityReport:
        return self._report

    def spec_for(self, official_name: str) -> OperationSpec:
        try:
            return _SPECS[official_name]
        except KeyError as exc:
            raise FacadeError(f"unknown official TraderApi method: {official_name}") from exc

    def invoke(
        self,
        official_name: str,
        *,
        business_args: Sequence[Any] = (),
        side_effect_permit: SideEffectPermit | None = None,
        profile: str = "ordinary",
        workload: str | None = None,
        trace_call: bool = False,
    ) -> NativeInvocation:
        """Call a direct operation using its explicit session argument convention."""

        spec = self.spec_for(official_name)
        self._ensure_callable(spec)
        if spec.kind is OperationKind.LIFECYCLE:
            raise LifecycleOwnedError(f"{official_name} is owned by EmtSession")
        if spec.kind is OperationKind.QUERY:
            raise FacadeError(
                f"use invoke_query for {official_name} so request_id ownership is preserved"
            )
        if spec.kind is OperationKind.MUTATION and side_effect_permit is None:
            raise SideEffectPermitError(f"{official_name} requires an explicit SideEffectPermit")
        if spec.kind is OperationKind.MUTATION and trace_call:
            raise FacadeError(
                f"{official_name} requires the durable command workflow before C0-C7 trace emission"
            )
        resolved_workload = workload or _workload_for(spec)
        correlation_id = str(uuid4()) if trace_call else None
        if correlation_id is not None:
            self._trace.begin(
                correlation_id,
                kind="api",
                interface=official_name,
                profile=profile,
                workload=resolved_workload,
            )
            self._trace.mark(correlation_id, "C1")
        arguments = self._arguments(spec, business_args, request_id=None)
        if correlation_id is not None:
            self._trace.mark(correlation_id, "C3")
        try:
            result = getattr(self._api, spec.binding_name)(*arguments)
        except Exception as exc:
            if correlation_id is not None:
                self._trace.mark(correlation_id, "C4")
                self._trace.complete(correlation_id, "failed", type(exc).__name__)
            raise
        if correlation_id is not None:
            self._trace.mark(correlation_id, "C4")
            self._trace.complete(correlation_id, "ok")
        return NativeInvocation(
            official_name=official_name, return_value=result, correlation_id=correlation_id
        )

    def invoke_query(
        self,
        official_name: str,
        *,
        business_args: Sequence[Any] = (),
        correlation_id: str | None = None,
        profile: str = "ordinary",
        workload: str | None = None,
        completion: QueryCompletion | None = None,
        timeout_ns: int = 8_000_000_000,
    ) -> QueryHandle:
        """Allocate a request ID, call one query, and leave terminality to SPI."""

        spec = self.spec_for(official_name)
        self._ensure_callable(spec)
        if spec.kind is not OperationKind.QUERY or spec.callback_name is None:
            raise FacadeError(f"{official_name} is not a callback-driven query")
        if self._queries is None:
            raise FacadeError("QueryRegistry is required for query dispatch")
        resolved_completion = completion or query_completion_for(official_name)
        handle = self._queries.open(
            api_name=official_name,
            callback_name=spec.callback_name,
            correlation_id=correlation_id or str(uuid4()),
            profile=profile,
            workload=workload or _workload_for(spec),
            completion=resolved_completion,
            timeout_ns=timeout_ns,
        )
        self._queries.mark_native_started(handle.request_id)
        try:
            result = int(
                getattr(self._api, spec.binding_name)(
                    *self._arguments(spec, business_args, request_id=handle.request_id)
                )
            )
        except Exception:
            self._queries.mark_native_returned(handle.request_id, 1)
            raise
        self._queries.mark_native_returned(handle.request_id, result)
        return handle

    def _ensure_callable(self, spec: OperationSpec) -> None:
        entry = self._report.entry_for(spec.official_name)
        if entry.status is not CapabilityStatus.DIRECT:
            detail = entry.reason or entry.status.value
            raise UnsupportedInterfaceError(f"{spec.official_name} is unavailable: {detail}")

    def _arguments(
        self,
        spec: OperationSpec,
        business_args: Sequence[Any],
        *,
        request_id: int | None,
    ) -> tuple[Any, ...]:
        arguments = tuple(business_args)
        if spec.argument_mode is ArgumentMode.EXPLICIT:
            return arguments
        if spec.argument_mode is ArgumentMode.SESSION:
            return arguments + (self._session_id,)
        if request_id is None or request_id <= 0:
            raise FacadeError(f"{spec.official_name} requires a positive owned request_id")
        return arguments + (self._session_id, request_id)


def required_direct_interfaces() -> tuple[str, ...]:
    """List direct interfaces a complete 2.27 binding would expose (85 names)."""

    return tuple(name for name in OFFICIAL_TRADER_API if name != "RegisterSpi")


def unsupported_interfaces(report: CapabilityReport) -> tuple[str, ...]:
    return tuple(
        entry.official_name
        for entry in report.entries
        if entry.status is CapabilityStatus.UNSUPPORTED
    )


def query_completion_for(official_name: str) -> QueryCompletion:
    """Return the binding-specific terminality contract for one query API."""

    spec = _SPECS.get(official_name)
    if spec is None or spec.kind is not OperationKind.QUERY:
        raise FacadeError(f"{official_name} is not a callback-driven query")
    return (
        QueryCompletion.SINGLE
        if official_name in _SINGLE_RESPONSE_QUERIES
        else QueryCompletion.LAST
    )


def _workload_for(spec: OperationSpec) -> str:
    return "cold" if spec.official_name in OFFICIAL_TRADER_API[:16] else "steady"
