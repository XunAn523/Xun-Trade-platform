"""Typed, payload-free ingress for EMT's approved read-only query callbacks.

This module deliberately has no EMT SDK or legacy-gateway import.  A future
native bridge may call :meth:`EmtQueryCallbackIngress.on_query_order_terminal`
after it has discarded the SDK row payload.  The resulting TradeCore event
contains only query terminal metadata, while unknown, duplicate, or
session-mismatched callbacks become conservative callback-gap evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from typing import Callable, Protocol

from ...domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    CallbackGapEvidence,
    RecoveryPlan,
    TradingDayEpoch,
)
from ..base import BrokerEventObserver, BrokerQuery, QueryHandle, RecoveryHandle
from ..mapping import ActivatedVenueMapping
from .namespace_recovery import (
    EmtAuthoritativeOrderObserverV1,
    EmtAuthoritativeOrderV1,
    EmtNamespaceRecoveryError,
)

_MAX_NATIVE_REQUEST_ID = 2**31 - 1
# EMT documents 200 as the maximum requested count for paged order, trade
# and position scans. This recovery-only boundary reduces session requests
# while retaining a deterministic, fail-closed cap.
_MAX_PAGE_SIZE = 200
_QUERY_CALLBACKS = {
    "unfinished_orders": "onQueryOrder",
    "orders": "onQueryOrder",
    "trades": "onQueryTrade",
    "orders_by_page": "onQueryOrderByPage",
    "trades_by_page": "onQueryTradeByPage",
    "positions": "onQueryPosition",
    "positions_by_page": "onQueryPositionByPage",
    "assets": "onQueryAsset",
    "funds": "onQueryFund",
    "credit_fund_info": "onQueryCreditFundInfo",
    "credit_asset_debt": "onQueryCreditAssetDebtInfo",
    "credit_position_extra_info_sz": "onQueryCreditPositionExtraInfo",
    "credit_position_extra_info_sh": "onQueryCreditPositionExtraInfo",
}
_PAGE_QUERY_KINDS = frozenset({"orders_by_page", "trades_by_page", "positions_by_page"})
_AUTHORITATIVE_ROW_FAILURE_CODES = frozenset(
    {
        "row_callback_arity_invalid",
        "row_not_mapping",
        "row_empty",
        "row_extraction_unclassified",
        "order_client_id_missing",
        "order_client_id_non_integer",
        "order_client_id_nonpositive",
        "order_emt_id_missing",
        "order_emt_id_non_integer",
        "order_emt_id_nonpositive",
        "order_status_missing",
        "order_status_non_integer",
        "order_status_negative",
        "filled_qty_missing",
        "filled_qty_non_integer",
        "filled_qty_negative",
        "leaves_qty_missing",
        "leaves_qty_non_integer",
        "leaves_qty_negative",
        "authoritative_scan_not_started",
        "authoritative_scan_already_complete",
        "authoritative_scan_conflicting_emt_identity",
        "authoritative_collector_rejected_row",
        "authoritative_collector_rejected_terminal",
    }
)
_QUERY_CALLBACK_GAP_REASON_CODES = frozenset(
    {
        "native_session_mismatch",
        "native_request_unregistered",
        "callback_name_mismatch",
        "authoritative_row_rejected",
        "authoritative_terminal_rejected",
    }
)


class EmtReadOnlyIngressError(RuntimeError):
    """An EMT read-only transport or callback violates its typed contract."""


def _is_authoritative_row_failure(value: str) -> bool:
    """Accept only a fixed diagnostic vocabulary, never vendor-derived text."""

    return type(value) is str and value in _AUTHORITATIVE_ROW_FAILURE_CODES


def _authoritative_collector_failure_code(error: EmtNamespaceRecoveryError) -> str:
    """Map internally generated recovery errors to a finite safe vocabulary."""

    return {
        "authoritative scan series has no active scan": "authoritative_scan_not_started",
        "authoritative recovery scan is already complete": "authoritative_scan_already_complete",
        "authoritative recovery scan has conflicting EMT order identity": (
            "authoritative_scan_conflicting_emt_identity"
        ),
    }.get(str(error), "authoritative_collector_rejected_row")


class EmtUnfinishedOrdersTransport(Protocol):
    """Backward-compatible minimum transport used by the original narrow port.

    The expanded port performs runtime capability checks for its optional
    order/trade/page methods, so an existing unfinished-order-only transport
    remains valid for the one operation it already implemented.
    """

    def query_unfinished_orders(self, native_request_id: int) -> int:
        """Return the SDK return code without retaining a vendor row."""

        ...


@dataclass(frozen=True)
class EmtReadOnlyIngressSnapshot:
    """Inspectable state that never contains raw callback data or credentials."""

    observer_attached: bool
    native_session_bound: bool
    active_query_count: int
    next_native_request_id: int
    normalized_terminal_callbacks: int
    callback_gap_count: int
    callback_gap_reason_counts: tuple[tuple[str, int], ...]
    authoritative_row_failure_counts: tuple[tuple[str, int], ...]


@dataclass
class _RegisteredQuery:
    request: BrokerQuery
    native_request_id: int
    callback_name: str
    result_count: int = 0


class EmtQueryCallbackIngress:
    """Correlate the one approved EMT terminal callback without its payload."""

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        trading_day: TradingDayEpoch,
        native_session_id: int | None,
        session_epoch: int,
        authoritative_order_observer: EmtAuthoritativeOrderObserverV1 | None = None,
        authoritative_row_failure_observer: Callable[[str], None] | None = None,
    ) -> None:
        if mapping.manifest.venue_id != "EMT":
            raise EmtReadOnlyIngressError("EMT query ingress requires an EMT mapping")
        if native_session_id is not None and native_session_id <= 0:
            raise EmtReadOnlyIngressError("EMT query ingress requires a positive native session")
        if session_epoch <= 0:
            raise EmtReadOnlyIngressError("EMT query ingress requires a positive session epoch")
        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtReadOnlyIngressError("EMT query ingress requires scope ownership")
        self._mapping = mapping
        self._trading_day = trading_day
        self._native_session_id = native_session_id
        self._session_epoch = session_epoch
        self._observer: BrokerEventObserver | None = None
        self._queries: dict[int, _RegisteredQuery] = {}
        self._sequence = 0
        self._normalized_terminal_callbacks = 0
        self._callback_gap_count = 0
        # Fixed, payload-free categories help distinguish a bridge/session
        # defect from an authority collector rejection.  They never make a
        # callback gap eligible for acceptance.
        self._callback_gap_reason_counts: dict[str, int] = {}
        self._authoritative_row_failure_counts: dict[str, int] = {}
        self._authoritative_order_observer = authoritative_order_observer
        self._authoritative_row_failure_observer = authoritative_row_failure_observer
        self._lock = RLock()

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        assert ownership is not None
        return ownership.adapter_instance_id

    @property
    def account_id(self) -> str:
        return self._mapping.runtime.account_capabilities.account_id

    @property
    def captures_authoritative_order_rows(self) -> bool:
        """Whether this explicitly constructed ingress may retain five row scalars.

        Normal read-only operation remains payload-free.  Only a bounded R1
        recovery composition or an F2 exact-owned-target preflight supplies
        an observer and therefore reaches this path.
        """

        return self._authoritative_order_observer is not None

    @property
    def callback_gap_reason_counts(self) -> dict[str, int]:
        """Return a copy of fixed, payload-free callback-gap categories."""

        with self._lock:
            return dict(self._callback_gap_reason_counts)

    @property
    def authoritative_row_failure_counts(self) -> dict[str, int]:
        """Return fixed R1 scalar-extraction failures without a vendor row."""

        with self._lock:
            return dict(self._authoritative_row_failure_counts)

    def attach_observer(self, observer: BrokerEventObserver) -> None:
        with self._lock:
            if self._observer is not None and self._observer is not observer:
                raise EmtReadOnlyIngressError("EMT query ingress already has another observer")
            self._observer = observer

    def bind_native_session(self, native_session_id: int) -> None:
        """Bind the native session after callback ingress is safely attached."""
        if native_session_id <= 0:
            raise EmtReadOnlyIngressError("EMT query ingress requires a positive native session")
        with self._lock:
            self._require_observer()
            if self._native_session_id is not None:
                raise EmtReadOnlyIngressError("EMT query ingress native session is already bound")
            self._native_session_id = native_session_id

    def detach_observer(self, observer: BrokerEventObserver) -> None:
        with self._lock:
            if self._observer is not observer:
                raise EmtReadOnlyIngressError("EMT query ingress observer does not match")
            self._observer = None
            self._queries.clear()

    def register_query(
        self,
        request: BrokerQuery,
        *,
        native_request_id: int,
        callback_name: str,
    ) -> QueryHandle:
        """Track an exact account-scoped request before the native call is made."""
        if not 1 <= native_request_id <= _MAX_NATIVE_REQUEST_ID:
            raise EmtReadOnlyIngressError("EMT native request id is outside the supported range")
        self._require_request_identity(request)
        expected_callback = _QUERY_CALLBACKS.get(request.query_kind)
        if expected_callback is None or callback_name != expected_callback:
            raise EmtReadOnlyIngressError("EMT query kind has no approved callback mapping")
        with self._lock:
            self._require_observer()
            if native_request_id in self._queries:
                raise EmtReadOnlyIngressError("EMT native request id is already active")
            self._queries[native_request_id] = _RegisteredQuery(
                request,
                native_request_id,
                callback_name,
            )
        return QueryHandle(request.request_id, self.adapter_id, self.adapter_instance_id)

    def abandon_query(self, *, native_request_id: int) -> None:
        with self._lock:
            self._queries.pop(native_request_id, None)

    def on_query_order_terminal(
        self,
        *,
        native_request_id: int,
        native_session_id: int,
        error_id: int | None,
        is_last: bool,
        received_monotonic_ns: int,
    ) -> BrokerEvent | None:
        """Compatibility wrapper for the documented non-page order callback."""

        return self.on_query_terminal(
            callback_name="onQueryOrder",
            native_request_id=native_request_id,
            native_session_id=native_session_id,
            error_id=error_id,
            is_last=is_last,
            received_monotonic_ns=received_monotonic_ns,
        )

    def on_query_callback(
        self,
        *,
        callback_name: str,
        native_request_id: int,
        native_session_id: int,
        error_id: int | None,
        is_last: bool,
        row_seen: bool,
        received_monotonic_ns: int,
        authoritative_order: EmtAuthoritativeOrderV1 | None = None,
        authoritative_order_failure: str | None = None,
        page_reference: int | None = None,
        page_result_count: int | None = None,
    ) -> BrokerEvent | None:
        """Count payload-free row-presence facts and normalize the terminal callback.

        EMT returns query output as zero or more callbacks.  The platform
        bridge supplies only the boolean ``row_seen`` and immediately drops
        the vendor row.  The resulting terminal count is enough to prove an
        empty unfinished-order query without retaining business data.
        """

        if callback_name not in _QUERY_CALLBACKS.values():
            raise EmtReadOnlyIngressError("EMT query callback name is not approved")
        if received_monotonic_ns < 0:
            raise EmtReadOnlyIngressError("EMT callback receive time must be nonnegative")
        if error_id is not None and error_id < 0:
            raise EmtReadOnlyIngressError("EMT callback error id must be nonnegative")
        if type(row_seen) is not bool:
            raise EmtReadOnlyIngressError("EMT query row presence must be boolean")
        if authoritative_order is not None and not row_seen:
            raise EmtReadOnlyIngressError("authoritative EMT order row lacks row presence")
        if authoritative_order is not None and authoritative_order_failure is not None:
            raise EmtReadOnlyIngressError(
                "authoritative EMT row has conflicting extraction evidence"
            )
        if authoritative_order_failure is not None and not _is_authoritative_row_failure(
            authoritative_order_failure
        ):
            raise EmtReadOnlyIngressError("authoritative EMT row failure code is invalid")
        if page_reference is not None and page_reference < 0:
            raise EmtReadOnlyIngressError("EMT query page reference must be nonnegative")
        if page_result_count is not None and page_result_count < 0:
            raise EmtReadOnlyIngressError("EMT query page result count must be nonnegative")
        with self._lock:
            observer = self._require_observer()
            registered = self._queries.get(native_request_id)
            if (
                native_session_id != self._native_session_id
                or registered is None
                or registered.callback_name != callback_name
            ):
                self._record_callback_gap_reason(
                    self._registration_gap_reason(
                        native_session_id=native_session_id,
                        registered=registered,
                        callback_name=callback_name,
                    )
                )
                gap = self._new_gap(received_monotonic_ns)
                self._callback_gap_count += 1
                event = None
            else:
                authoritative_observer = self._authoritative_order_observer
                authoritative_required = (
                    authoritative_observer is not None
                    and registered.request.capture_authoritative_order_rows
                )
                is_page_query = registered.request.query_kind in _PAGE_QUERY_KINDS
                if is_page_query != (page_reference is not None and page_result_count is not None):
                    raise EmtReadOnlyIngressError(
                        "EMT query callback pagination metadata does not match request"
                    )
                if authoritative_required and row_seen and authoritative_order is None:
                    self._observe_authoritative_row_failure(
                        authoritative_order_failure or "row_extraction_unclassified"
                    )
                    self._record_callback_gap_reason("authoritative_row_rejected")
                    gap = self._new_gap(received_monotonic_ns)
                    self._callback_gap_count += 1
                    self._queries.pop(native_request_id, None)
                    event = None
                else:
                    try:
                        # The native bridge may have transiently extracted the
                        # five recovery scalars because this session supports
                        # R1.  A normal certificate/query request still has no
                        # authority to feed the active scan collector.  Drop
                        # those scalars here; otherwise an idle scan series
                        # treats the unrelated row as an invalid overlap.
                        if authoritative_required and authoritative_order is not None:
                            if authoritative_observer is None:
                                raise EmtReadOnlyIngressError(
                                    "authoritative EMT order row arrived without a recovery observer"
                                )
                            authoritative_observer.on_authoritative_order_row(
                                query_kind=registered.request.query_kind,
                                order=authoritative_order,
                            )
                        if row_seen:
                            registered.result_count += 1
                        if not is_last:
                            return None
                        if authoritative_required:
                            assert authoritative_observer is not None
                            authoritative_observer.on_authoritative_order_terminal(
                                query_kind=registered.request.query_kind,
                                error_id=error_id,
                                page_has_more=(
                                    is_page_query
                                    and page_result_count == registered.request.page_size
                                ),
                            )
                    except EmtNamespaceRecoveryError as exc:
                        if authoritative_required:
                            self._observe_authoritative_row_failure(
                                _authoritative_collector_failure_code(exc)
                            )
                        self._record_callback_gap_reason("authoritative_row_rejected")
                        gap = self._new_gap(received_monotonic_ns)
                        self._callback_gap_count += 1
                        self._queries.pop(native_request_id, None)
                        event = None
                    except EmtReadOnlyIngressError:
                        if authoritative_required:
                            self._observe_authoritative_row_failure(
                                "authoritative_collector_rejected_terminal"
                            )
                        self._record_callback_gap_reason("authoritative_terminal_rejected")
                        gap = self._new_gap(received_monotonic_ns)
                        self._callback_gap_count += 1
                        self._queries.pop(native_request_id, None)
                        event = None
                    else:
                        if not is_last:
                            return None
                        self._queries.pop(native_request_id, None)
                        event = self._new_query_event(
                            registered,
                            error_id,
                            received_monotonic_ns,
                            page_reference=page_reference,
                            page_result_count=page_result_count,
                        )
                        self._normalized_terminal_callbacks += 1
                        gap = None
        if gap is not None:
            observer.on_callback_gap(gap)
            return None
        assert event is not None
        observer.on_broker_event(event)
        return event

    def _observe_authoritative_row_failure(self, failure_code: str) -> None:
        """Emit a static, payload-free R1 probe diagnostic when explicitly enabled."""

        if not _is_authoritative_row_failure(failure_code):
            raise AssertionError("EMT authoritative row failure is not approved")
        self._authoritative_row_failure_counts[failure_code] = (
            self._authoritative_row_failure_counts.get(failure_code, 0) + 1
        )

        observer = self._authoritative_row_failure_observer
        if observer is None:
            return
        try:
            observer(failure_code)
        except BaseException:
            # A diagnostic observer must never alter the conservative callback-gap decision.
            return

    def on_query_terminal(
        self,
        *,
        callback_name: str,
        native_request_id: int,
        native_session_id: int,
        error_id: int | None,
        is_last: bool,
        received_monotonic_ns: int,
        page_reference: int | None = None,
        page_result_count: int | None = None,
    ) -> BrokerEvent | None:
        """Normalize terminal scalar metadata while discarding every vendor row."""

        if callback_name not in _QUERY_CALLBACKS.values():
            raise EmtReadOnlyIngressError("EMT query callback name is not approved")
        if received_monotonic_ns < 0:
            raise EmtReadOnlyIngressError("EMT callback receive time must be nonnegative")
        if error_id is not None and error_id < 0:
            raise EmtReadOnlyIngressError("EMT callback error id must be nonnegative")
        if page_reference is not None and page_reference < 0:
            raise EmtReadOnlyIngressError("EMT query page reference must be nonnegative")
        if page_result_count is not None and page_result_count < 0:
            raise EmtReadOnlyIngressError("EMT query page result count must be nonnegative")
        if not is_last:
            return None
        with self._lock:
            observer = self._require_observer()
            registered = self._queries.pop(native_request_id, None)
            if (
                native_session_id != self._native_session_id
                or registered is None
                or registered.callback_name != callback_name
            ):
                self._record_callback_gap_reason(
                    self._registration_gap_reason(
                        native_session_id=native_session_id,
                        registered=registered,
                        callback_name=callback_name,
                    )
                )
                gap = self._new_gap(received_monotonic_ns)
                self._callback_gap_count += 1
                event = None
            else:
                event = self._new_query_event(
                    registered,
                    error_id,
                    received_monotonic_ns,
                    page_reference=page_reference,
                    page_result_count=page_result_count,
                )
                self._normalized_terminal_callbacks += 1
                gap = None
        if gap is not None:
            observer.on_callback_gap(gap)
            return None
        assert event is not None
        observer.on_broker_event(event)
        return event

    def snapshot(self) -> EmtReadOnlyIngressSnapshot:
        with self._lock:
            return EmtReadOnlyIngressSnapshot(
                observer_attached=self._observer is not None,
                native_session_bound=self._native_session_id is not None,
                active_query_count=len(self._queries),
                next_native_request_id=0,
                normalized_terminal_callbacks=self._normalized_terminal_callbacks,
                callback_gap_count=self._callback_gap_count,
                callback_gap_reason_counts=tuple(sorted(self._callback_gap_reason_counts.items())),
                authoritative_row_failure_counts=tuple(
                    sorted(self._authoritative_row_failure_counts.items())
                ),
            )

    def _registration_gap_reason(
        self,
        *,
        native_session_id: int,
        registered: _RegisteredQuery | None,
        callback_name: str,
    ) -> str:
        if native_session_id != self._native_session_id:
            return "native_session_mismatch"
        if registered is None:
            return "native_request_unregistered"
        if registered.callback_name != callback_name:
            return "callback_name_mismatch"
        raise AssertionError("matching callback cannot have a registration gap reason")

    def _record_callback_gap_reason(self, reason: str) -> None:
        if reason not in _QUERY_CALLBACK_GAP_REASON_CODES:
            raise AssertionError("EMT query callback gap reason is not approved")
        self._callback_gap_reason_counts[reason] = (
            self._callback_gap_reason_counts.get(reason, 0) + 1
        )

    def _require_request_identity(self, request: BrokerQuery) -> None:
        if (
            request.account_id != self.account_id
            or request.venue_id != "EMT"
            or request.adapter_instance_id != self.adapter_instance_id
        ):
            raise EmtReadOnlyIngressError("EMT query request belongs to another profile")

    def _require_observer(self) -> BrokerEventObserver:
        if self._observer is None:
            raise EmtReadOnlyIngressError("EMT query ingress requires an attached observer")
        return self._observer

    def _new_query_event(
        self,
        registered: _RegisteredQuery,
        error_id: int | None,
        received_monotonic_ns: int,
        *,
        page_reference: int | None,
        page_result_count: int | None,
    ) -> BrokerEvent:
        request_digest = sha256(registered.request.request_id.encode("utf-8")).hexdigest()
        source_key = f"emt-query-terminal:v1:{self._session_epoch}:{request_digest}"
        summary = {
            "callback_name": registered.callback_name,
            "query_kind": registered.request.query_kind,
            "terminal": "true",
            "error_id": str(error_id if error_id is not None else 0),
        }
        if registered.request.query_kind in _PAGE_QUERY_KINDS:
            if page_reference is None or page_result_count is None:
                raise EmtReadOnlyIngressError("EMT page query terminal lacks pagination metadata")
            summary.update(
                {
                    "page_reference": str(page_reference),
                    "page_result_count": str(page_result_count),
                    "page_has_more": str(page_result_count == registered.request.page_size).lower(),
                }
            )
        elif page_reference is not None or page_result_count is not None:
            raise EmtReadOnlyIngressError("non-page EMT query returned pagination metadata")
        else:
            summary["result_count"] = str(registered.result_count)
        return BrokerEvent(
            event_id=source_key,
            schema_version=1,
            source_key_schema_version=1,
            source_key=source_key,
            event_kind=BrokerEventKind.QUERY,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id=self.adapter_instance_id,
            account_id=self.account_id,
            trading_day=self._trading_day,
            received_monotonic_ns=received_monotonic_ns,
            normalized_summary=summary,
        )

    def _new_gap(self, received_monotonic_ns: int) -> CallbackGapEvidence:
        self._sequence += 1
        sequence = self._sequence
        return CallbackGapEvidence(
            evidence_id=f"emt-query-ingress-gap:v1:{self._session_epoch}:{sequence}",
            account_id=self.account_id,
            producer_id=f"emt-query-ingress:v1:{self.adapter_instance_id}",
            first_rejected_sequence=sequence,
            last_rejected_sequence=sequence,
            rejected_count=1,
            observed_monotonic_ns=received_monotonic_ns,
        )


class EmtSdkReadOnlyPort:
    """A no-SDK-import ``EmtReadOnlyPort`` implementation for approved queries.

    The caller supplies a transport that owns the eventual native call.  The
    port allocates a bounded numeric request id, registers it with the ingress,
    invokes one fixed query mapping, and raises on any nonzero return.
    Its recovery surface cannot claim any vendor coverage, so it returns an
    explicit incomplete evidence object instead of throwing or fabricating an
    empty snapshot.  The caller can therefore enter the normal fail-closed
    reconciliation path while this profile remains manual-only.
    """

    def __init__(
        self,
        ingress: EmtQueryCallbackIngress,
        *,
        transport: object,
        first_native_request_id: int = 1,
    ) -> None:
        if not 1 <= first_native_request_id <= _MAX_NATIVE_REQUEST_ID:
            raise EmtReadOnlyIngressError("first EMT native request id is invalid")
        self._ingress = ingress
        self._transport = transport
        self._next_native_request_id = first_native_request_id
        self._recovery_plans: dict[str, RecoveryPlan] = {}
        self._lock = RLock()

    def attach_observer(self, observer: BrokerEventObserver) -> None:
        self._ingress.attach_observer(observer)

    def detach_observer(self, observer: BrokerEventObserver) -> None:
        self._ingress.detach_observer(observer)

    def query(self, request: BrokerQuery) -> QueryHandle:
        with self._lock:
            native_request_id = self._peek_native_request_id()
            callback_name = _QUERY_CALLBACKS.get(request.query_kind)
            if callback_name is None:
                raise EmtReadOnlyIngressError("EMT query kind is not approved")
            self._validate_query_shape(request)
            handle = self._ingress.register_query(
                request,
                native_request_id=native_request_id,
                callback_name=callback_name,
            )
            self._next_native_request_id += 1
            return_code = self._invoke_native_query(request, native_request_id)
            if return_code != 0:
                self._ingress.abandon_query(native_request_id=native_request_id)
                raise EmtReadOnlyIngressError(
                    f"EMT {request.query_kind} query returned nonzero code {return_code}"
                )
            return handle

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        if plan.venue_id != "EMT":
            raise EmtReadOnlyIngressError("EMT recovery plan belongs to another venue")
        with self._lock:
            existing = self._recovery_plans.get(plan.recovery_run_id)
            if existing is not None and existing != plan:
                raise EmtReadOnlyIngressError(
                    "EMT recovery run id is already bound to another plan"
                )
            self._recovery_plans[plan.recovery_run_id] = plan
        return RecoveryHandle(
            recovery_run_id=plan.recovery_run_id,
            adapter_id="emt-gateway-p0",
            adapter_instance_id=plan.adapter_instance_id,
        )

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        with self._lock:
            plan = self._recovery_plans.get(handle.recovery_run_id)
        if plan is None:
            raise EmtReadOnlyIngressError("EMT recovery handle is unknown")
        if (
            handle.adapter_id != "emt-gateway-p0"
            or handle.adapter_instance_id != plan.adapter_instance_id
        ):
            raise EmtReadOnlyIngressError("EMT recovery handle belongs to another adapter")
        # Do not infer completeness from a missing callback or an empty query.
        # This narrow port owns neither RESTART nor the required snapshot family,
        # so every recovery dimension remains deliberately false.
        return BrokerRecoveryEvidence(
            evidence_id=f"emt-incomplete-recovery:{plan.recovery_run_id}",
            recovery_run_id=plan.recovery_run_id,
            account_id=plan.account_id,
            venue_id=plan.venue_id,
            adapter_instance_id=plan.adapter_instance_id,
            trading_day=plan.trading_day,
            callback_replay_complete=False,
            orders_complete=False,
            trades_complete=False,
            working_orders_complete=False,
            risk_snapshot_complete=False,
            negative_confirmation_complete=False,
            callback_gaps=plan.callback_gaps,
        )

    def snapshot(self) -> EmtReadOnlyIngressSnapshot:
        ingress = self._ingress.snapshot()
        with self._lock:
            return EmtReadOnlyIngressSnapshot(
                observer_attached=ingress.observer_attached,
                native_session_bound=ingress.native_session_bound,
                active_query_count=ingress.active_query_count,
                next_native_request_id=self._next_native_request_id,
                normalized_terminal_callbacks=ingress.normalized_terminal_callbacks,
                callback_gap_count=ingress.callback_gap_count,
                callback_gap_reason_counts=ingress.callback_gap_reason_counts,
                authoritative_row_failure_counts=ingress.authoritative_row_failure_counts,
            )

    def _peek_native_request_id(self) -> int:
        if self._next_native_request_id > _MAX_NATIVE_REQUEST_ID:
            raise EmtReadOnlyIngressError("EMT native request id space is exhausted")
        return self._next_native_request_id

    @staticmethod
    def _validate_query_shape(request: BrokerQuery) -> None:
        if request.query_kind in _PAGE_QUERY_KINDS:
            if request.page_size > _MAX_PAGE_SIZE:
                raise EmtReadOnlyIngressError("EMT page query size exceeds the controlled maximum")
        elif request.continuation != 0:
            raise EmtReadOnlyIngressError("non-page EMT query cannot carry a continuation")

    def _invoke_native_query(self, request: BrokerQuery, native_request_id: int) -> int:
        method_name: str
        arguments: tuple[object, ...]
        if request.query_kind == "unfinished_orders":
            method_name, arguments = "query_unfinished_orders", (native_request_id,)
        elif request.query_kind == "orders":
            method_name, arguments = "query_orders", (native_request_id,)
        elif request.query_kind == "trades":
            method_name, arguments = "query_trades", (native_request_id,)
        elif request.query_kind == "orders_by_page":
            method_name, arguments = (
                "query_orders_by_page",
                (
                    request.page_size,
                    request.continuation,
                    native_request_id,
                ),
            )
        elif request.query_kind == "trades_by_page":
            method_name, arguments = (
                "query_trades_by_page",
                (
                    request.page_size,
                    request.continuation,
                    native_request_id,
                ),
            )
        elif request.query_kind == "positions":
            method_name, arguments = "query_positions", (native_request_id,)
        elif request.query_kind == "positions_by_page":
            method_name, arguments = (
                "query_positions_by_page",
                (
                    request.page_size,
                    request.continuation,
                    native_request_id,
                ),
            )
        elif request.query_kind == "assets":
            method_name, arguments = "query_assets", (native_request_id,)
        elif request.query_kind == "funds":
            method_name, arguments = "query_funds", (native_request_id,)
        elif request.query_kind == "credit_fund_info":
            method_name, arguments = "query_credit_fund_info", (native_request_id,)
        elif request.query_kind == "credit_asset_debt":
            method_name, arguments = "query_credit_asset_debt", (native_request_id,)
        elif request.query_kind == "credit_position_extra_info_sz":
            method_name, arguments = "query_credit_position_extra_info", (1, native_request_id)
        elif request.query_kind == "credit_position_extra_info_sh":
            method_name, arguments = "query_credit_position_extra_info", (2, native_request_id)
        else:  # The mapping guard above makes this defensive branch unreachable.
            raise EmtReadOnlyIngressError("EMT query kind is not approved")
        method = getattr(self._transport, method_name, None)
        if not callable(method):
            raise EmtReadOnlyIngressError(f"EMT transport lacks {method_name}")
        result = method(*arguments)
        if type(result) is not int:
            raise EmtReadOnlyIngressError(f"EMT {method_name} did not return an integer code")
        return result
