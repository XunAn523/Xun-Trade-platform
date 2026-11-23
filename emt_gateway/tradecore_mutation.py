"""Platform-owned native mutation port for TradeCore's EMT Adapter.

This outer module is the only adapter extension allowed to touch an already
created vendor API object.  It receives a typed TradeCore request, delegates
vendor request construction to an explicit factory, and returns only numeric
native acceptance ids.  It neither reads credentials nor owns login/recovery.
"""

from __future__ import annotations

from time import monotonic_ns
from typing import Any, Callable, Protocol

from xuanling_tradecore.adapters.emt import (
    EmtBrokerEventIngress,
    EmtBrokerEventIngressError,
    EmtCancelReceipt,
    EmtCancelRequest,
    EmtMutationPort,
    EmtMutationPortError,
    EmtNativeBrokerEvent,
    EmtSubmitReceipt,
    EmtSubmitRequest,
)
from xuanling_tradecore.domain import BrokerEventKind, ErrorCategory, VenueOrderIdentity


class EmtTradeCoreMutationError(EmtMutationPortError):
    """A native EMT mutation return cannot be represented as an acceptance."""


class EmtNativeOrderRequestFactory(Protocol):
    """Encode one already-mapped request into the installed binding's native object."""

    def __call__(self, request: EmtSubmitRequest) -> Any:
        """Return the exact vendor order object expected by ``insertOrder``."""

        ...


class EmtNativeCallJournal(Protocol):
    """Durably fence each EMT native side effect without retaining SDK objects.

    The caller must record ``CALL_STARTED`` before the SDK call and the
    concrete native acceptance afterwards.  A post-call Journal failure is an
    unknown-outcome condition: the port raises instead of returning a false
    success, so the EMS recovery path remains responsible for reconciliation.
    """

    def record_submit_started(self, request: EmtSubmitRequest) -> None:
        """Durably record the exact TradeCore dispatch before ``insertOrder``."""

        ...

    def record_submit_accepted(self, request: EmtSubmitRequest, receipt: EmtSubmitReceipt) -> None:
        """Durably record one native order-id acceptance."""

        ...

    def record_cancel_started(self, request: EmtCancelRequest) -> None:
        """Durably record the exact TradeCore dispatch before ``cancelOrder``."""

        ...

    def record_cancel_accepted(self, request: EmtCancelRequest, receipt: EmtCancelReceipt) -> None:
        """Durably record one native cancellation-id acceptance."""

        ...


class EmtMutationTelemetry(Protocol):
    """Optional payload-free native-boundary timing observer."""

    def on_submit_stage(self, request: EmtSubmitRequest, stage: str, observed_ns: int) -> None: ...

    def on_cancel_stage(self, request: EmtCancelRequest, stage: str, observed_ns: int) -> None: ...


class EmtPlatformMutationPort(EmtMutationPort):
    """Call an already-open EMT API without exposing it to TradeCore.

    The port deliberately has no automatic retry.  A zero native return is a
    definite local rejection only after the binding's immediate error code is
    captured; callers must keep all other exception/outcome handling on the
    conservative EMS recovery path.
    """

    def __init__(
        self,
        api: Any,
        *,
        native_session_id: int,
        order_request_factory: EmtNativeOrderRequestFactory,
        native_call_journal: EmtNativeCallJournal,
        clock: Callable[[], int] = monotonic_ns,
        telemetry: EmtMutationTelemetry | None = None,
    ) -> None:
        if native_session_id <= 0:
            raise EmtTradeCoreMutationError("EMT native session id must be positive")
        self._api = api
        self._native_session_id = native_session_id
        self._order_request_factory = order_request_factory
        self._native_call_journal = native_call_journal
        self._clock = clock
        self._telemetry = telemetry

    def submit(self, request: EmtSubmitRequest) -> EmtSubmitReceipt:
        self._submit_stage(request, "C1A_BEFORE_NATIVE_JOURNAL")
        try:
            self._native_call_journal.record_submit_started(request)
        except BaseException as exc:
            raise EmtTradeCoreMutationError(
                "EMT submit CALL_STARTED Journal gate failed before native dispatch"
            ) from exc
        self._submit_stage(request, "C2_AFTER_PRE_NATIVE_MMAP_COMMIT")
        native_request = self._order_request_factory(request)
        self._submit_stage(request, "C3_BEFORE_SDK")
        try:
            # Credentials are supplied only during the session-login lifecycle.
            # Redirecting process-wide stdout/stderr around every order is not
            # re-entrant: it races distinct InsertOrder callers and adds file
            # descriptor churn directly to C3->C4. The SDK receives a private
            # log directory during session start, so the hot mutation path
            # invokes the documented thread-safe request directly.
            order_emt_id = int(self._api.insertOrder(native_request, self._native_session_id))
        except AttributeError as exc:
            raise EmtTradeCoreMutationError("EMT binding does not expose insertOrder") from exc
        if order_emt_id <= 0:
            raise EmtTradeCoreMutationError(
                f"EMT insertOrder returned a definite local rejection error_id={self._last_error_id()}"
            )
        self._submit_stage(request, "C4_AFTER_SDK")
        receipt = EmtSubmitReceipt(order_emt_id=order_emt_id, accepted_monotonic_ns=self._clock())
        try:
            self._native_call_journal.record_submit_accepted(request, receipt)
        except BaseException as exc:
            raise EmtTradeCoreMutationError(
                "EMT submit was accepted but NATIVE_ACCEPTED Journal record failed; reconcile"
            ) from exc
        self._submit_stage(request, "C4A_AFTER_ACCEPTED_JOURNAL")
        return receipt

    def cancel(self, request: EmtCancelRequest) -> EmtCancelReceipt:
        self._cancel_stage(request, "C1A_BEFORE_NATIVE_JOURNAL")
        try:
            self._native_call_journal.record_cancel_started(request)
        except BaseException as exc:
            raise EmtTradeCoreMutationError(
                "EMT cancel CALL_STARTED Journal gate failed before native dispatch"
            ) from exc
        self._cancel_stage(request, "C2_AFTER_PRE_NATIVE_MMAP_COMMIT")
        self._cancel_stage(request, "C3_BEFORE_SDK")
        try:
            cancel_emt_id = int(
                self._api.cancelOrder(request.order_emt_id, self._native_session_id)
            )
        except AttributeError as exc:
            raise EmtTradeCoreMutationError("EMT binding does not expose cancelOrder") from exc
        if cancel_emt_id <= 0:
            raise EmtTradeCoreMutationError(
                f"EMT cancelOrder returned a definite local rejection error_id={self._last_error_id()}"
            )
        self._cancel_stage(request, "C4_AFTER_SDK")
        receipt = EmtCancelReceipt(
            order_cancel_emt_id=cancel_emt_id,
            accepted_monotonic_ns=self._clock(),
        )
        try:
            self._native_call_journal.record_cancel_accepted(request, receipt)
        except BaseException as exc:
            raise EmtTradeCoreMutationError(
                "EMT cancel was accepted but NATIVE_ACCEPTED Journal record failed; reconcile"
            ) from exc
        self._cancel_stage(request, "C4A_AFTER_ACCEPTED_JOURNAL")
        return receipt

    def _submit_stage(self, request: EmtSubmitRequest, stage: str) -> None:
        if self._telemetry is None:
            return
        try:
            self._telemetry.on_submit_stage(request, stage, self._clock())
        except BaseException:
            return

    def _cancel_stage(self, request: EmtCancelRequest, stage: str) -> None:
        if self._telemetry is None:
            return
        try:
            self._telemetry.on_cancel_stage(request, stage, self._clock())
        except BaseException:
            return

    def _last_error_id(self) -> int:
        try:
            error = self._api.getApiLastError()
        except AttributeError as exc:
            raise EmtTradeCoreMutationError("EMT binding does not expose getApiLastError") from exc
        if not isinstance(error, dict) or type(error.get("error_id")) is not int:
            raise EmtTradeCoreMutationError("EMT getApiLastError returned no integer error_id")
        return error["error_id"]


def build_tradecore_execution_trader_bridge(
    trader_api_type: type[Any],
    *,
    event_ingress: EmtBrokerEventIngress,
    cancel_request_id_for_order: Callable[[int], str | None],
    cancel_target_identity_for_order: Callable[[int], VenueOrderIdentity | None] | None = None,
    known_foreign_cancel_client_order_ids: frozenset[int] = frozenset(),
    known_foreign_cancel_order_id: Callable[[int], bool] | None = None,
    known_foreign_cancel_client_order_id: Callable[[int], bool] | None = None,
    native_callback_observer: Callable[[str], None] | None = None,
) -> type[Any]:
    """Build the vendor subclass that copies only declared order-side scalars.

    Query callbacks remain owned by the separately typed read-only ingress.
    An unknown callback, absent stable trade key, or missing cancel correlation
    is converted to `CallbackGapEvidence`; no vendor mapping escapes the call.
    In a process-isolated same-account topology, an immutable pre-C0 client
    namespace may prove that an order-side callback belongs to another live
    scope. That determination occurs before event-specific field validation,
    so a foreign event cannot become a local callback gap merely because it
    lacks fields mandatory for local normalization. Those callbacks are
    counted but never treated as local events or gaps.
    """

    if any(type(value) is not int or value <= 0 for value in known_foreign_cancel_client_order_ids):
        raise ValueError("foreign cancel client order identifiers must be positive integers")
    if known_foreign_cancel_order_id is not None and not callable(known_foreign_cancel_order_id):
        raise ValueError("foreign cancel order lookup must be callable")
    if known_foreign_cancel_client_order_id is not None and not callable(
        known_foreign_cancel_client_order_id
    ):
        raise ValueError("foreign cancel client-order lookup must be callable")

    class TradeCoreExecutionTraderBridge(trader_api_type):
        def __init__(self) -> None:
            super().__init__()

        def onOrderEvent(self, *args: Any) -> None:
            _observe_native_callback(native_callback_observer, "onOrderEvent")
            _offer_order_side_event(
                event_ingress,
                kind=BrokerEventKind.ORDER,
                args=args,
                cancel_request_id_for_order=cancel_request_id_for_order,
                cancel_target_identity_for_order=cancel_target_identity_for_order,
                known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
                known_foreign_cancel_order_id=known_foreign_cancel_order_id,
                known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
            )

        def onTradeEvent(self, *args: Any) -> None:
            _observe_native_callback(native_callback_observer, "onTradeEvent")
            _offer_order_side_event(
                event_ingress,
                kind=BrokerEventKind.TRADE,
                args=args,
                cancel_request_id_for_order=cancel_request_id_for_order,
                cancel_target_identity_for_order=cancel_target_identity_for_order,
                known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
                known_foreign_cancel_order_id=known_foreign_cancel_order_id,
                known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
            )

        def onCancelOrderError(self, *args: Any) -> None:
            _observe_native_callback(native_callback_observer, "onCancelOrderError")
            _offer_order_side_event(
                event_ingress,
                kind=BrokerEventKind.CANCEL_ERROR,
                args=args,
                cancel_request_id_for_order=cancel_request_id_for_order,
                cancel_target_identity_for_order=cancel_target_identity_for_order,
                known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
                known_foreign_cancel_order_id=known_foreign_cancel_order_id,
                known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
            )

    return TradeCoreExecutionTraderBridge


def _observe_native_callback(observer: Callable[[str], None] | None, callback_name: str) -> None:
    """Emit a payload-free bridge-entry fact when P0 diagnostics request it."""

    if observer is None:
        return
    try:
        observer(callback_name)
    except BaseException:
        # Instrumentation must not turn an SDK callback into a failed callback
        # or alter the normal bridge path.
        return


def _offer_order_side_event(
    ingress: EmtBrokerEventIngress,
    *,
    kind: BrokerEventKind,
    args: tuple[Any, ...],
    cancel_request_id_for_order: Callable[[int], str | None],
    cancel_target_identity_for_order: Callable[[int], VenueOrderIdentity | None] | None,
    known_foreign_cancel_client_order_ids: frozenset[int],
    known_foreign_cancel_order_id: Callable[[int], bool] | None,
    known_foreign_cancel_client_order_id: Callable[[int], bool] | None,
) -> None:
    observed = monotonic_ns()
    try:
        data = next(value for value in args if isinstance(value, dict))
        order_emt_id = _positive_int(data.get("order_emt_id"))
        order_client_id = _positive_int(data.get("order_client_id"))
        if order_client_id is not None and ingress.record_verified_foreign_client_callback(
            order_client_id=order_client_id,
            order_emt_id=order_emt_id,
            cancellation=kind is BrokerEventKind.CANCEL_ERROR,
        ):
            return
        if (
            kind is BrokerEventKind.CANCEL_ERROR
            and order_emt_id is not None
            and ingress.record_verified_foreign_cancel_callback_for_order(order_emt_id)
        ):
            return
        if _is_verified_foreign_client_order_id(
            order_client_id,
            known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
            known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
        ):
            ingress.record_verified_foreign_cancel_callback()
            return
        cancel_emt_id = _positive_int(data.get("order_cancel_emt_id"))
        if kind is BrokerEventKind.TRADE:
            trade_key = _trade_key(data, order_emt_id)
            cancel_request_id = None
        elif kind is BrokerEventKind.CANCEL_ERROR:
            if order_emt_id is None:
                raise EmtTradeCoreMutationError("EMT cancel callback lacks order_emt_id")
            cancel_request_id = cancel_request_id_for_order(order_emt_id)
            if cancel_request_id is None:
                if (
                    known_foreign_cancel_order_id is not None
                    and known_foreign_cancel_order_id(order_emt_id)
                ) or _is_verified_foreign_client_order_id(
                    order_client_id,
                    known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
                    known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
                ):
                    ingress.record_verified_foreign_cancel_callback()
                    return
                raise EmtTradeCoreMutationError("EMT cancel callback has no local request mapping")
            if cancel_target_identity_for_order is None:
                raise EmtTradeCoreMutationError(
                    "EMT cancel callback has no target identity mapping"
                )
            target = cancel_target_identity_for_order(order_emt_id)
            if target is None or target.venue_order_id != str(order_emt_id):
                raise EmtTradeCoreMutationError(
                    "EMT cancel callback target identity does not match"
                )
            target_client_id = _client_order_id(target)
            if order_client_id is not None and order_client_id != target_client_id:
                raise EmtTradeCoreMutationError(
                    "EMT cancel callback client identity does not match"
                )
            order_client_id = target_client_id
            trade_key = None
        else:
            trade_key = None
            cancel_request_id = None
        normalized_summary = _order_summary(kind, data)
        ingress.offer(
            EmtNativeBrokerEvent(
                event_kind=kind,
                account_id=ingress.account_id,
                trading_day=ingress.trading_day,
                received_monotonic_ns=observed,
                order_emt_id=order_emt_id,
                order_client_id=order_client_id,
                order_cancel_emt_id=cancel_emt_id,
                order_status=_nonnegative_int(data.get("order_status")),
                filled_qty=_nonnegative_int(data.get("qty_traded")),
                leaves_qty=_nonnegative_int(data.get("qty_left")),
                trade_key=trade_key,
                cancel_request_id=cancel_request_id,
                error_category=(
                    ErrorCategory.VENUE_REJECTED if kind is BrokerEventKind.CANCEL_ERROR else None
                ),
                normalized_summary=normalized_summary,
            )
        )
    except BaseException as error:
        ingress.force_gap(
            account_id=ingress.account_id,
            observed_monotonic_ns=observed,
            reason=_callback_gap_reason(error),
        )


def _is_verified_foreign_client_order_id(
    order_client_id: int | None,
    *,
    known_foreign_cancel_client_order_ids: frozenset[int],
    known_foreign_cancel_client_order_id: Callable[[int], bool] | None,
) -> bool:
    """Return true only for a positive, presealed foreign client reference."""

    return order_client_id is not None and (
        order_client_id in known_foreign_cancel_client_order_ids
        or (
            known_foreign_cancel_client_order_id is not None
            and known_foreign_cancel_client_order_id(order_client_id)
        )
    )


def _callback_gap_reason(error: BaseException) -> str:
    """Classify a rejected vendor callback without copying payload or text."""

    if isinstance(error, StopIteration):
        return "bridge_callback_mapping_missing"
    if isinstance(error, EmtBrokerEventIngressError):
        return "bridge_event_ingress_validation"
    if isinstance(error, EmtTradeCoreMutationError):
        message = str(error)
        cancel_reasons = {
            "EMT cancel callback lacks order_emt_id": "bridge_cancel_order_id_missing",
            "EMT cancel callback has no local request mapping": "bridge_cancel_route_absent",
            "EMT cancel callback has no target identity mapping": "bridge_cancel_target_identity_unconfigured",
            "EMT cancel callback target identity does not match": "bridge_cancel_target_identity_mismatch",
            "EMT cancel callback client identity does not match": "bridge_cancel_client_identity_mismatch",
            "EMT cancel callback target lacks canonical client identity": "bridge_cancel_client_identity_invalid",
            "EMT cancel callback target client identity is invalid": "bridge_cancel_client_identity_invalid",
        }
        if message in cancel_reasons:
            return cancel_reasons[message]
        if message.startswith("EMT cancel callback"):
            return "bridge_cancel_correlation_unclassified"
        if message.startswith("EMT trade callback"):
            return "bridge_trade_identity"
        return "bridge_mutation_validation"
    if isinstance(error, (KeyError, TypeError, ValueError)):
        return "bridge_callback_scalar_shape"
    return "bridge_unexpected"


def _positive_int(value: object) -> int | None:
    return value if type(value) is int and value > 0 else None


def _nonnegative_int(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _client_order_id(identity: VenueOrderIdentity) -> int:
    value = identity.client_order_ref
    if value is None or not value.isascii() or not value.isdecimal():
        raise EmtTradeCoreMutationError(
            "EMT cancel callback target lacks canonical client identity"
        )
    parsed = int(value)
    if not 1 <= parsed <= 2**32 - 1:
        raise EmtTradeCoreMutationError("EMT cancel callback target client identity is invalid")
    return parsed


def _order_summary(kind: BrokerEventKind, data: dict[object, object]) -> dict[str, str]:
    """Map only documented EMT order states needed by the M1 callback journal.

    EMT reports partial fills by a trade callback in the normal case, but its
    order callback can still carry the documented partial terminal states.
    An unknown vendor status is intentionally left without ``ledger_state``;
    the V2 route observer then fails closed instead of inventing an OMS state.
    """

    if kind is not BrokerEventKind.ORDER:
        return {}
    status = _nonnegative_int(data.get("order_status"))
    if status is None:
        return {}
    state = {
        # EMT sends its documented INIT acknowledgement before an order enters
        # the exchange queue.  It is a live, zero-fill order for M1 purposes.
        0: "LIVE",  # EMT_ORDER_STATUS_INIT
        1: "FILLED",  # EMT_ORDER_STATUS_ALLTRADED
        2: "PART_FILLED",  # EMT_ORDER_STATUS_PARTTRADEDQUEUEING
        3: "CANCELED",  # EMT_ORDER_STATUS_PARTTRADEDNOTQUEUEING
        4: "LIVE",  # EMT_ORDER_STATUS_NOTRADEQUEUEING
        5: "CANCELED",  # EMT_ORDER_STATUS_CANCELED
        6: "REJECTED",  # EMT_ORDER_STATUS_REJECTED
    }.get(status)
    return {"ledger_state": state} if state is not None else {}


def _trade_key(data: dict[object, object], order_emt_id: int | None) -> str:
    report_index = _nonnegative_int(data.get("report_index"))
    if report_index is not None and order_emt_id is not None:
        return f"emt-report:{order_emt_id}:{report_index}"
    execution_id = data.get("exec_id")
    if isinstance(execution_id, str) and execution_id:
        return f"emt-exec:{execution_id}"
    raise EmtTradeCoreMutationError("EMT trade callback lacks stable trade identity")
