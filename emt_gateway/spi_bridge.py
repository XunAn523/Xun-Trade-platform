"""Full TraderSpi-to-P2 callback bridge without retaining native payloads."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Callable, Protocol, cast

from .order_ledger import LedgerCallbackFact, LedgerCallbackKind, OrderState

# Kept in the runtime package so the gateway can register every official
# callback without importing the developer-only performance package.
OFFICIAL_TRADER_SPI = (
    "OnConnected",
    "OnDisconnected",
    "OnError",
    "OnOrderEvent",
    "OnTradeEvent",
    "OnCancelOrderError",
    "OnFundTransfer",
    "OnCreditCashRepay",
    "OnCreditCashRepaySpecifiedDebt",
    "OnCreditExtendDebtDate",
    "OnCreditQuotaTransfer",
    "OnCreditMarginAvlTransfer",
    "OnOptionExerciseOrderEvent",
    "OnOptionCombinedOrderEvent",
    "OnOptionCombinedTradeEvent",
    "OnHoldingChangeEvent",
    "OnQueryOrder",
    "OnQueryOrderByPage",
    "OnQueryTrade",
    "OnQueryTradeByPage",
    "OnQueryPosition",
    "OnQueryPositionByPage",
    "OnQueryAsset",
    "OnQueryFund",
    "OnQueryFundTransfer",
    "OnQueryOtherServerFund",
    "OnQueryETF",
    "OnQueryETFBasket",
    "OnQueryETFByPage",
    "OnQueryIPOInfoList",
    "OnQueryIPOQuotaInfo",
    "OnQueryOptionAuctionInfo",
    "OnQueryEndpointId",
    "OnQueryDMATickerAssignInfoByPage",
    "OnQueryBlackWhiteListByPage",
    "OnQueryCreditCashRepayInfo",
    "OnQueryCreditFundInfo",
    "OnQueryCreditDebtInfo",
    "OnQueryCreditDebtInfoByPage",
    "OnQueryCreditTickerDebtInfo",
    "OnQueryCreditAssetDebtInfo",
    "OnQueryCreditTickerAssignInfo",
    "OnQueryCreditTickerAssignInfoByPage",
    "OnQueryCreditExcessStock",
    "OnQueryMulCreditExcessStock",
    "OnQueryCreditExtendDebtDateOrders",
    "OnQueryCreditFundExtraInfo",
    "OnQueryCreditPositionExtraInfo",
    "OnQueryCreditPledgeStkRate",
    "OnQueryCreditMarginRate",
    "OnQueryCreditPositionFullRate",
    "OnQueryCreditPledgeStkByPage",
    "OnQueryCreditTargetStkByPage",
    "OnQueryCreditDebtToRepayTodayByPage",
    "OnQueryCreditQuotaTransfer",
    "OnQueryCreditMarginAvlTransfer",
    "OnQueryYesterdayAsset",
    "OnQueryIssueInfoList",
    "OnQuerySecurityInfo",
    "OnQuerySecurityByPage",
    "OnQueryOptionCombinedOrders",
    "OnQueryOptionCombinedOrdersByPage",
    "OnQueryOptionCombinedTrades",
    "OnQueryOptionCombinedTradesByPage",
    "OnQueryOptionCombinedPosition",
    "OnQueryOptionCombinedStrategyInfo",
    "OnQueryOptionCombinedExecPosition",
    "OnQueryHKConnectIndicativeExchangeRateInfo",
    "OnQueryHkConnectSpreadTable",
)

_SESSION_CALLBACKS = frozenset({"OnConnected", "OnDisconnected", "OnError"})


class CallbackIngressPort(Protocol):
    def receive(
        self,
        callback_name: str,
        *,
        profile: str,
        workload: str,
        order_emt_id: int | None = None,
        operation_native_id: int | None = None,
        request_id: int | None = None,
        is_last: bool | None = None,
        query_item_present: bool | None = None,
        query_reference: int | None = None,
        ledger_fact: LedgerCallbackFact | None = None,
    ) -> str | None: ...


class DeferredCallbackIngress:
    """Thread-safe SPI ingress that can be armed after session startup.

    EMT may replay public-stream callbacks while login completes.  A mutation
    gateway cannot safely associate those historical events with a command, so
    they are dropped until the durable command pipeline is attached.
    """

    def __init__(self) -> None:
        self._target: CallbackIngressPort | None = None
        self._lock = Lock()

    def attach(self, target: CallbackIngressPort) -> None:
        with self._lock:
            if self._target is not None:
                raise RuntimeError("callback ingress is already attached")
            self._target = target

    def receive(
        self,
        callback_name: str,
        *,
        profile: str,
        workload: str,
        order_emt_id: int | None = None,
        operation_native_id: int | None = None,
        request_id: int | None = None,
        is_last: bool | None = None,
        query_item_present: bool | None = None,
        query_reference: int | None = None,
        ledger_fact: LedgerCallbackFact | None = None,
    ) -> str | None:
        with self._lock:
            target = self._target
        if target is None:
            return None
        metadata: dict[str, object] = {
            "order_emt_id": order_emt_id,
            "operation_native_id": operation_native_id,
            "request_id": request_id,
            "is_last": is_last,
        }
        # Keep the deferred ingress compatible with existing non-query
        # consumers that implement the original minimal callback protocol.
        if query_item_present is not None:
            metadata["query_item_present"] = query_item_present
        if query_reference is not None:
            metadata["query_reference"] = query_reference
        if ledger_fact is not None:
            metadata["ledger_fact"] = ledger_fact
        return cast(Any, target).receive(
            callback_name, profile=profile, workload=workload, **metadata
        )


def python_callback_name(official_name: str) -> str:
    if official_name not in OFFICIAL_TRADER_SPI:
        raise ValueError(f"unknown official TraderSpi callback: {official_name}")
    return official_name[0].lower() + official_name[1:]


def _as_positive_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _as_non_negative_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _query_terminal_flag(value: object) -> bool | None:
    """Normalize EMT's documented terminal flag without truthiness guessing.

    The CPython wrapper has been observed to expose the C++ ``bool`` as either
    a Python :class:`bool` or its exact integer representation ``0``/``1``.
    Accept only those two integer values; values such as ``2``, strings and
    arbitrary truthy objects remain uncorrelated and therefore fail closed.
    """

    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    return None


def _query_terminal_flag_index(args: tuple[Any, ...]) -> tuple[int, bool] | None:
    """Locate a query's terminal scalar without confusing count fields for it."""

    # Native wrappers normally produce a real bool.  Prefer it anywhere in
    # the documented callback signature over an incidental integer count.
    for index, value in enumerate(args):
        if type(value) is bool:
            return index, value
    # CPython's alternate representation is valid only in the documented
    # penultimate ``..., request_id, is_last, session_id`` position.
    if len(args) >= 2:
        normalized = _query_terminal_flag(args[-2])
        if normalized is not None:
            return len(args) - 2, normalized
    return None


def _token(value: object) -> str | None:
    """Copy a tiny scalar identifier, rejecting unbounded native values."""

    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return None


def _price_text(value: object) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        rendered = repr(value)
        return rendered if rendered not in {"nan", "inf", "-inf"} else None
    return None


_ORDER_STATUS_TO_LEDGER_STATE = {
    # EMT_ORDER_STATUS_* values from the supplied Trader API manual.  Partial
    # fills are normally represented by OnTradeEvent, not OnOrderEvent.
    1: OrderState.FILLED,
    2: OrderState.LIVE,
    3: OrderState.CANCELED,
    4: OrderState.LIVE,
    5: OrderState.CANCELED,
    6: OrderState.REJECTED,
}


@dataclass(frozen=True)
class EmtLedgerCallbackScalars:
    """Normalized fixed scalars shared by V1 mapping and V2 post-ring work."""

    order_emt_id: int | None
    client_order_id: str | None
    status: int | None
    cancel_emt_id: int | None
    update_time: str | None
    qty_traded: int | None
    qty_left: int | None
    market: str | None
    quantity: int | None
    price: str | None
    report_index: int | None
    exec_id: str | None


def ledger_callback_fact_from_scalars(
    official_name: str, scalars: EmtLedgerCallbackScalars
) -> LedgerCallbackFact | None:
    """Build the one legacy ledger fact only when its source identity is complete."""

    if official_name not in {"OnOrderEvent", "OnTradeEvent"} or scalars.order_emt_id is None:
        return None
    if official_name == "OnOrderEvent":
        if scalars.status is None:
            return None
        state = _ORDER_STATUS_TO_LEDGER_STATE.get(scalars.status)
        if state is None:
            return None
        # ``qty_left`` has a different vendor meaning for canceled orders
        # (canceled quantity), so local *open* quantity is explicitly zero.
        leaves_qty = (
            0
            if state in {OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED}
            else scalars.qty_left
        )
        source_key = ":".join(
            (
                "order",
                str(scalars.order_emt_id),
                str(scalars.status),
                scalars.update_time or "-",
                "-" if scalars.qty_traded is None else str(scalars.qty_traded),
                "-" if scalars.qty_left is None else str(scalars.qty_left),
                "-" if scalars.cancel_emt_id is None else str(scalars.cancel_emt_id),
            )
        )
        return LedgerCallbackFact(
            kind=LedgerCallbackKind.ORDER_UPDATE,
            source_key=source_key,
            order_emt_id=scalars.order_emt_id,
            client_order_id=scalars.client_order_id,
            state=state,
            leaves_qty=leaves_qty,
            cancel_emt_id=scalars.cancel_emt_id,
        )

    if scalars.market is None or scalars.quantity is None or scalars.price is None:
        return None
    if scalars.report_index is not None:
        execution_key = f"report:{scalars.report_index}"
    elif scalars.exec_id is not None:
        execution_key = f"exec:{scalars.exec_id}"
    else:
        # A repeated callback with no documented execution identity must not
        # be guessed into a new fill.  Reconciliation resolves that gap.
        return None
    return LedgerCallbackFact(
        kind=LedgerCallbackKind.TRADE,
        source_key=f"trade:{scalars.market}:{execution_key}",
        order_emt_id=scalars.order_emt_id,
        client_order_id=scalars.client_order_id,
        market=scalars.market,
        execution_key=execution_key,
        qty=scalars.quantity,
        price=scalars.price,
    )


def _ledger_callback_fact(official_name: str, args: tuple[Any, ...]) -> LedgerCallbackFact | None:
    """Normalize book-critical EMT mappings through the shared scalar contract."""

    if official_name not in {"OnOrderEvent", "OnTradeEvent"}:
        return None
    data = next((value for value in args if isinstance(value, dict)), None)
    if data is None:
        return None
    return ledger_callback_fact_from_scalars(
        official_name,
        EmtLedgerCallbackScalars(
            order_emt_id=_as_positive_int(data.get("order_emt_id")),
            client_order_id=_token(data.get("order_client_id")),
            status=_as_non_negative_int(data.get("order_status")),
            cancel_emt_id=_as_positive_int(data.get("order_cancel_emt_id")),
            update_time=_token(data.get("update_time")) or _token(data.get("cancel_time")),
            qty_traded=_as_non_negative_int(data.get("qty_traded")),
            qty_left=_as_non_negative_int(data.get("qty_left")),
            market=_token(data.get("market")),
            quantity=_as_positive_int(data.get("quantity")),
            price=_price_text(data.get("price")),
            report_index=_as_non_negative_int(data.get("report_index")),
            exec_id=_token(data.get("exec_id")),
        ),
    )


def _minimal_native_ids(args: tuple[Any, ...]) -> tuple[int | None, int | None]:
    """Read only the two P2 correlation ids from vendor callback dictionaries."""

    for candidate in args:
        if not isinstance(candidate, dict):
            continue
        order_id = _as_positive_int(candidate.get("order_emt_id"))
        operation_id = _as_positive_int(candidate.get("order_cancel_emt_id"))
        if operation_id is None:
            operation_id = _as_positive_int(candidate.get("emt_id"))
        # Credit quota transfer notifications identify the operation by their
        # own serial id rather than an EMT order id.  It is still only an
        # opaque positive correlation key; no callback payload is retained.
        if operation_id is None:
            operation_id = _as_positive_int(candidate.get("serial_id"))
        return order_id, operation_id
    return None, None


_PAGED_QUERY_REFERENCE_CALLBACKS = frozenset(
    {
        "OnQueryOrderByPage",
        "OnQueryTradeByPage",
        "OnQueryPositionByPage",
        "OnQueryETFByPage",
        "OnQuerySecurityByPage",
        "OnQueryDMATickerAssignInfoByPage",
        "OnQueryBlackWhiteListByPage",
        "OnQueryCreditTickerAssignInfoByPage",
        "OnQueryCreditPledgeStkByPage",
        "OnQueryCreditTargetStkByPage",
        "OnQueryCreditDebtToRepayTodayByPage",
        "OnQueryOptionCombinedOrdersByPage",
        "OnQueryOptionCombinedTradesByPage",
    }
)

# These documented callbacks are one-shot replies and therefore do not carry
# the streaming ``is_last`` flag.  Every other ``OnQuery*`` callback must have
# an unambiguous terminal scalar; otherwise it stays uncorrelated rather than
# treating a session id or malformed flag as a request id.
_SINGLE_RESPONSE_QUERY_CALLBACKS = frozenset(
    {
        "OnQueryFund",
        "OnQueryCreditFundInfo",
        "OnQueryCreditAssetDebtInfo",
    }
)


def _query_metadata(
    official_name: str,
    args: tuple[Any, ...],
) -> tuple[int | None, bool | None, bool | None, int | None]:
    """Extract correlation and payload-free query-semantic metadata.

    ``query_item_present`` is a Boolean derived immediately from the binding's
    leading data mapping; no callback object or field value is retained.
    ``query_reference`` is copied only for documented cursor callbacks and is
    an opaque non-negative integer used to request the next page.
    """

    if not official_name.startswith("OnQuery"):
        return None, None, None, None
    item_present = bool(args[0]) if args and isinstance(args[0], dict) else None
    terminal = _query_terminal_flag_index(args)
    if terminal is not None:
        index, is_last = terminal
        for candidate in reversed(args[:index]):
            request_id = _as_positive_int(candidate)
            if request_id is not None:
                reference = None
                if official_name in _PAGED_QUERY_REFERENCE_CALLBACKS and index >= 2:
                    candidate_reference = args[index - 2]
                    if isinstance(candidate_reference, int) and candidate_reference >= 0:
                        reference = candidate_reference
                return request_id, is_last, item_present, reference
    # Only the explicitly documented one-shot callbacks conventionally end
    # with ``request_id, session_id``.  Yesterday asset lacks request_id and
    # all malformed streaming callbacks must remain uncorrelated instead of
    # guessing from a session id or invalid terminal scalar.
    if official_name not in _SINGLE_RESPONSE_QUERY_CALLBACKS or len(args) < 2:
        return None, None, item_present, None
    return _as_positive_int(args[-2]), None, item_present, None


def workload_for_callback(official_name: str) -> str:
    """Choose a valid baseline workload; test runs may override it at orchestration."""

    if official_name in _SESSION_CALLBACKS:
        return "reconnect"
    return "steady"


def profile_for_callback(official_name: str, account_profile: str) -> str:
    # Session callbacks do not belong to a trading account workload in the
    # matrix. Credit/option callback names belong to their capability-specific
    # matrix rows even when the vendor emits historical public-stream traffic
    # immediately after an ordinary-account login.
    if official_name in _SESSION_CALLBACKS:
        return "ordinary"
    if "Credit" in official_name:
        return "credit"
    if "Option" in official_name:
        return "option"
    return account_profile


def build_trader_spi_bridge(
    trader_api_base: type[Any],
    *,
    callback_ingress: CallbackIngressPort,
    account_profile: str,
) -> type[Any]:
    """Create the concrete Python subclass required by the vendor binding.

    Every method accepts ``*args`` because callback signatures vary by query
    family and may evolve.  The bridge copies correlation ids and, for the two
    order-book callbacks only, a tiny normalized scalar fact.  It never stores
    raw data/error payloads and delegates immediately to the bounded ingress.
    """

    class P2TraderSpiBridge(trader_api_base):
        def __init__(self) -> None:
            super().__init__()

        def _forward_callback(self, official_name: str, args: tuple[Any, ...]) -> None:
            order_id, operation_id = _minimal_native_ids(args)
            request_id, is_last, query_item_present, query_reference = _query_metadata(
                official_name, args
            )
            ledger_fact = _ledger_callback_fact(official_name, args)
            metadata: dict[str, object] = {
                "order_emt_id": order_id,
                "operation_native_id": operation_id,
                "request_id": request_id,
                "is_last": is_last,
            }
            if official_name.startswith("OnQuery"):
                if query_item_present is not None:
                    metadata["query_item_present"] = query_item_present
                if query_reference is not None:
                    metadata["query_reference"] = query_reference
            if ledger_fact is not None:
                metadata["ledger_fact"] = ledger_fact
            cast(Any, callback_ingress).receive(
                official_name,
                profile=profile_for_callback(official_name, account_profile),
                workload=workload_for_callback(official_name),
                **metadata,
            )

    def _make_handler(official_name: str) -> Callable[..., None]:
        def handler(self: Any, *args: Any) -> None:
            self._forward_callback(official_name, args)

        handler.__name__ = python_callback_name(official_name)
        return handler

    for official_name in OFFICIAL_TRADER_SPI:
        setattr(
            P2TraderSpiBridge, python_callback_name(official_name), _make_handler(official_name)
        )
    return P2TraderSpiBridge
