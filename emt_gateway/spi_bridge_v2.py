"""C5-only TraderSpi bridge for the native ``fastspi v2`` ingress.

This bridge is deliberately separate from the P0/V1 bridge.  It extracts a
fixed set of scalar fields from the Python binding callback and immediately
calls the native ingress.  It does not construct domain records, UUIDs,
traces, metrics, ledger facts or lifecycle-observer payloads; those belong to
the post-ring worker.
"""

from __future__ import annotations

from time import perf_counter_ns
from typing import Any, Callable, Protocol, cast

from .spi_bridge import OFFICIAL_TRADER_SPI, _query_terminal_flag_index, python_callback_name

FASTSPI_V2_FLAG_QUERY = 1 << 0
FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT = 1 << 1
FASTSPI_V2_FLAG_QUERY_LAST = 1 << 2
FASTSPI_V2_FLAG_ERROR_BYTES_PRESENT = 1 << 3
# The native ingress sets this after UTF-8-boundary truncation.  C5 cannot
# derive encoded byte length without creating another Python object.
FASTSPI_V2_FLAG_ERROR_TRUNCATED = 1 << 4
FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT = 1 << 5
FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT = 1 << 6
FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT = 1 << 7
FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT = 1 << 8
FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT = 1 << 9
FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT = 1 << 10
FASTSPI_V2_FLAG_TRADE_EXECUTION_ID_PRESENT = 1 << 11
FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT = 1 << 12

FastSpiV2FixedScalars = tuple[
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    bytes | str,
    int,
    int,
    int,
    int,
    float | int,
    int,
    bytes | str,
    int,
]


class FastSpiV2NativeIngressPort(Protocol):
    """The no-domain-object C5 seam supplied by ``FastSpiV2NativeIngress``."""

    def offer(
        self,
        *,
        callback_kind: int,
        observed_monotonic_ns: int,
        client_id: int = 0,
        order_client_id: int = 0,
        order_emt_id: int = 0,
        order_cancel_emt_id: int = 0,
        status: int = 0,
        quantity: int = 0,
        trade_id_hash: int = 0,
        flags: int = 0,
        error_bytes: bytes | str = b"",
        order_update_time: int = 0,
        qty_traded: int = 0,
        qty_left: int = 0,
        trade_market: int = 0,
        trade_price: float | int = 0.0,
        trade_report_index: int = 0,
        trade_execution_id: bytes | str = b"",
        query_reference: int = 0,
    ) -> bool: ...

    def force_gap(self, *, callback_kind: int, observed_monotonic_ns: int) -> None: ...


class FastSpiV2CallbackKindRegistrarPort(Protocol):
    """The configuration-time half of the native gap-latch contract."""

    def register_callback_kind(self, *, callback_kind: int, label: str) -> None: ...


def register_official_trader_spi_v2_kinds(
    gap_latch: FastSpiV2CallbackKindRegistrarPort,
) -> None:
    """Register the immutable official callback code table before session start."""

    for callback_kind, official_name in enumerate(OFFICIAL_TRADER_SPI, start=1):
        gap_latch.register_callback_kind(callback_kind=callback_kind, label=official_name)


def _unsigned_scalar(value: object, maximum: int) -> int:
    if type(value) is int and 0 <= value <= maximum:
        return value
    return 0


def _signed_scalar(value: object, minimum: int, maximum: int) -> int:
    if type(value) is int and minimum <= value <= maximum:
        return value
    return 0


def _unsigned_scalar_with_presence(value: object, maximum: int) -> tuple[int, bool]:
    if type(value) is int and 0 <= value <= maximum:
        return value, True
    return 0, False


def _signed_scalar_with_presence(value: object, minimum: int, maximum: int) -> tuple[int, bool]:
    if type(value) is int and minimum <= value <= maximum:
        return value, True
    return 0, False


def _first_callback_mapping(args: tuple[Any, ...]) -> dict[str, Any] | None:
    """Borrow the first binding mapping without retaining it beyond C5."""

    for value in args:
        if isinstance(value, dict):
            return value
    return None


def _error_mapping(args: tuple[Any, ...]) -> dict[str, Any] | None:
    """Borrow the documented error mapping without retaining either mapping.

    EMT order, cancel and query callbacks pass their business data and error
    values as separate binding dictionaries.  The V2 C5 bridge must therefore
    copy the error scalars from the second mapping rather than assuming they
    were folded into the business payload by a test double.
    """

    for value in args:
        if not isinstance(value, dict):
            continue
        if "error_id" in value or "error_msg" in value or "error_message" in value:
            return value
    return None


def _query_is_last(args: tuple[Any, ...]) -> bool:
    """Copy only the documented terminal Boolean from a query callback."""

    terminal = _query_terminal_flag_index(args)
    return terminal[1] if terminal is not None else False


def _query_request_id(args: tuple[Any, ...]) -> int:
    """Copy the documented request id immediately preceding a query terminal flag."""

    terminal = _query_terminal_flag_index(args)
    if terminal is not None:
        index, _ = terminal
        for candidate in reversed(args[:index]):
            request_id = _unsigned_scalar(candidate, (1 << 64) - 1)
            if request_id > 0:
                return request_id
        return 0
    return 0


def _query_page_reference(args: tuple[Any, ...], *, is_paged: bool) -> tuple[int, bool]:
    """Copy the documented page cursor only for official paged callbacks."""

    if not is_paged:
        return 0, False
    terminal = _query_terminal_flag_index(args)
    if terminal is not None:
        index, _ = terminal
        if index >= 2:
            return _unsigned_scalar_with_presence(args[index - 2], (1 << 64) - 1)
    return 0, False


def _fixed_scalars(
    args: tuple[Any, ...],
    *,
    is_query: bool,
    is_paged_query: bool,
) -> FastSpiV2FixedScalars:
    """Return only the fixed POD fields accepted by the native slot.

    The supplied binding exposes callback structures as Python mappings.  The
    mapping itself is borrowed, never queued.  Unknown, malformed and absent
    values become zero so that the worker handles them conservatively rather
    than letting an exception cross a vendor callback boundary.
    """

    data = _first_callback_mapping(args)
    error = _error_mapping(args)
    flags = FASTSPI_V2_FLAG_QUERY if is_query else 0
    if data is None:
        return 0, 0, 0, 0, 0, 0, 0, flags, b"", 0, 0, 0, 0, 0.0, 0, b"", 0

    client_id = _unsigned_scalar(data.get("client_id"), (1 << 64) - 1)
    if is_query:
        client_id = _query_request_id(args) or client_id
    order_client_id = _unsigned_scalar(data.get("order_client_id"), (1 << 64) - 1)
    order_emt_id = _unsigned_scalar(data.get("order_emt_id"), (1 << 64) - 1)
    order_cancel_emt_id = _unsigned_scalar(data.get("order_cancel_emt_id"), (1 << 64) - 1)
    status_value = data.get("order_status")
    if type(status_value) is not int and error is not None:
        status_value = error.get("error_id", 0)
    status = _signed_scalar(status_value, -(1 << 31), (1 << 31) - 1)
    quantity = _signed_scalar(
        data.get("quantity", data.get("qty_traded", data.get("qty_left", 0))),
        -(1 << 63),
        (1 << 63) - 1,
    )
    trade_id_hash, trade_report_index_present = _unsigned_scalar_with_presence(
        data.get("report_index"), (1 << 64) - 1
    )
    if is_query and bool(data):
        flags |= FASTSPI_V2_FLAG_QUERY_ITEM_PRESENT
    if is_query and _query_is_last(args):
        flags |= FASTSPI_V2_FLAG_QUERY_LAST
    error_value = error.get("error_msg") if error is not None else None
    if error_value is None and error is not None:
        error_value = error.get("error_message")
    if isinstance(error_value, (bytes, str)):
        flags |= FASTSPI_V2_FLAG_ERROR_BYTES_PRESENT
        error_bytes = error_value
    else:
        error_bytes = b""
    update_time_value = data.get("update_time")
    if update_time_value is None:
        update_time_value = data.get("cancel_time")
    order_update_time, order_update_time_present = _unsigned_scalar_with_presence(
        update_time_value, (1 << 64) - 1
    )
    qty_traded, qty_traded_present = _unsigned_scalar_with_presence(
        data.get("qty_traded"), (1 << 64) - 1
    )
    qty_left, qty_left_present = _unsigned_scalar_with_presence(data.get("qty_left"), (1 << 64) - 1)
    trade_market, trade_market_present = _signed_scalar_with_presence(
        data.get("market"), -(1 << 31), (1 << 31) - 1
    )
    price_value = data.get("price")
    trade_price: float | int
    if type(price_value) in {float, int}:
        trade_price = cast(float | int, price_value)
        trade_price_present = True
    else:
        trade_price = 0.0
        trade_price_present = False
    trade_execution_id: bytes | str
    trade_execution_value = data.get("exec_id")
    if not isinstance(trade_execution_value, (bytes, str)) or not trade_execution_value:
        trade_execution_id = b""
        trade_execution_id_present = False
    else:
        trade_execution_id = trade_execution_value
        trade_execution_id_present = True
    query_reference, query_reference_present = _query_page_reference(args, is_paged=is_paged_query)
    if order_update_time_present:
        flags |= FASTSPI_V2_FLAG_ORDER_UPDATE_TIME_PRESENT
    if qty_traded_present:
        flags |= FASTSPI_V2_FLAG_ORDER_QTY_TRADED_PRESENT
    if qty_left_present:
        flags |= FASTSPI_V2_FLAG_ORDER_QTY_LEFT_PRESENT
    if trade_market_present:
        flags |= FASTSPI_V2_FLAG_TRADE_MARKET_PRESENT
    if trade_price_present:
        flags |= FASTSPI_V2_FLAG_TRADE_PRICE_PRESENT
    if trade_report_index_present:
        flags |= FASTSPI_V2_FLAG_TRADE_REPORT_INDEX_PRESENT
    if trade_execution_id_present:
        flags |= FASTSPI_V2_FLAG_TRADE_EXECUTION_ID_PRESENT
    if query_reference_present:
        flags |= FASTSPI_V2_FLAG_QUERY_REFERENCE_PRESENT
    return (
        client_id,
        order_client_id,
        order_emt_id,
        order_cancel_emt_id,
        status,
        quantity,
        trade_id_hash,
        flags,
        error_bytes,
        order_update_time,
        qty_traded,
        qty_left,
        trade_market,
        trade_price,
        trade_id_hash,
        trade_execution_id,
        query_reference,
    )


def build_trader_spi_v2_bridge(
    trader_api_base: type[Any],
    *,
    callback_ingress: FastSpiV2NativeIngressPort,
) -> type[Any]:
    """Create the official Python binding subclass wired directly to C5.

    Callback-kind codes are assigned once, in the immutable official callback
    order.  A runtime profile must pre-register this exact table in its native
    gap latch before it creates an SDK session.
    """

    class P2TraderSpiV2Bridge(trader_api_base):
        def __init__(self) -> None:
            super().__init__()

        def _forward_callback(
            self,
            callback_kind: int,
            official_name: str,
            args: tuple[Any, ...],
        ) -> None:
            observed_monotonic_ns = perf_counter_ns()
            try:
                (
                    client_id,
                    order_client_id,
                    order_emt_id,
                    order_cancel_emt_id,
                    status,
                    quantity,
                    trade_id_hash,
                    flags,
                    error_bytes,
                    order_update_time,
                    qty_traded,
                    qty_left,
                    trade_market,
                    trade_price,
                    trade_report_index,
                    trade_execution_id,
                    query_reference,
                ) = _fixed_scalars(
                    args,
                    is_query=official_name.startswith("OnQuery"),
                    is_paged_query=official_name.endswith("ByPage"),
                )
                callback_ingress.offer(
                    callback_kind=callback_kind,
                    observed_monotonic_ns=observed_monotonic_ns,
                    client_id=client_id,
                    order_client_id=order_client_id,
                    order_emt_id=order_emt_id,
                    order_cancel_emt_id=order_cancel_emt_id,
                    status=status,
                    quantity=quantity,
                    trade_id_hash=trade_id_hash,
                    flags=flags,
                    error_bytes=error_bytes,
                    order_update_time=order_update_time,
                    qty_traded=qty_traded,
                    qty_left=qty_left,
                    trade_market=trade_market,
                    trade_price=trade_price,
                    trade_report_index=trade_report_index,
                    trade_execution_id=trade_execution_id,
                    query_reference=query_reference,
                )
            except Exception:
                # Vendor callbacks must never leak a Python exception.  A
                # malformed binding value is a conservative callback gap, not
                # a reason to continue accepting new work.
                try:
                    callback_ingress.force_gap(
                        callback_kind=callback_kind,
                        observed_monotonic_ns=observed_monotonic_ns,
                    )
                except Exception:
                    return

    def _make_handler(callback_kind: int, official_name: str) -> Callable[..., None]:
        def handler(self: Any, *args: Any) -> None:
            self._forward_callback(callback_kind, official_name, args)

        handler.__name__ = python_callback_name(official_name)
        return handler

    for callback_kind, official_name in enumerate(OFFICIAL_TRADER_SPI, start=1):
        setattr(
            P2TraderSpiV2Bridge,
            python_callback_name(official_name),
            _make_handler(callback_kind, official_name),
        )
    return P2TraderSpiV2Bridge
