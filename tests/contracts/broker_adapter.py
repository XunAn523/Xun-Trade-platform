"""P0 test-only BrokerAdapter contract scaffold and deterministic fake port.

The production ``BrokerAdapter`` domain boundary is deliberately deferred to
M0.  This scaffold protects the current NativeOrderPort-compatible seam and
gives later adapter implementations one deterministic callback source for
contract tests without importing, loading, or connecting to the EMT SDK.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from threading import Lock
from typing import Iterable, Mapping, Protocol, Sequence, runtime_checkable


class BrokerCallbackKind(str, Enum):
    ORDER = "ORDER"
    TRADE = "TRADE"
    CANCEL_ERROR = "CANCEL_ERROR"
    QUERY_PAGE = "QUERY_PAGE"


@dataclass(frozen=True)
class NativeSendResult:
    """Deterministic native result; zero represents a local send rejection."""

    native_id: int
    error_code: int = 0

    def __post_init__(self) -> None:
        if self.native_id < 0 or self.error_code < 0:
            raise ValueError("native_id and error_code must be non-negative")
        if self.native_id == 0 and self.error_code == 0:
            raise ValueError("a rejected native send requires a non-zero error_code")
        if self.native_id != 0 and self.error_code != 0:
            raise ValueError("an accepted native send cannot carry an error_code")


@dataclass(frozen=True)
class BrokerCallback:
    """Payload-free callback fact used by adapter contract tests.

    ``source_key`` is stable across replay and is the only identity a future
    consumer-side de-duplicator may use in these fixtures.
    """

    source_key: str
    kind: BrokerCallbackKind
    callback_name: str
    order_emt_id: int | None = None
    cancel_emt_id: int | None = None
    execution_key: str | None = None
    request_id: int | None = None
    page_number: int | None = None
    row_count: int | None = None
    is_last: bool | None = None
    error_code: int | None = None
    replayed: bool = False


@runtime_checkable
class BrokerAdapterContract(Protocol):
    """Minimal P0 command contract shared with the legacy NativeOrderPort."""

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        """Return a positive native order id or zero for a local rejection."""

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        """Return a positive native cancel id or zero for a local rejection."""

    def get_last_error_code(self) -> int:
        """Return the bounded error associated with the latest zero result."""


@runtime_checkable
class BrokerCallbackSourceContract(Protocol):
    """Callback/query fixture surface to be exercised by future adapter tests."""

    def drain_callbacks(self) -> tuple[BrokerCallback, ...]:
        """Return and remove currently pending callback facts in FIFO order."""

    def request_query(self, *, request_id: int, query_name: str) -> int:
        """Queue configured page callbacks and return a native-style status code."""

    def replay_callbacks(self) -> tuple[BrokerCallback, ...]:
        """Re-queue historical callbacks with their original source identities."""


class FakeNativePort:
    """Thread-safe no-I/O fake for P0 contract and later adapter tests."""

    def __init__(
        self,
        *,
        order_results: Iterable[NativeSendResult] = (),
        cancel_results: Iterable[NativeSendResult] = (),
    ) -> None:
        self._order_results = deque(order_results)
        self._cancel_results = deque(cancel_results)
        self._last_error_code = 0
        self._callbacks: deque[BrokerCallback] = deque()
        self._history: list[BrokerCallback] = []
        self._query_pages: dict[str, tuple[tuple[Mapping[str, object], ...], ...]] = {}
        self._callback_sequence = 0
        self._lock = Lock()
        self.order_calls: list[tuple[str, str]] = []
        self.cancel_calls: list[tuple[str, int]] = []

    def queue_order_result(self, result: NativeSendResult) -> None:
        with self._lock:
            self._order_results.append(result)

    def queue_cancel_result(self, result: NativeSendResult) -> None:
        with self._lock:
            self._cancel_results.append(result)

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        if not command_id or not client_order_id:
            raise ValueError("command_id and client_order_id are required")
        with self._lock:
            self.order_calls.append((command_id, client_order_id))
            return self._next_result(self._order_results)

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        if not command_id or order_emt_id <= 0:
            raise ValueError("command_id and a positive order_emt_id are required")
        with self._lock:
            self.cancel_calls.append((command_id, order_emt_id))
            return self._next_result(self._cancel_results)

    def get_last_error_code(self) -> int:
        with self._lock:
            return self._last_error_code

    def emit_order_report(self, *, order_emt_id: int, status: str) -> BrokerCallback:
        if order_emt_id <= 0 or not status:
            raise ValueError("a positive order_emt_id and status are required")
        return self._queue_callback(
            kind=BrokerCallbackKind.ORDER,
            callback_name="OnOrderEvent",
            source_key=f"order:{order_emt_id}:{status}:{self._next_callback_sequence()}",
            order_emt_id=order_emt_id,
        )

    def emit_trade(self, *, order_emt_id: int, market: str, execution_id: str) -> BrokerCallback:
        if order_emt_id <= 0 or not market or not execution_id:
            raise ValueError("order_emt_id, market and execution_id are required")
        return self._queue_callback(
            kind=BrokerCallbackKind.TRADE,
            callback_name="OnTradeEvent",
            source_key=f"trade:{market}:{execution_id}",
            order_emt_id=order_emt_id,
            execution_key=f"{market}:{execution_id}",
        )

    def emit_cancel_error(
        self,
        *,
        order_emt_id: int,
        cancel_emt_id: int,
        error_code: int,
    ) -> BrokerCallback:
        if order_emt_id <= 0 or cancel_emt_id <= 0 or error_code <= 0:
            raise ValueError("order, cancel and error ids must be positive")
        return self._queue_callback(
            kind=BrokerCallbackKind.CANCEL_ERROR,
            callback_name="OnCancelOrderError",
            source_key=f"cancel-error:{order_emt_id}:{cancel_emt_id}:{error_code}",
            order_emt_id=order_emt_id,
            cancel_emt_id=cancel_emt_id,
            error_code=error_code,
        )

    def configure_query_pages(
        self,
        *,
        query_name: str,
        pages: Sequence[Sequence[Mapping[str, object]]],
    ) -> None:
        if not query_name.startswith("Query"):
            raise ValueError("query_name must start with Query")
        if not pages:
            raise ValueError("at least one query page is required")
        with self._lock:
            self._query_pages[query_name] = tuple(tuple(row for row in page) for page in pages)

    def request_query(self, *, request_id: int, query_name: str) -> int:
        if request_id <= 0 or not query_name.startswith("Query"):
            raise ValueError("a positive request_id and Query* name are required")
        with self._lock:
            try:
                pages = self._query_pages[query_name]
            except KeyError as exc:
                raise ValueError(f"no query fixture configured for {query_name}") from exc
            for page_number, page in enumerate(pages, start=1):
                self._queue_callback_locked(
                    BrokerCallback(
                        source_key=f"query:{query_name}:{request_id}:{page_number}",
                        kind=BrokerCallbackKind.QUERY_PAGE,
                        callback_name=f"On{query_name}",
                        request_id=request_id,
                        page_number=page_number,
                        row_count=len(page),
                        is_last=page_number == len(pages),
                    )
                )
            return 0

    def drain_callbacks(self) -> tuple[BrokerCallback, ...]:
        with self._lock:
            callbacks = tuple(self._callbacks)
            self._callbacks.clear()
            return callbacks

    def replay_callbacks(self) -> tuple[BrokerCallback, ...]:
        with self._lock:
            replayed = tuple(replace(callback, replayed=True) for callback in self._history)
            self._callbacks.extend(replayed)
            return replayed

    def _next_result(self, results: deque[NativeSendResult]) -> int:
        result = results.popleft() if results else NativeSendResult(native_id=1)
        self._last_error_code = result.error_code
        return result.native_id

    def _next_callback_sequence(self) -> int:
        with self._lock:
            self._callback_sequence += 1
            return self._callback_sequence

    def _queue_callback(self, **fields: object) -> BrokerCallback:
        callback = BrokerCallback(**fields)  # type: ignore[arg-type]  # contract fixture builds dynamic callback fields
        with self._lock:
            self._queue_callback_locked(callback)
        return callback

    def _queue_callback_locked(self, callback: BrokerCallback) -> None:
        self._callbacks.append(callback)
        self._history.append(callback)
