"""The only gateway boundary allowed to invoke the EMT Python binding.

The actual vendor adapter is deliberately not imported here yet: it needs the
P0 capability report and platform loader before it may touch ``vnemttrader``.
The core only knows this small protocol, enabling deterministic tests and
preventing strategy code from invoking EMT methods directly.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Protocol


class NativeOrderPort(Protocol):
    """Subset used by the P2 order and cancel emit paths."""

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        """Return a non-zero EMT order id, or zero after a local send rejection."""

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        """Return a non-zero EMT cancel id, or zero after a local send rejection."""

    def get_last_error_code(self) -> int:
        """Read the error immediately after a zero native return."""


class NativeAdapterError(RuntimeError):
    """The supplied, already-loaded EMT binding does not meet this narrow contract."""


class EmtOrderAdapter:
    """Translate the P2 emit protocol to an already-initialized EMT API object.

    ``order_request_factory`` is the only place where a vendor order structure
    is constructed.  Its field names and the legal native ``order_client_id``
    range remain capability-gated P0 concerns; this class does not guess them.
    """

    def __init__(
        self,
        api: Any,
        *,
        session_id: int,
        order_request_factory: Callable[[str, str], Any],
    ) -> None:
        if session_id == 0:
            raise NativeAdapterError("an active non-zero EMT session_id is required")
        self._api = api
        self._session_id = session_id
        self._order_request_factory = order_request_factory

    def insert_order(self, *, command_id: str, client_order_id: str) -> int:
        request = self._order_request_factory(command_id, client_order_id)
        try:
            return int(self._api.insertOrder(request, self._session_id))
        except AttributeError as exc:
            raise NativeAdapterError("binding does not expose insertOrder") from exc

    def cancel_order(self, *, command_id: str, order_emt_id: int) -> int:
        del command_id  # Native EMT cancel uses the proved order id and session only.
        try:
            return int(self._api.cancelOrder(order_emt_id, self._session_id))
        except AttributeError as exc:
            raise NativeAdapterError("binding does not expose cancelOrder") from exc

    def get_last_error_code(self) -> int:
        try:
            error = self._api.getApiLastError()
        except AttributeError as exc:
            raise NativeAdapterError("binding does not expose getApiLastError") from exc
        if not isinstance(error, Mapping) or not isinstance(error.get("error_id"), int):
            raise NativeAdapterError("binding getApiLastError returned no integer error_id")
        return int(error["error_id"])
