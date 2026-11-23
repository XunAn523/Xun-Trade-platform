"""Platform-owned EMT session transport for TradeCore's temporary read-only Adapter.

Only this outer gateway module may pair a loaded vendor binding with the
TradeCore callback ingress.  It retains no query row or error object and does
not expose a submit, cancel, replay, or public-stream operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns
from typing import Any, Callable, Protocol

from xuanling_tradecore.adapters.emt.read_only_ingress import (
    EmtQueryCallbackIngress,
    EmtReadOnlyIngressError,
)
from xuanling_tradecore.adapters.emt.session_adapter import EmtReadOnlySdkSession

from .platform_loader import LoadedSdk, suppress_native_console_output
from .session import SessionSettings, configure_optional_sdk_cpu_affinity


class EmtTradeCoreReadOnlySessionError(RuntimeError):
    """The platform session cannot safely serve the controlled read-only Adapter."""


class EmtLoadedSdkProvider(Protocol):
    """Load an ABI- and hash-verified vendor binding at the platform edge."""

    def load(self) -> LoadedSdk:
        """Return the verified EMT binding or raise before a native session exists."""


@dataclass(frozen=True)
class EmtReadOnlyPlatformSnapshot:
    """Safe factory state; it deliberately excludes account and SDK paths."""

    opening: bool
    session_open: bool


class EmtPlatformReadOnlySessionFactory:
    """Create exactly one minimal EMT read-only session at a time.

    ``sdk_provider`` is normally :class:`EmtPlatformLoader`, which enforces
    CPython 3.10, platform-specific library location, and declared SDK hashes
    before importing the binding.  Credential-bearing ``SessionSettings`` stay
    inside this platform layer and never become part of a TradeCore snapshot.
    """

    def __init__(
        self,
        *,
        sdk_provider: EmtLoadedSdkProvider,
        settings: SessionSettings,
    ) -> None:
        settings.validate()
        self._sdk_provider = sdk_provider
        self._settings = settings
        self._opening = False
        self._session_open = False
        self._lock = Lock()

    def open(self, *, callback_ingress: EmtQueryCallbackIngress) -> EmtReadOnlySdkSession:
        """Open a minimal session only after M2-E-02G has attached its observer."""

        if not callback_ingress.snapshot().observer_attached:
            raise EmtTradeCoreReadOnlySessionError(
                "TradeCore observer must attach before the EMT session opens"
            )
        with self._lock:
            if self._opening or self._session_open:
                raise EmtTradeCoreReadOnlySessionError(
                    "an EMT controlled read-only session is already open"
                )
            self._opening = True
        try:
            loaded_sdk = self._sdk_provider.load()
            session = _EmtNativeReadOnlySession(
                trader_api_type=loaded_sdk.trader_api_type,
                callback_ingress=callback_ingress,
                settings=self._settings,
                on_closed=self._on_session_closed,
            )
            session.start()
        except BaseException as exc:
            with self._lock:
                self._opening = False
            if isinstance(exc, EmtTradeCoreReadOnlySessionError):
                raise
            raise EmtTradeCoreReadOnlySessionError(
                "EMT controlled read-only platform session startup failed"
            ) from exc
        with self._lock:
            self._opening = False
            self._session_open = True
        return session

    def snapshot(self) -> EmtReadOnlyPlatformSnapshot:
        with self._lock:
            return EmtReadOnlyPlatformSnapshot(
                opening=self._opening,
                session_open=self._session_open,
            )

    def _on_session_closed(self) -> None:
        with self._lock:
            self._session_open = False


class _EmtNativeReadOnlySession:
    """Private vendor object wrapper exposing one typed query and terminal callback."""

    def __init__(
        self,
        *,
        trader_api_type: type[Any],
        callback_ingress: EmtQueryCallbackIngress,
        settings: SessionSettings,
        on_closed: Callable[[], None],
    ) -> None:
        self._callback_ingress = callback_ingress
        self._settings = settings
        self._on_closed = on_closed
        self._bridge_type = _build_read_only_trader_bridge(trader_api_type, callback_ingress)
        self._api: Any | None = None
        self._native_session_id: int | None = None
        self._api_version: str | None = None
        self._trading_day: str | None = None
        self._closed = False
        self._lock = Lock()

    @property
    def native_session_id(self) -> int:
        session_id = self._native_session_id
        if session_id is None or self._closed:
            raise EmtTradeCoreReadOnlySessionError("EMT read-only native session is not ready")
        return session_id

    @property
    def api_version(self) -> str:
        if self._api_version is None or self._closed:
            raise EmtTradeCoreReadOnlySessionError("EMT read-only native session is not ready")
        return self._api_version

    @property
    def trading_day(self) -> str:
        if self._trading_day is None or self._closed:
            raise EmtTradeCoreReadOnlySessionError("EMT read-only native session is not ready")
        return self._trading_day

    def start(self) -> None:
        self._settings.sdk_log_dir.mkdir(parents=True, exist_ok=True)
        api = self._bridge_type()
        native_session_id = 0
        try:
            with suppress_native_console_output():
                api.createTraderApi(
                    self._settings.client_id,
                    str(self._settings.sdk_log_dir),
                    self._settings.sdk_log_level,
                )
                api.setSoftwareVersion(self._settings.software_version)
                api.setHeartBeatInterval(self._settings.heartbeat_seconds)
                native_session_id = (
                    _positive_int(
                        api.login(
                            self._settings.host,
                            self._settings.port,
                            self._settings.account,
                            self._settings.password,
                            1,
                            self._settings.local_ip,
                        )
                    )
                    or 0
                )
                if native_session_id > 0:
                    configure_optional_sdk_cpu_affinity(api=api, settings=self._settings)
            if native_session_id == 0:
                raise EmtTradeCoreReadOnlySessionError("EMT read-only login returned no session")
            api_version = str(api.getApiVersion())
            trading_day = str(api.getTradingDay())
            if not api_version or not trading_day:
                raise EmtTradeCoreReadOnlySessionError(
                    "EMT read-only session lacks API version or trading day"
                )
        except BaseException as exc:
            _release_api(api, native_session_id)
            self._closed = True
            self._on_closed()
            if isinstance(exc, EmtTradeCoreReadOnlySessionError):
                raise
            raise EmtTradeCoreReadOnlySessionError("EMT read-only session setup failed") from exc
        self._api = api
        self._native_session_id = native_session_id
        self._api_version = api_version
        self._trading_day = trading_day

    def query_unfinished_orders(self, native_request_id: int) -> int:
        if _positive_int(native_request_id) is None:
            raise EmtTradeCoreReadOnlySessionError("EMT native request id must be positive")
        with self._lock:
            api = self._api
            session_id = self._native_session_id
            if api is None or session_id is None or self._closed:
                raise EmtTradeCoreReadOnlySessionError("EMT read-only native session is not ready")
            with suppress_native_console_output():
                result = api.queryUnfinishedOrders(session_id, native_request_id)
        if type(result) is not int:
            raise EmtTradeCoreReadOnlySessionError(
                "EMT queryUnfinishedOrders did not return an integer code"
            )
        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            api, session_id = self._api, self._native_session_id
            self._api = None
            self._native_session_id = None
            self._api_version = None
            self._trading_day = None
            self._closed = True
        try:
            if api is not None:
                _release_api(api, session_id or 0)
        finally:
            self._on_closed()


def _build_read_only_trader_bridge(
    trader_api_type: type[Any], callback_ingress: EmtQueryCallbackIngress
) -> type[Any]:
    """Build a native subclass that discards every callback payload immediately."""

    class ReadOnlyTraderBridge(trader_api_type):
        def __init__(self) -> None:
            super().__init__()

        def onQueryOrder(self, *args: Any) -> None:
            metadata = _terminal_query_metadata(args)
            if metadata is None:
                return
            native_request_id, native_session_id, error_id, is_last = metadata
            if not is_last:
                return
            try:
                callback_ingress.on_query_order_terminal(
                    native_request_id=native_request_id,
                    native_session_id=native_session_id,
                    error_id=error_id,
                    is_last=True,
                    received_monotonic_ns=monotonic_ns(),
                )
            except EmtReadOnlyIngressError:
                # A callback concurrent with a controlled teardown has no safe
                # observer to receive it.  Never let native callback threads
                # retain or surface the vendor payload while closing.
                return

    return ReadOnlyTraderBridge


def _terminal_query_metadata(
    args: tuple[Any, ...],
) -> tuple[int, int, int | None, bool] | None:
    """Copy only documented terminal scalar metadata from ``onQueryOrder``."""

    if len(args) < 5:
        return None
    error, request_id, is_last, session_id = args[-4:]
    native_request_id = _positive_int(request_id)
    native_session_id = _positive_int(session_id)
    if native_request_id is None or native_session_id is None or type(is_last) is not bool:
        return None
    error_id = None
    if isinstance(error, dict):
        candidate = error.get("error_id")
        if type(candidate) is int and candidate >= 0:
            error_id = candidate
    return native_request_id, native_session_id, error_id, is_last


def _positive_int(value: object) -> int | None:
    return value if type(value) is int and value > 0 else None


def _release_api(api: Any, native_session_id: int) -> None:
    with suppress_native_console_output():
        try:
            if native_session_id > 0:
                api.logout(native_session_id)
        finally:
            api.release()
