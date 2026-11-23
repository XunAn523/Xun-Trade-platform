"""Platform factory for TradeCore's controlled EMT execution Adapter.

Credentials and vendor objects remain in this outer platform module.  The
TradeCore package receives only typed session metadata, a mutation port, and
normalized callback ingress; no native mapping leaves this boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns
from typing import Any, Callable, Protocol

from xuanling_tradecore.adapters.emt import (
    EmtAuthoritativeOrderV1,
    EmtBrokerEventIngress,
    EmtExecutionSdkSession,
    EmtQueryCallbackIngress,
)
from xuanling_tradecore.domain import VenueOrderIdentity

from .platform_loader import LoadedSdk, suppress_native_console_output
from .session import (
    SessionError,
    SessionSettings,
    configure_optional_sdk_cpu_affinity,
    normalize_emt_trading_day,
)
from .tradecore_mutation import (
    EmtMutationTelemetry,
    EmtNativeCallJournal,
    EmtNativeOrderRequestFactory,
    EmtPlatformMutationPort,
    build_tradecore_execution_trader_bridge,
)


class EmtTradeCoreExecutionSessionError(RuntimeError):
    """A platform execution session cannot safely serve the TradeCore Adapter."""


class EmtLoadedSdkProvider(Protocol):
    """Structural protocol documented by `EmtPlatformLoader` without importing it here."""

    def load(self) -> LoadedSdk:
        """Return the ABI- and hash-verified vendor binding."""

        ...


class EmtExecutionLifecycleObserver(Protocol):
    """Receives payload-free execution-session lifecycle stage names."""

    def __call__(self, stage: str) -> None:
        """Record a safe lifecycle stage without endpoint or credential data."""

        ...


@dataclass(frozen=True)
class EmtExecutionPlatformSnapshot:
    """Safe factory diagnostics excluding credentials, endpoints, and native objects."""

    opening: bool
    session_open: bool


class EmtPlatformExecutionSessionFactory:
    """Create one authenticated execution session after both ingresses are attached."""

    def __init__(
        self,
        *,
        sdk_provider: EmtLoadedSdkProvider,
        settings: SessionSettings,
        order_request_factory: EmtNativeOrderRequestFactory,
        native_call_journal: EmtNativeCallJournal,
        cancel_request_id_for_order: Callable[[int], str | None],
        cancel_target_identity_for_order: Callable[[int], VenueOrderIdentity | None] | None = None,
        known_foreign_cancel_client_order_ids: frozenset[int] = frozenset(),
        known_foreign_cancel_order_id: Callable[[int], bool] | None = None,
        known_foreign_cancel_client_order_id: Callable[[int], bool] | None = None,
        mutation_telemetry: EmtMutationTelemetry | None = None,
        lifecycle_observer: EmtExecutionLifecycleObserver | None = None,
        native_reference_retainer: Callable[[object], None] | None = None,
        native_callback_observer: Callable[[str], None] | None = None,
    ) -> None:
        settings.validate()
        self._sdk_provider = sdk_provider
        self._settings: SessionSettings | None = settings
        self._order_request_factory = order_request_factory
        self._native_call_journal = native_call_journal
        self._cancel_request_id_for_order = cancel_request_id_for_order
        self._cancel_target_identity_for_order = cancel_target_identity_for_order
        self._known_foreign_cancel_client_order_ids = known_foreign_cancel_client_order_ids
        self._known_foreign_cancel_order_id = known_foreign_cancel_order_id
        self._known_foreign_cancel_client_order_id = known_foreign_cancel_client_order_id
        self._mutation_telemetry = mutation_telemetry
        self._lifecycle_observer = lifecycle_observer
        self._native_reference_retainer = native_reference_retainer
        self._native_callback_observer = native_callback_observer
        self._opening = False
        self._session_open = False
        self._lock = Lock()

    def open(
        self,
        *,
        query_callback_ingress: EmtQueryCallbackIngress,
        broker_event_ingress: EmtBrokerEventIngress,
    ) -> EmtExecutionSdkSession:
        if not query_callback_ingress.snapshot().observer_attached:
            raise EmtTradeCoreExecutionSessionError(
                "TradeCore query observer must attach before the EMT session opens"
            )
        if not broker_event_ingress.observer_attached:
            raise EmtTradeCoreExecutionSessionError(
                "TradeCore broker-event observer must attach before the EMT session opens"
            )
        with self._lock:
            if self._opening or self._session_open:
                raise EmtTradeCoreExecutionSessionError("an EMT execution session is already open")
            settings = self._settings
            if settings is None:
                raise EmtTradeCoreExecutionSessionError(
                    "an EMT execution factory cannot reuse consumed login material"
                )
            self._opening = True
        session: _EmtNativeExecutionSession | None = None
        try:
            self._mark("sdk_loading")
            loaded_sdk = self._sdk_provider.load()
            self._mark("sdk_loaded")
            session = _EmtNativeExecutionSession(
                trader_api_type=loaded_sdk.trader_api_type,
                query_callback_ingress=query_callback_ingress,
                broker_event_ingress=broker_event_ingress,
                settings=settings,
                order_request_factory=self._order_request_factory,
                native_call_journal=self._native_call_journal,
                cancel_request_id_for_order=self._cancel_request_id_for_order,
                cancel_target_identity_for_order=self._cancel_target_identity_for_order,
                known_foreign_cancel_client_order_ids=self._known_foreign_cancel_client_order_ids,
                known_foreign_cancel_order_id=self._known_foreign_cancel_order_id,
                known_foreign_cancel_client_order_id=self._known_foreign_cancel_client_order_id,
                mutation_telemetry=self._mutation_telemetry,
                lifecycle_observer=self._lifecycle_observer,
                native_callback_observer=self._native_callback_observer,
                on_closed=self._on_session_closed,
            )
            self._mark("native_session_starting")
            session.start()
            self._mark("native_session_started")
        except BaseException as exc:
            with self._lock:
                self._opening = False
                self._settings = None
            if session is not None and self._native_reference_retainer is not None:
                # The SDK wrapper may block in finalization after an internal
                # startup failure.  Only a disposable acceptance process
                # supplies this hook and exits with os._exit after it has
                # emitted its bounded report; services retain nothing here.
                self._mark("native_failed_session_retain_starting")
                self._native_reference_retainer(session)
                self._mark("native_failed_session_retained")
            if isinstance(exc, EmtTradeCoreExecutionSessionError):
                raise
            raise EmtTradeCoreExecutionSessionError(
                "EMT execution platform session startup failed"
            ) from exc
        session.discard_login_settings()
        with self._lock:
            self._opening = False
            self._session_open = True
            # The execution session does not support automatic relogin.  Once
            # native Login and post-login setup have succeeded, neither the
            # factory nor the session requires its endpoint/account/password
            # object any longer.  A later session needs a new factory and a
            # fresh, purpose-bound secret lease.
            self._settings = None
        return session

    def snapshot(self) -> EmtExecutionPlatformSnapshot:
        with self._lock:
            return EmtExecutionPlatformSnapshot(
                opening=self._opening,
                session_open=self._session_open,
            )

    def _on_session_closed(self) -> None:
        with self._lock:
            self._session_open = False

    def _mark(self, stage: str) -> None:
        if self._lifecycle_observer is not None:
            self._lifecycle_observer(stage)


class _EmtNativeExecutionSession:
    def __init__(
        self,
        *,
        trader_api_type: type[Any],
        query_callback_ingress: EmtQueryCallbackIngress,
        broker_event_ingress: EmtBrokerEventIngress,
        settings: SessionSettings,
        order_request_factory: EmtNativeOrderRequestFactory,
        native_call_journal: EmtNativeCallJournal,
        cancel_request_id_for_order: Callable[[int], str | None],
        cancel_target_identity_for_order: Callable[[int], VenueOrderIdentity | None] | None,
        known_foreign_cancel_client_order_ids: frozenset[int],
        known_foreign_cancel_order_id: Callable[[int], bool] | None,
        known_foreign_cancel_client_order_id: Callable[[int], bool] | None,
        mutation_telemetry: EmtMutationTelemetry | None,
        lifecycle_observer: EmtExecutionLifecycleObserver | None,
        native_callback_observer: Callable[[str], None] | None,
        on_closed: Callable[[], None],
    ) -> None:
        self._query_callback_ingress = query_callback_ingress
        self._broker_event_ingress = broker_event_ingress
        self._settings: SessionSettings | None = settings
        self._order_request_factory = order_request_factory
        self._native_call_journal = native_call_journal
        self._mutation_telemetry = mutation_telemetry
        self._lifecycle_observer = lifecycle_observer
        self._on_closed = on_closed
        self._bridge_type = _build_execution_trader_bridge(
            trader_api_type,
            event_ingress=broker_event_ingress,
            query_ingress=query_callback_ingress,
            cancel_request_id_for_order=cancel_request_id_for_order,
            cancel_target_identity_for_order=cancel_target_identity_for_order,
            known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
            known_foreign_cancel_order_id=known_foreign_cancel_order_id,
            known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
            native_callback_observer=native_callback_observer,
            query_callback_observer=lambda name, arity, terminal_shape: self._mark(
                f"native_query_callback:{name}:arity={arity}:{terminal_shape}"
            ),
        )
        self._api: Any | None = None
        self._native_session_id: int | None = None
        self._api_version: str | None = None
        self._trading_day: str | None = None
        self._mutation_port: EmtPlatformMutationPort | None = None
        self._closed = False
        self._lock = Lock()

    @property
    def native_session_id(self) -> int:
        return self._require_ready()[1]

    @property
    def api_version(self) -> str:
        self._require_ready()
        assert self._api_version is not None
        return self._api_version

    @property
    def trading_day(self) -> str:
        self._require_ready()
        assert self._trading_day is not None
        return self._trading_day

    @property
    def mutation_port(self) -> EmtPlatformMutationPort:
        self._require_ready()
        if self._mutation_port is None:
            raise EmtTradeCoreExecutionSessionError("EMT execution mutation port is not ready")
        return self._mutation_port

    def start(self) -> None:
        settings = self._settings
        if settings is None:
            raise EmtTradeCoreExecutionSessionError("EMT execution login material is unavailable")
        settings.sdk_log_dir.mkdir(parents=True, exist_ok=True)
        api = self._bridge_type()
        native_session_id = 0
        try:
            with suppress_native_console_output():
                self._mark("native_api_creating")
                api.createTraderApi(
                    settings.client_id,
                    str(settings.sdk_log_dir),
                    settings.sdk_log_level,
                )
                self._mark("native_api_created")
                api.setSoftwareVersion(settings.software_version)
                self._mark("native_software_version_set")
                api.setHeartBeatInterval(settings.heartbeat_seconds)
                self._mark("native_heartbeat_set")
                # The native EMT API requires public-topic subscription before login
                # for order/cancel callbacks to reach the controlled adapter.  Keep
                # this in the execution lifecycle as the read-only session does.
                api.subscribePublicTopic(settings.public_topic_resume_type)
                self._mark("native_public_topic_subscribed")
                self._mark("native_login_starting")
                native_session_id = int(
                    api.login(
                        settings.host,
                        settings.port,
                        settings.account,
                        settings.password,
                        1,
                        settings.local_ip,
                    )
                )
                self._mark("native_login_returned")
                if native_session_id > 0 and configure_optional_sdk_cpu_affinity(api=api, settings=settings):
                    self._mark("native_cpu_affinity_set")
            if native_session_id <= 0:
                raise EmtTradeCoreExecutionSessionError("EMT execution login returned no session")
            api_version, raw_trading_day = str(api.getApiVersion()), str(api.getTradingDay())
            self._mark("native_metadata_read")
            if not api_version or not raw_trading_day:
                raise EmtTradeCoreExecutionSessionError(
                    "EMT execution session lacks API version or trading day"
                )
            try:
                trading_day = normalize_emt_trading_day(raw_trading_day)
            except SessionError as exc:
                raise EmtTradeCoreExecutionSessionError(
                    "EMT execution session returned an invalid trading day"
                ) from exc
        except BaseException as exc:
            try:
                _release_api(
                    api,
                    native_session_id,
                    lifecycle_observer=self._lifecycle_observer,
                )
            finally:
                # Retain the released native wrapper on this closed session
                # until an optional isolated-worker retainer takes ownership.
                # `close()` is idempotent once `_closed` is set, so this never
                # exposes a usable or double-released native session.
                self._api = api
                self._native_session_id = native_session_id if native_session_id > 0 else None
                self._settings = None
                self._closed = True
                self._on_closed()
            if isinstance(exc, EmtTradeCoreExecutionSessionError):
                raise
            raise EmtTradeCoreExecutionSessionError("EMT execution session setup failed") from exc
        self._api = api
        self._native_session_id = native_session_id
        self._api_version = api_version
        self._trading_day = trading_day
        self._mutation_port = EmtPlatformMutationPort(
            api,
            native_session_id=native_session_id,
            order_request_factory=self._order_request_factory,
            native_call_journal=self._native_call_journal,
            telemetry=self._mutation_telemetry,
        )

    def discard_login_settings(self) -> None:
        """Drop the no-longer-needed endpoint and credential reference.

        Native login has already returned and this execution session has no
        relogin operation.  This is intentionally separate from ``close`` so
        the outer factory can make the drop part of its successful-open
        transaction, before the session is returned to the Adapter runtime.
        """

        with self._lock:
            if self._closed or self._api is None or self._native_session_id is None:
                raise EmtTradeCoreExecutionSessionError(
                    "EMT execution session cannot discard unavailable login material"
                )
            self._settings = None

    def _mark(self, stage: str) -> None:
        if self._lifecycle_observer is not None:
            self._lifecycle_observer(stage)

    def query_unfinished_orders(self, native_request_id: int) -> int:
        return self._invoke_query("queryUnfinishedOrders", native_request_id)

    def query_orders(self, native_request_id: int) -> int:
        return self._invoke_query(
            "queryOrders",
            {"ticker": "", "begin_time": 0, "end_time": 0},
            native_request_id,
        )

    def query_trades(self, native_request_id: int) -> int:
        return self._invoke_query(
            "queryTrades",
            {"ticker": "", "begin_time": 0, "end_time": 0},
            native_request_id,
        )

    def query_orders_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        return self._invoke_query(
            "queryOrdersByPage",
            {"req_count": page_size, "reference": continuation, "reserved": 0},
            native_request_id,
        )

    def query_trades_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        return self._invoke_query(
            "queryTradesByPage",
            {"req_count": page_size, "reference": continuation, "reserved": 0},
            native_request_id,
        )

    def query_positions(self, native_request_id: int) -> int:
        return self._invoke_query("queryPosition", "", native_request_id)

    def query_positions_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        return self._invoke_query(
            "queryPositionByPage",
            {"req_count": page_size, "reference": continuation, "reserved": 0},
            native_request_id,
        )

    def query_assets(self, native_request_id: int) -> int:
        return self._invoke_query("queryAsset", native_request_id)

    def query_funds(self, native_request_id: int) -> int:
        return self._invoke_query("queryFund", native_request_id)

    def query_credit_fund_info(self, native_request_id: int) -> int:
        return self._invoke_query("queryCreditFundInfo", native_request_id)

    def query_credit_asset_debt(self, native_request_id: int) -> int:
        return self._invoke_query("queryCreditAssetDebtInfo", native_request_id)

    def query_credit_position_extra_info(self, market: int, native_request_id: int) -> int:
        if market not in {1, 2}:
            raise EmtTradeCoreExecutionSessionError("EMT credit position market is invalid")
        # EMT requires an explicit market plus a zero-filled all-ticker filter.
        # R1 invokes the bounded Shenzhen and Shanghai scans separately.
        return self._invoke_query(
            "queryCreditPositionExtraInfo",
            {"market": market, "ticker": "000000"},
            native_request_id,
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            api, session_id = self._api, self._native_session_id
            self._api = None
            self._native_session_id = None
            self._api_version = None
            self._trading_day = None
            self._mutation_port = None
            self._closed = True
        try:
            if api is not None:
                _release_api(
                    api,
                    session_id or 0,
                    lifecycle_observer=self._lifecycle_observer,
                )
        finally:
            self._on_closed()

    def _require_ready(self) -> tuple[Any, int]:
        with self._lock:
            if self._closed or self._api is None or self._native_session_id is None:
                raise EmtTradeCoreExecutionSessionError("EMT execution session is not ready")
            return self._api, self._native_session_id

    def _invoke_query(self, method_name: str, *arguments: object) -> int:
        if not arguments or type(arguments[-1]) is not int or arguments[-1] <= 0:
            raise EmtTradeCoreExecutionSessionError("EMT native request id must be positive")
        api, session_id = self._require_ready()
        method = getattr(api, method_name, None)
        if not callable(method):
            raise EmtTradeCoreExecutionSessionError(
                f"EMT execution binding does not expose {method_name}"
            )
        with suppress_native_console_output():
            result = method(*arguments[:-1], session_id, arguments[-1])
        if type(result) is not int:
            raise EmtTradeCoreExecutionSessionError(
                f"EMT {method_name} did not return an integer code"
            )
        return result


def _build_execution_trader_bridge(
    trader_api_type: type[Any],
    *,
    event_ingress: EmtBrokerEventIngress,
    query_ingress: EmtQueryCallbackIngress,
    cancel_request_id_for_order: Callable[[int], str | None],
    cancel_target_identity_for_order: Callable[[int], VenueOrderIdentity | None] | None,
    known_foreign_cancel_client_order_ids: frozenset[int],
    known_foreign_cancel_order_id: Callable[[int], bool] | None,
    known_foreign_cancel_client_order_id: Callable[[int], bool] | None,
    native_callback_observer: Callable[[str], None] | None,
    query_callback_observer: Callable[[str, int, str], None] | None = None,
) -> type[Any]:
    execution_base = build_tradecore_execution_trader_bridge(
        trader_api_type,
        event_ingress=event_ingress,
        cancel_request_id_for_order=cancel_request_id_for_order,
        cancel_target_identity_for_order=cancel_target_identity_for_order,
        known_foreign_cancel_client_order_ids=known_foreign_cancel_client_order_ids,
        known_foreign_cancel_order_id=known_foreign_cancel_order_id,
        known_foreign_cancel_client_order_id=known_foreign_cancel_client_order_id,
        native_callback_observer=native_callback_observer,
    )

    class ExecutionTraderBridge(execution_base):
        def onQueryOrder(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryOrder", args)
            _forward_query_terminal(query_ingress, "onQueryOrder", args)

        def onQueryTrade(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryTrade", args)
            _forward_query_terminal(query_ingress, "onQueryTrade", args)

        def onQueryOrderByPage(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryOrderByPage", args)
            _forward_page_query_terminal(query_ingress, "onQueryOrderByPage", args)

        def onQueryTradeByPage(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryTradeByPage", args)
            _forward_page_query_terminal(query_ingress, "onQueryTradeByPage", args)

        def onQueryPosition(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryPosition", args)
            _forward_query_terminal(query_ingress, "onQueryPosition", args)

        def onQueryPositionByPage(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryPositionByPage", args)
            _forward_page_query_terminal(query_ingress, "onQueryPositionByPage", args)

        def onQueryAsset(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryAsset", args)
            _forward_query_terminal(query_ingress, "onQueryAsset", args)

        def onQueryFund(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryFund", args)
            _forward_single_query_terminal(query_ingress, "onQueryFund", args)

        def onQueryCreditFundInfo(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryCreditFundInfo", args)
            _forward_single_query_terminal(query_ingress, "onQueryCreditFundInfo", args)

        def onQueryCreditAssetDebtInfo(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryCreditAssetDebtInfo", args)
            _forward_single_query_terminal(query_ingress, "onQueryCreditAssetDebtInfo", args)

        def onQueryCreditPositionExtraInfo(self, *args: Any) -> None:
            _observe_query_callback(query_callback_observer, "onQueryCreditPositionExtraInfo", args)
            _forward_query_terminal(query_ingress, "onQueryCreditPositionExtraInfo", args)

    return ExecutionTraderBridge


def _observe_query_callback(
    observer: Callable[[str, int, str], None] | None,
    callback_name: str,
    args: tuple[Any, ...],
) -> None:
    """Expose only callback identity and arity for bounded lifecycle diagnosis."""

    if observer is None:
        return
    try:
        observer(callback_name, len(args), _query_terminal_shape(args))
    except BaseException:
        return


def _query_terminal_shape(args: tuple[Any, ...]) -> str:
    """Describe only the terminal scalar representation, never callback content."""

    if len(args) < 2:
        return "last=absent"
    value = args[-2]
    if type(value) is bool:
        return f"last=bool-{int(value)}"
    if type(value) is int:
        return f"last=int-{value}" if value in {0, 1} else "last=int-other"
    return f"last={type(value).__name__}"


def _terminal_query_metadata(
    args: tuple[Any, ...],
) -> tuple[int, int, int | None, bool, bool] | None:
    if len(args) < 5:
        return None
    data, error, request_id, is_last, session_id = args[-5:]
    if (
        type(request_id) is not int
        or request_id <= 0
        or type(session_id) is not int
        or session_id <= 0
    ):
        return None
    normalized_last = _normalize_query_last(is_last)
    if normalized_last is None:
        return None
    error_id = error.get("error_id") if isinstance(error, dict) else None
    if type(error_id) is not int or error_id < 0:
        error_id = None
    # The native order/trade row is never copied across this boundary.  EMT's
    # Linux binding represents its empty terminal query sentinel as an empty
    # mapping, rather than ``None``.  Interpret only that row-presence fact;
    # no fields or values are retained or allowed into the normalized event.
    return request_id, session_id, error_id, normalized_last, _query_row_seen(data)


def _query_row_seen(data: object) -> bool:
    """Return a payload-free presence fact for one non-page EMT callback."""

    if data is None:
        return False
    if isinstance(data, Mapping):
        return bool(data)
    # A native binding structure represents one row even if it does not use a
    # mapping in CPython.  Its contents must remain unreachable to TradeCore.
    return True


def _single_query_metadata(args: tuple[Any, ...]) -> tuple[int, int, int | None, bool] | None:
    """Normalize EMT's documented ``QueryFund`` callback without ``is_last``.

    The binding supplies ``data, error, request_id, session_id`` for this
    single-response endpoint.  Treating it as a normal streaming callback
    would either leak a session id into the request position or wait forever
    for a terminal flag that EMT never emits.
    """

    if len(args) < 4:
        return None
    data, error, request_id, session_id = args[-4:]
    if (
        type(request_id) is not int
        or request_id <= 0
        or type(session_id) is not int
        or session_id <= 0
    ):
        return None
    error_id = error.get("error_id") if isinstance(error, Mapping) else None
    if type(error_id) is not int or error_id < 0:
        error_id = None
    return request_id, session_id, error_id, _query_row_seen(data)


def _page_query_metadata(args: tuple[Any, ...]) -> tuple[int, int, int, int, bool] | None:
    """Copy only documented page cursor/count/request/session scalars."""

    if len(args) < 7:
        return None
    request_count, result_count, reference, request_id, is_last, session_id = args[-6:]
    normalized_last = _normalize_query_last(is_last)
    if (
        type(request_count) is not int
        or request_count <= 0
        or type(result_count) is not int
        or result_count < 0
        or type(reference) is not int
        or reference < 0
        or type(request_id) is not int
        or request_id <= 0
        or type(session_id) is not int
        or session_id <= 0
        or normalized_last is None
    ):
        return None
    return request_id, session_id, reference, result_count, normalized_last


def _normalize_query_last(value: object) -> bool | None:
    """Accept EMT's Boolean and its CPython binding's canonical ``0/1`` form."""

    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    return None


def _forward_query_terminal(
    query_ingress: EmtQueryCallbackIngress,
    callback_name: str,
    args: tuple[Any, ...],
) -> None:
    metadata = _terminal_query_metadata(args)
    if metadata is None:
        return
    request_id, session_id, error_id, is_last, row_seen = metadata
    authoritative_order: EmtAuthoritativeOrderV1 | None = None
    authoritative_order_failure: str | None = None
    if query_ingress.captures_authoritative_order_rows and callback_name == "onQueryOrder":
        authoritative_order, authoritative_order_failure = _authoritative_query_order(args)
    try:
        query_ingress.on_query_callback(
            callback_name=callback_name,
            native_request_id=request_id,
            native_session_id=session_id,
            error_id=error_id,
            is_last=is_last,
            row_seen=row_seen,
            received_monotonic_ns=monotonic_ns(),
            authoritative_order=authoritative_order,
            authoritative_order_failure=authoritative_order_failure,
        )
    except Exception:
        return


def _forward_single_query_terminal(
    query_ingress: EmtQueryCallbackIngress,
    callback_name: str,
    args: tuple[Any, ...],
) -> None:
    """Forward a one-shot query callback as a terminal payload-free fact."""

    metadata = _single_query_metadata(args)
    if metadata is None:
        return
    request_id, session_id, error_id, row_seen = metadata
    try:
        query_ingress.on_query_callback(
            callback_name=callback_name,
            native_request_id=request_id,
            native_session_id=session_id,
            error_id=error_id,
            is_last=True,
            row_seen=row_seen,
            received_monotonic_ns=monotonic_ns(),
        )
    except Exception:
        return


def _authoritative_query_order(
    args: tuple[Any, ...],
    *,
    data_offset: int = -5,
) -> tuple[EmtAuthoritativeOrderV1 | None, str | None]:
    """Copy R1's five approved scalars from a full-order recovery row only.

    This helper is reached only by an explicitly configured recovery ingress.
    Ordinary query paths continue to retain the payload-free ``row_seen``
    marker returned by :func:`_terminal_query_metadata`.
    """

    if len(args) < abs(data_offset):
        return None, "row_callback_arity_invalid"
    data = args[data_offset]
    if not isinstance(data, Mapping):
        return None, "row_not_mapping"
    if not data:
        return None, "row_empty"
    scalar_specs = (
        # EMT permits an omitted user-defined order_client_id; preserve its
        # zero value so the signed namespace layer can classify it as unknown
        # and fail recovery closed instead of dropping the whole page.
        ("order_client_id", "order_client_id", False),
        ("order_emt_id", "order_emt_id", True),
        ("order_status", "order_status", False),
        ("qty_traded", "filled_qty", False),
        ("qty_left", "leaves_qty", False),
    )
    values: dict[str, int] = {}
    for native_name, normalized_name, positive in scalar_specs:
        value = data.get(native_name)
        if value is None:
            return None, f"{normalized_name}_missing"
        if type(value) is not int:
            return None, f"{normalized_name}_non_integer"
        if positive and value <= 0:
            return None, f"{normalized_name}_nonpositive"
        if not positive and value < 0:
            return None, f"{normalized_name}_negative"
        values[normalized_name] = value
    return (
        EmtAuthoritativeOrderV1(
            order_client_id=values["order_client_id"],
            order_emt_id=values["order_emt_id"],
            order_status=values["order_status"],
            filled_qty=values["filled_qty"],
            leaves_qty=values["leaves_qty"],
        ),
        None,
    )


def _forward_page_query_terminal(
    query_ingress: EmtQueryCallbackIngress,
    callback_name: str,
    args: tuple[Any, ...],
) -> None:
    metadata = _page_query_metadata(args)
    if metadata is None:
        return
    request_id, session_id, reference, result_count, is_last = metadata
    data = args[-7]
    authoritative_order: EmtAuthoritativeOrderV1 | None = None
    authoritative_order_failure: str | None = None
    if query_ingress.captures_authoritative_order_rows and callback_name == "onQueryOrderByPage":
        authoritative_order, authoritative_order_failure = _authoritative_query_order(
            args, data_offset=-7
        )
    try:
        query_ingress.on_query_callback(
            callback_name=callback_name,
            native_request_id=request_id,
            native_session_id=session_id,
            error_id=None,
            is_last=is_last,
            row_seen=_query_row_seen(data),
            received_monotonic_ns=monotonic_ns(),
            page_reference=reference,
            page_result_count=result_count,
            authoritative_order=authoritative_order,
            authoritative_order_failure=authoritative_order_failure,
        )
    except Exception:
        return


def _release_api(
    api: Any,
    native_session_id: int,
    *,
    lifecycle_observer: EmtExecutionLifecycleObserver | None = None,
) -> None:
    def mark(stage: str) -> None:
        if lifecycle_observer is None:
            return
        try:
            lifecycle_observer(stage)
        except BaseException:
            return

    # ``release`` has already made this session unusable.  Some Linux EMT
    # bindings retain a native stdio lock while their worker threads unwind;
    # avoid a process-wide final ``fflush`` deadlock on this terminal path.
    with suppress_native_console_output(flush_after=False):
        try:
            if native_session_id > 0:
                mark("native_logout_starting")
                api.logout(native_session_id)
                mark("native_logout_returned")
        finally:
            mark("native_release_starting")
            api.release()
            mark("native_release_returned")
