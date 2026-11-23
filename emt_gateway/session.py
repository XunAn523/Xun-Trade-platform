"""Fail-closed EMT session lifecycle using the platform loader and SPI bridge."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any

from .platform_loader import LoadedSdk, suppress_native_console_output
from .spi_bridge import CallbackIngressPort, build_trader_spi_bridge


class SessionError(RuntimeError):
    """A lifecycle operation failed; callers must keep the gateway non-trading."""


def normalize_emt_trading_day(value: str) -> str:
    """Return the ISO trading-day form required by the typed Adapter boundary.

    EMT bindings in the field may return either ``YYYYMMDD`` or ``YYYY-MM-DD``.
    Convert that vendor representation at the outer platform boundary so the
    core never compares two spellings of the same trading day.
    """

    candidate = value.strip()
    if len(candidate) == 8 and candidate.isascii() and candidate.isdecimal():
        candidate = f"{candidate[:4]}-{candidate[4:6]}-{candidate[6:]}"
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError as exc:
        raise SessionError("EMT returned an invalid trading day") from exc


class SessionState(str, Enum):
    NEW = "NEW"
    API_CREATED = "API_CREATED"
    READY = "READY"
    FAILED = "FAILED"
    CLOSED = "CLOSED"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"


@dataclass(frozen=True)
class SessionSettings:
    """Outer-platform session inputs.

    A running native session needs these values for an explicit relogin, but
    they are never diagnostic data.  In particular, do not let an exception
    that happens to render this dataclass disclose a broker endpoint,
    account, password, or client identifier.
    """

    host: str = field(repr=False)
    port: int = field(repr=False)
    account: str = field(repr=False)
    password: str = field(repr=False)
    client_id: int = field(repr=False)
    sdk_log_dir: Path = field(repr=False)
    account_profile: str = "ordinary"
    local_ip: str = field(default="", repr=False)
    public_topic_resume_type: int = 2
    heartbeat_seconds: int = 10
    software_version: str = "emt-gateway/0.1"
    sdk_log_level: int = 0
    # EMT's Python sample configures its native receive/process threads after
    # a successful Login.  Both values are deliberately opt-in: a deployment
    # must reserve two host CPUs for the vendor threads instead of silently
    # sharing a TradeCore worker CPU.
    sdk_receive_cpu: int | None = None
    sdk_process_cpu: int | None = None

    def validate(self) -> None:
        if not self.host or not self.account or not self.password:
            raise SessionError("host, account and password are required")
        if not 1 <= self.port <= 65535:
            raise SessionError("port must be in 1..65535")
        if not 1 <= self.client_id <= 255:
            raise SessionError("client_id must be in 1..255")
        if self.account_profile not in {"ordinary", "credit", "option"}:
            raise SessionError("account_profile must be ordinary, credit or option")
        if not 5 <= self.heartbeat_seconds <= 15:
            raise SessionError("heartbeat_seconds must be in the approved 5..15 second range")
        if (self.sdk_receive_cpu is None) != (self.sdk_process_cpu is None):
            raise SessionError("EMT SDK receive and process CPUs must be configured together")
        if any(
            isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 0
            for cpu in (self.sdk_receive_cpu, self.sdk_process_cpu)
            if cpu is not None
        ):
            raise SessionError("EMT SDK CPU affinity values must be non-negative integers")
        if self.sdk_receive_cpu is not None and self.sdk_receive_cpu == self.sdk_process_cpu:
            raise SessionError("EMT SDK receive and process CPUs must be distinct")


def configure_optional_sdk_cpu_affinity(*, api: Any, settings: SessionSettings) -> bool:
    """Apply an explicitly reserved EMT receive/process CPU pair after login.

    ``setCpuAffinity`` is intentionally never probed or inferred.  When a
    deployment requests it, absence or failure is a lifecycle error; allowing
    a run to continue on shared, unverified CPUs would make the performance
    profile claim misleading.  The official Python sample invokes this method
    only after a successful ``login``.
    """

    receive_cpu, process_cpu = settings.sdk_receive_cpu, settings.sdk_process_cpu
    if receive_cpu is None:
        return False
    assert process_cpu is not None
    configure = getattr(api, "setCpuAffinity", None)
    if not callable(configure):
        raise RuntimeError("EMT binding does not expose setCpuAffinity")
    try:
        configure(receive_cpu, process_cpu)
    except Exception as exc:
        raise RuntimeError("EMT setCpuAffinity failed") from exc
    return True


@dataclass(frozen=True)
class SessionSnapshot:
    state: SessionState
    session_id: int | None
    api_version: str | None
    trading_day: str | None


class EmtSession:
    """Owns one account/client-id session and never performs automatic relogin."""

    def __init__(
        self,
        *,
        trader_api_base: type[Any],
        callback_ingress: CallbackIngressPort,
        settings: SessionSettings,
    ) -> None:
        settings.validate()
        self._initialize(
            settings=settings,
            bridge_type=build_trader_spi_bridge(
                trader_api_base,
                callback_ingress=callback_ingress,
                account_profile=settings.account_profile,
            ),
        )

    def _initialize(self, *, settings: SessionSettings, bridge_type: type[Any]) -> None:
        self._settings = settings
        self._bridge_type = bridge_type
        self._api: Any | None = None
        self._session_id: int | None = None
        self._state = SessionState.NEW
        self._api_version: str | None = None
        self._trading_day: str | None = None

    @classmethod
    def from_loaded_sdk(
        cls,
        loaded_sdk: LoadedSdk,
        *,
        callback_ingress: CallbackIngressPort,
        settings: SessionSettings,
    ) -> "EmtSession":
        return cls(
            trader_api_base=loaded_sdk.trader_api_type,
            callback_ingress=callback_ingress,
            settings=settings,
        )

    @classmethod
    def from_prebuilt_trader_spi_type(
        cls,
        *,
        trader_spi_type: type[Any],
        settings: SessionSettings,
    ) -> "EmtSession":
        """Prepare a session shell around an already verified SPI subclass.

        This is the V2 composition seam: it does not import a vendor binding,
        instantiate an API, create the SDK log directory, or call login.  The
        normal ``start`` lifecycle remains the sole owner of those effects.
        """

        settings.validate()
        if not isinstance(trader_spi_type, type):
            raise SessionError("prebuilt TraderSpi type is invalid")
        session = cls.__new__(cls)
        session._initialize(settings=settings, bridge_type=trader_spi_type)
        return session

    def start(self) -> SessionSnapshot:
        if self._state is not SessionState.NEW:
            raise SessionError(f"session cannot start from {self._state}")
        self._settings.sdk_log_dir.mkdir(parents=True, exist_ok=True)
        api = self._bridge_type()
        try:
            with suppress_native_console_output():
                api.createTraderApi(
                    self._settings.client_id,
                    str(self._settings.sdk_log_dir),
                    self._settings.sdk_log_level,
                )
                self._state = SessionState.API_CREATED
                api.setSoftwareVersion(self._settings.software_version)
                api.setHeartBeatInterval(self._settings.heartbeat_seconds)
                api.subscribePublicTopic(self._settings.public_topic_resume_type)
                session_id = int(
                    api.login(
                        self._settings.host,
                        self._settings.port,
                        self._settings.account,
                        self._settings.password,
                        1,
                        self._settings.local_ip,
                    )
                )
                if session_id > 0:
                    configure_optional_sdk_cpu_affinity(api=api, settings=self._settings)
            if session_id == 0:
                self._state = SessionState.FAILED
                error_id = self._error_id(api)
                self._release_after_failure(api)
                raise SessionError(f"EMT login failed: error_id={error_id}")
            self._api = api
            self._session_id = session_id
            self._api_version = str(api.getApiVersion())
            self._trading_day = normalize_emt_trading_day(str(api.getTradingDay()))
            self._state = SessionState.READY
            return self.snapshot()
        except SessionError:
            raise
        except Exception as exc:
            self._state = SessionState.FAILED
            self._release_after_failure(api)
            raise SessionError(f"EMT session setup failed: {type(exc).__name__}") from exc

    def close(self) -> None:
        if self._state is SessionState.CLOSED:
            return
        api, session_id = self._api, self._session_id
        self._api = None
        self._session_id = None
        self._state = SessionState.CLOSED
        if api is None:
            return
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()

    def relogin_after_disconnect(self) -> SessionSnapshot:
        """Explicitly establish a new EMT session after a native disconnect.

        Older bundled samples describe an explicit ``Login`` after
        ``onDisconnected``; newer bundled C++ documentation describes native
        automatic reconnect.  This method therefore never decides whether a
        relogin is appropriate.  A version-qualified higher-level supervisor
        must first latch the route as disarmed and complete its reconciliation
        policy before it can deliberately invoke this lifecycle operation.
        """

        if self._state is not SessionState.READY or self._api is None:
            raise SessionError("EMT session cannot relogin unless it is ready")
        api = self._api
        try:
            with suppress_native_console_output():
                session_id = int(
                    api.login(
                        self._settings.host,
                        self._settings.port,
                        self._settings.account,
                        self._settings.password,
                        1,
                        self._settings.local_ip,
                    )
                )
            if session_id <= 0:
                self._state = SessionState.FAILED
                self._session_id = None
                raise SessionError(f"EMT relogin failed: error_id={self._error_id(api)}")
            self._session_id = session_id
            self._api_version = str(api.getApiVersion())
            self._trading_day = normalize_emt_trading_day(str(api.getTradingDay()))
            return self.snapshot()
        except SessionError:
            raise
        except Exception as exc:
            self._state = SessionState.FAILED
            self._session_id = None
            raise SessionError(f"EMT relogin setup failed: {type(exc).__name__}") from exc

    def snapshot(self) -> SessionSnapshot:
        return SessionSnapshot(
            state=self._state,
            session_id=self._session_id,
            api_version=self._api_version,
            trading_day=self._trading_day,
        )

    @property
    def api(self) -> Any:
        if self._state is not SessionState.READY or self._api is None:
            raise SessionError("EMT session is not ready")
        return self._api

    @property
    def session_id(self) -> int:
        if self._state is not SessionState.READY or self._session_id is None:
            raise SessionError("EMT session is not ready")
        return self._session_id

    @staticmethod
    def _error_id(api: Any) -> int | None:
        try:
            error = api.getApiLastError()
        except Exception:
            return None
        return (
            error.get("error_id")
            if isinstance(error, dict) and isinstance(error.get("error_id"), int)
            else None
        )

    @staticmethod
    def _release_after_failure(api: Any) -> None:
        try:
            api.release()
        except Exception:
            return None
