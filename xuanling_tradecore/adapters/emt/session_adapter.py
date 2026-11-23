"""Controlled lifecycle composition for an injected EMT read-only SDK session.

The TradeCore package does not import ``vnemttrader`` or read credentials.
An outer, platform-specific factory creates the native session only after its
caller has passed the ownership/session-supervisor gates.  This composition
attaches the durable callback observer first, validates the returned session
identity, and exposes only the typed unfinished-orders query port.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ...domain import (
    BrokerCapabilities,
    BrokerRecoveryEvidence,
    ExecutionInstruction,
    RecoveryPlan,
    TradingDayEpoch,
)
from ..base import (
    BrokerEventObserver,
    BrokerQuery,
    CancelAccepted,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    SubmitAccepted,
)
from ..mapping import ActivatedVenueMapping
from .adapter import EmtBrokerAdapter, EmtBrokerAdapterSnapshot
from .read_only_ingress import (
    EmtQueryCallbackIngress,
    EmtSdkReadOnlyPort,
)


class EmtControlledSessionError(RuntimeError):
    """An injected EMT session cannot safely become a TradeCore read-only Adapter."""


class EmtReadOnlySdkSession(Protocol):
    """Payload-free handle returned by the outer platform-specific session factory."""

    @property
    def native_session_id(self) -> int:
        """Return the positive native session identity used only for callback fencing."""

        ...

    @property
    def api_version(self) -> str:
        """Return the vendor binding's runtime API version."""

        ...

    @property
    def trading_day(self) -> str:
        """Return the vendor-provided session trading day."""

        ...

    def query_unfinished_orders(self, native_request_id: int) -> int:
        """Issue the one allowed native read-only query without retaining rows."""

        ...

    def close(self) -> None:
        """Logout and release the native session exactly once."""

        ...


class EmtReadOnlySdkSessionFactory(Protocol):
    """Create a native session after callback ingress is already attached."""

    def open(self, *, callback_ingress: EmtQueryCallbackIngress) -> EmtReadOnlySdkSession:
        """Return an authenticated session or raise without exposing credentials."""

        ...


@dataclass(frozen=True)
class EmtControlledSessionSnapshot:
    """Session state safe for local diagnostics; no native object escapes."""

    started: bool
    session_open: bool
    api_version: str | None
    trading_day: str | None
    adapter_snapshot: EmtBrokerAdapterSnapshot | None


class EmtControlledReadOnlyAdapter:
    """A ``BrokerAdapter``-shaped composition for one typed EMT read-only session.

    ``start`` intentionally has no direct SDK dependency.  It is valid only
    behind ``SessionSupervisor`` in a real runtime; its injected factory is the
    exclusive platform/credential boundary.  The adapter never exposes session
    objects, callback payloads, or a submit/cancel implementation.
    """

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        session_factory: EmtReadOnlySdkSessionFactory,
        expected_api_version: str,
        expected_trading_day: TradingDayEpoch,
        session_epoch: int,
    ) -> None:
        if mapping.manifest.venue_id != "EMT":
            raise EmtControlledSessionError("EMT controlled Adapter requires an EMT mapping")
        if not expected_api_version.strip():
            raise EmtControlledSessionError("EMT expected API version is required")
        if session_epoch <= 0:
            raise EmtControlledSessionError("EMT session epoch must be positive")
        self._mapping = mapping
        self._session_factory = session_factory
        self._expected_api_version = expected_api_version.strip()
        self._expected_trading_day = expected_trading_day
        self._session_epoch = session_epoch
        self._delegate: EmtBrokerAdapter | None = None
        self._ingress: EmtQueryCallbackIngress | None = None
        self._session: EmtReadOnlySdkSession | None = None
        self._api_version: str | None = None
        self._trading_day: str | None = None

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtControlledSessionError("EMT controlled Adapter lacks scope ownership")
        return ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._mapping.runtime.capabilities

    def start(self, observer: BrokerEventObserver) -> None:
        if self._delegate is not None:
            raise EmtControlledSessionError("EMT controlled Adapter is already started")
        ingress = EmtQueryCallbackIngress(
            self._mapping,
            trading_day=self._expected_trading_day,
            native_session_id=None,
            session_epoch=self._session_epoch,
        )
        session: EmtReadOnlySdkSession | None = None
        delegate: EmtBrokerAdapter | None = None
        api_version: str | None = None
        trading_day: str | None = None
        try:
            ingress.attach_observer(observer)
            session = self._session_factory.open(callback_ingress=ingress)
            api_version = session.api_version
            trading_day = session.trading_day
            if not api_version_matches(self._expected_api_version, api_version):
                raise EmtControlledSessionError(
                    "EMT session API version does not match the profile"
                )
            if trading_day != self._expected_trading_day.value:
                raise EmtControlledSessionError(
                    "EMT session trading day does not match the profile"
                )
            ingress.bind_native_session(session.native_session_id)
            port = EmtSdkReadOnlyPort(ingress, transport=session)
            delegate = EmtBrokerAdapter(self._mapping, read_only_port=port)
            delegate.start(observer)
        except BaseException as exc:
            if delegate is not None:
                try:
                    delegate.stop()
                except BaseException:
                    pass
            if session is not None:
                try:
                    session.close()
                except BaseException:
                    pass
            try:
                ingress.detach_observer(observer)
            except BaseException:
                pass
            if isinstance(exc, EmtControlledSessionError):
                raise
            raise EmtControlledSessionError(
                "EMT controlled read-only session startup failed"
            ) from exc
        assert session is not None and delegate is not None
        assert api_version is not None and trading_day is not None
        self._delegate = delegate
        self._ingress = ingress
        self._session = session
        self._api_version = api_version
        self._trading_day = trading_day

    def stop(self) -> None:
        delegate, session = self._delegate, self._session
        self._delegate = None
        self._ingress = None
        self._session = None
        self._api_version = None
        self._trading_day = None
        try:
            if delegate is not None:
                delegate.stop()
        finally:
            if session is not None:
                close_session(session)

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        return self._require_delegate().submit(instruction)

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        return self._require_delegate().cancel(instruction)

    def query(self, request: BrokerQuery) -> QueryHandle:
        return self._require_delegate().query(request)

    def begin_recovery(self, plan: RecoveryPlan, observer: BrokerEventObserver) -> RecoveryHandle:
        return self._require_delegate().begin_recovery(plan, observer)

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        return self._require_delegate().collect_recovery_evidence(handle)

    def snapshot(self) -> EmtControlledSessionSnapshot:
        delegate = self._delegate
        return EmtControlledSessionSnapshot(
            started=delegate is not None,
            session_open=self._session is not None,
            api_version=self._api_version,
            trading_day=self._trading_day,
            adapter_snapshot=delegate.snapshot() if delegate is not None else None,
        )

    def _require_delegate(self) -> EmtBrokerAdapter:
        if self._delegate is None:
            raise EmtControlledSessionError("EMT controlled Adapter is not started")
        return self._delegate


def api_version_matches(expected: str, actual: str) -> bool:
    try:
        expected_parts = tuple(int(part) for part in expected.split("."))
        actual_parts = tuple(int(part) for part in actual.split("."))
    except ValueError:
        return False
    return (
        len(expected_parts) >= 1
        and len(actual_parts) >= len(expected_parts)
        and actual_parts[: len(expected_parts)] == expected_parts
    )


def close_session(session: EmtReadOnlySdkSession) -> None:
    try:
        session.close()
    except BaseException as exc:
        raise EmtControlledSessionError("EMT controlled session close failed") from exc
