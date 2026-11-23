"""Controlled EMT execution-session composition without a vendor SDK import."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable, Protocol

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
from .adapter import (
    EmtAdapterSubmitStageObserver,
    EmtBrokerAdapter,
    EmtBrokerAdapterSnapshot,
    PreparedEmtSubmit,
)
from .event_ingress import EmtBrokerEventIngress
from .mutation import EmtMutationPort
from .namespace_recovery import (
    EmtAuthoritativeOrderObserverV1,
    OrderClientIdNamespaceDirectoryV1,
)
from .read_only_ingress import EmtQueryCallbackIngress, EmtSdkReadOnlyPort
from .session_adapter import EmtControlledSessionError, api_version_matches, close_session


class EmtExecutionSdkSession(Protocol):
    """Payload-free native session surface used by the controlled execution adapter."""

    @property
    def native_session_id(self) -> int:
        """Return the positive native session identity for callback fencing."""

        ...

    @property
    def api_version(self) -> str:
        """Return the runtime vendor API version."""

        ...

    @property
    def trading_day(self) -> str:
        """Return the runtime vendor trading day."""

        ...

    @property
    def mutation_port(self) -> EmtMutationPort:
        """Return the platform-owned InsertOrder/CancelOrder boundary."""

        ...

    def query_unfinished_orders(self, native_request_id: int) -> int:
        """Issue the pinned typed query without retaining vendor rows."""

        ...

    def query_orders(self, native_request_id: int) -> int:
        """Issue the current-day order query without retaining vendor rows."""

        ...

    def query_trades(self, native_request_id: int) -> int:
        """Issue the current-day trade query without retaining vendor rows."""

        ...

    def query_orders_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        """Issue one bounded order page using the returned continuation reference."""

        ...

    def query_trades_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        """Issue one bounded trade page using the returned continuation reference."""

        ...

    def query_positions(self, native_request_id: int) -> int:
        """Issue the all-position read-only query without retaining vendor rows."""

        ...

    def query_positions_by_page(
        self,
        page_size: int,
        continuation: int,
        native_request_id: int,
    ) -> int:
        """Issue one bounded position page using the returned continuation reference."""

        ...

    def query_assets(self, native_request_id: int) -> int:
        """Issue the account-asset read-only query without retaining vendor rows."""

        ...

    def query_funds(self, native_request_id: int) -> int:
        """Issue the account-fund read-only query without retaining vendor rows."""

        ...

    def query_credit_fund_info(self, native_request_id: int) -> int:
        """Issue the capability-gated credit fund query without retaining vendor rows."""

        ...

    def query_credit_asset_debt(self, native_request_id: int) -> int:
        """Issue the capability-gated credit asset/debt query without vendor rows."""

        ...

    def query_credit_position_extra_info(self, market: int, native_request_id: int) -> int:
        """Issue one market-specific credit-position query without vendor rows."""

        ...

    def close(self) -> None:
        """Logout and release the native session exactly once."""

        ...


class EmtExecutionSdkSessionFactory(Protocol):
    """Open one platform-owned session after both durable ingresses are attached."""

    def open(
        self,
        *,
        query_callback_ingress: EmtQueryCallbackIngress,
        broker_event_ingress: EmtBrokerEventIngress,
    ) -> EmtExecutionSdkSession:
        """Return an authenticated session without exposing credentials."""

        ...


@dataclass(frozen=True)
class EmtControlledExecutionSnapshot:
    """Safe execution-session diagnostics; native objects and credentials stay private."""

    started: bool
    session_open: bool
    api_version: str | None
    trading_day: str | None
    adapter_snapshot: EmtBrokerAdapterSnapshot | None
    query_callback_gap_reason_counts: dict[str, int]
    query_authoritative_row_failure_counts: dict[str, int]
    callback_gap_reason_counts: dict[str, int]
    verified_foreign_cancel_callback_count: int
    verified_foreign_namespace_callback_count: int


class EmtControlledExecutionAdapter:
    """Bind one verified session, mutation port, and dual callback ingresses.

    It becomes usable only after the caller's SessionSupervisor/ownership and
    DISARMED gates select it.  The class does not arm an account, retry native
    calls, or relax the incomplete-recovery eligibility of the EMT manifest.
    """

    def __init__(
        self,
        mapping: ActivatedVenueMapping,
        *,
        session_factory: EmtExecutionSdkSessionFactory,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        expected_api_version: str,
        expected_trading_day: TradingDayEpoch,
        session_epoch: int,
        event_ingress_capacity: int = 1024,
        broker_event_observer_wrapper: Callable[[BrokerEventObserver], BrokerEventObserver]
        | None = None,
        lifecycle_observer: Callable[[str], None] | None = None,
        submit_stage_observer: EmtAdapterSubmitStageObserver | None = None,
        native_reference_retainer: Callable[[object], None] | None = None,
        namespace_directory: OrderClientIdNamespaceDirectoryV1 | None = None,
        authoritative_order_observer: EmtAuthoritativeOrderObserverV1 | None = None,
        authoritative_row_failure_observer: Callable[[str], None] | None = None,
    ) -> None:
        if mapping.manifest.venue_id != "EMT":
            raise EmtControlledSessionError(
                "EMT controlled execution Adapter requires an EMT mapping"
            )
        if not expected_api_version.strip():
            raise EmtControlledSessionError("EMT expected API version is required")
        if session_epoch <= 0:
            raise EmtControlledSessionError("EMT session epoch must be positive")
        self._mapping = mapping
        self._session_factory = session_factory
        self._client_order_ref_factory = client_order_ref_factory
        self._expected_api_version = expected_api_version.strip()
        self._expected_trading_day = expected_trading_day
        self._session_epoch = session_epoch
        self._event_ingress_capacity = event_ingress_capacity
        self._broker_event_observer_wrapper = broker_event_observer_wrapper
        self._lifecycle_observer = lifecycle_observer
        self._submit_stage_observer = submit_stage_observer
        self._native_reference_retainer = native_reference_retainer
        self._namespace_directory = namespace_directory
        self._authoritative_order_observer = authoritative_order_observer
        self._authoritative_row_failure_observer = authoritative_row_failure_observer
        self._validate_namespace_directory()
        self._delegate: EmtBrokerAdapter | None = None
        self._query_ingress: EmtQueryCallbackIngress | None = None
        self._event_ingress: EmtBrokerEventIngress | None = None
        self._session: EmtExecutionSdkSession | None = None
        self._api_version: str | None = None
        self._trading_day: str | None = None
        self._root_observer: BrokerEventObserver | None = None
        self._attached_observer: BrokerEventObserver | None = None

    @property
    def adapter_id(self) -> str:
        return self._mapping.manifest.fingerprint.adapter_id

    @property
    def adapter_instance_id(self) -> str:
        ownership = self._mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtControlledSessionError(
                "EMT controlled execution Adapter lacks scope ownership"
            )
        return ownership.adapter_instance_id

    @property
    def capabilities(self) -> BrokerCapabilities:
        return self._mapping.runtime.capabilities

    def start(self, observer: BrokerEventObserver) -> None:
        if self._delegate is not None:
            raise EmtControlledSessionError("EMT controlled execution Adapter is already started")
        attached_observer = (
            self._broker_event_observer_wrapper(observer)
            if self._broker_event_observer_wrapper is not None
            else observer
        )
        query_ingress = EmtQueryCallbackIngress(
            self._mapping,
            trading_day=self._expected_trading_day,
            native_session_id=None,
            session_epoch=self._session_epoch,
            authoritative_order_observer=self._authoritative_order_observer,
            authoritative_row_failure_observer=self._authoritative_row_failure_observer,
        )
        event_ingress = EmtBrokerEventIngress(
            self._mapping,
            trading_day=self._expected_trading_day,
            capacity=self._event_ingress_capacity,
            namespace_directory=self._namespace_directory,
        )
        session: EmtExecutionSdkSession | None = None
        delegate: EmtBrokerAdapter | None = None
        try:
            self._mark("controlled_ingresses_attaching")
            query_ingress.attach_observer(attached_observer)
            event_ingress.attach_observer(attached_observer)
            self._mark("controlled_ingresses_attached")
            session = self._session_factory.open(
                query_callback_ingress=query_ingress,
                broker_event_ingress=event_ingress,
            )
            self._mark("controlled_session_opened")
            if not api_version_matches(self._expected_api_version, session.api_version):
                raise EmtControlledSessionError(
                    "EMT session API version does not match the profile"
                )
            self._mark("controlled_api_version_verified")
            if session.trading_day != self._expected_trading_day.value:
                raise EmtControlledSessionError(
                    "EMT session trading day does not match the profile"
                )
            self._mark("controlled_trading_day_verified")
            query_ingress.bind_native_session(session.native_session_id)
            self._mark("controlled_query_ingress_bound")
            delegate = EmtBrokerAdapter(
                self._mapping,
                read_only_port=EmtSdkReadOnlyPort(query_ingress, transport=session),
                mutation_port=session.mutation_port,
                client_order_ref_factory=self._client_order_ref_factory,
                event_ingress=event_ingress,
                submit_stage_observer=self._submit_stage_observer,
            )
            self._mark("controlled_delegate_starting")
            delegate.start(attached_observer)
            self._mark("controlled_delegate_started")
        except BaseException as exc:
            self._mark("controlled_start_failed")
            if delegate is not None:
                try:
                    delegate.stop()
                except BaseException:
                    pass
            if session is not None:
                try:
                    close_session(session)
                except BaseException:
                    pass
                # A Linux EMT wrapper can block in its Python finalizer even
                # after its native session has been released.  A bounded
                # acceptance worker supplies this hook and exits with
                # ``os._exit`` only after it has written its report; retain
                # the inert wrapper through that point so a failed version or
                # trading-day check cannot hang before producing evidence.
                # Long-running services do not provide the hook and retain
                # no closed native reference.
                if self._native_reference_retainer is not None:
                    self._mark("controlled_native_session_retain_starting")
                    self._native_reference_retainer(session)
                    self._mark("controlled_native_session_retained")
            for ingress in (query_ingress, event_ingress):
                try:
                    ingress.detach_observer(attached_observer)
                except BaseException:
                    pass
            if isinstance(exc, EmtControlledSessionError):
                raise
            raise EmtControlledSessionError(
                "EMT controlled execution session startup failed"
            ) from exc
        self._delegate = delegate
        self._query_ingress = query_ingress
        self._event_ingress = event_ingress
        self._session = session
        self._api_version = session.api_version
        self._trading_day = session.trading_day
        self._root_observer = observer
        self._attached_observer = attached_observer

    def _mark(self, stage: str) -> None:
        if self._lifecycle_observer is not None:
            self._lifecycle_observer(stage)

    def _submit_stage(self, instruction: ExecutionInstruction, stage: str) -> None:
        observer = self._submit_stage_observer
        if observer is None:
            return
        try:
            observer(instruction, stage, monotonic_ns())
        except BaseException:
            return

    def stop(self) -> None:
        delegate, session = self._delegate, self._session
        try:
            # The vendor can still deliver one final callback while logout or
            # release unwinds native worker threads.  Keep both ingresses
            # attached through that boundary; otherwise a valid late callback
            # raises from the vendor thread after the observer was detached.
            if session is not None:
                close_session(session)
        finally:
            try:
                if delegate is not None:
                    self._mark("controlled_delegate_stop_starting")
                    delegate.stop()
                    self._mark("controlled_delegate_stop_returned")
            finally:
                self._mark("controlled_state_clearing")
                self._delegate = None
                self._query_ingress = None
                self._event_ingress = None
                self._session = None
                self._api_version = None
                self._trading_day = None
                self._root_observer = None
                self._attached_observer = None
                self._mark("controlled_state_cleared")
        # The Linux vendor binding can block while its already-released
        # Python wrapper is finalized.  A caller may retain the now-inert
        # delegate only when it will immediately terminate a dedicated native
        # subprocess after writing its bounded report.  Normal services never
        # provide this hook and retain no closed native reference.
        if delegate is not None and self._native_reference_retainer is not None:
            self._mark("controlled_native_reference_retain_starting")
            self._native_reference_retainer(delegate)
            self._mark("controlled_native_reference_retained")

    def submit(self, instruction: ExecutionInstruction) -> SubmitAccepted:
        delegate = self._require_delegate()
        if self._submit_stage_observer is not None:
            self._submit_stage(instruction, "C1P_AFTER_CONTROLLED_DELEGATE")
        return delegate.submit(instruction)

    def prepare_submit(self, instruction: ExecutionInstruction) -> PreparedEmtSubmit:
        """Prepare a pure-Python descriptor without invoking the SDK."""

        return self._require_delegate().prepare_submit(instruction)

    def submit_prepared(self, prepared: PreparedEmtSubmit) -> SubmitAccepted:
        """Dispatch an active-session descriptor through the fenced mutation port."""

        return self._require_delegate().submit_prepared(prepared)

    def cancel(self, instruction: CancelExecutionInstruction) -> CancelAccepted:
        return self._require_delegate().cancel(instruction)

    def query(self, request: BrokerQuery) -> QueryHandle:
        return self._require_delegate().query(request)

    def begin_recovery(self, plan: RecoveryPlan, observer: BrokerEventObserver) -> RecoveryHandle:
        if observer is not self._root_observer or self._attached_observer is None:
            raise EmtControlledSessionError("recovery must use the execution Adapter's observer")
        return self._require_delegate().begin_recovery(plan, self._attached_observer)

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        return self._require_delegate().collect_recovery_evidence(handle)

    def drain_broker_events(self, *, wait_timeout_seconds: float = 0.0) -> int:
        """Drain callbacks, optionally blocking for the next native callback.

        Hosts should use the bounded blocking form from their callback worker
        instead of sleeping between empty polls.  The actual observer work
        remains off the native SDK callback thread.
        """

        if self._event_ingress is None:
            raise EmtControlledSessionError("EMT controlled execution Adapter is not started")
        return self._event_ingress.drain(wait_timeout_seconds=wait_timeout_seconds)

    def snapshot(self) -> EmtControlledExecutionSnapshot:
        delegate = self._delegate
        return EmtControlledExecutionSnapshot(
            started=delegate is not None,
            session_open=self._session is not None,
            api_version=self._api_version,
            trading_day=self._trading_day,
            adapter_snapshot=delegate.snapshot() if delegate is not None else None,
            query_callback_gap_reason_counts=(
                self._query_ingress.callback_gap_reason_counts
                if self._query_ingress is not None
                else {}
            ),
            query_authoritative_row_failure_counts=(
                self._query_ingress.authoritative_row_failure_counts
                if self._query_ingress is not None
                else {}
            ),
            callback_gap_reason_counts=(
                self._event_ingress.callback_gap_reason_counts
                if self._event_ingress is not None
                else {}
            ),
            verified_foreign_cancel_callback_count=(
                self._event_ingress.verified_foreign_cancel_callback_count
                if self._event_ingress is not None
                else 0
            ),
            verified_foreign_namespace_callback_count=(
                self._event_ingress.verified_foreign_namespace_callback_count
                if self._event_ingress is not None
                else 0
            ),
        )

    def _validate_namespace_directory(self) -> None:
        """Fence opt-in R1 ownership against this exact mapped adapter scope."""

        directory = self._namespace_directory
        if directory is None:
            return
        ownership = self._mapping.manifest.scope_ownership
        if ownership is None:
            raise EmtControlledSessionError("R1 namespace directory lacks scope ownership")
        scope = directory.scope
        if (
            scope.account_id != self._mapping.runtime.account_capabilities.account_id
            or scope.trading_day != self._expected_trading_day
            or scope.emt_client_id != int(ownership.adapter_client_scope)
            or directory.own.adapter_instance_id != ownership.adapter_instance_id
        ):
            raise EmtControlledSessionError("R1 namespace directory belongs to another EMT scope")

    def _require_delegate(self) -> EmtBrokerAdapter:
        if self._delegate is None:
            raise EmtControlledSessionError("EMT controlled execution Adapter is not started")
        return self._delegate
