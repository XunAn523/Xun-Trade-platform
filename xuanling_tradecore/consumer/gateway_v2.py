"""In-process ConsumerGateway V2 session authorization boundary.

This module owns no broker, Journal, database, or socket connection.  It pins
an already verified local principal to the immutable binding snapshot that was
current when the session was opened, then checks that snapshot's epoch before
each request.  Later replay and transport stages use the same guard.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import count
from threading import Lock
from typing import Callable

from ..domain import (
    ConsumerAccountBinding,
    ConsumerAuditTrail,
    ConsumerOrderEvent,
    ConsumerPrincipal,
    ConsumerWireContractV2,
)
from .base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerAccountView,
    ConsumerAuthorizationError,
    ConsumerEventSubscription,
    ConsumerGatewayError,
    ConsumerNotFoundError,
    ConsumerOrderSignal,
    ConsumerSession,
    ConsumerVersionRange,
    IntentReceipt,
    PositionedConsumerEventV2,
    negotiate_consumer_versions,
)
from .bindings_v2 import ConsumerBindingError, ConsumerBindingSnapshotV2
from .operations_v2 import ConsumerGatewayOperationsV2
from .projection_reads_v2 import ConsumerProjectionReadErrorV2, projection_read_scope
from .replay_v2 import (
    ConsumerCursorReplayError,
    ConsumerCursorReplayServiceV2,
    SignedConsumerEventPageV2,
    SignedConsumerReplayPlanV2,
)
from .snapshot_index_v2 import (
    ConsumerOrderSnapshotPageV2,
    ConsumerOrderSnapshotPlanV2,
    ConsumerPagedSnapshotErrorV2,
    ConsumerSnapshotIndexV2,
)
from .subscription_v2 import ConsumerLiveSubscriptionV2


class ConsumerGatewayConfigurationError(ConsumerGatewayError):
    """A trusted local ConsumerGateway dependency produced an unsafe result."""


@dataclass(frozen=True)
class _LiveSessionV2:
    session: ConsumerSession
    principal: ConsumerPrincipal
    contract_schema_hash: str


@dataclass
class _TransientSessionLeaseV2:
    active: bool = True


@dataclass(frozen=True, kw_only=True)
class _TransientConsumerSessionV2(ConsumerSession):
    gateway_nonce: object
    principal: ConsumerPrincipal
    contract_schema_hash: str
    lease: _TransientSessionLeaseV2


class ConsumerGatewayV2:
    """Authorize V2 Consumer sessions against the current local snapshot."""

    def __init__(
        self,
        *,
        binding_snapshot: Callable[[], ConsumerBindingSnapshotV2],
        account_view_for: Callable[[ConsumerAccountBinding], ConsumerAccountView],
        wire_contract: ConsumerWireContractV2,
        event_replay: ConsumerCursorReplayServiceV2 | None = None,
        operations: ConsumerGatewayOperationsV2 | None = None,
        paged_snapshots: ConsumerSnapshotIndexV2 | None = None,
        projection_read_delivery_guard: Callable[[], None] | None = None,
    ) -> None:
        self._binding_snapshot = binding_snapshot
        self._account_view_for = account_view_for
        self._wire_contract = wire_contract
        self._event_replay = event_replay
        self._operations = operations
        self._paged_snapshots = paged_snapshots
        self._projection_read_delivery_guard = projection_read_delivery_guard
        self._lock = Lock()
        self._next_session_number = count(1)
        self._sessions: dict[str, _LiveSessionV2] = {}
        self._transient_session_nonce = object()

    def open_session(
        self,
        principal: ConsumerPrincipal,
        requested_versions: ConsumerVersionRange,
    ) -> ConsumerSession:
        """Open an authenticated local session without a broker-side effect."""

        snapshot = self._require_snapshot_principal(principal)
        api_version, event_schema_version = negotiate_consumer_versions(
            requested_versions,
            (self._wire_contract.api_version,),
            (2,),
        )
        with self._lock:
            session = ConsumerSession(
                session_id=f"cgs2-{next(self._next_session_number)}",
                consumer_id=principal.consumer_id,
                api_version=api_version,
                binding_epoch=snapshot.authorization_epoch,
                event_schema_version=event_schema_version,
            )
            self._sessions[session.session_id] = _LiveSessionV2(
                session=session,
                principal=principal,
                contract_schema_hash=self._wire_contract.schema_hash,
            )
        return session

    def open_transient_session(
        self,
        principal: ConsumerPrincipal,
        requested_versions: ConsumerVersionRange,
    ) -> ConsumerSession:
        """Authorize one synchronous request without publishing session state.

        The request-reactor's one-request connection profile never needs a
        session after response construction.  An unforgeable, explicitly
        revoked local lease avoids three uncontended global-lock turns per RPC
        while retaining the same principal, version and epoch checks.
        """

        snapshot = self._require_snapshot_principal(principal)
        api_version, event_schema_version = negotiate_consumer_versions(
            requested_versions,
            (self._wire_contract.api_version,),
            (2,),
        )
        return _TransientConsumerSessionV2(
            session_id=f"cgs2t-{next(self._next_session_number)}",
            consumer_id=principal.consumer_id,
            api_version=api_version,
            binding_epoch=snapshot.authorization_epoch,
            event_schema_version=event_schema_version,
            gateway_nonce=self._transient_session_nonce,
            principal=principal,
            contract_schema_hash=self._wire_contract.schema_hash,
            lease=_TransientSessionLeaseV2(),
        )

    def close_transient_session(self, session: ConsumerSession) -> None:
        """Invalidate a reactor-owned transient session after one response."""

        if not isinstance(session, _TransientConsumerSessionV2):
            raise ConsumerAuthorizationError("consumer session is not transient")
        if session.gateway_nonce is not self._transient_session_nonce:
            raise ConsumerAuthorizationError("consumer transient session belongs elsewhere")
        session.lease.active = False

    def close_session(self, session: ConsumerSession) -> None:
        """Release transport-owned session state when its connection closes."""

        with self._lock:
            self._sessions.pop(session.session_id, None)

    def list_accounts(self, session: ConsumerSession) -> tuple[ConsumerAccountView, ...]:
        """Return only active accounts from the current session binding epoch."""

        snapshot = self._require_current_session(session)
        views: list[ConsumerAccountView] = []
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                view = self._account_view_for(binding)
            except Exception as exc:
                raise ConsumerGatewayConfigurationError(
                    "consumer account view is unavailable"
                ) from exc
            if (
                view.consumer_account_id != binding.consumer_account_id
                or view.venue_id != binding.venue_id
            ):
                raise ConsumerGatewayConfigurationError(
                    "consumer account view does not match the binding"
                )
            views.append(view)
        return tuple(views)

    def contract_schema_hash(self, session: ConsumerSession) -> str:
        """Return the verified contract hash recorded when this session opened."""

        live = self._require_live_session(session)
        return live.contract_schema_hash

    def issue_event_cursor(self, session: ConsumerSession, *, consumer_account_id: str) -> str:
        """Issue a bound BEGIN cursor from the account's registered stream."""

        snapshot = self._require_current_session(session)
        return self._require_event_replay().issue_begin_cursor(
            binding=self._require_account_binding(snapshot, session, consumer_account_id),
            authorization_epoch=session.binding_epoch,
        )

    def poll_event_cursor(
        self,
        session: ConsumerSession,
        *,
        token: str,
        limit: int,
    ) -> SignedConsumerEventPageV2:
        """Replay from one authenticated account cursor without caller routing hints."""

        snapshot = self._require_current_session(session)
        replay = self._require_event_replay()
        errors: list[ConsumerCursorReplayError] = []
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                return replay.poll(
                    binding=binding,
                    authorization_epoch=session.binding_epoch,
                    token=token,
                    limit=limit,
                )
            except ConsumerCursorReplayError as exc:
                # A reader failure or current-stream gap is not an account
                # routing mismatch. Preserve that fail-closed outcome instead
                # of disguising it as a bad client token after the loop.
                if exc.code not in {"CURSOR_INVALID", "CURSOR_EXPIRED"}:
                    raise
                errors.append(exc)
        expired = next((error for error in errors if error.code == "CURSOR_EXPIRED"), None)
        if expired is not None:
            raise expired
        raise ConsumerCursorReplayError("CURSOR_INVALID", "consumer cursor cannot be used")

    def create_replay_partitions(
        self,
        session: ConsumerSession,
        *,
        consumer_account_id: str,
        partition_count: int,
        after_cursor: str | None = None,
    ) -> SignedConsumerReplayPlanV2:
        snapshot = self._require_current_session(session)
        return self._require_event_replay().issue_partitions(
            binding=self._require_account_binding(snapshot, session, consumer_account_id),
            authorization_epoch=session.binding_epoch,
            partition_count=partition_count,
            after_cursor=after_cursor,
        )

    def validate_projection_read_delivery(
        self, session: ConsumerSession, *, operation: str, tokens: tuple[str, ...]
    ) -> None:
        """Recheck a completed cold result without repeating any storage read."""
        snapshot = self._require_current_session(session)
        if self._projection_read_delivery_guard is None:
            raise ConsumerProjectionReadErrorV2("cold read delivery guard is unavailable")
        with projection_read_scope(operation):
            self._projection_read_delivery_guard()
        if operation in {"GetOrder", "GetOrderAuditTrail"}:
            return
        replay = self._require_event_replay()
        for token in tokens:
            errors: list[ConsumerCursorReplayError] = []
            for binding in snapshot.accounts_for(session.consumer_id):
                try:
                    replay.validate_delivery(
                        binding=binding, authorization_epoch=session.binding_epoch, token=token
                    )
                    break
                except ConsumerCursorReplayError as exc:
                    errors.append(exc)
            else:
                expired = next((e for e in errors if e.code == "CURSOR_EXPIRED"), None)
                if expired is not None:
                    raise expired
                gap = next((e for e in errors if e.code == "JOURNAL_GAP"), None)
                if gap is not None:
                    raise gap
                raise ConsumerCursorReplayError("CURSOR_INVALID", "delivery cursor is invalid")

    def submit_signal(self, session: ConsumerSession, signal: ConsumerOrderSignal) -> IntentReceipt:
        """Authorize a signal before handing it to the Journal-backed OMS lane."""

        snapshot = self._require_current_session(session)
        binding = self._require_account_binding(snapshot, session, signal.consumer_account_id)
        try:
            strategy = snapshot.require_strategy(
                consumer_id=session.consumer_id,
                consumer_account_id=binding.consumer_account_id,
                strategy_id=signal.strategy_id,
            )
        except ConsumerBindingError as exc:
            raise ConsumerAuthorizationError("consumer strategy is not authorized") from exc
        receipt = self._require_operations().submit_signal(
            session=session,
            account_binding=binding,
            strategy_binding_id=strategy.binding_id,
            signal=signal,
        )
        self._validate_receipt(binding, session, signal, receipt)
        return receipt

    def cancel_order(
        self, session: ConsumerSession, *, consumer_request_id: str, oms_order_id: str, reason: str
    ) -> IntentReceipt:
        """Authorize an account-scoped cancel without exposing execution routing."""

        snapshot = self._require_current_session(session)
        order = self._find_order(snapshot, session, oms_order_id)
        binding = self._require_account_binding(
            snapshot, session, order.consumer_trace.consumer_account_id
        )
        receipt = self._require_operations().cancel_order(
            session=session,
            account_binding=binding,
            consumer_request_id=consumer_request_id,
            oms_order_id=oms_order_id,
            reason=reason,
        )
        if receipt.consumer_request_id != consumer_request_id:
            raise ConsumerGatewayConfigurationError("cancel receipt has another request id")
        self._validate_receipt_trace(binding, session, receipt)
        return receipt

    def get_order(self, session: ConsumerSession, *, oms_order_id: str) -> ConsumerOrderEvent:
        """Return one order only when its immutable trace matches a live binding."""

        snapshot = self._require_current_session(session)
        return self._find_order(snapshot, session, oms_order_id)

    def get_order_audit_trail(
        self, session: ConsumerSession, *, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]:
        """Return durable audit pointers only after non-disclosing order authorization."""

        snapshot = self._require_current_session(session)
        order = self._find_order(snapshot, session, oms_order_id)
        binding = self._require_account_binding(
            snapshot, session, order.consumer_trace.consumer_account_id
        )
        trail = self._require_operations().get_order_audit_trail(
            account_binding=binding, oms_order_id=oms_order_id
        )
        if any(
            item.consumer_id != session.consumer_id
            or item.account_id != binding.account_id
            or item.oms_order_id != oms_order_id
            for item in trail
        ):
            raise ConsumerGatewayConfigurationError("consumer audit trail is outside its binding")
        return trail

    def get_account_order_snapshot(
        self,
        session: ConsumerSession,
        *,
        consumer_account_id: str,
        reset_context: str | None = None,
    ) -> ConsumerAccountOrderSnapshotV2:
        """Return a bound snapshot/restart point, optionally after a stream reset."""

        snapshot = self._require_current_session(session)
        binding = self._require_account_binding(snapshot, session, consumer_account_id)
        result = self._require_operations().get_account_order_snapshot(
            session=session, account_binding=binding, reset_context=reset_context
        )
        if result.consumer_account_id != binding.consumer_account_id:
            raise ConsumerGatewayConfigurationError("consumer snapshot has another binding")
        if any(
            order.consumer_trace.consumer_id != session.consumer_id
            or order.consumer_trace.consumer_account_id != binding.consumer_account_id
            for order in result.orders
        ):
            raise ConsumerGatewayConfigurationError(
                "consumer snapshot includes unauthorized orders"
            )
        if (
            result.next_cursor.consumer_id != session.consumer_id
            or result.next_cursor.consumer_account_id != binding.consumer_account_id
            or result.next_cursor.authorization_epoch != session.binding_epoch
        ):
            raise ConsumerGatewayConfigurationError(
                "consumer snapshot cursor is outside its binding"
            )
        return result

    def issue_snapshot_cursor(
        self, session: ConsumerSession, snapshot: ConsumerAccountOrderSnapshotV2
    ) -> str:
        """Sign the restart point of a previously validated account snapshot."""

        current = self._require_current_session(session)
        binding = self._require_account_binding(current, session, snapshot.consumer_account_id)
        cursor = snapshot.next_cursor
        if (
            cursor.consumer_id != session.consumer_id
            or cursor.consumer_account_id != binding.consumer_account_id
            or cursor.authorization_epoch != session.binding_epoch
        ):
            raise ConsumerGatewayConfigurationError(
                "consumer snapshot cursor is outside its binding"
            )
        return self._require_event_replay().issue_cursor(
            binding=binding,
            authorization_epoch=session.binding_epoch,
            after_position=cursor.after_position,
        )

    def create_order_snapshot(
        self,
        session: ConsumerSession,
        *,
        consumer_account_id: str,
        page_size: int,
    ) -> ConsumerOrderSnapshotPlanV2:
        """Freeze one authorized immutable order view for bounded pagination."""

        snapshot = self._require_current_session(session)
        binding = self._require_account_binding(snapshot, session, consumer_account_id)
        return self._require_paged_snapshots().create(
            session=session, binding=binding, page_size=page_size
        )

    def cached_snapshot_page_ready(self, session: ConsumerSession, page_cursor: str) -> bool:
        snapshot = self._require_current_session(session)
        return self._paged_snapshots is not None and any(
            self._paged_snapshots.cached_page_ready(
                session=session, binding=binding, page_cursor=page_cursor
            )
            for binding in snapshot.accounts_for(session.consumer_id)
        )

    def read_order_snapshot_page(
        self, session: ConsumerSession, *, page_cursor: str
    ) -> tuple[ConsumerOrderSnapshotPageV2, str | None]:
        """Read one signed page and issue a replay cursor only on the final page."""

        snapshot = self._require_current_session(session)
        errors: list[ConsumerPagedSnapshotErrorV2] = []
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                page = self._require_paged_snapshots().read_page(
                    session=session, binding=binding, page_cursor=page_cursor
                )
            except ConsumerPagedSnapshotErrorV2 as exc:
                # Only a scope mismatch should try another account. Expiry can
                # precede scope validation; keep its existing aggregate handling.
                if exc.code not in {"CURSOR_INVALID", "SNAPSHOT_EXPIRED"}:
                    raise
                errors.append(exc)
                continue
            replay_cursor = None
            if not page.has_more:
                replay_cursor = self._require_event_replay().issue_cursor(
                    binding=binding,
                    authorization_epoch=session.binding_epoch,
                    after_position=page.snapshot_position,
                )
            return page, replay_cursor
        for code in ("SNAPSHOT_EXPIRED", "SNAPSHOT_GENERATION_MISMATCH"):
            matching = next((error for error in errors if error.code == code), None)
            if matching is not None:
                raise matching
        raise ConsumerPagedSnapshotErrorV2("CURSOR_INVALID", "snapshot cursor cannot be used")

    def subscribe_order_events(
        self, session: ConsumerSession, *, cursor_token: str
    ) -> ConsumerEventSubscription:
        """Open an optional bounded live feed after validating its signed cursor."""

        snapshot = self._require_current_session(session)
        replay = self._require_event_replay()
        errors: list[ConsumerCursorReplayError] = []
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                replay.poll(
                    binding=binding,
                    authorization_epoch=session.binding_epoch,
                    token=cursor_token,
                    limit=1,
                )
            except ConsumerCursorReplayError as exc:
                errors.append(exc)
                continue
            return self._require_operations().subscribe_order_events(
                session=session, account_binding=binding, cursor_token=cursor_token
            )
        expired = next((error for error in errors if error.code == "CURSOR_EXPIRED"), None)
        if expired is not None:
            raise expired
        raise ConsumerCursorReplayError("CURSOR_INVALID", "consumer cursor cannot be used")

    def open_live_subscription(
        self, session: ConsumerSession, *, cursor_token: str
    ) -> ConsumerLiveSubscriptionV2:
        """Open bounded push while retaining signed cursor replay as authority."""

        snapshot = self._require_current_session(session)
        replay = self._require_event_replay()
        errors: list[ConsumerCursorReplayError] = []
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                replay.poll(
                    binding=binding,
                    authorization_epoch=session.binding_epoch,
                    token=cursor_token,
                    limit=1,
                )
            except ConsumerCursorReplayError as exc:
                errors.append(exc)
                continue
            subscription = self._require_operations().subscribe_order_events(
                session=session, account_binding=binding, cursor_token=cursor_token
            )

            def issue_cursor(
                item: PositionedConsumerEventV2, *, _binding: ConsumerAccountBinding = binding
            ) -> str:
                event = item.event
                if (
                    event.consumer_trace.consumer_id != _binding.consumer_id
                    or event.consumer_trace.consumer_account_id != _binding.consumer_account_id
                    or event.account_id != _binding.account_id
                ):
                    raise ConsumerGatewayConfigurationError(
                        "consumer live event is outside its binding"
                    )
                return replay.issue_cursor(
                    binding=_binding,
                    authorization_epoch=session.binding_epoch,
                    after_position=item.position,
                )

            return ConsumerLiveSubscriptionV2(subscription=subscription, cursor_for=issue_cursor)
        expired = next((error for error in errors if error.code == "CURSOR_EXPIRED"), None)
        if expired is not None:
            raise expired
        raise ConsumerCursorReplayError("CURSOR_INVALID", "consumer cursor cannot be used")

    def validate_current_session(self, session: ConsumerSession) -> None:
        """Recheck authorization before delivering a deferred, read-only result."""
        self._require_current_session(session)

    def validate_snapshot_page_delivery(self, session: ConsumerSession, page_cursor: str) -> None:
        snapshot = self._require_current_session(session)
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                self._require_paged_snapshots().validate_page_delivery(
                    session=session, binding=binding, page_cursor=page_cursor
                )
                return
            except ConsumerPagedSnapshotErrorV2 as exc:
                if exc.code != "CURSOR_INVALID":
                    raise
        raise ConsumerPagedSnapshotErrorV2("CURSOR_INVALID", "snapshot cursor scope is unavailable")

    def _require_current_session(self, session: ConsumerSession) -> ConsumerBindingSnapshotV2:
        live = self._require_live_session(session)
        snapshot = self._require_snapshot_principal(live.principal)
        if session.binding_epoch != snapshot.authorization_epoch:
            raise ConsumerAuthorizationError("consumer session authorization epoch is stale")
        return snapshot

    def _require_live_session(
        self, session: ConsumerSession
    ) -> _LiveSessionV2 | _TransientConsumerSessionV2:
        if isinstance(session, _TransientConsumerSessionV2):
            if (
                session.gateway_nonce is not self._transient_session_nonce
                or not session.lease.active
            ):
                raise ConsumerAuthorizationError("consumer session is not active")
            return session
        with self._lock:
            live = self._sessions.get(session.session_id)
        if live is None or live.session != session:
            raise ConsumerAuthorizationError("consumer session is not active")
        return live

    def _require_event_replay(self) -> ConsumerCursorReplayServiceV2:
        if self._event_replay is None:
            raise ConsumerGatewayConfigurationError("consumer event replay is unavailable")
        return self._event_replay

    def _require_operations(self) -> ConsumerGatewayOperationsV2:
        if self._operations is None:
            raise ConsumerGatewayConfigurationError("consumer OMS operations are unavailable")
        return self._operations

    def _require_paged_snapshots(self) -> ConsumerSnapshotIndexV2:
        if self._paged_snapshots is None:
            raise ConsumerGatewayConfigurationError("consumer snapshot pagination is unavailable")
        return self._paged_snapshots

    def _find_order(
        self, snapshot: ConsumerBindingSnapshotV2, session: ConsumerSession, oms_order_id: str
    ):
        for binding in snapshot.accounts_for(session.consumer_id):
            try:
                order = self._require_operations().get_order(
                    account_binding=binding, oms_order_id=oms_order_id
                )
            except ConsumerNotFoundError:
                continue
            if (
                order.consumer_trace.consumer_id != session.consumer_id
                or order.consumer_trace.consumer_account_id != binding.consumer_account_id
                or order.account_id != binding.account_id
            ):
                raise ConsumerGatewayConfigurationError("consumer order is outside its binding")
            return order
        raise ConsumerNotFoundError("consumer order was not found")

    @staticmethod
    def _validate_receipt(
        binding: ConsumerAccountBinding,
        session: ConsumerSession,
        signal: ConsumerOrderSignal,
        receipt: IntentReceipt,
    ) -> None:
        if receipt.consumer_request_id != signal.consumer_request_id:
            raise ConsumerGatewayConfigurationError("consumer receipt has another request id")
        ConsumerGatewayV2._validate_receipt_trace(binding, session, receipt)
        trace = receipt.consumer_trace
        if (
            trace.strategy_id != signal.strategy_id
            or trace.strategy_order_id != signal.strategy_order_id
        ):
            raise ConsumerGatewayConfigurationError("consumer receipt has another strategy lineage")

    @staticmethod
    def _validate_receipt_trace(
        binding: ConsumerAccountBinding, session: ConsumerSession, receipt: IntentReceipt
    ) -> None:
        trace = receipt.consumer_trace
        if (
            trace.consumer_id != session.consumer_id
            or trace.consumer_account_id != binding.consumer_account_id
            or trace.account_id != binding.account_id
        ):
            raise ConsumerGatewayConfigurationError("consumer receipt is outside its binding")

    @staticmethod
    def _require_account_binding(
        snapshot: ConsumerBindingSnapshotV2,
        session: ConsumerSession,
        consumer_account_id: str,
    ) -> ConsumerAccountBinding:
        try:
            return snapshot.require_account(
                consumer_id=session.consumer_id,
                consumer_account_id=consumer_account_id,
            )
        except ConsumerBindingError as exc:
            raise ConsumerAuthorizationError("consumer account is not authorized") from exc

    def _require_snapshot_principal(
        self, principal: ConsumerPrincipal
    ) -> ConsumerBindingSnapshotV2:
        try:
            snapshot = self._binding_snapshot()
            snapshot.require_principal(principal)
        except ConsumerBindingError as exc:
            raise ConsumerAuthorizationError("consumer principal is not authorized") from exc
        except Exception as exc:
            raise ConsumerGatewayConfigurationError(
                "consumer binding snapshot is unavailable"
            ) from exc
        return snapshot
