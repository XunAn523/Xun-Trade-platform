"""Journal-first M1 OrderBook implementation of Consumer gateway operations."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic_ns
from typing import Callable

from ..domain import (
    AttributionBinding,
    ConsumerAccountBinding,
    ConsumerAuditTrail,
    ConsumerEventKind,
    ConsumerOrderEvent,
    ConsumerStrategyBinding,
    ConsumerTrace,
    DecisionContext,
    OrderOrigin,
    RiskSnapshot,
    TradingDayEpoch,
)
from ..oms import (
    IntentAdmission,
    IntentAdmissionOutcome,
    OmsJournalCommit,
    OmsOrderEvent,
    OmsOrderIntent,
    OrderBook,
    OrderBookOrder,
    ReservationRequirement,
)
from ..oms.cold_intent import ColdIntentIndexError, ColdIntentResolver
from ..oms.order_book import OmsOrderBookError
from ..oms.retained_order_book import (
    ColdIntentLookupRequired,
    OmsHotCapacityFull,
    RetainedOrderBook,
)
from .base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerAuthorizationError,
    ConsumerEventSubscription,
    ConsumerGatewayError,
    ConsumerNotFoundError,
    ConsumerOrderSignal,
    ConsumerSession,
    IntentReceipt,
)
from .cold_admission_v2 import (
    ConsumerAdmissionCapacityV2,
    ConsumerColdLookupRequiredV2,
    admission_resolution,
)
from .gateway_v2 import ConsumerGatewayConfigurationError
from .projection_reads_v2 import ConsumerColdOrderReaderV2
from .request_trace_v2 import mark_request_stage
from .subscription_v2 import ConsumerSubscriptionLimitsV2, ConsumerSubscriptionRegistryV2


@dataclass(frozen=True)
class ConsumerOmsAdmissionContextV2:
    """Approved risk and attribution inputs required to admit one signal."""

    strategy_binding: ConsumerStrategyBinding
    attribution_binding: AttributionBinding
    risk_snapshot: RiskSnapshot
    decision_context: DecisionContext
    reservation_requirement: ReservationRequirement


class OrderBookConsumerOperationsV2:
    """Expose only Journal-committed M1 OMS state through the Consumer seam.

    It deliberately does not call an Adapter.  EMS later consumes the accepted
    execution from its normal Journal-backed dispatch lane, preserving the
    no-side-effect-before-fact invariant.
    """

    def __init__(
        self,
        *,
        order_book: OrderBook,
        admission_context_for: Callable[
            [ConsumerAccountBinding, str, ConsumerOrderSignal], ConsumerOmsAdmissionContextV2
        ],
        now_monotonic_ns: Callable[[], int] = monotonic_ns,
        snapshot_for: Callable[
            [ConsumerSession, ConsumerAccountBinding, str | None], ConsumerAccountOrderSnapshotV2
        ]
        | None = None,
        subscription_registry: ConsumerSubscriptionRegistryV2 | None = None,
        subscription_limits: ConsumerSubscriptionLimitsV2 | None = None,
        subscription_id_for: Callable[[ConsumerSession], str] | None = None,
        feature_flag_enabled: Callable[[str, str], bool] | None = None,
        feature_flag_for: Callable[[ConsumerOrderSignal], str] | None = None,
        on_execution_committed: Callable[[OrderBookOrder], None] | None = None,
        on_execution_commit_evidence: Callable[[OrderBookOrder, OmsJournalCommit], None]
        | None = None,
        on_execution_wakeup_failed: Callable[[], None] | None = None,
        on_order_event_committed: Callable[[ConsumerOrderEvent], None] | None = None,
        cancel_admission_allowed: Callable[[OrderBookOrder], bool] | None = None,
        on_cancel_committed: Callable[[OrderBookOrder, str, str], None] | None = None,
        on_cancel_commit_evidence: Callable[[OrderBookOrder, OmsOrderEvent, str, str], None]
        | None = None,
        on_cancel_wakeup_failed: Callable[[], None] | None = None,
        cold_resolver: ColdIntentResolver | None = None,
        cold_order_reader: ConsumerColdOrderReaderV2 | None = None,
    ) -> None:
        self._order_book = order_book
        if cold_resolver is not None and not isinstance(order_book, RetainedOrderBook):
            raise ConsumerGatewayConfigurationError("cold resolver requires retained OMS")
        self._cold_resolver = cold_resolver
        self._cold_order_reader = cold_order_reader
        self._admission_context_for = admission_context_for
        self._now_monotonic_ns = now_monotonic_ns
        self._snapshot_for = snapshot_for
        self._subscription_registry = subscription_registry
        self._subscription_limits = subscription_limits
        self._subscription_id_for = subscription_id_for or self._session_subscription_id
        self._feature_flag_enabled = feature_flag_enabled
        self._feature_flag_for = feature_flag_for or self._default_feature_flag
        # This is only a post-Journal wake-up into the normal EMS lane.  It
        # cannot call an Adapter; journal replay remains the authority when a
        # wake-up is delayed or fails.
        if on_execution_commit_evidence is not None and (
            on_execution_committed is not None or on_execution_wakeup_failed is None
        ):
            raise ConsumerGatewayConfigurationError(
                "committed execution evidence requires one wake-up and a failure fence"
            )
        if on_execution_commit_evidence is None and on_execution_wakeup_failed is not None:
            raise ConsumerGatewayConfigurationError(
                "execution failure fence has no evidence wake-up"
            )
        self._on_execution_committed = on_execution_committed
        self._on_execution_commit_evidence = on_execution_commit_evidence
        self._on_execution_wakeup_failed = on_execution_wakeup_failed
        self._on_order_event_committed = on_order_event_committed
        self._cancel_admission_allowed = cancel_admission_allowed
        if on_cancel_commit_evidence is not None and (
            on_cancel_committed is not None or on_cancel_wakeup_failed is None
        ):
            raise ConsumerGatewayConfigurationError(
                "committed cancel evidence requires one wake-up and a failure fence"
            )
        if on_cancel_commit_evidence is None and on_cancel_wakeup_failed is not None:
            raise ConsumerGatewayConfigurationError("cancel failure fence has no evidence wake-up")
        self._on_cancel_committed = on_cancel_committed
        self._on_cancel_commit_evidence = on_cancel_commit_evidence
        self._on_cancel_wakeup_failed = on_cancel_wakeup_failed

    def submit_signal(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        strategy_binding_id: str,
        signal: ConsumerOrderSignal,
    ) -> IntentReceipt:
        mark_request_stage("submit_context_started_ns")
        if self._feature_flag_enabled is not None and not self._feature_flag_enabled(
            account_binding.account_id, self._feature_flag_for(signal)
        ):
            raise ConsumerGatewayError("account capability feature flag rejects new signal")
        context = self._admission_context_for(account_binding, strategy_binding_id, signal)
        if self._feature_flag_enabled is not None and (
            context.decision_context.decision_bundle_hash is None
            or context.decision_context.decision_bundle_epoch is None
        ):
            raise ConsumerGatewayError(
                "feature-flagged admission requires decision bundle evidence"
            )
        if context.strategy_binding.binding_id != strategy_binding_id:
            raise ConsumerNotFoundError("consumer strategy is unavailable")
        trace = self._trace(account_binding, context, signal)
        intent = OmsOrderIntent(
            consumer_trace=trace,
            trading_day=signal.trading_day,
            symbol=signal.symbol,
            market=signal.market,
            side=signal.side,
            price_type=signal.price_type,
            business_type=signal.business_type,
            quantity=signal.quantity,
            protection_price_ticks=signal.protection_price_ticks,
            limit_price_ticks=signal.limit_price_ticks,
            execution_parameters=signal.execution_parameters,
        )
        try:
            mark_request_stage("submit_admission_started_ns")
            admission = self._admit_intent(intent, account_binding, context)
            mark_request_stage("submit_admission_finished_ns")
        except ColdIntentLookupRequired as exc:
            if self._cold_resolver is None:
                raise ConsumerGatewayConfigurationError(
                    "cold admission resolver is unavailable"
                ) from exc
            raise ConsumerColdLookupRequiredV2(exc.key, self._cold_resolver) from exc
        except ColdIntentIndexError as exc:
            raise ConsumerGatewayConfigurationError("cold index evidence is unavailable") from exc
        except OmsHotCapacityFull as exc:
            raise ConsumerAdmissionCapacityV2("OMS live state capacity reached") from exc
        except OmsOrderBookError as exc:
            raise ConsumerGatewayConfigurationError(
                "OMS admission evidence is unavailable"
            ) from exc
        return self._admission_receipt(admission, signal)

    def _admit_intent(
        self,
        intent: OmsOrderIntent,
        account_binding: ConsumerAccountBinding,
        context: ConsumerOmsAdmissionContextV2,
    ) -> IntentAdmission:
        if isinstance(self._order_book, RetainedOrderBook):
            return self._order_book.admit(
                intent=intent,
                account_binding=account_binding,
                strategy_binding=context.strategy_binding,
                attribution_binding=context.attribution_binding,
                risk_snapshot=context.risk_snapshot,
                decision_context=context.decision_context,
                reservation_requirement=context.reservation_requirement,
                now_monotonic_ns=self._now_monotonic_ns(),
                cold_resolution=admission_resolution(self._cold_resolver)
                if self._cold_resolver is not None
                else None,
            )
        return self._order_book.admit(
            intent=intent,
            account_binding=account_binding,
            strategy_binding=context.strategy_binding,
            attribution_binding=context.attribution_binding,
            risk_snapshot=context.risk_snapshot,
            decision_context=context.decision_context,
            reservation_requirement=context.reservation_requirement,
            now_monotonic_ns=self._now_monotonic_ns(),
        )

    def _admission_receipt(
        self, admission: IntentAdmission, signal: ConsumerOrderSignal
    ) -> IntentReceipt:
        if admission.outcome is IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT:
            raise ConsumerNotFoundError("consumer signal conflicts with durable strategy order")
        event = (
            admission.event
            or admission.replayed_event
            or self._latest_event(admission.order.order.oms_order_id)
        )
        if admission.outcome is IntentAdmissionOutcome.ACCEPTED:
            if self._on_order_event_committed is not None:
                self._notify_order_event_committed(
                    self._project_order(
                        event,
                        admission.order.order.consumer_trace,
                        admission.order.order.trading_day,
                    )
                )
            if self._on_execution_commit_evidence is not None:
                commit = admission.journal_commit
                if commit is None:
                    raise ConsumerGatewayConfigurationError(
                        "accepted execution lacks journal receipt"
                    )
                self._notify_execution_commit_evidence(admission.order, commit)
            else:
                self._notify_execution_committed(admission.order)
        return IntentReceipt(
            signal.consumer_request_id,
            event.event_id,
            admission.order.order.oms_order_id,
            admission.order.execution.execution_id,
            event.journal_id,
            event.journal_lsn,
            admission.order.order.consumer_trace,
        )

    def cancel_order(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        consumer_request_id: str,
        oms_order_id: str,
        reason: str,
    ) -> IntentReceipt:
        order = self._require_bound_order(account_binding, oms_order_id)
        if self._cancel_admission_allowed is not None and not self._cancel_admission_allowed(order):
            raise ConsumerAuthorizationError("consumer cancel is not authorized for this order identity")
        requested_event = self._order_book.request_cancel(
            oms_order_id=oms_order_id,
            cancel_request_id=consumer_request_id,
            reason=reason,
        )
        event = requested_event or self._latest_event(oms_order_id)
        if requested_event is not None:
            if self._on_order_event_committed is not None:
                self._notify_order_event_committed(
                    self._project_order(event, order.order.consumer_trace, order.order.trading_day)
                )
            if self._on_cancel_commit_evidence is not None:
                self._notify_cancel_commit_evidence(
                    order, requested_event, consumer_request_id, reason
                )
            else:
                self._notify_cancel_committed(order, consumer_request_id, reason)
        return IntentReceipt(
            consumer_request_id,
            event.event_id,
            order.order.oms_order_id,
            order.execution.execution_id,
            event.journal_id,
            event.journal_lsn,
            order.order.consumer_trace,
        )

    def get_order(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> ConsumerOrderEvent:
        try:
            order = self._require_bound_order(account_binding, oms_order_id)
            return self._project_order(
                self._latest_event(oms_order_id),
                order.order.consumer_trace,
                order.order.trading_day,
            )
        except ConsumerNotFoundError:
            if self._cold_order_reader is None:
                raise
        # The reader requires a reviewed order/audit read context. Cancel
        # authorization must fail closed rather than touch cold SQL on WRITE.
        return self._cold_order_reader.get_order(
            account_binding=account_binding, oms_order_id=oms_order_id
        )

    def get_order_audit_trail(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]:
        if self._cold_order_reader is not None:
            # Also use Journal-backed verification for hot orders: the retained
            # account event window is not a complete historical audit sequence.
            return self._cold_order_reader.get_order_audit_trail(
                account_binding=account_binding, oms_order_id=oms_order_id
            )
        order = self._require_bound_order(account_binding, oms_order_id)
        return tuple(
            ConsumerAuditTrail(
                audit_id=f"audit:{event.event_id}",
                consumer_id=order.order.consumer_trace.consumer_id,
                account_id=account_binding.account_id,
                oms_order_id=oms_order_id,
                execution_id=event.execution_id,
                journal_id=event.journal_id,
                journal_lsn=event.journal_lsn,
                action=event.kind.value,
            )
            for event in self._order_book.events(account_binding.account_id)
            if event.oms_order_id == oms_order_id
        )

    def get_account_order_snapshot(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        reset_context: str | None,
    ) -> ConsumerAccountOrderSnapshotV2:
        if self._snapshot_for is None:
            raise ConsumerGatewayError("consumer snapshot projection is unavailable")
        return self._snapshot_for(session, account_binding, reset_context)

    def subscribe_order_events(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        cursor_token: str,
    ) -> ConsumerEventSubscription:
        del cursor_token
        if self._subscription_registry is None or self._subscription_limits is None:
            raise ConsumerGatewayError("consumer live subscription is unavailable")
        return self._subscription_registry.register(
            subscription_id=self._subscription_id_for(session),
            binding=account_binding,
            limits=self._subscription_limits,
        )

    def _require_bound_order(
        self, binding: ConsumerAccountBinding, oms_order_id: str
    ) -> OrderBookOrder:
        try:
            order = self._order_book.order(oms_order_id)
        except Exception as exc:
            raise ConsumerNotFoundError("consumer order was not found") from exc
        if order.order.account_id != binding.account_id:
            raise ConsumerNotFoundError("consumer order was not found")
        return order

    @staticmethod
    def _default_feature_flag(signal: ConsumerOrderSignal) -> str:
        return signal.business_type.lower()

    def _latest_event(self, oms_order_id: str) -> OmsOrderEvent:
        try:
            return self._order_book.last_order_event(oms_order_id)
        except (StopIteration, Exception) as exc:
            raise ConsumerNotFoundError("consumer order evidence was not found") from exc

    def _notify_execution_committed(self, order: OrderBookOrder) -> None:
        notifier = self._on_execution_committed
        if notifier is None:
            return
        try:
            notifier(order)
        except Exception:
            # The accepted execution remains Journal-authoritative; EMS must
            # replay it if this advisory wake-up is lost.
            return

    def _notify_execution_commit_evidence(
        self, order: OrderBookOrder, commit: OmsJournalCommit
    ) -> None:
        notifier = self._on_execution_commit_evidence
        fence = self._on_execution_wakeup_failed
        assert notifier is not None and fence is not None
        try:
            # This is only an EMS wake-up: the receiver must verify the
            # receipt against the original journal before any native call.
            notifier(order, commit)
        except Exception:
            try:
                fence()
            except Exception as exc:
                # The order is already committed. A transport error here has
                # uncertain outcome; clients must keep the same order key.
                raise ConsumerGatewayConfigurationError(
                    "execution wake-up failed and admission closure is unconfirmed"
                ) from exc

    def _notify_order_event_committed(self, event: ConsumerOrderEvent) -> None:
        notifier = self._on_order_event_committed
        if notifier is None:
            return
        try:
            notifier(event)
        except Exception:
            # The read-side notification is advisory. Snapshot publication
            # fails closed until the isolated projector has a verified view.
            return

    def _notify_cancel_committed(
        self, order: OrderBookOrder, consumer_request_id: str, reason: str
    ) -> None:
        notifier = self._on_cancel_committed
        if notifier is None:
            return
        try:
            notifier(order, consumer_request_id, reason)
        except Exception:
            # A wake-up failure does not establish a remote cancellation.
            return

    def _notify_cancel_commit_evidence(
        self, order: OrderBookOrder, event: OmsOrderEvent, request_id: str, reason: str
    ) -> None:
        notifier = self._on_cancel_commit_evidence
        fence = self._on_cancel_wakeup_failed
        assert notifier is not None and fence is not None
        try:
            notifier(order, event, request_id, reason)
        except Exception:
            try:
                fence()
            except Exception as exc:
                raise ConsumerGatewayConfigurationError(
                    "cancel wake-up failed and admission closure is unconfirmed"
                ) from exc

    @staticmethod
    def _trace(
        binding: ConsumerAccountBinding,
        context: ConsumerOmsAdmissionContextV2,
        signal: ConsumerOrderSignal,
    ) -> ConsumerTrace:
        strategy = context.strategy_binding
        attribution = context.attribution_binding
        if (
            strategy.consumer_id != binding.consumer_id
            or strategy.consumer_account_id != binding.consumer_account_id
            or attribution.consumer_account_binding_id != binding.binding_id
            or attribution.consumer_strategy_binding_id != strategy.binding_id
        ):
            raise ConsumerNotFoundError("consumer attribution is unavailable")
        return ConsumerTrace(
            binding.consumer_id,
            binding.consumer_account_id,
            binding.account_id,
            signal.strategy_id,
            signal.strategy_order_id,
            attribution.attribution_binding_id,
            attribution.binding_version,
            OrderOrigin.STRATEGY,
        )

    @staticmethod
    def _project_order(
        event: OmsOrderEvent, trace: ConsumerTrace, trading_day: TradingDayEpoch
    ) -> ConsumerOrderEvent:
        return ConsumerOrderEvent(
            consumer_event_id=f"order:{event.event_id}",
            event_kind=ConsumerEventKind.ORDER,
            schema_version=2,
            account_id=event.account_id,
            trading_day=trading_day,
            journal_id=event.journal_id,
            journal_lsn=event.journal_lsn,
            consumer_trace=trace,
            oms_order_id=event.oms_order_id,
            execution_id=event.execution_id,
            order_state=event.order_state,
            order_revision=event.order_revision,
        )

    @staticmethod
    def _session_subscription_id(session: ConsumerSession) -> str:
        return session.session_id
