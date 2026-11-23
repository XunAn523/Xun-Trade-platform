"""Project already-committed, route-proven EMT outcomes into the OMS.

This is an opt-in composition building block, not a running trading service.
Call it only as the post-commit hook of ``FastSpiV2NativeOutcomeObserver``;
it must never be called directly from an unverified SDK callback.
"""

from __future__ import annotations

from dataclasses import replace
from threading import Lock
from typing import Callable

from emt_gateway.fastspi_v2_outcome import FastSpiV2OutcomeResolution

from ..domain import (
    BrokerEvent,
    BrokerEventKind,
    ConsumerEventKind,
    ConsumerOrderEvent,
    OmsOrderState,
)
from ..oms import OrderBook
from ..persistence import AccountJournalBinding, PreNativeRouteCommand


class EmtConsumerProjectionErrorV1(RuntimeError):
    """A committed broker outcome cannot safely advance this Consumer OMS."""


class EmtCommittedOutcomeProjectorV1:
    """Serialized, fail-closed projection of an owned M1 callback outcome.

    A failure latches this instance and invokes ``close_admission``. Recovery
    must rebuild the OMS and replay authoritative outcomes before a new
    projector is installed; neither an in-memory queue nor this hook is a
    recovery certificate. Cancel-error callbacks are not order cancellation.
    """

    def __init__(
        self,
        *,
        order_book: OrderBook,
        adapter_instance_id: str,
        adapter_client_scope: str,
        publish: Callable[[ConsumerOrderEvent], None],
        close_admission: Callable[[str], None],
    ) -> None:
        if not adapter_instance_id or not adapter_client_scope:
            raise ValueError("EMT projection requires an exact adapter scope")
        self._book = order_book
        self._adapter = adapter_instance_id
        self._scope = adapter_client_scope
        self._publish = publish
        self._close_admission = close_admission
        self._lock = Lock()
        self._failed = False

    @property
    def order_book(self) -> OrderBook:
        return self._book

    @property
    def adapter_scope(self) -> tuple[str, str]:
        return self._adapter, self._scope

    def __call__(self, event: BrokerEvent, resolution: FastSpiV2OutcomeResolution) -> None:
        with self._lock:
            if self._failed:
                raise EmtConsumerProjectionErrorV1("EMT Consumer projection is latched")
            try:
                self._project(event, resolution)
            except Exception as exc:
                self._failed = True
                try:
                    self._close_admission("emt-consumer-projection-failed")
                except Exception as close_error:
                    raise EmtConsumerProjectionErrorV1(
                        "EMT projection failed and admission closure could not be confirmed"
                    ) from close_error
                raise EmtConsumerProjectionErrorV1(
                    "committed EMT outcome requires reconciliation"
                ) from exc

    def _project(self, event: BrokerEvent, resolution: FastSpiV2OutcomeResolution) -> None:
        command = resolution.command
        observation = resolution.observation
        identity = event.venue_order_identity
        if (
            not isinstance(command, PreNativeRouteCommand)
            or not isinstance(resolution.binding, AccountJournalBinding)
            or identity is None
        ):
            raise ValueError("owned PRE_NATIVE route and native identity are required")
        # EMT callbacks normally echo the client id, not TradeCore's internal
        # execution id. The already-resolved, committed route supplies it; an
        # explicitly supplied conflicting execution id still fails below.
        if event.execution_id is None:
            event = replace(event, execution_id=command.execution_id)
        if (
            event.event_kind not in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}
            or event.venue_id != "EMT"
            or resolution.binding.venue_id != "EMT"
            or event.adapter_instance_id != self._adapter
            or identity.adapter_client_scope != self._scope
            or identity.client_order_ref != command.client_order_ref
            or event.account_id != command.account_id
            or resolution.binding.account_id != command.account_id
            or event.execution_id != command.execution_id
            or observation.broker_event_id != event.event_id
            or observation.source_key != event.source_key
            or not identity.venue_order_id
            or observation.venue_order_id != identity.venue_order_id
        ):
            raise ValueError("EMT outcome route identity is not proven")
        current = self._book.order(command.oms_order_id)
        initial = command.recovery_seed.order
        if (
            current.order.account_id != command.account_id
            or current.order.trading_day != event.trading_day
            or current.order.intent_hash != initial.order.intent_hash
            or current.execution != initial.execution
        ):
            raise ValueError("EMT outcome differs from the committed OMS admission")
        observation.validate_total_quantity(current.execution.quantity)
        new_event = None
        if event.event_kind is BrokerEventKind.TRADE:
            if event.trade_key is None or not event.filled_qty:
                raise ValueError("trade quantity and identity are required")
            # The live book retains trade keys but not each trade's quantity.
            # Therefore a repeated key alone cannot prove this payload is an
            # identical replay. Require authoritative reconciliation instead
            # of silently accepting a conflicting duplicate quantity.
            if event.trade_key in current.trade_keys:
                raise ValueError("duplicate trade requires authoritative reconciliation")
            expected_qty = current.filled_quantity + event.filled_qty
            expected_keys = current.trade_keys | {event.trade_key}
            if (
                observation.cumulative_filled_quantity != expected_qty
                or frozenset(observation.trade_keys) != expected_keys
                or observation.order_state
                is not (
                    OmsOrderState.FILLED
                    if expected_qty == current.execution.quantity
                    else OmsOrderState.PARTIALLY_FILLED
                )
            ):
                raise ValueError("trade observation is missing or out of sequence")
            new_event = self._book.apply_trade(
                oms_order_id=command.oms_order_id,
                trade_key=event.trade_key,
                quantity=event.filled_qty,
            )
        else:
            if (
                observation.cumulative_filled_quantity != current.filled_quantity
                or frozenset(observation.trade_keys) != current.trade_keys
            ):
                raise ValueError("order observation is missing committed trades")
            target = observation.order_state
            if target is OmsOrderState.WORKING:
                if current.order.state is OmsOrderState.ACCEPTED:
                    new_event = self._book.mark_working(oms_order_id=command.oms_order_id)
                elif current.order.state is not OmsOrderState.WORKING:
                    raise ValueError("working observation would regress OMS state")
            elif target is OmsOrderState.CANCELED:
                new_event = self._book.cancel(oms_order_id=command.oms_order_id)
            elif target is OmsOrderState.REJECTED:
                new_event = self._book.reject(oms_order_id=command.oms_order_id)
            elif target not in {OmsOrderState.PARTIALLY_FILLED, OmsOrderState.FILLED}:
                raise ValueError("unsupported observed order state")
        current = self._book.order(command.oms_order_id)
        if (
            current.filled_quantity != observation.cumulative_filled_quantity
            or current.leaves_quantity != observation.leaves_quantity
            or current.order.state is not observation.order_state
        ):
            raise ValueError("projected OMS state does not match committed observation")
        if new_event is not None:
            self._publish(
                ConsumerOrderEvent(
                    consumer_event_id=f"order:{new_event.event_id}",
                    event_kind=ConsumerEventKind.ORDER,
                    schema_version=2,
                    account_id=new_event.account_id,
                    trading_day=current.order.trading_day,
                    journal_id=new_event.journal_id,
                    journal_lsn=new_event.journal_lsn,
                    consumer_trace=current.order.consumer_trace,
                    oms_order_id=new_event.oms_order_id,
                    execution_id=new_event.execution_id,
                    order_state=new_event.order_state,
                    order_revision=new_event.order_revision,
                )
            )
