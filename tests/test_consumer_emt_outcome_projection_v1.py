from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest
from emt_gateway.fastspi_v2_outcome import (
    FastSpiV2NativeOutcomeObserver,
    FastSpiV2OutcomeResolution,
)
from xuanling_tradecore.consumer.emt_outcome_projection_v1 import (
    EmtCommittedOutcomeProjectorV1,
    EmtConsumerProjectionErrorV1,
)
from xuanling_tradecore.domain import (
    BrokerEvent,
    BrokerEventKind,
    BrokerSourceKind,
    OmsOrderState,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import (
    BrokerOrderObservationV1,
    OmsFactKind,
    OmsJournalFact,
    OmsRecoverySeedV1,
)
from xuanling_tradecore.persistence import (
    AccountJournalBinding,
    MmapAdmissionEnvelopeV1,
    PreNativeRouteCommand,
)

from test_consumer_oms_operations_v2 import ConsumerOmsOperationsV2Tests


def _setup():
    fixture = ConsumerOmsOperationsV2Tests()
    fixture.setUp()
    fixture.account = replace(fixture.account, venue_id="EMT", adapter_instance_id="emt-1")
    receipt = fixture.operations.submit_signal(
        session=fixture.session,
        account_binding=fixture.account,
        strategy_binding_id="strategy-bind",
        signal=fixture._signal(),
    )
    book = fixture.operations._order_book
    order = book.order(receipt.oms_order_id)
    digest = sha256(b"route").hexdigest()
    facts = tuple(
        OmsJournalFact(
            fact_id=f"fact-{kind.value}",
            kind=kind,
            account_id="account-1",
            oms_order_id=receipt.oms_order_id,
            execution_id=receipt.execution_id,
            attributes={"kind": kind.value},
        )
        for kind in (
            OmsFactKind.ORDER_ATTRIBUTED,
            OmsFactKind.RESERVATION_CREATED,
            OmsFactKind.CALL_STARTED,
        )
    )
    command = PreNativeRouteCommand(
        account_id="account-1",
        command_id="command-1",
        execution_id=receipt.execution_id,
        oms_order_id=receipt.oms_order_id,
        client_order_ref="client-1",
        route_token_digest=digest,
        binding_manifest_hash=digest,
        intent_hash=order.order.intent_hash,
        decision_context_id=order.decision_context.decision_context_id,
        native_start_deadline_ns=100,
        fact_envelope=MmapAdmissionEnvelopeV1.build(
            account_id="account-1",
            facts=facts,
            maximum_fact_count=16,
            maximum_encoded_bytes=65536,
        ),
        recovery_seed=OmsRecoverySeedV1.build(order),
    )
    return book, command


def _event(command, *, kind=BrokerEventKind.ORDER, name="one", qty=None, trade_key=None):
    identity = VenueOrderIdentity("EMT", "emt-1", "scope-1", "client-1", "venue-1")
    return BrokerEvent(
        event_id=name,
        schema_version=1,
        source_key_schema_version=1,
        source_key=name,
        event_kind=kind,
        source_kind=BrokerSourceKind.CALLBACK,
        venue_id="EMT",
        adapter_instance_id="emt-1",
        account_id="account-1",
        trading_day=command.recovery_seed.order.order.trading_day,
        received_monotonic_ns=1,
        venue_order_identity=identity,
        execution_id=command.execution_id,
        trade_key=trade_key,
        filled_qty=qty,
    )


def _resolution(command, event, state, qty, keys=()):
    return FastSpiV2OutcomeResolution(
        binding=AccountJournalBinding("account-1", "EMT", "xuanling:journal:account-1:", 1),
        command=command,
        observation=BrokerOrderObservationV1(
            broker_event_id=event.event_id,
            source_key=event.source_key,
            order_state=state,
            cumulative_filled_quantity=qty,
            leaves_quantity=10 - qty,
            trade_keys=keys,
            venue_order_id="venue-1",
        ),
        transition_id="transition-1",
    )


def test_committed_callbacks_project_to_consumer_after_journal_commit():
    book, command = _setup()
    published, closed, stages = [], [], []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=lambda event: (stages.append("consumer"), published.append(event)),
        close_admission=closed.append,
    )

    class Journal:
        def record_callback_observed(self, *_args, **_kwargs):
            stages.append("callback_committed")

    state = {}
    observer = FastSpiV2NativeOutcomeObserver(
        journal=Journal(),
        resolve_outcome=lambda event: state[event.event_id],
        outcome_committed=projector,
    )
    first = _event(command)
    state[first.event_id] = _resolution(command, first, OmsOrderState.WORKING, 0)
    observer.on_broker_event(first)
    trade = _event(command, kind=BrokerEventKind.TRADE, name="trade", qty=3, trade_key="t1")
    state[trade.event_id] = _resolution(command, trade, OmsOrderState.PARTIALLY_FILLED, 3, ("t1",))
    observer.on_broker_event(trade)
    cancel = _event(command, name="cancel")
    state[cancel.event_id] = _resolution(command, cancel, OmsOrderState.CANCELED, 3, ("t1",))
    observer.on_broker_event(cancel)
    assert stages == ["callback_committed", "consumer"] * 3
    assert [e.order_state for e in published] == [
        OmsOrderState.WORKING,
        OmsOrderState.PARTIALLY_FILLED,
        OmsOrderState.CANCELED,
    ]
    assert book.order(command.oms_order_id).filled_quantity == 3
    assert closed == []


def test_committed_route_supplies_missing_execution_id_but_rejects_wrong_one():
    book, command = _setup()
    published, closed = [], []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=published.append,
        close_admission=closed.append,
    )
    event = replace(_event(command), execution_id=None)
    projector(event, _resolution(command, event, OmsOrderState.WORKING, 0))
    assert published[0].execution_id == command.execution_id
    bad = replace(_event(command, name="two"), execution_id="some-other-execution")
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(bad, _resolution(command, bad, OmsOrderState.WORKING, 0))
    assert closed == ["emt-consumer-projection-failed"]


def test_bad_scope_or_missing_trades_latches_without_wrong_state():
    book, command = _setup()
    closed, published = [], []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=published.append,
        close_admission=closed.append,
    )
    event = _event(command)
    bad = replace(
        event, venue_order_identity=replace(event.venue_order_identity, client_order_ref="foreign")
    )
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(bad, _resolution(command, bad, OmsOrderState.WORKING, 0))
    with pytest.raises(EmtConsumerProjectionErrorV1, match="latched"):
        projector(event, _resolution(command, event, OmsOrderState.WORKING, 0))
    assert closed == ["emt-consumer-projection-failed"]
    assert published == []
    assert book.order(command.oms_order_id).order.state is OmsOrderState.ACCEPTED


def test_cancel_error_is_not_a_cancelled_order():
    book, command = _setup()
    closed = []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=lambda _: None,
        close_admission=closed.append,
    )
    event = replace(
        _event(command), event_kind=BrokerEventKind.CANCEL_ERROR, cancel_request_id="cancel-1"
    )
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(event, _resolution(command, event, OmsOrderState.CANCELED, 0))
    assert book.order(command.oms_order_id).order.state is OmsOrderState.ACCEPTED


def test_callback_journal_failure_prevents_consumer_projection():
    book, command = _setup()
    published = []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=published.append,
        close_admission=lambda _: None,
    )
    event = _event(command)

    class BrokenJournal:
        def record_callback_observed(self, *_args, **_kwargs):
            raise OSError("injected journal failure")

    observer = FastSpiV2NativeOutcomeObserver(
        journal=BrokenJournal(),
        resolve_outcome=lambda _: _resolution(command, event, OmsOrderState.WORKING, 0),
        outcome_committed=projector,
    )
    with pytest.raises(OSError):
        observer.on_broker_event(event)
    assert published == []
    assert book.order(command.oms_order_id).order.state is OmsOrderState.ACCEPTED


def test_missing_trade_evidence_and_failed_publication_fail_closed():
    book, command = _setup()
    closed = []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=lambda _: None,
        close_admission=closed.append,
    )
    event = _event(command)
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(event, _resolution(command, event, OmsOrderState.CANCELED, 3, ("unknown-trade",)))
    assert book.order(command.oms_order_id).filled_quantity == 0
    assert closed == ["emt-consumer-projection-failed"]

    book, command = _setup()
    closed = []

    def broken_publish(_event):
        raise OSError("injected publication failure")

    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=broken_publish,
        close_admission=closed.append,
    )
    event = _event(command)
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(event, _resolution(command, event, OmsOrderState.WORKING, 0))
    assert book.order(command.oms_order_id).order.state is OmsOrderState.WORKING
    assert closed == ["emt-consumer-projection-failed"]
    with pytest.raises(EmtConsumerProjectionErrorV1, match="latched"):
        projector(event, _resolution(command, event, OmsOrderState.WORKING, 0))


def test_duplicate_trade_with_unverifiable_quantity_fails_closed():
    book, command = _setup()
    closed = []
    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=lambda _: None,
        close_admission=closed.append,
    )
    trade = _event(command, kind=BrokerEventKind.TRADE, name="trade", qty=3, trade_key="t1")
    projector(trade, _resolution(command, trade, OmsOrderState.PARTIALLY_FILLED, 3, ("t1",)))
    conflicting = replace(trade, filled_qty=4)
    with pytest.raises(EmtConsumerProjectionErrorV1):
        projector(
            conflicting,
            _resolution(command, conflicting, OmsOrderState.PARTIALLY_FILLED, 3, ("t1",)),
        )
    assert book.order(command.oms_order_id).filled_quantity == 3
    assert closed == ["emt-consumer-projection-failed"]


def test_failed_admission_closure_is_not_silently_reported_as_closed():
    book, command = _setup()

    def broken_close(_reason):
        raise OSError("injected admission failure")

    projector = EmtCommittedOutcomeProjectorV1(
        order_book=book,
        adapter_instance_id="emt-1",
        adapter_client_scope="scope-1",
        publish=lambda _: None,
        close_admission=broken_close,
    )
    event = _event(command)
    bad = replace(
        event, venue_order_identity=replace(event.venue_order_identity, client_order_ref="foreign")
    )
    with pytest.raises(EmtConsumerProjectionErrorV1, match="closure could not be confirmed"):
        projector(bad, _resolution(command, bad, OmsOrderState.WORKING, 0))
    with pytest.raises(EmtConsumerProjectionErrorV1, match="latched"):
        projector(event, _resolution(command, event, OmsOrderState.WORKING, 0))
