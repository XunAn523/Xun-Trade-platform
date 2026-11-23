from __future__ import annotations

import json
import socket
import time
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread, current_thread
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer import ConsumerWireStreamDecoderV2
from xuanling_tradecore.consumer.cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from xuanling_tradecore.consumer.oms_operations_v2 import OrderBookConsumerOperationsV2
from xuanling_tradecore.consumer.projection_reads_v2 import (
    PROJECTION_READ_OPERATIONS,
    ConsumerProjectionReadErrorV2,
)
from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayServiceV2
from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.consumer.wire_v2 import (
    MessageKind,
    encode_frame,
)
from xuanling_tradecore.domain import AttributionBinding
from xuanling_tradecore.oms import ReservationRequirement
from xuanling_tradecore.oms.retained_order_book import RetainedOrderBook
from xuanling_tradecore.oms.segmented_fact_journal import SegmentedOmsFactJournal
from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex
from xuanling_tradecore.persistence.order_projection_store import OrderProjectionStore
from xuanling_tradecore.read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from xuanling_tradecore.read_model.incremental_order_projector import IncrementalOrderProjector

import test_consumer_reactor_v2 as fixtures
from test_order_projection_store import retry
from test_retained_order_book import retire
from test_tradecore_oms import _decision_context, _intent, _risk_snapshot

pytestmark = pytest.mark.skipif(
    not hasattr(socket, "SO_PEERCRED"), reason="requires Linux SO_PEERCRED"
)


def frame(operation, payload, name="read"):
    return encode_frame(
        MessageKind.REQUEST,
        91,
        {
            "message_type": "request",
            "schema_version": 2,
            "request_id": name,
            "operation": operation,
            "payload": payload,
        },
    )


def receive(peer):
    decoder = ConsumerWireStreamDecoderV2()
    while True:
        chunk = peer.recv(65536)
        assert chunk, "closed before complete response"
        result = decoder.feed(chunk)
        if result:
            return result[0].payload


@pytest.fixture
def live():
    fixture = fixtures.ConsumerReactorV2Tests()
    fixture.setUp()
    root = Path(fixture.directory.name)
    gateway = fixture.service._listener._gateway
    snapshot = gateway._binding_snapshot()
    account, strategy = snapshot.accounts[0], snapshot.strategies[0]
    journal = SegmentedOmsFactJournal(
        root=root / "segments", journal_id="live-stream", segment_frames=8
    )
    cold = ColdIntentIndex(
        root=root / "cold", journal_id=journal.journal_id, initialize=True, filter_bytes=64
    )
    book = RetainedOrderBook(journal=journal, cold_index=cold)
    attribution = AttributionBinding(
        "attribution",
        account.binding_id,
        strategy.binding_id,
        account.consumer_id,
        account.consumer_account_id,
        strategy.strategy_id,
        account.binding_version,
    )
    orders = []
    for n in range(6):
        intent = _intent(strategy_order_id=str(n))
        intent = replace(
            intent,
            consumer_trace=replace(
                intent.consumer_trace,
                consumer_id=account.consumer_id,
                consumer_account_id=account.consumer_account_id,
                account_id=account.account_id,
                strategy_id=strategy.strategy_id,
                attribution_binding_id=attribution.attribution_binding_id,
                binding_version=account.binding_version,
            ),
        )
        result = retry(
            lambda: book.admit(
                intent=intent,
                account_binding=account,
                strategy_binding=strategy,
                attribution_binding=attribution,
                risk_snapshot=replace(
                    _risk_snapshot(cash_minor=10**9), account_id=account.account_id
                ),
                decision_context=replace(_decision_context(), account_id=account.account_id),
                reservation_requirement=ReservationRequirement(intent.symbol, cash_minor=1000),
                now_monotonic_ns=20,
            )
        )
        orders.append(result.order.order.oms_order_id)
    retry(lambda: book.reject(oms_order_id=orders[0]))
    retire(book, cold, orders[0])
    assert book.order_count(account.account_id) == 5
    projector = IncrementalOrderProjector(
        journal=journal, root=root / "projection", initialize=True
    )
    stream = [CursorStreamRefV2(journal.journal_id, 1)]
    reader = ConsumerOrderProjectionReaderV2(
        root=root / "projection",
        journal=journal,
        stream=stream[0],
        current_stream=lambda: stream[0],
        health_for=projector.health,
    )
    clock = [10]
    replay = ConsumerCursorReplayServiceV2(
        key_ring_for=lambda _: CursorKeyRingV2(current=CursorSigningKeyV2("live-test", b"l" * 32)),
        registry_for=lambda _: CursorStreamRegistryV2(stream[0], (stream[0],)),
        event_reader=reader,
        partition_reader=reader,
        cursor_ttl_ns=1000,
        wall_clock_ns=lambda: clock[0],
    )
    gateway._operations = OrderBookConsumerOperationsV2(
        order_book=book, admission_context_for=lambda *_: None, cold_order_reader=reader
    )
    gateway._event_replay = replay
    gateway._projection_read_delivery_guard = reader.validate_delivery
    old = fixture.service
    old._listener._parallel_read_operations |= PROJECTION_READ_OPERATIONS
    service = ConsumerUnixServiceV2(
        old._listener,
        request_reactor=True,
        deferred_read_workers=1,
        deferred_read_capacity=1,
        managed_components=(*old._managed_components, projector),
    )
    fixture.service = service
    service.set_request_trace(fixture.trace)
    fixture.runner = Thread(target=service.serve_forever)
    service.start()
    fixture.runner.start()
    deadline = time.monotonic() + 5
    while projector.health().projected_lsn != journal.read_bound().committed_lsn:
        assert time.monotonic() < deadline
        time.sleep(0.005)
    env = SimpleNamespace(
        fixture=fixture,
        service=service,
        gateway=gateway,
        reader=reader,
        journal=journal,
        book=book,
        orders=orders,
        account=account,
        stream=stream,
        clock=clock,
        projector=projector,
    )
    try:
        yield env
    finally:
        service.stop()
        fixture.runner.join(5)
        projector.close()
        cold.close()
        journal.close()
        fixture.tearDown()


def call(env, operation, payload):
    peer = env.fixture.connect()
    peer.sendall(frame(operation, payload))
    return receive(peer)


def test_real_cold_order_and_signed_partition_poll_use_projected_committed_segments(live):
    before = live.journal.read_bound()
    original = OrderProjectionStore.lookup
    threads = []

    def lookup(*args, **kwargs):
        threads.append(current_thread().name)
        return original(*args, **kwargs)

    with patch.object(OrderProjectionStore, "lookup", autospec=True, side_effect=lookup):
        # Capture thread ownership without replacing the real implementation.
        response = call(live, "GetOrder", {"oms_order_id": live.orders[0]})
        assert response["status"] == "COMPLETED"
        assert response["payload"]["order"]["payload"]["oms_order_id"] == live.orders[0]
        assert threads == ["consumer-cold-read-0"]
    plan = call(
        live,
        "CreateReplayPartitions",
        {"consumer_account_id": live.account.consumer_account_id, "partition_count": 3},
    )["payload"]
    assert plan["event_count"] == 7 and len(plan["partitions"]) == 3
    seen = []
    for part in plan["partitions"]:
        token = part["cursor"]
        count = 0
        while True:
            page = call(live, "PollEvents", {"cursor": token, "limit": 1})["payload"]
            assert page["high_watermark_position"] == part["high_watermark_position"]
            seen.extend(e["position"] for e in page["events"])
            count += len(page["events"])
            token = page["next_cursor"]
            if not page["has_more"]:
                break
        assert count == part["event_count"]
    assert len(seen) == len(set(seen)) == 7
    assert live.journal.read_bound() == before
    live.service.stop()
    live.fixture.runner.join(5)
    live.fixture.trace.close()
    records = [json.loads(line) for line in live.fixture.trace_path.read_text().splitlines()]
    for record in records:
        assert (
            record["deferred_enqueued_ns"]
            <= record["deferred_worker_started_ns"]
            <= record["deferred_delivery_ns"]
            <= record["send_finished_ns"]
        )


def test_slow_cold_query_leaves_hot_query_responsive_and_enforces_read_capacity(live):
    entered, release = Event(), Event()
    original = OrderProjectionStore.lookup

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    try:
        with patch.object(OrderProjectionStore, "lookup", side_effect=slow, autospec=True):
            peer = live.fixture.connect()
            peer.sendall(frame("GetOrder", {"oms_order_id": live.orders[0]}))
            assert entered.wait(2)
            excess = call(live, "GetOrder", {"oms_order_id": live.orders[0]})
            assert excess["code"] == "RATE_LIMITED" and excess["detail"] == "READ_BACKPRESSURE"
            hot = live.fixture.connect()
            hot.sendall(frame("ListAccounts", {}, "hot"))
            assert receive(hot)["status"] == "COMPLETED"
            assert not release.is_set()
            release.set()
            assert receive(peer)["status"] == "COMPLETED"
    finally:
        release.set()


@pytest.mark.parametrize("change", ["epoch", "generation", "expiry", "failed"])
def test_changed_delivery_authority_does_not_disclose_completed_event_page(live, change):
    token = live.gateway._event_replay.issue_begin_cursor(
        binding=live.account, authorization_epoch=1
    )
    entered, release = Event(), Event()
    original = live.reader.read_events

    def slow(**kwargs):
        result = original(**kwargs)
        entered.set()
        assert release.wait(3)
        return result

    before = live.journal.read_bound()
    try:
        with patch.object(live.reader, "read_events", side_effect=slow):
            peer = live.fixture.connect()
            peer.sendall(frame("PollEvents", {"cursor": token, "limit": 2}))
            assert entered.wait(2)
            if change == "epoch":
                snapshot = live.gateway._binding_snapshot()
                live.gateway._binding_snapshot = lambda: replace(snapshot, authorization_epoch=2)
            elif change == "generation":
                live.stream[0] = replace(live.stream[0], stream_generation=2)
            elif change == "expiry":
                live.clock[0] = 1010
            else:

                def failed_guard():
                    raise ConsumerProjectionReadErrorV2("failed")

                live.gateway._projection_read_delivery_guard = failed_guard
            release.set()
            if change == "epoch":
                assert peer.recv(65536) == b""
            else:
                response = receive(peer)
                assert response["message_type"] == "error" and "payload" not in response
                if change == "expiry":
                    assert response["code"] == "CURSOR_EXPIRED"
    finally:
        release.set()
    assert live.journal.read_bound() == before


def test_non_read_dispatch_cannot_fall_through_to_cold_storage(live):
    before = live.journal.read_bound()
    with patch.object(OrderProjectionStore, "lookup", side_effect=AssertionError("SQL on WRITE")):
        response = call(live, "CancelOrder", {"oms_order_id": live.orders[0], "reason": "test"})
        assert response["code"] == "INTERNAL_ERROR" and response["retryable"]
    assert live.journal.read_bound() == before


def test_real_cold_audit_replays_on_read_worker_and_preserves_public_contract(live):
    from xuanling_tradecore.oms.journal_audit import verify_order_audit

    before = live.journal.read_bound()
    threads = []

    def verify(*args, **kwargs):
        threads.append(current_thread().name)
        return verify_order_audit(*args, **kwargs)

    with patch(
        "xuanling_tradecore.read_model.consumer_order_reader_v2.verify_order_audit",
        side_effect=verify,
    ):
        result = call(live, "GetOrderAuditTrail", {"oms_order_id": live.orders[0]})
        assert result["status"] == "COMPLETED"
        trail = result["payload"]["audit_trail"]
        assert len(trail) == 2 and threads == ["consumer-cold-read-0"]
        assert all(row["oms_order_id"] == live.orders[0] for row in trail)
    assert live.journal.read_bound() == before


def test_audit_physical_failure_returns_stable_error_without_partial_audit(live):
    with patch(
        "xuanling_tradecore.read_model.consumer_order_reader_v2.verify_order_audit",
        side_effect=OSError("private Journal path and credentials"),
    ):
        result = call(live, "GetOrderAuditTrail", {"oms_order_id": live.orders[0]})
        assert result["code"] == "AUDIT_EVIDENCE_UNAVAILABLE" and result["retryable"]
        assert "payload" not in result and "credentials" not in str(result)


@pytest.mark.parametrize("change", ["epoch", "generation", "health"])
def test_slow_audit_isolated_and_late_authority_loss_prevents_delivery(live, change):
    from xuanling_tradecore.oms.journal_audit import verify_order_audit

    entered, release = Event(), Event()

    def slow(*args, **kwargs):
        result = verify_order_audit(*args, **kwargs)
        entered.set()
        assert release.wait(3)
        return result

    try:
        with patch(
            "xuanling_tradecore.read_model.consumer_order_reader_v2.verify_order_audit",
            side_effect=slow,
        ):
            peer = live.fixture.connect()
            peer.sendall(frame("GetOrderAuditTrail", {"oms_order_id": live.orders[0]}))
            assert entered.wait(2)
            hot = call(live, "ListAccounts", {})
            assert hot["status"] == "COMPLETED" and not release.is_set()
            if change == "epoch":
                snapshot = live.gateway._binding_snapshot()
                live.gateway._binding_snapshot = lambda: replace(snapshot, authorization_epoch=2)
            elif change == "generation":
                live.stream[0] = replace(live.stream[0], stream_generation=2)
            else:

                def failed():
                    raise ConsumerProjectionReadErrorV2("failed")

                live.gateway._projection_read_delivery_guard = failed
            release.set()
            if change == "epoch":
                assert peer.recv(65536) == b""
            else:
                result = receive(peer)
                assert result["code"] == "AUDIT_EVIDENCE_UNAVAILABLE" and "payload" not in result
    finally:
        release.set()
