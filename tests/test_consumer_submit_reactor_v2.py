import socket
from threading import Event, Thread, current_thread
from time import monotonic, sleep
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.reactor_budget import FixedReactorBudgets
from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2
from xuanling_tradecore.consumer.wire_v2 import MessageKind, decode_frame, encode_frame

import test_consumer_cold_reactor_v2 as cold_fixtures
import test_consumer_reactor_v2 as fixtures

pytestmark = pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="real Linux peer")


@pytest.fixture
def runtime(request):
    f = fixtures.ConsumerReactorV2Tests()
    f.setUp()
    original = f.service
    f.service = ConsumerUnixServiceV2(
        original._listener,
        request_reactor=True,
        pending_capacity=4,
        submit_execution_capacity=2,
        managed_components=original._managed_components,
    )
    f.service._listener._max_requests_per_connection = 1
    if getattr(request, "param", None) == "tiny":
        f.service.set_reactor_turn_budgets(
            FixedReactorBudgets(
                accepts=1,
                reads=1,
                writes=1,
                ready=1,
                dispatches=1,
                read_bytes=64,
                write_bytes=64,
            )
        )
    f.errors, f.gates = [], []

    def serve():
        try:
            f.service.serve_forever()
        except BaseException as exc:
            f.errors.append(exc)

    f.runner = Thread(target=serve, name="test-submit-transport-owner")
    f.service.start()
    f.runner.start()
    try:
        yield f
    finally:
        for event in f.gates:
            event.set()
        lane = f.service._submit_lane
        if (
            lane is not None
            and not lane._thread.is_alive()
            and not f.service.submission_shutdown_verified
            and f.service._managed_components_started
        ):
            # Test-only resource cleanup after verified thread exit. Do not
            # change the failed drain flag or publish lifecycle success.
            for component in reversed(f.service._managed_components):
                component.close()
            f.service._managed_components_started = False
        f.tearDown()


def wait_for(predicate):
    deadline = monotonic() + 3
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.001)
    raise AssertionError("expected bounded reactor condition")


def receive(peer):
    decoder = fixtures.ConsumerWireStreamDecoderV2()
    while True:
        block = peer.recv(65536)
        if not block:
            return None
        frames = decoder.feed(block)
        if frames:
            return frames[0].payload


def submit(f, ref):
    peer = f.connect()
    peer.sendall(cold_fixtures.ColdReactorTests.frame(ref))
    return peer


def slow_first(f):
    entered, release = Event(), Event()
    f.gates.append(release)
    operations = f.service._listener._gateway._operations
    original = operations.submit_signal
    calls = []

    def call(**kw):
        calls.append(current_thread().name)
        if len(calls) == 1:
            entered.set()
            assert release.wait(3)
        return original(**kw)

    return operations, call, calls, entered, release


def test_slow_write_does_not_block_read_and_queue_credits_remain_bounded(runtime):
    f = runtime
    ops, call, calls, entered, release = slow_first(f)
    with patch.object(ops, "submit_signal", side_effect=call):
        first = submit(f, "first")
        assert entered.wait(2)
        second = submit(f, "second")
        wait_for(lambda: f.service.ingress_stats().submit_execution_pending == 2)
        read = f.connect()
        read.sendall(f.frame("while-write-blocked"))
        assert receive(read)["status"] == "COMPLETED"
        assert len(f.journal.scan().records) == 0
        stats = f.service.ingress_stats()
        assert stats.pending_capacity == 4 and stats.pending_connections <= 4
        assert stats.peak_pending_connections <= 4
        release.set()
        assert receive(first)["status"] == receive(second)["status"] == "ACCEPTED"
        assert len(f.journal.scan().records) == 4
        assert calls == ["consumer-submit-owner"] * 2
    wait_for(lambda: f.service.ingress_stats().submit_execution_pending == 0)
    assert f.errors == []


@pytest.mark.parametrize("half_close", (False, True))
def test_observed_queued_close_cancels_but_half_close_keeps_response(runtime, half_close):
    f = runtime
    ops, call, calls, entered, release = slow_first(f)
    with patch.object(ops, "submit_signal", side_effect=call):
        first = submit(f, "first")
        assert entered.wait(2)
        second = submit(f, "second")
        wait_for(lambda: f.service.ingress_stats().submit_execution_pending == 2)
        if half_close:
            second.shutdown(socket.SHUT_WR)
            barrier = f.connect()
            barrier.sendall(f.frame("after-half-close"))
            assert receive(barrier)["status"] == "COMPLETED"
        else:
            second.close()
            wait_for(lambda: f.service.ingress_stats().served_connections >= 1)
        release.set()
        assert receive(first)["status"] == "ACCEPTED"
        if half_close:
            assert receive(second)["status"] == "ACCEPTED"
        wait_for(lambda: f.service.ingress_stats().submit_execution_pending == 0)
        assert len(f.journal.scan().records) == (4 if half_close else 2)
        assert len(calls) == (2 if half_close else 1)
        assert f.service.ingress_stats().submit_execution_not_started == int(not half_close)
    assert f.errors == []


def test_started_unknown_commit_is_not_retried_and_never_has_clean_drain(runtime):
    f = runtime
    original = f.journal.append_batch
    calls = []

    def lost_reply(*args):
        calls.append("commit")
        original(*args)
        raise OSError("injected after source commit")

    with patch.object(f.journal, "append_batch", side_effect=lost_reply):
        peer = submit(f, "unknown")
        assert receive(peer) is None
        f.runner.join(3)
    assert not f.runner.is_alive()
    assert f.errors and not f.service.submission_shutdown_verified
    assert calls == ["commit"] and len(f.journal.scan().records) == 2
    stats = f.service.ingress_stats()
    assert stats.submit_execution_unknown == 1 and stats.submit_execution_failed
    assert not f.service._submit_lane._thread.is_alive()


def test_stop_fences_queued_job_without_canceling_already_running_outcome(runtime):
    f = runtime
    ops, call, calls, entered, release = slow_first(f)
    with patch.object(ops, "submit_signal", side_effect=call):
        first = submit(f, "first")
        assert entered.wait(2)
        second = submit(f, "second")
        wait_for(lambda: f.service.ingress_stats().submit_execution_pending == 2)
        f.service.request_stop()
        release.set()
        f.runner.join(3)
        assert not f.runner.is_alive()
        assert calls == ["consumer-submit-owner"]
        assert len(f.journal.scan().records) == 2
        assert receive(first) is None and receive(second) is None
    assert f.service.submission_shutdown_verified
    assert f.service._listener._gateway._sessions == {}
    assert f.errors == []


def test_invalid_write_schema_never_runs_handler_or_poison_subsequent_valid_request(runtime):
    f = runtime
    listener = f.service._listener
    original = decode_frame(cold_fixtures.ColdReactorTests.frame("bad"))
    payload = dict(original.payload)
    payload["payload"] = {**payload["payload"], "quantity": "invalid"}
    with patch.object(
        listener, "_validate_request_frame", wraps=listener._validate_request_frame
    ) as schema:
        peer = f.connect()
        peer.sendall(encode_frame(MessageKind.REQUEST, 17, payload))
        assert receive(peer) is None
        assert schema.call_count == 1
        assert len(f.journal.scan().records) == 0
        assert not f.service.ingress_stats().submit_execution_failed
        valid = submit(f, "valid")
        assert receive(valid)["status"] == "ACCEPTED"
        assert schema.call_count == 2
    assert len(f.journal.scan().records) == 2 and not f.errors


@pytest.mark.parametrize("runtime", ["tiny"], indirect=True)
def test_isolated_write_preserves_partial_frame_half_close_and_partial_response(runtime):
    f = runtime
    frame = cold_fixtures.ColdReactorTests.frame("partial")
    peer = f.connect()
    peer.sendall(frame[:16])
    barrier = f.connect()
    barrier.sendall(f.frame("before-complete-frame"))
    assert receive(barrier)["status"] == "COMPLETED"
    assert len(f.journal.scan().records) == 0
    peer.sendall(frame[16:])
    peer.shutdown(socket.SHUT_WR)
    assert receive(peer)["status"] == "ACCEPTED"
    assert len(f.journal.scan().records) == 2 and not f.errors
