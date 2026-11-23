from __future__ import annotations

import json
import multiprocessing as mp
import os
import selectors
import socket
import sys
from types import SimpleNamespace
from typing import Any, cast

import pytest
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1
from xuanling_tradecore.consumer.owner_read_reactor import OWNER_READ_SLOT_COUNT
from xuanling_tradecore.consumer.spawned_owner_brokers import (
    _READ_BROKER_TURN_BUDGET_NS,
    _WRITE_BROKER_TURN_BUDGET_NS,
    SpawnedOwnerBrokerV1,
    _flush_read_broker_outputs,
    _flush_write_broker_outputs,
    _open_broker_diagnostic_writer,
    _round_robin_frontend_events,
    _run_budgeted_broker_turn,
    _set_selector_events,
)
from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace


def test_turn_budgets_keep_independent_write_and_read_service_windows() -> None:
    assert _WRITE_BROKER_TURN_BUDGET_NS == 1_000_000
    assert _READ_BROKER_TURN_BUDGET_NS == 500_000
    assert _WRITE_BROKER_TURN_BUDGET_NS > _READ_BROKER_TURN_BUDGET_NS > 0


def test_backpressure_unregisters_read_then_restores_without_losing_data():
    left, right = socket.socketpair()
    selector = selectors.DefaultSelector()
    registered: dict[socket.socket, int] = {}
    try:
        selector.register(right, selectors.EVENT_WRITE, "owner")
        right.sendall(b"a")
        _set_selector_events(selector, registered, left, selectors.EVENT_READ, "frontend")
        assert any(key.fileobj is left for key, _ in selector.select(0))
        _set_selector_events(selector, registered, left, 0, "frontend")
        assert all(key.fileobj is not left for key, _ in selector.select(0))
        _set_selector_events(selector, registered, left, selectors.EVENT_WRITE, "frontend")
        assert any(
            key.fileobj is left and mask & selectors.EVENT_WRITE for key, mask in selector.select(0)
        )
        _set_selector_events(selector, registered, left, selectors.EVENT_READ, "frontend")
        assert any(
            key.fileobj is left and mask & selectors.EVENT_READ for key, mask in selector.select(0)
        )
        assert left.recv(1) == b"a"
    finally:
        selector.close()
        left.close()
        right.close()


def _ready() -> OwnerReadyV1:
    return OwnerReadyV1(
        "a" * 32,
        123,
        "b" * 64,
        "journal",
        0,
        bytes(32),
        "",
    )


def test_broker_diagnostic_writer_seals_redacted_hash_chained_trace(tmp_path):
    ready = _ready()
    writer = _open_broker_diagnostic_writer(tmp_path, ready)
    writer.submit({"operation": "SubmitSignal", "serial": 1})
    writer.close()
    assert writer.coverage_verified
    path = tmp_path / f"broker-diagnostic-{ready.generation}"
    assert verify_closed_request_trace(path.with_suffix(".manifest.json")) == 1
    status = json.loads(path.with_suffix(".status.json").read_text())
    assert status["trace_storage"]["mode"] == "rotating"
    assert status["journal_id"] == ready.journal_id


def test_spawned_broker_rejects_mixed_or_incomplete_topology():
    context = mp.get_context("spawn")
    shared = FrontendSharedCreditV1(context, capacity=128, worker_count=2)
    read_slots = FrontendSharedCreditV1(context, capacity=OWNER_READ_SLOT_COUNT, worker_count=2)
    left, right = socket.socketpair()
    try:
        with pytest.raises(OwnerProtocolError, match="WRITE broker topology"):
            SpawnedOwnerBrokerV1(
                context,
                kind="WRITE",
                frontend_channels=(left, right),
                owner_channels=(),
                ready=_ready(),
                shared_credit=shared,
                shared_read_slots=None,
                cpu=0,
            )
        with pytest.raises(OwnerProtocolError, match="READ broker topology"):
            SpawnedOwnerBrokerV1(
                context,
                kind="READ",
                frontend_channels=((left,), (right,)),
                owner_channels=(left,),
                ready=None,
                shared_credit=shared,
                shared_read_slots=read_slots,
                cpu=0,
            )
    finally:
        left.close()
        right.close()


def test_write_broker_eager_flush_is_bounded_to_owner_and_two_events_per_frontend():
    class Owner:
        wants_write = True

    class Broker:
        owner = Owner()

        def __init__(self) -> None:
            self.owner_writes = 0
            self.frontend_writes = [0, 0]

        def write_owner(self) -> None:
            self.owner_writes += 1

        def wants_frontend_write(self, worker_id: int) -> bool:
            return self.frontend_writes[worker_id] < 3

        def write_frontend(self, worker_id: int) -> bool:
            self.frontend_writes[worker_id] += 1
            return worker_id == 0

    channels = socket.socketpair()
    broker = Broker()
    try:
        next_worker = _flush_write_broker_outputs(
            cast(Any, broker), {channels[0]: 0, channels[1]: 1}, 0
        )
        assert broker.owner_writes == 1
        assert broker.frontend_writes == [2, 1]
        assert next_worker == 1
    finally:
        channels[0].close()
        channels[1].close()


def test_read_broker_eager_flush_advances_each_single_slot_once():
    class OwnerReads:
        def __init__(self, channels: tuple[socket.socket, ...]) -> None:
            self.channels = channels

    class Broker:
        def __init__(self, channels: tuple[socket.socket, ...]) -> None:
            self.owner_reads = OwnerReads(channels[:2])
            self.pumps = 0
            self.owner_writes: list[int] = []
            self.frontend_writes: list[int] = []

        def pump(self) -> int:
            self.pumps += 1
            return 0

        def owner_events(self, channel: socket.socket) -> int:
            return selectors.EVENT_WRITE if channel is self.owner_reads.channels[0] else 0

        def io_owner(self, channel: socket.socket, mask: int) -> None:
            assert mask == selectors.EVENT_WRITE
            self.owner_writes.append(channel.fileno())

        def frontend_events(self, channel: socket.socket) -> int:
            return selectors.EVENT_WRITE

        def write_frontend(self, channel: socket.socket) -> bool:
            self.frontend_writes.append(channel.fileno())
            return True

    pairs = tuple(socket.socketpair() for _ in range(4))
    channels = tuple(pair[0] for pair in pairs)
    broker = Broker(channels)
    try:
        next_worker = _flush_read_broker_outputs(cast(Any, broker), (channels[2:],), 0)
        assert broker.pumps == 1
        assert broker.owner_writes == [channels[0].fileno()]
        assert broker.frontend_writes == [channel.fileno() for channel in channels[2:]]
        assert next_worker == 0
    finally:
        for left, right in pairs:
            left.close()
            right.close()


def test_budgeted_broker_turn_uses_zero_wait_until_deadline():
    class Selector:
        def __init__(self) -> None:
            self.timeouts: list[float] = []
            self.results = [
                [(SimpleNamespace(data=("owner", "two")), selectors.EVENT_WRITE)],
                [(SimpleNamespace(data=("frontend", "three")), selectors.EVENT_READ)],
            ]

        def select(self, timeout: float):
            self.timeouts.append(timeout)
            return self.results.pop(0)

    class Clock:
        def __init__(self) -> None:
            self.now = -4

        def __call__(self) -> int:
            self.now += 4
            return self.now

    selector = Selector()
    dispatched: list[object] = []
    flushed: list[None] = []
    polls = _run_budgeted_broker_turn(
        cast(Any, selector),
        cast(Any, [(SimpleNamespace(data=("frontend", "one")), selectors.EVENT_READ)]),
        lambda events: dispatched.extend(events),
        lambda: flushed.append(None),
        10,
        clock_ns=Clock(),
    )
    assert selector.timeouts == [0, 0]
    assert len(dispatched) == 3
    assert len(flushed) == 3
    assert polls == 3

    empty = SimpleNamespace(select=lambda timeout: [])
    assert (
        _run_budgeted_broker_turn(
            cast(Any, empty),
            [],
            lambda _events: pytest.fail("empty immediate poll dispatched"),
            lambda: pytest.fail("empty immediate poll flushed"),
            10,
        )
        == 0
    )


def test_write_turn_yields_after_offer_budget_even_if_frontend_remains_readable():
    dispatched = []
    flushed = []
    selector = SimpleNamespace(select=lambda _timeout: pytest.fail("no-progress poll"))
    initial = cast(Any, [(SimpleNamespace(data=("frontend", "one")), selectors.EVENT_READ)])
    polls = _run_budgeted_broker_turn(
        cast(Any, selector),
        initial,
        lambda events: dispatched.extend(events),
        lambda: flushed.append(True),
        1_000_000,
        clock_ns=lambda: 0,
        yield_after_flush=lambda: True,
    )
    assert polls == 1
    assert len(dispatched) == 1 and len(flushed) == 1


def test_ready_frontends_are_interleaved_and_start_cursor_rotates():
    def event(worker: int, sequence: int):
        return (SimpleNamespace(data=("frontend", (worker, sequence))), selectors.EVENT_READ)

    owner = (SimpleNamespace(data=("owner", "owner")), selectors.EVENT_WRITE)
    worker_ids = {(worker, sequence): worker for worker in range(3) for sequence in range(2)}
    ready = [event(0, 0), event(0, 1), event(1, 0), event(2, 0), event(2, 1), owner]
    ordered, cursor = _round_robin_frontend_events(cast(Any, ready), worker_ids, 1)
    assert ordered[0] is owner
    assert [key.data[1] for key, _mask in ordered[1:]] == [
        (1, 0),
        (2, 0),
        (0, 0),
        (2, 1),
        (0, 1),
    ]
    assert cursor == 2

    next_ready = [event(0, 0), event(1, 0), event(2, 0)]
    ordered, cursor = _round_robin_frontend_events(cast(Any, next_ready), worker_ids, cursor)
    assert [key.data[1] for key, _mask in ordered] == [(2, 0), (0, 0), (1, 0)]
    assert cursor == 0


linux_spawn = pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(os, "sched_getaffinity"),
    reason="broker process qualification requires Linux affinity",
)


@linux_spawn
@pytest.mark.parametrize("kind", ("WRITE", "READ"))
def test_spawned_broker_two_phase_drain_keeps_transport_until_release(kind):
    context = mp.get_context("spawn")
    cpu = min(os.sched_getaffinity(0))
    shared = FrontendSharedCreditV1(context, capacity=128, worker_count=2)
    read_slots = FrontendSharedCreditV1(context, capacity=OWNER_READ_SLOT_COUNT, worker_count=2)
    frontend_pairs = tuple(socket.socketpair() for _ in range(2 if kind == "WRITE" else 8))
    owner_pairs = tuple(
        socket.socketpair() for _ in range(1 if kind == "WRITE" else OWNER_READ_SLOT_COUNT)
    )
    frontend = (
        tuple(pair[0] for pair in frontend_pairs)
        if kind == "WRITE"
        else (
            tuple(frontend_pairs[index][0] for index in range(4)),
            tuple(frontend_pairs[index][0] for index in range(4, 8)),
        )
    )
    broker = SpawnedOwnerBrokerV1(
        context,
        kind=kind,
        frontend_channels=frontend,
        owner_channels=tuple(pair[0] for pair in owner_pairs),
        ready=_ready() if kind == "WRITE" else None,
        shared_credit=shared,
        shared_read_slots=read_slots if kind == "READ" else None,
        cpu=cpu,
    )
    try:
        broker.start()
        assert broker.alive and broker.affinity == (cpu,)
        broker.drain()
        assert broker.alive and broker.drained
        # The peer still observes an open transport while the supervisor would
        # close frontends and stop the owner control plane.
        for _parent, peer in (*frontend_pairs, *owner_pairs):
            peer.setblocking(False)
            with pytest.raises(BlockingIOError):
                peer.recv(1)
        broker.release()
        assert not broker.alive
    finally:
        if broker.alive:
            broker.abort()
        for parent, peer in (*frontend_pairs, *owner_pairs):
            parent.close()
            peer.close()
