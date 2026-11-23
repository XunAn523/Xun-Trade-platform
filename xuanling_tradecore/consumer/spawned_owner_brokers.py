"""Supervised owner broker processes for the fixed multi-frontend topology."""

from __future__ import annotations

import os
import selectors
import socket
from collections.abc import Callable, Mapping
from multiprocessing.connection import Connection
from pathlib import Path
from time import monotonic, monotonic_ns
from typing import Any, Literal, cast

from .broker_diagnostic_trace import BrokerDiagnosticTrace
from .fair_queue_v2 import BoundedFairOperationQueueV2, ConsumerOperationLaneV2
from .frontend_owner_broker import FrontendOwnerWriteBrokerV1
from .frontend_read_broker import FrontendOwnerReadBrokerV1
from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1
from .owner_reactor_ipc import ReactorOwnerPortV1
from .owner_read_reactor import OWNER_READ_SLOT_COUNT, ReactorOwnerReadsV1
from .request_trace_v2 import BoundedRequestTraceWriterV2

_BrokerKind = Literal["WRITE", "READ"]
_MAX_EAGER_WRITE_EVENTS_PER_FRONTEND = 2
_WRITE_BROKER_TURN_BUDGET_NS = 1_000_000
_READ_BROKER_TURN_BUDGET_NS = 500_000
_BROKER_IDLE_POLL_SECONDS = 0.005
_SelectorEvents = list[tuple[selectors.SelectorKey, int]]


def _open_broker_diagnostic_writer(
    trace_root: Path, ready: OwnerReadyV1
) -> BoundedRequestTraceWriterV2:
    """Create opt-in broker evidence with closed, hash-verified rotation."""
    return BoundedRequestTraceWriterV2(
        trace_root / f"broker-diagnostic-{ready.generation}.jsonl",
        journal_id=ready.journal_id,
        capacity=65_536,
        max_bytes=64 * 1024 * 1024,
        rotation_bytes=16 * 1024 * 1024,
        record_schema="consumer-stage-trace-v1",
    )


def _flush_write_broker_outputs(
    broker: FrontendOwnerWriteBrokerV1[socket.socket],
    worker_ids: dict[socket.socket, int],
    next_worker: int,
) -> int:
    """Advance only the output made ready by this selector turn.

    An owner reply can contain STARTED and terminal for the same request.  Both
    packets are already bounded by the broker's existing event budget, so at
    most those two protocol packets may be handed to each frontend before the
    reactor returns to ``select``.  This removes an avoidable wakeup without
    creating another queue, spinning on EAGAIN or weakening fairness.
    """

    if broker.owner.wants_write:
        broker.write_owner()
    worker_count = len(worker_ids)
    for offset in range(worker_count):
        worker_id = (next_worker + offset) % worker_count
        for _ in range(_MAX_EAGER_WRITE_EVENTS_PER_FRONTEND):
            if not broker.wants_frontend_write(worker_id):
                break
            if not broker.write_frontend(worker_id):
                break
    return (next_worker + 1) % worker_count


def _flush_read_broker_outputs(
    broker: FrontendOwnerReadBrokerV1,
    frontend_channels: tuple[tuple[socket.socket, ...], ...],
    next_worker: int,
) -> int:
    """Perform one bounded nonblocking write for each ready READ channel.

    Each authoritative owner slot and each frontend channel already has a
    single output buffer.  A single eager syscall per channel therefore keeps
    the frozen bounded-slot envelope while avoiding an otherwise mandatory extra
    selector cycle at REQUEST, GRANT and RELEASED transitions.
    """

    broker.pump()
    for channel in broker.owner_reads.channels:
        if broker.owner_events(channel) & selectors.EVENT_WRITE:
            broker.io_owner(channel, selectors.EVENT_WRITE)
    worker_count = len(frontend_channels)
    for offset in range(worker_count):
        channels = frontend_channels[(next_worker + offset) % worker_count]
        for channel in channels:
            if broker.frontend_events(channel) & selectors.EVENT_WRITE:
                broker.write_frontend(channel)
    return (next_worker + 1) % worker_count


def _round_robin_frontend_events(
    events: _SelectorEvents,
    worker_ids: Mapping[Any, int],
    next_worker: int,
) -> tuple[_SelectorEvents, int]:
    """Put owner progress first, then interleave ready frontends fairly.

    Selector registration order must not become an implicit frontend priority.
    Each ready worker contributes one event before another event from the same
    worker is dispatched.  The start worker advances across turns even when all
    workers are continuously ready.
    """

    owner_events: _SelectorEvents = []
    by_worker: dict[int, _SelectorEvents] = {}
    for event in events:
        key, _mask = event
        kind, channel = key.data
        if kind != "frontend":
            owner_events.append(event)
            continue
        worker_id = worker_ids[channel]
        by_worker.setdefault(worker_id, []).append(event)
    if not by_worker:
        return owner_events, next_worker
    worker_count = len(set(worker_ids.values()))
    ordered_workers = [
        (next_worker + offset) % worker_count
        for offset in range(worker_count)
        if (next_worker + offset) % worker_count in by_worker
    ]
    frontend_events: _SelectorEvents = []
    while any(by_worker[worker_id] for worker_id in ordered_workers):
        for worker_id in ordered_workers:
            ready = by_worker[worker_id]
            if ready:
                frontend_events.append(ready.pop(0))
    return owner_events + frontend_events, (next_worker + 1) % worker_count


def _dispatch_write_broker_events(
    broker: FrontendOwnerWriteBrokerV1[socket.socket],
    worker_ids: dict[socket.socket, int],
    events: _SelectorEvents,
) -> None:
    for key, mask in events:
        kind, channel = key.data
        if kind == "frontend":
            worker_id = worker_ids[channel]
            if mask & selectors.EVENT_READ:
                broker.read_frontend(worker_id)
            if mask & selectors.EVENT_WRITE:
                broker.write_frontend(worker_id)
        else:
            if mask & selectors.EVENT_WRITE:
                broker.write_owner()
            if mask & selectors.EVENT_READ:
                broker.read_owner()


def _dispatch_read_broker_events(
    broker: FrontendOwnerReadBrokerV1,
    events: _SelectorEvents,
) -> None:
    for key, mask in events:
        kind, channel = key.data
        if kind == "frontend":
            if mask & selectors.EVENT_READ:
                broker.read_frontend(channel)
            if mask & selectors.EVENT_WRITE:
                broker.write_frontend(channel)
        else:
            broker.io_owner(channel, mask)


def _run_budgeted_broker_turn(
    selector: selectors.BaseSelector,
    initial_events: _SelectorEvents,
    dispatch: Callable[[_SelectorEvents], None],
    flush: Callable[[], None],
    budget_ns: int,
    *,
    clock_ns: Callable[[], int] = monotonic_ns,
    yield_after_flush: Callable[[], bool] | None = None,
) -> int:
    """Advance one ready set and only already-ready work within a time budget.

    The initial blocking select is outside the budget.  Once it wakes, the
    broker may collapse protocol handoffs until the deadline.  Every follow-up
    select is zero-wait and an empty result ends the turn immediately, so this
    is neither a sleep nor a busy-spin loop.  Time, not a frontend packet count,
    caps a hot turn; the fixed topology and existing protocol buffers retain the
    hard memory bound.
    """

    if not initial_events:
        return 0
    if type(budget_ns) is not int or budget_ns <= 0:
        raise OwnerProtocolError("broker turn budget must be positive")
    deadline = clock_ns() + budget_ns
    dispatch(initial_events)
    flush()
    polls = 1
    while clock_ns() < deadline:
        # Readable frontend sockets remain level-triggered after the WRITE
        # port has consumed its four-offer turn budget.  Polling them again
        # cannot admit anything until begin_turn(); yield immediately instead
        # of spinning on no-progress readiness for the remainder of 1 ms.
        if yield_after_flush is not None and yield_after_flush():
            break
        ready = selector.select(0)
        if not ready:
            break
        dispatch(ready)
        flush()
        polls += 1
    return polls


def _set_selector_events(
    selector: selectors.BaseSelector,
    registered: dict[socket.socket, int],
    channel: socket.socket,
    events: int,
    data: object,
) -> None:
    """Stop polling a fully backpressured input without losing its socket."""
    current = registered.get(channel)
    if events == 0:
        if current is not None:
            selector.unregister(channel)
            del registered[channel]
    elif current is None:
        selector.register(channel, events, data)
        registered[channel] = events
    elif current != events:
        selector.modify(channel, events, data)
        registered[channel] = events


def _bind_cpu(cpu: int) -> tuple[int, ...]:
    if not hasattr(os, "sched_setaffinity"):
        raise OwnerProtocolError("owner broker process affinity requires Linux")
    os.sched_setaffinity(0, {cpu})
    affinity = tuple(sorted(os.sched_getaffinity(0)))
    if affinity != (cpu,):
        raise OwnerProtocolError("owner broker process CPU binding changed")
    return affinity


def _run_write_broker(
    frontend_channels: tuple[socket.socket, ...],
    owner_channel: socket.socket,
    ready: OwnerReadyV1,
    shared_credit: FrontendSharedCreditV1,
    cpu: int,
    drain_event: Any,
    release_event: Any,
    status: Connection,
) -> None:
    broker = None
    diagnostic_trace = None
    diagnostic_capture = None
    selector = selectors.DefaultSelector()
    registered: dict[socket.socket, int] = {}

    def register(channel: socket.socket, events: int, data: object) -> None:
        selector.register(channel, events, data)
        registered[channel] = events

    def set_events(channel: socket.socket, events: int, data: object) -> None:
        _set_selector_events(selector, registered, channel, events, data)

    try:
        affinity = _bind_cpu(cpu)
        diagnostic_dir = os.environ.get("TRADECORE_DIAGNOSTIC_BROKER_TRACE_DIR")
        if diagnostic_dir is not None:
            trace_root = Path(diagnostic_dir)
            if not trace_root.is_absolute() or trace_root.is_symlink() or not trace_root.is_dir():
                raise OwnerProtocolError("candidate broker diagnostic directory unavailable")
            diagnostic_trace = _open_broker_diagnostic_writer(trace_root, ready)
            diagnostic_capture = BrokerDiagnosticTrace(diagnostic_trace, ready.generation)
        queue = BoundedFairOperationQueueV2[socket.socket](
            total_capacity=128,
            lane_capacities={
                ConsumerOperationLaneV2.WRITE: 128,
                ConsumerOperationLaneV2.REPLAY: 1,
                ConsumerOperationLaneV2.SNAPSHOT: 1,
                ConsumerOperationLaneV2.OTHER: 1,
            },
        )
        handoff_limit_raw = os.environ.get("TRADECORE_DIAGNOSTIC_WRITE_HANDOFF_LIMIT", "128")
        if handoff_limit_raw not in {"32", "128"}:
            raise OwnerProtocolError("invalid candidate WRITE handoff limit")
        diagnostic_backpressure = handoff_limit_raw == "32"
        owner = ReactorOwnerPortV1(
            owner_channel,
            ready=ready,
            ready_queue=queue,
            diagnostic_sent=diagnostic_capture.sent if diagnostic_capture is not None else None,
            handoff_limit=int(handoff_limit_raw),
        )
        broker = FrontendOwnerWriteBrokerV1(
            frontend_channels,
            owner=owner,
            shared_credit=shared_credit,
            diagnostic_ingested=(
                diagnostic_capture.ingested if diagnostic_capture is not None else None
            ),
        )
        worker_ids = {channel: worker_id for worker_id, channel in enumerate(frontend_channels)}
        for channel in frontend_channels:
            register(channel, selectors.EVENT_READ, ("frontend", channel))
        register(owner.channel, selectors.EVENT_READ, ("owner", owner.channel))
        status.send(("READY", os.getpid(), affinity, "WRITE"))
        input_cursor = 0
        output_cursor = 0

        def dispatch_ready(events: _SelectorEvents) -> None:
            nonlocal input_cursor
            ordered, input_cursor = _round_robin_frontend_events(
                events,
                worker_ids,
                input_cursor,
            )
            _dispatch_write_broker_events(broker, worker_ids, ordered)

        def flush_ready() -> None:
            nonlocal output_cursor
            output_cursor = _flush_write_broker_outputs(
                broker,
                worker_ids,
                output_cursor,
            )

        while not drain_event.is_set():
            broker.begin_turn()
            set_events(
                owner.channel,
                selectors.EVENT_READ | (selectors.EVENT_WRITE if owner.wants_write else 0),
                ("owner", owner.channel),
            )
            for channel in frontend_channels:
                set_events(
                    channel,
                    (0 if diagnostic_backpressure and owner.full else selectors.EVENT_READ)
                    | (
                        selectors.EVENT_WRITE
                        if broker.wants_frontend_write(worker_ids[channel])
                        else 0
                    ),
                    ("frontend", channel),
                )
            _run_budgeted_broker_turn(
                selector,
                selector.select(_BROKER_IDLE_POLL_SECONDS),
                dispatch_ready,
                flush_ready,
                _WRITE_BROKER_TURN_BUDGET_NS,
                yield_after_flush=lambda: (
                    broker.owner.turn_offer_budget_exhausted
                    or (diagnostic_backpressure and owner.full)
                ),
            )
        if broker.outstanding:
            raise OwnerProtocolError("external WRITE broker stopped before drain")
        broker.stop()
        if diagnostic_capture is not None and diagnostic_trace is not None:
            diagnostic_capture.require_drained()
            diagnostic_trace.close()
            if not diagnostic_trace.coverage_verified:
                raise OwnerProtocolError("broker diagnostic trace is incomplete")
        status.send(("DRAINED", os.getpid(), "WRITE"))
        if not release_event.wait(60):
            raise OwnerProtocolError("external WRITE broker release expired")
        status.send(("STOPPED", os.getpid(), "WRITE"))
    except BaseException as error:
        if broker is not None:
            try:
                broker.fail()
            except BaseException:
                pass
        try:
            status.send(("FAILED", os.getpid(), "WRITE", type(error).__name__))
        except BaseException:
            pass
        raise
    finally:
        if diagnostic_trace is not None:
            diagnostic_trace.close()
        selector.close()
        status.close()


def _run_read_broker(
    frontend_channels: tuple[tuple[socket.socket, ...], ...],
    owner_channels: tuple[socket.socket, ...],
    shared_credit: FrontendSharedCreditV1,
    shared_read_slots: FrontendSharedCreditV1,
    cpu: int,
    drain_event: Any,
    release_event: Any,
    status: Connection,
) -> None:
    broker = None
    selector = selectors.DefaultSelector()
    registered: dict[socket.socket, int] = {}

    def register(channel: socket.socket, events: int, data: object) -> None:
        selector.register(channel, events, data)
        registered[channel] = events

    def set_events(channel: socket.socket, events: int, data: object) -> None:
        if registered.get(channel) != events:
            selector.modify(channel, events, data)
            registered[channel] = events

    try:
        affinity = _bind_cpu(cpu)
        queue = BoundedFairOperationQueueV2[socket.socket](
            total_capacity=OWNER_READ_SLOT_COUNT,
            lane_capacities={
                ConsumerOperationLaneV2.WRITE: 1,
                ConsumerOperationLaneV2.REPLAY: OWNER_READ_SLOT_COUNT,
                ConsumerOperationLaneV2.SNAPSHOT: OWNER_READ_SLOT_COUNT,
                ConsumerOperationLaneV2.OTHER: 1,
            },
        )
        owner = ReactorOwnerReadsV1(owner_channels, ready_queue=queue)
        broker = FrontendOwnerReadBrokerV1(
            frontend_channels,
            owner_reads=cast(ReactorOwnerReadsV1[object], owner),
            shared_credit=shared_credit,
            shared_read_slots=shared_read_slots,
        )
        for channels in frontend_channels:
            for channel in channels:
                register(channel, selectors.EVENT_READ, ("frontend", channel))
        for channel in owner.channels:
            register(channel, owner.events(channel), ("owner", channel))
        status.send(("READY", os.getpid(), affinity, "READ"))
        worker_ids = {
            channel: worker_id
            for worker_id, channels in enumerate(frontend_channels)
            for channel in channels
        }
        input_cursor = 0
        output_cursor = 0

        def dispatch_ready(events: _SelectorEvents) -> None:
            nonlocal input_cursor
            ordered, input_cursor = _round_robin_frontend_events(
                events,
                worker_ids,
                input_cursor,
            )
            _dispatch_read_broker_events(broker, ordered)

        def flush_ready() -> None:
            nonlocal output_cursor
            output_cursor = _flush_read_broker_outputs(
                broker,
                frontend_channels,
                output_cursor,
            )

        while not drain_event.is_set():
            broker.pump()
            for channels in frontend_channels:
                for channel in channels:
                    set_events(
                        channel,
                        broker.frontend_events(channel),
                        ("frontend", channel),
                    )
            for channel in owner.channels:
                set_events(channel, broker.owner_events(channel), ("owner", channel))
            _run_budgeted_broker_turn(
                selector,
                selector.select(_BROKER_IDLE_POLL_SECONDS),
                dispatch_ready,
                flush_ready,
                _READ_BROKER_TURN_BUDGET_NS,
            )
        if broker.outstanding or shared_read_slots.snapshot().held:
            raise OwnerProtocolError("external READ broker stopped before drain")
        broker.stop()
        status.send(("DRAINED", os.getpid(), "READ"))
        if not release_event.wait(60):
            raise OwnerProtocolError("external READ broker release expired")
        status.send(("STOPPED", os.getpid(), "READ"))
    except BaseException as error:
        if broker is not None:
            try:
                broker.fail()
            except BaseException:
                pass
        try:
            status.send(("FAILED", os.getpid(), "READ", type(error).__name__))
        except BaseException:
            pass
        raise
    finally:
        selector.close()
        status.close()


class SpawnedOwnerBrokerV1:
    """One-shot broker process with verified DRAINED then STOPPED phases."""

    def __init__(
        self,
        context: Any,
        *,
        kind: _BrokerKind,
        frontend_channels: tuple[socket.socket, ...] | tuple[tuple[socket.socket, ...], ...],
        owner_channels: tuple[socket.socket, ...],
        ready: OwnerReadyV1 | None,
        shared_credit: FrontendSharedCreditV1,
        shared_read_slots: FrontendSharedCreditV1 | None,
        cpu: int,
    ) -> None:
        if kind == "WRITE":
            if len(owner_channels) != 1 or ready is None or shared_read_slots is not None:
                raise OwnerProtocolError("invalid external WRITE broker topology")
            target = _run_write_broker
            arguments = (
                frontend_channels,
                owner_channels[0],
                ready,
                shared_credit,
                cpu,
            )
        elif kind == "READ":
            if (
                len(owner_channels) != OWNER_READ_SLOT_COUNT
                or ready is not None
                or shared_read_slots is None
            ):
                raise OwnerProtocolError("invalid external READ broker topology")
            target = _run_read_broker
            arguments = (
                frontend_channels,
                owner_channels,
                shared_credit,
                shared_read_slots,
                cpu,
            )
        else:  # pragma: no cover - Literal callers, retained as runtime fence
            raise OwnerProtocolError("unknown external broker kind")
        self.kind = kind
        self._drain: Any = context.Event()
        self._release: Any = context.Event()
        parent_status, child_status = context.Pipe(duplex=False)
        self._status = cast(Connection, parent_status)
        self._child_status = cast(Connection, child_status)
        flattened = tuple(
            channel
            for item in frontend_channels
            for channel in (item if isinstance(item, tuple) else (item,))
        )
        self._parent_sockets = (*flattened, *owner_channels)
        self._process: Any = context.Process(
            target=target,
            args=(*arguments, self._drain, self._release, child_status),
            name=f"tradecore-{kind.lower()}-broker",
        )
        self._state = "NEW"
        self.pid: int | None = None
        self.affinity: tuple[int, ...] = ()
        self.shutdown_verified = False

    def start(self, *, timeout: float = 15) -> int:
        if self._state != "NEW":
            raise OwnerProtocolError("owner broker already started")
        self._process.start()
        self._child_status.close()
        for channel in self._parent_sockets:
            channel.close()
        self._state = "STARTING"
        message = self._receive(timeout, "readiness")
        affinity_value = cast(object, message[2]) if len(message) == 4 else None
        if (
            len(message) != 4
            or message[0] != "READY"
            or message[1] != self._process.pid
            or type(affinity_value) is not tuple
            or any(type(cpu) is not int for cpu in cast(tuple[object, ...], affinity_value))
            or message[3] != self.kind
        ):
            self.abort()
            raise OwnerProtocolError("owner broker readiness invalid")
        self.pid = cast(int, message[1])
        self.affinity = cast(tuple[int, ...], affinity_value)
        self._state = "RUNNING"
        return self.pid

    def drain(self, *, timeout: float = 15) -> None:
        if self._state != "RUNNING":
            raise OwnerProtocolError("owner broker is not running")
        self._state = "DRAINING"
        self._drain.set()
        message = self._receive(timeout, "drain")
        if message != ("DRAINED", self.pid, self.kind) or not self._process.is_alive():
            self.abort()
            raise OwnerProtocolError("owner broker drain was not verified")
        self._state = "DRAINED"

    def release(self, *, timeout: float = 15) -> None:
        if self._state != "DRAINED":
            raise OwnerProtocolError("owner broker is not drained")
        self._state = "STOPPING"
        self._release.set()
        deadline = monotonic() + timeout
        message = self._receive(max(0.0, deadline - monotonic()), "release")
        self._process.join(max(0.0, deadline - monotonic()))
        if message != ("STOPPED", self.pid, self.kind) or self._process.exitcode != 0:
            self.abort()
            raise OwnerProtocolError("owner broker stop was not verified")
        self._status.close()
        self._state = "CLOSED"
        self.shutdown_verified = True

    def _receive(self, timeout: float, phase: str) -> tuple[Any, ...]:
        if timeout <= 0 or not self._status.poll(timeout):
            self.abort()
            raise OwnerProtocolError(f"owner broker {phase} expired")
        return cast(tuple[Any, ...], self._status.recv())

    def abort(self) -> None:
        self._drain.set()
        self._release.set()
        if self._process.is_alive():
            self._process.terminate()
            self._process.join(5)
        if self._process.is_alive():
            self._process.kill()
            self._process.join(5)
        self._status.close()
        self._state = "FAILED"
        self.shutdown_verified = False

    @property
    def alive(self) -> bool:
        return self._process.is_alive()

    @property
    def drained(self) -> bool:
        return self._state == "DRAINED"

    @property
    def exit_code(self) -> int | None:
        value = self._process.exitcode
        return value if type(value) is int else None
