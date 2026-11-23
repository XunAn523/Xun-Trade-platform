"""Lifecycle wrapper for the already-authorized Consumer V2 Unix listener."""

from __future__ import annotations

import logging
import os
import select
import selectors
import socket
import struct
import sys
from collections import OrderedDict
from dataclasses import dataclass
from functools import partial
from hashlib import sha256
from queue import Empty, Full, Queue
from threading import Event, RLock, Thread
from time import monotonic_ns
from typing import Callable, Protocol, cast

from ..domain import ConsumerPrincipal
from ..oms.cold_intent import ColdIntentIndexBusy, ColdIntentIndexError
from ..persistence.stage_timing import StageTiming
from .base import ConsumerGatewayError, ConsumerSession
from .candidate_affinity import temporary_thread_affinity
from .cold_admission_v2 import (
    BoundedColdAdmissionsV2,
    ColdAdmissionResumeV2,
    ConsumerColdLookupRequiredV2,
    cold_admission_scope,
)
from .deferred_reads_v2 import BoundedDeferredReadsV2
from .fair_queue_v2 import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
    consumer_operation_lane_v2,
)
from .owner_ipc_v1 import (
    OwnerAuthorityV1,
    OwnerProtocolError,
)
from .owner_reactor_ipc import ReactorOwnerPortV1, reactor_owner_read_request, reactor_owner_request
from .owner_read_reactor import ReactorOwnerReadsV1
from .reactor_budget import FixedReactorBudgets, ReactorIoTurn
from .request_trace_v2 import BoundedRequestTraceWriterV2, request_trace_scope
from .submit_lane_v2 import BoundedSubmitLaneV2, SubmissionTicketV2
from .unix_v2 import (
    ConsumerUnixPeerV2,
    ConsumerUnixSocketError,
    ConsumerUnixSocketListenerV2,
    read_linux_peer_credentials,
)
from .wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    ConsumerWireError,
    DecodedFrameV2,
    MessageKind,
    decode_frame,
    encode_frame,
)

_LOG = logging.getLogger(__name__)

# The formal mixed profile releases 70 WRITE/SNAPSHOT requests every 10 ms.
# Keeping 96 decoded frames ready improved accept amortization but added a
# measured 8--28 ms frame-to-dispatch saw-tooth under the fresh mixed profile.
# A later 32-entry profile still filled to depth 30/31 at P95/P99 while the
# trace proved zero ingress rejection.  Use three 24-entry turns for the
# 70-request/10 ms target burst and yield after 2 ms; this lowers an admitted
# request's queue horizon without weakening EOF reclamation or capacity gates.
_REACTOR_READY_HIGH_WATERMARK = 24
_REACTOR_BURST_CONNECTION_THRESHOLD = 512
_REACTOR_BURST_READY_HIGH_WATERMARK = 512
_REACTOR_ACCEPT_BUDGET = 128
_REACTOR_DISPATCH_BATCH = 24
_REACTOR_BURST_DISPATCH_BATCH = 128
_REACTOR_DISPATCH_BUDGET_NS = 2_000_000
_REACTOR_BURST_DISPATCH_BUDGET_NS = 25_000_000
_REACTOR_IDLE_POLL_SECONDS = 0.005
_WRITE_ONLY = frozenset({ConsumerOperationLaneV2.WRITE})
_NO_EXCLUDED_LANES: frozenset[ConsumerOperationLaneV2] = frozenset()


class ConsumerManagedComponentV2(Protocol):
    def start(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class ConsumerUnixIngressStatsV2:
    socket_accepted_connections: int
    accepted_connections: int
    overloaded_connections: int
    pending_connections: int
    active_connections: int
    peak_pending_connections: int
    served_connections: int
    preframe_connections: int
    peak_preframe_connections: int
    expired_preframe_connections: int
    frame_wait_ns_total: int
    frame_wait_ns_max: int
    worker_queue_wait_ns_total: int
    worker_queue_wait_ns_max: int
    ready_frame_connections: int
    ready_on_accept_connections: int
    worker_started_connections: int
    overload_responses_sent: int
    overload_connections_closed_without_response: int
    overload_response_failures: int
    lane_rejected_connections: int
    pending_output_bytes: int
    peak_pending_output_bytes: int
    pending_capacity: int
    preframe_capacity: int
    total_connection_capacity: int
    overload_response_capacity: int
    reactor_output_capacity_bytes: int
    deferred_reads_pending: int = 0
    deferred_reads_completed: int = 0
    deferred_reads_rejected: int = 0
    cold_admissions_pending: int = 0
    cold_admissions_resumed: int = 0
    cold_admissions_rejected: int = 0
    observed_preframes_pending: int = 0
    peak_observed_preframes: int = 0
    preframe_readiness_events: int = 0
    submit_execution_pending: int = 0
    submit_execution_completed: int = 0
    submit_execution_not_started: int = 0
    submit_execution_unknown: int = 0
    submit_execution_capacity: int = 0
    submit_execution_failed: bool = False


@dataclass(frozen=True)
class _ReadyConnection:
    connection: socket.socket
    principal: ConsumerPrincipal
    initial_bytes: bytes = b""
    frame_ready_ns: int = 0


@dataclass
class _PreframeConnection:
    principal: ConsumerPrincipal
    accepted_ns: int
    buffer: bytearray


@dataclass(slots=True)
class _ReactorConnection:
    principal: ConsumerPrincipal
    accepted_ns: int
    progress_ns: int
    buffer: bytearray
    session: ConsumerSession | None = None
    output: bytes = b""
    output_offset: int = 0
    requests: int = 0
    queued: bool = False
    ready_ns: int = 0
    first_frame_ready: bool = False
    first_byte_ns: int = 0
    trace: dict[str, object] | None = None
    frame: DecodedFrameV2 | None = None
    frame_length: int = 0
    lane: ConsumerOperationLaneV2 = ConsumerOperationLaneV2.OTHER
    frame_decoded_ns: int = 0
    selector_registered: bool = False
    deferred: bool = False
    cold_token: int | None = None
    cold_attempts: int = 0
    cold_resume: ColdAdmissionResumeV2 | None = None
    resuming: bool = False
    preframe_observed_ns: int = 0
    preframe_deferred_ns: int = 0
    submit_ticket: SubmissionTicketV2 | None = None
    peer: ConsumerUnixPeerV2 | None = None
    owner_serial: int | None = None
    owner_authority: OwnerAuthorityV1 | None = None
    owner_read_serial: int | None = None
    owner_credit_retry: bool = False


class ConsumerUnixServiceV2:
    """Run one owned AF_UNIX listener without importing an Adapter or SDK.

    The constructor receives a fully assembled listener: principal mapping,
    Gateway V2, schema validation and OMS/EMS operation composition must be
    supplied by the runtime bootstrap.  This keeps socket lifecycle out of the
    execution and callback workers.
    """

    def __init__(
        self,
        listener: ConsumerUnixSocketListenerV2,
        *,
        worker_count: int = 16,
        pending_capacity: int = 2_048,
        frame_ready_ingress: bool = False,
        preframe_capacity: int = 4_096,
        preframe_timeout_seconds: float = 5.0,
        overload_response_capacity: int = 1_024,
        request_reactor: bool = False,
        managed_components: tuple[ConsumerManagedComponentV2, ...] = (),
        reactor_output_capacity_bytes: int = 64 * 1_024 * 1_024,
        deferred_read_workers: int = 0,
        deferred_read_capacity: int = 32,
        cold_admission_capacity: int = 0,
        submit_execution_capacity: int = 0,
        owner_port_factory: Callable[
            [BoundedFairOperationQueueV2[socket.socket]], ReactorOwnerPortV1[socket.socket]
        ]
        | None = None,
        owner_account_id: str = "",
        owner_read_factory: Callable[
            [BoundedFairOperationQueueV2[socket.socket]], ReactorOwnerReadsV1[socket.socket]
        ]
        | None = None,
    ) -> None:
        if owner_read_factory is not None and (owner_port_factory is None or deferred_read_workers):
            raise ConsumerUnixSocketError("owner read IPC replaces, not stacks deferred workers")
        self._owner_read_factory = owner_read_factory
        self._owner_reads: ReactorOwnerReadsV1[socket.socket] | None = None
        if owner_port_factory is not None and (
            not request_reactor
            or submit_execution_capacity
            or cold_admission_capacity
            or not owner_account_id
        ):
            raise ConsumerUnixSocketError("owner IPC requires exclusive reactor WRITE composition")
        self._owner_port_factory = owner_port_factory
        self._owner_account_id = owner_account_id
        self._owner_port: ReactorOwnerPortV1[socket.socket] | None = None
        if (
            type(submit_execution_capacity) is not int
            or not 0 <= submit_execution_capacity <= min(128, pending_capacity)
            or (submit_execution_capacity and not request_reactor)
        ):
            raise ConsumerUnixSocketError("invalid isolated submission limits")
        self._submit_execution_capacity = submit_execution_capacity
        self._submit_lane: BoundedSubmitLaneV2 | None = None
        self._submit_lane_drained = False
        self._submit_completed = self._submit_not_started = self._submit_unknown = 0
        if (
            type(cold_admission_capacity) is not int
            or not 0 <= cold_admission_capacity <= 1024
            or (cold_admission_capacity and not request_reactor)
        ):
            raise ConsumerUnixSocketError("invalid cold admission limits")
        self._cold_admission_capacity = cold_admission_capacity
        self._cold_admissions: BoundedColdAdmissionsV2 | None = None
        self._cold_admissions_drained = False
        self._cold_admissions_resumed = self._cold_admissions_rejected = 0
        if (
            type(deferred_read_workers) is not int
            or not 0 <= deferred_read_workers <= 16
            or type(deferred_read_capacity) is not int
            or not 1 <= deferred_read_capacity <= 1024
            or deferred_read_workers > deferred_read_capacity
            or (deferred_read_workers and not request_reactor)
            or (
                deferred_read_workers
                and deferred_read_capacity * MAX_FRAME_BYTES > reactor_output_capacity_bytes
            )
        ):
            raise ConsumerUnixSocketError("invalid deferred read limits")
        self._deferred_read_workers = deferred_read_workers
        self._deferred_read_capacity = deferred_read_capacity
        self._deferred_reads: BoundedDeferredReadsV2 | None = None
        self._deferred_reads_drained = False
        self._deferred_reads_completed = 0
        self._deferred_reads_rejected = 0
        if (
            worker_count <= 0
            or pending_capacity <= 0
            or preframe_capacity <= 0
            or preframe_timeout_seconds <= 0
            or overload_response_capacity <= 0
            or reactor_output_capacity_bytes <= 0
        ):
            raise ConsumerUnixSocketError("Consumer V2 ingress limits must be positive")
        self._listener = listener
        self._managed_components = tuple(managed_components)
        self._managed_components_started = False
        self._reactor_output_capacity_bytes = reactor_output_capacity_bytes
        if request_reactor and not listener.supports_request_reactor:
            raise ConsumerUnixSocketError("request reactor does not support live subscriptions")
        self._request_reactor = request_reactor
        self._diagnostic_dispatch_errors = (
            os.getenv("TRADECORE_DIAGNOSTIC_DISPATCH_ERRORS") == "1"
        )
        self._reactor_pending = 0
        self._frame_ready_ingress = frame_ready_ingress
        self._preframe_capacity = preframe_capacity
        self._preframe_timeout_ns = int(preframe_timeout_seconds * 1_000_000_000)
        self._stopped = Event()
        self._started = False
        self._pending: Queue[_ReadyConnection] = Queue(maxsize=pending_capacity)
        self._workers = [
            Thread(target=self._work, name=f"consumer-uds-{index}") for index in range(worker_count)
        ]
        # Queue-full replies never occupy an OMS worker or block the acceptor.
        # The queue is bounded; a saturated reply lane falls back to close.
        self._overload_responses: Queue[tuple[socket.socket, bytes]] = Queue(
            maxsize=overload_response_capacity
        )
        self._overload_workers = [
            Thread(target=self._respond_overload, name=f"consumer-uds-overload-{index}")
            for index in range(2)
        ]
        self._active: set[socket.socket] = set()
        # The main-thread SIGTERM handler may interrupt the acceptor while it
        # accounts for an admission; stop() must be re-entrant in that case.
        self._state_lock = RLock()
        self._socket_accepted_connections = 0
        self._accepted_connections = 0
        self._overloaded_connections = 0
        self._peak_pending_connections = 0
        self._served_connections = 0
        self._preframe_connections = 0
        self._peak_preframe_connections = 0
        self._expired_preframe_connections = 0
        self._observed_preframes_pending = 0
        self._peak_observed_preframes = 0
        self._preframe_readiness_events = 0
        self._frame_wait_ns_total = 0
        self._frame_wait_ns_max = 0
        self._worker_queue_wait_ns_total = 0
        self._worker_queue_wait_ns_max = 0
        self._ready_frame_connections = 0
        self._ready_on_accept_connections = 0
        self._worker_started_connections = 0
        self._overload_responses_sent = 0
        self._overload_connections_closed_without_response = 0
        self._overload_response_failures = 0
        self._lane_rejected_connections = 0
        self._reactor_pending_output_bytes = 0
        self._peak_reactor_pending_output_bytes = 0
        self._last_stats_log_ns = monotonic_ns()
        self._last_logged_signature = (0, 0, 0, 0)
        self._fatal: Exception | None = None
        self._request_trace: BoundedRequestTraceWriterV2 | None = None
        self._stage_trace: BoundedRequestTraceWriterV2 | None = None
        self._turn_budgets: FixedReactorBudgets | None = None
        self._background_thread_cpus: tuple[int, ...] | None = None

    def set_background_thread_cpus(self, cpus: tuple[int, ...]) -> None:
        if (
            self._started
            or self._background_thread_cpus is not None
            or not self._request_reactor
            or not 1 <= len(cpus) <= 2
            or any(type(cpu) is not int or cpu < 0 for cpu in cpus)
            or len(set(cpus)) != len(cpus)
        ):
            raise ConsumerUnixSocketError("fixed background thread CPUs required before startup")
        self._background_thread_cpus = cpus

    def set_reactor_turn_budgets(self, policy: FixedReactorBudgets) -> None:
        if (
            self._started
            or self._turn_budgets is not None
            or not self._request_reactor
            or type(policy) is not FixedReactorBudgets
            or policy.ready > self._pending.maxsize
        ):
            raise ConsumerUnixSocketError("reactor budgets must be fixed before startup")
        self._turn_budgets = policy

    def set_stage_trace(self, writer: BoundedRequestTraceWriterV2) -> None:
        if self._started or self._stage_trace is not None:
            raise ConsumerUnixSocketError("reactor stage diagnostics must be fixed before start")
        self._stage_trace = writer

    def set_request_trace(self, writer: BoundedRequestTraceWriterV2) -> None:
        if self._started or not self._request_reactor:
            raise ConsumerUnixSocketError("trace requires an unstarted request reactor")
        self._request_trace = writer

    @property
    def submission_shutdown_verified(self) -> bool:
        return (
            (self._owner_reads is None or self._owner_reads.shutdown_verified)
            and (self._owner_port is None or self._owner_port.shutdown_verified)
            and (self._submit_lane is None or self._submit_lane_drained)
        )

    @property
    def owner_submission_listener(self) -> ConsumerUnixSocketListenerV2:
        """Composition-only port; it never exposes a second socket or Book."""
        return self._listener

    def ingress_stats(self) -> ConsumerUnixIngressStatsV2:
        with self._state_lock:
            return ConsumerUnixIngressStatsV2(
                socket_accepted_connections=self._socket_accepted_connections,
                accepted_connections=self._accepted_connections,
                overloaded_connections=self._overloaded_connections,
                pending_connections=self._pending.qsize() + self._reactor_pending,
                active_connections=len(self._active),
                peak_pending_connections=self._peak_pending_connections,
                served_connections=self._served_connections,
                preframe_connections=self._preframe_connections,
                peak_preframe_connections=self._peak_preframe_connections,
                expired_preframe_connections=self._expired_preframe_connections,
                frame_wait_ns_total=self._frame_wait_ns_total,
                frame_wait_ns_max=self._frame_wait_ns_max,
                worker_queue_wait_ns_total=self._worker_queue_wait_ns_total,
                worker_queue_wait_ns_max=self._worker_queue_wait_ns_max,
                ready_frame_connections=self._ready_frame_connections,
                ready_on_accept_connections=self._ready_on_accept_connections,
                worker_started_connections=self._worker_started_connections,
                overload_responses_sent=self._overload_responses_sent,
                overload_connections_closed_without_response=(
                    self._overload_connections_closed_without_response
                ),
                overload_response_failures=self._overload_response_failures,
                lane_rejected_connections=self._lane_rejected_connections,
                pending_output_bytes=self._reactor_pending_output_bytes,
                peak_pending_output_bytes=self._peak_reactor_pending_output_bytes,
                pending_capacity=self._pending.maxsize,
                preframe_capacity=self._preframe_capacity,
                total_connection_capacity=self._preframe_capacity + self._pending.maxsize,
                overload_response_capacity=self._overload_responses.maxsize,
                reactor_output_capacity_bytes=self._reactor_output_capacity_bytes,
                deferred_reads_pending=(
                    self._deferred_reads.outstanding if self._deferred_reads is not None else 0
                ),
                deferred_reads_completed=self._deferred_reads_completed,
                deferred_reads_rejected=self._deferred_reads_rejected,
                cold_admissions_pending=self._cold_admissions.outstanding
                if self._cold_admissions
                else 0,
                cold_admissions_resumed=self._cold_admissions_resumed,
                cold_admissions_rejected=self._cold_admissions_rejected,
                observed_preframes_pending=self._observed_preframes_pending,
                peak_observed_preframes=self._peak_observed_preframes,
                preframe_readiness_events=self._preframe_readiness_events,
                submit_execution_pending=(
                    self._owner_port.outstanding
                    if self._owner_port is not None
                    else self._submit_lane.outstanding
                    if self._submit_lane
                    else 0
                ),
                submit_execution_completed=self._submit_completed,
                submit_execution_not_started=self._submit_not_started,
                submit_execution_unknown=self._submit_unknown,
                submit_execution_capacity=self._submit_execution_capacity,
                submit_execution_failed=(
                    self._owner_port.failed
                    if self._owner_port is not None
                    else self._submit_lane.failed
                    if self._submit_lane
                    else False
                ),
            )

    def _log_ingress_stats(self) -> None:
        stats = self.ingress_stats()
        _LOG.info(
            "Consumer UDS ingress stats socket_accepted=%d accepted=%d overloaded=%d served=%d "
            "pending=%d active=%d peak_pending=%d preframe=%d peak_preframe=%d "
            "expired_preframe=%d frame_wait_ns_total=%d frame_wait_ns_max=%d "
            "worker_queue_wait_ns_total=%d worker_queue_wait_ns_max=%d "
            "ready_frames=%d ready_on_accept=%d worker_started=%d overload_replied=%d "
            "overload_closed_without_reply=%d overload_reply_failures=%d "
            "lane_rejected=%d pending_output_bytes=%d peak_pending_output_bytes=%d",
            stats.socket_accepted_connections,
            stats.accepted_connections,
            stats.overloaded_connections,
            stats.served_connections,
            stats.pending_connections,
            stats.active_connections,
            stats.peak_pending_connections,
            stats.preframe_connections,
            stats.peak_preframe_connections,
            stats.expired_preframe_connections,
            stats.frame_wait_ns_total,
            stats.frame_wait_ns_max,
            stats.worker_queue_wait_ns_total,
            stats.worker_queue_wait_ns_max,
            stats.ready_frame_connections,
            stats.ready_on_accept_connections,
            stats.worker_started_connections,
            stats.overload_responses_sent,
            stats.overload_connections_closed_without_response,
            stats.overload_response_failures,
            stats.lane_rejected_connections,
            stats.pending_output_bytes,
            stats.peak_pending_output_bytes,
        )

    def start(self) -> None:
        if self._started:
            raise ConsumerUnixSocketError("Consumer V2 service is already running")
        started_components: list[ConsumerManagedComponentV2] = []
        try:
            for component in self._managed_components:
                component.start()
                started_components.append(component)
            self._managed_components_started = True
            self._listener.start()
        except Exception:
            for component in reversed(started_components):
                component.close()
            self._managed_components_started = False
            raise
        self._started = True
        if not self._request_reactor:
            for worker in self._workers:
                worker.start()
        for worker in self._overload_workers:
            worker.start()

    def serve_forever(self) -> None:
        if not self._started:
            raise ConsumerUnixSocketError("Consumer V2 service is not running")
        try:
            if self._request_reactor:
                self._serve_request_reactor()
            elif self._frame_ready_ingress:
                self._serve_frame_ready()
            else:
                self._serve_legacy_accept()
        finally:
            self._log_ingress_stats()
            self.stop()
            for worker in self._workers:
                if worker.is_alive():
                    worker.join()
            for worker in self._overload_workers:
                if worker.is_alive():
                    worker.join()
            if self._fatal is not None:
                raise ConsumerUnixSocketError("Consumer V2 ingress worker failed") from self._fatal

    def _serve_request_reactor(self) -> None:
        """Bounded, single-owner request transport for the local OMS composition.

        Socket reads, partial writes and idle persistent connections never wait
        inside an OMS worker. Each turn dispatches at most 24 complete requests;
        a connection has only one outstanding response. The existing dispatcher
        still performs authentication, validation and journal-before-receipt.
        This mode is opt-in for handlers with no remote I/O or live subscription.
        """
        states: dict[socket.socket, _ReactorConnection] = {}
        ready = BoundedFairOperationQueueV2[socket.socket](
            total_capacity=self._pending.maxsize,
            lane_capacities={
                ConsumerOperationLaneV2.WRITE: self._pending.maxsize,
                ConsumerOperationLaneV2.REPLAY: self._pending.maxsize,
                ConsumerOperationLaneV2.SNAPSHOT: max(1, self._pending.maxsize // 4),
                ConsumerOperationLaneV2.OTHER: max(1, self._pending.maxsize // 8),
            },
        )
        pending_output_bytes = 0
        listener_socket = self._listener.listening_socket
        listener_socket.setblocking(False)
        timeout_ns = int(self._listener.connection_timeout_seconds * 1_000_000_000)
        last_expiry_ns = monotonic_ns()
        connection_capacity = self._preframe_capacity + self._pending.maxsize
        # Readable first frames which cannot yet enter the decoded-ready
        # window are disarmed once, then resumed in observation order. Leaving
        # all of them level-triggered made every 2 ms turn enumerate the same
        # hundreds/thousands of sockets although only a few could be read.
        # Keys are owned states, not extra capacity; close/expiry removes them.
        observed_preframes: OrderedDict[socket.socket, int] = OrderedDict()
        cycle: StageTiming | None = None
        turn: ReactorIoTurn | None = None
        policy: FixedReactorBudgets | None = self._turn_budgets
        decode: Callable[[bytes], DecodedFrameV2] = decode_frame
        accept = self._listener.accept_connection
        with selectors.DefaultSelector() as selector:
            selector.register(listener_socket, selectors.EVENT_READ)
            owner: ReactorOwnerPortV1[socket.socket] | None = (
                self._owner_port_factory(ready) if self._owner_port_factory else None
            )
            self._owner_port = owner
            owner_reads = self._owner_read_factory(ready) if self._owner_read_factory else None
            self._owner_reads = owner_reads
            owner_read_inflight: dict[
                int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]
            ] = {}
            if owner_reads is not None:
                for read_channel in owner_reads.channels:
                    selector.register(read_channel, selectors.EVENT_READ)
            owner_inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]] = {}
            next_owner_serial = 0
            if owner is not None:
                selector.register(owner.channel, selectors.EVENT_READ)
            reads = (
                BoundedDeferredReadsV2(
                    workers=self._deferred_read_workers,
                    capacity=self._deferred_read_capacity,
                    maximum_response_bytes=MAX_FRAME_BYTES,
                )
                if self._deferred_read_workers
                else None
            )
            inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]] = {}
            next_read_token = 0
            cold = (
                BoundedColdAdmissionsV2(self._cold_admission_capacity)
                if self._cold_admission_capacity
                else None
            )
            cold_inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]] = {}
            next_cold_token = 0
            submissions: BoundedSubmitLaneV2 | None = (
                BoundedSubmitLaneV2(
                    capacity=self._submit_execution_capacity,
                    maximum_response_bytes=MAX_FRAME_BYTES,
                    stopped=self._stopped,
                )
                if self._submit_execution_capacity
                else None
            )
            submit_inflight: dict[
                int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]
            ] = {}
            next_submit_token = 0
            if submissions is not None:
                self._submit_lane = submissions
                with temporary_thread_affinity(self._background_thread_cpus):
                    submissions.start()
                selector.register(submissions.reader, selectors.EVENT_READ)
            if cold is not None:
                self._cold_admissions = cold
                selector.register(cold.reader, selectors.EVENT_READ)
            if reads is not None:
                self._deferred_reads = reads
                with temporary_thread_affinity(self._background_thread_cpus):
                    reads.start()
                selector.register(reads.reader, selectors.EVENT_READ)

            def disarm(connection: socket.socket, state: _ReactorConnection) -> None:
                if not state.selector_registered:
                    return
                try:
                    selector.unregister(connection)
                except (KeyError, ValueError):
                    pass
                state.selector_registered = False

            def arm(connection: socket.socket, state: _ReactorConnection, events: int) -> None:
                if state.selector_registered:
                    selector.modify(connection, events)
                else:
                    selector.register(connection, events)
                    state.selector_registered = True

            def close(connection: socket.socket, *, transfer: bool = False) -> None:
                nonlocal pending_output_bytes
                state = states.pop(connection, None)
                if state is None:
                    return
                if (
                    self._diagnostic_dispatch_errors
                    and not transfer
                    and not state.requests
                    and state.buffer
                ):
                    print(
                        "tradecore pre-dispatch close: "
                        f"queued={state.queued} deferred={state.deferred} "
                        f"frame={state.frame is not None} bytes={len(state.buffer)}",
                        file=sys.stderr,
                        flush=True,
                    )
                if state.owner_read_serial is not None and owner_reads is not None:
                    owner_reads.dispose(state.owner_read_serial)
                    owner_read_inflight.pop(state.owner_read_serial, None)
                    state.owner_read_serial = None
                    state.deferred = False  # no local worker/session still references it
                if state.submit_ticket is not None:
                    state.submit_ticket.cancel_before_start()
                if state.owner_serial is not None and not state.deferred and owner is not None:
                    owner.dispose(state.owner_serial)
                    state.owner_serial = None
                observed_preframes.pop(connection, None)
                self._observed_preframes_pending = len(observed_preframes)
                if state.queued:
                    ready.discard(connection, state.lane)
                    state.queued = False
                    self._reactor_pending = ready.used
                    self._peak_pending_connections = max(self._peak_pending_connections, ready.used)
                pending_output_bytes -= max(0, len(state.output) - state.output_offset)
                self._reactor_pending_output_bytes = pending_output_bytes
                if (
                    not state.deferred
                    and state.trace is not None
                    and self._request_trace is not None
                ):
                    state.trace.update(
                        outcome="closed_before_send_complete", closed_ns=monotonic_ns()
                    )
                    self._request_trace.submit(state.trace)
                    state.trace = None
                disarm(connection, state)
                if not state.deferred and state.session is not None:
                    self._listener.close_authenticated_session(state.session)
                if not state.deferred and state.cold_token is not None and cold is not None:
                    cold.release(state.cold_token)
                    state.cold_token = None
                    state.cold_resume = None
                with self._state_lock:
                    self._active.discard(connection)
                    if state.requests:
                        self._served_connections += 1
                    elif not state.first_frame_ready:
                        self._preframe_connections -= 1
                if not transfer:
                    connection.close()

            def schedule(connection: socket.socket, state: _ReactorConnection) -> None:
                if (
                    state.deferred
                    or state.queued
                    or state.output
                    or len(state.buffer) < HEADER_SIZE_BYTES
                ):
                    return
                length = HEADER_SIZE_BYTES + struct.unpack_from(">I", state.buffer, 24)[0]
                if length > MAX_FRAME_BYTES:
                    raise ConsumerUnixSocketError("oversize request frame")
                if len(state.buffer) < length:
                    return
                frame_ready_ns = monotonic_ns()
                # Freeze exactly one complete request before mutating the
                # receive buffer.  Building bytes from a view avoids the
                # bytearray slice that previously made a second full-frame
                # copy on every request.
                frame = decode(
                    bytes(state.buffer)
                    if len(state.buffer) == length
                    else bytes(memoryview(state.buffer)[:length])
                )
                state.frame = frame
                state.frame_length = length
                state.frame_decoded_ns = monotonic_ns()
                state.lane = consumer_operation_lane_v2(frame.payload.get("operation"))
                if self._request_trace is not None and state.trace is None:
                    state.trace = {
                        "accepted_ns": state.accepted_ns,
                        "first_byte_observed_ns": state.first_byte_ns,
                        "preframe_deferred_ns": state.preframe_deferred_ns,
                        "frame_ready_ns": frame_ready_ns,
                        "decode_before_dispatch": True,
                        "pending_depth": len(ready),
                        "operation_lane": state.lane.value,
                        "lane_queue_depth": ready.depth(state.lane),
                        "request_bytes": length,
                    }
                if not state.first_frame_ready:
                    state.first_frame_ready = True
                    self._preframe_connections -= 1
                    self._ready_frame_connections += 1
                    duration = monotonic_ns() - state.accepted_ns
                    self._frame_wait_ns_total += duration
                    self._frame_wait_ns_max = max(self._frame_wait_ns_max, duration)
                if not ready.offer(connection, state.lane):
                    self._lane_rejected_connections += 1
                    frame = bytes(state.buffer[:length])
                    close(connection, transfer=True)
                    self._reject_overload(connection, initial_bytes=frame)
                    return
                state.queued = True
                state.ready_ns = monotonic_ns()
                # A complete frame no longer needs read readiness until its
                # response has been produced.  In the one-request profile,
                # frames are normally complete at accept time, so avoiding an
                # epoll add/delete pair here removes two redundant syscalls per
                # request.  Partial reads and partial writes remain armed.
                disarm(connection, state)
                self._reactor_pending = ready.used
                self._peak_pending_connections = max(self._peak_pending_connections, ready.used)

            def assign_output(
                state: _ReactorConnection, frame: DecodedFrameV2, response: bytes
            ) -> bool:
                nonlocal pending_output_bytes
                candidate = response
                if pending_output_bytes + len(candidate) > self._reactor_output_capacity_bytes:
                    if state.owner_serial is not None or state.owner_read_serial is not None:
                        # A committed WRITE must not turn into retryable output
                        # backpressure. Close without a receipt; retain source.
                        return False
                    request_id = frame.payload.get("request_id")
                    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
                        return False
                    candidate = encode_frame(
                        MessageKind.ERROR,
                        frame.header.correlation_id,
                        {
                            "message_type": "error",
                            "schema_version": 2,
                            "request_id": request_id,
                            "code": "RATE_LIMITED",
                            "retryable": True,
                            "detail": "OUTPUT_BACKPRESSURE",
                        },
                    )
                    if pending_output_bytes + len(candidate) > self._reactor_output_capacity_bytes:
                        return False
                    if state.trace is not None:
                        state.trace.update(
                            failure_stage="output_queue",
                            failure_code="RATE_LIMITED",
                        )
                state.output = candidate
                state.output_offset = 0
                pending_output_bytes += len(candidate)
                self._reactor_pending_output_bytes = pending_output_bytes
                self._peak_reactor_pending_output_bytes = max(
                    self._peak_reactor_pending_output_bytes, pending_output_bytes
                )
                return True

            def output_sent(sent: int) -> None:
                nonlocal pending_output_bytes
                pending_output_bytes -= sent
                self._reactor_pending_output_bytes = pending_output_bytes

            def write(connection: socket.socket, state: _ReactorConnection) -> None:
                self._write_reactor_output(
                    connection,
                    state,
                    turn=turn,
                    owner=owner,
                    owner_reads=owner_reads,
                    ready=ready,
                    owner_read_inflight=owner_read_inflight,
                    arm=arm,
                    disarm=disarm,
                    close=close,
                    schedule=schedule,
                    debit=output_sent,
                )

            def read(connection: socket.socket, state: _ReactorConnection) -> None:
                self._read_reactor_input(
                    connection, state, turn=turn, schedule=schedule, disarm=disarm, close=close
                )

            def run_submission(
                session: ConsumerSession,
                frame: DecodedFrameV2,
                resume: ColdAdmissionResumeV2 | None,
                trace: dict[str, object] | None,
            ) -> bytes:
                with request_trace_scope(trace), cold_admission_scope(resume):
                    return self._listener.response_for_submission(session, frame)

            complete_owner = partial(
                self._complete_owner,
                owner,
                owner_inflight,
                states,
                close,
                assign_output,
                write,
            )

            def complete_submissions() -> None:
                if submissions is None:
                    return
                submissions.clear_notification()
                for _ in range(_REACTOR_DISPATCH_BATCH):
                    result = submissions.take()
                    if result is None:
                        break
                    connection, state, frame = submit_inflight.pop(result.token)
                    ready.release(ConsumerOperationLaneV2.WRITE)
                    self._reactor_pending = ready.used
                    disarm(connection, state)
                    state.deferred = False
                    state.submit_ticket = None
                    state.trace = result.trace
                    self._submit_completed += 1
                    self._submit_not_started += int(not result.started)
                    self._submit_unknown += int(result.outcome_unknown)
                    if state.trace is not None:
                        now = monotonic_ns()
                        state.trace.setdefault("submit_result_observed_ns", now)
                        state.trace["submit_last_result_observed_ns"] = now
                        state.trace["submit_outcome_unknown"] = result.outcome_unknown
                    if states.get(connection) is not state:
                        if state.session is not None:
                            self._listener.close_authenticated_session(state.session)
                        if state.trace is not None and self._request_trace is not None:
                            state.trace.update(
                                outcome="closed_before_send_complete", closed_ns=monotonic_ns()
                            )
                            self._request_trace.submit(state.trace)
                        continue
                    if self._stopped.is_set() or result.outcome_unknown or result.precheck_failed:
                        # No fabricated retryable nack for a possibly committed
                        # write. The client must resolve its original identity.
                        close(connection)
                        continue
                    try:
                        if not result.started:
                            response = encode_frame(
                                MessageKind.ERROR,
                                frame.header.correlation_id,
                                {
                                    "message_type": "error",
                                    "schema_version": 2,
                                    "request_id": frame.payload["request_id"],
                                    "code": "RATE_LIMITED",
                                    "retryable": True,
                                    "detail": "SUBMIT_NOT_STARTED",
                                },
                            )
                            if assign_output(state, frame, response):
                                write(connection, state)
                            else:
                                close(connection)
                        elif result.cold is not None:
                            suspend_cold(
                                connection,
                                state,
                                frame,
                                ConsumerColdLookupRequiredV2(result.cold.key, result.cold.resolver),
                            )
                        elif result.response is not None and assign_output(
                            state, frame, result.response
                        ):
                            state.progress_ns = monotonic_ns()
                            write(connection, state)
                        else:
                            close(connection)
                    except (
                        ConsumerUnixSocketError,
                        ConsumerWireError,
                        ConsumerGatewayError,
                        OSError,
                    ):
                        close(connection)

            complete_reads = partial(
                self._complete_deferred_reads, reads, inflight, states, close, assign_output, write
            )

            def run_read(
                session: ConsumerSession, frame: DecodedFrameV2, trace: dict[str, object] | None
            ) -> bytes:
                # This private copy is never published or mutated by the reactor
                # until completion. Context-local instrumentation follows it.
                with request_trace_scope(trace):
                    if trace is not None:
                        trace["deferred_worker_started_ns"] = monotonic_ns()
                    return self._listener.response_for_deferred_read(session, frame)

            def cold_error(
                connection: socket.socket,
                state: _ReactorConnection,
                frame: DecodedFrameV2,
                code: str,
                detail: str,
            ) -> None:
                self._cold_admissions_rejected += 1
                state.resuming = False
                state.cold_resume = None
                response = encode_frame(
                    MessageKind.ERROR,
                    frame.header.correlation_id,
                    {
                        "message_type": "error",
                        "schema_version": 2,
                        "request_id": frame.payload["request_id"],
                        "code": code,
                        "retryable": True,
                        "detail": detail,
                    },
                )
                if state.trace is not None:
                    state.trace.update(
                        response_kind="ERROR", failure_stage="cold_admission", failure_code=code
                    )
                if assign_output(state, frame, response):
                    write(connection, state)
                else:
                    close(connection)

            def suspend_cold(
                connection: socket.socket,
                state: _ReactorConnection,
                frame: DecodedFrameV2,
                required: ConsumerColdLookupRequiredV2,
            ) -> None:
                nonlocal next_cold_token
                if (
                    cold is None
                    or frame.payload.get("operation") != "SubmitSignal"
                    or state.session is None
                ):
                    cold_error(
                        connection, state, frame, "INTERNAL_ERROR", "COLD_ADMISSION_UNAVAILABLE"
                    )
                    return
                if state.cold_attempts >= 3:
                    cold_error(
                        connection, state, frame, "RATE_LIMITED", "COLD_REVALIDATION_REQUIRED"
                    )
                    return
                next_cold_token += 1
                try:
                    accepted = cold.offer(next_cold_token, required)
                except ColdIntentIndexBusy:
                    accepted = False
                except ColdIntentIndexError:
                    cold_error(
                        connection, state, frame, "INTERNAL_ERROR", "COLD_EVIDENCE_UNAVAILABLE"
                    )
                    return
                except Exception:
                    cold_error(
                        connection, state, frame, "INTERNAL_ERROR", "COLD_EVIDENCE_UNAVAILABLE"
                    )
                    return
                if not accepted:
                    cold_error(connection, state, frame, "RATE_LIMITED", "COLD_LOOKUP_BACKPRESSURE")
                    return
                state.cold_attempts += 1
                state.cold_token = next_cold_token
                state.deferred = True
                if state.trace is not None:
                    state.trace.update(
                        cold_lookup_enqueued_ns=monotonic_ns(),
                        cold_lookup_attempts=state.cold_attempts,
                    )
                cold_inflight[next_cold_token] = (connection, state, frame)

            def complete_cold() -> None:
                if cold is None:
                    return
                cold.clear_notification()
                for _ in range(_REACTOR_DISPATCH_BATCH):
                    result = cold.take()
                    if result is None:
                        break
                    connection, state, frame = cold_inflight.pop(result.token)
                    state.deferred = False
                    if states.get(connection) is not state:
                        cold.release(result.token)
                        state.cold_token = None
                        if state.session is not None:
                            self._listener.close_authenticated_session(state.session)
                        if state.trace is not None and self._request_trace is not None:
                            state.trace.update(
                                outcome="closed_before_send_complete", closed_ns=monotonic_ns()
                            )
                            self._request_trace.submit(state.trace)
                        continue
                    if self._stopped.is_set() or monotonic_ns() - state.progress_ns > timeout_ns:
                        close(connection)
                        continue
                    if state.trace is not None:
                        state.trace["cold_lookup_completed_ns"] = monotonic_ns()
                    if result.error is not None or result.resolution is None:
                        cold.release(result.token)
                        state.cold_token = None
                        cold_error(
                            connection, state, frame, "INTERNAL_ERROR", "COLD_EVIDENCE_UNAVAILABLE"
                        )
                        continue
                    state.cold_resume = ColdAdmissionResumeV2(result.required, result.resolution)
                    state.frame = frame
                    state.frame_length = HEADER_SIZE_BYTES + frame.header.payload_length
                    state.resuming = True
                    state.lane = ConsumerOperationLaneV2.WRITE
                    if not ready.offer(connection, state.lane):
                        cold.release(result.token)
                        state.cold_token = None
                        state.cold_resume = None
                        cold_error(
                            connection, state, frame, "RATE_LIMITED", "COLD_RESUME_BACKPRESSURE"
                        )
                        continue
                    state.queued = True
                    state.ready_ns = monotonic_ns()
                    self._reactor_pending = ready.used

            if self._stage_trace is not None:
                original_read, original_write = read, write
                original_decode, original_accept = decode, accept

                def measured_read(connection: socket.socket, state: _ReactorConnection) -> None:
                    if cycle is not None:
                        cycle.call("read", original_read, connection, state)
                    else:
                        original_read(connection, state)

                def measured_write(connection: socket.socket, state: _ReactorConnection) -> None:
                    if cycle is not None:
                        cycle.call("write", original_write, connection, state)
                    else:
                        original_write(connection, state)

                def measured_decode(data: bytes) -> DecodedFrameV2:
                    return (
                        original_decode(data)
                        if cycle is None
                        else cycle.call("decode", original_decode, data)
                    )

                def measured_accept() -> tuple[socket.socket, ConsumerPrincipal]:
                    return (
                        original_accept()
                        if cycle is None
                        else cycle.call("accept", original_accept)
                    )

                read, write, decode, accept = (
                    measured_read,
                    measured_write,
                    measured_decode,
                    measured_accept,
                )

            try:
                while not self._stopped.is_set():
                    turn = ReactorIoTurn(policy) if policy is not None else None
                    cycle = (
                        StageTiming("reactor", self._stage_trace.submit)
                        if self._stage_trace is not None
                        else None
                    )
                    accepted_before = self._socket_accepted_connections
                    served_before = self._served_connections
                    if owner is not None:
                        owner.begin_turn()
                        owner.expire()
                        selector.modify(
                            owner.channel,
                            selectors.EVENT_READ
                            | (selectors.EVENT_WRITE if owner.wants_write else 0),
                        )
                    if owner_reads is not None:
                        owner_reads.expire()
                        for read_channel in owner_reads.channels:
                            selector.modify(read_channel, owner_reads.events(read_channel))
                    if cycle is None:
                        complete_submissions()
                        complete_reads()
                        complete_cold()
                    else:
                        cycle.call("completion", complete_submissions)
                        cycle.call("completion", complete_reads)
                        cycle.call("completion", complete_cold)
                    if submissions is not None and submissions.failed:
                        raise ConsumerUnixSocketError("submission execution owner failed")
                    excluded = self._excluded_owner_lanes(
                        submissions,
                        cast(ReactorOwnerPortV1[socket.socket] | None, owner),
                        owner_reads,
                    )
                    ready_window = (
                        policy.ready
                        if policy is not None
                        else _REACTOR_BURST_READY_HIGH_WATERMARK
                        if len(states) >= _REACTOR_BURST_CONNECTION_THRESHOLD
                        else _REACTOR_READY_HIGH_WATERMARK
                    )
                    select_timeout = (
                        0.0
                        if ready.has_eligible(excluded)
                        or (
                            observed_preframes
                            and (submissions is None or len(ready) < ready_window)
                        )
                        or (reads is not None and reads.has_completed)
                        or (cold is not None and cold.has_completed)
                        or (submissions is not None and submissions.has_completed)
                        else _REACTOR_IDLE_POLL_SECONDS
                    )
                    events = (
                        selector.select(timeout=select_timeout)
                        if cycle is None
                        else cycle.call("select", selector.select, timeout=select_timeout)
                    )

                    # A fresh-per-request peer closes only after receiving its
                    # response.  Under a sustained connect burst those EOFs
                    # must be consumed before accepting more sockets; otherwise
                    # completed sessions remain in ``states``, the kernel
                    # accept queue grows, and healthy requests are eventually
                    # rejected by the total-connection gate.  Established
                    # persistent peers retain their ordered next-request path.
                    listener_ready = False
                    for key, mask in events:
                        connection = key.fileobj
                        if connection is listener_socket:
                            listener_ready = True
                            continue
                        selected_owner = cast(ReactorOwnerPortV1[socket.socket] | None, owner)
                        if selected_owner is not None and connection is selected_owner.channel:
                            if mask & selectors.EVENT_WRITE:
                                selected_owner.write_ready()
                            if mask & selectors.EVENT_READ:
                                complete_owner()
                            continue
                        if (
                            owner_reads is not None
                            and isinstance(connection, socket.socket)
                            and connection in owner_reads.channels
                        ):
                            serial = owner_reads.io(connection, mask)
                            if serial is not None:
                                self._complete_owner_read(
                                    serial,
                                    owner_reads,
                                    owner_read_inflight,
                                    close,
                                    assign_output,
                                    write,
                                )
                            continue
                        if reads is not None and connection is reads.reader:
                            # Drained at the next loop head, never from a worker.
                            continue
                        if cold is not None and connection is cold.reader:
                            continue
                        if submissions is not None and connection is submissions.reader:
                            continue
                        if not isinstance(connection, socket.socket):
                            continue
                        state = states.get(connection)
                        if state is None:
                            continue
                        if not state.requests:
                            disarm(connection, state)
                            observed_preframes[connection] = mask
                            state.preframe_observed_ns = monotonic_ns()
                            self._preframe_readiness_events += 1
                            continue
                        try:
                            if state.submit_ticket is not None or state.owner_serial is not None:
                                poller = select.poll()
                                poller.register(connection, select.POLLHUP | select.POLLERR)
                                if poller.poll(0):
                                    close(connection)
                                    continue
                            if mask & selectors.EVENT_WRITE:
                                write(connection, state)
                            elif mask & selectors.EVENT_READ:
                                read(connection, state)
                        except (ConsumerUnixSocketError, ConsumerWireError, OSError):
                            close(connection)

                    # Read already-accepted first frames before adding more
                    # sockets to the selector.  Level-triggered readiness keeps
                    # unprocessed peers eligible for the next bounded turn.
                    burst_mode = (
                        policy is None and len(states) >= _REACTOR_BURST_CONNECTION_THRESHOLD
                    )
                    ready_high_watermark = (
                        policy.ready
                        if policy is not None
                        else _REACTOR_BURST_READY_HIGH_WATERMARK
                        if burst_mode
                        else _REACTOR_READY_HIGH_WATERMARK
                    )
                    self._observed_preframes_pending = len(observed_preframes)
                    self._peak_observed_preframes = max(
                        self._peak_observed_preframes, len(observed_preframes)
                    )
                    if len(ready) < ready_high_watermark:
                        for _ in range(
                            policy.reads if policy is not None else _REACTOR_ACCEPT_BUDGET
                        ):
                            if not observed_preframes:
                                break
                            connection, mask = observed_preframes.popitem(last=False)
                            self._observed_preframes_pending = len(observed_preframes)
                            state = states.get(connection)
                            if state is None or state.requests:
                                continue
                            state.preframe_deferred_ns += (
                                monotonic_ns() - state.preframe_observed_ns
                            )
                            state.preframe_observed_ns = 0
                            try:
                                if mask & selectors.EVENT_WRITE:
                                    write(connection, state)
                                elif mask & selectors.EVENT_READ:
                                    read(connection, state)
                                # Partial frames or a spurious readiness still
                                # need a new kernel notification. Complete
                                # frames stay disarmed until dispatch/send.
                                if connection in states and not state.queued:
                                    arm(connection, state, selectors.EVENT_READ)
                            except (ConsumerUnixSocketError, ConsumerWireError, OSError):
                                close(connection)
                            if len(ready) >= ready_high_watermark:
                                break

                    # Drain the kernel accept queue even when the low-latency
                    # decoded-ready window is full. New peers remain bounded
                    # pre-frame states until the next dispatch turn. This
                    # makes the configured 8,192 first-frame capacity real
                    # instead of accidentally reducing it to 24 sockets.
                    if listener_ready:
                        for _ in range(
                            policy.accepts if policy is not None else _REACTOR_ACCEPT_BUDGET
                        ):
                            if turn is not None and not turn.may_accept():
                                break
                            accept_started = monotonic_ns() if turn is not None else 0
                            try:
                                connection, principal = accept()
                            except (BlockingIOError, InterruptedError):
                                break
                            except ConsumerUnixSocketError:
                                continue
                            finally:
                                if turn is not None:
                                    turn.accepts += 1
                                    turn.accept_ns += monotonic_ns() - accept_started
                            with self._state_lock:
                                self._socket_accepted_connections += 1
                            if (
                                len(states) >= connection_capacity
                                or self._preframe_connections >= self._preframe_capacity
                            ):
                                self._listener.accepted_connection_installed(connection)
                                self._reject_overload(connection)
                                continue
                            connection.setblocking(False)
                            now = monotonic_ns()
                            state = _ReactorConnection(principal, now, now, bytearray())
                            if owner is not None:
                                state.peer = read_linux_peer_credentials(connection)
                            states[connection] = state
                            self._preframe_connections += 1
                            self._peak_preframe_connections = max(
                                self._peak_preframe_connections, self._preframe_connections
                            )
                            try:
                                if len(ready) < ready_high_watermark:
                                    read(connection, state)
                                    if state.queued:
                                        self._ready_on_accept_connections += 1
                                    elif connection in states:
                                        arm(connection, state, selectors.EVENT_READ)
                                else:
                                    arm(connection, state, selectors.EVENT_READ)
                            except (ConsumerUnixSocketError, ConsumerWireError, OSError):
                                close(connection)
                            self._listener.accepted_connection_installed(connection)
                    burst_mode = (
                        policy is None and len(states) >= _REACTOR_BURST_CONNECTION_THRESHOLD
                    )
                    dispatch_batch = (
                        policy.dispatches
                        if policy is not None
                        else _REACTOR_BURST_DISPATCH_BATCH
                        if burst_mode
                        else _REACTOR_DISPATCH_BATCH
                    )
                    dispatch_budget_ns = (
                        policy.dispatch_ns
                        if policy is not None
                        else _REACTOR_BURST_DISPATCH_BUDGET_NS
                        if burst_mode
                        else _REACTOR_DISPATCH_BUDGET_NS
                    )
                    dispatch_deadline = monotonic_ns() + dispatch_budget_ns
                    dispatch_started = monotonic_ns() if cycle is not None else 0
                    dispatch_count = 0
                    for _ in range(min(dispatch_batch, len(ready))):
                        if (
                            policy is not None
                            and dispatch_count
                            and monotonic_ns() >= dispatch_deadline
                        ):
                            break  # includes earlier deferred branches that used continue
                        try:
                            connection, lane = ready.pop(
                                excluded=self._excluded_owner_lanes(submissions, owner, owner_reads)
                            )
                        except IndexError:
                            break  # owner full: leave WRITE FIFO intact; other lanes can run
                        dispatch_count += 1
                        self._reactor_pending = ready.used
                        state = states.get(connection)
                        if state is None:
                            continue
                        state.queued = False
                        frame = state.frame
                        try:
                            prepared = self._prepare_reactor_dispatch(
                                connection,
                                state,
                                lane=lane,
                                timeout_ns=timeout_ns,
                                cold=cold,
                                needs_owner_raw=owner is not None
                                and (
                                    lane is ConsumerOperationLaneV2.WRITE or owner_reads is not None
                                ),
                                close=close,
                            )
                            if prepared is None:
                                continue
                            frame, owner_raw = prepared
                            if (
                                owner is not None
                                and owner_reads is not None
                                and lane is not ConsumerOperationLaneV2.WRITE
                            ):
                                if state.session is None:
                                    state.session = self._listener.open_authenticated_session(
                                        state.principal
                                    )
                                self._listener.validate_owner_frontend_delivery(state.session)
                                self._listener.validate_owner_frontend_read(state.session, frame)
                                next_owner_serial += 1
                                request = reactor_owner_read_request(
                                    ready=owner.ready,
                                    serial=next_owner_serial,
                                    session=state.session,
                                    account_id=self._owner_account_id,
                                    peer=state.peer,
                                    raw=owner_raw,
                                    deadline_ns=state.progress_ns + timeout_ns,
                                )
                                if not owner_reads.offer(request, lane):
                                    # Another frontend can acquire the shared
                                    # bounded read credit after our lock-free
                                    # exclusion hint.  The owner has not seen
                                    # this request, so retain the original fair
                                    # queue credit and retry; closing here turns
                                    # ordinary capacity contention into a false
                                    # POST_SEND_NO_RESPONSE.
                                    if not ready.offer(connection, lane):
                                        raise ConsumerUnixSocketError(
                                            "owner read retry lost original credit"
                                        )
                                    state.frame = frame
                                    state.frame_length = len(owner_raw)
                                    state.owner_credit_retry = True
                                    state.queued = True
                                    self._reactor_pending = ready.used
                                    continue
                                state.owner_read_serial, state.deferred = next_owner_serial, True
                                owner_read_inflight[next_owner_serial] = (connection, state, frame)
                                continue
                            if owner is not None and lane is ConsumerOperationLaneV2.WRITE:
                                if state.session is None:
                                    state.session = self._listener.open_authenticated_session(
                                        state.principal
                                    )
                                self._listener.validate_owner_frontend_request(state.session, frame)
                                next_owner_serial += 1
                                request = reactor_owner_request(
                                    ready=owner.ready,
                                    serial=next_owner_serial,
                                    session=state.session,
                                    account_id=self._owner_account_id,
                                    peer=state.peer,
                                    raw=owner_raw,
                                    deadline_ns=state.progress_ns + timeout_ns,
                                )
                                if not owner.offer(request):
                                    # Shared total credit is authoritative and
                                    # may be won concurrently by another fixed
                                    # frontend.  No owner STARTED boundary was
                                    # crossed; requeue the exact decoded frame
                                    # instead of manufacturing a transport loss.
                                    if not ready.offer(connection, lane):
                                        raise ConsumerUnixSocketError(
                                            "owner WRITE retry lost original credit"
                                        )
                                    state.frame = frame
                                    state.frame_length = len(owner_raw)
                                    state.owner_credit_retry = True
                                    state.queued = True
                                    self._reactor_pending = ready.used
                                    continue
                                state.owner_serial, state.owner_authority = (
                                    next_owner_serial,
                                    request.identity.authority,
                                )
                                state.deferred = True
                                owner_inflight[next_owner_serial] = (connection, state, frame)
                                self._reactor_pending = ready.used
                                self._peak_pending_connections = max(
                                    self._peak_pending_connections, ready.used
                                )
                                if state.trace is not None:
                                    state.trace["ipc_frontend_offered_ns"] = monotonic_ns()
                                if owner.turn_batch_ready:
                                    # Hand the complete four-packet micro-batch
                                    # to the kernel now. ``begin_turn`` keeps
                                    # WRITE excluded for the rest of this turn,
                                    # so this cannot fill all 128 credits.
                                    owner.write_ready()
                                arm(connection, state, selectors.EVENT_READ)
                                continue
                            if submissions is not None and lane is ConsumerOperationLaneV2.WRITE:
                                if state.session is None:
                                    state.session = self._listener.open_authenticated_session(
                                        state.principal
                                    )
                                trace = dict(state.trace) if state.trace is not None else None
                                if trace is not None:
                                    enqueued = monotonic_ns()
                                    trace.setdefault("submit_enqueued_ns", enqueued)
                                    trace["submit_last_enqueued_ns"] = enqueued
                                next_submit_token += 1
                                if not ready.reserve(lane):
                                    raise ConsumerUnixSocketError("submission credit unavailable")
                                ticket = submissions.offer(
                                    next_submit_token,
                                    partial(
                                        run_submission,
                                        state.session,
                                        frame,
                                        state.cold_resume,
                                        trace,
                                    ),
                                    deadline_ns=state.progress_ns + timeout_ns,
                                    trace=trace,
                                )
                                if ticket is None:
                                    ready.release(lane)
                                    close(connection)
                                    continue
                                state.submit_ticket = ticket
                                state.cold_resume = None
                                state.deferred = True
                                submit_inflight[next_submit_token] = (connection, state, frame)
                                self._reactor_pending = ready.used
                                self._peak_pending_connections = max(
                                    self._peak_pending_connections, ready.used
                                )
                                # Observe full close while queued. SHUT_WR is
                                # retained as a valid response-receiving peer.
                                arm(connection, state, selectors.EVENT_READ)
                                continue
                            if reads is not None and self._listener.can_defer_read(frame):
                                if state.session is None:
                                    state.session = self._listener.open_authenticated_session(
                                        state.principal
                                    )
                                self._listener.validate_deferred_read(state.session, frame)
                                with request_trace_scope(state.trace):
                                    inline = self._listener.try_inline_cached_read(
                                        state.session, frame
                                    )
                                if inline is not None:
                                    if state.trace is not None:
                                        state.trace["inline_cached_read"] = True
                                    if not assign_output(state, frame, inline):
                                        close(connection)
                                    else:
                                        state.progress_ns = monotonic_ns()
                                        write(connection, state)
                                    if monotonic_ns() >= dispatch_deadline:
                                        break
                                    continue
                                next_read_token += 1
                                trace = dict(state.trace) if state.trace is not None else None
                                if trace is not None:
                                    trace["deferred_enqueued_ns"] = monotonic_ns()
                                if reads.offer(
                                    next_read_token,
                                    partial(run_read, state.session, frame, trace),
                                    trace,
                                ):
                                    state.deferred = True
                                    inflight[next_read_token] = (connection, state, frame)
                                    continue
                                self._deferred_reads_rejected += 1
                                response = encode_frame(
                                    MessageKind.ERROR,
                                    frame.header.correlation_id,
                                    {
                                        "message_type": "error",
                                        "schema_version": 2,
                                        "request_id": frame.payload["request_id"],
                                        "code": "RATE_LIMITED",
                                        "retryable": True,
                                        "detail": "READ_BACKPRESSURE",
                                    },
                                )
                            elif state.cold_resume is not None:
                                with (
                                    request_trace_scope(state.trace),
                                    cold_admission_scope(state.cold_resume),
                                ):
                                    response = (
                                        self._listener.response_for_single_request(
                                            state.principal, frame
                                        )
                                        if state.session is None
                                        else self._listener.response_for_frame(
                                            state.session,  # pyright: ignore[reportArgumentType]
                                            frame,
                                        )
                                    )
                            elif state.trace is not None:
                                with request_trace_scope(state.trace):
                                    response = (
                                        self._listener.response_for_single_request(
                                            state.principal, frame
                                        )
                                        if state.session is None
                                        else self._listener.response_for_frame(state.session, frame)
                                    )
                            else:
                                response = (
                                    self._listener.response_for_single_request(
                                        state.principal, frame
                                    )
                                    if state.session is None
                                    else self._listener.response_for_frame(state.session, frame)
                                )
                            state.cold_resume = None
                            if not assign_output(state, frame, response):
                                close(connection)
                                continue
                            state.progress_ns = monotonic_ns()
                            write(connection, state)
                        except ConsumerColdLookupRequiredV2 as required:
                            state.cold_resume = None
                            if frame is None:
                                close(connection)
                            else:
                                suspend_cold(connection, state, frame, required)
                        except (
                            ConsumerUnixSocketError,
                            ConsumerWireError,
                            ConsumerGatewayError,
                            OSError,
                        ) as error:
                            if self._diagnostic_dispatch_errors:
                                print(
                                    "tradecore reactor dispatch failure: "
                                    f"{type(error).__name__}: {error}",
                                    file=sys.stderr,
                                    flush=True,
                                )
                            close(connection)
                        # Bound CPU monopolization as well as request count. This
                        # is cooperative: a single long handler is not preempted.
                        if monotonic_ns() >= dispatch_deadline:
                            break
                    # Requests offered during this dispatch turn are flushed
                    # together under the owner's four-packet/64 KiB bound.
                    # A non-writable socket simply retains the exact packet
                    # cursor for EPOLLOUT; this never waits or releases credit.
                    if owner is not None and owner.wants_write:
                        owner.write_ready()
                    now = monotonic_ns()
                    dispatch_finished = now
                    if now - last_expiry_ns >= 100_000_000:
                        last_expiry_ns = now
                        for connection, state in tuple(states.items()):
                            # Incomplete frames have an absolute deadline; idle/read and
                            # slow-write sessions have a bounded inactivity deadline.
                            since = state.accepted_ns if not state.requests else state.progress_ns
                            deadline = (
                                timeout_ns if state.first_frame_ready else self._preframe_timeout_ns
                            )
                            if now - since > deadline:
                                if not state.first_frame_ready:
                                    self._expired_preframe_connections += 1
                                close(connection)
                    # Request-reactor diagnostics are published by the
                    # asynchronous trace writer's status provider.  Never
                    # write the per-second INFO record from this sole socket
                    # owner: a busy journald pipe would otherwise stop accept,
                    # dispatch and send together.  The final shutdown summary
                    # remains synchronous after the reactor has left the hot
                    # path; legacy worker modes retain their periodic log.
                    if cycle is not None:
                        oldest = next(iter(observed_preframes), None)
                        observed = states.get(oldest) if oldest is not None else None
                        age = (
                            max(0, monotonic_ns() - observed.preframe_observed_ns)
                            if observed is not None and observed.preframe_observed_ns
                            else 0
                        )
                        cycle.finish(
                            {
                                "turn_completed": True,
                                "accepted": self._socket_accepted_connections - accepted_before,
                                "served_connections": self._served_connections - served_before,
                                "dispatch_attempts": dispatch_count,
                                "dispatch_wall_ns": dispatch_finished - dispatch_started,
                                "ready": len(ready),
                                "connections": len(states),
                                "observed_preframes": len(observed_preframes),
                                "oldest_observed_preframe_age_ns": age,
                                "pending_output_bytes": pending_output_bytes,
                                **(
                                    {
                                        "accept_budget_calls": turn.accepts,
                                        "receive_budget_calls": turn.reads,
                                        "send_budget_calls": turn.writes,
                                        "receive_reserved_bytes": turn.read_bytes,
                                        "send_bytes": turn.write_bytes,
                                        "accept_work_ns": turn.accept_ns,
                                        "receive_work_ns": turn.read_ns,
                                        "send_syscall_ns": turn.write_ns,
                                    }
                                    if turn is not None
                                    else {}
                                ),
                            }
                        )
                        cycle = None
            except (OSError, ValueError):
                if not self._stopped.is_set():
                    raise
            finally:
                turn = None
                if cycle is not None:
                    cycle.finish({"turn_completed": False})
                    cycle = None
                for connection in tuple(states):
                    close(connection)
                if owner_reads is not None:
                    self._drain_owner_reads(owner_reads)
                if owner is not None:
                    # Socket disappearance is not cancellation. Retain the
                    # original credits until authoritative completion or fail.
                    self._drain_owner(
                        owner,
                        owner_inflight,
                        complete_owner,
                    )
                if submissions is not None:
                    submit_close_error = None
                    try:
                        submissions.close()
                    except Exception as exc:
                        submit_close_error = exc
                    while submissions.has_completed:
                        complete_submissions()
                    if submit_inflight or submissions.outstanding:
                        raise ConsumerUnixSocketError("submission drain leaked reserved slots")
                    if submit_close_error is not None:
                        raise ConsumerUnixSocketError(
                            "submission owner drain failed"
                        ) from submit_close_error
                    self._submit_lane_drained = True
                if reads is not None:
                    # Reads are immutable: do not retry them or fabricate success
                    # on shutdown. Accepted work drains before session/component
                    # teardown; a stuck read fails the lifecycle explicitly.
                    reads.close()
                    while inflight:
                        complete_reads()
                    self._deferred_reads_drained = True
                if cold is not None:
                    cold.close()
                    while cold_inflight:
                        complete_cold()
                    if cold.outstanding:
                        raise ConsumerUnixSocketError("cold admission drain leaked reserved slots")
                    self._cold_admissions_drained = True
                self._reactor_pending = 0

    @staticmethod
    def _excluded_owner_lanes(
        submissions: BoundedSubmitLaneV2 | None,
        owner: ReactorOwnerPortV1[socket.socket] | None,
        reads: ReactorOwnerReadsV1[socket.socket] | None,
    ) -> frozenset[ConsumerOperationLaneV2]:
        excluded = set[ConsumerOperationLaneV2]()
        if (submissions is not None and submissions.full) or (owner is not None and owner.full):
            excluded.add(ConsumerOperationLaneV2.WRITE)
        if reads is not None:
            excluded.update(reads.excluded_lanes)
        return frozenset(excluded)

    def _prepare_reactor_dispatch(
        self,
        connection: socket.socket,
        state: _ReactorConnection,
        *,
        lane: ConsumerOperationLaneV2,
        timeout_ns: int,
        cold: BoundedColdAdmissionsV2 | None,
        needs_owner_raw: bool,
        close: Callable[[socket.socket], None],
    ) -> tuple[DecodedFrameV2, bytes] | None:
        resuming = state.resuming
        state.resuming = False
        credit_retry = state.owner_credit_retry
        state.owner_credit_retry = False
        if resuming:
            # A full peer close is visible even with unread
            # pipelined bytes. Do not mistake SHUT_WR alone
            # for disconnect: read-half-close peers can still
            # receive their response. This is only an observed
            # pre-commit fence, never a cancellation guarantee.
            poller = select.poll()
            peer_gone = connection.fileno() < 0
            if not peer_gone:
                poller.register(connection, select.POLLHUP | select.POLLERR)
                peer_gone = bool(poller.poll(0))
            if (
                self._stopped.is_set()
                or peer_gone
                or monotonic_ns() - state.progress_ns > timeout_ns
            ):
                close(connection)
                return None
            assert cold is not None and state.cold_token is not None
            cold.release(state.cold_token)
            state.cold_token = None
            self._cold_admissions_resumed += 1
        else:
            state.cold_attempts = 0
        if state.trace is not None:
            state.trace["cold_resume_started_ns" if resuming else "dispatch_started_ns"] = (
                monotonic_ns()
            )
        if not state.requests:
            with self._state_lock:
                self._active.add(connection)
                self._accepted_connections += 1
                self._worker_started_connections += 1
            # Mark admitted before session creation so failure cleanup balances.
            state.requests = 1
            if self._listener.request_limit != 1 or (
                cold is not None
                and state.frame is not None
                and state.frame.payload.get("operation") == "SubmitSignal"
            ):
                state.session = self._listener.open_authenticated_session(state.principal)
        elif not resuming and not credit_retry:
            state.requests += 1
        if state.requests > self._listener.request_limit:
            raise ConsumerUnixSocketError("connection request limit exceeded")
        if not credit_retry:
            duration = monotonic_ns() - state.ready_ns
            self._worker_queue_wait_ns_total += duration
            self._worker_queue_wait_ns_max = max(self._worker_queue_wait_ns_max, duration)
        length = state.frame_length
        frame = state.frame
        if frame is None or not length or lane != state.lane:
            raise ConsumerUnixSocketError("classified request frame is missing")
        owner_raw = bytes(memoryview(state.buffer)[:length]) if needs_owner_raw else b""
        state.frame = None
        state.frame_length = 0
        # A one-request connection closes immediately after
        # its response.  Retaining its buffer until close
        # avoids a redundant bytearray memmove/resize in the
        # formal fresh-per-request profile.  Persistent
        # transports still consume exactly one frame.
        if self._listener.request_limit != 1 and not resuming:
            del state.buffer[:length]
        if state.trace is not None:
            request_id = frame.payload.get("request_id")
            operation = frame.payload.get("operation")
            state.trace.update(
                frame_decoded_ns=state.frame_decoded_ns,
                correlation_id=frame.header.correlation_id,
                request_id_sha256=sha256(
                    request_id.encode("utf-8", errors="surrogatepass")
                ).hexdigest()
                if isinstance(request_id, str)
                else None,
                operation=operation
                if operation
                in (
                    "ListAccounts",
                    "SubmitSignal",
                    "CancelOrder",
                    "GetOrder",
                    "GetOrderAuditTrail",
                    "GetAccountOrderSnapshot",
                    "CreateOrderSnapshot",
                    "ReadOrderSnapshotPage",
                    "PollEvents",
                    "CreateReplayPartitions",
                    "SubscribeOrderEvents",
                )
                else "UNKNOWN",
            )
        return frame, owner_raw

    def _complete_deferred_reads(
        self,
        reads: BoundedDeferredReadsV2 | None,
        inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]],
        states: dict[socket.socket, _ReactorConnection],
        close: Callable[[socket.socket], None],
        assign_output: Callable[[_ReactorConnection, DecodedFrameV2, bytes], bool],
        write: Callable[[socket.socket, _ReactorConnection], None],
    ) -> None:
        if reads is None:
            return
        reads.clear_notification()
        # Work per turn is bounded just like normal dispatch. Remaining
        # completions are drained before the next potentially idle select.
        for _ in range(_REACTOR_DISPATCH_BATCH):
            result = reads.take()
            if result is None:
                break
            connection, state, frame = inflight.pop(result.token)
            state.deferred = False
            state.trace = result.trace
            self._deferred_reads_completed += 1
            if states.get(connection) is not state:
                if state.session is not None:
                    self._listener.close_authenticated_session(state.session)
                if state.trace is not None and self._request_trace is not None:
                    state.trace.update(
                        outcome="closed_before_send_complete", closed_ns=monotonic_ns()
                    )
                    self._request_trace.submit(state.trace)
                continue
            try:
                assert state.session is not None
                # Revocation/epoch changes while a page was read must
                # never turn an old authorized result into a disclosure.
                response = result.response
                if result.error is not None:
                    response = encode_frame(
                        MessageKind.ERROR,
                        frame.header.correlation_id,
                        {
                            "message_type": "error",
                            "schema_version": 2,
                            "request_id": frame.payload["request_id"],
                            "code": "INTERNAL_ERROR",
                            "retryable": True,
                        },
                    )
                    if state.trace is not None:
                        state.trace.update(
                            response_kind="ERROR",
                            failure_stage="deferred_read",
                            failure_code="INTERNAL_ERROR",
                        )
                assert response is not None
                with request_trace_scope(state.trace):
                    response = self._listener.validate_deferred_delivery(
                        state.session, frame, response
                    )
                if state.trace is not None:
                    state.trace["deferred_delivery_ns"] = monotonic_ns()
                if not assign_output(state, frame, response):
                    close(connection)
                    continue
                state.progress_ns = monotonic_ns()
                write(connection, state)
            except (
                ConsumerUnixSocketError,
                ConsumerGatewayError,
                ConsumerWireError,
                OSError,
            ):
                close(connection)

    def _read_reactor_input(
        self,
        connection: socket.socket,
        state: _ReactorConnection,
        *,
        turn: ReactorIoTurn | None,
        schedule: Callable[[socket.socket, _ReactorConnection], None],
        disarm: Callable[[socket.socket, _ReactorConnection], None],
        close: Callable[[socket.socket], None],
    ) -> None:
        if state.queued:
            return
        capacity = turn.receive_capacity() if turn is not None else 65536
        if capacity <= 0:
            return
        read_started = monotonic_ns() if turn is not None else 0
        if turn is not None:
            # Reserve the maximum recv size, including rejected ancillary
            # input. Actual bytes can only be smaller than this budget.
            turn.read_bytes += capacity
        try:
            try:
                chunk = self._listener._receive_without_ancillary(connection, capacity)  # pyright: ignore[reportPrivateUsage]
            except BlockingIOError:
                return
            if not chunk:
                if state.submit_ticket is not None or state.owner_serial is not None:
                    # SHUT_WR is not cancellation. A full close/HUP is
                    # observed separately by the reactor, never a worker.
                    disarm(connection, state)
                else:
                    close(connection)
                return
            if not state.buffer and not state.first_byte_ns:
                state.first_byte_ns = monotonic_ns()
            if len(state.buffer) + len(chunk) > MAX_FRAME_BYTES:
                raise ConsumerUnixSocketError("request buffer exceeds frame limit")
            state.buffer.extend(chunk)
            state.progress_ns = monotonic_ns()
            schedule(connection, state)
        finally:
            if turn is not None:
                turn.reads += 1
                turn.read_ns += monotonic_ns() - read_started

    def _write_reactor_output(
        self,
        connection: socket.socket,
        state: _ReactorConnection,
        *,
        turn: ReactorIoTurn | None,
        owner: ReactorOwnerPortV1[socket.socket] | None,
        owner_reads: ReactorOwnerReadsV1[socket.socket] | None,
        ready: BoundedFairOperationQueueV2[socket.socket],
        owner_read_inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]],
        arm: Callable[[socket.socket, _ReactorConnection, int], None],
        disarm: Callable[[socket.socket, _ReactorConnection], None],
        close: Callable[[socket.socket], None],
        schedule: Callable[[socket.socket, _ReactorConnection], None],
        debit: Callable[[int], None],
    ) -> None:
        if not state.output:
            raise ConsumerUnixSocketError("reactor attempted an empty response delivery")
        if state.owner_read_serial is not None and owner_reads is not None:
            assert state.session is not None
            self._listener.validate_owner_frontend_delivery(state.session)
            if owner_reads.delivery(state.owner_read_serial) != state.output:
                raise ConsumerUnixSocketError("owner read changed after partial delivery")
        if state.owner_serial is not None and owner is not None:
            if state.session is None or state.owner_authority is None:
                raise ConsumerUnixSocketError("owner delivery authority missing")
            self._listener.validate_owner_frontend_delivery(state.session)
            owner.validate_delivery(state.owner_serial, current_authority=state.owner_authority)
        capacity = (
            turn.send_capacity() if turn is not None else len(state.output) - state.output_offset
        )
        if capacity <= 0 and state.output_offset < len(state.output):
            if state.owner_read_serial is not None and owner_reads is not None:
                owner_reads.recheck(state.owner_read_serial)
                state.deferred = True
                disarm(connection, state)
                return
            arm(connection, state, selectors.EVENT_WRITE)
            return
        send_started = monotonic_ns() if turn is not None else 0
        try:
            sent = connection.send(
                memoryview(state.output)[state.output_offset : state.output_offset + capacity]
            )
        except BlockingIOError:
            sent = 0
        finally:
            if turn is not None:
                turn.writes += 1
                turn.write_ns += monotonic_ns() - send_started
        if sent:
            if turn is not None:
                turn.write_bytes += sent
            state.output_offset += sent
            state.progress_ns = monotonic_ns()
            debit(sent)
        if state.output_offset < len(state.output):
            if state.owner_read_serial is not None and owner_reads is not None:
                owner_reads.recheck(state.owner_read_serial)
                state.deferred = True
                disarm(connection, state)
                return
            arm(connection, state, selectors.EVENT_WRITE)
            return
        if state.owner_serial is not None and owner is not None:
            assert state.owner_authority is not None
            owner.acknowledge_delivery(state.owner_serial, current_authority=state.owner_authority)
            state.owner_serial = None
            self._reactor_pending = ready.used
        if state.owner_read_serial is not None and owner_reads is not None:
            owner_reads.dispose(state.owner_read_serial)
            owner_read_inflight.pop(state.owner_read_serial)
            state.owner_read_serial = None
        if state.trace is not None and self._request_trace is not None:
            state.trace.update(
                outcome="sent",
                send_finished_ns=monotonic_ns(),
                response_bytes=len(state.output),
            )
            self._request_trace.submit(state.trace)
            state.trace = None
        state.output = b""
        state.output_offset = 0
        # A deployment may explicitly declare one request per connection.
        # Half-close the response direction only after the complete frame
        # entered the kernel, then retain a bounded read-side tombstone until
        # the client observes the frame and closes.  Immediate close can
        # produce response-less EOF/RST under concurrent SCM_RIGHTS handoff
        # even though send() reported the full byte count.  Inactivity expiry
        # still bounds a peer which refuses to close.
        if state.requests >= self._listener.request_limit:
            state.buffer.clear()
            try:
                connection.shutdown(socket.SHUT_WR)
            except OSError:
                close(connection)
                return
            state.progress_ns = monotonic_ns()
            arm(connection, state, selectors.EVENT_READ)
            return
        if not state.buffer:
            state.first_byte_ns = 0
        arm(connection, state, selectors.EVENT_READ)
        schedule(connection, state)

    def _complete_owner_read(
        self,
        serial: int,
        reads: ReactorOwnerReadsV1[socket.socket],
        inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]],
        close: Callable[[socket.socket], None],
        assign: Callable[[_ReactorConnection, DecodedFrameV2, bytes], bool],
        write: Callable[[socket.socket, _ReactorConnection], None],
    ) -> None:
        connection, state, frame = inflight[serial]
        state.deferred = False
        try:
            response = reads.delivery(serial)
            if not state.output:
                if not assign(state, frame, response):
                    close(connection)
                    return
                self._deferred_reads_completed += 1
            state.progress_ns = monotonic_ns()
            write(connection, state)
        except (
            ConsumerGatewayError,
            ConsumerUnixSocketError,
            ConsumerWireError,
            OSError,
        ) as error:
            if self._diagnostic_dispatch_errors:
                print(
                    "tradecore owner-read completion failure: "
                    f"{type(error).__name__}: {error}",
                    file=sys.stderr,
                    flush=True,
                )
            close(connection)

    @staticmethod
    def _drain_owner_reads(reads: ReactorOwnerReadsV1[socket.socket]) -> None:
        deadline = monotonic_ns() + 10_000_000_000
        try:
            while reads.outstanding:
                if monotonic_ns() >= deadline:
                    raise OwnerProtocolError("owner read drain expired")
                channels = reads.channels
                readable, writable, _ = select.select(
                    channels,
                    [c for c in channels if reads.events(c) & selectors.EVENT_WRITE],
                    [],
                    0.05,
                )
                for channel in set(readable + writable):
                    reads.io(
                        channel,
                        (selectors.EVENT_READ if channel in readable else 0)
                        | (selectors.EVENT_WRITE if channel in writable else 0),
                    )
            reads.stop()
        except BaseException:
            reads.fail()
            raise

    def _complete_owner(
        self,
        owner: ReactorOwnerPortV1[socket.socket] | None,
        inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]],
        states: dict[socket.socket, _ReactorConnection],
        close: Callable[[socket.socket], None],
        assign: Callable[[_ReactorConnection, DecodedFrameV2, bytes], bool],
        write: Callable[[socket.socket, _ReactorConnection], None],
    ) -> None:
        if owner is None:
            return
        for event in owner.read_ready_batch():
            connection, state, frame = inflight.pop(event.identity.serial)
            state.deferred = False
            self._submit_completed += 1
            self._submit_not_started += int(event.phase == "NOT_STARTED")
            if state.trace is not None:
                state.trace.update(
                    owner_generation=event.identity.generation,
                    owner_serial=event.identity.serial,
                    owner_pid=owner.ready.owner_pid,
                    ipc_frontend_result_received_ns=monotonic_ns(),
                )
            if states.get(connection) is not state:
                owner.dispose(event.identity.serial)
                state.owner_serial = None
                if state.session is not None:
                    self._listener.close_authenticated_session(state.session)
                if state.trace is not None and self._request_trace is not None:
                    state.trace.update(
                        outcome="closed_before_send_complete", closed_ns=monotonic_ns()
                    )
                    self._request_trace.submit(state.trace)
                continue
            if self._stopped.is_set() or event.reason == "PRECHECK":
                close(connection)
                continue
            response = (
                event.response_frame
                if event.phase == "COMPLETED"
                else encode_frame(
                    MessageKind.ERROR,
                    frame.header.correlation_id,
                    {
                        "message_type": "error",
                        "schema_version": 2,
                        "request_id": frame.payload["request_id"],
                        "code": "RATE_LIMITED",
                        "retryable": True,
                        "detail": "SUBMIT_NOT_STARTED",
                    },
                )
            )
            try:
                if assign(state, frame, response):
                    state.progress_ns = monotonic_ns()
                    write(connection, state)
                else:
                    close(connection)
            except (ConsumerGatewayError, ConsumerUnixSocketError, ConsumerWireError, OSError):
                close(connection)

    @staticmethod
    def _drain_owner(
        owner: ReactorOwnerPortV1[socket.socket],
        inflight: dict[int, tuple[socket.socket, _ReactorConnection, DecodedFrameV2]],
        complete: Callable[[], None],
    ) -> None:
        deadline = monotonic_ns() + 10_000_000_000
        try:
            while inflight:
                owner.expire()
                remaining = (deadline - monotonic_ns()) / 1e9
                if remaining <= 0:
                    raise OwnerProtocolError("owner reactor drain expired")
                r, w, _ = select.select(
                    [owner.channel],
                    [owner.channel] if owner.wants_write else [],
                    [],
                    min(0.05, remaining),
                )
                if w:
                    owner.write_ready()
                if r:
                    complete()
            owner.stop()
        except BaseException:
            owner.fail()
            raise

    def _serve_legacy_accept(self) -> None:
        while not self._stopped.is_set():
            try:
                connection, principal = self._listener.accept_connection()
            except socket.timeout:
                continue
            except ConsumerUnixSocketError:
                continue
            except OSError:
                if self._stopped.is_set():
                    break
                raise
            with self._state_lock:
                self._socket_accepted_connections += 1
            self._admit_ready(_ReadyConnection(connection, principal))
            self._maybe_log_ingress_stats()

    def _serve_frame_ready(self) -> None:
        """Only complete first frames occupy a business worker.

        The selector owns pre-frame sockets; it never decodes JSON or calls
        OMS. A bounded capacity and deadline prevent idle peers from pinning
        descriptors or worker slots. Remaining frames on a connection retain
        the existing ordered, blocking transport semantics.
        """

        preframe: dict[socket.socket, _PreframeConnection] = {}
        listener_socket = self._listener.listening_socket
        listener_socket.setblocking(False)
        with selectors.DefaultSelector() as selector:
            selector.register(listener_socket, selectors.EVENT_READ)
            try:
                while not self._stopped.is_set():
                    try:
                        events = selector.select(timeout=0.05)
                    except (OSError, ValueError):
                        if self._stopped.is_set():
                            break
                        raise
                    for key, _mask in events:
                        if key.fileobj is listener_socket:
                            self._accept_preframe(selector, preframe)
                        else:
                            connection = key.fileobj
                            if isinstance(connection, socket.socket):
                                self._read_preframe(selector, preframe, connection)
                    self._expire_preframe(selector, preframe)
                    self._maybe_log_ingress_stats()
            finally:
                for connection in tuple(preframe):
                    self._close_preframe(selector, preframe, connection)

    def _accept_preframe(
        self,
        selector: selectors.BaseSelector,
        preframe: dict[socket.socket, _PreframeConnection],
    ) -> None:
        # Bound accept work per poll so already-connected, readable sockets
        # cannot starve behind a sustained new-connection burst.
        for _ in range(256):
            try:
                connection, principal = self._listener.accept_connection()
            except (BlockingIOError, InterruptedError):
                break
            except ConsumerUnixSocketError:
                continue
            except OSError:
                if self._stopped.is_set():
                    break
                raise
            with self._state_lock:
                self._socket_accepted_connections += 1
            if len(preframe) >= self._preframe_capacity:
                self._reject_overload(connection)
                continue
            connection.setblocking(False)
            preframe[connection] = _PreframeConnection(principal, monotonic_ns(), bytearray())
            with self._state_lock:
                self._preframe_connections = len(preframe)
                self._peak_preframe_connections = max(
                    self._peak_preframe_connections, len(preframe)
                )
            # Most fresh-request peers have already sent their first frame by
            # the time accept() drains the kernel backlog. Read it immediately
            # before paying for selector registration and another poll turn.
            self._read_preframe(selector, preframe, connection, registered=False)
            if connection in preframe:
                try:
                    selector.register(connection, selectors.EVENT_READ)
                except (OSError, ValueError):
                    self._close_preframe(selector, preframe, connection, registered=False)

    def _read_preframe(
        self,
        selector: selectors.BaseSelector,
        preframe: dict[socket.socket, _PreframeConnection],
        connection: socket.socket,
        *,
        registered: bool = True,
    ) -> None:
        state = preframe[connection]
        try:
            chunk = self._listener._receive_without_ancillary(  # pyright: ignore[reportPrivateUsage]
                connection
            )
        except BlockingIOError:
            return
        except (ConsumerUnixSocketError, OSError):
            self._close_preframe(selector, preframe, connection, registered=registered)
            return
        if not chunk or len(state.buffer) + len(chunk) > MAX_FRAME_BYTES:
            self._close_preframe(selector, preframe, connection, registered=registered)
            return
        state.buffer.extend(chunk)
        if len(state.buffer) < HEADER_SIZE_BYTES:
            return
        payload_length = struct.unpack_from(">I", state.buffer, 24)[0]
        if HEADER_SIZE_BYTES + payload_length > MAX_FRAME_BYTES:
            self._close_preframe(selector, preframe, connection, registered=registered)
            return
        if len(state.buffer) < HEADER_SIZE_BYTES + payload_length:
            return
        ready_ns = monotonic_ns()
        if registered:
            selector.unregister(connection)
        del preframe[connection]
        try:
            connection.setblocking(True)
        except OSError:
            connection.close()
            with self._state_lock:
                self._preframe_connections = len(preframe)
            return
        with self._state_lock:
            self._preframe_connections = len(preframe)
            self._ready_frame_connections += 1
            if not registered:
                self._ready_on_accept_connections += 1
            duration = ready_ns - state.accepted_ns
            self._frame_wait_ns_total += duration
            self._frame_wait_ns_max = max(self._frame_wait_ns_max, duration)
        self._admit_ready(
            _ReadyConnection(connection, state.principal, bytes(state.buffer), ready_ns)
        )

    def _expire_preframe(
        self,
        selector: selectors.BaseSelector,
        preframe: dict[socket.socket, _PreframeConnection],
    ) -> None:
        now = monotonic_ns()
        # Accepted timestamps increase with insertion order. Once the oldest
        # live socket is within its deadline, no later socket can have expired.
        while preframe:
            connection = next(iter(preframe))
            if now - preframe[connection].accepted_ns < self._preframe_timeout_ns:
                break
            self._close_preframe(selector, preframe, connection)
            with self._state_lock:
                self._expired_preframe_connections += 1

    def _close_preframe(
        self,
        selector: selectors.BaseSelector,
        preframe: dict[socket.socket, _PreframeConnection],
        connection: socket.socket,
        *,
        registered: bool = True,
    ) -> None:
        if registered:
            selector.unregister(connection)
        del preframe[connection]
        connection.close()
        with self._state_lock:
            self._preframe_connections = len(preframe)

    def _admit_ready(self, ready: _ReadyConnection) -> None:
        try:
            self._pending.put_nowait(ready)
        except Full:
            self._reject_overload(ready.connection, initial_bytes=ready.initial_bytes)
        else:
            with self._state_lock:
                self._accepted_connections += 1
                self._peak_pending_connections = max(
                    self._peak_pending_connections, self._pending.qsize()
                )

    def _reject_overload(self, connection: socket.socket, *, initial_bytes: bytes = b"") -> None:
        with self._state_lock:
            self._overloaded_connections += 1
            count = self._overloaded_connections
        if initial_bytes and not self._stopped.is_set():
            try:
                self._overload_responses.put_nowait((connection, initial_bytes))
            except Full:
                connection.close()
                with self._state_lock:
                    self._overload_connections_closed_without_response += 1
        else:
            connection.close()
            with self._state_lock:
                self._overload_connections_closed_without_response += 1
        if count & (count - 1) == 0:
            _LOG.warning("Consumer UDS ingress full; rejected_connections=%d", count)

    def _respond_overload(self) -> None:
        while not self._stopped.is_set():
            try:
                connection, frame_bytes = self._overload_responses.get(timeout=0.1)
            except Empty:
                continue
            try:
                if len(frame_bytes) < HEADER_SIZE_BYTES:
                    continue
                payload_length = struct.unpack_from(">I", frame_bytes, 24)[0]
                frame = decode_frame(frame_bytes[: HEADER_SIZE_BYTES + payload_length])
                request_id = frame.payload.get("request_id")
                if (
                    frame.header.message_kind is not MessageKind.REQUEST
                    or not isinstance(request_id, str)
                    or not 1 <= len(request_id) <= 128
                ):
                    continue
                response = encode_frame(
                    MessageKind.ERROR,
                    frame.header.correlation_id,
                    {
                        "message_type": "error",
                        "schema_version": 2,
                        "request_id": request_id,
                        "code": "RATE_LIMITED",
                        "retryable": True,
                        "detail": "INGRESS_OVERLOADED_PRE_DISPATCH",
                    },
                )
                connection.settimeout(0.05)
                connection.sendall(response)
                with self._state_lock:
                    self._overload_responses_sent += 1
            except (ConsumerWireError, OSError, ValueError):
                with self._state_lock:
                    self._overload_response_failures += 1
            finally:
                connection.close()
                self._overload_responses.task_done()

    def _maybe_log_ingress_stats(self) -> None:
        now = monotonic_ns()
        if now - self._last_stats_log_ns >= 1_000_000_000:
            self._last_stats_log_ns = now
            stats = self.ingress_stats()
            signature = (
                stats.accepted_connections,
                stats.overloaded_connections,
                stats.served_connections,
                stats.expired_preframe_connections,
            )
            if signature != self._last_logged_signature or any(
                (stats.preframe_connections, stats.pending_connections, stats.active_connections)
            ):
                self._last_logged_signature = signature
                self._log_ingress_stats()

    def _work(self) -> None:
        while not self._stopped.is_set():
            try:
                ready = self._pending.get(timeout=0.1)
            except Empty:
                continue
            connection = ready.connection
            try:
                with self._state_lock:
                    if self._stopped.is_set():
                        connection.close()
                        continue
                    self._active.add(connection)
                    self._worker_started_connections += 1
                    if ready.frame_ready_ns:
                        duration = monotonic_ns() - ready.frame_ready_ns
                        self._worker_queue_wait_ns_total += duration
                        self._worker_queue_wait_ns_max = max(
                            self._worker_queue_wait_ns_max, duration
                        )
                try:
                    if ready.initial_bytes:
                        self._listener.serve_authenticated_connection(
                            connection, ready.principal, initial_bytes=ready.initial_bytes
                        )
                    else:
                        self._listener.serve_authenticated_connection(connection, ready.principal)
                except Exception as exc:
                    _LOG.exception("Consumer V2 ingress worker failed; stopping listener")
                    with self._state_lock:
                        self._fatal = exc
                    self.stop()
                    return
            finally:
                with self._state_lock:
                    self._active.discard(connection)
                    self._served_connections += 1
                self._pending.task_done()

    def stop(self) -> None:
        self._stopped.set()
        if self._started:
            self._listener.close()
            self._started = False
        while True:
            try:
                ready = self._pending.get_nowait()
            except Empty:
                break
            ready.connection.close()
            self._pending.task_done()
        while True:
            try:
                connection, _frame_bytes = self._overload_responses.get_nowait()
            except Empty:
                break
            connection.close()
            self._overload_responses.task_done()
        with self._state_lock:
            active = tuple(self._active)
        for connection in active:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if self._deferred_reads is not None and not self._deferred_reads_drained:
            # serve_forever owns the accepted read drain. Do not close its
            # backing files/components underneath a worker from another thread.
            return
        if self._submit_lane is not None and not self._submit_lane_drained:
            return
        if self._cold_admissions is not None and not self._cold_admissions_drained:
            return
        if self._managed_components_started:
            self._managed_components_started = False
            for component in reversed(self._managed_components):
                component.close()

    def request_stop(self) -> None:
        """Signal-safe shutdown request; accept polling performs cleanup."""

        self._stopped.set()
