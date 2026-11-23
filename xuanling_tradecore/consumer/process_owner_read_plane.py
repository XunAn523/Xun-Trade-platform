"""Dedicated read coordinator inside the unique OMS owner's spawn tree.

The process owns cursor codecs, snapshot pins and response encoding, but no
writable Book, Journal lease, cold index or Adapter.  Its one SQL child uses the
second pre-existing read CPU.  The fixed inherited rendezvous continue to borrow
the original public fair-queue credits; no additional business queue is introduced.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
from dataclasses import dataclass
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic
from typing import Any

import msgspec

from ..domain import (
    ConsumerAccountBinding,
    ConsumerAuditTrail,
    ConsumerOrderEvent,
    ConsumerPrincipal,
)
from ..oms.published_replay_window import PublishedCommittedReplayBook
from ..oms.read_only_journal_source import PublishedJournalSourceV1
from ..read_model.consumer_order_reader_v2 import ConsumerOrderProjectionReaderV2
from ..read_model.process_projection_reads import ProcessProjectionReads
from ..read_model.versioned_snapshot_reader import VersionedConsumerSnapshotReader
from .base import (
    ConsumerAccountOrderSnapshotV2,
    ConsumerEventSubscription,
    ConsumerGatewayError,
    ConsumerOrderSignal,
    ConsumerSession,
    IntentReceipt,
)
from .bindings_v2 import ConsumerBindingSnapshotV2
from .cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from .dispatch_v2 import ConsumerWireDispatcherV2
from .fair_queue_v2 import ConsumerOperationLaneV2
from .gateway_v2 import ConsumerGatewayV2
from .long_running_candidate import candidate_binding_scope
from .owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1
from .owner_read_fence import PublishedOwnerReadFenceV1
from .owner_read_ipc import OwnerReadServiceBudgetV1, serve_owner_reads
from .owner_read_reactor import OWNER_READ_SLOT_COUNT
from .projection_reads_v2 import PROJECTION_READ_OPERATIONS
from .replay_v2 import ConsumerCursorReplayServiceV2
from .snapshot_cursor_v2 import SnapshotCursorCodecV2, SnapshotCursorSigningKeyV2
from .snapshot_index_v2 import ConsumerSnapshotIndexV2
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from .uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    consumer_no_adapter_account_view,
    consumer_registry_for_binding,
    load_consumer_request_schema,
)
from .unix_v2 import ConsumerUnixSocketError, ConsumerUnixSocketListenerV2
from .wire_v2 import load_contract_identity

_CONTROL_LIMIT = 1024


@dataclass(frozen=True)
class OwnerReadPlaneKeysV1:
    event_current: CursorSigningKeyV2
    event_retired: tuple[CursorSigningKeyV2, ...]
    snapshot_current: SnapshotCursorSigningKeyV2
    snapshot_retired: tuple[SnapshotCursorSigningKeyV2, ...]


@dataclass(frozen=True)
class OwnerReadPlaneBootV1:
    role: str
    allowed_lanes: frozenset[ConsumerOperationLaneV2]
    config: ConsumerUdsServerConfigV2
    contract_root: Path
    projection_root: Path
    journal_root: Path
    journal_id: str
    profile_hash: str
    segment_frames: int
    maximum_database_bytes: int
    coordinator_cpu: int
    store_cpu: int
    keys: OwnerReadPlaneKeysV1
    ready: OwnerReadyV1


class _Status(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    phase: str
    pid: int


def _send_status(channel: Connection, phase: str) -> None:
    body = msgspec.msgpack.encode(_Status(phase, os.getpid()))
    if not 0 < len(body) <= _CONTROL_LIMIT:
        raise OwnerProtocolError("owner read-plane status exceeds fixed budget")
    channel.send_bytes(body)


def _receive_status(channel: Connection, *, timeout: float) -> _Status:
    if not channel.poll(timeout):
        raise OwnerProtocolError("owner read-plane status timed out")
    body = channel.recv_bytes(_CONTROL_LIMIT)
    try:
        return msgspec.msgpack.decode(body, type=_Status)
    except msgspec.DecodeError as exc:
        raise OwnerProtocolError("owner read-plane status is invalid") from exc


class _ReadOnlyOperations:
    def __init__(self, reader: ConsumerOrderProjectionReaderV2) -> None:
        self._reader = reader

    def get_order(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> ConsumerOrderEvent:
        return self._reader.get_order(
            account_binding=account_binding, oms_order_id=oms_order_id
        )

    def get_order_audit_trail(
        self, *, account_binding: ConsumerAccountBinding, oms_order_id: str
    ) -> tuple[ConsumerAuditTrail, ...]:
        return self._reader.get_order_audit_trail(
            account_binding=account_binding, oms_order_id=oms_order_id
        )

    def submit_signal(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        strategy_binding_id: str,
        signal: ConsumerOrderSignal,
    ) -> IntentReceipt:
        del session, account_binding, strategy_binding_id, signal
        raise ConsumerGatewayError("read plane cannot submit")

    def cancel_order(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        consumer_request_id: str,
        oms_order_id: str,
        reason: str,
    ) -> IntentReceipt:
        del session, account_binding, consumer_request_id, oms_order_id, reason
        raise ConsumerGatewayError("read plane cannot cancel")

    def get_account_order_snapshot(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        reset_context: str | None,
    ) -> ConsumerAccountOrderSnapshotV2:
        del session, account_binding, reset_context
        raise ConsumerGatewayError("legacy snapshot is unavailable in long-running read plane")

    def subscribe_order_events(
        self,
        *,
        session: ConsumerSession,
        account_binding: ConsumerAccountBinding,
        cursor_token: str,
    ) -> ConsumerEventSubscription:
        del session, account_binding, cursor_token
        raise ConsumerGatewayError("live subscriptions are unavailable in the read plane")


def _build_listener(
    boot: OwnerReadPlaneBootV1, shared: Any
) -> tuple[
    ConsumerUnixSocketListenerV2,
    ProcessProjectionReads,
    ConsumerSnapshotIndexV2,
    PublishedCommittedReplayBook | None,
]:
    fence = PublishedOwnerReadFenceV1(
        shared,
        journal_id=boot.journal_id,
        profile_hash=boot.profile_hash,
        segment_frames=boot.segment_frames,
    )
    journal = PublishedJournalSourceV1(
        root=boot.journal_root,
        journal_id=boot.journal_id,
        profile_hash=boot.profile_hash,
        segment_frames=boot.segment_frames,
        bound_for=fence.read_bound,
    )
    stream = CursorStreamRefV2(boot.journal_id, 1)
    registry = CursorStreamRegistryV2(stream, (stream,))
    account, strategies, scope = candidate_binding_scope(boot.config)
    policy = boot.config.long_running_policy
    if (
        scope != boot.ready.scope_sha256
        or boot.ready.journal_id != boot.journal_id
        or policy is None
    ):
        raise OwnerProtocolError("owner read-plane bootstrap scope changed")
    reads = ProcessProjectionReads(
        root=boot.projection_root,
        journal_id=boot.journal_id,
        profile_hash=boot.profile_hash,
        maximum_database_bytes=boot.maximum_database_bytes,
        cpus=(boot.store_cpu,),
        reuse_connections=True,
    )
    replay_book = (
        PublishedCommittedReplayBook(
            journal,
            event_capacity=policy.replay_cache_events,
            checkpoint_capacity=policy.replay_cache_events,
        )
        if boot.allowed_lanes == frozenset({ConsumerOperationLaneV2.REPLAY})
        else None
    )
    reader = ConsumerOrderProjectionReaderV2(
        root=boot.projection_root,
        journal=journal,
        stream=stream,
        current_stream=lambda: stream,
        health_for=fence.health,
        maximum_database_bytes=boot.maximum_database_bytes,
        process_reads=reads,
        committed_replay_book=replay_book,
    )
    snapshot_reader = VersionedConsumerSnapshotReader(
        root=boot.projection_root,
        stream=stream,
        profile_hash=boot.profile_hash,
        health_for=fence.health,
        maximum_database_bytes=boot.maximum_database_bytes,
    )
    event_codec = CursorKeyRingV2(
        current=boot.keys.event_current, retired=boot.keys.event_retired
    )
    snapshot_codec = SnapshotCursorCodecV2(
        boot.keys.snapshot_current, retired_keys=boot.keys.snapshot_retired
    )
    snapshots = ConsumerSnapshotIndexV2(
        registry_for=lambda binding: consumer_registry_for_binding(binding, account, registry),
        reader=snapshot_reader,
        cursor_codec=snapshot_codec,
        cursor_codec_for=lambda: snapshot_codec,
        maximum_retained_bytes=policy.snapshot_budget_bytes,
        maximum_page_cache_bytes=policy.snapshot_budget_bytes * 7 // 8,
    )
    replay = ConsumerCursorReplayServiceV2(
        key_ring_for=lambda binding: event_codec
        if binding == account
        else _raise_binding(),
        registry_for=lambda binding: consumer_registry_for_binding(binding, account, registry),
        cursor_ttl_ns=3_600_000_000_000,
        event_reader=reader,
        partition_reader=reader,
    )
    bindings = ConsumerBindingSnapshotV2(1, (account,), strategies)
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: bindings,
        account_view_for=lambda binding: consumer_no_adapter_account_view(binding, account),
        wire_contract=load_contract_identity(boot.contract_root),
        event_replay=replay,
        operations=_ReadOnlyOperations(reader),
        paged_snapshots=snapshots,
        projection_read_delivery_guard=reader.validate_delivery,
    )
    principal = ConsumerPrincipal(
        boot.config.consumer_id, boot.config.login_name, boot.config.legal_name
    )
    listener = ConsumerUnixSocketListenerV2(
        socket_path=boot.config.journal_root / "private-read-plane-unused.sock",
        gateway=gateway,
        principal_for_peer=lambda _peer: principal,
        request_schema=load_consumer_request_schema(boot.contract_root),
        request_handler=ConsumerWireDispatcherV2(gateway),
        max_requests_per_connection=boot.config.max_requests_per_connection,
        parallel_read_operations=PROJECTION_READ_OPERATIONS
        | frozenset({"CreateOrderSnapshot", "ReadOrderSnapshotPage"}),
        socket_mode=0o600,
        inline_cached_snapshot_pages=True,
        allow_socket_writes=False,
        allow_socket_reads=True,
    )
    return listener, reads, snapshots, replay_book


def _raise_binding() -> CursorKeyRingV2:
    raise ConsumerUnixSocketError("owner read-plane binding is unavailable")


def _run_read_plane(
    control: Connection,
    channels: tuple[socket.socket, ...],
    boot: OwnerReadPlaneBootV1,
    shared: Any,
    read_service_signal: Any | None,
) -> None:
    stopped, failed = Event(), Event()
    failures: list[BaseException] = []
    failure_lock = Lock()
    readers: list[Thread] = []
    components: tuple[object, ...] | None = None
    try:
        if len(channels) != OWNER_READ_SLOT_COUNT or os.getppid() != boot.ready.owner_pid:
            raise OwnerProtocolError("owner read-plane parent/channel identity mismatch")
        os.sched_setaffinity(0, {boot.coordinator_cpu})
        listener, projection_reads, snapshots, replay_book = _build_listener(boot, shared)
        components = tuple(
            component
            for component in (projection_reads, snapshots, replay_book)
            if component is not None
        )
        projection_reads.start()
        if replay_book is not None:
            replay_book.start()
        service_budget = OwnerReadServiceBudgetV1(
            read_service_signal=read_service_signal
        )

        def fence(error: BaseException) -> None:
            with failure_lock:
                if not failures:
                    failures.append(error)
            failed.set()
            stopped.set()

        for index, channel in enumerate(channels):
            thread = Thread(
                target=serve_owner_reads,
                kwargs={
                    "channel": channel,
                    "listener": listener,
                    "original_config": boot.config,
                    "ready": boot.ready,
                    "stopped": stopped,
                    "fence": fence,
                    "service_budget": service_budget,
                    "allowed_lanes": boot.allowed_lanes,
                },
                name=f"{boot.role}-{index}",
                daemon=True,
            )
            thread.start()
            readers.append(thread)
        _send_status(control, "READY")
        while not stopped.is_set():
            if replay_book is not None and replay_book.failed:
                raise OwnerProtocolError(
                    "owner replay Journal pump failed: "
                    f"{replay_book.failure_kind or 'UNKNOWN'}"
                )
            if control.poll(0.05):
                message = control.recv_bytes(_CONTROL_LIMIT)
                if message != b"STOP":
                    raise OwnerProtocolError("owner read-plane control is invalid")
                stopped.set()
                break
        for thread in readers:
            thread.join(10)
        if failed.is_set() or any(thread.is_alive() for thread in readers):
            if failures:
                failure = failures[0]
                raise OwnerProtocolError(
                    "owner read-plane actor failed: "
                    f"{type(failure).__name__}: {failure}"
                ) from failure
            raise OwnerProtocolError("owner read-plane rendezvous did not drain")
        snapshots.close()
        if replay_book is not None:
            replay_book.close()
        projection_reads.close()
        if not projection_reads.shutdown_verified:
            raise OwnerProtocolError("owner read-plane SQL child did not exit cleanly")
        _send_status(control, "STOPPED")
    except BaseException:
        stopped.set()
        try:
            _send_status(control, "FAILED")
        except BaseException:
            pass
        raise
    finally:
        if components is not None:
            for component in reversed(components):
                try:
                    component.close()
                except BaseException:
                    pass
        for channel in channels:
            channel.close()
        control.close()


class ProcessOwnerReadPlaneV1:
    def __init__(
        self,
        *,
        channels: tuple[socket.socket, ...],
        boot: OwnerReadPlaneBootV1,
        shared: Any,
        read_service_signal: Any | None = None,
    ) -> None:
        valid_scopes = {
            "owner-replay-plane": frozenset({ConsumerOperationLaneV2.REPLAY}),
            "owner-snapshot-plane": frozenset(
                {ConsumerOperationLaneV2.SNAPSHOT, ConsumerOperationLaneV2.OTHER}
            ),
        }
        if (
            len(channels) != OWNER_READ_SLOT_COUNT
            or len({channel.fileno() for channel in channels}) != OWNER_READ_SLOT_COUNT
            or boot.coordinator_cpu == boot.store_cpu
            or valid_scopes.get(boot.role) != boot.allowed_lanes
        ):
            raise OwnerProtocolError("owner read-plane fixed resources are invalid")
        context = mp.get_context("spawn")
        self._parent, child = context.Pipe(duplex=True)
        self._child = child
        self._channels = channels
        self._process = context.Process(
            target=_run_read_plane,
            args=(child, channels, boot, shared, read_service_signal),
            name=boot.role,
        )
        self._state = "NEW"
        self.shutdown_verified = False

    @property
    def role(self) -> str:
        return self._process.name

    @property
    def pid(self) -> int | None:
        value = self._process.pid
        return value if type(value) is int else None

    @property
    def failed(self) -> bool:
        return self._state == "FAILED" or (
            self._state == "RUNNING" and not self._process.is_alive()
        )

    @property
    def exit_code(self) -> int | None:
        value = self._process.exitcode
        return value if type(value) is int else None

    def start(self) -> None:
        if self._state != "NEW":
            raise OwnerProtocolError("owner read-plane cannot restart")
        self._state = "STARTING"
        self._process.start()
        self._child.close()
        for channel in self._channels:
            channel.close()
        status = _receive_status(self._parent, timeout=30)
        if status.phase != "READY" or status.pid != self.pid or not self._process.is_alive():
            self.abort()
            raise OwnerProtocolError("owner read-plane readiness mismatch")
        self._state = "RUNNING"

    def close(self, *, timeout: float = 30) -> None:
        if self._state == "CLOSED":
            return
        if self.failed or self._state != "RUNNING":
            self.abort()
            raise OwnerProtocolError("owner read-plane failed before drain")
        self._state = "STOPPING"
        deadline = monotonic() + timeout
        self._parent.send_bytes(b"STOP")
        status = _receive_status(self._parent, timeout=max(0, deadline - monotonic()))
        self._process.join(max(0, deadline - monotonic()))
        if status.phase != "STOPPED" or status.pid != self.pid or self._process.exitcode != 0:
            self.abort()
            raise OwnerProtocolError("owner read-plane stop acknowledgement mismatch")
        self.shutdown_verified, self._state = True, "CLOSED"
        self._parent.close()

    def abort(self) -> None:
        self._state = "FAILED"
        self.shutdown_verified = False
        self._parent.close()
        if self._process.pid is not None:
            self._process.join(0.2)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(5)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(5)
