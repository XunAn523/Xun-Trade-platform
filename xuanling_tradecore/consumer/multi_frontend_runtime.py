"""Default-off fixed multi-frontend candidate under one fail-closed supervisor."""

from __future__ import annotations

import multiprocessing as mp
import os
import selectors
import socket
import sys
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic, monotonic_ns
from typing import Any

from .frontend_fd_handoff import (
    BoundedFrontendFdRouterV1,
    FrontendFdHandoffError,
    create_frontend_fd_handoff_pair,
)
from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1, OwnerStoppedV1
from .owner_read_reactor import (
    FRONTEND_READ_SLOT_COUNT,
    OWNER_DIRECT_READ_SLOT_COUNT,
    OWNER_READ_SLOT_COUNT,
)
from .process_oms_owner import SpawnedOmsOwnerCandidate
from .spawned_frontend_worker import SpawnedFrontendWorkerV1
from .spawned_owner_brokers import SpawnedOwnerBrokerV1
from .uds_server_v2 import ConsumerUdsServerConfigV2
from .unix_v2 import read_linux_peer_credentials

_MIN_FRONTEND_HANDOFF_WINDOW = 64


class MultiFrontendOwnerRuntimeV1:
    """One acceptor, fixed frontend processes, brokers and one OMS owner.

    This class is qualification-only and has no automatic restart.  The caller
    owns the supplied OMS candidate. Any child/channel/protocol failure fences
    the complete topology; a replacement runtime requires a new generation and
    a fresh authoritative recovery ceremony.
    """

    def __init__(
        self,
        *,
        config: ConsumerUdsServerConfigV2,
        owner: SpawnedOmsOwnerCandidate,
        contract_root: Path,
        frontend_cpus: tuple[int, ...],
        broker_cpu: int,
        read_broker_cpu: int,
        acceptor_cpu: int,
        direct_frontend_reads: bool = False,
    ) -> None:
        if not 2 <= len(frontend_cpus) <= 8 or len(set(frontend_cpus)) != len(frontend_cpus):
            raise OwnerProtocolError("multi-frontend candidate requires 2..8 unique CPUs")
        if (
            type(broker_cpu) is not int
            or broker_cpu < 0
            or type(read_broker_cpu) is not int
            or read_broker_cpu < 0
            or type(acceptor_cpu) is not int
            or acceptor_cpu < 0
            or len({broker_cpu, read_broker_cpu, acceptor_cpu, *frontend_cpus})
            != 3 + len(frontend_cpus)
        ):
            raise OwnerProtocolError("multi-frontend brokers and acceptor require distinct CPUs")
        self.config, self.owner, self.contract_root = config, owner, contract_root
        self.frontend_cpus = frontend_cpus
        self.broker_cpu = broker_cpu
        self.read_broker_cpu = read_broker_cpu
        self.acceptor_cpu = acceptor_cpu
        self.direct_frontend_reads = direct_frontend_reads
        if direct_frontend_reads and len(frontend_cpus) != OWNER_READ_SLOT_COUNT:
            raise OwnerProtocolError(
                "direct frontend READ requires one frontend per authoritative slot"
            )
        self._context: Any = mp.get_context("spawn")
        self._shared_total = FrontendSharedCreditV1(
            self._context, capacity=128, worker_count=len(frontend_cpus)
        )
        self._shared_reads = FrontendSharedCreditV1(
            self._context, capacity=OWNER_READ_SLOT_COUNT, worker_count=len(frontend_cpus)
        )
        self._workers: list[SpawnedFrontendWorkerV1] = []
        self._handoff_parent: list[socket.socket] = []
        self._write_parent: list[socket.socket] = []
        self._read_parent: list[tuple[socket.socket, ...]] = []
        self._ready: OwnerReadyV1 | None = None
        self._write_broker: SpawnedOwnerBrokerV1 | None = None
        self._read_broker: SpawnedOwnerBrokerV1 | None = None
        self._acceptor_thread: Thread | None = None
        self._acceptor_ready = Event()
        self._handoffs_drained = Event()
        self._handoffs_drained.set()
        self._stop_accepting = Event()
        self._shutdown = Event()
        self._error: BaseException | None = None
        self._error_lock = Lock()
        self._acceptor: socket.socket | None = None
        self._stopped: OwnerStoppedV1 | None = None
        self.shutdown_verified = False

    @property
    def worker_pids(self) -> tuple[int, ...]:
        return tuple(worker.pid for worker in self._workers if worker.pid is not None)

    @property
    def broker_pids(self) -> tuple[int, ...]:
        brokers = (self._write_broker, self._read_broker)
        return tuple(
            broker.pid for broker in brokers if broker is not None and broker.pid is not None
        )

    @property
    def generation(self) -> str | None:
        return self._ready.generation if self._ready is not None else None

    @property
    def ready(self) -> OwnerReadyV1:
        if self._ready is None:
            raise OwnerProtocolError("multi-frontend owner has not published readiness")
        return self._ready

    @property
    def stopped(self) -> OwnerStoppedV1:
        if not self.shutdown_verified or self._stopped is None:
            raise OwnerProtocolError("multi-frontend owner stop is not verified")
        return self._stopped

    @property
    def failed(self) -> bool:
        return self._error is not None

    @property
    def failure_type(self) -> str | None:
        return type(self._error).__name__ if self._error is not None else None

    @property
    def failure_reason(self) -> str | None:
        """Stable internal protocol reason; never includes public payload bytes."""

        return str(self._error) if self._error is not None else None

    def wait_failed(self, timeout: float = 10) -> bool:
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            if self.failed:
                return True
            Event().wait(0.01)
        return self.failed

    def start(self, *, timeout: float = 60) -> OwnerReadyV1:
        if (
            self._ready is not None
            or self._write_broker is not None
            or self._read_broker is not None
            or self._acceptor_thread is not None
        ):
            raise OwnerProtocolError("multi-frontend runtime already started")
        ready = self.owner.start(timeout=timeout)
        self._ready = ready
        try:
            direct_read_channels = (
                self.owner.transfer_read_transport() if self.direct_frontend_reads else ()
            )
            if (
                self.direct_frontend_reads
                and len(direct_read_channels) != OWNER_DIRECT_READ_SLOT_COUNT
            ):
                raise OwnerProtocolError("direct owner READ topology is incomplete")
            for worker_id, cpu in enumerate(self.frontend_cpus):
                handoff_parent, handoff_child = create_frontend_fd_handoff_pair()
                write_parent, write_child = socket.socketpair()
                read_pairs = (
                    []
                    if self.direct_frontend_reads
                    else [socket.socketpair() for _ in range(FRONTEND_READ_SLOT_COUNT)]
                )
                self._handoff_parent.append(handoff_parent)
                self._write_parent.append(write_parent)
                parent_reads = tuple(pair[0] for pair in read_pairs)
                child_reads = (
                    (
                        direct_read_channels[worker_id],
                        direct_read_channels[OWNER_READ_SLOT_COUNT + worker_id],
                    )
                    if self.direct_frontend_reads
                    else tuple(pair[1] for pair in read_pairs)
                )
                self._read_parent.append(parent_reads)
                worker = SpawnedFrontendWorkerV1(
                    self._context,
                    config=self.config,
                    contract_root=self.contract_root,
                    ready=ready,
                    handoff_channel=handoff_child,
                    write_channel=write_child,
                    read_channels=child_reads,
                    shared_credit=self._shared_total,
                    shared_read_slots=self._shared_reads,
                    worker_id=worker_id,
                    cpu=cpu,
                    direct_owner_reads=self.direct_frontend_reads,
                )
                self._workers.append(worker)
                worker.start(timeout=min(timeout, 15))
            owner_channel, broker_ready = self.owner.transfer_reactor_transport()
            self._write_broker = SpawnedOwnerBrokerV1(
                self._context,
                kind="WRITE",
                frontend_channels=tuple(self._write_parent),
                owner_channels=(owner_channel,),
                ready=broker_ready,
                shared_credit=self._shared_total,
                shared_read_slots=None,
                cpu=self.broker_cpu,
            )
            self._write_broker.start(timeout=min(timeout, 15))
            if not self.direct_frontend_reads:
                owner_read_channels = self.owner.transfer_read_transport()
                self._read_broker = SpawnedOwnerBrokerV1(
                    self._context,
                    kind="READ",
                    frontend_channels=tuple(self._read_parent),
                    owner_channels=owner_read_channels,
                    ready=None,
                    shared_credit=self._shared_total,
                    shared_read_slots=self._shared_reads,
                    cpu=self.read_broker_cpu,
                )
                self._read_broker.start(timeout=min(timeout, 15))
            self._acceptor_thread = Thread(
                target=self._run_acceptor,
                name="frontend-public-acceptor",
                daemon=True,
            )
            self._acceptor_thread.start()
            if not self._acceptor_ready.wait(timeout) or self._error is not None:
                raise OwnerProtocolError("multi-frontend acceptor readiness failed")
            return ready
        except BaseException:
            self.abort()
            raise

    def _record_failure(self, error: BaseException) -> None:
        with self._error_lock:
            if self._error is None:
                self._error = error
                if os.getenv("TRADECORE_DIAGNOSTIC_DISPATCH_ERRORS") == "1":
                    print(
                        f"tradecore multi-frontend failure: {type(error).__name__}: {error}",
                        file=sys.stderr,
                        flush=True,
                    )
        self._shared_total.fence()
        self._shared_reads.fence()
        self._shutdown.set()

    def _run_acceptor(self) -> None:
        selector = selectors.DefaultSelector()
        router = None
        acceptor = None
        try:
            if not hasattr(os, "sched_setaffinity"):
                raise OwnerProtocolError("multi-frontend acceptor affinity requires Linux")
            os.sched_setaffinity(0, {self.acceptor_cpu})
            if os.sched_getaffinity(0) != {self.acceptor_cpu}:
                raise OwnerProtocolError("multi-frontend acceptor CPU binding changed")
            assert self._ready is not None
            router = BoundedFrontendFdRouterV1(
                tuple(self._handoff_parent),
                generation=self._ready.generation,
                max_outstanding_per_worker=max(
                    _MIN_FRONTEND_HANDOFF_WINDOW,
                    self._shared_total.capacity // len(self._handoff_parent),
                ),
            )
            path = self.config.socket_path
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() or path.is_symlink():
                raise OwnerProtocolError("multi-frontend public socket already exists")
            acceptor = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._acceptor = acceptor
            acceptor.bind(str(path))
            path.chmod(self.config.socket_mode)
            acceptor.listen(self.config.listen_backlog)
            acceptor.setblocking(False)
            selector.register(acceptor, selectors.EVENT_READ, ("accept", acceptor))
            accept_registered = True
            for channel in self._handoff_parent:
                selector.register(channel, selectors.EVENT_READ, ("ack", channel))
            allowed = frozenset(
                (
                    (self.config.peer_uid, self.config.peer_gid),
                    *self.config.additional_peer_credentials,
                )
            )
            self._acceptor_ready.set()
            while not self._shutdown.is_set():
                brokers = tuple(
                    broker
                    for broker in (self._write_broker, self._read_broker)
                    if broker is not None
                )
                if any(not worker.alive for worker in self._workers) or any(
                    not broker.alive for broker in brokers
                ):
                    raise OwnerProtocolError("frontend/broker process exited; candidate fenced")
                accepting = not self._stop_accepting.is_set()
                should_poll_accept = accepting and router.available_capacity > 0
                if should_poll_accept and not accept_registered:
                    selector.register(acceptor, selectors.EVENT_READ, ("accept", acceptor))
                    accept_registered = True
                elif not should_poll_accept and accept_registered:
                    selector.unregister(acceptor)
                    accept_registered = False
                if not accepting and acceptor.fileno() >= 0:
                    acceptor.close()
                listener_ready = False
                for key, _mask in selector.select(0.05):
                    kind, channel = key.data
                    if kind == "ack":
                        router.acknowledge(channel)
                        if not router.stats().outstanding_handoffs:
                            self._handoffs_drained.set()
                        continue
                    listener_ready = True
                if listener_ready:
                    # Never accept more descriptors than can be transferred
                    # immediately. Excess peers stay in the configured kernel
                    # backlog; they are not accepted and silently closed.
                    for _ in range(min(64, router.available_capacity)):
                        try:
                            connection, _address = acceptor.accept()
                        except BlockingIOError:
                            break
                        peer = read_linux_peer_credentials(connection)
                        authorized = (peer.uid, peer.gid) in allowed
                        offered = (
                            router.offer(
                                connection,
                                peer=peer,
                                accepted_monotonic_ns=monotonic_ns(),
                            )
                            if authorized
                            else None
                        )
                        if offered is not None:
                            self._handoffs_drained.clear()
                        else:
                            # The accept budget is computed on this sole thread,
                            # so this is a protocol invariant violation rather
                            # than ordinary overload.
                            connection.close()
                            raise OwnerProtocolError(
                                "frontend handoff capacity changed during accept"
                            )
                if self._stop_accepting.is_set() and not router.stats().outstanding_handoffs:
                    self._handoffs_drained.set()
            if router.stats().outstanding_handoffs:
                raise OwnerProtocolError("acceptor stopped before FD handoff drain")
        except FrontendFdHandoffError as error:
            self._record_failure(OwnerProtocolError(f"frontend FD handoff failed: {error}"))
        except BaseException as error:
            self._record_failure(error)
        finally:
            self._acceptor_ready.set()
            if acceptor is not None:
                acceptor.close()
            selector.close()

    def close(self, *, timeout: float = 60) -> None:
        if (
            self._write_broker is None
            or (not self.direct_frontend_reads and self._read_broker is None)
            or self._ready is None
            or self.shutdown_verified
        ):
            raise OwnerProtocolError("multi-frontend runtime is not running")
        self._stop_accepting.set()
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            if self._error is not None:
                break
            total, reads = self._shared_total.snapshot(), self._shared_reads.snapshot()
            if not total.held and not reads.held and self._handoffs_drained.is_set():
                break
            Event().wait(0.01)
        else:
            self.abort()
            raise OwnerProtocolError("multi-frontend public drain expired")
        self._shutdown.set()
        if self._acceptor_thread is not None:
            self._acceptor_thread.join(max(0.0, deadline - monotonic()))
        if (
            self._acceptor_thread is None
            or self._acceptor_thread.is_alive()
            or self._error is not None
        ):
            self.abort()
            raise OwnerProtocolError("multi-frontend acceptor shutdown failed")
        try:
            self._write_broker.drain(timeout=max(1.0, deadline - monotonic()))
            if self._read_broker is not None:
                self._read_broker.drain(timeout=max(1.0, deadline - monotonic()))
                for worker in self._workers:
                    worker.close(timeout=max(1.0, deadline - monotonic()))
            else:
                for worker in self._workers:
                    worker.drain(timeout=max(1.0, deadline - monotonic()))
            self.owner.verify_external_reactor_shutdown()
            self.owner.verify_external_read_shutdown()
            stopped = self.owner.close(timeout=max(1.0, deadline - monotonic()))
            if stopped is None:
                raise OwnerProtocolError("multi-frontend owner stop receipt is missing")
            self._stopped = stopped
            self._write_broker.release(timeout=max(1.0, deadline - monotonic()))
            if self._read_broker is not None:
                self._read_broker.release(timeout=max(1.0, deadline - monotonic()))
            else:
                for worker in self._workers:
                    worker.release(timeout=max(1.0, deadline - monotonic()))
        except BaseException:
            self.abort()
            raise
        self._close_parent_channels()
        path = self.config.socket_path
        if path.exists() and not path.is_symlink():
            path.unlink()
        self._shared_total.require_all_drained()
        self._shared_reads.require_all_drained()
        self.shutdown_verified = True

    def external_lifecycle_components(self) -> tuple[dict[str, object], ...]:
        """Return exact launcher-owned roles only after a verified clean stop."""

        if (
            not self.shutdown_verified
            or self._acceptor_thread is None
            or self._acceptor_thread.is_alive()
            or self._write_broker is None
            or (not self.direct_frontend_reads and self._read_broker is None)
            or self.owner.exit_code != 0
        ):
            raise OwnerProtocolError("multi-frontend external lifecycle is incomplete")
        components: list[dict[str, object]] = [
            {
                "role": "public-acceptor",
                "kind": "THREAD",
                "pid": os.getpid(),
                "native_id": self._acceptor_thread.native_id,
                "affinity": [self.acceptor_cpu],
                "exit_code": 0,
                "shutdown_verified": True,
            }
        ]
        for worker_id, worker in enumerate(self._workers):
            if (
                worker.pid is None
                or worker.exit_code != 0
                or not worker.shutdown_verified
                or worker.affinity != (self.frontend_cpus[worker_id],)
            ):
                raise OwnerProtocolError("frontend lifecycle proof is incomplete")
            components.append(
                {
                    "role": f"public-frontend-{worker_id}",
                    "kind": "PROCESS",
                    "pid": worker.pid,
                    "affinity": list(worker.affinity),
                    "exit_code": worker.exit_code,
                    "shutdown_verified": True,
                }
            )
        broker_roles = [(self._write_broker, self.broker_cpu, "write-broker")]
        if self._read_broker is not None:
            broker_roles.append((self._read_broker, self.read_broker_cpu, "read-broker"))
        for broker, cpu, role in broker_roles:
            assert broker is not None
            if (
                broker.pid is None
                or broker.exit_code != 0
                or not broker.shutdown_verified
                or broker.affinity != (cpu,)
            ):
                raise OwnerProtocolError("broker lifecycle proof is incomplete")
            components.append(
                {
                    "role": role,
                    "kind": "PROCESS",
                    "pid": broker.pid,
                    "affinity": list(broker.affinity),
                    "exit_code": broker.exit_code,
                    "shutdown_verified": True,
                }
            )
        components.append(
            {
                "role": "oms-owner",
                "kind": "PROCESS",
                "pid": self.ready.owner_pid,
                "exit_code": self.owner.exit_code,
                "shutdown_verified": self.owner.shutdown_verified,
            }
        )
        return tuple(components)

    def _close_parent_channels(self) -> None:
        for channel in (
            *self._handoff_parent,
            *self._write_parent,
            *(channel for channels in self._read_parent for channel in channels),
        ):
            channel.close()

    def abort(self) -> None:
        self._stop_accepting.set()
        self._shutdown.set()
        for worker in self._workers:
            if worker.alive:
                worker.abort()
        for broker in (self._write_broker, self._read_broker):
            if broker is not None and broker.alive:
                broker.abort()
        if self._acceptor_thread is not None:
            self._acceptor_thread.join(5)
        if self._acceptor is not None:
            self._acceptor.close()
        self._close_parent_channels()
        self.owner.abort()
        path = self.config.socket_path
        if path.exists() and not path.is_symlink():
            path.unlink()
