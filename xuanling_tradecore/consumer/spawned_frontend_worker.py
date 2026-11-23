"""Controlled spawned frontend lifecycle for the fixed E5b candidate topology."""

from __future__ import annotations

import os
import socket
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Thread
from time import monotonic
from typing import Any, cast

from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1
from .owner_read_reactor import FRONTEND_READ_SLOT_COUNT
from .process_owner_frontend import build_process_owner_frontend
from .request_trace_v2 import BoundedRequestTraceWriterV2
from .uds_server_v2 import ConsumerUdsServerConfigV2


def _run_frontend_worker(
    config: ConsumerUdsServerConfigV2,
    contract_root: str,
    ready: OwnerReadyV1,
    handoff_channel: socket.socket,
    write_channel: socket.socket,
    read_channels: tuple[socket.socket, ...],
    shared_credit: FrontendSharedCreditV1,
    shared_read_slots: FrontendSharedCreditV1,
    worker_id: int,
    cpu: int | None,
    direct_owner_reads: bool,
    stop_event: Any,
    release_event: Any,
    status: Connection,
) -> None:
    service = None
    trace = None
    try:
        if cpu is not None:
            if not hasattr(os, "sched_setaffinity"):
                raise OwnerProtocolError("frontend CPU affinity requires Linux")
            os.sched_setaffinity(0, {cpu})
        service = build_process_owner_frontend(
            config=config,
            contract_root=Path(contract_root),
            handoff_channel=handoff_channel,
            handoff_generation=ready.generation,
            broker_ready=ready,
            broker_write_channel=write_channel,
            broker_read_channels=read_channels,
            shared_credit=shared_credit,
            shared_read_slots=shared_read_slots,
            frontend_worker_id=worker_id,
            direct_owner_reads=direct_owner_reads,
        )
        if os.environ.get("TRADECORE_DIAGNOSTIC_FRONTEND_TRACE") == "1":
            policy = config.long_running_policy
            if policy is None or policy.trace_disk_budget_bytes // 16 < policy.trace_segment_bytes:
                raise OwnerProtocolError("frontend diagnostic trace budget is unavailable")
            trace = BoundedRequestTraceWriterV2(
                config.journal_root / "traces" / f"frontend-diagnostic-{ready.generation}-{worker_id}.jsonl",
                journal_id=ready.journal_id,
                rotation_bytes=policy.trace_segment_bytes,
                max_bytes=policy.trace_disk_budget_bytes // 16,
            )
            service.set_request_trace(trace)
        service.start()

        def watch_stop() -> None:
            stop_event.wait()
            service.request_stop()

        watcher = Thread(target=watch_stop, name="frontend-control", daemon=True)
        watcher.start()
        status.send(("READY", os.getpid(), tuple(sorted(os.sched_getaffinity(0)))))
        service.serve_forever()
        if not service.submission_shutdown_verified:
            raise OwnerProtocolError("frontend owner ports did not drain")
        if trace is not None:
            trace.close()
            if not trace.coverage_verified:
                raise OwnerProtocolError("frontend diagnostic trace is incomplete")
        status.send(("DRAINED", os.getpid()))
        if not release_event.wait(60):
            raise OwnerProtocolError("frontend worker release expired")
        status.send(("STOPPED", os.getpid()))
    except BaseException as error:
        try:
            status.send(("FAILED", os.getpid(), type(error).__name__))
        except BaseException:
            pass
        raise
    finally:
        if service is not None:
            service.request_stop()
        if trace is not None:
            trace.close()
        status.close()


class SpawnedFrontendWorkerV1:
    """One fixed frontend process; no restart or attach-after-crash path."""

    def __init__(
        self,
        context: Any,
        *,
        config: ConsumerUdsServerConfigV2,
        contract_root: Path,
        ready: OwnerReadyV1,
        handoff_channel: socket.socket,
        write_channel: socket.socket,
        read_channels: tuple[socket.socket, ...],
        shared_credit: FrontendSharedCreditV1,
        shared_read_slots: FrontendSharedCreditV1,
        worker_id: int,
        cpu: int | None,
        direct_owner_reads: bool = False,
    ) -> None:
        expected_read_channels = 2 if direct_owner_reads else FRONTEND_READ_SLOT_COUNT
        if len(read_channels) != expected_read_channels:
            raise OwnerProtocolError("spawned frontend has the wrong fixed read channel count")
        self._stop: Any = context.Event()
        self._release: Any = context.Event()
        parent_status, child_status = context.Pipe(duplex=False)
        self._status = cast(Connection, parent_status)
        self._child_status = cast(Connection, child_status)
        self._child_sockets = (handoff_channel, write_channel, *read_channels)
        self._process: Any = context.Process(
            target=_run_frontend_worker,
            args=(
                config,
                str(contract_root),
                ready,
                handoff_channel,
                write_channel,
                read_channels,
                shared_credit,
                shared_read_slots,
                worker_id,
                cpu,
                direct_owner_reads,
                self._stop,
                self._release,
                child_status,
            ),
            name=f"tradecore-frontend-{worker_id}",
        )
        self._state = "NEW"
        self.pid: int | None = None
        self.affinity: tuple[int, ...] = ()
        self.shutdown_verified = False

    def start(self, *, timeout: float = 15) -> int:
        if self._state != "NEW":
            raise OwnerProtocolError("frontend worker already started")
        self._process.start()
        self._child_status.close()
        for channel in self._child_sockets:
            channel.close()
        self._state = "STARTING"
        if not self._status.poll(timeout):
            self.abort()
            raise OwnerProtocolError("frontend worker readiness expired")
        message = cast(tuple[Any, ...], self._status.recv())
        affinity_value = cast(object, message[2]) if len(message) == 3 else None
        if (
            len(message) != 3
            or message[0] != "READY"
            or message[1] != self._process.pid
            or type(message[1]) is not int
            or type(affinity_value) is not tuple
            or any(type(cpu) is not int for cpu in cast(tuple[object, ...], affinity_value))
        ):
            self.abort()
            raise OwnerProtocolError("frontend worker readiness invalid")
        pid = message[1]
        affinity = cast(tuple[int, ...], affinity_value)
        self.pid, self.affinity, self._state = pid, affinity, "RUNNING"
        return pid

    def drain(self, *, timeout: float = 15) -> None:
        if self._state != "RUNNING":
            raise OwnerProtocolError("frontend worker is not running")
        self._state = "DRAINING"
        self._stop.set()
        if not self._status.poll(timeout):
            self.abort()
            raise OwnerProtocolError("frontend worker drain expired")
        message = cast(tuple[Any, ...], self._status.recv())
        if message != ("DRAINED", self.pid) or not self._process.is_alive():
            self.abort()
            raise OwnerProtocolError("frontend worker drain was not verified")
        self._state = "DRAINED"

    def release(self, *, timeout: float = 15) -> None:
        if self._state != "DRAINED":
            raise OwnerProtocolError("frontend worker is not drained")
        self._state = "STOPPING"
        deadline = monotonic() + timeout
        self._release.set()
        if not self._status.poll(max(0.0, deadline - monotonic())):
            self.abort()
            raise OwnerProtocolError("frontend worker release expired")
        message = cast(tuple[Any, ...], self._status.recv())
        self._process.join(max(0.0, deadline - monotonic()))
        if message != ("STOPPED", self.pid) or self._process.exitcode != 0:
            self.abort()
            raise OwnerProtocolError("frontend worker stop was not verified")
        self._status.close()
        self._state = "CLOSED"
        self.shutdown_verified = True

    def close(self, *, timeout: float = 15) -> None:
        deadline = monotonic() + timeout
        self.drain(timeout=max(0.1, deadline - monotonic()))
        self.release(timeout=max(0.1, deadline - monotonic()))

    def abort(self) -> None:
        self._stop.set()
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
    def exit_code(self) -> int | None:
        value = self._process.exitcode
        return value if type(value) is int else None
