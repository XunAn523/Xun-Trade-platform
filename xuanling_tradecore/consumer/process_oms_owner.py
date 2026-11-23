"""Explicit no-Adapter spawn-owner composition; NOT the public frontend.

The launcher holds configuration, an inherited control socket and a Process,
never an OrderBook, Journal, lease, cursor key or live runtime. The child owns
the COMPLETE existing long-running composition, including all managed workers.
Its private UDS is for owner qualification only. A public proxy must not simply
stack another ready queue in front of it or discard original peer authority.
"""

from __future__ import annotations

import gc
import json
import multiprocessing as mp
import os
import select
import signal
import socket
import sys
import uuid
from dataclasses import fields, replace
from hashlib import sha256
from pathlib import Path
from threading import Event, Thread
from time import monotonic, monotonic_ns
from typing import Any, TypeVar, cast

import msgspec

from ..persistence.runtime_files import atomic_runtime_json, require_private_runtime_file
from .candidate_affinity import temporary_thread_affinity
from .candidate_systemd import require_coordinated_candidate_unit
from .fair_queue_v2 import BoundedFairOperationQueueV2, ConsumerOperationLaneV2
from .long_running_candidate import (
    LongRunningConsumerCandidate,
    candidate_binding_scope,
    configure_candidate_gil_switch,
)
from .owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadyV1,
    OwnerStoppedV1,
    OwnerStopV1,
    receive_owner_packet,
    send_owner_packet,
)
from .owner_reactor_ipc import ReactorOwnerPortV1
from .owner_read_fence import new_owner_read_fence, publish_owner_read_fence
from .owner_read_ipc import serve_owner_reads
from .owner_read_reactor import (
    OWNER_DIRECT_READ_SLOT_COUNT,
    OWNER_READ_SLOT_COUNT,
    ReactorOwnerReadsV1,
)
from .owner_submission_ipc import OwnerSubmissionPortV1, serve_owner_submissions
from .process_owner_read_plane import (
    OwnerReadPlaneBootV1,
    OwnerReadPlaneKeysV1,
    ProcessOwnerReadPlaneV1,
)
from .runtime_health import sample_cgroup_resources
from .runtime_profile import ConsumerLongRunningPolicy, ConsumerRuntimeProfile
from .uds_server_v2 import ConsumerUdsServerConfigV2, require_safe_consumer_runtime_root

_T = TypeVar("_T")


def _start_write_actor(
    actor: Thread, role_cpus: tuple[int, ...] | None, background_cpus: tuple[int, ...] | None
) -> None:
    """Spawn the single WRITE actor on its reviewed core, not the background pool."""
    cpus = (role_cpus[0],) if background_cpus is not None and role_cpus else None
    with temporary_thread_affinity(cpus):
        actor.start()


class _Boot(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    generation: str
    launcher_pid: int
    config: ConsumerUdsServerConfigV2
    private_socket: str
    contract_root: str
    cgroup_root: str
    initialize: bool
    role_cpus: tuple[int, ...] | None = None
    projection_read_cpus: tuple[int, ...] | None = None
    background_cpus: tuple[int, ...] | None = None
    maximum_database_bytes: int = 8 * 1024**3
    projection_transaction_batches: int = 16
    optimize_projection_transactions: bool = False
    reuse_verified_admission_seeds: bool = False
    scoped_cold_lookup_epochs: bool = False
    projection_commit_diagnostics: bool = False
    submission_ipc: bool = False
    read_ipc: bool = False
    isolated_read_process: bool = False
    projection_read_service_pause_ns: int = 0
    gil_switch_us: int | None = None


def _encode_path(value: object) -> str:
    if isinstance(value, Path):
        return str(value)
    raise TypeError("unsupported owner boot object")


def _decode_path(target: type[object], value: object) -> object:
    if target is Path and type(value) is str:
        return Path(value)
    raise TypeError("unsupported owner boot value")


def _decode_boot(encoded: bytes) -> _Boot:
    if type(encoded) is not bytes or not 0 < len(encoded) <= 16384:
        raise OwnerProtocolError("owner bootstrap exceeds fixed budget")
    # msgspec dataclasses do not have forbid_unknown_fields; enforce exact
    # configuration/policy fields too, not just the outer typed envelope.
    decoded: object = msgspec.json.decode(encoded)
    if not isinstance(decoded, dict):
        raise OwnerProtocolError("owner bootstrap must be an object")
    raw = cast(dict[str, object], decoded)
    config = raw.get("config")
    if not isinstance(config, dict) or set(cast(dict[str, object], config)) != {
        field.name for field in fields(ConsumerUdsServerConfigV2)
    }:
        raise OwnerProtocolError("owner bootstrap config schema mismatch")
    config_raw = cast(dict[str, object], config)
    policy = config_raw.get("long_running_policy")
    if not isinstance(policy, dict) or set(cast(dict[str, object], policy)) != {
        field.name for field in fields(ConsumerLongRunningPolicy)
    }:
        raise OwnerProtocolError("owner bootstrap requires exact persistent policy")
    boot = msgspec.json.decode(encoded, type=_Boot, dec_hook=_decode_path)
    # Reuse protocol range/identity checks without creating any runtime.
    scope = candidate_binding_scope(boot.config)[2]
    OwnerReadyV1(boot.generation, boot.launcher_pid, scope, "bootstrap-only", 0, bytes(32))
    if (
        boot.config.runtime_profile is not ConsumerRuntimeProfile.LONG_RUNNING
        or boot.config.venue_id != "NO_ADAPTER_BENCHMARK"
        or boot.config.adapter_instance_id != "disabled-no-dispatch"
        or boot.private_socket == str(boot.config.socket_path)
        or not all(
            Path(value).is_absolute()
            for value in (
                boot.private_socket,
                boot.contract_root,
                boot.cgroup_root,
            )
        )
        or not boot.config.journal_root.is_absolute()
        or not boot.config.socket_path.is_absolute()
        or len(os.fsencode(boot.private_socket)) > 107
        or (boot.read_ipc and not boot.submission_ipc)
        or (boot.isolated_read_process and (
            not boot.read_ipc
            or boot.projection_read_cpus is None
            or len(boot.projection_read_cpus) != 4
        ))
        or type(boot.projection_read_service_pause_ns) is not int
        or not 0 <= boot.projection_read_service_pause_ns <= 10_000_000
        or (
            boot.gil_switch_us is not None
            and (
                type(boot.gil_switch_us) is not int
                or boot.gil_switch_us not in {100, 250, 500, 1000, 2000, 5000}
            )
        )
    ):
        raise OwnerProtocolError("owner bootstrap must be a private no-Adapter candidate")
    return boot


def _run_owner(
    channel: socket.socket,
    encoded: bytes,
    business: socket.socket | None = None,
    read_channels: tuple[socket.socket, ...] = (),
) -> None:
    """Top-level spawn target; no live parent objects or business callables."""
    candidate: LongRunningConsumerCandidate | None = None
    watcher: Thread | None = None
    submitter: Thread | None = None
    readers: list[Thread] = []
    read_planes: tuple[ProcessOwnerReadPlaneV1, ...] = ()
    read_publisher: Thread | None = None
    read_publisher_failures: list[BaseException] = []
    read_planes_closed = False
    stop_received, control_failed, finished = Event(), Event(), Event()
    try:
        boot = _decode_boot(encoded)
        if boot.submission_ipc != (business is not None):
            raise OwnerProtocolError("owner inherited business channel missing/unexpected")
        expected_read_channels = (
            OWNER_DIRECT_READ_SLOT_COUNT
            if boot.isolated_read_process
            else OWNER_READ_SLOT_COUNT
        )
        if (boot.read_ipc and len(read_channels) != expected_read_channels) or (
            not boot.read_ipc and read_channels
        ):
            raise OwnerProtocolError("owner requires the exact fixed read rendezvous")
        if os.getppid() != boot.launcher_pid or not gc.isenabled():
            raise OwnerProtocolError("owner launcher identity/GC changed")
        require_coordinated_candidate_unit(Path(boot.cgroup_root))
        require_safe_consumer_runtime_root(Path(boot.private_socket).parent)
        # ``spawn`` starts a fresh interpreter and therefore does not inherit
        # the launcher's switch interval.  Freeze the reviewed CLI experiment
        # before constructing the runtime or any of its managed threads.
        configure_candidate_gil_switch(boot.gil_switch_us)
        # The private endpoint maps ONLY this trusted local launcher identity.
        # Original public SO_PEERCRED forwarding is deliberately not implemented
        # by this package, and must not be inferred from the private peer.
        private_config = replace(
            boot.config,
            socket_path=Path(boot.private_socket),
            peer_uid=os.geteuid(),
            peer_gid=os.getegid(),
            additional_peer_credentials=(),
            socket_mode=0o600,
        )
        read_service_signal = (
            mp.get_context("spawn").RawValue("b", 0)
            if boot.isolated_read_process
            else None
        )
        candidate = LongRunningConsumerCandidate(
            config=private_config,
            contract_root=Path(boot.contract_root),
            initialize=boot.initialize,
            sample_resources=lambda: sample_cgroup_resources(
                cgroup_root=Path(boot.cgroup_root),
                data_root=boot.config.journal_root,
            ),
            maximum_database_bytes=boot.maximum_database_bytes,
            role_cpus=boot.role_cpus,
            projection_read_cpus=(
                boot.projection_read_cpus[:2]
                if boot.isolated_read_process and boot.projection_read_cpus is not None
                else boot.projection_read_cpus
            ),
            background_cpus=boot.background_cpus,
            projection_transaction_batches=boot.projection_transaction_batches,
            optimize_projection_transactions=boot.optimize_projection_transactions,
            reuse_verified_admission_seeds=boot.reuse_verified_admission_seeds,
            scoped_cold_lookup_epochs=boot.scoped_cold_lookup_epochs,
            projection_commit_diagnostics=boot.projection_commit_diagnostics,
            isolate_submit_execution=False,
            owner_submission_ipc_only=boot.submission_ipc,
            owner_read_ipc_only=boot.read_ipc,
            external_read_plane=boot.isolated_read_process,
            committed_replay_window=False,
            projection_read_service_pause_ns=boot.projection_read_service_pause_ns,
            projection_read_service_signal=read_service_signal,
        )
        owned = candidate

        def fence(_error: BaseException | None = None) -> None:
            control_failed.set()
            owned.guard.fail()
            owned.service.request_stop()

        def control() -> None:
            try:
                while not finished.is_set():
                    if not select.select([channel], [], [], 0.05)[0]:
                        continue
                    message = receive_owner_packet(
                        channel,
                        deadline_ns=monotonic_ns() + 5_000_000_000,
                    )
                    if type(message) is not OwnerStopV1 or (
                        message.generation != boot.generation or message.serial != 1
                    ):
                        raise OwnerProtocolError("owner control generation/serial mismatch")
                    stop_received.set()
                    owned.service.request_stop()
                    return
            except Exception:
                fence()  # EOF/partial control cannot publish a clean stop

        def on_signal(_number: int, _frame: object) -> None:
            fence()  # containment is not a verified stop acknowledgement

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, on_signal)
        owned.start()
        # This dedicated owner is now fully constructed and has not published
        # READY. Compact/freeze only the stable startup graph so later full-GC
        # scans do not repeatedly traverse it. Automatic GC remains enabled for
        # every object allocated after this boundary.
        owned.gc_observer.freeze_stable_graph()
        ready = OwnerReadyV1(
            boot.generation,
            os.getpid(),
            owned.scope_sha256,
            owned.runtime.journal.journal_id,
            owned.runtime.recovered_watermark,
            bytes.fromhex(owned.runtime.recovered_source_sha256),
            owned.trace.path.name,
        )
        if boot.isolated_read_process:
            assert boot.projection_read_cpus is not None
            policy = boot.config.long_running_policy
            if policy is None:
                raise OwnerProtocolError("owner read-plane requires persistent policy")
            shared = new_owner_read_fence(mp.get_context("spawn"))
            publish_owner_read_fence(
                shared,
                health=owned.projector.health(),
                bound=owned.runtime.journal.read_bound(),
            )
            identity = owned.runtime.identity
            replay_channels = read_channels[:OWNER_READ_SLOT_COUNT]
            snapshot_channels = read_channels[OWNER_READ_SLOT_COUNT:]

            def read_plane_boot(
                *,
                role: str,
                allowed_lanes: frozenset[ConsumerOperationLaneV2],
                coordinator_cpu: int,
                store_cpu: int,
            ) -> OwnerReadPlaneBootV1:
                return OwnerReadPlaneBootV1(
                    role=role,
                    allowed_lanes=allowed_lanes,
                    config=boot.config,
                    contract_root=Path(boot.contract_root),
                    projection_root=owned.runtime.projection_root,
                    journal_root=owned.runtime.journal.path,
                    journal_id=owned.runtime.journal.journal_id,
                    profile_hash=owned.runtime.journal.profile_hash,
                    segment_frames=policy.segment_frames,
                    maximum_database_bytes=boot.maximum_database_bytes,
                    coordinator_cpu=coordinator_cpu,
                    store_cpu=store_cpu,
                    keys=OwnerReadPlaneKeysV1(
                        identity.event_key,
                        identity.event_retired,
                        identity.snapshot_key,
                        identity.snapshot_retired,
                    ),
                    ready=ready,
                )

            read_planes = (
                ProcessOwnerReadPlaneV1(
                    channels=replay_channels,
                    boot=read_plane_boot(
                        role="owner-replay-plane",
                        allowed_lanes=frozenset({ConsumerOperationLaneV2.REPLAY}),
                        coordinator_cpu=boot.projection_read_cpus[0],
                        store_cpu=boot.projection_read_cpus[1],
                    ),
                    shared=shared,
                    read_service_signal=read_service_signal,
                ),
                ProcessOwnerReadPlaneV1(
                    channels=snapshot_channels,
                    boot=read_plane_boot(
                        role="owner-snapshot-plane",
                        allowed_lanes=frozenset(
                            {
                                ConsumerOperationLaneV2.SNAPSHOT,
                                ConsumerOperationLaneV2.OTHER,
                            }
                        ),
                        coordinator_cpu=boot.projection_read_cpus[2],
                        store_cpu=boot.projection_read_cpus[3],
                    ),
                    shared=shared,
                    read_service_signal=None,
                ),
            )
            def publish_reads() -> None:
                try:
                    while not finished.wait(0.01):
                        if any(read_plane.failed for read_plane in read_planes):
                            raise OwnerProtocolError("owner read-plane exited unexpectedly")
                        publish_owner_read_fence(
                            shared,
                            health=owned.projector.health(),
                            bound=owned.runtime.journal.read_bound(),
                        )
                except BaseException as error:
                    if not finished.is_set():
                        read_publisher_failures.append(error)
                        fence(error)

            read_publisher = Thread(
                target=publish_reads, name="oms-read-fence-publisher", daemon=True
            )
            with temporary_thread_affinity(boot.background_cpus):
                read_publisher.start()
            # Keep the initial fence fresh while both read-plane spawn trees
            # construct their SQL children.  Starting this publisher only
            # after both READY handshakes leaves the already-running replay
            # pump exposed to the two-second stale-fence gate on a cold host.
            for read_plane in read_planes:
                read_plane.start()
            # Every READ now terminates outside the OMS owner. Replay and
            # snapshot/other have independent coordinators, stores and fixed
            # channel sets, so neither can consume WRITE's GIL or CPU budget.
            read_channels = ()
        # Recovery and every child start precede the handshake. The original
        # public socket is never opened by this child.
        send_owner_packet(channel, ready, deadline_ns=monotonic_ns() + 5_000_000_000)
        watcher = Thread(target=control, name="oms-owner-control", daemon=True)
        with temporary_thread_affinity(boot.background_cpus):
            watcher.start()
            if business is not None:
                submitter = Thread(
                    target=serve_owner_submissions,
                    kwargs={
                        "channel": business,
                        "candidate": owned,
                        "original_config": boot.config,
                        "ready": ready,
                        "stopped": finished,
                        "fence": fence,
                    },
                    name="oms-inherited-write",
                    daemon=True,
                )
                # Background roles must not accidentally inherit the WRITE
                # actor: it is the latency-sensitive single OMS submitter.
                _start_write_actor(submitter, boot.role_cpus, boot.background_cpus)
            for read_channel in read_channels:
                reader = Thread(
                    target=serve_owner_reads,
                    kwargs={
                        "channel": read_channel,
                        "listener": owned.service.owner_submission_listener,
                        "original_config": boot.config,
                        "ready": ready,
                        "stopped": finished,
                        "fence": fence,
                        "allowed_lanes": (
                            frozenset({ConsumerOperationLaneV2.REPLAY})
                            if boot.isolated_read_process
                            else None
                        ),
                    },
                    name="oms-inherited-read",
                    daemon=True,
                )
                reader.start()
                readers.append(reader)
        evidence_cpus = os.environ.get("TRADECORE_DIAGNOSTIC_EVIDENCE_CPUS")
        if evidence_cpus is not None:
            from .owner_thread_identity import set_evidence_thread_affinity

            set_evidence_thread_affinity(evidence_cpus)
        if os.environ.get("TRADECORE_DIAGNOSTIC_OWNER_THREAD_IDENTITIES") == "1":
            # One startup snapshot only, after known managed threads exist.
            # Not on the request path; no payload or credential is recorded.
            from .owner_thread_identity import write_owner_thread_snapshot

            write_owner_thread_snapshot(
                boot.config.journal_root / "traces" / f"owner-threads-{uuid.uuid4().hex}.json"
            )
        owned.serve_forever()
        finished.set()
        if read_publisher is not None:
            read_publisher.join(2)
            if read_publisher.is_alive():
                raise OwnerProtocolError("owner read fence publisher did not stop")
            if read_publisher_failures:
                failure = read_publisher_failures[0]
                raise OwnerProtocolError(
                    "owner read fence publisher failed: "
                    f"{type(failure).__name__}: {failure}"
                ) from failure
        watcher.join(2)
        if watcher.is_alive():
            raise OwnerProtocolError("owner control failed to drain")
        if submitter is not None:
            submitter.join(10)
            if submitter.is_alive():
                raise OwnerProtocolError("owner WRITE still references source; drain not verified")
        for reader in readers:
            reader.join(10)
        if any(reader.is_alive() for reader in readers):
            raise OwnerProtocolError("owner read actors did not stop")
        if read_planes:
            for read_plane in read_planes:
                read_plane.close()
            read_planes_closed = all(
                read_plane.shutdown_verified for read_plane in read_planes
            )
            if not read_planes_closed:
                raise OwnerProtocolError("owner read-plane drain was not verified")
        owned.close()
        if control_failed.is_set() or not stop_received.is_set():
            raise OwnerProtocolError("owner control lost; no clean shutdown receipt")
        managed = owned.trace.path.with_suffix(".managed-exit.json")
        require_private_runtime_file(managed)
        final = cast(dict[str, object], json.loads(managed.read_bytes()))
        if final.get("all_managed_workers_exited_zero") is not True:
            raise OwnerProtocolError("owner managed children not verified")
        if read_planes:
            assert boot.projection_read_cpus is not None
            if any(
                read_plane.pid is None
                or read_plane.exit_code != 0
                or not read_plane.shutdown_verified
                for read_plane in read_planes
            ):
                raise OwnerProtocolError("owner read-plane exit proof is incomplete")
            final["owner_managed_processes"] = [
                {
                    "role": read_plane.role,
                    "kind": "PROCESS",
                    "pid": read_plane.pid,
                    "affinity": [boot.projection_read_cpus[index * 2]],
                    "exit_code": read_plane.exit_code,
                    "shutdown_verified": True,
                }
                for index, read_plane in enumerate(read_planes)
            ]
            atomic_runtime_json(managed, final)
        final_lsn = final.get("final_lsn")
        if type(final_lsn) is not int:
            raise OwnerProtocolError("owner final source position unavailable")
        send_owner_packet(
            channel,
            OwnerStoppedV1(
                boot.generation,
                1,
                os.getpid(),
                ready.journal_id,
                final_lsn,
                sha256(managed.read_bytes()).digest(),
            ),
            deadline_ns=monotonic_ns() + 5_000_000_000,
        )
    finally:
        finished.set()
        try:
            if read_publisher is not None:
                read_publisher.join(2)
            if submitter is not None:
                submitter.join(2)
            for reader in readers:
                reader.join(2)
            if (
                candidate is not None
                and (submitter is None or not submitter.is_alive())
                and not any(reader.is_alive() for reader in readers)
            ):
                candidate.close()
        finally:
            if read_planes and not read_planes_closed:
                for read_plane in read_planes:
                    if not read_plane.shutdown_verified:
                        read_plane.abort()
            channel.close()
            if business is not None:
                business.close()
            for read_channel in read_channels:
                read_channel.close()
            if watcher is not None:
                watcher.join(2)


class SpawnedOmsOwnerCandidate:
    """One-shot launcher for ownership qualification, never auto-restarted.

    A standalone frontend CLI must own/drain its resource tracker separately.
    close() proves the managed OMS subtree, NOT the launcher's shared tracker
    or a full public-UDS/performance/READY lifecycle.
    """

    def __init__(
        self,
        *,
        config: ConsumerUdsServerConfigV2,
        private_socket: Path,
        contract_root: Path,
        cgroup_root: Path,
        initialize: bool,
        role_cpus: tuple[int, ...] | None = None,
        projection_read_cpus: tuple[int, ...] | None = None,
        background_cpus: tuple[int, ...] | None = None,
        maximum_database_bytes: int = 8 * 1024**3,
        projection_transaction_batches: int = 16,
        optimize_projection_transactions: bool = False,
        reuse_verified_admission_seeds: bool = False,
        scoped_cold_lookup_epochs: bool = False,
        projection_commit_diagnostics: bool = False,
        submission_ipc: bool = False,
        read_ipc: bool = False,
        isolated_read_process: bool = False,
        projection_read_service_pause_ns: int = 0,
        gil_switch_us: int | None = None,
    ) -> None:
        boot = _Boot(
            uuid.uuid4().hex,
            os.getpid(),
            config,
            str(private_socket),
            str(contract_root),
            str(cgroup_root),
            initialize,
            role_cpus,
            projection_read_cpus,
            background_cpus,
            maximum_database_bytes,
            projection_transaction_batches,
            optimize_projection_transactions,
            reuse_verified_admission_seeds,
            scoped_cold_lookup_epochs,
            projection_commit_diagnostics,
            submission_ipc,
            read_ipc,
            isolated_read_process,
            projection_read_service_pause_ns,
            gil_switch_us,
        )
        self._encoded = msgspec.json.encode(boot, enc_hook=_encode_path)
        self._boot = _decode_boot(self._encoded)
        self._process: Any = None  # multiprocessing context-specific process implementation
        self._channel: socket.socket | None = None
        self._business_channel: socket.socket | None = None
        self._submission_port: OwnerSubmissionPortV1[Any] | None = None
        self._reactor_port: ReactorOwnerPortV1[Any] | None = None
        self._reactor_transport_transferred = False
        self._external_reactor_shutdown_verified = False
        self._read_channels: tuple[socket.socket, ...] = ()
        self._read_pool: ReactorOwnerReadsV1[Any] | None = None
        self._read_transport_transferred = False
        self._external_read_shutdown_verified = False
        self._state = "NEW"
        self._ready: OwnerReadyV1 | None = None
        self.shutdown_verified = False

    @property
    def pid(self) -> int | None:
        pid = self._process.pid if self._process is not None else None
        return pid if type(pid) is int else None

    @property
    def has_read_ipc(self) -> bool:
        return self._boot.read_ipc

    @property
    def exit_code(self) -> int | None:
        value = self._process.exitcode if self._process is not None else None
        return value if type(value) is int else None

    @property
    def failed(self) -> bool:
        if self._state == "RUNNING" and not self._process.is_alive():
            self._state = "FAILED"
        return self._state == "FAILED"

    @property
    def private_socket(self) -> Path:
        if self.failed or self._state != "RUNNING":
            raise OwnerProtocolError("owner is not available; do not retry unknown writes")
        return Path(self._boot.private_socket)

    def start(self, *, timeout: float = 60) -> OwnerReadyV1:
        if sys.platform != "linux" or sys.version_info[:2] != (3, 10):
            raise OwnerProtocolError("OMS process owner requires Linux project CPython 3.10")
        self._timeout(timeout)
        if self._state != "NEW":
            raise OwnerProtocolError("OMS process owner cannot restart")
        require_coordinated_candidate_unit(Path(self._boot.cgroup_root))
        require_safe_consumer_runtime_root(Path(self._boot.private_socket).parent)
        self._state = "STARTING"
        parent, child = socket.socketpair()
        business_parent, business_child = (
            socket.socketpair() if self._boot.submission_ipc else (None, None)
        )
        read_pairs = (
            tuple(
                socket.socketpair()
                for _ in range(
                    OWNER_DIRECT_READ_SLOT_COUNT
                    if self._boot.isolated_read_process
                    else OWNER_READ_SLOT_COUNT
                )
            )
            if self._boot.read_ipc
            else ()
        )
        self._read_channels = tuple(pair[0] for pair in read_pairs)
        read_children = tuple(pair[1] for pair in read_pairs)
        self._business_channel = business_parent
        self._channel = parent
        self._process = mp.get_context("spawn").Process(
            target=_run_owner,
            args=(child, self._encoded, business_child, read_children)
            if read_children
            else (child, self._encoded, business_child),
            name="oms-runtime-owner",
        )
        try:
            self._process.start()
            child.close()
            if business_child is not None:
                business_child.close()
            for read_child in read_children:
                read_child.close()
            ready = receive_owner_packet(parent, deadline_ns=monotonic_ns() + int(timeout * 1e9))
            if type(ready) is not OwnerReadyV1 or (
                ready.generation != self._boot.generation
                or ready.owner_pid != self.pid
                or ready.scope_sha256 != candidate_binding_scope(self._boot.config)[2]
                or not ready.trace_name
                or not self._process.is_alive()
            ):
                raise OwnerProtocolError("OMS process owner readiness mismatch")
            self._ready, self._state = ready, "RUNNING"
            return ready
        except BaseException:
            self.abort()
            raise
        finally:
            child.close()
            if business_child is not None:
                business_child.close()
            for read_child in read_children:
                read_child.close()

    def open_read_pool(
        self, ready_queue: BoundedFairOperationQueueV2[_T]
    ) -> ReactorOwnerReadsV1[_T]:
        if (
            self.failed
            or self._state != "RUNNING"
            or not self._read_channels
            or self._read_pool is not None
            or self._read_transport_transferred
        ):
            raise OwnerProtocolError("owner read pool unavailable/already transferred")
        channel_lanes = (
            (frozenset({ConsumerOperationLaneV2.REPLAY}),) * OWNER_READ_SLOT_COUNT
            + (
                frozenset(
                    {
                        ConsumerOperationLaneV2.SNAPSHOT,
                        ConsumerOperationLaneV2.OTHER,
                    }
                ),
            )
            * OWNER_READ_SLOT_COUNT
            if self._boot.isolated_read_process
            else None
        )
        self._read_pool = pool = ReactorOwnerReadsV1(
            self._read_channels,
            ready_queue=ready_queue,
            channel_lanes=channel_lanes,
        )
        return pool

    def transfer_read_transport(self) -> tuple[socket.socket, ...]:
        """Transfer the fixed read rendezvous to one supervised broker process.

        The launcher deliberately retains no duplicate descriptor.  A clean
        owner shutdown is impossible until the supervisor reports that the
        external broker completed the same bounded drain contract as the
        in-process ``ReactorOwnerReadsV1``.
        """

        if (
            self.failed
            or self._state != "RUNNING"
            or not self._read_channels
            or self._read_pool is not None
            or self._read_transport_transferred
        ):
            raise OwnerProtocolError("owner read transport unavailable/already transferred")
        channels = self._read_channels
        self._read_channels = ()
        self._read_transport_transferred = True
        return channels

    def verify_external_read_shutdown(self) -> None:
        if (
            self.failed
            or self._state != "RUNNING"
            or not self._read_transport_transferred
            or self._external_read_shutdown_verified
        ):
            raise OwnerProtocolError("external read shutdown proof is not admissible")
        self._external_read_shutdown_verified = True

    def open_submission_port(
        self, ready_queue: BoundedFairOperationQueueV2[_T]
    ) -> OwnerSubmissionPortV1[_T]:
        if (
            self.failed
            or self._state != "RUNNING"
            or self._ready is None
            or self._business_channel is None
            or self._submission_port is not None
            or self._reactor_port is not None
            or self._reactor_transport_transferred
        ):
            raise OwnerProtocolError("owner business port unavailable/already transferred")
        port = OwnerSubmissionPortV1(
            self._business_channel, ready=self._ready, ready_queue=ready_queue
        )
        self._submission_port = port
        return port

    def open_reactor_port(
        self, ready_queue: BoundedFairOperationQueueV2[_T]
    ) -> ReactorOwnerPortV1[_T]:
        """Transfer to the actual public reactor thread exactly once."""
        if (
            self.failed
            or self._state != "RUNNING"
            or self._ready is None
            or self._business_channel is None
            or self._submission_port is not None
            or self._reactor_port is not None
            or self._reactor_transport_transferred
        ):
            raise OwnerProtocolError("owner reactor port unavailable/already transferred")
        port = ReactorOwnerPortV1(
            self._business_channel, ready=self._ready, ready_queue=ready_queue
        )
        self._reactor_port = port
        return port

    def transfer_reactor_transport(self) -> tuple[socket.socket, OwnerReadyV1]:
        """Transfer the unique business transport to a supervised broker.

        This is a one-way ownership move.  The caller must close its local
        descriptor after spawning the broker and may only certify shutdown
        after receiving the broker's bounded STOPPED acknowledgement.
        """

        if (
            self.failed
            or self._state != "RUNNING"
            or self._ready is None
            or self._business_channel is None
            or self._submission_port is not None
            or self._reactor_port is not None
            or self._reactor_transport_transferred
        ):
            raise OwnerProtocolError("owner reactor transport unavailable/already transferred")
        channel = self._business_channel
        self._business_channel = None
        self._reactor_transport_transferred = True
        return channel, self._ready

    def verify_external_reactor_shutdown(self) -> None:
        if (
            self.failed
            or self._state != "RUNNING"
            or not self._reactor_transport_transferred
            or self._external_reactor_shutdown_verified
        ):
            raise OwnerProtocolError("external reactor shutdown proof is not admissible")
        self._external_reactor_shutdown_verified = True

    def close(self, *, timeout: float = 60) -> OwnerStoppedV1 | None:
        self._timeout(timeout)
        if self._state == "CLOSED":
            return None
        if self._state == "NEW":
            self._state = "CLOSED"
            return None
        if self.failed or self._state != "RUNNING" or self._ready is None:
            self.abort()
            raise OwnerProtocolError("owner failed; shutdown not verified")
        deadline = monotonic() + timeout
        self._state = "STOPPING"
        channel = self._channel
        assert channel is not None
        try:
            if self._submission_port is not None:
                self._submission_port.stop()
            if self._reactor_port is not None and not self._reactor_port.shutdown_verified:
                raise OwnerProtocolError("public reactor has not drained owner credits")
            if (
                self._reactor_transport_transferred
                and not self._external_reactor_shutdown_verified
            ):
                raise OwnerProtocolError("external reactor has not drained owner credits")
            if self._read_pool is not None and not self._read_pool.shutdown_verified:
                raise OwnerProtocolError("public reactor has not drained read rendezvous")
            if (
                self._read_transport_transferred
                and not self._external_read_shutdown_verified
            ):
                raise OwnerProtocolError("external reactor has not drained read rendezvous")
            send_owner_packet(
                channel,
                OwnerStopV1(self._boot.generation, 1),
                deadline_ns=monotonic_ns() + int(max(0, deadline - monotonic()) * 1e9),
            )
            result = receive_owner_packet(
                channel,
                deadline_ns=monotonic_ns() + int(max(0, deadline - monotonic()) * 1e9),
            )
            self._process.join(max(0, deadline - monotonic()))
            if type(result) is not OwnerStoppedV1 or (
                result.generation != self._boot.generation
                or result.serial != 1
                or result.owner_pid != self.pid
                or result.journal_id != self._ready.journal_id
                or result.final_lsn < self._ready.recovered_lsn
                or self._process.exitcode != 0
            ):
                raise OwnerProtocolError("owner stop acknowledgement/exit mismatch")
            managed = (
                self._boot.config.journal_root / "traces" / self._ready.trace_name
            ).with_suffix(".managed-exit.json")
            require_private_runtime_file(managed)
            payload = managed.read_bytes()
            if len(payload) > 16384 or sha256(payload).digest() != result.managed_exit_sha256:
                raise OwnerProtocolError("owner managed-exit hash mismatch")
            evidence = cast(dict[str, object], json.loads(payload))
            if (
                evidence.get("runtime_closed") is not True
                or evidence.get("all_managed_workers_exited_zero") is not True
                or evidence.get("journal_id") != result.journal_id
                or evidence.get("final_lsn") != result.final_lsn
                or evidence.get("owned_helper_exit_status") != "NOT_VERIFIED"
            ):
                raise OwnerProtocolError("owner managed-exit evidence mismatch")
            self.shutdown_verified, self._state = True, "CLOSED"
            return result
        except BaseException:
            self.abort()
            raise
        finally:
            channel.close()
            if self._business_channel is not None:
                self._business_channel.close()
            for read_channel in self._read_channels:
                read_channel.close()

    def abort(self) -> None:
        """Finite containment of this exact child only, never a successful drain."""
        self._state = "FAILED"
        self.shutdown_verified = False
        if self._submission_port is not None:
            self._submission_port.fail()
        if self._reactor_port is not None:
            self._reactor_port.close_transport()
        for read_channel in self._read_channels:
            read_channel.close()
        if self._business_channel is not None:
            self._business_channel.close()
        if self._channel is not None:
            self._channel.close()  # owner sees control loss and fences admission
        if self._process is not None and self._process.pid is not None:
            self._process.join(0.2)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(5)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(5)
            if self._process.is_alive():
                raise OwnerProtocolError("owner containment failed; cgroup supervisor required")

    @staticmethod
    def _timeout(value: float) -> None:
        if type(value) not in (int, float) or not 0 < value <= 60:
            raise OwnerProtocolError("owner lifecycle requires a bounded timeout")
