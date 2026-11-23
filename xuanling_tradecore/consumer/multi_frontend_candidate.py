"""Standalone default-off E5b multi-frontend no-Adapter qualification runtime."""

from __future__ import annotations

import argparse
import json
import os
import signal
from hashlib import sha256
from pathlib import Path
from threading import Event
from time import monotonic_ns
from typing import Protocol, cast

from ..persistence.runtime_files import atomic_runtime_json, require_private_runtime_file
from .candidate_helper import OwnedCandidateTracker
from .candidate_systemd import require_coordinated_candidate_unit
from .multi_frontend_runtime import MultiFrontendOwnerRuntimeV1
from .owner_ipc_v1 import OwnerProtocolError, OwnerReadyV1, OwnerStoppedV1
from .process_oms_owner import SpawnedOmsOwnerCandidate
from .qualification_bootstrap import initialize_new_qualification_runtime
from .uds_server_v2 import load_consumer_uds_server_config_v2


class _RuntimeHealth(Protocol):
    @property
    def failed(self) -> bool: ...

    @property
    def failure_type(self) -> str | None: ...

    @property
    def failure_reason(self) -> str | None: ...


def _cpus(value: str, count: int) -> tuple[int, ...]:
    parts = value.split(",")
    if (
        len(parts) != count
        or any(not part.isascii() or not part.isdecimal() for part in parts)
        or len(set(parts)) != count
    ):
        raise ValueError(f"exactly {count} unique decimal CPUs required")
    return tuple(int(part) for part in parts)


def _frontend_cpus(value: str) -> tuple[int, ...]:
    parts = value.split(",")
    if (
        not 2 <= len(parts) <= 8
        or any(not part.isascii() or not part.isdecimal() for part in parts)
        or len(set(parts)) != len(parts)
    ):
        raise ValueError("2..8 unique decimal frontend CPUs required")
    return tuple(int(part) for part in parts)


def _wait_for_stop_or_failure(
    runtime: _RuntimeHealth,
    stopping: Event,
    *,
    poll_seconds: float = 0.1,
) -> None:
    """Return only for an operator stop; surface a failed child topology."""

    while not stopping.wait(poll_seconds):
        if runtime.failed:
            reason = runtime.failure_reason or "UNAVAILABLE"
            raise OwnerProtocolError(
                "multi-frontend runtime failed: "
                f"{runtime.failure_type or 'UNKNOWN'}: {reason}"
            )


def publish_multi_frontend_lifecycle(
    *,
    journal_root: Path,
    ready: OwnerReadyV1,
    stopped: OwnerStoppedV1,
    external_components: tuple[dict[str, object], ...],
    helper_exit: dict[str, object],
) -> Path:
    """Publish one hash-bound certificate after every owned role has exited."""

    managed_path = (journal_root / "traces" / ready.trace_name).with_suffix(
        ".managed-exit.json"
    )
    require_private_runtime_file(managed_path)
    managed_bytes = managed_path.read_bytes()
    if len(managed_bytes) > 32_768 or sha256(managed_bytes).digest() != stopped.managed_exit_sha256:
        raise OwnerProtocolError("multi-frontend managed-exit hash mismatch")
    managed: object = json.loads(managed_bytes)
    if not isinstance(managed, dict):
        raise OwnerProtocolError("multi-frontend managed-exit is not an object")
    managed_payload = dict(cast(dict[str, object], managed))
    nested = managed_payload.get("owner_managed_processes")
    if not isinstance(nested, list):
        raise OwnerProtocolError("multi-frontend owner read-plane proof is missing")
    nested_values = cast(list[object], nested)
    if not nested_values or any(not isinstance(item, dict) for item in nested_values):
        raise OwnerProtocolError("multi-frontend owner read-plane proof is missing")
    nested_roles = {
        cast(dict[str, object], item).get("role") for item in nested_values
    }
    if nested_roles not in (
        {"owner-read-plane"},
        {"owner-replay-plane", "owner-snapshot-plane"},
    ):
        raise OwnerProtocolError("multi-frontend owner read-plane proof is invalid")
    components: list[dict[str, object]] = [
        *external_components,
        *cast(list[dict[str, object]], nested_values),
    ]
    external_roles = {
        component.get("role")
        for component in external_components
        if isinstance(component.get("role"), str)
    }
    frontend_roles = {
        role for role in external_roles if isinstance(role, str) and role.startswith("public-frontend-")
    }
    expected_roles = {
        "public-acceptor",
        "write-broker",
        "oms-owner",
        *cast(set[str], nested_roles),
        *frontend_roles,
    }
    if "read-broker" in external_roles:
        expected_roles.add("read-broker")
    expected_frontends = {
        f"public-frontend-{index}" for index in range(len(frontend_roles))
    }
    if frontend_roles != expected_frontends:
        raise OwnerProtocolError("multi-frontend lifecycle component proof is incomplete")
    roles: list[str] = []
    process_pids: list[int] = []
    for component in components:
        role, pid = component.get("role"), component.get("pid")
        if (
            not isinstance(role, str)
            or type(pid) is not int
            or pid < 1
            or component.get("shutdown_verified") is not True
            or component.get("exit_code") != 0
        ):
            raise OwnerProtocolError("multi-frontend lifecycle component proof is incomplete")
        roles.append(role)
        if component.get("kind") == "PROCESS":
            process_pids.append(pid)
    if (
        set(roles) != expected_roles
        or len(set(roles)) != len(roles)
        or len(set(process_pids)) != len(process_pids)
    ):
        raise OwnerProtocolError("multi-frontend lifecycle component proof is incomplete")
    if (
        helper_exit.get("schema") != "candidate-owned-helper-exit-v1"
        or helper_exit.get("ownership_verified") is not True
        or helper_exit.get("exit_code") != 0
        or helper_exit.get("remaining_descendants") != 0
        or helper_exit.get("forced_kill") is not False
    ):
        raise OwnerProtocolError("multi-frontend owned helper proof is incomplete")
    component_bytes = json.dumps(
        components, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    lifecycle = journal_root / "traces" / (
        ready.journal_id + "-" + ready.generation + "-multi-frontend-lifecycle.json"
    )
    if lifecycle.exists() or lifecycle.is_symlink():
        raise OwnerProtocolError("multi-frontend lifecycle certificate already exists")
    atomic_runtime_json(
        lifecycle,
        {
            **managed_payload,
            "schema": "consumer-multi-frontend-lifecycle-v1",
            "generation_sha256": sha256(ready.generation.encode("ascii")).hexdigest(),
            "journal_id": ready.journal_id,
            "recovered_lsn": ready.recovered_lsn,
            "final_lsn": stopped.final_lsn,
            "managed_exit_sha256": stopped.managed_exit_sha256.hex(),
            "runtime_closed": True,
            "all_managed_workers_exited_zero": True,
            "all_owned_descendants_exited_zero": True,
            "owned_helper_exit_status": "VERIFIED",
            "helper_exit": helper_exit,
            "public_socket_removed": True,
            "shared_credits_drained": True,
            "lifecycle_components": components,
            "lifecycle_components_sha256": sha256(component_bytes).hexdigest(),
            "observed_monotonic_ns": monotonic_ns(),
            "performance_certificate": False,
        },
    )
    return lifecycle


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--contract-root", type=Path, required=True)
    parser.add_argument("--cgroup-root", type=Path, required=True)
    parser.add_argument("--private-socket", type=Path, required=True)
    parser.add_argument(
        "--role-cpus", required=True, help="reactor,projection,cold,segment-sealer"
    )
    parser.add_argument("--projection-read-cpus", required=True)
    parser.add_argument("--background-cpus", help="two bounded owner background CPU IDs")
    parser.add_argument(
        "--gil-switch-us", type=int, choices=(100, 250, 500, 1000, 2000, 5000),
        help="candidate-only owner interpreter GIL switch interval in microseconds",
    )
    parser.add_argument("--broker-cpu", type=int, required=True)
    parser.add_argument("--read-broker-cpu", type=int, required=True)
    parser.add_argument("--acceptor-cpu", type=int, required=True)
    parser.add_argument("--frontend-cpus", required=True)
    parser.add_argument("--initialize-if-new", action="store_true")
    parser.add_argument("--projection-transaction-batches", type=int, default=256)
    args = parser.parse_args()
    require_coordinated_candidate_unit(args.cgroup_root)
    roles = _cpus(args.role_cpus, 4)
    reads = _cpus(args.projection_read_cpus, 4)
    background = _cpus(args.background_cpus, 2) if args.background_cpus else ()
    frontends = _frontend_cpus(args.frontend_cpus)
    cpus = (
        *roles,
        *reads,
        args.broker_cpu,
        args.read_broker_cpu,
        args.acceptor_cpu,
        *frontends,
        *background,
    )
    if (
        args.broker_cpu < 0
        or args.acceptor_cpu < 0
        or args.read_broker_cpu < 0
        or len(set(cpus)) != len(cpus)
        or not set(cpus) <= os.sched_getaffinity(0)
    ):
        parser.error("owner, broker and frontend roles require distinct allowed CPUs")
    config = load_consumer_uds_server_config_v2(args.config)
    helper = OwnedCandidateTracker(cgroup_root=args.cgroup_root)
    owner = SpawnedOmsOwnerCandidate(
        config=config,
        private_socket=args.private_socket,
        contract_root=args.contract_root.resolve(),
        cgroup_root=args.cgroup_root,
        initialize=(
            initialize_new_qualification_runtime(config.journal_root / "persistent")
            if args.initialize_if_new
            else False
        ),
        role_cpus=roles,
        background_cpus=background or None,
        gil_switch_us=args.gil_switch_us,
        projection_read_cpus=reads,
        projection_transaction_batches=args.projection_transaction_batches,
        optimize_projection_transactions=True,
        reuse_verified_admission_seeds=True,
        scoped_cold_lookup_epochs=True,
        submission_ipc=True,
        read_ipc=True,
        isolated_read_process=True,
        # Yield a complete lock hand-off window only while replay/control READ
        # demand is published by the isolated read plane.  The projector keeps
        # running without any pause when no such reader is waiting or active.
        projection_read_service_pause_ns=5_000_000,
    )
    runtime = MultiFrontendOwnerRuntimeV1(
        config=config,
        owner=owner,
        contract_root=args.contract_root.resolve(),
        frontend_cpus=frontends,
        broker_cpu=args.broker_cpu,
        read_broker_cpu=args.read_broker_cpu,
        acceptor_cpu=args.acceptor_cpu,
        direct_frontend_reads=True,
    )
    stopping = Event()

    def stop(_number: int, _frame: object) -> None:
        stopping.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    runtime_clean = helper_clean = False
    try:
        runtime.start()
        _wait_for_stop_or_failure(runtime, stopping)
        runtime.close()
        runtime_clean = True
        external_components = runtime.external_lifecycle_components()
        proof = helper.close()
        helper_clean = True
        publish_multi_frontend_lifecycle(
            journal_root=config.journal_root,
            ready=runtime.ready,
            stopped=runtime.stopped,
            external_components=external_components,
            helper_exit=proof,
        )
        return 0
    finally:
        if not runtime_clean:
            runtime.abort()
        if not helper_clean:
            helper.close()


if __name__ == "__main__":
    raise SystemExit(main())
