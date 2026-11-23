"""Standalone public UDS + unique process OMS owner qualification runtime.

No Adapter or broker is available. The parent owns public sockets and fixed
IPC only; the spawned child owns every mutable OMS component and read key.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import uuid
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns
from typing import cast

from ..persistence.runtime_files import atomic_runtime_json
from .candidate_affinity import set_thread_affinity_exact
from .candidate_helper import OwnedCandidateTracker
from .candidate_systemd import require_coordinated_candidate_unit
from .long_running_candidate import configure_candidate_gil_switch, process_owner_trace_budgets
from .process_oms_owner import SpawnedOmsOwnerCandidate
from .process_owner_frontend import build_process_owner_frontend
from .qualification_bootstrap import initialize_new_qualification_runtime
from .request_trace_v2 import BoundedRequestTraceWriterV2
from .uds_server_v2 import load_consumer_uds_server_config_v2


def _cpus(text: str, *, count: int | None = None) -> tuple[int, ...]:
    values = text.split(",")
    if not values or not all(value.isascii() and value.isdecimal() for value in values):
        raise ValueError("CPU IDs must be comma-separated decimals")
    cpus = tuple(int(value) for value in values)
    if len(set(cpus)) != len(cpus) or (count is not None and len(cpus) != count):
        raise ValueError("CPU IDs are repeated or have the wrong role count")
    return cpus


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--contract-root", type=Path, required=True)
    parser.add_argument("--cgroup-root", type=Path, required=True)
    parser.add_argument("--private-socket", type=Path, required=True)
    parser.add_argument("--role-cpus", required=True, help="reactor,projector,cold,segment")
    parser.add_argument("--projection-read-cpus", required=True)
    parser.add_argument("--background-cpus", required=True)
    parser.add_argument("--frontend-cpu", type=int, required=True)
    parser.add_argument("--initialize-if-new", action="store_true")
    parser.add_argument("--projection-transaction-batches", type=int, default=256)
    parser.add_argument("--projection-commit-diagnostics", action="store_true")
    parser.add_argument(
        "--isolate-owner-reads",
        action="store_true",
        help="Run response preparation/signing in the fixed owner read process",
    )
    parser.add_argument(
        "--frontend-request-trace",
        action="store_true",
        help="Enable full public request trace for attribution, not performance qualification",
    )
    parser.add_argument(
        "--gil-switch-us",
        type=int,
        choices=(100, 250, 500, 1000, 2000, 5000),
        help="Fixed interpreter-local experiment for both frontend and spawned owner",
    )
    args = parser.parse_args()
    require_coordinated_candidate_unit(args.cgroup_root)
    configure_candidate_gil_switch(args.gil_switch_us)
    config = load_consumer_uds_server_config_v2(args.config)
    roles = _cpus(args.role_cpus, count=4)
    read_cpus = _cpus(
        args.projection_read_cpus, count=4 if args.isolate_owner_reads else 2
    )
    background = _cpus(args.background_cpus)
    owner_cpus = set((*roles, *read_cpus, *background))
    initial = os.sched_getaffinity(0)
    if (
        args.frontend_cpu < 0
        or args.frontend_cpu in owner_cpus
        or not owner_cpus | {args.frontend_cpu} <= initial
        or len(owner_cpus | {args.frontend_cpu})
        != (10 if args.isolate_owner_reads else 8)
    ):
        parser.error(
            "frontend and owner roles must use the exact fixed physical CPU count"
        )
    helper = OwnedCandidateTracker(cgroup_root=args.cgroup_root)
    helper.set_affinity_exact(background)
    owner: SpawnedOmsOwnerCandidate | None = None
    service = None
    frontend_trace: BoundedRequestTraceWriterV2 | None = None
    frontend_trace_clean = not args.frontend_request_trace
    owner_clean = False
    helper_clean = False
    stopped = None
    try:
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
            projection_read_cpus=read_cpus,
            background_cpus=background,
            projection_transaction_batches=args.projection_transaction_batches,
            optimize_projection_transactions=True,
            reuse_verified_admission_seeds=True,
            scoped_cold_lookup_epochs=True,
            projection_commit_diagnostics=args.projection_commit_diagnostics,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=args.isolate_owner_reads,
            gil_switch_us=args.gil_switch_us,
        )
        ready = owner.start()
        # Spawn first so the complete owner receives the original 8-CPU mask.
        set_thread_affinity_exact({args.frontend_cpu})
        service = build_process_owner_frontend(
            config=config, owner=owner, contract_root=args.contract_root.resolve()
        )
        if args.frontend_request_trace:
            policy = config.long_running_policy
            if policy is None:
                raise RuntimeError("process owner frontend requires LONG_RUNNING trace policy")
            *_, frontend_trace_budget = process_owner_trace_budgets(
                config,
                projection_commit_diagnostics=args.projection_commit_diagnostics,
            )
            frontend_trace = BoundedRequestTraceWriterV2(
                config.journal_root
                / "traces"
                / ("frontend-" + ready.journal_id + "-" + uuid.uuid4().hex + ".jsonl"),
                journal_id=ready.journal_id,
                rotation_bytes=policy.trace_segment_bytes,
                max_bytes=frontend_trace_budget,
            )
            service.set_request_trace(frontend_trace)

        def stop(_number: int, _frame: object) -> None:
            assert service is not None
            service.request_stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, stop)
        service.start()
        service.serve_forever()
        if not service.submission_shutdown_verified:
            raise RuntimeError("public reactor owner channels did not drain")
        if frontend_trace is not None:
            frontend_trace.close()
            frontend_trace_clean = frontend_trace.coverage_verified
            if not frontend_trace_clean:
                raise RuntimeError("public frontend request trace did not close completely")
        stopped = owner.close()
        owner_clean = owner.shutdown_verified and stopped is not None
        if not owner_clean:
            raise RuntimeError("unique owner managed exit was not verified")
        assert stopped is not None
        managed_path = (config.journal_root / "traces" / ready.trace_name).with_suffix(
            ".managed-exit.json"
        )
        managed_bytes = managed_path.read_bytes()
        if (
            len(managed_bytes) > 16_384
            or sha256(managed_bytes).digest() != stopped.managed_exit_sha256
        ):
            raise RuntimeError("owner managed lifecycle changed after verification")
        managed: object = json.loads(managed_bytes)
        if not isinstance(managed, dict):
            raise RuntimeError("owner managed lifecycle is not an object")
        managed_payload = cast(dict[str, object], managed)
        proof = helper.close()
        helper_clean = True
        lifecycle = config.journal_root / "traces" / (ready.journal_id + "-frontend-lifecycle.json")
        atomic_runtime_json(lifecycle, {
            **managed_payload,
            "schema": "consumer-process-owner-lifecycle-v1",
            "public_frontend_pid": os.getpid(), "owner_pid": ready.owner_pid,
            "generation_sha256": sha256(ready.generation.encode("ascii")).hexdigest(),
            "journal_id": ready.journal_id, "recovered_lsn": ready.recovered_lsn,
            "final_lsn": stopped.final_lsn, "managed_exit_sha256": stopped.managed_exit_sha256.hex(),
            "public_reactor_drained": True, "owner_shutdown_verified": True,
            "all_owned_descendants_exited_zero": True,
            "owned_helper_exit_status": "VERIFIED", "helper_exit": proof,
            "frontend_cpu": args.frontend_cpu, "owner_role_cpus": roles,
            "projection_read_cpus": read_cpus, "background_cpus": background,
            "isolated_owner_read_process": args.isolate_owner_reads,
            "frontend_trace_enabled": args.frontend_request_trace,
            "frontend_trace_name": (
                frontend_trace.path.name if frontend_trace is not None else None
            ),
            "frontend_trace_coverage_verified": (
                frontend_trace_clean if frontend_trace is not None else None
            ),
            "gil_switch_interval_ns": round(sys.getswitchinterval() * 1_000_000_000),
            "observed_monotonic_ns": monotonic_ns(),
            "performance_certificate": False,
        })
        return 0
    finally:
        if service is not None:
            service.request_stop()
        if frontend_trace is not None and not frontend_trace_clean:
            frontend_trace.close()
        if not owner_clean and owner is not None:
            owner.abort()
        if not helper_clean:
            # Drain this CLI's proven helper after exact child containment, but
            # never publish a successful lifecycle certificate for this path.
            helper.close()


if __name__ == "__main__":
    raise SystemExit(main())
