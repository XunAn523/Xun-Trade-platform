import multiprocessing as mp
import os
import socket
from pathlib import Path

import pytest
from xuanling_tradecore.consumer.frontend_fd_handoff import (
    create_frontend_fd_handoff_pair,
)
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_read_reactor import OWNER_READ_SLOT_COUNT
from xuanling_tradecore.consumer.spawned_frontend_worker import SpawnedFrontendWorkerV1

from test_long_running_consumer_candidate import config
from test_owner_ipc_v1 import ready
from test_process_oms_owner import owner_directory


@pytest.mark.skipif(not hasattr(socket, "SCM_RIGHTS"), reason="Linux FD handoff required")
@pytest.mark.parametrize("diagnostic_trace", [False, True])
def test_spawned_frontend_starts_on_fixed_cpu_and_drains_without_work(monkeypatch, diagnostic_trace):
    if diagnostic_trace:
        monkeypatch.setenv("TRADECORE_DIAGNOSTIC_FRONTEND_TRACE", "1")
    else:
        monkeypatch.delenv("TRADECORE_DIAGNOSTIC_FRONTEND_TRACE", raising=False)
    context = mp.get_context("spawn")
    cpu = min(os.sched_getaffinity(0))
    with owner_directory("tc-e5b-frontend-worker-") as root:
        worker_config = config(Path(root))
        (worker_config.journal_root / "traces").mkdir(mode=0o700)
        handoff_parent, handoff_child = create_frontend_fd_handoff_pair()
        write_parent, write_child = socket.socketpair()
        read_pairs = [socket.socketpair() for _ in range(4)]
        shared_total = FrontendSharedCreditV1(context, capacity=128, worker_count=1)
        shared_reads = FrontendSharedCreditV1(
            context, capacity=OWNER_READ_SLOT_COUNT, worker_count=1
        )
        worker = SpawnedFrontendWorkerV1(
            context,
            config=worker_config,
            contract_root=Path("contracts/consumer/v2").resolve(),
            ready=ready(),
            handoff_channel=handoff_child,
            write_channel=write_child,
            read_channels=tuple(pair[1] for pair in read_pairs),
            shared_credit=shared_total,
            shared_read_slots=shared_reads,
            worker_id=0,
            cpu=cpu,
        )
        try:
            pid = worker.start()
            assert pid != os.getpid()
            assert worker.affinity == (cpu,) and worker.alive
            worker.close()
            assert not worker.alive
            trace_status = list((worker_config.journal_root / "traces").glob("*.status.json"))
            assert bool(trace_status) is diagnostic_trace
            shared_total.require_all_drained()
            shared_reads.require_all_drained()
        finally:
            if worker.alive:
                worker.abort()
            handoff_parent.close()
            write_parent.close()
            for parent, _child in read_pairs:
                parent.close()
