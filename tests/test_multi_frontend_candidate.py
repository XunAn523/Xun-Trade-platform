import json
from hashlib import sha256
from threading import Event

import pytest
from xuanling_tradecore.consumer.multi_frontend_candidate import (
    _cpus,
    _frontend_cpus,
    _wait_for_stop_or_failure,
    publish_multi_frontend_lifecycle,
)
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadyV1,
    OwnerStoppedV1,
)
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json


def test_multi_frontend_candidate_requires_exact_unique_cpu_groups():
    assert _cpus("1,2,3", 3) == (1, 2, 3)
    assert _cpus("1,2,3,4", 4) == (1, 2, 3, 4)
    with pytest.raises(ValueError):
        _cpus("1,1", 2)
    with pytest.raises(ValueError):
        _cpus("1,-2", 2)


def test_multi_frontend_candidate_accepts_only_bounded_frontend_cpu_group():
    assert _frontend_cpus("1,2") == (1, 2)
    assert _frontend_cpus("1,2,3,4,5,6,7,8") == tuple(range(1, 9))
    for value in ("1", "1,1", "1,-2", "1,2,3,4,5,6,7,8,9"):
        with pytest.raises(ValueError):
            _frontend_cpus(value)


class _RuntimeState:
    def __init__(self, *, failed=False, failure_type=None, failure_reason=None):
        self.failed = failed
        self.failure_type = failure_type
        self.failure_reason = failure_reason


def test_multi_frontend_candidate_wait_returns_only_for_operator_stop():
    stopping = Event()
    stopping.set()
    _wait_for_stop_or_failure(_RuntimeState(), stopping, poll_seconds=0.001)


def test_multi_frontend_candidate_wait_surfaces_child_topology_failure():
    with pytest.raises(
        OwnerProtocolError,
        match="WorkerExited: frontend read output is not single-slot",
    ):
        _wait_for_stop_or_failure(
            _RuntimeState(
                failed=True,
                failure_type="WorkerExited",
                failure_reason="frontend read output is not single-slot",
            ),
            Event(),
            poll_seconds=0.001,
        )


def _component(role, pid, *, thread=False):
    return {
        "role": role,
        "kind": "THREAD" if thread else "PROCESS",
        "pid": pid,
        "exit_code": 0,
        "shutdown_verified": True,
    }


def test_multi_frontend_lifecycle_is_published_only_after_exact_role_closure(tmp_path):
    traces = tmp_path / "traces"
    traces.mkdir()
    trace_name = "consumer-" + "a" * 32 + "-" + "b" * 32 + ".jsonl"
    managed_path = (traces / trace_name).with_suffix(".managed-exit.json")
    atomic_runtime_json(
        managed_path,
        {
            "schema": "consumer-candidate-managed-exit-v1",
            "runtime_closed": True,
            "all_managed_workers_exited_zero": True,
            "owned_helper_exit_status": "NOT_VERIFIED",
            "formal_qualification": False,
            "producer_model": "NO_ADAPTER_ACCEPT_TERMINAL_TWO_FACTS",
            "journal_id": "journal-test",
            "final_lsn": 8,
            "trace_status_sha256": "c" * 64,
            "source_timing_manifest_sha256": "d" * 64,
            "owner_managed_processes": [_component("owner-read-plane", 50)],
        },
    )
    ready = OwnerReadyV1("e" * 32, 40, "f" * 64, "journal-test", 0, b"g" * 32, trace_name)
    stopped = OwnerStoppedV1(
        ready.generation,
        1,
        ready.owner_pid,
        ready.journal_id,
        8,
        sha256(managed_path.read_bytes()).digest(),
    )
    components = (
        _component("public-acceptor", 10, thread=True),
        _component("public-frontend-0", 20),
        _component("public-frontend-1", 21),
        _component("write-broker", 30),
        _component("read-broker", 31),
        _component("oms-owner", 40),
    )
    helper = {
        "schema": "candidate-owned-helper-exit-v1",
        "ownership_verified": True,
        "exit_code": 0,
        "remaining_descendants": 0,
        "forced_kill": False,
    }
    lifecycle = publish_multi_frontend_lifecycle(
        journal_root=tmp_path,
        ready=ready,
        stopped=stopped,
        external_components=components,
        helper_exit=helper,
    )
    body = json.loads(lifecycle.read_text())
    assert body["schema"] == "consumer-multi-frontend-lifecycle-v1"
    assert body["all_owned_descendants_exited_zero"] is True
    assert {item["role"] for item in body["lifecycle_components"]} == {
        "public-acceptor",
        "public-frontend-0",
        "public-frontend-1",
        "write-broker",
        "read-broker",
        "oms-owner",
        "owner-read-plane",
    }
    with pytest.raises(OwnerProtocolError, match="already exists"):
        publish_multi_frontend_lifecycle(
            journal_root=tmp_path,
            ready=ready,
            stopped=stopped,
            external_components=components,
            helper_exit=helper,
        )


def test_multi_frontend_lifecycle_accepts_direct_read_topology_without_read_broker(tmp_path):
    traces = tmp_path / "traces"
    traces.mkdir()
    trace_name = "consumer-" + "1" * 32 + "-" + "2" * 32 + ".jsonl"
    managed_path = (traces / trace_name).with_suffix(".managed-exit.json")
    atomic_runtime_json(
        managed_path,
        {
            "owner_managed_processes": [
                _component("owner-replay-plane", 50),
                _component("owner-snapshot-plane", 51),
            ],
            "runtime_closed": True,
        },
    )
    ready = OwnerReadyV1("3" * 32, 40, "4" * 64, "journal-direct", 0, b"5" * 32, trace_name)
    stopped = OwnerStoppedV1(
        ready.generation,
        1,
        ready.owner_pid,
        ready.journal_id,
        8,
        sha256(managed_path.read_bytes()).digest(),
    )
    components = (
        _component("public-acceptor", 10, thread=True),
        *(_component(f"public-frontend-{index}", 20 + index) for index in range(8)),
        _component("write-broker", 30),
        _component("oms-owner", 40),
    )
    lifecycle = publish_multi_frontend_lifecycle(
        journal_root=tmp_path,
        ready=ready,
        stopped=stopped,
        external_components=components,
        helper_exit={
            "schema": "candidate-owned-helper-exit-v1",
            "ownership_verified": True,
            "exit_code": 0,
            "remaining_descendants": 0,
            "forced_kill": False,
        },
    )
    roles = {
        item["role"] for item in json.loads(lifecycle.read_text())["lifecycle_components"]
    }
    assert "read-broker" not in roles
    assert roles == {
        "public-acceptor",
        "write-broker",
        "oms-owner",
        "owner-replay-plane",
        "owner-snapshot-plane",
        *(f"public-frontend-{index}" for index in range(8)),
    }


def test_multi_frontend_lifecycle_rejects_missing_read_plane_proof(tmp_path):
    traces = tmp_path / "traces"
    traces.mkdir()
    trace_name = "consumer-" + "a" * 32 + "-" + "b" * 32 + ".jsonl"
    managed_path = (traces / trace_name).with_suffix(".managed-exit.json")
    atomic_runtime_json(managed_path, {"owner_managed_processes": []})
    ready = OwnerReadyV1("e" * 32, 40, "f" * 64, "journal-test", 0, b"g" * 32, trace_name)
    stopped = OwnerStoppedV1(
        ready.generation,
        1,
        ready.owner_pid,
        ready.journal_id,
        8,
        sha256(managed_path.read_bytes()).digest(),
    )
    with pytest.raises(OwnerProtocolError, match="read-plane"):
        publish_multi_frontend_lifecycle(
            journal_root=tmp_path,
            ready=ready,
            stopped=stopped,
            external_components=(),
            helper_exit={},
        )
    assert not list(traces.glob("*multi-frontend-lifecycle.json"))
