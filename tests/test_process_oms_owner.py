"""E3b real ownership qualification, not a public frontend/performance certificate."""

from __future__ import annotations

import json
import os
import signal as signals
import socket
import tempfile
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

import msgspec
import pytest
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadyV1,
    OwnerStoppedV1,
    OwnerStopV1,
    decode_owner_packet,
    encode_owner_packet,
    send_owner_packet,
)
from xuanling_tradecore.consumer.owner_read_reactor import OWNER_READ_SLOT_COUNT
from xuanling_tradecore.consumer.process_oms_owner import (
    SpawnedOmsOwnerCandidate,
    _decode_boot,
    _start_write_actor,
)
from xuanling_tradecore.consumer.uds_server_v2 import require_safe_consumer_runtime_root
from xuanling_tradecore.persistence.runtime_files import ExclusiveRuntimeLease, atomic_runtime_json

from test_long_running_consumer_candidate import call, config, signal


def launcher(c, initialize=True, **kw):
    group = (
        Path("/sys/fs/cgroup")
        / Path("/proc/self/cgroup").read_text().strip().split("0::", 1)[1].lstrip("/")
        if os.name == "posix"
        else Path("C:/fixture-cgroup")
    )
    return SpawnedOmsOwnerCandidate(
        config=c,
        private_socket=c.journal_root / "o",
        contract_root=Path("contracts/consumer/v2").resolve(),
        cgroup_root=group,
        initialize=initialize,
        **kw,
    )


def test_write_actor_inherits_dedicated_role_not_background_mask():
    observed = []

    @contextmanager
    def affinity(cpus):
        observed.append(("enter", cpus))
        try:
            yield
        finally:
            observed.append(("exit", cpus))

    actor = Mock()
    actor.start.side_effect = lambda: observed.append(("start", None))
    with patch("xuanling_tradecore.consumer.process_oms_owner.temporary_thread_affinity", affinity):
        _start_write_actor(actor, (64, 65, 66, 77), (171, 172))
        _start_write_actor(actor, (64, 65, 66, 77), None)
    assert observed == [
        ("enter", (64,)), ("start", None), ("exit", (64,)),
        ("enter", None), ("start", None), ("exit", None),
    ]


def test_bootstrap_is_immutable_configuration_not_a_live_runtime(tmp_path):
    c = config(tmp_path)
    with patch(
        "xuanling_tradecore.consumer.long_running_candidate.ColdAwareOmsRuntime",
        side_effect=AssertionError("launcher must not create a runtime"),
    ):
        owner = launcher(c)
        assert owner.pid is None
        assert owner._boot.config == c
        assert not hasattr(owner, "runtime") and not hasattr(owner, "order_book")
        assert list(c.journal_root.iterdir()) == []
        owner.close()
        assert not owner.shutdown_verified  # an unstarted owner is no success certificate


def test_bootstrap_freezes_only_reviewed_process_owner_gil_interval(tmp_path):
    owner = launcher(config(tmp_path), gil_switch_us=1000)
    assert owner._boot.gil_switch_us == 1000
    raw = json.loads(owner._encoded)
    raw["gil_switch_us"] = 1500
    with pytest.raises(OwnerProtocolError):
        _decode_boot(json.dumps(raw).encode())


@pytest.mark.parametrize("field", ("outer", "config", "policy", "root", "oversize"))
def test_unknown_bootstrap_fields_and_nonobjects_are_rejected(tmp_path, field):
    owner = launcher(config(tmp_path))
    raw = json.loads(owner._encoded)
    if field == "outer":
        raw["business_callable"] = "must not accept arbitrary code"
    elif field == "config":
        raw["config"]["password"] = "no such configuration field"
    elif field == "policy":
        raw["config"]["long_running_policy"]["unbounded_capacity"] = 1
    elif field == "root":
        raw = []
    else:
        raw["private_socket"] = "x" * 17000
    with pytest.raises((OwnerProtocolError, msgspec.DecodeError)):
        _decode_boot(json.dumps(raw).encode())


@pytest.mark.parametrize("change", ("public_socket", "adapter", "benchmark", "relative"))
def test_owner_never_opens_public_or_adapter_profile(tmp_path, change):
    from xuanling_tradecore.consumer.runtime_profile import ConsumerRuntimeProfile

    c = config(tmp_path)
    if change == "public_socket":
        c = replace(c, socket_path=c.journal_root / "o")
    elif change == "adapter":
        c = replace(c, adapter_instance_id="live-EMT")
    elif change == "benchmark":
        c = replace(c, runtime_profile=ConsumerRuntimeProfile.BENCHMARK)
    else:
        c = replace(c, journal_root=Path("relative-runtime"))
    with pytest.raises(OwnerProtocolError):
        launcher(c)


def test_control_messages_are_typed_immutable_and_generation_bound():
    stop = OwnerStopV1("a" * 32, 1)
    stopped = OwnerStoppedV1("a" * 32, 1, 123, "journal", 5, bytes(32))
    for value in (stop, stopped):
        assert decode_owner_packet(encode_owner_packet(value)) == value
        with pytest.raises(AttributeError):
            value.generation = "b" * 32
    with pytest.raises(OwnerProtocolError):
        OwnerStopV1("a" * 32, True)


def test_non_linux_owner_start_rejects_before_any_data_or_process(tmp_path):
    c = config(tmp_path)
    owner = launcher(c)
    with patch("xuanling_tradecore.consumer.process_oms_owner.sys.platform", "win32"):
        with pytest.raises(OwnerProtocolError, match="Linux"):
            owner.start()
    assert owner.pid is None and list(c.journal_root.iterdir()) == []


def test_external_broker_transport_is_one_way_and_requires_bounded_shutdown_proof(tmp_path):
    owner = launcher(config(tmp_path), submission_ipc=True, read_ipc=True)
    business_parent, business_peer = socket.socketpair()
    read_pairs = tuple(socket.socketpair() for _ in range(OWNER_READ_SLOT_COUNT))
    owner._state = "RUNNING"
    owner._process = Mock(is_alive=Mock(return_value=True))
    owner._ready = OwnerReadyV1(
        "a" * 32,
        123,
        "b" * 64,
        "journal",
        0,
        bytes(32),
        "",
    )
    owner._business_channel = business_parent
    owner._read_channels = tuple(pair[0] for pair in read_pairs)
    try:
        business, ready = owner.transfer_reactor_transport()
        reads = owner.transfer_read_transport()
        assert business is business_parent and ready is owner._ready
        assert reads == tuple(pair[0] for pair in read_pairs)
        assert owner._business_channel is None and owner._read_channels == ()
        with pytest.raises(OwnerProtocolError, match="already transferred"):
            owner.transfer_reactor_transport()
        with pytest.raises(OwnerProtocolError, match="already transferred"):
            owner.transfer_read_transport()
        with pytest.raises(OwnerProtocolError, match="already transferred"):
            owner.open_reactor_port(object())
        with pytest.raises(OwnerProtocolError, match="already transferred"):
            owner.open_read_pool(object())
        owner.verify_external_reactor_shutdown()
        owner.verify_external_read_shutdown()
        assert owner._external_reactor_shutdown_verified
        assert owner._external_read_shutdown_verified
        with pytest.raises(OwnerProtocolError, match="not admissible"):
            owner.verify_external_reactor_shutdown()
        with pytest.raises(OwnerProtocolError, match="not admissible"):
            owner.verify_external_read_shutdown()
    finally:
        business_parent.close()
        business_peer.close()
        for parent, peer in read_pairs:
            parent.close()
            peer.close()


real_owner = pytest.mark.skipif(
    not hasattr(socket, "SO_PEERCRED") or os.getenv("TRADECORE_OMS_OWNER_PROCESS_TEST") != "1",
    reason="explicit Linux 6GiB KillMode=mixed owner qualification unit required",
)


def wait_for(test, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if test():
            return
        time.sleep(0.02)
    assert test(), "bounded owner predicate not satisfied"


def live_children(pid):
    result = set()
    for task in Path(f"/proc/{pid}/task").iterdir():
        result.update(int(p) for p in (task / "children").read_text().split())
    return result


def is_live(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


@contextmanager
def owner_directory(prefix):
    # Real process qualification must preserve the original source files,
    # including unknown/kill outcomes; never copy a live mapping as a proof or
    # let TemporaryDirectory delete the failure under investigation.
    evidence = os.getenv("TRADECORE_OMS_OWNER_EVIDENCE_ROOT")
    if not evidence:
        raise AssertionError("real owner qualification requires a private evidence root")
    evidence_root = Path(evidence)
    require_safe_consumer_runtime_root(evidence_root)
    directory = Path(tempfile.mkdtemp(prefix=prefix))
    atomic_runtime_json(
        evidence_root / (directory.name + ".json"),
        {
            "schema": "oms-owner-test-runtime-location-v1",
            "runtime_directory": str(directory),
            "preserve_original_source": True,
            "contains_private_identity_material": True,
            "performance_certificate": False,
        },
    )
    yield directory  # intentionally preserved on both success and failure


@real_owner
def test_spawn_owns_complete_book_lease_mock_read_models_and_explicit_recovery():
    with owner_directory("tc-e3b-") as directory:
        c = config(Path(directory))
        owner = launcher(c, scoped_cold_lookup_epochs=True)
        try:
            ready = owner.start()
            assert owner.pid != os.getpid()
            assert not c.socket_path.exists()  # no public frontend yet
            assert owner.private_socket.exists()
            assert len(live_children(owner.pid)) >= 3  # SQL, segment and projection owner
            with pytest.raises(OSError):
                ExclusiveRuntimeLease(c.journal_root / "persistent/identity/runtime-owner.lock")
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(owner.private_socket))
            assert call(client, "ListAccounts", {})["status"] == "COMPLETED"
            ids = []
            for n in range(10):
                result = call(client, "SubmitSignal", signal(n), n)
                assert result["status"] == "ACCEPTED", result
                ids.append(result["payload"]["oms_order_id"])

            def all_terminal():
                for n, order in enumerate(ids):
                    result = call(client, "GetOrder", {"oms_order_id": order}, n)
                    if result["status"] != "COMPLETED":
                        return False
                    expected = "REJECTED" if n % 5 == 4 else "FILLED"
                    if result["payload"]["order"]["payload"]["order_state"] != expected:
                        return False
                return True

            wait_for(all_terminal)
            # GetOrder may already resolve a hot terminal while the FULL
            # projection still legitimately exposes an older snapshot version.
            # Wait through a PUBLIC read fence, not a parent Book/worker handle
            # or a delay that guesses whether projection has caught up.
            replay_plans = []

            def projection_complete():
                result = call(
                    client,
                    "CreateReplayPartitions",
                    {"consumer_account_id": "test", "partition_count": 2},
                )
                if result["status"] == "COMPLETED" and result["payload"]["event_count"] == 20:
                    replay_plans.append(result)
                    return True
                return False

            wait_for(projection_complete)
            plan = call(
                client, "CreateOrderSnapshot", {"consumer_account_id": "test", "page_size": 100}
            )
            assert plan["status"] == "COMPLETED" and plan["payload"]["order_count"] == 10
            page = call(
                client, "ReadOrderSnapshotPage", {"page_cursor": plan["payload"]["page_cursor"]}
            )
            assert page["status"] == "COMPLETED" and len(page["payload"]["orders"]) == 10
            partitions = replay_plans[0]
            assert partitions["status"] == "COMPLETED"
            events = []
            for partition in partitions["payload"]["partitions"]:
                cursor = partition["cursor"]
                while True:
                    result = call(client, "PollEvents", {"cursor": cursor, "limit": 100})
                    assert result["status"] == "COMPLETED", result
                    events.extend(result["payload"]["events"])
                    cursor = result["payload"]["next_cursor"]
                    if not result["payload"]["has_more"]:
                        break
            assert len(events) == 20
            stopped = owner.close()
            assert stopped is not None and stopped.final_lsn == 40
            assert owner.shutdown_verified and not owner.failed
            assert not list((c.journal_root / "traces").glob("*.lifecycle.json"))
            assert not c.socket_path.exists()
            with pytest.raises(OwnerProtocolError):
                owner.start()  # explicit NEW launcher required, no automatic replacement
            reopened = launcher(c, initialize=False, scoped_cold_lookup_epochs=True)
            try:
                recovered = reopened.start()
                assert recovered.journal_id == ready.journal_id
                assert recovered.generation != ready.generation
                assert recovered.recovered_lsn == stopped.final_lsn
                assert recovered.source_prefix_sha256 != ready.source_prefix_sha256
                second = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(reopened.private_socket))
                duplicate = call(second, "SubmitSignal", signal(0), "duplicate-after-recovery")
                assert duplicate["status"] == "ACCEPTED", duplicate
                assert duplicate["payload"]["oms_order_id"] == ids[0]
                closed = reopened.close()
                assert closed is not None and closed.final_lsn == stopped.final_lsn
            finally:
                if not reopened.shutdown_verified:
                    reopened.abort()
        finally:
            if not owner.shutdown_verified:
                owner.abort()


@real_owner
@pytest.mark.parametrize("fault", ("kill", "control_eof", "stale_stop"))
def test_owner_loss_fences_private_access_never_restarts_or_publishes_clean_exit(fault):
    with owner_directory("tc-e3b-fault-") as directory:
        c = config(Path(directory))
        owner = launcher(c)
        try:
            owner.start()
            pid = owner.pid
            descendants = live_children(pid)
            assert descendants
            if fault == "kill":
                os.kill(pid, signals.SIGKILL)
            elif fault == "control_eof":
                owner._channel.close()
            else:
                send_owner_packet(
                    owner._channel,
                    OwnerStopV1("f" * 32, 1),
                    deadline_ns=time.monotonic_ns() + 1_000_000_000,
                )
            owner._process.join(20)
            assert owner._process.exitcode != 0 and owner.failed
            with pytest.raises(OwnerProtocolError):
                _ = owner.private_socket
            with pytest.raises(OwnerProtocolError):
                owner.start()
            with pytest.raises(OwnerProtocolError):
                owner.close()
            assert not owner.shutdown_verified and owner.pid == pid
            assert not list((c.journal_root / "traces").glob("*.lifecycle.json"))
            wait_for(lambda: not any(is_live(p) for p in descendants), seconds=15)
        finally:
            owner.abort()


@real_owner
def test_second_owner_cannot_acquire_the_live_writer_lease():
    with owner_directory("tc-e3b-lease-") as directory:
        c = config(Path(directory))
        first = launcher(c)
        second = launcher(c, initialize=False)
        try:
            first.start()
            with pytest.raises((OwnerProtocolError, OSError)):
                second.start(timeout=15)
            assert second.failed and not second.shutdown_verified
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(first.private_socket))
            assert call(client, "ListAccounts", {})["status"] == "COMPLETED"
            assert first.close() is not None and first.shutdown_verified
        finally:
            second.abort()
            if not first.shutdown_verified:
                first.abort()


@real_owner
def test_spawned_owner_all_original_role_affinities_and_launcher_affinity_preserved():
    original = os.sched_getaffinity(0)
    physical = {}
    for cpu in sorted(original):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = ((topology / "physical_package_id").read_text(), (topology / "core_id").read_text())
        physical.setdefault(key, cpu)
    assert len(physical) >= 8, "owner role qualification needs original eight physical cores"
    cpus = tuple(physical.values())[:8]
    with owner_directory("tc-e3b-cpu-") as directory:
        c = config(Path(directory))
        owner = launcher(
            c,
            role_cpus=cpus[:4],
            projection_read_cpus=cpus[4:6],
            background_cpus=cpus[6:],
        )
        try:
            owner.start()
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(owner.private_socket))
            assert call(client, "ListAccounts", {})["status"] == "COMPLETED"
            assert os.sched_getaffinity(0) == original
            assert os.sched_getaffinity(owner.pid) == {cpus[0]}
            children = live_children(owner.pid)
            assert len(children) == 5  # segment / cold SQL / projector / two read owners
            assert sorted(tuple(os.sched_getaffinity(pid)) for pid in children) == [
                (cpu,) for cpu in cpus[1:6]
            ]
            tasks = list(Path(f"/proc/{owner.pid}/task").iterdir())
            assert len(tasks) > 5
            for task in tasks:
                if int(task.name) != owner.pid:
                    assert os.sched_getaffinity(int(task.name)) == set(cpus[6:]), task
            assert owner.close() is not None and owner.shutdown_verified
            assert os.sched_getaffinity(0) == original
        finally:
            if not owner.shutdown_verified:
                owner.abort()
