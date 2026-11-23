"""Real public WRITE frontend qualification; full read relay is separate."""

import os
import select
import signal as signal_module
import socket
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Thread
from time import monotonic_ns
from typing import cast
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.client_v2 import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientErrorV2,
    ConsumerUnixClientV2,
)
from xuanling_tradecore.consumer.frontend_fd_handoff import (
    BoundedFrontendFdRouterV1,
    create_frontend_fd_handoff_pair,
)
from xuanling_tradecore.consumer.multi_frontend_runtime import MultiFrontendOwnerRuntimeV1
from xuanling_tradecore.consumer.owner_ipc_v1 import OwnerProtocolError
from xuanling_tradecore.consumer.process_owner_frontend import build_process_owner_frontend
from xuanling_tradecore.consumer.unix_v2 import read_linux_peer_credentials
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

from test_long_running_consumer_candidate import call, config, signal
from test_process_oms_owner import launcher, owner_directory, real_owner, wait_for


def _children(pid):
    result = []
    for task in Path(f"/proc/{pid}/task").iterdir():
        result.extend(int(value) for value in (task / "children").read_text().split())
    return set(result)


def _kill_at_source_boundary(channel, encoded, business, read_channels, *, before_commit):
    """Test-only exact segmented-Journal append/commit-word boundaries."""
    from xuanling_tradecore.consumer import process_oms_owner as process_module
    from xuanling_tradecore.oms import segmented_fact_journal as journal_module

    def interrupted(segment, prepared, batch):
        if not before_commit:
            os._exit(71)
        # Materialize every frame body exactly as production does, then die
        # before the first final eight-byte commit word. Recovery must detect
        # the torn tail and fail closed; it must never expose a partial batch.
        for ordinal, (_position, body) in enumerate(prepared):
            offset = (
                journal_module.HEADER_BYTES + (segment.count + ordinal) * journal_module.FRAME_BYTES
            )
            segment._map[offset : offset + len(body)] = body
        os._exit(72)

    with patch.object(journal_module._Segment, "append_prepared", interrupted):
        process_module._run_owner(channel, encoded, business, read_channels)


def _kill_before_source_append_owner(channel, encoded, business, read_channels):
    _kill_at_source_boundary(channel, encoded, business, read_channels, before_commit=False)


def _kill_before_source_commit_word_owner(channel, encoded, business, read_channels):
    _kill_at_source_boundary(channel, encoded, business, read_channels, before_commit=True)


def frontend(c, owner):
    """No Book/Journal/cursor key/read model exists in this parent."""
    service = build_process_owner_frontend(
        config=c, owner=owner, contract_root=Path("contracts/consumer/v2").resolve()
    )
    errors = []

    def run():
        try:
            service.serve_forever()
        except BaseException as error:
            errors.append(error)

    runner = Thread(target=run, name="test-public-owner-reactor")
    service.start()
    runner.start()
    return service, runner, errors


@real_owner
def test_two_spawned_frontends_share_one_real_owner_and_public_socket():
    with owner_directory("tc-e5b-multi-frontend-") as root:
        c = config(Path(root))
        owner = launcher(
            c,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=True,
            role_cpus=(64, 65, 66),
            projection_read_cpus=(67, 68, 74, 75),
        )
        runtime = MultiFrontendOwnerRuntimeV1(
            config=c,
            owner=owner,
            contract_root=Path("contracts/consumer/v2").resolve(),
            frontend_cpus=(70, 71),
            broker_cpu=69,
            read_broker_cpu=72,
            acceptor_cpu=73,
        )
        try:
            runtime.start()
            assert len(set(runtime.worker_pids)) == 2
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            for index in range(8):
                assert call(client, "SubmitSignal", signal(800 + index))["status"] == "ACCEPTED"
                assert call(client, "ListAccounts", {})["status"] == "COMPLETED"

            # Cross-frontend shared-credit hints are intentionally lock-free.
            # A lost READ/total-credit race is ordinary backpressure before
            # owner STARTED, not a reason to reset a client connection.
            release = Event()

            def concurrent_call(index):
                release.wait(5)
                separate = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
                if index % 2:
                    return call(
                        separate, "SubmitSignal", signal(900 + index), index
                    )["status"]
                return call(separate, "ListAccounts", {}, index)["status"]

            with ThreadPoolExecutor(max_workers=32) as pool:
                futures = [pool.submit(concurrent_call, index) for index in range(32)]
                release.set()
                statuses = []
                failures = []
                for index, future in enumerate(futures):
                    try:
                        statuses.append(future.result(timeout=10))
                    except BaseException as error:
                        failures.append((index, type(error).__name__, str(error)))
            assert not runtime.failed, (
                runtime.failure_type,
                runtime.failure_reason,
                failures,
            )
            assert not failures, failures
            assert statuses.count("ACCEPTED") == 16
            assert statuses.count("COMPLETED") == 16
            runtime.close()
            assert runtime.shutdown_verified and owner.shutdown_verified
        finally:
            if not runtime.shutdown_verified:
                runtime.abort()


@real_owner
@pytest.mark.parametrize("victim", ("frontend", "write_broker", "read_broker", "owner"))
def test_multi_frontend_process_loss_fences_generation_without_restart(victim):
    with owner_directory(f"tc-e5b-multi-{victim}-kill-") as root:
        c = config(Path(root))
        owner = launcher(
            c,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=True,
            role_cpus=(64, 65, 66),
            projection_read_cpus=(67, 68, 74, 75),
        )
        runtime = MultiFrontendOwnerRuntimeV1(
            config=c,
            owner=owner,
            contract_root=Path("contracts/consumer/v2").resolve(),
            frontend_cpus=(70, 71),
            broker_cpu=69,
            read_broker_cpu=72,
            acceptor_cpu=73,
        )
        try:
            generation = runtime.start().generation
            pids = runtime.worker_pids
            broker_pids = runtime.broker_pids
            assert len(pids) == 2 and len(broker_pids) == 2 and owner.pid is not None
            victims = {
                "frontend": pids[0],
                "write_broker": broker_pids[0],
                "read_broker": broker_pids[1],
                "owner": owner.pid,
            }
            os.kill(victims[victim], signal_module.SIGKILL)
            assert runtime.wait_failed()
            assert runtime.failure_type == "OwnerProtocolError"
            assert not runtime.shutdown_verified
            assert runtime.worker_pids == pids  # no replacement PID/generation
            assert runtime.broker_pids == broker_pids
            assert runtime.generation == generation
            with pytest.raises((ConsumerUnixClientErrorV2, OSError)):
                call(
                    ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path)),
                    "SubmitSignal",
                    signal(999),
                )
        finally:
            runtime.abort()
        assert not runtime.shutdown_verified


@real_owner
def test_real_public_request_crosses_single_acceptor_fd_handoff():
    """One-worker integration proves the FD path before multi-worker routing."""

    with owner_directory("tc-e4-fd-handoff-") as root:
        c = config(Path(root))
        owner = launcher(
            c,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=True,
            role_cpus=(64, 65, 66, 67),
            projection_read_cpus=(68, 69, 71, 72),
            background_cpus=(70,),
        )
        service = runner = acceptor_thread = None
        acceptor_socket = None
        stop_acceptor = Event()
        acceptor_channel, worker_channel = create_frontend_fd_handoff_pair()
        acceptor_errors = []
        try:
            ready = owner.start()
            service = build_process_owner_frontend(
                config=c,
                owner=owner,
                contract_root=Path("contracts/consumer/v2").resolve(),
                handoff_channel=worker_channel,
                handoff_generation=ready.generation,
            )
            errors = []

            def run_service():
                try:
                    service.serve_forever()
                except BaseException as error:
                    errors.append((error, traceback.format_exc()))

            c.socket_path.parent.mkdir(parents=True, exist_ok=True)
            acceptor_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            acceptor_socket.bind(str(c.socket_path))
            c.socket_path.chmod(c.socket_mode)
            acceptor_socket.listen(c.listen_backlog)
            acceptor_socket.settimeout(0.05)
            router = BoundedFrontendFdRouterV1((acceptor_channel,), generation=ready.generation)

            def accept_connections():
                try:
                    while not stop_acceptor.is_set():
                        try:
                            connection, _address = acceptor_socket.accept()
                        except socket.timeout:
                            continue
                        peer = read_linux_peer_credentials(connection)
                        assert (peer.uid, peer.gid) in {
                            (c.peer_uid, c.peer_gid),
                            *c.additional_peer_credentials,
                        }
                        sent = router.offer(
                            connection,
                            peer=peer,
                            accepted_monotonic_ns=monotonic_ns(),
                        )
                        assert sent is not None
                        assert select.select([acceptor_channel], [], [], 5)[0]
                        assert router.acknowledge(acceptor_channel).serial == sent.serial
                except BaseException as error:
                    acceptor_errors.append(error)

            service.start()
            runner = Thread(target=run_service, name="test-handoff-frontend")
            acceptor_thread = Thread(target=accept_connections, name="test-single-acceptor")
            runner.start()
            acceptor_thread.start()
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            try:
                submitted = call(client, "SubmitSignal", signal(700))
            except BaseException as error:
                wait_for(lambda: bool(errors or acceptor_errors), seconds=2)
                raise AssertionError(
                    "FD handoff request failed; "
                    f"frontend_errors={errors!r}; acceptor_errors={acceptor_errors!r}; "
                    f"router={router.stats()!r}"
                ) from error
            assert submitted["status"] == "ACCEPTED"
            try:
                account_view = call(client, "ListAccounts", {})
            except BaseException as error:
                wait_for(lambda: bool(errors or acceptor_errors), seconds=2)
                raise AssertionError(
                    "FD handoff read failed; "
                    f"frontend_errors={errors!r}; acceptor_errors={acceptor_errors!r}; "
                    f"router={router.stats()!r}"
                ) from error
            assert account_view["status"] == "COMPLETED"
            service.request_stop()
            runner.join(15)
            stop_acceptor.set()
            acceptor_thread.join(5)
            assert not runner.is_alive() and not acceptor_thread.is_alive()
            assert not errors and not acceptor_errors and service.submission_shutdown_verified
            stopped = owner.close()
            assert stopped is not None and stopped.final_lsn == 4
        finally:
            stop_acceptor.set()
            if acceptor_socket is not None:
                acceptor_socket.close()
            if acceptor_thread is not None:
                acceptor_thread.join(5)
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            if c.socket_path.exists():
                c.socket_path.unlink()
            acceptor_channel.close()
            worker_channel.close()
            if not owner.shutdown_verified:
                owner.abort()


@real_owner
def test_isolated_read_process_keeps_public_snapshot_and_replay_contracts():
    with owner_directory("tc-e3c-read-plane-") as root:
        c = config(Path(root))
        owner = launcher(
            c,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=True,
            role_cpus=(64, 65, 66, 67),
            projection_read_cpus=(68, 69, 71, 72),
            background_cpus=(70,),
            scoped_cold_lookup_epochs=True,
        )
        service = runner = None
        try:
            owner.start()
            service, runner, errors = frontend(c, owner)
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            order_ids = []
            for index in range(10):
                submitted = call(client, "SubmitSignal", signal(index))
                assert submitted["status"] == "ACCEPTED"
                order_ids.append(submitted["payload"]["oms_order_id"])

            def projected():
                result = call(
                    client,
                    "CreateReplayPartitions",
                    {"consumer_account_id": c.consumer_account_id, "partition_count": 2},
                )
                payload = result.get("payload")
                return (
                    result.get("status") == "COMPLETED"
                    and isinstance(payload, dict)
                    and payload.get("event_count") == 20
                )

            wait_for(projected)
            replay = call(
                client,
                "CreateReplayPartitions",
                {"consumer_account_id": c.consumer_account_id, "partition_count": 2},
            )
            assert replay["status"] == "COMPLETED"
            replayed = 0
            for partition in replay["payload"]["partitions"]:
                cursor = partition["cursor"]
                while True:
                    event_page = call(client, "PollEvents", {"cursor": cursor, "limit": 100})
                    assert event_page["status"] == "COMPLETED", event_page
                    replayed += len(event_page["payload"]["events"])
                    cursor = event_page["payload"]["next_cursor"]
                    if not event_page["payload"]["has_more"]:
                        break
            assert replayed == 20
            plan = call(
                client,
                "CreateOrderSnapshot",
                {"consumer_account_id": c.consumer_account_id, "page_size": 100},
            )
            assert plan["status"] == "COMPLETED" and plan["payload"]["order_count"] == 10
            page = call(
                client,
                "ReadOrderSnapshotPage",
                {"page_cursor": plan["payload"]["page_cursor"]},
            )
            assert page["status"] == "COMPLETED" and len(page["payload"]["orders"]) == 10
            audit = call(client, "GetOrderAuditTrail", {"oms_order_id": order_ids[0]})
            assert audit["status"] == "COMPLETED"
            assert len(audit["payload"]["audit_trail"]) == 2
            direct = _children(owner.pid)
            coordinator = next(pid for pid in direct if os.sched_getaffinity(pid) == {68})
            assert any(os.sched_getaffinity(pid) == {69} for pid in _children(coordinator))
            service.request_stop()
            runner.join(10)
            assert not runner.is_alive() and not errors and service.submission_shutdown_verified
            stopped = owner.close()
            assert stopped is not None and stopped.final_lsn == 40
            assert owner.shutdown_verified
        finally:
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            if not owner.shutdown_verified:
                owner.abort()


@real_owner
def test_isolated_read_process_loss_fences_public_service_and_is_not_restarted():
    with owner_directory("tc-e3c-read-plane-kill-") as root:
        c = config(Path(root))
        owner = launcher(
            c,
            submission_ipc=True,
            read_ipc=True,
            isolated_read_process=True,
            role_cpus=(64, 65, 66, 67),
            projection_read_cpus=(68, 69, 71, 72),
            background_cpus=(70,),
        )
        service = runner = None
        try:
            owner.start()
            service, runner, errors = frontend(c, owner)
            coordinator = next(
                pid for pid in _children(owner.pid) if os.sched_getaffinity(pid) == {68}
            )
            os.kill(coordinator, 9)
            wait_for(lambda: owner.failed, seconds=15)
            runner.join(15)
            assert not runner.is_alive() and errors
            assert not service.submission_shutdown_verified
            # The owner is expected to terminate after fencing, so its /proc tree may
            # already be gone here.  Verify the killed coordinator did not survive;
            # owner.failed above proves no replacement can remain under this owner.
            assert not Path(f"/proc/{coordinator}").exists()
        finally:
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            owner.abort()


@real_owner
def test_original_public_peer_async_write_and_manual_recovery_have_no_parent_book():
    with owner_directory("tc-e3c2-") as root:
        c = config(Path(root))
        owner = launcher(c, submission_ipc=True, read_ipc=True, scoped_cold_lookup_epochs=True)
        service = runner = None
        try:
            boot = owner.start()
            service, runner, errors = frontend(c, owner)
            assert service._listener._gateway._operations is None
            assert not hasattr(service, "runtime") and not hasattr(service, "identity")
            assert owner.pid != os.getpid() and not hasattr(owner, "runtime")
            ids = []
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            for n in range(10):
                response = call(client, "SubmitSignal", signal(n))
                assert response["status"] == "ACCEPTED"
                payload = cast(dict[str, object], response["payload"])
                assert payload["journal_id"] == boot.journal_id
                ids.append(payload["oms_order_id"])
            assert call(client, "ListAccounts", {})["status"] == "COMPLETED"
            assert service is not None
            public_service = service
            wait_for(lambda: public_service.ingress_stats().submit_execution_pending == 0)

            def projected():
                result = call(
                    client,
                    "CreateReplayPartitions",
                    {
                        "consumer_account_id": c.consumer_account_id,
                        "partition_count": 1,
                    },
                )
                payload = result.get("payload")
                return (
                    result.get("status") == "COMPLETED"
                    and isinstance(payload, dict)
                    and payload.get("event_count") == 20
                )

            wait_for(projected)
            service.request_stop()
            runner.join(5)
            assert not runner.is_alive() and not errors
            assert service.submission_shutdown_verified
            stopped = owner.close()
            assert stopped is not None
            assert stopped.final_lsn == 40 and owner.shutdown_verified
            # Inspect original IPC trace for exact kernel identity correlation,
            # no fixture-injected UID or private-listener principal override.
            assert len(set(ids)) == 10
        finally:
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            if not owner.shutdown_verified:
                owner.abort()
        recovered = launcher(
            c, initialize=False, submission_ipc=True, read_ipc=True, scoped_cold_lookup_epochs=True
        )
        try:
            assert recovered.start().recovered_lsn == 40
            service, runner, errors = frontend(c, recovered)
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            payload = cast(dict[str, object], call(client, "SubmitSignal", signal(0))["payload"])
            assert payload["oms_order_id"] == ids[0]
            service.request_stop()
            runner.join(5)
            assert not runner.is_alive() and not errors
            stopped = recovered.close()
            assert stopped is not None and stopped.final_lsn == 40
        finally:
            service.request_stop()
            runner.join(5)
            if not recovered.shutdown_verified:
                recovered.abort()


@real_owner
def test_public_disconnect_and_owner_kill_never_clean_drain_or_automatic_retry():
    with owner_directory("tc-e3c2-") as root:
        c = config(Path(root))
        owner = launcher(c, submission_ipc=True, read_ipc=True, scoped_cold_lookup_epochs=True)
        service = runner = None
        try:
            owner.start()
            service, runner, errors = frontend(c, owner)
            wait_for(lambda: service._owner_port is not None)
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peer.settimeout(3)
            peer.connect(str(c.socket_path))
            peer.sendall(
                encode_frame(
                    MessageKind.REQUEST,
                    3,
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "operation": "SubmitSignal",
                        "request_id": "kill-original",
                        "payload": signal(0),
                    },
                )
            )
            peer.close()
            # Kill the exact owned PID; never a broker or unrelated process.
            owner._process.kill()
            owner._process.join(5)
            runner.join(5)
            assert not runner.is_alive() and errors
            assert not service.submission_shutdown_verified
            assert owner.failed
        finally:
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            owner.abort()


@real_owner
@pytest.mark.parametrize(
    ("target", "exit_code", "recoverable"),
    (
        (_kill_before_source_append_owner, 71, True),
        (_kill_before_source_commit_word_owner, 72, False),
    ),
)
def test_public_source_commit_boundaries_never_fabricate_receipt_or_retry(
    target, exit_code, recoverable
):
    with owner_directory("tc-e3c2-source-fault-") as root:
        c = config(Path(root))
        owner = launcher(c, submission_ipc=True, read_ipc=True)
        service = runner = None
        try:
            with patch("xuanling_tradecore.consumer.process_oms_owner._run_owner", target):
                owner.start()
            service, runner, errors = frontend(c, owner)
            client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
            with pytest.raises((ConsumerUnixClientErrorV2, TimeoutError)):
                call(client, "SubmitSignal", signal(0))
            owner._process.join(15)
            runner.join(15)
            assert owner._process.exitcode == exit_code and owner.failed
            assert not service.submission_shutdown_verified and errors
        finally:
            if service is not None:
                service.request_stop()
            if runner is not None:
                runner.join(5)
            owner.abort()

        recovered = launcher(c, initialize=False, submission_ipc=True, read_ipc=True)
        try:
            if recoverable:
                assert recovered.start().recovered_lsn == 0
                stopped = recovered.close()
                assert stopped is not None and stopped.final_lsn == 0
            else:
                with pytest.raises(OwnerProtocolError):
                    recovered.start()
                assert recovered.failed and not recovered.shutdown_verified
        finally:
            if not recovered.shutdown_verified:
                recovered.abort()
