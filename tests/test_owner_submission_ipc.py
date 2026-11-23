import os
import socket
import time
from concurrent.futures import Future
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import Event, Thread
from unittest.mock import Mock, patch

import msgspec
import pytest
from xuanling_tradecore.consumer.base import ConsumerAuthorizationError
from xuanling_tradecore.consumer.client_v2 import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientErrorV2,
    ConsumerUnixClientV2,
)
from xuanling_tradecore.consumer.cold_admission_v2 import ConsumerColdLookupRequiredV2
from xuanling_tradecore.consumer.fair_queue_v2 import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
)
from xuanling_tradecore.consumer.gateway_v2 import ConsumerGatewayV2
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerAuthorityV1,
    OwnerEventV1,
    OwnerIdentityV1,
    OwnerPeerV1,
    OwnerProtocolError,
    OwnerRequestV1,
    receive_owner_packet,
    send_owner_packet,
)
from xuanling_tradecore.consumer.owner_submission_ipc import (
    OwnerSubmissionPortV1,
    _cold_response,
    owner_request_fields,
)
from xuanling_tradecore.consumer.unix_v2 import (
    ConsumerUnixSocketError,
    ConsumerUnixSocketListenerV2,
)
from xuanling_tradecore.consumer.wire_v2 import MessageKind, decode_frame, encode_frame
from xuanling_tradecore.domain import ConsumerPrincipal
from xuanling_tradecore.oms.cold_intent import ColdIntentKey, ColdIntentResolution

from test_consumer_gateway_v2 import _contract, _snapshot
from test_long_running_consumer_candidate import call, config, signal
from test_owner_ipc_v1 import ready
from test_process_oms_owner import (
    is_live,
    launcher,
    live_children,
    owner_directory,
    real_owner,
    wait_for,
)


def queue():
    return BoundedFairOperationQueueV2(
        total_capacity=2,
        lane_capacities={lane: 2 for lane in ConsumerOperationLaneV2},
    )


def command(boot, c, serial=1, n=0, **changes):
    payload = {
        "message_type": "request",
        "schema_version": 2,
        "operation": "SubmitSignal",
        "request_id": f"ipc-{serial}",
        "payload": signal(n),
    }
    payload.update(changes.pop("payload_changes", {}))
    frame = encode_frame(MessageKind.REQUEST, serial, payload)
    key, strategy = owner_request_fields(decode_frame(frame))
    authority = OwnerAuthorityV1(
        c.consumer_id, c.consumer_account_id, strategy, boot.scope_sha256, 1
    )
    identity = OwnerIdentityV1(
        boot.generation,
        serial,
        authority,
        payload["request_id"],
        key,
        sha256(frame).digest(),
        OwnerPeerV1(os.getpid(), c.peer_uid, c.peer_gid),
    )
    values = dict(
        identity=identity,
        operation=payload["operation"],
        deadline_ns=time.monotonic_ns() + 5_000_000_000,
        request_frame=frame,
    )
    values.update(changes)
    return OwnerRequestV1(**values)


def listener(tmp_path, handler, *, allow=False, snapshot=None):
    current = [snapshot or _snapshot()]
    gateway = ConsumerGatewayV2(
        binding_snapshot=lambda: current[0],
        wire_contract=_contract(),
        account_view_for=Mock(),
    )
    endpoint = ConsumerUnixSocketListenerV2(
        socket_path=tmp_path / "s",
        gateway=gateway,
        principal_for_peer=Mock(),
        request_schema={"type": "object"},
        request_handler=handler,
        allow_socket_writes=allow,
    )
    frame = decode_frame(
        encode_frame(
            MessageKind.REQUEST,
            42,
            {
                "message_type": "request",
                "schema_version": 2,
                "operation": "SubmitSignal",
                "request_id": "r",
                "payload": {},
            },
        )
    )
    return endpoint, frame, current


def test_private_socket_write_disabled_but_verified_owner_lease_runs_once(tmp_path):
    handler = Mock(return_value={"message_type": "response", "request_id": "r", "payload": {}})
    endpoint, frame, _ = listener(tmp_path, handler)
    with pytest.raises(ConsumerUnixSocketError, match="disabled"):
        endpoint.response_for_frame(None, frame)
    with endpoint.authorize_owner_submission(
        ConsumerPrincipal("consumer-1", "login", "legal"),
        frame,
        authorization_epoch=3,
    ) as lease:
        result = decode_frame(lease.run())
        assert result.payload["request_id"] == "r"
        with pytest.raises(ConsumerUnixSocketError):
            lease.run()  # cannot accidentally replay even while authorization context remains open
    with pytest.raises(ConsumerUnixSocketError):
        lease.run()
    assert handler.call_count == 1


def test_current_epoch_rechecked_each_attempt_and_only_cold_precommit_permits_resume(tmp_path):
    required = ConsumerColdLookupRequiredV2(ColdIntentKey("a", "s", "r"), Mock())
    handler = Mock(side_effect=[required, {"request_id": "r"}])
    endpoint, frame, current = listener(tmp_path, handler)
    with endpoint.authorize_owner_submission(
        ConsumerPrincipal("consumer-1", "login", "legal"),
        frame,
        authorization_epoch=3,
    ) as lease:
        with pytest.raises(ConsumerColdLookupRequiredV2):
            lease.run()
        current[0] = replace(current[0], authorization_epoch=4)
        with pytest.raises(ConsumerAuthorizationError):
            lease.run()
        with pytest.raises(ConsumerUnixSocketError):
            lease.run()
    assert handler.call_count == 1  # stale session did not enter handler a second time


@pytest.mark.parametrize("failure", (OSError, KeyboardInterrupt))
def test_unknown_handler_exception_consumes_lease_no_implicit_retry(tmp_path, failure):
    handler = Mock(side_effect=failure("possibly committed"))
    endpoint, frame, _ = listener(tmp_path, handler)
    with endpoint.authorize_owner_submission(
        ConsumerPrincipal("consumer-1", "login", "legal"),
        frame,
        authorization_epoch=3,
    ) as lease:
        with pytest.raises(failure):
            lease.run()
        with pytest.raises(ConsumerUnixSocketError):
            lease.run()
    assert handler.call_count == 1


def test_wrong_initial_authorization_epoch_never_runs_handler(tmp_path):
    handler = Mock()
    endpoint, frame, _ = listener(tmp_path, handler)
    with pytest.raises(ConsumerUnixSocketError, match="epoch"):
        with endpoint.authorize_owner_submission(
            ConsumerPrincipal("consumer-1", "login", "legal"),
            frame,
            authorization_epoch=2,
        ):
            pytest.fail("must fail before a lease is available")
    handler.assert_not_called()


@pytest.mark.parametrize("fault", ("key", "oversize"))
def test_cold_resume_keeps_original_key_and_256k_checkpoint_bound(tmp_path, fault):
    work = command(ready(), config(tmp_path))
    key = ColdIntentKey("a", "s", "ref")
    future = Future()
    future.set_result(
        ColdIntentResolution(
            ColdIntentKey("other", "s", "ref") if fault == "key" else key,
            1,
            b"nonce",
            b"x" * (256 * 1024 + 1) if fault == "oversize" else None,
        )
    )
    resolver = Mock()
    resolver.resolve.return_value = future
    lease = Mock()
    lease.run.side_effect = ConsumerColdLookupRequiredV2(key, resolver)
    with pytest.raises(OwnerProtocolError):
        _cold_response(lease, decode_frame(work.request_frame), work, Event(), {})
    assert lease.run.call_count == 1


@pytest.mark.parametrize(
    "field,value", (("pid", 0), ("uid", -1), ("gid", True), ("pid", 2**31), ("uid", 2**32))
)
def test_original_peer_protocol_ranges(field, value):
    values = dict(pid=1, uid=1000, gid=1000)
    values[field] = value
    with pytest.raises(OwnerProtocolError):
        OwnerPeerV1(**values)


def test_port_correlates_reply_and_keeps_original_credit_until_delivery(tmp_path):
    c = config(tmp_path)
    boot = ready()
    work = command(boot, c)
    parent, child = socket.socketpair()
    credits = queue()
    port = OwnerSubmissionPortV1(parent, ready=boot, ready_queue=credits)

    def peer():
        try:
            received = receive_owner_packet(child, deadline_ns=time.monotonic_ns() + 3_000_000_000)
            assert received == work
            send_owner_packet(
                child,
                OwnerEventV1(work.identity, "STARTED", time.monotonic_ns()),
                deadline_ns=time.monotonic_ns() + 3_000_000_000,
            )
            response = encode_frame(
                MessageKind.RESPONSE,
                1,
                {
                    "request_id": work.identity.request_id,
                    "payload": {
                        "consumer_request_id": work.identity.request_id,
                        "journal_id": "journal",
                        "journal_lsn": 12,
                    },
                },
            )
            send_owner_packet(
                child,
                OwnerEventV1(
                    work.identity,
                    "COMPLETED",
                    time.monotonic_ns(),
                    response,
                    "journal",
                    12,
                    boot.source_prefix_sha256,
                    source_anchor="RECOVERED_PREFIX",
                ),
                deadline_ns=time.monotonic_ns() + 3_000_000_000,
            )
        finally:
            child.close()

    worker = Thread(target=peer)
    worker.start()
    try:
        result = port.submit(work, current_authority=work.identity.authority)
        assert result.phase == "COMPLETED" and credits.used == 1
        with pytest.raises(OwnerProtocolError, match="pending delivery"):
            port.submit(command(boot, c, serial=2), current_authority=work.identity.authority)
        with pytest.raises(OwnerProtocolError):
            port.acknowledge_delivery(
                1,
                current_authority=msgspec.structs.replace(
                    work.identity.authority, authorization_epoch=2
                ),
            )
        assert credits.used == 1
        port.acknowledge_delivery(1, current_authority=work.identity.authority)
        port.stop()
        assert credits.used == 0
    finally:
        parent.close()
        worker.join(3)
        assert not worker.is_alive()


@pytest.mark.parametrize(
    "fault",
    ("missing_start", "wrong_correlation", "future_receipt", "drop_after_start", "wrong_anchor"),
)
def test_untrusted_or_lost_reply_never_releases_credit_or_can_be_retried(tmp_path, fault):
    c, boot = config(tmp_path), ready()
    work = command(boot, c)
    parent, child = socket.socketpair()
    credits = queue()
    port = OwnerSubmissionPortV1(parent, ready=boot, ready_queue=credits)
    failures = []

    def peer():
        try:
            receive_owner_packet(child, deadline_ns=time.monotonic_ns() + 3_000_000_000)
            if fault != "missing_start":
                send_owner_packet(
                    child,
                    OwnerEventV1(work.identity, "STARTED", time.monotonic_ns()),
                    deadline_ns=time.monotonic_ns() + 3_000_000_000,
                )
            if fault == "drop_after_start":
                return
            response = encode_frame(
                MessageKind.RESPONSE,
                99 if fault == "wrong_correlation" else 1,
                {
                    "request_id": work.identity.request_id,
                    "payload": {
                        "consumer_request_id": work.identity.request_id,
                        "journal_id": "journal",
                        "journal_lsn": 99 if fault == "future_receipt" else 12,
                    },
                },
            )
            send_owner_packet(
                child,
                OwnerEventV1(
                    work.identity,
                    "COMPLETED",
                    time.monotonic_ns(),
                    response,
                    "journal",
                    12,
                    b"x" * 32 if fault == "wrong_anchor" else boot.source_prefix_sha256,
                    source_anchor="RECOVERED_PREFIX",
                ),
                deadline_ns=time.monotonic_ns() + 3_000_000_000,
            )
        except BaseException as error:
            failures.append(error)
        finally:
            child.close()

    worker = Thread(target=peer)
    worker.start()
    try:
        with pytest.raises(OwnerProtocolError):
            port.submit(work, current_authority=work.identity.authority)
        assert port.failed and credits.used == 1
        with pytest.raises(OwnerProtocolError):
            port.require_drained()
        with pytest.raises(OwnerProtocolError):
            port.submit(command(boot, c, serial=2), current_authority=work.identity.authority)
        port.dispose(1)
        assert credits.used == 0 and port.failed
    finally:
        parent.close()
        worker.join(3)
        assert not worker.is_alive() and not failures


def _kill_after_effect_owner(channel, encoded, business):
    """Test-only: terminal/FULL retirement observed, still no IPC result sent."""
    from xuanling_tradecore.consumer import owner_submission_ipc as module
    from xuanling_tradecore.consumer import process_oms_owner as process_module

    original = module._cold_response
    candidate_class = process_module.LongRunningConsumerCandidate
    created = []

    def capture_candidate(*args, **kwargs):
        candidate = candidate_class(*args, **kwargs)
        created.append(candidate)
        return candidate

    def commit_then_die(*args, **kwargs):
        original(*args, **kwargs)
        # Freeze an exact, reproducible fault boundary. If killed before the
        # terminal, slow recovery may correctly trip the existing 1s Mock
        # deadline; that is a DIFFERENT matrix case, not an idempotent retry
        # success to obtain by changing timestamps/deadlines.
        deadline = time.monotonic() + 3
        while created[0].mock.health().retired != 1:
            assert time.monotonic() < deadline and created[0].mock.health().state != "FAILED"
            time.sleep(0.001)
        os._exit(77)

    with (
        patch.object(module, "_cold_response", commit_then_die),
        patch.object(process_module, "LongRunningConsumerCandidate", capture_candidate),
    ):
        process_module._run_owner(channel, encoded, business)


@real_owner
@pytest.mark.parametrize("detailed", (False, True))
@pytest.mark.parametrize("nonblocking", (False, True))
def test_real_inherited_write_peer_epoch_receipt_cold_retry_and_single_private_writer(
    detailed, nonblocking, monkeypatch
):
    if detailed:
        monkeypatch.setenv("TRADECORE_DIAGNOSTIC_OWNER_DETAIL", "1")
    else:
        monkeypatch.delenv("TRADECORE_DIAGNOSTIC_OWNER_DETAIL", raising=False)
    if nonblocking:
        monkeypatch.setenv("TRADECORE_DIAGNOSTIC_OWNER_SEND_NONBLOCKING", "1")
    else:
        monkeypatch.delenv("TRADECORE_DIAGNOSTIC_OWNER_SEND_NONBLOCKING", raising=False)
    with owner_directory("tc-e3c-") as directory:
        c = config(Path(directory))
        owner = launcher(c, submission_ipc=True, scoped_cold_lookup_epochs=True)
        try:
            boot = owner.start()
            assert not c.socket_path.exists()
            read_client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(owner.private_socket))
            with pytest.raises(ConsumerUnixClientErrorV2):
                call(read_client, "SubmitSignal", signal(999), "private-write-blocked")
            credits = queue()
            port = owner.open_submission_port(credits)
            with pytest.raises(OwnerProtocolError):
                owner.open_submission_port(credits)
            expired = command(boot, c, deadline_ns=1)
            result = port.submit(expired, current_authority=expired.identity.authority)
            assert result.phase == "NOT_STARTED" and result.reason == "DEADLINE"
            port.acknowledge_delivery(1, current_authority=expired.identity.authority)
            stale = command(boot, c, serial=2)
            stale = msgspec.structs.replace(
                stale,
                identity=msgspec.structs.replace(
                    stale.identity,
                    authority=msgspec.structs.replace(
                        stale.identity.authority, authorization_epoch=2
                    ),
                ),
            )
            result = port.submit(stale, current_authority=stale.identity.authority)
            assert result.phase == "NOT_STARTED" and result.reason == "PRECHECK"
            port.acknowledge_delivery(2, current_authority=stale.identity.authority)
            work = command(boot, c, serial=3)
            result = port.submit(work, current_authority=work.identity.authority)
            assert result.phase == "COMPLETED" and result.source_anchor == "RECOVERED_PREFIX"
            response = decode_frame(result.response_frame).payload
            assert response["status"] == "ACCEPTED", response
            order = response["payload"]["oms_order_id"]
            assert credits.used == 1
            port.acknowledge_delivery(3, current_authority=work.identity.authority)
            assert credits.used == 0

            def terminal():
                found = call(read_client, "GetOrder", {"oms_order_id": order})
                return (
                    found["status"] == "COMPLETED"
                    and found["payload"]["order"]["payload"]["order_state"] == "FILLED"
                )

            wait_for(terminal)
            duplicate = command(boot, c, serial=4)
            result = port.submit(duplicate, current_authority=duplicate.identity.authority)
            assert decode_frame(result.response_frame).payload["payload"]["oms_order_id"] == order
            port.acknowledge_delivery(4, current_authority=duplicate.identity.authority)
            cancel = command(
                boot,
                c,
                serial=5,
                payload_changes={
                    "operation": "CancelOrder",
                    "payload": {"oms_order_id": "not-an-owned-order", "reason": "test"},
                },
            )
            canceled = port.submit(cancel, current_authority=cancel.identity.authority)
            assert canceled.phase == "COMPLETED"
            # Existing long-running contract: Cancel cannot use the cold SQL
            # GetOrder fallback on a WRITE context. Preserve that fail-closed
            # rejection; this is NOT cold-order cancel capability acceptance.
            assert decode_frame(canceled.response_frame).payload["code"] == "INTERNAL_ERROR"
            port.acknowledge_delivery(5, current_authority=cancel.identity.authority)
            closed = owner.close()
            assert closed is not None and closed.final_lsn == 4 and owner.shutdown_verified
            status = list((c.journal_root / "traces").glob("owner-ipc-*.status.json"))
            assert len(status) == 1
            import json

            evidence = json.loads(status[0].read_bytes())
            assert evidence["submitted"] == evidence["written"] == 5
            assert evidence["closed"] and evidence["queue_dropped"] == 0
            records = [
                json.loads(line)
                for trace in (c.journal_root / "traces").glob("owner-ipc-*.jsonl")
                for line in trace.read_text().splitlines()
            ]
            accepted = next(row for row in records if row["serial"] == 3)
            names = (
                "context_exit_started_ns",
                "context_exit_finished_ns",
                "journal_bound_started_ns",
                "journal_bound_finished_ns",
                "response_check_started_ns",
                "response_check_finished_ns",
                "completed_emit_started_ns",
                "completed_emit_finished_ns",
            )
            assert all(name in accepted for name in names) == detailed
            if detailed:
                stamps = [accepted[name] for name in names]
                assert stamps == sorted(stamps)
                assert (
                    accepted["submit_actor_cpu_started_ns"]
                    <= accepted["submit_actor_cpu_finished_ns"]
                )
                assert (
                    accepted["completed_emit_cpu_started_ns"]
                    <= accepted["completed_emit_cpu_finished_ns"]
                )
                if not nonblocking:
                    assert (
                        accepted["completed_packet_encode_started_ns"]
                        <= accepted["completed_packet_encode_finished_ns"]
                        <= accepted["completed_socket_send_started_ns"]
                        <= accepted["completed_socket_send_finished_ns"]
                    )
                    assert (
                        accepted["completed_socket_send_cpu_started_ns"]
                        <= accepted["completed_socket_send_cpu_finished_ns"]
                    )
        finally:
            if not owner.shutdown_verified:
                owner.abort()


@real_owner
@pytest.mark.parametrize("fault", ("peer", "scope", "request_id", "business_key"))
def test_actual_owner_rejects_forged_frontend_identity_and_manual_recovery_proves_no_fact(fault):
    with owner_directory("tc-e3c-reject-") as directory:
        c = config(Path(directory))
        owner = launcher(c, submission_ipc=True)
        try:
            boot = owner.start()
            descendants = live_children(owner.pid)
            work = command(boot, c)
            identity = work.identity
            if fault == "peer":
                identity = msgspec.structs.replace(
                    identity, peer=OwnerPeerV1(os.getpid(), c.peer_uid + 1, c.peer_gid)
                )
            elif fault == "scope":
                identity = msgspec.structs.replace(
                    identity,
                    authority=msgspec.structs.replace(identity.authority, scope_sha256="e" * 64),
                )
            elif fault == "request_id":
                identity = msgspec.structs.replace(identity, request_id="unrelated-request")
            else:
                identity = msgspec.structs.replace(identity, business_key="unrelated-business-key")
            work = msgspec.structs.replace(work, identity=identity)
            send_owner_packet(
                owner._business_channel, work, deadline_ns=time.monotonic_ns() + 3_000_000_000
            )
            with pytest.raises((OwnerProtocolError, OSError)):
                receive_owner_packet(
                    owner._business_channel, deadline_ns=time.monotonic_ns() + 5_000_000_000
                )
            owner._process.join(15)
            assert owner.failed and not owner.shutdown_verified
            wait_for(lambda: not any(is_live(pid) for pid in descendants), seconds=15)
            recovered = launcher(c, initialize=False, submission_ipc=True)
            try:
                assert recovered.start().recovered_lsn == 0
                assert recovered.close().final_lsn == 0
            finally:
                if not recovered.shutdown_verified:
                    recovered.abort()
        finally:
            owner.abort()


@real_owner
def test_actual_commit_then_process_death_loses_reply_and_recovery_never_duplicates_order():
    with owner_directory("tc-e3c-unknown-") as directory:
        c = config(Path(directory))
        owner = launcher(c, submission_ipc=True)
        try:
            with patch(
                "xuanling_tradecore.consumer.process_oms_owner._run_owner", _kill_after_effect_owner
            ):
                boot = owner.start()
            descendants = live_children(owner.pid)
            credits = queue()
            port = owner.open_submission_port(credits)
            work = command(boot, c)
            with pytest.raises((OwnerProtocolError, OSError)):
                port.submit(work, current_authority=work.identity.authority)
            owner._process.join(15)
            assert owner._process.exitcode == 77 and owner.failed
            assert port.failed and credits.used == 1
            with pytest.raises(OwnerProtocolError):
                owner.close()
            assert not owner.shutdown_verified
            wait_for(lambda: not any(is_live(pid) for pid in descendants), seconds=15)
            recovered = launcher(c, initialize=False, submission_ipc=True)
            try:
                new_boot = recovered.start()
                assert new_boot.recovered_lsn == 4 and new_boot.generation != boot.generation
                reader = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(recovered.private_socket))
                plan = call(
                    reader, "CreateOrderSnapshot", {"consumer_account_id": "test", "page_size": 100}
                )
                assert plan["status"] == "COMPLETED" and plan["payload"]["order_count"] == 1
                page = call(
                    reader, "ReadOrderSnapshotPage", {"page_cursor": plan["payload"]["page_cursor"]}
                )
                original_id = page["payload"]["orders"][0]["payload"]["oms_order_id"]
                new_port = recovered.open_submission_port(queue())
                retry = command(new_boot, c)
                receipt = new_port.submit(retry, current_authority=retry.identity.authority)
                assert (
                    decode_frame(receipt.response_frame).payload["payload"]["oms_order_id"]
                    == original_id
                )
                new_port.acknowledge_delivery(1, current_authority=retry.identity.authority)
                final = recovered.close()
                assert final is not None and final.final_lsn == 4
            finally:
                if not recovered.shutdown_verified:
                    recovered.abort()
        finally:
            owner.abort()
