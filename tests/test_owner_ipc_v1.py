"""E3 protocol qualification only: the spawned peer below is NOT an OMS."""

import multiprocessing as mp
import os
import socket
import struct
import sys
import zlib
from hashlib import sha256
from threading import Thread
from time import monotonic_ns
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.consumer.fair_queue_v2 import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
)
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerAuthorityV1,
    OwnerCancelV1,
    OwnerCreditLedgerV1,
    OwnerEventV1,
    OwnerIdentityV1,
    OwnerProtocolError,
    OwnerReadyV1,
    OwnerRequestV1,
    decode_owner_packet,
    encode_owner_packet,
    receive_owner_packet,
    send_owner_packet,
    send_owner_packet_nonblocking_candidate,
)
from xuanling_tradecore.consumer.wire_v2 import MAX_FRAME_BYTES

GENERATION = "a" * 32
SCOPE = "b" * 64


def authority(**changes):
    values = dict(
        consumer_id="consumer",
        account_id="account",
        strategy_id="strategy",
        scope_sha256=SCOPE,
        authorization_epoch=1,
    )
    values.update(changes)
    return OwnerAuthorityV1(**values)


def request(serial=1, **changes):
    identity = OwnerIdentityV1(
        GENERATION,
        serial,
        authority(),
        "request",
        "original-idempotency-key",
        sha256(b"original frame").digest(),
    )
    values = dict(
        identity=identity,
        operation="SubmitSignal",
        deadline_ns=monotonic_ns() + 5_000_000_000,
        request_frame=b"original frame",
    )
    values.update(changes)
    return OwnerRequestV1(**values)


def ready(**changes):
    values = dict(
        generation=GENERATION,
        owner_pid=123,
        scope_sha256=SCOPE,
        journal_id="journal",
        recovered_lsn=10,
        source_prefix_sha256=sha256(b"verified prefix").digest(),
    )
    values.update(changes)
    return OwnerReadyV1(**values)


def complete(job, **changes):
    values = dict(
        identity=job.identity,
        phase="COMPLETED",
        observed_ns=monotonic_ns(),
        response_frame=b"exact original response",
        journal_id="journal",
        source_lsn=12,
        source_prefix_sha256=sha256(b"next verified prefix").digest(),
    )
    values.update(changes)
    return OwnerEventV1(**values)


def book(capacity=2, maximum=128, is_ready=True):
    queue = BoundedFairOperationQueueV2(
        total_capacity=capacity,
        lane_capacities={lane: capacity for lane in ConsumerOperationLaneV2},
    )
    ledger = OwnerCreditLedgerV1(
        generation=GENERATION,
        scope_sha256=SCOPE,
        owner_pid=123,
        ready_queue=queue,
        maximum_inflight=maximum,
    )
    if is_ready:
        ledger.accept_ready(ready())
    return ledger, queue


def started(job, **changes):
    return OwnerEventV1(job.identity, "STARTED", changes.get("observed_ns", monotonic_ns()))


def altered_packet(message, **changes):
    value = msgspec.msgpack.decode(msgspec.msgpack.encode(message))
    value.update(changes)
    body = msgspec.msgpack.encode(value)
    return struct.pack("!4sII", b"TCO1", len(body), zlib.crc32(body)) + body


@pytest.mark.parametrize(
    "kind", ("request", "cancel", "ready", "started", "completed", "unknown", "not_started")
)
def test_strict_immutable_bytes_round_trip(kind):
    job = request()
    messages = {
        "request": job,
        "cancel": OwnerCancelV1(job.identity),
        "ready": ready(),
        "started": started(job),
        "completed": complete(job),
        "unknown": OwnerEventV1(job.identity, "UNKNOWN", monotonic_ns()),
        "not_started": OwnerEventV1(job.identity, "NOT_STARTED", monotonic_ns(), reason="STOPPED"),
    }
    message = messages[kind]
    assert decode_owner_packet(encode_owner_packet(message)) == message
    with pytest.raises(AttributeError):
        message.identity = "cannot mutate an IPC message"
    with pytest.raises(OwnerProtocolError):
        encode_owner_packet(lambda: message)  # no closure/resolver serialization


@pytest.mark.parametrize(
    "mutation",
    ("crc", "magic", "truncated", "extra", "empty", "unknown", "boolean", "digest", "operation"),
)
def test_bad_packet_schema_and_corruption_are_not_receipts(mutation):
    packet = encode_owner_packet(request())
    if mutation == "crc":
        packet = packet[:-1] + bytes([packet[-1] ^ 1])
    elif mutation == "magic":
        packet = b"BAD!" + packet[4:]
    elif mutation == "truncated":
        packet = packet[:-1]
    elif mutation == "extra":
        packet += b"trailing packet/garbage"
    elif mutation == "empty":
        packet = b""
    elif mutation == "unknown":
        packet = altered_packet(request(), unknown_critical_field=True)
    elif mutation == "boolean":
        packet = altered_packet(request(), deadline_ns=True)
    elif mutation == "digest":
        packet = altered_packet(request(), request_frame=b"different request")
    else:
        packet = altered_packet(request(), operation="NotAWrite")
    with pytest.raises(OwnerProtocolError):
        decode_owner_packet(packet)


@pytest.mark.parametrize(
    "field,value",
    (
        ("serial", 0),
        ("serial", True),
        ("serial", 1 << 64),
        ("generation", "x" * 32),
        ("request_id", "secret" * 100),
        ("business_key", "x" * 1025),
        ("request_sha256", b"short"),
    ),
)
def test_invalid_identity_is_rejected_even_before_encoding(field, value):
    values = msgspec.structs.asdict(request().identity)
    values[field] = value
    with pytest.raises(OwnerProtocolError):
        OwnerIdentityV1(**values)


def test_exact_maximum_frame_and_bounded_header_before_reading_body():
    body = b"x" * MAX_FRAME_BYTES
    identity = msgspec.structs.replace(request().identity, request_sha256=sha256(body).digest())
    job = request(identity=identity, request_frame=body)
    assert decode_owner_packet(encode_owner_packet(job)) == job
    with pytest.raises(OwnerProtocolError):
        request(identity=identity, request_frame=body + b"x")
    left, right = socket.socketpair()
    try:
        right.sendall(struct.pack("!4sII", b"TCO1", MAX_OWNER_PACKET_BYTES + 1, 0))
        with pytest.raises(OwnerProtocolError, match="header"):
            receive_owner_packet(left, deadline_ns=monotonic_ns() + 1_000_000_000)
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("failure", ("idle_eof", "header_eof", "body_eof", "timeout"))
def test_socket_loss_and_timeout_never_mean_definite_not_started(failure):
    left, right = socket.socketpair()
    try:
        packet = encode_owner_packet(request())
        if failure == "header_eof":
            right.sendall(packet[:5])
        elif failure == "body_eof":
            right.sendall(packet[:-1])
        if failure != "timeout":
            right.shutdown(socket.SHUT_WR)
        with pytest.raises((OwnerProtocolError, TimeoutError)):
            receive_owner_packet(left, deadline_ns=monotonic_ns() + 40_000_000)
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


def test_existing_credits_include_queued_running_and_undelivered_not_a_new_queue():
    ledger, queue = book()
    one, two = request(), request(2)
    assert ledger.reserve(one)
    assert queue.offer("already decoded", ConsumerOperationLaneV2.SNAPSHOT)
    assert queue.used == 2
    assert not ledger.reserve(two)
    assert queue.pop() == ("already decoded", ConsumerOperationLaneV2.SNAPSHOT)
    assert ledger.reserve(two)
    ledger.observe(started(one))
    ledger.observe(complete(one))
    assert queue.used == 2 and ledger.outstanding == 2
    assert not queue.offer("overflow", ConsumerOperationLaneV2.OTHER)
    assert (
        ledger.result(1, current_authority=authority()).response_frame == b"exact original response"
    )
    ledger.release(1)  # only after socket delivery or definitive client disposal
    assert queue.used == 1
    ledger.observe(started(two))
    ledger.observe(complete(two))
    ledger.release(2)
    ledger.require_drained()
    assert queue.used == 0


def test_same_lsn_cannot_change_prefix_and_known_idempotent_prefix_is_allowed():
    ledger, queue = book()
    one, two = request(), request(2)
    assert ledger.reserve(one) and ledger.reserve(two)
    ledger.observe(started(one))
    first = complete(one)
    ledger.observe(first)
    ledger.release(1)
    ledger.observe(started(two))
    ledger.observe(complete(two, source_prefix_sha256=first.source_prefix_sha256))
    ledger.release(2)
    assert queue.used == 0
    three = request(3)
    assert ledger.reserve(three)
    ledger.observe(started(three))
    with pytest.raises(OwnerProtocolError, match="source"):
        ledger.observe(complete(three, source_prefix_sha256=b"different prefix".ljust(32, b"!")))
    assert ledger.failed


def test_wrong_direction_or_kind_is_fatal_not_a_duck_typed_reply():
    ledger, _ = book()
    assert ledger.reserve(request())
    with pytest.raises(OwnerProtocolError, match="kind"):
        ledger.observe(request())
    assert ledger.failed
    ledger, _ = book(is_ready=False)
    with pytest.raises(OwnerProtocolError, match="readiness"):
        ledger.accept_ready(request())
    assert ledger.failed


@pytest.mark.parametrize("failure", ("expired", "too_far", "disconnect"))
def test_send_deadlines_and_disconnect_are_finite_and_never_retry(failure):
    left, right = socket.socketpair()
    try:
        deadline = monotonic_ns() + 1_000_000_000
        if failure == "expired":
            deadline = 1
        elif failure == "too_far":
            deadline += 61_000_000_000
        else:
            # Windows socketpair is TCP-backed: peer close need not fail the
            # first send immediately. A locally closed write half is a
            # deterministic send fault on both supported platforms.
            left.shutdown(socket.SHUT_WR)
        with pytest.raises((OwnerProtocolError, OSError)):
            send_owner_packet(left, request(), deadline_ns=deadline)
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


def test_partial_send_timeout_fences_credit_instead_of_replaying_command():
    left, right = socket.socketpair()
    try:
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        right.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        body = b"x" * MAX_FRAME_BYTES
        identity = msgspec.structs.replace(request().identity, request_sha256=sha256(body).digest())
        job = request(identity=identity, request_frame=body)
        ledger, queue = book()
        assert ledger.reserve(job)
        original_send = socket.socket.sendall

        def partial_send(channel, packet):
            original_send(channel, packet[:13])
            raise TimeoutError("injected after first byte of body")

        with patch.object(socket.socket, "sendall", partial_send), pytest.raises(TimeoutError):
            send_owner_packet(left, job, deadline_ns=monotonic_ns() + 1_000_000_000)
        assert right.recv(64) == encode_owner_packet(job)[:13]
        ledger.fail()
        assert ledger.failed and queue.used == 1
        assert not ledger.reserve(request(2))
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


def test_optional_send_diagnostics_preserve_packet_and_reveal_incomplete_send():
    left, right = socket.socketpair()
    try:
        job = request()
        evidence = {}
        send_owner_packet(
            left, job, deadline_ns=monotonic_ns() + 1_000_000_000, diagnostic=evidence
        )
        assert receive_owner_packet(right, deadline_ns=monotonic_ns() + 1_000_000_000) == job
        assert (
            evidence["completed_packet_encode_started_ns"]
            <= evidence["completed_packet_encode_finished_ns"]
            <= evidence["completed_timeout_get_started_ns"]
            <= evidence["completed_timeout_get_finished_ns"]
            <= evidence["completed_timeout_set_started_ns"]
            <= evidence["completed_timeout_set_finished_ns"]
            <= evidence["completed_socket_send_started_ns"]
            <= evidence["completed_socket_send_finished_ns"]
        )
        assert (
            evidence["completed_socket_send_cpu_started_ns"]
            <= evidence["completed_socket_send_cpu_finished_ns"]
        )
        failed = {}
        left.shutdown(socket.SHUT_WR)
        with pytest.raises(OSError):
            send_owner_packet(
                left, job, deadline_ns=monotonic_ns() + 1_000_000_000, diagnostic=failed
            )
        assert "completed_socket_send_started_ns" in failed
        assert "completed_socket_send_finished_ns" not in failed
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


@pytest.mark.skipif(sys.platform != "linux", reason="candidate requires Linux MSG_DONTWAIT")
def test_nonblocking_candidate_preserves_frame_deadline_and_socket_mode():
    left, right = socket.socketpair()
    try:
        job = request()
        send_owner_packet_nonblocking_candidate(
            left, job, deadline_ns=monotonic_ns() + 1_000_000_000
        )
        assert receive_owner_packet(right, deadline_ns=monotonic_ns() + 1_000_000_000) == job
        assert left.gettimeout() is None
        with pytest.raises(OwnerProtocolError):
            send_owner_packet_nonblocking_candidate(left, job, deadline_ns=1)
        left.settimeout(1.0)
        with pytest.raises(OwnerProtocolError, match="un-timed"):
            send_owner_packet_nonblocking_candidate(
                left, job, deadline_ns=monotonic_ns() + 1_000_000_000
            )
        assert left.gettimeout() == 1.0
    finally:
        left.close()
        right.close()


@pytest.mark.skipif(sys.platform != "linux", reason="candidate requires Linux MSG_DONTWAIT")
def test_nonblocking_candidate_partial_send_times_out_without_replay():
    left, right = socket.socketpair()
    try:
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        body = b"x" * MAX_FRAME_BYTES
        identity = msgspec.structs.replace(request().identity, request_sha256=sha256(body).digest())
        job = request(identity=identity, request_frame=body)
        with pytest.raises((TimeoutError, OwnerProtocolError)):
            send_owner_packet_nonblocking_candidate(
                left, job, deadline_ns=monotonic_ns() + 50_000_000
            )
        assert right.recv(65536)  # partial bytes are UNKNOWN, not a retryable absence
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


@pytest.mark.skipif(os.name != "posix", reason="actual AF_UNIX send-buffer saturation needs Linux")
def test_actual_unix_socket_send_buffer_timeout_has_no_hidden_retry():
    left, right = socket.socketpair()
    try:
        assert left.family == socket.AF_UNIX
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        body = b"x" * MAX_FRAME_BYTES
        identity = msgspec.structs.replace(request().identity, request_sha256=sha256(body).digest())
        with pytest.raises(TimeoutError):
            send_owner_packet(
                left,
                request(identity=identity, request_frame=body),
                deadline_ns=monotonic_ns() + 50_000_000,
            )
        assert right.recv(65536)  # actual partial bytes exist, not an all-or-none send
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()


def test_owner_local_cap_and_serial_never_reset_when_credit_released():
    ledger, queue = book(capacity=4, maximum=1)
    one = request()
    assert ledger.reserve(one)
    assert not ledger.reserve(request(2)) and queue.used == 1
    ledger.observe(OwnerEventV1(one.identity, "NOT_STARTED", monotonic_ns(), reason="PRECHECK"))
    ledger.release(1)
    with pytest.raises(OwnerProtocolError, match="serial"):
        ledger.reserve(one)
    assert ledger.reserve(request(2))
    ledger.fail()
    ledger.release(2)
    assert queue.used == 0
    with pytest.raises(OwnerProtocolError, match="shutdown"):
        ledger.require_drained()  # zero entries does not erase the failed owner


def test_cancel_is_only_a_request_until_owner_proves_not_started():
    ledger, queue = book()
    one = request()
    assert ledger.reserve(one)
    assert ledger.request_cancel(1) == OwnerCancelV1(one.identity)
    assert ledger.request_cancel(1) is None
    with pytest.raises(OwnerProtocolError, match="unresolved"):
        ledger.release(1)
    assert queue.used == 1
    # Cancel lost the race: STARTED still legal; never forge cancellation.
    ledger.observe(started(one))
    assert ledger.request_cancel(1) is None
    ledger.observe(complete(one))
    assert ledger.result(1, current_authority=authority()).phase == "COMPLETED"
    ledger.release(1)


@pytest.mark.parametrize("reason", ("CANCELLED", "DEADLINE", "STOPPED", "PRECHECK"))
def test_owner_confirmed_not_started_has_no_response_or_source(reason):
    deadline = 1 if reason == "DEADLINE" else monotonic_ns() + 5_000_000_000
    job = request(deadline_ns=deadline)
    ledger, queue = book()
    assert ledger.reserve(job)
    if reason == "CANCELLED":
        ledger.request_cancel(1)
    ledger.observe(OwnerEventV1(job.identity, "NOT_STARTED", monotonic_ns(), reason=reason))
    result = ledger.result(1, current_authority=authority())
    assert result.phase == "NOT_STARTED" and not result.response_frame and result.source_lsn == 0
    ledger.release(1)
    assert queue.used == 0


@pytest.mark.parametrize(
    "changed",
    (
        dict(authorization_epoch=2),
        dict(consumer_id="other"),
        dict(account_id="other"),
        dict(strategy_id="other"),
        dict(scope_sha256="c" * 64),
    ),
)
def test_current_delivery_authority_rechecked_even_after_commit(changed):
    ledger, queue = book()
    job = request()
    assert ledger.reserve(job)
    ledger.observe(started(job))
    ledger.observe(complete(job))
    with pytest.raises(OwnerProtocolError, match="revoked"):
        ledger.result(1, current_authority=authority(**changed))
    assert queue.used == 1 and not ledger.failed
    ledger.release(1)  # disposed without delivering the fenced response


@pytest.mark.parametrize(
    "case",
    (
        "old_generation",
        "wrong_serial",
        "wrong_key",
        "wrong_scope",
        "wrong_request_digest",
        "wrong_epoch",
        "finish_without_start",
        "second_start",
        "reordered_start",
        "parallel_start",
        "expired_start",
        "wrong_journal",
        "source_regression",
        "cancel_after_start",
        "unrequested_cancel",
        "premature_deadline",
        "duplicate_finish",
        "time_regression",
    ),
)
def test_unrelated_or_out_of_order_reply_poisons_owner_never_issues_receipt(case):
    ledger, queue = book()
    one, two = request(), request(2)
    assert ledger.reserve(one) and ledger.reserve(two)
    event = started(one)
    if case == "old_generation":
        event = msgspec.structs.replace(
            event, identity=msgspec.structs.replace(one.identity, generation="d" * 32)
        )
    elif case == "wrong_serial":
        event = msgspec.structs.replace(
            event, identity=msgspec.structs.replace(one.identity, serial=99)
        )
    elif case == "wrong_key":
        event = msgspec.structs.replace(
            event, identity=msgspec.structs.replace(one.identity, business_key="other")
        )
    elif case == "wrong_scope":
        event = msgspec.structs.replace(
            event,
            identity=msgspec.structs.replace(
                one.identity, authority=authority(scope_sha256="c" * 64)
            ),
        )
    elif case == "wrong_request_digest":
        event = msgspec.structs.replace(
            event, identity=msgspec.structs.replace(one.identity, request_sha256=b"x" * 32)
        )
    elif case == "wrong_epoch":
        event = msgspec.structs.replace(
            event,
            identity=msgspec.structs.replace(
                one.identity, authority=authority(authorization_epoch=2)
            ),
        )
    elif case == "finish_without_start":
        event = complete(one)
    elif case in ("second_start", "parallel_start"):
        ledger.observe(started(one))
        event = started(one if case == "second_start" else two)
    elif case == "reordered_start":
        event = started(two)
    elif case == "expired_start":
        event = started(one, observed_ns=one.deadline_ns)
    elif case in ("wrong_journal", "source_regression", "time_regression"):
        ledger.observe(started(one))
        event = complete(
            one,
            **{
                "wrong_journal": dict(journal_id="other"),
                "source_regression": dict(source_lsn=9),
                "time_regression": dict(observed_ns=1),
            }[case],
        )
    elif case == "cancel_after_start":
        ledger.request_cancel(1)
        ledger.observe(started(one))
        event = OwnerEventV1(one.identity, "NOT_STARTED", monotonic_ns(), reason="CANCELLED")
    elif case == "unrequested_cancel":
        event = OwnerEventV1(one.identity, "NOT_STARTED", monotonic_ns(), reason="CANCELLED")
    elif case == "premature_deadline":
        event = OwnerEventV1(one.identity, "NOT_STARTED", monotonic_ns(), reason="DEADLINE")
    elif case == "duplicate_finish":
        ledger.observe(started(one))
        event = complete(one)
        ledger.observe(event)
    with pytest.raises(OwnerProtocolError):
        ledger.observe(event)
    assert ledger.failed and queue.used == 2
    assert not ledger.reserve(request(3))
    with pytest.raises(OwnerProtocolError):
        ledger.result(1, current_authority=authority())
    with pytest.raises(OwnerProtocolError):
        ledger.require_drained()
    ledger.release(1)
    ledger.release(2)
    assert queue.used == 0


@pytest.mark.parametrize("failure", ("disconnect", "explicit_unknown", "shutdown_pending"))
def test_lost_reply_is_unknown_all_writes_fenced_and_not_automatically_retried(failure):
    ledger, queue = book()
    one = request()
    assert ledger.reserve(one)
    ledger.observe(started(one))
    if failure == "explicit_unknown":
        ledger.observe(OwnerEventV1(one.identity, "UNKNOWN", monotonic_ns()))
    elif failure == "disconnect":
        ledger.fail()
    else:
        ledger.stop()
    assert not ledger.reserve(request(2))
    assert queue.used == 1
    with pytest.raises(OwnerProtocolError):
        ledger.require_drained()
    with pytest.raises(OwnerProtocolError):
        ledger.result(1, current_authority=authority())


@pytest.mark.parametrize(
    "changed",
    (
        dict(generation="e" * 32),
        dict(owner_pid=124),
        dict(scope_sha256="e" * 64),
    ),
)
def test_readiness_is_from_exact_spawn_pid_and_boot_generation(changed):
    ledger, _ = book(is_ready=False)
    assert not ledger.reserve(request())
    with pytest.raises(OwnerProtocolError):
        ledger.accept_ready(ready(**changed))
    assert ledger.failed and not ledger.reserve(request())


def test_ready_is_single_use_and_ledger_cannot_migrate_between_threads():
    ledger, _ = book()
    failures = []

    def use_from_other_thread():
        try:
            ledger.reserve(request())
        except OwnerProtocolError as error:
            failures.append(str(error))

    worker = Thread(target=use_from_other_thread)
    worker.start()
    worker.join(2)
    assert failures and "one reactor" in failures[0]
    assert ledger.outstanding == 0
    with pytest.raises(OwnerProtocolError):
        ledger.accept_ready(ready())


def _spawn_protocol_peer(channel, mode):
    """A protocol test peer only: no OrderBook, journal, broker or success proof."""
    try:

        def send_fragmented(message):
            packet = encode_owner_packet(message)
            for start in range(0, len(packet), 7):
                channel.sendall(packet[start : start + 7])

        send_fragmented(ready(owner_pid=os.getpid()))
        job = receive_owner_packet(channel, deadline_ns=monotonic_ns() + 10_000_000_000)
        assert isinstance(job, OwnerRequestV1)
        send_fragmented(started(job))
        if mode == "drop_after_start":
            return
        send_fragmented(complete(job))
    finally:
        channel.close()


@pytest.mark.parametrize("mode", ("reply", "drop_after_start"))
def test_actual_spawn_fragmented_socketpair_and_lost_reply(mode):
    context = mp.get_context("spawn")
    parent, child = socket.socketpair()
    process = context.Process(target=_spawn_protocol_peer, args=(child, mode))
    process.start()
    child.close()
    try:
        assert process.pid and process.pid != os.getpid()
        owner_ready = receive_owner_packet(parent, deadline_ns=monotonic_ns() + 10_000_000_000)
        assert isinstance(owner_ready, OwnerReadyV1)
        assert owner_ready.owner_pid == process.pid
        queue = BoundedFairOperationQueueV2(
            total_capacity=1,
            lane_capacities={lane: 1 for lane in ConsumerOperationLaneV2},
        )
        ledger = OwnerCreditLedgerV1(
            generation=GENERATION,
            scope_sha256=SCOPE,
            owner_pid=process.pid,
            ready_queue=queue,
        )
        ledger.accept_ready(owner_ready)
        job = request()
        assert ledger.reserve(job)
        send_owner_packet(parent, job, deadline_ns=monotonic_ns() + 5_000_000_000)
        event = receive_owner_packet(parent, deadline_ns=monotonic_ns() + 5_000_000_000)
        assert isinstance(event, OwnerEventV1) and event.phase == "STARTED"
        ledger.observe(event)
        if mode == "reply":
            event = receive_owner_packet(parent, deadline_ns=monotonic_ns() + 5_000_000_000)
            assert isinstance(event, OwnerEventV1)
            ledger.observe(event)
            assert (
                ledger.result(1, current_authority=authority()).response_frame
                == b"exact original response"
            )
            ledger.release(1)
            ledger.stop()
            ledger.require_drained()
        else:
            with pytest.raises(OwnerProtocolError, match="EOF"):
                receive_owner_packet(parent, deadline_ns=monotonic_ns() + 5_000_000_000)
            ledger.fail()
            assert ledger.failed and queue.used == 1
            assert not ledger.reserve(request(2))
            with pytest.raises(OwnerProtocolError):
                ledger.result(1, current_authority=authority())
            with pytest.raises(OwnerProtocolError):
                ledger.require_drained()
    finally:
        parent.close()
        process.join(10)
        if process.is_alive():
            process.kill()
            process.join(5)
            pytest.fail("protocol fixture did not terminate")
        assert process.exitcode == 0
