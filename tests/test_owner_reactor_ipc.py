"""Transport peers here are protocol fixtures, not authoritative OMS runtimes."""

import multiprocessing as mp
import socket
import struct
from hashlib import sha256
from threading import Thread
from time import monotonic_ns

import msgspec
import pytest
from xuanling_tradecore.consumer.fair_queue_v2 import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
)
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerEventV1,
    OwnerIdentityV1,
    OwnerProtocolError,
    OwnerRequestV1,
    encode_owner_packet,
    receive_owner_packet,
)
from xuanling_tradecore.consumer.owner_reactor_ipc import ReactorOwnerPortV1
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

from test_owner_ipc_v1 import authority, ready
from test_owner_submission_ipc import queue


def command(serial=1):
    frame = encode_frame(
        MessageKind.REQUEST,
        serial,
        {
            "message_type": "request",
            "schema_version": 2,
            "operation": "SubmitSignal",
            "request_id": f"request-{serial}",
            "payload": {},
        },
    )
    return OwnerRequestV1(
        OwnerIdentityV1(
            ready().generation,
            serial,
            authority(),
            f"request-{serial}",
            f"key-{serial}",
            sha256(frame).digest(),
        ),
        "SubmitSignal",
        monotonic_ns() + 5_000_000_000,
        frame,
    )


def completed(job, *, source_lsn: int = 12, identity: OwnerIdentityV1 | None = None):
    response = encode_frame(
        MessageKind.RESPONSE,
        job.identity.serial,
        {
            "request_id": job.identity.request_id,
            "payload": {
                "journal_id": ready().journal_id,
                "journal_lsn": 12,
                "consumer_request_id": job.identity.request_id,
            },
        },
    )
    return OwnerEventV1(
        identity=job.identity if identity is None else identity,
        phase="COMPLETED",
        observed_ns=monotonic_ns(),
        response_frame=response,
        journal_id=ready().journal_id,
        source_lsn=source_lsn,
        source_prefix_sha256=ready().source_prefix_sha256,
        source_anchor="RECOVERED_PREFIX",
    )


@pytest.fixture
def transport():
    parent, child = socket.socketpair()
    credits = queue()
    port = ReactorOwnerPortV1(parent, ready=ready(), ready_queue=credits)
    try:
        yield port, child, credits
    finally:
        parent.close()
        child.close()


@pytest.fixture
def batch_transport():
    parent, child = socket.socketpair()
    credits = BoundedFairOperationQueueV2(
        total_capacity=128,
        lane_capacities={lane: 128 for lane in ConsumerOperationLaneV2},
    )
    port = ReactorOwnerPortV1(parent, ready=ready(), ready_queue=credits)
    try:
        yield port, child, credits
    finally:
        parent.close()
        child.close()


def transfer(port, peer, job):
    assert port.offer(job)
    while port.wants_write:
        port.write_ready(maximum_bytes=7)
    assert receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000) == job


def test_diagnostic_send_boundary_waits_for_complete_fragmented_packet():
    parent, peer = socket.socketpair()
    observations = []
    port = ReactorOwnerPortV1(
        parent, ready=ready(), ready_queue=queue(),
        diagnostic_sent=lambda serial, before, after: observations.append((serial, before, after)),
    )
    try:
        job = command()
        assert port.offer(job)
        port.write_ready(maximum_bytes=7)
        assert observations == []
        while port.wants_write:
            port.write_ready(maximum_bytes=7)
        assert len(observations) == 1
        serial, before, after = observations[0]
        assert serial == job.identity.serial and 0 < before <= after
        assert receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000) == job
    finally:
        parent.close()
        peer.close()


def test_diagnostic_send_boundary_preserves_batch_serial_order():
    parent, peer = socket.socketpair()
    observations = []
    credits = BoundedFairOperationQueueV2(
        total_capacity=128,
        lane_capacities={lane: 128 for lane in ConsumerOperationLaneV2},
    )
    port = ReactorOwnerPortV1(
        parent, ready=ready(), ready_queue=credits,
        diagnostic_sent=lambda serial, before, after: observations.append((serial, before, after)),
    )
    try:
        jobs = (command(1), command(2))
        for job in jobs:
            assert port.offer(job)
        while port.wants_write:
            port.write_ready(maximum_bytes=31)
        assert [entry[0] for entry in observations] == [1, 2]
        assert all(0 < before <= after for _, before, after in observations)
        for job in jobs:
            assert receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000) == job
    finally:
        parent.close()
        peer.close()


def test_handoff_limit_backpressures_until_terminal_credit_disposed():
    parent, peer = socket.socketpair()
    port = ReactorOwnerPortV1(parent, ready=ready(), ready_queue=queue(), handoff_limit=1)
    try:
        job = command(1)
        transfer(port, peer, job)
        assert port.full and not port.offer(command(2))
        start(port, peer, job)
        assert receive(port, peer, encode_owner_packet(completed(job))) is not None
        assert port.full
        port.dispose(1)
        assert not port.full and port.offer(command(2))
    finally:
        parent.close()
        peer.close()


def receive(port, peer, packet):
    # Deliberately one byte per call, no socket timeout or blocking read.
    answer = None
    for octet in packet:
        peer.sendall(bytes((octet,)))
        current = port.read_ready(maximum_bytes=7)
        if current is not None:
            assert answer is None
            answer = current
    return answer


def start(port, peer, job):
    event = OwnerEventV1(job.identity, "STARTED", monotonic_ns())
    assert receive(port, peer, encode_owner_packet(event)) is None


def test_nonblocking_fragmented_send_read_holds_original_credit_until_delivery(transport):
    port, peer, credits = transport
    assert port.channel.gettimeout() == 0
    assert port.read_ready() is None
    job = command()
    transfer(port, peer, job)
    assert credits.used == 1 and not port.full
    start(port, peer, job)
    result = completed(job)
    assert receive(port, peer, encode_owner_packet(result)) == result
    assert not port.full and credits.used == 1
    with pytest.raises(OwnerProtocolError):
        port.acknowledge_delivery(1, current_authority=authority(authorization_epoch=2))
    assert credits.used == 1
    port.acknowledge_delivery(1, current_authority=authority())
    assert credits.used == 0
    port.stop()
    assert port.shutdown_verified and port.channel.fileno() >= 0


def test_completed_undelivered_and_active_share_original_total_capacity(transport):
    port, peer, credits = transport
    jobs = [command(1), command(2)]
    for job in jobs:
        transfer(port, peer, job)
    assert credits.used == 2 and port.outstanding == 2
    for job in jobs:
        start(port, peer, job)
        receive(port, peer, encode_owner_packet(completed(job)))
    assert credits.used == 2
    with pytest.raises(OwnerProtocolError):
        port.stop()
    for serial in (1, 2):
        port.dispose(serial)
    port.stop()
    assert port.shutdown_verified


def test_multiple_frontend_ports_share_one_hard_credit_total_until_delivery():
    shared = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=3, worker_count=2)
    pairs = [socket.socketpair(), socket.socketpair()]
    credits = [queue(), queue()]
    ports = [
        ReactorOwnerPortV1(
            pair[0],
            ready=ready(),
            ready_queue=credits[index],
            shared_credit=shared,
            worker_id=index,
        )
        for index, pair in enumerate(pairs)
    ]
    try:
        jobs = ((command(1), command(2)), (command(1), command(2)))
        transfer(ports[0], pairs[0][1], jobs[0][0])
        transfer(ports[1], pairs[1][1], jobs[1][0])
        transfer(ports[0], pairs[0][1], jobs[0][1])
        assert shared.snapshot().held_by_worker == (2, 1)
        assert ports[0].full and ports[1].full
        assert not ports[1].offer(jobs[1][1])

        start(ports[1], pairs[1][1], jobs[1][0])
        first_result = completed(jobs[1][0])
        assert receive(
            ports[1], pairs[1][1], encode_owner_packet(first_result)
        ) == first_result
        ports[1].dispose(1)
        assert shared.snapshot().held_by_worker == (2, 0)

        ports[1].begin_turn()
        transfer(ports[1], pairs[1][1], jobs[1][1])
        for index, port in enumerate(ports):
            for job in jobs[index]:
                if index == 1 and job.identity.serial == 1:
                    continue
                start(port, pairs[index][1], job)
                result = completed(job)
                assert receive(port, pairs[index][1], encode_owner_packet(result)) == result
                port.dispose(job.identity.serial)
            port.stop()
        shared.require_all_drained()
    finally:
        for pair in pairs:
            pair[0].close()
            pair[1].close()


def test_owner_failure_fences_shared_credit_without_reclaiming_unknown_work():
    shared = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=2, worker_count=1)
    parent, child = socket.socketpair()
    credits = queue()
    port = ReactorOwnerPortV1(
        parent,
        ready=ready(),
        ready_queue=credits,
        shared_credit=shared,
        worker_id=0,
    )
    try:
        job = command()
        transfer(port, child, job)
        child.close()
        with pytest.raises((OwnerProtocolError, OSError)):
            port.read_ready()
        snapshot = shared.snapshot()
        assert snapshot.fenced and snapshot.held == 1
        assert credits.used == 1
        with pytest.raises(OwnerProtocolError):
            port.stop()
    finally:
        parent.close()
        child.close()


def test_one_turn_batches_exactly_four_ordered_packets_under_original_credits(batch_transport):
    port, peer, credits = batch_transport
    jobs = [command(serial) for serial in range(1, 6)]
    for job in jobs[:4]:
        assert port.offer(job)
    assert port.full and credits.used == 4 and port.outstanding == 4
    assert not port.offer(jobs[4])

    # One bounded call hands all four small packets to the kernel in order.
    port.write_ready()
    assert not port.wants_write and port.full  # the rest of this turn stays fenced
    assert port.turn_offer_budget_exhausted
    assert [
        receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000)
        for _ in range(4)
    ] == jobs[:4]
    assert credits.used == 4  # transport progress never releases business credit

    port.begin_turn()
    assert not port.full
    assert not port.turn_offer_budget_exhausted
    assert port.offer(jobs[4])
    port.write_ready()
    assert receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000) == jobs[4]
    for job in jobs:
        start(port, peer, job)
        result = completed(job)
        assert receive(port, peer, encode_owner_packet(result)) == result
        port.dispose(job.identity.serial)
    assert credits.used == 0
    port.stop()


def test_one_read_turn_consumes_started_and_four_terminal_replies(batch_transport):
    port, peer, credits = batch_transport
    jobs = [command(serial) for serial in range(1, 5)]
    for job in jobs:
        assert port.offer(job)
    port.write_ready()
    for _ in jobs:
        receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000)
    results = []
    events = []
    for job in jobs:
        events.append(OwnerEventV1(job.identity, "STARTED", monotonic_ns()))
        result = completed(job)
        results.append(result)
        events.append(result)
    peer.sendall(b"".join(encode_owner_packet(event) for event in events))
    assert port.read_ready_batch() == tuple(results)
    assert credits.used == 4
    for job in jobs:
        port.dispose(job.identity.serial)
    port.stop()


def test_pipelined_requests_remain_strictly_serial_and_out_of_order_fences(transport):
    port, peer, credits = transport
    first, second = command(1), command(2)
    transfer(port, peer, first)
    transfer(port, peer, second)
    assert credits.used == 2 and port.outstanding == 2 and not port.full

    with pytest.raises(OwnerProtocolError, match="unsolicited/wrong-direction"):
        receive(
            port,
            peer,
            encode_owner_packet(OwnerEventV1(second.identity, "STARTED", monotonic_ns())),
        )
    assert port.failed and credits.used == 2


@pytest.mark.parametrize(
    "fault",
    [
        "eof",
        "crc",
        "oversize",
        "wrong_serial",
        "wrong_generation",
        "no_start",
        "future_receipt",
    ],
)
def test_failure_never_releases_unresolved_credit_or_allows_retry(transport, fault):
    port, peer, credits = transport
    job = command()
    transfer(port, peer, job)
    if fault not in ("no_start", "oversize"):
        start(port, peer, job)
    packet = encode_owner_packet(completed(job))
    if fault == "crc":
        packet = packet[:-1] + bytes((packet[-1] ^ 1,))
    elif fault == "oversize":
        packet = struct.pack("!4sII", b"TCO1", MAX_OWNER_PACKET_BYTES + 1, 0)
    elif fault == "wrong_serial":
        packet = encode_owner_packet(completed(command(2)))
    elif fault == "wrong_generation":
        packet = encode_owner_packet(
            completed(
                job,
                identity=msgspec.structs.replace(job.identity, generation="f" * 32),
            )
        )
    elif fault == "future_receipt":
        packet = encode_owner_packet(completed(job, source_lsn=11))
    with pytest.raises((OwnerProtocolError, OSError)):
        if fault == "eof":
            peer.close()
            port.read_ready()
        else:
            receive(port, peer, packet)
    assert port.failed and credits.used == 1
    assert not port.offer(command(2))
    port.dispose(1)  # failed outcome may be disposed, never makes clean stop
    with pytest.raises(OwnerProtocolError):
        port.stop()
    assert not port.shutdown_verified


def test_expiry_before_send_is_unknown_not_fabricated_retryable_rejection(transport):
    port, _peer, credits = transport
    job = command()
    assert port.offer(job)
    with pytest.raises(OwnerProtocolError):
        port.expire(now_ns=job.deadline_ns)
    assert port.failed and credits.used == 1


def test_thread_ownership_includes_nonblocking_syscalls(transport):
    port, _peer, credits = transport
    failures = []

    def wrong_thread():
        try:
            port.read_ready()
        except OwnerProtocolError as error:
            failures.append(error)

    thread = Thread(target=wrong_thread)
    thread.start()
    thread.join(1)
    assert len(failures) == 1 and credits.used == 0 and not port.failed
