import multiprocessing as mp
import selectors
import socket
from hashlib import sha256
from time import monotonic_ns

import msgspec
import pytest
from xuanling_tradecore.consumer.fair_queue_v2 import ConsumerOperationLaneV2
from xuanling_tradecore.consumer.frontend_read_broker import FrontendOwnerReadBrokerV1
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadMessageV1,
    encode_owner_packet,
    receive_owner_packet,
)
from xuanling_tradecore.consumer.owner_read_reactor import (
    OWNER_READ_SLOT_COUNT,
    ReactorOwnerReadsV1,
)
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

from test_owner_reactor_ipc import command
from test_owner_submission_ipc import queue


def _drive_broker_input(broker, channel):
    for _ in range(3):
        if broker.read_frontend(channel):
            return
    raise AssertionError("broker did not consume complete frontend packet")


def _drive_owner_input(broker, channel):
    result = None
    for _ in range(3):
        observed = broker.io_owner(channel, selectors.EVENT_READ)
        if observed is not None:
            result = observed
    return result


def _flush_frontend_output(broker, channel):
    while broker.frontend_events(channel) & selectors.EVENT_WRITE:
        broker.write_frontend(channel)


def _drive_worker_input(pool, channel):
    result = None
    for _ in range(3):
        observed = pool.io(channel, selectors.EVENT_READ)
        if observed is not None:
            result = observed
    return result


def _read_request(serial=1, operation="GetAccountOrderSnapshot"):
    frame = encode_frame(
        MessageKind.REQUEST,
        serial,
        {
            "message_type": "request",
            "schema_version": 2,
            "operation": operation,
            "request_id": f"read-{serial}",
            "payload": {},
        },
    )
    identity = msgspec.structs.replace(
        command(serial).identity,
        request_id=f"read-{serial}",
        business_key=f"READ:{operation}",
        request_sha256=sha256(frame).digest(),
    )
    return OwnerReadMessageV1(
        identity,
        "REQUEST",
        frame=frame,
        deadline_ns=monotonic_ns() + 5_000_000_000,
    )


class _TemporarilyFullOwnerReads:
    def __init__(self):
        self.available = False
        self.offers = []

    def offer(self, message, lane):
        self.offers.append((message, lane))
        return self.available

    def fail(self):
        self.available = False


def test_lane_specific_central_backpressure_queues_admitted_read_without_fencing():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=4, worker_count=1)
    shared_reads = FrontendSharedCreditV1(
        context, capacity=OWNER_READ_SLOT_COUNT, worker_count=1
    )
    pairs = [socket.socketpair() for _ in range(4)]
    central = _TemporarilyFullOwnerReads()
    broker = FrontendOwnerReadBrokerV1(
        (tuple(pair[1] for pair in pairs),),
        owner_reads=central,  # type: ignore[arg-type]
        shared_credit=shared_total,
        shared_read_slots=shared_reads,
    )
    try:
        assert shared_total.try_acquire(0)
        assert shared_reads.try_acquire(0)
        request = _read_request(1, "ListAccounts")
        pairs[0][0].sendall(encode_owner_packet(request))
        _drive_broker_input(broker, pairs[0][1])

        assert not broker.failed
        assert broker.outstanding == 1
        assert len(central.offers) == 1
        assert central.offers[0][1] is ConsumerOperationLaneV2.OTHER

        central.available = True
        assert broker.pump() == 1
        assert broker.outstanding == 1
        assert central.offers[-1][0].identity.serial == 1
    finally:
        for channel, peer in pairs:
            channel.close()
            peer.close()


def test_frontend_read_serials_may_arrive_out_of_order_across_fixed_channels():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=4, worker_count=1)
    shared_reads = FrontendSharedCreditV1(
        context, capacity=OWNER_READ_SLOT_COUNT, worker_count=1
    )
    pairs = [socket.socketpair() for _ in range(4)]
    central = _TemporarilyFullOwnerReads()
    broker = FrontendOwnerReadBrokerV1(
        (tuple(pair[1] for pair in pairs),),
        owner_reads=central,  # type: ignore[arg-type]
        shared_credit=shared_total,
        shared_read_slots=shared_reads,
    )
    try:
        for _ in range(2):
            assert shared_total.try_acquire(0)
            assert shared_reads.try_acquire(0)
        later, earlier = _read_request(2), _read_request(1)
        pairs[1][0].sendall(encode_owner_packet(later))
        _drive_broker_input(broker, pairs[1][1])
        pairs[0][0].sendall(encode_owner_packet(earlier))
        _drive_broker_input(broker, pairs[0][1])
        assert broker.outstanding == 2
        assert not broker.failed

        # The same trusted child cannot replay a still-recent serial after the
        # legitimate cross-channel reordering was accepted.
        pairs[2][0].sendall(encode_owner_packet(earlier))
        with pytest.raises(OwnerProtocolError, match="repeated/reordered"):
            _drive_broker_input(broker, pairs[2][1])
        assert broker.failed
    finally:
        for channel, peer in pairs:
            channel.close()
            peer.close()


def test_two_frontends_share_owner_read_slots_and_complete_release_ack():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=4, worker_count=2)
    shared_reads = FrontendSharedCreditV1(
        context, capacity=OWNER_READ_SLOT_COUNT, worker_count=2
    )
    worker_pairs = [[socket.socketpair() for _ in range(4)] for _ in range(2)]
    owner_pairs = [socket.socketpair() for _ in range(OWNER_READ_SLOT_COUNT)]
    workers = [
        ReactorOwnerReadsV1(
            tuple(pair[0] for pair in pairs),
            ready_queue=queue(),
            shared_credit=shared_total,
            shared_read_slots=shared_reads,
            worker_id=worker_id,
            release_ack_required=True,
        )
        for worker_id, pairs in enumerate(worker_pairs)
    ]
    central = ReactorOwnerReadsV1(
        tuple(pair[0] for pair in owner_pairs), ready_queue=queue()
    )
    broker = FrontendOwnerReadBrokerV1(
        tuple(tuple(pair[1] for pair in pairs) for pairs in worker_pairs),
        owner_reads=central,
        shared_credit=shared_total,
        shared_read_slots=shared_reads,
    )
    try:
        originals = (_read_request(1), _read_request(1))
        for worker_id, (worker, work) in enumerate(zip(workers, originals, strict=True)):
            assert worker.offer(work, ConsumerOperationLaneV2.SNAPSHOT)
            worker_channel, worker_peer = worker_pairs[worker_id][0]
            while worker.events(worker_channel) & selectors.EVENT_WRITE:
                worker.io(worker_channel, selectors.EVENT_WRITE)
            _drive_broker_input(broker, worker_peer)

        central_requests = []
        active_owner_pairs = []
        for channel, peer in owner_pairs:
            if central.events(channel) & selectors.EVENT_WRITE:
                broker.io_owner(channel, selectors.EVENT_WRITE)
                central_requests.append(
                    receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000)
                )
                active_owner_pairs.append((channel, peer))
        assert [item.identity.serial for item in central_requests] == [1, 2]

        for worker_id, (work, owner_work, owner_pair) in enumerate(
            zip(originals, central_requests, active_owner_pairs, strict=True)
        ):
            owner_channel, owner_peer = owner_pair
            response = encode_frame(
                MessageKind.RESPONSE,
                1,
                {"request_id": owner_work.identity.request_id, "payload": {"ok": True}},
            )
            grant = OwnerReadMessageV1(
                owner_work.identity,
                "GRANT",
                frame=response,
                response_sha256=sha256(response).digest(),
            )
            owner_peer.sendall(encode_owner_packet(grant))
            assert _drive_owner_input(broker, owner_channel) == owner_work.identity.serial
            broker_channel = worker_pairs[worker_id][0][1]
            _flush_frontend_output(broker, broker_channel)
            worker_channel, _worker_peer = worker_pairs[worker_id][0]
            assert _drive_worker_input(workers[worker_id], worker_channel) == 1
            assert workers[worker_id].delivery(1) == response

            if worker_id == 0:
                workers[worker_id].recheck(1)
                while workers[worker_id].events(worker_channel) & selectors.EVENT_WRITE:
                    workers[worker_id].io(worker_channel, selectors.EVENT_WRITE)
                _drive_broker_input(broker, broker_channel)
                assert broker.io_owner(owner_channel, selectors.EVENT_WRITE) is None
                check = receive_owner_packet(
                    owner_peer, deadline_ns=monotonic_ns() + 1_000_000_000
                )
                assert check.phase == "CHECK"
                owner_peer.sendall(
                    encode_owner_packet(
                        OwnerReadMessageV1(
                            owner_work.identity,
                            "GRANT",
                            response_sha256=sha256(response).digest(),
                        )
                    )
                )
                assert _drive_owner_input(broker, owner_channel) == owner_work.identity.serial
                _flush_frontend_output(broker, broker_channel)
                assert _drive_worker_input(workers[worker_id], worker_channel) == 1
                assert workers[worker_id].delivery(1) == response

            workers[worker_id].dispose(1)
            while workers[worker_id].events(worker_channel) & selectors.EVENT_WRITE:
                workers[worker_id].io(worker_channel, selectors.EVENT_WRITE)
            _drive_broker_input(broker, broker_channel)
            assert broker.io_owner(owner_channel, selectors.EVENT_WRITE) is None
            release = receive_owner_packet(
                owner_peer, deadline_ns=monotonic_ns() + 1_000_000_000
            )
            assert release.phase == "RELEASE"
            _flush_frontend_output(broker, broker_channel)
            assert _drive_worker_input(workers[worker_id], worker_channel) is None
            assert workers[worker_id].outstanding == 0
            workers[worker_id].stop()
        assert broker.outstanding == 0
        broker.stop()
        shared_total.require_all_drained()
        shared_reads.require_all_drained()
    finally:
        for pairs in worker_pairs:
            for channel, peer in pairs:
                channel.close()
                peer.close()
        for channel, peer in owner_pairs:
            channel.close()
            peer.close()
