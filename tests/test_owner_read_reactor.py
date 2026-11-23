"""Read transport fixtures: not substitutes for authoritative owner TTL tests."""

import multiprocessing as mp
import selectors
import socket
from hashlib import sha256
from time import monotonic_ns
from typing import Literal

import pytest
from xuanling_tradecore.consumer.fair_queue_v2 import (
    BoundedFairOperationQueueV2,
    ConsumerOperationLaneV2,
)
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerProtocolError,
    OwnerReadMessageV1,
    encode_owner_packet,
    receive_owner_packet,
)
from xuanling_tradecore.consumer.owner_read_reactor import (
    OWNER_DIRECT_READ_SLOT_COUNT,
    OWNER_READ_SLOT_COUNT,
    ReactorOwnerReadsV1,
)
from xuanling_tradecore.consumer.wire_v2 import MessageKind, encode_frame

from test_owner_reactor_ipc import command
from test_owner_submission_ipc import queue


@pytest.fixture
def transport():
    pairs = [socket.socketpair() for _ in range(OWNER_READ_SLOT_COUNT)]
    credits = queue()
    pool = ReactorOwnerReadsV1(tuple(p[0] for p in pairs), ready_queue=credits)
    try:
        yield pool, pairs, credits
    finally:
        for a, b in pairs:
            a.close()
            b.close()


def flush(pool, channel, peer):
    while pool.events(channel) & selectors.EVENT_WRITE:
        pool.io(channel, selectors.EVENT_WRITE)
    message = receive_owner_packet(peer, deadline_ns=monotonic_ns() + 1_000_000_000)
    assert type(message) is OwnerReadMessageV1
    return message


def feed(pool, channel, peer, message):
    result = None
    for octet in encode_owner_packet(message):
        peer.sendall(bytes((octet,)))
        observed = pool.io(channel, selectors.EVENT_READ)
        if observed is not None:
            assert result is None
            result = observed
    return result


def request(serial=1):
    job = command(serial)
    return OwnerReadMessageV1(
        job.identity, "REQUEST", frame=job.request_frame, deadline_ns=job.deadline_ns
    )


def prepared(
    work, phase: Literal["PREPARED", "GRANT"] = "PREPARED", error=False
) -> OwnerReadMessageV1:
    frame = encode_frame(
        MessageKind.ERROR if error else MessageKind.RESPONSE,
        work.identity.serial,
        {
            "request_id": work.identity.request_id,
            "code": "CURSOR_EXPIRED" if error else "COMPLETED",
        },
    )
    return OwnerReadMessageV1(work.identity, phase, frame, sha256(frame).digest())


def test_prepared_is_not_permission_and_every_partial_send_requires_new_grant(transport):
    pool, pairs, credits = transport
    channel, peer = pairs[0]
    work = request()
    assert pool.offer(work, ConsumerOperationLaneV2.SNAPSHOT)
    assert flush(pool, channel, peer) == work and credits.used == 1
    response = prepared(work)
    assert feed(pool, channel, peer, response) is None
    with pytest.raises(OwnerProtocolError):
        pool.delivery(1)
    check = flush(pool, channel, peer)
    assert check.phase == "CHECK" and check.response_sha256 == response.response_sha256
    grant = OwnerReadMessageV1(work.identity, "GRANT", response_sha256=response.response_sha256)
    assert feed(pool, channel, peer, grant) == 1
    assert pool.delivery(1) == response.frame and credits.used == 1
    pool.recheck(1)
    with pytest.raises(OwnerProtocolError):
        pool.delivery(1)
    assert flush(pool, channel, peer).phase == "CHECK"
    expired = prepared(work, "GRANT", error=True)
    assert feed(pool, channel, peer, expired) == 1
    assert pool.delivery(1) == expired.frame
    pool.dispose(1)
    assert credits.used == 1  # RELEASE still not on inherited wire
    assert flush(pool, channel, peer).phase == "RELEASE"
    assert credits.used == 0
    pool.stop()
    assert pool.shutdown_verified


def test_initial_owner_grant_removes_unobservable_prepare_round_trip(transport):
    pool, pairs, credits = transport
    channel, peer = pairs[0]
    work = request()
    assert pool.offer(work, ConsumerOperationLaneV2.SNAPSHOT)
    assert flush(pool, channel, peer) == work and credits.used == 1
    grant = prepared(work, "GRANT")
    assert feed(pool, channel, peer, grant) == 1
    assert pool.delivery(1) == grant.frame

    # A subsequent public send attempt still loses the old grant and must
    # obtain a current owner authorization before any more bytes are sent.
    pool.recheck(1)
    with pytest.raises(OwnerProtocolError):
        pool.delivery(1)
    check = flush(pool, channel, peer)
    assert check.phase == "CHECK" and check.response_sha256 == grant.response_sha256
    assert feed(
        pool,
        channel,
        peer,
        OwnerReadMessageV1(work.identity, "GRANT", response_sha256=grant.response_sha256),
    ) == 1
    pool.dispose(1)
    assert flush(pool, channel, peer).phase == "RELEASE"
    pool.stop()
    assert pool.shutdown_verified


def test_broker_release_ack_keeps_all_credits_until_released_message():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=2, worker_count=1)
    shared_reads = FrontendSharedCreditV1(context, capacity=1, worker_count=1)
    pairs = [socket.socketpair() for _ in range(4)]
    credits = queue()
    pool = ReactorOwnerReadsV1(
        tuple(pair[0] for pair in pairs),
        ready_queue=credits,
        shared_credit=shared_total,
        shared_read_slots=shared_reads,
        worker_id=0,
        release_ack_required=True,
    )
    try:
        channel, peer = pairs[0]
        work = request()
        assert pool.offer(work, ConsumerOperationLaneV2.SNAPSHOT)
        flush(pool, channel, peer)
        assert feed(pool, channel, peer, prepared(work, "GRANT")) == 1
        pool.dispose(1)
        assert flush(pool, channel, peer).phase == "RELEASE"
        assert credits.used == 1
        assert shared_total.snapshot().held == shared_reads.snapshot().held == 1
        assert feed(
            pool,
            channel,
            peer,
            OwnerReadMessageV1(work.identity, "RELEASED"),
        ) is None
        assert credits.used == 0
        shared_total.require_all_drained()
        shared_reads.require_all_drained()
        pool.stop()
    finally:
        for channel, peer in pairs:
            channel.close()
            peer.close()


def test_direct_frontend_read_routes_replay_and_snapshot_to_dedicated_channels():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=8, worker_count=1)
    shared_reads = FrontendSharedCreditV1(context, capacity=8, worker_count=1)
    replay_channel, replay_peer = socket.socketpair()
    snapshot_channel, snapshot_peer = socket.socketpair()
    credits = queue()
    pool = ReactorOwnerReadsV1(
        (replay_channel, snapshot_channel),
        ready_queue=credits,
        shared_credit=shared_total,
        shared_read_slots=shared_reads,
        worker_id=0,
        frontend_slot_count=2,
        channel_lanes=(
            frozenset({ConsumerOperationLaneV2.REPLAY}),
            frozenset(
                {ConsumerOperationLaneV2.SNAPSHOT, ConsumerOperationLaneV2.OTHER}
            ),
        ),
    )
    try:
        work = request()
        assert pool.offer(work, ConsumerOperationLaneV2.REPLAY)
        assert not pool.events(snapshot_channel) & selectors.EVENT_WRITE
        assert flush(pool, replay_channel, replay_peer) == work
        assert feed(pool, replay_channel, replay_peer, prepared(work, "GRANT")) == 1
        pool.dispose(1)
        assert flush(pool, replay_channel, replay_peer).phase == "RELEASE"

        snapshot = request(2)
        assert pool.offer(snapshot, ConsumerOperationLaneV2.SNAPSHOT)
        assert not pool.events(replay_channel) & selectors.EVENT_WRITE
        assert flush(pool, snapshot_channel, snapshot_peer) == snapshot
        assert feed(pool, snapshot_channel, snapshot_peer, prepared(snapshot, "GRANT")) == 2
        pool.dispose(2)
        assert flush(pool, snapshot_channel, snapshot_peer).phase == "RELEASE"
        assert credits.used == 0
        shared_total.require_all_drained()
        shared_reads.require_all_drained()
        pool.stop()
        assert pool.shutdown_verified
    finally:
        replay_channel.close()
        replay_peer.close()
        snapshot_channel.close()
        snapshot_peer.close()


def test_single_frontend_dual_read_plane_preserves_fixed_lane_partition():
    pairs = [socket.socketpair() for _ in range(OWNER_DIRECT_READ_SLOT_COUNT)]
    lanes = (
        (frozenset({ConsumerOperationLaneV2.REPLAY}),) * OWNER_READ_SLOT_COUNT
        + (
            frozenset(
                {ConsumerOperationLaneV2.SNAPSHOT, ConsumerOperationLaneV2.OTHER}
            ),
        )
        * OWNER_READ_SLOT_COUNT
    )
    pool = ReactorOwnerReadsV1(
        tuple(pair[0] for pair in pairs),
        ready_queue=queue(),
        channel_lanes=lanes,
    )
    try:
        replay = request()
        assert pool.offer(replay, ConsumerOperationLaneV2.REPLAY)
        replay_channel, replay_peer = pairs[0]
        assert flush(pool, replay_channel, replay_peer) == replay
        assert all(
            not pool.events(channel) & selectors.EVENT_WRITE
            for channel, _peer in pairs[OWNER_READ_SLOT_COUNT:]
        )
        assert feed(pool, replay_channel, replay_peer, prepared(replay, "GRANT")) == 1
        pool.dispose(1)
        assert flush(pool, replay_channel, replay_peer).phase == "RELEASE"

        snapshot = request(2)
        assert pool.offer(snapshot, ConsumerOperationLaneV2.SNAPSHOT)
        snapshot_channel, snapshot_peer = pairs[OWNER_READ_SLOT_COUNT]
        assert flush(pool, snapshot_channel, snapshot_peer) == snapshot
        assert all(
            not pool.events(channel) & selectors.EVENT_WRITE
            for channel, _peer in pairs[:OWNER_READ_SLOT_COUNT]
        )
        assert feed(pool, snapshot_channel, snapshot_peer, prepared(snapshot, "GRANT")) == 2
        pool.dispose(2)
        assert flush(pool, snapshot_channel, snapshot_peer).phase == "RELEASE"
        pool.stop()
        assert pool.shutdown_verified
    finally:
        for channel, peer in pairs:
            channel.close()
            peer.close()


def test_frontend_read_slot_count_is_bounded():
    channel, peer = socket.socketpair()
    try:
        with pytest.raises(OwnerProtocolError, match="slot count"):
            ReactorOwnerReadsV1(
                (channel,),
                ready_queue=queue(),
                frontend_slot_count=0,
            )
    finally:
        channel.close()
        peer.close()


def test_disconnected_client_retains_credit_until_known_read_is_released(transport):
    pool, pairs, credits = transport
    channel, peer = pairs[0]
    work = request()
    assert pool.offer(work, ConsumerOperationLaneV2.REPLAY)
    flush(pool, channel, peer)
    pool.dispose(1)
    assert credits.used == 1 and pool.outstanding == 1
    assert feed(pool, channel, peer, prepared(work)) is None
    assert flush(pool, channel, peer).phase == "RELEASE"
    assert credits.used == 0 and pool.outstanding == 0


def test_frontend_read_pools_share_total_and_bounded_slot_budgets_until_release():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=3, worker_count=2)
    shared_reads = FrontendSharedCreditV1(context, capacity=2, worker_count=2)
    pairs = [[socket.socketpair() for _ in range(4)] for _ in range(2)]
    pools = [
        ReactorOwnerReadsV1(
            tuple(pair[0] for pair in worker_pairs),
            ready_queue=queue(),
            shared_credit=shared_total,
            shared_read_slots=shared_reads,
            worker_id=worker_id,
        )
        for worker_id, worker_pairs in enumerate(pairs)
    ]
    try:
        first, second = request(1), request(1)
        assert pools[0].offer(first, ConsumerOperationLaneV2.SNAPSHOT)
        assert pools[1].offer(second, ConsumerOperationLaneV2.REPLAY)
        assert shared_total.snapshot().held_by_worker == (1, 1)
        assert shared_reads.snapshot().held_by_worker == (1, 1)
        assert pools[0].full_for(ConsumerOperationLaneV2.SNAPSHOT)
        assert pools[1].full_for(ConsumerOperationLaneV2.SNAPSHOT)

        channel, peer = pairs[0][0]
        flush(pools[0], channel, peer)
        grant = prepared(first, "GRANT")
        assert feed(pools[0], channel, peer, grant) == 1
        pools[0].dispose(1)
        assert flush(pools[0], channel, peer).phase == "RELEASE"
        assert shared_total.snapshot().held_by_worker == (0, 1)
        assert shared_reads.snapshot().held_by_worker == (0, 1)

        assert pools[0].offer(request(2), ConsumerOperationLaneV2.SNAPSHOT)
        for pool_index, serial in ((0, 2), (1, 1)):
            channel, peer = pairs[pool_index][0]
            work = request(serial)
            flush(pools[pool_index], channel, peer)
            assert feed(pools[pool_index], channel, peer, prepared(work, "GRANT")) == serial
            pools[pool_index].dispose(serial)
            assert flush(pools[pool_index], channel, peer).phase == "RELEASE"
            pools[pool_index].stop()
        shared_total.require_all_drained()
        shared_reads.require_all_drained()
    finally:
        for worker_pairs in pairs:
            for channel, peer in worker_pairs:
                channel.close()
                peer.close()


def test_shared_snapshot_pressure_preserves_existing_replay_service_budget():
    context = mp.get_context("spawn")
    shared_total = FrontendSharedCreditV1(context, capacity=16, worker_count=2)
    shared_reads = FrontendSharedCreditV1(
        context,
        capacity=OWNER_READ_SLOT_COUNT,
        worker_count=2,
    )
    pairs = [[socket.socketpair() for _ in range(4)] for _ in range(2)]
    pools = [
        ReactorOwnerReadsV1(
            tuple(pair[0] for pair in worker_pairs),
            ready_queue=BoundedFairOperationQueueV2(
                total_capacity=4,
                lane_capacities={lane: 4 for lane in ConsumerOperationLaneV2},
            ),
            shared_credit=shared_total,
            shared_read_slots=shared_reads,
            worker_id=worker_id,
        )
        for worker_id, worker_pairs in enumerate(pairs)
    ]
    try:
        for serial in range(1, 5):
            assert pools[0].offer(request(serial), ConsumerOperationLaneV2.SNAPSHOT)
        for serial in range(1, 3):
            assert pools[1].offer(request(serial), ConsumerOperationLaneV2.SNAPSHOT)
        assert shared_reads.snapshot().held == 6
        assert shared_reads.snapshot().restricted_held == 6
        assert pools[1].full_for(ConsumerOperationLaneV2.SNAPSHOT)
        assert not pools[1].offer(request(3), ConsumerOperationLaneV2.SNAPSHOT)
        assert not pools[1].full_for(ConsumerOperationLaneV2.REPLAY)
        assert pools[1].offer(request(3), ConsumerOperationLaneV2.REPLAY)
        assert shared_reads.snapshot().held == 7
        assert shared_reads.snapshot().restricted_held == 6
    finally:
        for pool in pools:
            pool.fail()
        for worker_pairs in pairs:
            for channel, peer in worker_pairs:
                channel.close()
                peer.close()


def test_large_replay_lane_is_bounded_without_reducing_snapshot_slots():
    pairs = [socket.socketpair() for _ in range(OWNER_READ_SLOT_COUNT)]
    credits = BoundedFairOperationQueueV2(
        total_capacity=8,
        lane_capacities={lane: 8 for lane in ConsumerOperationLaneV2},
    )
    pool = ReactorOwnerReadsV1(tuple(pair[0] for pair in pairs), ready_queue=credits)
    try:
        first = request(1)
        assert pool.offer(first, ConsumerOperationLaneV2.REPLAY)
        assert pool.full_for(ConsumerOperationLaneV2.REPLAY)
        assert ConsumerOperationLaneV2.REPLAY in pool.excluded_lanes
        assert ConsumerOperationLaneV2.SNAPSHOT not in pool.excluded_lanes
        assert not pool.offer(request(2), ConsumerOperationLaneV2.REPLAY)

        # The remaining physical rendezvous still borrow the original snapshot
        # credits; no separate business queue exists.
        for serial in range(2, OWNER_READ_SLOT_COUNT + 1):
            assert pool.offer(request(serial), ConsumerOperationLaneV2.SNAPSHOT)
        assert (
            pool.full
            and pool.outstanding == OWNER_READ_SLOT_COUNT
            and credits.used == OWNER_READ_SLOT_COUNT
        )

        channel, peer = pairs[0]
        flush(pool, channel, peer)
        pool.dispose(1)
        assert feed(pool, channel, peer, prepared(first)) is None
        assert flush(pool, channel, peer).phase == "RELEASE"
        assert not pool.full_for(ConsumerOperationLaneV2.REPLAY)
    finally:
        for channel, peer in pairs:
            channel.close()
            peer.close()


@pytest.mark.parametrize("fault", ["identity", "grant_without_response", "crc", "eof"])
def test_bad_peer_reply_fences_all_reads_without_freeing_credit(transport, fault):
    pool, pairs, credits = transport
    channel, peer = pairs[0]
    work = request()
    assert pool.offer(work, ConsumerOperationLaneV2.REPLAY)
    flush(pool, channel, peer)
    with pytest.raises((OwnerProtocolError, OSError)):
        if fault == "identity":
            feed(pool, channel, peer, prepared(request(2)))
        elif fault == "grant_without_response":
            feed(
                pool,
                channel,
                peer,
                OwnerReadMessageV1(work.identity, "GRANT", response_sha256=bytes(32)),
            )
        elif fault == "eof":
            peer.close()
            pool.io(channel, selectors.EVENT_READ)
        else:
            packet = encode_owner_packet(prepared(work))
            packet = packet[:-1] + bytes((packet[-1] ^ 1,))
            peer.sendall(packet)
            for _ in range(3):
                pool.io(channel, selectors.EVENT_READ)
    assert pool.failed and credits.used == 1
    with pytest.raises(OwnerProtocolError):
        pool.stop()
