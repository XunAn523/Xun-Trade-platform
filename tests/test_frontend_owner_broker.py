import multiprocessing as mp
import socket
from time import monotonic_ns

from xuanling_tradecore.consumer.frontend_owner_broker import FrontendOwnerWriteBrokerV1
from xuanling_tradecore.consumer.frontend_shared_credit import FrontendSharedCreditV1
from xuanling_tradecore.consumer.owner_ipc_v1 import (
    OwnerEventV1,
    OwnerRequestV1,
    encode_owner_packet,
    receive_owner_packet,
)
from xuanling_tradecore.consumer.owner_reactor_ipc import ReactorOwnerPortV1
from xuanling_tradecore.consumer.wire_v2 import MessageKind, decode_frame, encode_frame

from test_owner_ipc_v1 import ready
from test_owner_reactor_ipc import command, queue


def _flush(port: ReactorOwnerPortV1[object]) -> None:
    while port.wants_write:
        port.write_ready()


def _flush_broker(broker: FrontendOwnerWriteBrokerV1[object], worker_id: int) -> None:
    while broker.wants_frontend_write(worker_id):
        broker.write_frontend(worker_id)


def _completed_for_remapped(request: OwnerRequestV1) -> OwnerEventV1:
    original = decode_frame(request.request_frame)
    response = encode_frame(
        MessageKind.RESPONSE,
        original.header.correlation_id,
        {
            "request_id": request.identity.request_id,
            "payload": {
                "journal_id": ready().journal_id,
                "journal_lsn": 12,
                "consumer_request_id": request.identity.request_id,
            },
        },
    )
    return OwnerEventV1(
        request.identity,
        "COMPLETED",
        monotonic_ns(),
        response_frame=response,
        journal_id=ready().journal_id,
        source_lsn=12,
        source_prefix_sha256=ready().source_prefix_sha256,
        source_anchor="RECOVERED_PREFIX",
    )


def test_two_frontends_get_unique_owner_serials_and_original_terminal_identity():
    shared = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=3, worker_count=2)
    frontend_pairs = [socket.socketpair(), socket.socketpair()]
    owner_parent, owner_peer = socket.socketpair()
    central = ReactorOwnerPortV1(owner_parent, ready=ready(), ready_queue=queue())
    workers = [
        ReactorOwnerPortV1(
            pair[0],
            ready=ready(),
            ready_queue=queue(),
            shared_credit=shared,
            worker_id=index,
        )
        for index, pair in enumerate(frontend_pairs)
    ]
    broker = FrontendOwnerWriteBrokerV1(
        tuple(pair[1] for pair in frontend_pairs), owner=central, shared_credit=shared
    )
    try:
        original = (command(1), command(1))
        for worker, request in zip(workers, original, strict=True):
            assert worker.offer(request)
            _flush(worker)

        broker.begin_turn()
        assert broker.read_frontend(0)
        assert broker.read_frontend(1)
        broker.write_owner()
        remapped = tuple(
            receive_owner_packet(owner_peer, deadline_ns=monotonic_ns() + 1_000_000_000)
            for _ in range(2)
        )
        assert all(type(item) is OwnerRequestV1 for item in remapped)
        assert tuple(item.identity.serial for item in remapped) == (1, 2)
        assert tuple(item.identity.request_id for item in remapped) == (
            original[0].identity.request_id,
            original[1].identity.request_id,
        )

        for worker_id, (worker, central_request) in enumerate(
            zip(workers, remapped, strict=True)
        ):
            assert type(central_request) is OwnerRequestV1
            started = OwnerEventV1(central_request.identity, "STARTED", monotonic_ns())
            terminal = _completed_for_remapped(central_request)
            owner_peer.sendall(encode_owner_packet(started) + encode_owner_packet(terminal))
            assert broker.read_owner() == 2
            _flush_broker(broker, worker_id)
            result = worker.read_ready()
            assert result is not None
            assert result.identity == original[worker_id].identity
            assert result.phase == "COMPLETED"
            worker.dispose(1)

        assert broker.outstanding == 0
        for worker in workers:
            worker.stop()
        broker.stop()
        assert broker.shutdown_verified
    finally:
        for pair in frontend_pairs:
            pair[0].close()
            pair[1].close()
        owner_parent.close()
        owner_peer.close()


def test_frontend_channel_loss_fences_owner_and_never_reclaims_shared_credit():
    shared = FrontendSharedCreditV1(mp.get_context("spawn"), capacity=2, worker_count=1)
    frontend, broker_channel = socket.socketpair()
    owner_parent, owner_peer = socket.socketpair()
    central = ReactorOwnerPortV1(owner_parent, ready=ready(), ready_queue=queue())
    worker = ReactorOwnerPortV1(
        frontend,
        ready=ready(),
        ready_queue=queue(),
        shared_credit=shared,
        worker_id=0,
    )
    broker = FrontendOwnerWriteBrokerV1(
        (broker_channel,), owner=central, shared_credit=shared
    )
    try:
        assert worker.offer(command())
        _flush(worker)
        broker.begin_turn()
        assert broker.read_frontend(0)
        broker_channel.close()
        try:
            broker.read_frontend(0)
        except Exception:
            pass
        snapshot = shared.snapshot()
        assert broker.failed and central.failed and snapshot.fenced and snapshot.held == 1
    finally:
        frontend.close()
        broker_channel.close()
        owner_parent.close()
        owner_peer.close()
