import array
import os
import socket
import zlib
from pathlib import Path
from time import monotonic_ns

import pytest
from xuanling_tradecore.consumer.dispatch_v2 import ConsumerWireDispatcherV2
from xuanling_tradecore.consumer.frontend_fd_handoff import (
    FRONTEND_FD_HANDOFF_BYTES,
    BoundedFrontendFdRouterV1,
    FrontendFdAckV1,
    FrontendFdHandoffError,
    FrontendFdHandoffV1,
    create_frontend_fd_handoff_pair,
    decode_frontend_fd_handoff,
    encode_frontend_fd_handoff,
    receive_frontend_fd,
    receive_frontend_fd_ack,
    send_frontend_fd,
    send_frontend_fd_ack,
)
from xuanling_tradecore.consumer.frontend_handoff_listener import (
    ConsumerHandoffSocketListenerV2,
)
from xuanling_tradecore.consumer.gateway_v2 import ConsumerGatewayV2
from xuanling_tradecore.consumer.unix_v2 import (
    ConsumerUnixPeerV2,
    read_linux_peer_credentials,
)

GENERATION = "a" * 32


def handoff(**changes):
    values = dict(
        generation=GENERATION,
        serial=1,
        accepted_monotonic_ns=monotonic_ns(),
        peer=ConsumerUnixPeerV2(123, 456, 789),
    )
    values.update(changes)
    return FrontendFdHandoffV1(**values)


def test_fixed_envelope_round_trip_and_strict_corruption():
    message = handoff()
    packet = encode_frontend_fd_handoff(message)
    assert len(packet) == FRONTEND_FD_HANDOFF_BYTES
    assert decode_frontend_fd_handoff(packet) == message

    corrupt = packet[:-1] + bytes((packet[-1] ^ 1,))
    with pytest.raises(FrontendFdHandoffError, match="checksum"):
        decode_frontend_fd_handoff(corrupt)
    with pytest.raises(FrontendFdHandoffError, match="length"):
        decode_frontend_fd_handoff(packet + b"x")
    body = b"BAD!" + packet[4:-4]
    unknown = body + zlib.crc32(body).to_bytes(4, "big")
    with pytest.raises(FrontendFdHandoffError, match="unsupported"):
        decode_frontend_fd_handoff(unknown)


@pytest.mark.parametrize(
    "changes",
    (
        {"generation": "A" * 32},
        {"serial": 0},
        {"accepted_monotonic_ns": 0},
        {"peer": ConsumerUnixPeerV2(0, 1, 1)},
    ),
)
def test_invalid_attribution_is_rejected(changes):
    with pytest.raises(FrontendFdHandoffError):
        handoff(**changes)


requires_rights = pytest.mark.skipif(
    not hasattr(socket, "SCM_RIGHTS") or not hasattr(socket, "SO_PEERCRED"),
    reason="requires Linux SCM_RIGHTS and SO_PEERCRED",
)


@requires_rights
def test_transferred_socket_preserves_peer_and_bytes():
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    sender, receiver = create_frontend_fd_handoff_pair()
    try:
        peer = read_linux_peer_credentials(public)
        message = handoff(peer=peer)
        send_frontend_fd(sender, public, message)
        public.close()
        transferred, observed = receive_frontend_fd(receiver)
        try:
            assert observed == message
            assert read_linux_peer_credentials(transferred) == peer
            client.sendall(b"request")
            assert transferred.recv(7) == b"request"
            transferred.sendall(b"response")
            assert client.recv(8) == b"response"
        finally:
            transferred.close()
    finally:
        public.close()
        client.close()
        sender.close()
        receiver.close()


@requires_rights
def test_sender_refuses_a_forged_peer():
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    sender, receiver = create_frontend_fd_handoff_pair()
    try:
        actual = read_linux_peer_credentials(public)
        forged = ConsumerUnixPeerV2(actual.pid, actual.uid, actual.gid + 1)
        with pytest.raises(FrontendFdHandoffError, match="attribution changed"):
            send_frontend_fd(sender, public, handoff(peer=forged))
        assert public.fileno() >= 0
    finally:
        public.close()
        client.close()
        sender.close()
        receiver.close()


@requires_rights
def test_receiver_rejects_extra_descriptors_without_leaking_them():
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    extra, extra_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    sender, receiver = create_frontend_fd_handoff_pair()
    before = len(os.listdir("/proc/self/fd"))
    try:
        packet = encode_frontend_fd_handoff(handoff(peer=read_linux_peer_credentials(public)))
        rights = array.array("i", (public.fileno(), extra.fileno()))
        sender.sendmsg([packet], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
        with pytest.raises(FrontendFdHandoffError, match="truncated|exactly one"):
            receive_frontend_fd(receiver)
        assert len(os.listdir("/proc/self/fd")) == before
    finally:
        public.close()
        client.close()
        extra.close()
        extra_peer.close()
        sender.close()
        receiver.close()


@requires_rights
def test_receiver_rejects_corrupt_envelope_and_closes_received_descriptor():
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    sender, receiver = create_frontend_fd_handoff_pair()
    before = len(os.listdir("/proc/self/fd"))
    try:
        packet = encode_frontend_fd_handoff(handoff(peer=read_linux_peer_credentials(public)))
        corrupt = packet[:-1] + bytes((packet[-1] ^ 1,))
        rights = array.array("i", (public.fileno(),))
        sender.sendmsg([corrupt], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
        with pytest.raises(FrontendFdHandoffError, match="checksum"):
            receive_frontend_fd(receiver)
        assert len(os.listdir("/proc/self/fd")) == before
    finally:
        public.close()
        client.close()
        sender.close()
        receiver.close()


@requires_rights
def test_ack_is_exact_and_carries_no_descriptor():
    sender, receiver = create_frontend_fd_handoff_pair()
    try:
        ack = FrontendFdAckV1(GENERATION, 42)
        send_frontend_fd_ack(sender, ack)
        assert receive_frontend_fd_ack(receiver) == ack
    finally:
        sender.close()
        receiver.close()


@requires_rights
def test_empty_nonblocking_handoff_is_normal_accept_queue_exhaustion():
    sender, receiver = create_frontend_fd_handoff_pair()
    try:
        receiver.setblocking(False)
        with pytest.raises(BlockingIOError):
            receive_frontend_fd(receiver)
    finally:
        sender.close()
        receiver.close()


@requires_rights
def test_router_has_one_slot_per_worker_and_reuses_only_after_exact_ack():
    acceptor_channels = []
    worker_channels = []
    public_clients = []
    try:
        for _ in range(2):
            acceptor, worker = create_frontend_fd_handoff_pair()
            acceptor_channels.append(acceptor)
            worker_channels.append(worker)
        router = BoundedFrontendFdRouterV1(tuple(acceptor_channels), generation=GENERATION)
        assert router.available_capacity == 2

        offered = []
        for serial in (1, 2):
            public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            public_clients.append(client)
            offered.append(
                router.offer(
                    public,
                    peer=read_linux_peer_credentials(public),
                    accepted_monotonic_ns=monotonic_ns(),
                )
            )
            assert public.fileno() == -1
            assert offered[-1] is not None and offered[-1].serial == serial
            assert router.available_capacity == 2 - serial

        blocked, blocked_client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        public_clients.append(blocked_client)
        assert (
            router.offer(
                blocked,
                peer=read_linux_peer_credentials(blocked),
                accepted_monotonic_ns=monotonic_ns(),
            )
            is None
        )
        # A backpressured FD is still caller-owned and was never silently dropped.
        assert blocked.fileno() >= 0
        blocked.close()

        moved, first = receive_frontend_fd(worker_channels[0])
        moved.close()
        send_frontend_fd_ack(worker_channels[0], FrontendFdAckV1(first.generation, first.serial))
        assert router.acknowledge(acceptor_channels[0]).serial == 1
        assert router.available_capacity == 1

        next_public, next_client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        public_clients.append(next_client)
        third = router.offer(
            next_public,
            peer=read_linux_peer_credentials(next_public),
            accepted_monotonic_ns=monotonic_ns(),
        )
        assert third is not None and third.serial == 3
        assert router.available_capacity == 0
        stats = router.stats()
        assert stats.handed_off == 3
        assert stats.installed == 1
        assert stats.backpressured == 1
        assert stats.outstanding_handoffs == 2
        assert not stats.failed
    finally:
        for channel in (*acceptor_channels, *worker_channels, *public_clients):
            channel.close()


@requires_rights
def test_router_bounded_window_round_robins_and_requires_ordered_ack():
    acceptors = []
    workers = []
    clients = []
    try:
        for _ in range(2):
            acceptor, worker = create_frontend_fd_handoff_pair()
            acceptors.append(acceptor)
            workers.append(worker)
        router = BoundedFrontendFdRouterV1(
            tuple(acceptors), generation=GENERATION, max_outstanding_per_worker=2
        )
        for expected in range(1, 5):
            public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            clients.append(client)
            sent = router.offer(
                public,
                peer=read_linux_peer_credentials(public),
                accepted_monotonic_ns=monotonic_ns(),
            )
            assert sent is not None and sent.serial == expected

        blocked, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        clients.append(client)
        assert (
            router.offer(
                blocked,
                peer=read_linux_peer_credentials(blocked),
                accepted_monotonic_ns=monotonic_ns(),
            )
            is None
        )
        assert blocked.fileno() >= 0
        blocked.close()
        stats = router.stats()
        assert stats.per_worker_capacity == 2
        assert stats.total_capacity == 4
        assert stats.outstanding_handoffs == 4
        assert stats.idle_workers == 0

        received = []
        for worker in workers:
            first_socket, first = receive_frontend_fd(worker)
            second_socket, second = receive_frontend_fd(worker)
            first_socket.close()
            second_socket.close()
            received.append((first, second))
        # Each complete worker cycle advances the next cycle's phase, so an
        # alternating upstream request pattern cannot permanently split by
        # worker parity.
        assert [[item.serial for item in pair] for pair in received] == [[1, 4], [2, 3]]

        for acceptor, worker_messages, worker in zip(acceptors, received, workers):
            for message in worker_messages:
                send_frontend_fd_ack(
                    worker, FrontendFdAckV1(message.generation, message.serial)
                )
                assert router.acknowledge(acceptor).serial == message.serial
        assert router.stats().outstanding_handoffs == 0
        assert router.stats().idle_workers == 2
    finally:
        for channel in (*acceptors, *workers, *clients):
            channel.close()


@requires_rights
def test_wrong_ack_fences_router():
    acceptor, worker = create_frontend_fd_handoff_pair()
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        router = BoundedFrontendFdRouterV1((acceptor,), generation=GENERATION)
        sent = router.offer(
            public,
            peer=read_linux_peer_credentials(public),
            accepted_monotonic_ns=monotonic_ns(),
        )
        assert sent is not None
        moved, received = receive_frontend_fd(worker)
        moved.close()
        send_frontend_fd_ack(worker, FrontendFdAckV1(received.generation, received.serial + 1))
        with pytest.raises(FrontendFdHandoffError, match="does not match"):
            router.acknowledge(acceptor)
        assert router.failed
    finally:
        public.close()
        client.close()
        acceptor.close()
        worker.close()


@requires_rights
def test_duplicate_ack_is_consumed_and_reports_bounded_router_state():
    acceptor, worker = create_frontend_fd_handoff_pair()
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        router = BoundedFrontendFdRouterV1((acceptor,), generation=GENERATION)
        sent = router.offer(
            public,
            peer=read_linux_peer_credentials(public),
            accepted_monotonic_ns=monotonic_ns(),
        )
        assert sent is not None
        moved, received = receive_frontend_fd(worker)
        moved.close()
        ack = FrontendFdAckV1(received.generation, received.serial)
        send_frontend_fd_ack(worker, ack)
        assert router.acknowledge(acceptor) == ack
        send_frontend_fd_ack(worker, ack)
        with pytest.raises(
            FrontendFdHandoffError,
            match=r"serial=1, handed_off=1, installed=1",
        ):
            router.acknowledge(acceptor)
        assert router.failed
    finally:
        public.close()
        client.close()
        acceptor.close()
        worker.close()


@requires_rights
def test_handoff_listener_reauthenticates_and_acks_only_after_install():
    acceptor, worker = create_frontend_fd_handoff_pair()
    public, client = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    actual = read_linux_peer_credentials(public)
    principal = object()
    observed = []

    def authorize(peer):
        observed.append(peer)
        return principal

    listener = ConsumerHandoffSocketListenerV2(
        handoff_channel=worker,
        generation=GENERATION,
        socket_path=Path("/run/tradecore-test.sock"),
        gateway=ConsumerGatewayV2.__new__(ConsumerGatewayV2),
        principal_for_peer=authorize,
        request_schema={},
        request_handler=ConsumerWireDispatcherV2.__new__(ConsumerWireDispatcherV2),
    )
    try:
        router = BoundedFrontendFdRouterV1((acceptor,), generation=GENERATION)
        sent = router.offer(
            public,
            peer=actual,
            accepted_monotonic_ns=monotonic_ns(),
        )
        assert sent is not None
        listener.start()
        moved, accepted_principal = listener.accept_connection()
        assert accepted_principal is principal
        assert observed == [actual]
        assert router.stats().outstanding_handoffs == 1
        listener.accepted_connection_installed(moved)
        assert router.acknowledge(acceptor).serial == sent.serial
        assert router.stats().outstanding_handoffs == 0
        moved.close()
    finally:
        public.close()
        client.close()
        listener.close()
        acceptor.close()
