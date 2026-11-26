from __future__ import annotations

import socket
import time
import unittest
from queue import Queue
from threading import Event, Thread

from xuanling_tradecore.consumer.service_v2 import ConsumerUnixServiceV2, _ReadyConnection
from xuanling_tradecore.consumer.wire_v2 import MessageKind, decode_frame, encode_frame
from xuanling_tradecore.domain import ConsumerPrincipal


class _ControlledListener:
    def __init__(self) -> None:
        self.incoming: Queue[socket.socket] = Queue()
        self.release = Event()
        self.closed = False

    def start(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def accept_connection(self) -> tuple[socket.socket, object]:
        try:
            return self.incoming.get(timeout=0.02), object()
        except Exception as exc:
            raise socket.timeout() from exc

    def serve_authenticated_connection(self, connection: socket.socket, _principal: object) -> None:
        with connection:
            self.release.wait(timeout=3)


class ConsumerUnixServiceV2Tests(unittest.TestCase):
    def test_complete_frame_gets_explicit_pre_dispatch_overload_reply(self) -> None:
        listener = _ControlledListener()
        service = ConsumerUnixServiceV2(listener, worker_count=1, pending_capacity=1)
        service.start()
        runner = Thread(target=service.serve_forever)
        runner.start()
        peers: list[socket.socket] = []
        try:
            for expected in (1, 2):
                server, client = socket.socketpair()
                peers.append(client)
                listener.incoming.put(server)
                deadline = time.monotonic() + 2
                while service.ingress_stats().accepted_connections < expected:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                if expected == 1:
                    while service.ingress_stats().active_connections != 1:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.005)
            server, client = socket.socketpair()
            peers.append(client)
            client.settimeout(2)
            frame = encode_frame(
                MessageKind.REQUEST,
                19,
                {"message_type": "request", "schema_version": 2, "request_id": "overload-1"},
            )
            service._admit_ready(  # pyright: ignore[reportPrivateUsage]
                _ReadyConnection(server, ConsumerPrincipal("consumer", "login", "legal"), frame)
            )
            response = decode_frame(client.recv(4096))
            self.assertEqual(MessageKind.ERROR, response.header.message_kind)
            self.assertEqual(19, response.header.correlation_id)
            self.assertEqual("overload-1", response.payload["request_id"])
            self.assertEqual("RATE_LIMITED", response.payload["code"])
            self.assertEqual("INGRESS_OVERLOADED_PRE_DISPATCH", response.payload["detail"])
            # recv can return before the sender records its post-send counter.
            deadline = time.monotonic() + 2
            while service.ingress_stats().overload_responses_sent != 1:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.005)
            stats = service.ingress_stats()
            self.assertEqual(1, stats.overloaded_connections)
            self.assertEqual(1, stats.overload_responses_sent)
        finally:
            listener.release.set()
            service.stop()
            runner.join(timeout=3)
            for peer in peers:
                peer.close()
        self.assertFalse(runner.is_alive())

    def test_pending_queue_is_bounded_and_overflow_is_closed(self) -> None:
        listener = _ControlledListener()
        service = ConsumerUnixServiceV2(listener, worker_count=1, pending_capacity=1)
        service.start()
        runner = Thread(target=service.serve_forever)
        runner.start()
        peers: list[socket.socket] = []
        try:
            for expected in (1, 2, 3):
                server, client = socket.socketpair()
                peers.append(client)
                listener.incoming.put(server)
                deadline = time.monotonic() + 2
                while (
                    service.ingress_stats().accepted_connections
                    + service.ingress_stats().overloaded_connections
                    < expected
                ):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                if expected == 1:
                    deadline = time.monotonic() + 2
                    while service.ingress_stats().active_connections != 1:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.005)
            stats = service.ingress_stats()
            self.assertEqual(2, stats.accepted_connections)
            self.assertEqual(1, stats.overloaded_connections)
            self.assertEqual(1, stats.pending_connections)
            self.assertEqual(1, stats.active_connections)
        finally:
            listener.release.set()
            service.stop()
            runner.join(timeout=3)
            for peer in peers:
                peer.close()
        self.assertFalse(runner.is_alive())


if __name__ == "__main__":
    unittest.main()
