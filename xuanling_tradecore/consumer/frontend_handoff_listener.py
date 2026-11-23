"""Frontend-side listener facade for sockets received from the sole acceptor."""

from __future__ import annotations

import socket
from pathlib import Path
from typing import Callable, Mapping

from ..domain import ConsumerPrincipal
from .frontend_fd_handoff import (
    FrontendFdAckV1,
    FrontendFdHandoffError,
    FrontendFdHandoffV1,
    receive_frontend_fd,
    send_frontend_fd_ack,
)
from .gateway_v2 import ConsumerGatewayV2
from .unix_v2 import (
    ConsumerUnixPeerV2,
    ConsumerUnixRequestHandlerV2,
    ConsumerUnixSocketListenerV2,
)


class ConsumerHandoffSocketListenerV2(ConsumerUnixSocketListenerV2):
    """Behave like a listener while receiving already-accepted stream FDs.

    The packet channel is inherited from the trusted single acceptor.  Public
    authentication is deliberately repeated in this process.  An ACK is sent
    only when ``ConsumerUnixServiceV2`` confirms that the connection entered
    its owned lifecycle (selector/ready state or explicit overload handling).
    """

    def __init__(
        self,
        *,
        handoff_channel: socket.socket,
        generation: str,
        socket_path: Path,
        gateway: ConsumerGatewayV2,
        principal_for_peer: Callable[[ConsumerUnixPeerV2], ConsumerPrincipal],
        request_schema: Mapping[str, object],
        request_handler: ConsumerUnixRequestHandlerV2,
        connection_timeout_seconds: float = 5.0,
        max_requests_per_connection: int = 1_024,
        socket_mode: int = 0o600,
    ) -> None:
        # The path is contract identity only in this worker.  start/close never
        # bind, chmod, unlink, or otherwise claim the acceptor-owned pathname.
        super().__init__(
            socket_path=socket_path,
            gateway=gateway,
            principal_for_peer=principal_for_peer,
            request_schema=request_schema,
            request_handler=request_handler,
            connection_timeout_seconds=connection_timeout_seconds,
            max_requests_per_connection=max_requests_per_connection,
            socket_mode=socket_mode,
        )
        FrontendFdAckV1(generation, 1)
        self._handoff_channel = handoff_channel
        self._handoff_generation = generation
        self._handoff_started = False
        self._pending_install: dict[socket.socket, FrontendFdHandoffV1] = {}

    def start(self) -> None:
        if self._handoff_started or self._handoff_channel.fileno() < 0:
            raise FrontendFdHandoffError("frontend handoff listener cannot start")
        self._handoff_started = True

    def close(self) -> None:
        self._handoff_started = False
        for connection in tuple(self._pending_install):
            connection.close()
        self._pending_install.clear()
        self._handoff_channel.close()

    @property
    def listening_socket(self) -> socket.socket:
        if not self._handoff_started:
            raise FrontendFdHandoffError("frontend handoff listener is not running")
        return self._handoff_channel

    def accept_connection(self) -> tuple[socket.socket, ConsumerPrincipal]:
        if not self._handoff_started:
            raise FrontendFdHandoffError("frontend handoff listener is not running")
        connection, message = receive_frontend_fd(self._handoff_channel)
        try:
            if message.generation != self._handoff_generation:
                raise FrontendFdHandoffError("frontend received a stale generation")
            # receive_frontend_fd already compared this value with the FD.  The
            # callback repeats the same allow-list decision as the acceptor.
            principal = self._principal_for_peer(message.peer)  # pyright: ignore[reportPrivateUsage]
        except BaseException:
            connection.close()
            raise
        self._pending_install[connection] = message
        return connection, principal

    def accepted_connection_installed(self, connection: socket.socket) -> None:
        message = self._pending_install.pop(connection, None)
        if message is None:
            raise FrontendFdHandoffError("frontend installed an unknown connection")
        try:
            send_frontend_fd_ack(
                self._handoff_channel,
                FrontendFdAckV1(message.generation, message.serial),
            )
        except BaseException:
            connection.close()
            raise
