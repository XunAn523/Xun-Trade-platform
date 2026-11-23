"""Strict Linux accepted-socket handoff for the multi-frontend candidate.

The public listener remains unique.  It authenticates the peer and transfers
the still-unread connected socket to exactly one fixed frontend process over an
inherited ``AF_UNIX/SOCK_SEQPACKET`` channel.  The receiver independently
re-reads ``SO_PEERCRED`` before it may admit any business bytes.

This is an internal process boundary, not a public protocol.  It deliberately
does not pickle objects, buffer accepted descriptors, or infer success from a
partial/unknown ancillary message.
"""

from __future__ import annotations

import array
import os
import re
import socket
import struct
import zlib
from collections import deque
from dataclasses import dataclass
from typing import Generic, TypeVar

from .unix_v2 import ConsumerUnixPeerV2, read_linux_peer_credentials

_MAGIC = b"TCF1"
_VERSION = 1
_GENERATION = re.compile(r"[0-9a-f]{32}\Z")
_BODY = struct.Struct("!4sB3x16sQQiII")
_CRC = struct.Struct("!I")
FRONTEND_FD_HANDOFF_BYTES = _BODY.size + _CRC.size
_ACK_MAGIC = b"TCA1"
_ACK_BODY = struct.Struct("!4sB3x16sQ")
FRONTEND_FD_ACK_BYTES = _ACK_BODY.size + _CRC.size
_INT_BYTES = array.array("i").itemsize
_U64_MAX = (1 << 64) - 1
_U32_MAX = (1 << 32) - 1


class FrontendFdHandoffError(ValueError):
    """An accepted socket could not cross the trusted process boundary."""


@dataclass(frozen=True)
class FrontendFdHandoffV1:
    """Immutable attribution accompanying one accepted public connection."""

    generation: str
    serial: int
    accepted_monotonic_ns: int
    peer: ConsumerUnixPeerV2

    def __post_init__(self) -> None:
        if type(self.generation) is not str or not _GENERATION.fullmatch(self.generation):
            raise FrontendFdHandoffError("invalid frontend handoff generation")
        if type(self.serial) is not int or not 1 <= self.serial <= _U64_MAX:
            raise FrontendFdHandoffError("invalid frontend handoff serial")
        if (
            type(self.accepted_monotonic_ns) is not int
            or not 1 <= self.accepted_monotonic_ns <= _U64_MAX
        ):
            raise FrontendFdHandoffError("invalid frontend handoff timestamp")
        if type(self.peer) is not ConsumerUnixPeerV2:
            raise FrontendFdHandoffError("invalid frontend handoff peer")
        if self.peer.pid <= 0 or self.peer.pid > 2**31 - 1:
            raise FrontendFdHandoffError("invalid frontend handoff peer pid")
        if self.peer.uid > _U32_MAX or self.peer.gid > _U32_MAX:
            raise FrontendFdHandoffError("invalid frontend handoff peer identity")


@dataclass(frozen=True)
class FrontendFdAckV1:
    """A frontend has installed the exact FD in its owned selector."""

    generation: str
    serial: int

    def __post_init__(self) -> None:
        if type(self.generation) is not str or not _GENERATION.fullmatch(self.generation):
            raise FrontendFdHandoffError("invalid frontend ACK generation")
        if type(self.serial) is not int or not 1 <= self.serial <= _U64_MAX:
            raise FrontendFdHandoffError("invalid frontend ACK serial")


@dataclass(frozen=True)
class FrontendFdRouterStatsV1:
    worker_count: int
    idle_workers: int
    per_worker_capacity: int
    total_capacity: int
    outstanding_handoffs: int
    handed_off: int
    installed: int
    backpressured: int
    failed: bool


def encode_frontend_fd_handoff(message: FrontendFdHandoffV1) -> bytes:
    """Encode a fixed-size envelope; the connected socket travels separately."""

    if type(message) is not FrontendFdHandoffV1:
        raise FrontendFdHandoffError("invalid frontend handoff message")
    body = _BODY.pack(
        _MAGIC,
        _VERSION,
        bytes.fromhex(message.generation),
        message.serial,
        message.accepted_monotonic_ns,
        message.peer.pid,
        message.peer.uid,
        message.peer.gid,
    )
    return body + _CRC.pack(zlib.crc32(body))


def decode_frontend_fd_handoff(packet: bytes) -> FrontendFdHandoffV1:
    """Decode only the exact v1 envelope; trailing or truncated bytes fail."""

    if type(packet) is not bytes or len(packet) != FRONTEND_FD_HANDOFF_BYTES:
        raise FrontendFdHandoffError("invalid frontend handoff packet length")
    body, encoded_crc = packet[: -_CRC.size], packet[-_CRC.size :]
    if zlib.crc32(body) != _CRC.unpack(encoded_crc)[0]:
        raise FrontendFdHandoffError("frontend handoff checksum mismatch")
    magic, version, generation, serial, accepted_ns, pid, uid, gid = _BODY.unpack(body)
    if magic != _MAGIC or version != _VERSION:
        raise FrontendFdHandoffError("unsupported frontend handoff protocol")
    return FrontendFdHandoffV1(
        generation=generation.hex(),
        serial=serial,
        accepted_monotonic_ns=accepted_ns,
        peer=ConsumerUnixPeerV2(pid, uid, gid),
    )


def encode_frontend_fd_ack(message: FrontendFdAckV1) -> bytes:
    if type(message) is not FrontendFdAckV1:
        raise FrontendFdHandoffError("invalid frontend ACK message")
    body = _ACK_BODY.pack(
        _ACK_MAGIC,
        _VERSION,
        bytes.fromhex(message.generation),
        message.serial,
    )
    return body + _CRC.pack(zlib.crc32(body))


def decode_frontend_fd_ack(packet: bytes) -> FrontendFdAckV1:
    if type(packet) is not bytes or len(packet) != FRONTEND_FD_ACK_BYTES:
        raise FrontendFdHandoffError("invalid frontend ACK packet length")
    body, encoded_crc = packet[: -_CRC.size], packet[-_CRC.size :]
    if zlib.crc32(body) != _CRC.unpack(encoded_crc)[0]:
        raise FrontendFdHandoffError("frontend ACK checksum mismatch")
    magic, version, generation, serial = _ACK_BODY.unpack(body)
    if magic != _ACK_MAGIC or version != _VERSION:
        raise FrontendFdHandoffError("unsupported frontend ACK protocol")
    return FrontendFdAckV1(generation.hex(), serial)


def _require_seqpacket(channel: socket.socket) -> None:
    if (
        not hasattr(socket, "SCM_RIGHTS")
        or not hasattr(channel, "sendmsg")
        or not hasattr(channel, "recvmsg")
        or channel.family != socket.AF_UNIX
        or channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_SEQPACKET
    ):
        raise FrontendFdHandoffError(
            "frontend FD handoff requires Linux AF_UNIX/SOCK_SEQPACKET SCM_RIGHTS"
        )


def _require_public_stream(connection: socket.socket) -> ConsumerUnixPeerV2:
    if (
        connection.family != socket.AF_UNIX
        or connection.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM
    ):
        raise FrontendFdHandoffError("handoff descriptor is not an AF_UNIX stream")
    return read_linux_peer_credentials(connection)


def create_frontend_fd_handoff_pair() -> tuple[socket.socket, socket.socket]:
    """Create one private packet-preserving channel with no ambient inheritance."""

    if not hasattr(socket, "SCM_RIGHTS") or not hasattr(socket, "SOCK_SEQPACKET"):
        raise FrontendFdHandoffError("frontend FD handoff is unavailable on this host")
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    sender.set_inheritable(False)
    receiver.set_inheritable(False)
    return sender, receiver


def send_frontend_fd(
    channel: socket.socket,
    connection: socket.socket,
    message: FrontendFdHandoffV1,
) -> None:
    """Transfer one verified stream FD in one atomic packet.

    Ownership of ``connection`` remains with the caller.  The caller closes it
    only after this function succeeds; on failure no transfer is claimed.
    """

    _require_seqpacket(channel)
    if _require_public_stream(connection) != message.peer:
        raise FrontendFdHandoffError("frontend handoff peer attribution changed")
    rights = array.array("i", (connection.fileno(),))
    flags = getattr(socket, "MSG_NOSIGNAL", 0)
    try:
        sent = channel.sendmsg(
            [encode_frontend_fd_handoff(message)],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)],
            flags,
        )
    except OSError as exc:
        raise FrontendFdHandoffError("frontend FD handoff send failed") from exc
    if sent != FRONTEND_FD_HANDOFF_BYTES:
        raise FrontendFdHandoffError("frontend FD handoff was not atomic")


def _received_fds(ancillary: list[tuple[int, int, bytes]]) -> list[int]:
    descriptors: list[int] = []
    try:
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS or len(data) % _INT_BYTES:
                raise FrontendFdHandoffError("unexpected frontend handoff ancillary data")
            values = array.array("i")
            values.frombytes(data)
            descriptors.extend(values)
        return descriptors
    except BaseException:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def receive_frontend_fd(
    channel: socket.socket,
) -> tuple[socket.socket, FrontendFdHandoffV1]:
    """Receive one socket, verify its original peer again, and assume ownership."""

    _require_seqpacket(channel)
    descriptors: list[int] = []
    try:
        packet, ancillary, flags, _address = channel.recvmsg(
            FRONTEND_FD_HANDOFF_BYTES,
            socket.CMSG_SPACE(_INT_BYTES),
        )
        descriptors = _received_fds(ancillary)
        if not packet:
            raise FrontendFdHandoffError("frontend FD handoff channel closed")
        if flags & (getattr(socket, "MSG_TRUNC", 0) | getattr(socket, "MSG_CTRUNC", 0)):
            raise FrontendFdHandoffError("frontend FD handoff packet was truncated")
        if len(descriptors) != 1:
            raise FrontendFdHandoffError("frontend FD handoff requires exactly one descriptor")
        message = decode_frontend_fd_handoff(packet)
        descriptor = descriptors.pop()
        connection = socket.socket(fileno=descriptor)
        try:
            connection.set_inheritable(False)
            if _require_public_stream(connection) != message.peer:
                raise FrontendFdHandoffError("received frontend peer attribution changed")
        except BaseException:
            connection.close()
            raise
        return connection, message
    except (FrontendFdHandoffError, BlockingIOError, InterruptedError):
        raise
    except OSError as exc:
        raise FrontendFdHandoffError("frontend FD handoff receive failed") from exc
    finally:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


def send_frontend_fd_ack(channel: socket.socket, message: FrontendFdAckV1) -> None:
    """Acknowledge only after the receiver owns and has installed the socket."""

    _require_seqpacket(channel)
    try:
        sent = channel.send(encode_frontend_fd_ack(message), getattr(socket, "MSG_NOSIGNAL", 0))
    except OSError as exc:
        raise FrontendFdHandoffError("frontend ACK send failed") from exc
    if sent != FRONTEND_FD_ACK_BYTES:
        raise FrontendFdHandoffError("frontend ACK was not atomic")


def receive_frontend_fd_ack(channel: socket.socket) -> FrontendFdAckV1:
    """Receive one ACK and reject any descriptor or ancillary extension."""

    _require_seqpacket(channel)
    descriptors: list[int] = []
    try:
        packet, ancillary, flags, _address = channel.recvmsg(
            FRONTEND_FD_ACK_BYTES,
            socket.CMSG_SPACE(_INT_BYTES),
        )
        descriptors = _received_fds(ancillary)
        if not packet:
            raise FrontendFdHandoffError("frontend ACK channel closed")
        if flags & (getattr(socket, "MSG_TRUNC", 0) | getattr(socket, "MSG_CTRUNC", 0)):
            raise FrontendFdHandoffError("frontend ACK packet was truncated")
        if descriptors:
            raise FrontendFdHandoffError("frontend ACK must not carry descriptors")
        return decode_frontend_fd_ack(packet)
    except (FrontendFdHandoffError, BlockingIOError, InterruptedError):
        raise
    except OSError as exc:
        raise FrontendFdHandoffError("frontend ACK receive failed") from exc
    finally:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


_ChannelT = TypeVar("_ChannelT", bound=socket.socket)


class BoundedFrontendFdRouterV1(Generic[_ChannelT]):
    """Round-robin bounded handoff windows between acceptor and frontends.

    The window contains only descriptors which have already been transferred
    into a worker's kernel ``SOCK_SEQPACKET`` receive queue.  It is not an
    unbounded Python connection queue.  ACKs remain strictly ordered per
    worker; a missing, duplicate or reordered ACK fences the router.
    """

    def __init__(
        self,
        channels: tuple[_ChannelT, ...],
        *,
        generation: str,
        max_outstanding_per_worker: int = 1,
    ) -> None:
        if (
            not channels
            or len(channels) > 64
            or len({channel.fileno() for channel in channels}) != len(channels)
        ):
            raise FrontendFdHandoffError("frontend router requires 1..64 unique workers")
        if (
            type(max_outstanding_per_worker) is not int
            or not 1 <= max_outstanding_per_worker <= 4_096
        ):
            raise FrontendFdHandoffError("frontend router handoff window is invalid")
        # Validate the generation without manufacturing a handoff timestamp.
        FrontendFdAckV1(generation, 1)
        for channel in channels:
            _require_seqpacket(channel)
        self._channels = channels
        self._generation = generation
        self._max_outstanding_per_worker = max_outstanding_per_worker
        self._next_worker = 0
        self._outstanding: dict[_ChannelT, deque[int]] = {
            channel: deque() for channel in channels
        }
        self._next_serial = 0
        self._handed_off = 0
        self._installed = 0
        self._backpressured = 0
        self._failed = False

    @property
    def channels(self) -> tuple[_ChannelT, ...]:
        return self._channels

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def available_capacity(self) -> int:
        """Return the exact number of descriptors the router can still own.

        The sole acceptor uses this value before ``accept()``. Once the
        bounded handoff window is full, new connections remain in the kernel
        listener backlog until a frontend ACK releases a slot. Taking
        ownership first and then discovering a full window would turn normal
        bounded backpressure into an unexplained post-send disconnect.
        """

        return len(self._channels) * self._max_outstanding_per_worker - sum(
            map(len, self._outstanding.values())
        )

    def stats(self) -> FrontendFdRouterStatsV1:
        return FrontendFdRouterStatsV1(
            worker_count=len(self._channels),
            idle_workers=sum(
                len(self._outstanding[channel]) < self._max_outstanding_per_worker
                for channel in self._channels
            ),
            per_worker_capacity=self._max_outstanding_per_worker,
            total_capacity=len(self._channels) * self._max_outstanding_per_worker,
            outstanding_handoffs=sum(map(len, self._outstanding.values())),
            handed_off=self._handed_off,
            installed=self._installed,
            backpressured=self._backpressured,
            failed=self._failed,
        )

    def offer(
        self,
        connection: socket.socket,
        *,
        peer: ConsumerUnixPeerV2,
        accepted_monotonic_ns: int,
    ) -> FrontendFdHandoffV1 | None:
        if self._failed:
            raise FrontendFdHandoffError("frontend router is fenced")
        selected: int | None = None
        for offset in range(len(self._channels)):
            worker = (self._next_worker + offset) % len(self._channels)
            if len(self._outstanding[self._channels[worker]]) < self._max_outstanding_per_worker:
                selected = worker
                break
        if selected is None:
            self._backpressured += 1
            return None
        channel = self._channels[selected]
        self._next_serial += 1
        message = FrontendFdHandoffV1(
            self._generation,
            self._next_serial,
            accepted_monotonic_ns,
            peer,
        )
        try:
            send_frontend_fd(channel, connection, message)
        except BaseException:
            self._failed = True
            raise
        connection.close()
        self._outstanding[channel].append(message.serial)
        self._next_worker = (selected + 1) % len(self._channels)
        self._handed_off += 1
        if self._handed_off % len(self._channels) == 0:
            # Rotate the next cycle's phase. A stable alternating workload must
            # not pin every WRITE to one half of an even worker set and every
            # READ to the other half. This remains deterministic and balanced,
            # while mixing periodic request patterns across all frontends.
            self._next_worker = (self._next_worker + 1) % len(self._channels)
        return message

    def acknowledge(self, channel: _ChannelT) -> FrontendFdAckV1:
        if self._failed:
            raise FrontendFdHandoffError("frontend router is fenced")
        outstanding = self._outstanding.get(channel)
        if outstanding is None:
            self._failed = True
            raise FrontendFdHandoffError("frontend ACK channel is unknown")
        try:
            ack = receive_frontend_fd_ack(channel)
        except BaseException:
            self._failed = True
            raise
        if not outstanding:
            self._failed = True
            raise FrontendFdHandoffError(
                "frontend ACK has no outstanding handoff: "
                f"serial={ack.serial}, handed_off={self._handed_off}, "
                f"installed={self._installed}"
            )
        expected = outstanding[0]
        if ack.generation != self._generation or ack.serial != expected:
            self._failed = True
            raise FrontendFdHandoffError("frontend ACK does not match outstanding handoff")
        outstanding.popleft()
        self._installed += 1
        return ack

    def fence(self) -> None:
        self._failed = True
