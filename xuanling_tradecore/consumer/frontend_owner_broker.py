"""Bounded cross-frontend WRITE coordinator for one authoritative OMS owner.

Frontend-local serials are never exposed to the unique owner.  The broker
assigns one global serial, forwards the owner's real STARTED/terminal events,
and retains the central owner credit until the terminal packet has been fully
handed back to the originating frontend.  There is no reconnect, replay, or
credit reclamation after a channel/process failure.
"""

from __future__ import annotations

import socket
import struct
from collections import deque
from dataclasses import dataclass
from threading import get_ident
from time import monotonic_ns
from typing import Callable, Generic, TypeVar

import msgspec

from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerIdentityV1,
    OwnerProtocolError,
    OwnerRequestV1,
    decode_owner_packet,
    encode_owner_packet,
)
from .owner_reactor_ipc import ReactorOwnerPortV1

_T = TypeVar("_T")
_HEADER = struct.Struct("!4sII")
_MAX_FRONTENDS = 64
_MAX_GLOBAL_CREDIT = 128
_MAX_EVENT_PACKETS = _MAX_GLOBAL_CREDIT * 2


@dataclass
class _PacketInput:
    data: bytearray
    expected: int = _HEADER.size

    def read(self, channel: socket.socket) -> object | None:
        # One packet needs at most the fixed header and payload reads when the
        # local SOCK_STREAM has both available. Fragmentation remains in this
        # bounded assembler for a later readiness turn.
        for _ in range(2):
            try:
                part = channel.recv(self.expected - len(self.data))
            except BlockingIOError:
                return None
            if not part:
                raise OwnerProtocolError("frontend owner channel EOF; outcome unknown")
            self.data.extend(part)
            if len(self.data) != self.expected:
                return None
            if self.expected == _HEADER.size:
                magic, length, _crc = _HEADER.unpack(self.data)
                if magic != b"TCO1" or not 0 < length <= MAX_OWNER_PACKET_BYTES:
                    raise OwnerProtocolError("frontend owner packet outside fixed budget")
                self.expected += length
                continue
            message = decode_owner_packet(bytes(self.data))
            self.data.clear()
            self.expected = _HEADER.size
            return message
        return None

    @property
    def empty(self) -> bool:
        return not self.data and self.expected == _HEADER.size


@dataclass(frozen=True)
class _Route:
    worker_id: int
    frontend_identity: OwnerIdentityV1


@dataclass
class _Outbound:
    packet: bytes
    central_serial: int
    terminal: bool
    offset: int = 0


class FrontendOwnerWriteBrokerV1(Generic[_T]):
    """One-thread coordinator from fixed frontend channels to one owner port."""

    def __init__(
        self,
        frontend_channels: tuple[socket.socket, ...],
        *,
        owner: ReactorOwnerPortV1[_T],
        shared_credit: FrontendSharedCreditV1,
        diagnostic_ingested: Callable[[OwnerRequestV1, int, int, int, int], None] | None = None,
    ) -> None:
        if not 1 <= len(frontend_channels) <= _MAX_FRONTENDS:
            raise OwnerProtocolError("invalid fixed frontend broker topology")
        snapshot = shared_credit.snapshot()
        if snapshot.worker_count != len(frontend_channels) or snapshot.capacity > 128:
            raise OwnerProtocolError("frontend broker/shared credit topology mismatch")
        self.channels = frontend_channels
        self.owner = owner
        self.shared_credit = shared_credit
        self._diagnostic_ingested = diagnostic_ingested
        self._thread = get_ident()
        self._inputs = [_PacketInput(bytearray()) for _ in frontend_channels]
        self._outputs = [deque[_Outbound]() for _ in frontend_channels]
        self._last_frontend_serial = [0] * len(frontend_channels)
        self._worker_pending = [0] * len(frontend_channels)
        self._routes: dict[int, _Route] = {}
        self._central_serial = 0
        self._event_packets = 0
        self.failed = False
        self.shutdown_verified = False
        for channel in frontend_channels:
            channel.setblocking(False)

    def _require_thread(self) -> None:
        if get_ident() != self._thread:
            raise OwnerProtocolError("frontend owner broker belongs to one reactor")

    @property
    def outstanding(self) -> int:
        return len(self._routes)

    def wants_frontend_write(self, worker_id: int) -> bool:
        return bool(self._outputs[self._worker(worker_id)])

    def _worker(self, worker_id: int) -> int:
        if type(worker_id) is not int or not 0 <= worker_id < len(self.channels):
            raise OwnerProtocolError("frontend worker outside broker topology")
        return worker_id

    def begin_turn(self) -> None:
        self._require_thread()
        if self.failed:
            raise OwnerProtocolError("frontend owner broker is fenced")
        self.owner.begin_turn()

    def read_frontend(self, worker_id: int) -> bool:
        """Read at most one request and hand it to the central owner port."""

        self._require_thread()
        index = self._worker(worker_id)
        if self.failed or self.owner.full or len(self._routes) >= _MAX_GLOBAL_CREDIT:
            return False
        try:
            message = self._inputs[index].read(self.channels[index])
            if message is None:
                return False
            if type(message) is not OwnerRequestV1:
                raise OwnerProtocolError("frontend broker accepts WRITE requests only")
            diagnostic_received = monotonic_ns() if self._diagnostic_ingested is not None else 0
            identity = message.identity
            if (
                identity.generation != self.owner.ready.generation
                or identity.serial <= self._last_frontend_serial[index]
                or identity.authority.scope_sha256 != self.owner.ready.scope_sha256
            ):
                raise OwnerProtocolError("frontend request generation/serial/scope mismatch")
            snapshot = self.shared_credit.snapshot()
            if snapshot.fenced or snapshot.held_by_worker[index] <= self._worker_pending[index]:
                raise OwnerProtocolError("frontend request has no matching shared credit")
            self._central_serial += 1
            central_identity = msgspec.structs.replace(
                identity,
                generation=self.owner.ready.generation,
                serial=self._central_serial,
            )
            central_request = msgspec.structs.replace(message, identity=central_identity)
            if not self.owner.offer(central_request):
                raise OwnerProtocolError("central owner credit unavailable after admission")
            if self._diagnostic_ingested is not None:
                self._diagnostic_ingested(
                    central_request,
                    index,
                    diagnostic_received,
                    monotonic_ns(),
                    len(self._routes),
                )
            self._routes[self._central_serial] = _Route(index, identity)
            self._last_frontend_serial[index] = identity.serial
            self._worker_pending[index] += 1
            return True
        except BaseException:
            self.fail()
            raise

    def write_owner(self) -> None:
        self._require_thread()
        try:
            self.owner.write_ready()
        except BaseException:
            self.fail()
            raise

    def read_owner(self) -> int:
        """Route verified real STARTED/terminal events to their source worker."""

        self._require_thread()
        routed = 0
        try:
            for event in self.owner.read_protocol_batch():
                route = self._routes.get(event.identity.serial)
                if route is None:
                    raise OwnerProtocolError("owner event has no frontend route")
                forwarded = msgspec.structs.replace(event, identity=route.frontend_identity)
                if self._event_packets >= _MAX_EVENT_PACKETS:
                    raise OwnerProtocolError("frontend broker event budget exhausted")
                terminal = event.phase != "STARTED"
                self._outputs[route.worker_id].append(
                    _Outbound(encode_owner_packet(forwarded), event.identity.serial, terminal)
                )
                self._event_packets += 1
                routed += 1
            return routed
        except BaseException:
            self.fail()
            raise

    def write_frontend(self, worker_id: int) -> bool:
        """Advance one event packet; release central credit only after terminal handoff."""

        self._require_thread()
        index = self._worker(worker_id)
        output = self._outputs[index]
        if not output:
            return False
        item = output[0]
        try:
            try:
                sent = self.channels[index].send(memoryview(item.packet)[item.offset :])
            except BlockingIOError:
                return False
            if sent <= 0:
                raise OwnerProtocolError("frontend broker send made no progress")
            item.offset += sent
            if item.offset != len(item.packet):
                return True
            output.popleft()
            self._event_packets -= 1
            if item.terminal:
                route = self._routes.pop(item.central_serial)
                if route.worker_id != index:
                    raise OwnerProtocolError("frontend terminal route changed")
                self.owner.dispose(item.central_serial)
                self._worker_pending[index] -= 1
            return True
        except BaseException:
            self.fail()
            raise

    def fail(self) -> None:
        if self.failed:
            return
        self.failed = True
        self.shared_credit.fence()
        self.owner.fail()
        for channel in self.channels:
            channel.close()

    def stop(self) -> None:
        self._require_thread()
        if self.shutdown_verified:
            return
        if (
            self.failed
            or self._routes
            or self._event_packets
            or any(self._worker_pending)
            or any(self._outputs)
            or any(not item.empty for item in self._inputs)
        ):
            raise OwnerProtocolError("frontend owner broker is not drained")
        self.owner.stop()
        self.shared_credit.require_all_drained()
        self.shutdown_verified = True
