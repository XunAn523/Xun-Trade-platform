"""Bounded multi-frontend proxy for one authoritative fixed read pool."""

from __future__ import annotations

import selectors
import socket
import struct
from collections import deque
from dataclasses import dataclass
from hashlib import sha256
from threading import get_ident

import msgspec

from .fair_queue_v2 import ConsumerOperationLaneV2, consumer_operation_lane_v2
from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerIdentityV1,
    OwnerProtocolError,
    OwnerReadMessageV1,
    decode_owner_packet,
    encode_owner_packet,
)
from .owner_read_reactor import (
    FRONTEND_READ_SLOT_COUNT,
    OWNER_READ_SLOT_COUNT,
    ReactorOwnerReadsV1,
)
from .wire_v2 import decode_frame

_HEADER = struct.Struct("!4sII")
_FRONTEND_SERIAL_REORDER_WINDOW = 256


@dataclass
class _Input:
    data: bytearray
    expected: int = _HEADER.size

    def read(self, channel: socket.socket) -> object | None:
        for _ in range(2):
            try:
                part = channel.recv(self.expected - len(self.data))
            except BlockingIOError:
                return None
            if not part:
                raise OwnerProtocolError("frontend read channel EOF")
            self.data.extend(part)
            if len(self.data) != self.expected:
                return None
            if self.expected == _HEADER.size:
                magic, length, _crc = _HEADER.unpack(self.data)
                if magic != b"TCO1" or not 0 < length <= MAX_OWNER_PACKET_BYTES:
                    raise OwnerProtocolError("frontend read packet outside fixed budget")
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


@dataclass
class _Output:
    packet: bytes
    central_serial: int
    released: bool
    offset: int = 0


@dataclass
class _Route:
    worker_id: int
    channel: socket.socket
    frontend_identity: OwnerIdentityV1
    central_serial: int
    phase: str = "PREPARING"
    digest: bytes = b""
    request: OwnerReadMessageV1 | None = None
    lane: ConsumerOperationLaneV2 = ConsumerOperationLaneV2.OTHER


class FrontendOwnerReadBrokerV1:
    """Proxy fixed worker rendezvous into exactly one bounded owner pool."""

    def __init__(
        self,
        frontend_channels: tuple[tuple[socket.socket, ...], ...],
        *,
        owner_reads: ReactorOwnerReadsV1[object],
        shared_credit: FrontendSharedCreditV1,
        shared_read_slots: FrontendSharedCreditV1,
    ) -> None:
        if not frontend_channels or any(
            len(channels) != FRONTEND_READ_SLOT_COUNT for channels in frontend_channels
        ):
            raise OwnerProtocolError("read broker has the wrong per-frontend channel count")
        all_channels = tuple(channel for channels in frontend_channels for channel in channels)
        if len({channel.fileno() for channel in all_channels}) != len(all_channels):
            raise OwnerProtocolError("read broker frontend channels must be unique")
        total_snapshot, read_snapshot = shared_credit.snapshot(), shared_read_slots.snapshot()
        if (
            total_snapshot.worker_count != len(frontend_channels)
            or read_snapshot.worker_count != len(frontend_channels)
            or read_snapshot.capacity != OWNER_READ_SLOT_COUNT
        ):
            raise OwnerProtocolError("read broker/shared credit topology mismatch")
        self.frontend_channels = frontend_channels
        self.owner_reads = owner_reads
        self.shared_credit = shared_credit
        self.shared_read_slots = shared_read_slots
        self._thread = get_ident()
        self._channel_worker = {
            channel: worker_id
            for worker_id, channels in enumerate(frontend_channels)
            for channel in channels
        }
        self._inputs = {channel: _Input(bytearray()) for channel in all_channels}
        self._outputs = {channel: deque[_Output]() for channel in all_channels}
        self._by_frontend: dict[tuple[int, int], _Route] = {}
        self._by_central: dict[int, _Route] = {}
        self._pending: deque[_Route] = deque()
        # A frontend owns a fixed set of independent SOCK_STREAM rendezvous. Its serials
        # are monotonic at dispatch, but arrival at this broker is legitimately
        # reordered across those kernel queues.  Keep a fixed replay
        # window instead of imposing a false cross-channel arrival order.
        self._serial_high_water = [0] * len(frontend_channels)
        self._recent_serials = [set[int]() for _ in frontend_channels]
        self._central_serial = 0
        self.failed = self.shutdown_verified = False
        for channel in all_channels:
            channel.setblocking(False)

    def _require_thread(self) -> None:
        if get_ident() != self._thread:
            raise OwnerProtocolError("frontend read broker belongs to one reactor")

    @property
    def outstanding(self) -> int:
        return len(self._by_frontend)

    def pump(self) -> int:
        """Move admitted frontend READs into currently eligible central slots.

        Shared credits bound this queue to the authoritative fixed-read
        envelope.  A lane-specific central limit (for example one OTHER or
        one REPLAY) is ordinary scheduling backpressure, not a protocol
        violation and must never fence unrelated frontends.
        """

        self._require_thread()
        if self.failed:
            raise OwnerProtocolError("frontend read broker unavailable")
        dispatched = 0
        for _ in range(len(self._pending)):
            route = self._pending.popleft()
            assert route.request is not None
            central_serial = self._central_serial + 1
            identity = msgspec.structs.replace(
                route.request.identity,
                serial=central_serial,
            )
            remapped = msgspec.structs.replace(route.request, identity=identity)
            if not self.owner_reads.offer(remapped, route.lane):
                self._pending.append(route)
                continue
            self._central_serial = central_serial
            route.central_serial = central_serial
            route.phase = "PREPARING"
            route.request = None
            self._by_central[central_serial] = route
            dispatched += 1
        return dispatched

    def frontend_events(self, channel: socket.socket) -> int:
        if channel not in self._inputs:
            raise OwnerProtocolError("unknown frontend read channel")
        return selectors.EVENT_READ | (
            selectors.EVENT_WRITE if self._outputs[channel] else 0
        )

    def read_frontend(self, channel: socket.socket) -> bool:
        self._require_thread()
        if self.failed or channel not in self._inputs:
            raise OwnerProtocolError("frontend read broker unavailable/channel unknown")
        try:
            message = self._inputs[channel].read(channel)
            if message is None:
                return False
            if type(message) is not OwnerReadMessageV1:
                raise OwnerProtocolError("read broker received non-read message")
            worker_id = self._channel_worker[channel]
            key = (worker_id, message.identity.serial)
            if message.phase == "REQUEST":
                serial = message.identity.serial
                high_water = self._serial_high_water[worker_id]
                recent = self._recent_serials[worker_id]
                if (
                    key in self._by_frontend
                    or serial in recent
                    or (
                        high_water > _FRONTEND_SERIAL_REORDER_WINDOW
                        and serial <= high_water - _FRONTEND_SERIAL_REORDER_WINDOW
                    )
                ):
                    raise OwnerProtocolError("frontend read serial repeated/reordered")
                frame = decode_frame(message.frame)
                lane = consumer_operation_lane_v2(frame.payload.get("operation"))
                if lane is ConsumerOperationLaneV2.WRITE:
                    raise OwnerProtocolError("WRITE entered frontend read broker")
                total, reads = self.shared_credit.snapshot(), self.shared_read_slots.snapshot()
                active_for_worker = sum(
                    route.worker_id == worker_id for route in self._by_frontend.values()
                )
                if (
                    total.fenced
                    or reads.fenced
                    or reads.held_by_worker[worker_id] <= active_for_worker
                ):
                    raise OwnerProtocolError("frontend read has no matching shared slot")
                route = _Route(
                    worker_id,
                    channel,
                    message.identity,
                    0,
                    phase="QUEUED",
                    request=message,
                    lane=lane,
                )
                self._by_frontend[key] = route
                self._pending.append(route)
                recent.add(serial)
                if serial > high_water:
                    high_water = serial
                    self._serial_high_water[worker_id] = serial
                    retired = high_water - _FRONTEND_SERIAL_REORDER_WINDOW
                    if retired > 0:
                        active = {
                            active_serial
                            for active_worker, active_serial in self._by_frontend
                            if active_worker == worker_id
                        }
                        recent.difference_update(
                            item for item in tuple(recent) if item <= retired and item not in active
                        )
                self.pump()
                return True
            route = self._by_frontend.get(key)
            if route is None or route.channel is not channel or message.identity != route.frontend_identity:
                raise OwnerProtocolError("frontend read control has no exact route")
            if message.phase == "CHECK":
                if route.phase != "GRANTED" or message.response_sha256 != route.digest:
                    raise OwnerProtocolError("frontend read CHECK reordered/digest changed")
                self.owner_reads.recheck(route.central_serial)
                route.phase = "CHECKING"
                return True
            if message.phase == "RELEASE":
                if route.phase != "GRANTED":
                    raise OwnerProtocolError("frontend read RELEASE reordered")
                self.owner_reads.dispose(route.central_serial)
                route.phase = "RELEASING"
                return True
            raise OwnerProtocolError("frontend read broker direction/phase invalid")
        except BaseException:
            self.fail()
            raise

    def owner_events(self, channel: socket.socket) -> int:
        return self.owner_reads.events(channel)

    def io_owner(self, channel: socket.socket, mask: int) -> int | None:
        self._require_thread()
        before = self.owner_reads.active_serials
        try:
            serial = self.owner_reads.io(channel, mask)
            if serial is not None:
                route = self._by_central.get(serial)
                if route is None or route.phase not in ("PREPARING", "CHECKING"):
                    raise OwnerProtocolError("central read grant has no pending route")
                response = self.owner_reads.delivery(serial)
                digest = sha256(response).digest()
                forwarded = OwnerReadMessageV1(
                    route.frontend_identity,
                    "GRANT",
                    frame=response,
                    response_sha256=digest,
                )
                if self._outputs[route.channel]:
                    raise OwnerProtocolError("frontend read output is not single-slot")
                self._outputs[route.channel].append(
                    _Output(encode_owner_packet(forwarded), serial, False)
                )
                route.phase, route.digest = "GRANTED", digest
            removed = before - self.owner_reads.active_serials
            for removed_serial in removed:
                route = self._by_central.get(removed_serial)
                if route is None or route.phase != "RELEASING":
                    raise OwnerProtocolError("central read slot disappeared without RELEASE")
                if self._outputs[route.channel]:
                    raise OwnerProtocolError("frontend release acknowledgement output occupied")
                acknowledgement = OwnerReadMessageV1(route.frontend_identity, "RELEASED")
                self._outputs[route.channel].append(
                    _Output(encode_owner_packet(acknowledgement), removed_serial, True)
                )
                route.phase = "RELEASE_ACK"
            self.pump()
            return serial
        except BaseException:
            self.fail()
            raise

    def write_frontend(self, channel: socket.socket) -> bool:
        self._require_thread()
        if channel not in self._outputs:
            raise OwnerProtocolError("unknown frontend read channel")
        output = self._outputs[channel]
        if not output:
            return False
        item = output[0]
        try:
            try:
                sent = channel.send(memoryview(item.packet)[item.offset :])
            except BlockingIOError:
                return False
            if sent <= 0:
                raise OwnerProtocolError("frontend read broker send made no progress")
            item.offset += sent
            if item.offset != len(item.packet):
                return True
            output.popleft()
            if item.released:
                route = self._by_central.pop(item.central_serial)
                del self._by_frontend[(route.worker_id, route.frontend_identity.serial)]
            return True
        except BaseException:
            self.fail()
            raise

    def fail(self) -> None:
        if self.failed:
            return
        self.failed = True
        self._pending.clear()
        self.shared_credit.fence()
        self.shared_read_slots.fence()
        self.owner_reads.fail()
        for channel in self._inputs:
            channel.close()

    def stop(self) -> None:
        self._require_thread()
        if (
            self.failed
            or self._by_central
            or self._by_frontend
            or self._pending
            or any(self._outputs.values())
            or any(not item.empty for item in self._inputs.values())
        ):
            raise OwnerProtocolError("frontend read broker is not drained")
        self.owner_reads.stop()
        self.shared_read_slots.require_all_drained()
        self.shutdown_verified = True
