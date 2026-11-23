"""Four nonblocking read rendezvous, borrowing the ORIGINAL fair-ready credits."""

from __future__ import annotations

import selectors
import socket
import struct
from dataclasses import dataclass, field
from hashlib import sha256
from threading import get_ident
from time import monotonic_ns
from typing import Generic, TypeVar, cast

from .fair_queue_v2 import BoundedFairOperationQueueV2, ConsumerOperationLaneV2
from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerIdentityV1,
    OwnerProtocolError,
    OwnerReadMessageV1,
    decode_owner_packet,
    encode_owner_packet,
)
from .wire_v2 import MessageKind, decode_frame

_T = TypeVar("_T")
_HEADER = struct.Struct("!4sII")
OWNER_READ_SLOT_COUNT = 8
OWNER_DIRECT_READ_SLOT_COUNT = OWNER_READ_SLOT_COUNT * 2
FRONTEND_READ_SLOT_COUNT = 4
_LANE_INFLIGHT_LIMIT = {
    # A replay page can carry many events and hold one rendezvous much longer
    # than a cached snapshot page.  Keep one of the bounded slots for replay
    # instead of allowing large replies to head-of-line block all
    # snapshot work.  This partitions existing credits; it adds no capacity.
    ConsumerOperationLaneV2.REPLAY: 1,
    ConsumerOperationLaneV2.SNAPSHOT: OWNER_READ_SLOT_COUNT,
    ConsumerOperationLaneV2.OTHER: 1,
}
# Snapshot pages are the steady high-QPS class.  Keep their concurrency below
# the single-CPU read-plane actor count so replay/control work receives an
# actual scheduling window, not merely unused rendezvous credits.  Six slots
# allow the qualified 500 snapshot/s LOW envelope to absorb short handler
# tails, while two physical rendezvous remain unavailable to snapshots.  The
# read-plane service gate also pauses new snapshot turns as soon as
# replay/control work is waiting.
_MAX_SHARED_SNAPSHOT_READ_SLOTS = 6


@dataclass
class _Slot:
    socket: socket.socket
    phase: str = "FREE"
    identity: OwnerIdentityV1 | None = None
    lane: ConsumerOperationLaneV2 = ConsumerOperationLaneV2.OTHER
    correlation: int = 0
    deadline_ns: int = 0
    response: bytes = b""
    digest: bytes = b""
    output: bytes = b""
    offset: int = 0
    input: bytearray = field(default_factory=bytearray)
    expected: int = _HEADER.size
    abandoned: bool = False


class ReactorOwnerReadsV1(Generic[_T]):
    def __init__(
        self,
        channels: tuple[socket.socket, ...],
        *,
        ready_queue: BoundedFairOperationQueueV2[_T],
        shared_credit: FrontendSharedCreditV1 | None = None,
        shared_read_slots: FrontendSharedCreditV1 | None = None,
        worker_id: int | None = None,
        release_ack_required: bool = False,
        frontend_slot_count: int = FRONTEND_READ_SLOT_COUNT,
        channel_lanes: tuple[frozenset[ConsumerOperationLaneV2], ...] | None = None,
    ) -> None:
        if not (
            (shared_credit is None and shared_read_slots is None and worker_id is None)
            or (
                shared_credit is not None
                and shared_read_slots is not None
                and worker_id is not None
            )
        ):
            raise OwnerProtocolError(
                "shared total/read credit and worker identity must be supplied together"
            )
        if (
            type(frontend_slot_count) is not int
            or not 1 <= frontend_slot_count <= FRONTEND_READ_SLOT_COUNT
        ):
            raise OwnerProtocolError("frontend read slot count is outside fixed bounds")
        direct_dual_plane = (
            shared_credit is None
            and channel_lanes is not None
            and channel_lanes
            == (
                (frozenset({ConsumerOperationLaneV2.REPLAY}),) * OWNER_READ_SLOT_COUNT
                + (
                    frozenset(
                        {
                            ConsumerOperationLaneV2.SNAPSHOT,
                            ConsumerOperationLaneV2.OTHER,
                        }
                    ),
                )
                * OWNER_READ_SLOT_COUNT
            )
        )
        expected_channels = (
            OWNER_DIRECT_READ_SLOT_COUNT
            if direct_dual_plane
            else OWNER_READ_SLOT_COUNT
            if shared_credit is None
            else frontend_slot_count
        )
        if (
            len(channels) != expected_channels
            or len({c.fileno() for c in channels}) != expected_channels
        ):
            raise OwnerProtocolError("owner read rendezvous has the wrong fixed channel count")
        if channel_lanes is None:
            channel_lanes = tuple(
                frozenset(
                    {
                        ConsumerOperationLaneV2.REPLAY,
                        ConsumerOperationLaneV2.SNAPSHOT,
                        ConsumerOperationLaneV2.OTHER,
                    }
                )
                for _ in channels
            )
        if len(channel_lanes) != len(channels) or any(
            not lanes
            or ConsumerOperationLaneV2.WRITE in lanes
            or not lanes
            <= {
                ConsumerOperationLaneV2.REPLAY,
                ConsumerOperationLaneV2.SNAPSHOT,
                ConsumerOperationLaneV2.OTHER,
            }
            for lanes in channel_lanes
        ):
            raise OwnerProtocolError("owner read lane topology is invalid")
        self._thread, self._queue = get_ident(), ready_queue
        self._shared_credit = shared_credit
        self._shared_read_slots = shared_read_slots
        self._worker_id = worker_id
        self._release_ack_required = release_ack_required
        self._slots = {c: _Slot(c) for c in channels}
        self._channel_lanes = dict(zip(channels, channel_lanes, strict=True))
        self._last_serial = 0
        self.failed = self.shutdown_verified = False
        for channel in channels:
            channel.setblocking(False)

    def _require_thread(self) -> None:
        if get_ident() != self._thread:
            raise OwnerProtocolError("owner read pool belongs to one reactor")

    @property
    def channels(self) -> tuple[socket.socket, ...]:
        return tuple(self._slots)

    @property
    def full(self) -> bool:
        return self.failed or all(slot.phase != "FREE" for slot in self._slots.values())

    def _shared_read_limit(self, lane: ConsumerOperationLaneV2) -> int | None:
        shared_reads = self._shared_read_slots
        if shared_reads is None:
            return None
        if lane is ConsumerOperationLaneV2.SNAPSHOT:
            return max(
                1,
                min(
                    _MAX_SHARED_SNAPSHOT_READ_SLOTS,
                    shared_reads.capacity - 1,
                ),
            )
        return shared_reads.capacity

    def full_for(self, lane: ConsumerOperationLaneV2) -> bool:
        """Whether this lane may borrow one of the fixed existing slots."""

        self._require_thread()
        if lane is ConsumerOperationLaneV2.WRITE:
            return True
        shared_total, shared_reads = self._shared_credit, self._shared_read_slots
        shared_read_limit = self._shared_read_limit(lane)
        return (
            not any(
                slot.phase == "FREE" and lane in self._channel_lanes[channel]
                for channel, slot in self._slots.items()
            )
            or (
                shared_total is not None
                and shared_reads is not None
                and (
                    not shared_total.maybe_available
                    or (
                        lane is ConsumerOperationLaneV2.SNAPSHOT
                        and not shared_reads.maybe_restricted_available(
                            cast(int, shared_read_limit)
                        )
                    )
                    or (
                        lane is not ConsumerOperationLaneV2.SNAPSHOT
                        and not shared_reads.maybe_available
                    )
                )
            )
            or sum(
                slot.phase != "FREE" and slot.lane is lane
                for slot in self._slots.values()
            )
            >= _LANE_INFLIGHT_LIMIT[lane]
        )

    @property
    def excluded_lanes(self) -> frozenset[ConsumerOperationLaneV2]:
        self._require_thread()
        return frozenset(
            lane
            for lane in ConsumerOperationLaneV2
            if lane is not ConsumerOperationLaneV2.WRITE and self.full_for(lane)
        )

    @property
    def outstanding(self) -> int:
        return sum(slot.phase != "FREE" for slot in self._slots.values())

    @property
    def active_serials(self) -> frozenset[int]:
        return frozenset(
            slot.identity.serial
            for slot in self._slots.values()
            if slot.identity is not None
        )

    def _find(self, serial: int) -> _Slot:
        for slot in self._slots.values():
            if slot.identity is not None and slot.identity.serial == serial:
                return slot
        raise OwnerProtocolError("unknown owner read serial")

    def _queue_message(self, slot: _Slot, message: OwnerReadMessageV1) -> None:
        if slot.output:
            raise OwnerProtocolError("owner read channel already has one bounded output")
        slot.output, slot.offset = encode_owner_packet(message), 0

    def offer(self, message: OwnerReadMessageV1, lane: ConsumerOperationLaneV2) -> bool:
        self._require_thread()
        if self.full_for(lane):
            return False
        if (
            message.phase != "REQUEST"
            or message.identity.serial <= self._last_serial
            or lane is ConsumerOperationLaneV2.WRITE
        ):
            raise OwnerProtocolError("invalid owner read offer")
        slot = next(
            slot
            for channel, slot in self._slots.items()
            if slot.phase == "FREE" and lane in self._channel_lanes[channel]
        )
        frame = decode_frame(message.frame)
        packet = encode_owner_packet(message)
        shared_total, shared_reads, worker_id = (
            self._shared_credit,
            self._shared_read_slots,
            self._worker_id,
        )
        total_acquired = read_acquired = False
        if shared_total is not None:
            assert shared_reads is not None and worker_id is not None
            if not shared_total.try_acquire(worker_id):
                return False
            total_acquired = True
            if not shared_reads.try_acquire(
                worker_id,
                restricted_limit=(
                    self._shared_read_limit(lane)
                    if lane is ConsumerOperationLaneV2.SNAPSHOT
                    else None
                ),
            ):
                shared_total.release(worker_id)
                return False
            read_acquired = True
        try:
            if not self._queue.reserve(lane):
                if read_acquired:
                    assert shared_reads is not None and worker_id is not None
                    shared_reads.release(
                        worker_id,
                        restricted=lane is ConsumerOperationLaneV2.SNAPSHOT,
                    )
                if total_acquired:
                    assert shared_total is not None and worker_id is not None
                    shared_total.release(worker_id)
                return False
        except BaseException:
            if read_acquired:
                assert shared_reads is not None and worker_id is not None
                shared_reads.release(
                    worker_id,
                    restricted=lane is ConsumerOperationLaneV2.SNAPSHOT,
                )
            if total_acquired:
                assert shared_total is not None and worker_id is not None
                shared_total.release(worker_id)
            raise
        slot.identity, slot.lane, slot.correlation = (
            message.identity,
            lane,
            frame.header.correlation_id,
        )
        slot.phase, slot.deadline_ns, slot.output = "PREPARING", message.deadline_ns, packet
        self._last_serial = message.identity.serial
        return True

    def events(self, channel: socket.socket) -> int:
        slot = self._slots[channel]
        return selectors.EVENT_READ | (selectors.EVENT_WRITE if slot.output else 0)

    def expire(self) -> None:
        self._require_thread()
        if any(
            slot.phase != "FREE" and monotonic_ns() >= slot.deadline_ns
            for slot in self._slots.values()
        ):
            self.fail()
            raise OwnerProtocolError("owner read deadline expired")

    def io(self, channel: socket.socket, mask: int) -> int | None:
        """One nonblocking send and recv per turn. Returns serial only on GRANT."""
        self.expire()
        slot = self._slots[channel]
        try:
            if mask & selectors.EVENT_WRITE and slot.output:
                try:
                    sent = channel.send(memoryview(slot.output)[slot.offset : slot.offset + 65536])
                except BlockingIOError:
                    sent = 0
                slot.offset += sent
                if slot.offset == len(slot.output):
                    slot.output, slot.offset = b"", 0
                    if slot.phase == "RELEASING":
                        if self._release_ack_required:
                            slot.phase = "AWAITING_RELEASE"
                        else:
                            self._release_credit(channel, slot)
                        return None
            if not mask & selectors.EVENT_READ:
                return None
            try:
                part = channel.recv(min(65536, slot.expected - len(slot.input)))
            except BlockingIOError:
                return None
            if not part:
                raise OwnerProtocolError("owner read EOF")
            slot.input.extend(part)
            if len(slot.input) != slot.expected:
                return None
            if slot.expected == _HEADER.size:
                magic, length, _crc = _HEADER.unpack(slot.input)
                if magic != b"TCO1" or not 0 < length <= MAX_OWNER_PACKET_BYTES:
                    raise OwnerProtocolError("owner read packet outside fixed budget")
                slot.expected += length
                return None
            reply = decode_owner_packet(bytes(slot.input))
            slot.input.clear()
            slot.expected = _HEADER.size
            if type(reply) is not OwnerReadMessageV1 or reply.identity != slot.identity:
                raise OwnerProtocolError("owner read reply association mismatch")
            expected_phases: frozenset[str] = (
                frozenset(("PREPARED", "GRANT"))
                if slot.phase == "PREPARING"
                else frozenset(("GRANT",))
                if slot.phase == "CHECKING"
                else frozenset(("RELEASED",))
                if slot.phase == "AWAITING_RELEASE"
                else frozenset()
            )
            if reply.phase not in expected_phases:
                raise OwnerProtocolError("owner read reply reordered/duplicated")
            if reply.phase == "RELEASED":
                self._release_credit(channel, slot)
                return None
            if reply.frame:
                decoded = decode_frame(reply.frame)
                if decoded.header.message_kind not in {MessageKind.RESPONSE, MessageKind.ERROR} or (
                    decoded.header.correlation_id != slot.correlation
                    or decoded.payload.get("request_id") != reply.identity.request_id
                ):
                    raise OwnerProtocolError("owner read public response association invalid")
                slot.response = reply.frame
            if not slot.response or sha256(slot.response).digest() != reply.response_sha256:
                raise OwnerProtocolError("owner read response digest mismatch")
            slot.digest = reply.response_sha256
            if slot.abandoned:
                self._release(slot)
                return None
            if reply.phase == "PREPARED":
                self._check(slot)
                return None
            slot.phase = "DELIVERABLE"
            return reply.identity.serial
        except BaseException:
            self.fail()
            raise

    def _check(self, slot: _Slot) -> None:
        assert slot.identity is not None
        slot.phase = "CHECKING"
        self._queue_message(
            slot, OwnerReadMessageV1(slot.identity, "CHECK", response_sha256=slot.digest)
        )

    def delivery(self, serial: int) -> bytes:
        self._require_thread()
        slot = self._find(serial)
        if self.failed or slot.phase != "DELIVERABLE":
            raise OwnerProtocolError("owner read has no current delivery grant")
        return slot.response

    def recheck(self, serial: int) -> None:
        self._require_thread()
        slot = self._find(serial)
        if slot.phase != "DELIVERABLE":
            raise OwnerProtocolError("owner read recheck phase invalid")
        self._check(slot)

    def _release(self, slot: _Slot) -> None:
        assert slot.identity is not None
        slot.phase = "RELEASING"
        slot.response, slot.digest = b"", b""
        self._queue_message(slot, OwnerReadMessageV1(slot.identity, "RELEASE"))

    def _release_credit(self, channel: socket.socket, slot: _Slot) -> None:
        self._queue.release(slot.lane)
        if self._shared_credit is not None:
            assert self._shared_read_slots is not None
            assert self._worker_id is not None
            self._shared_read_slots.release(
                self._worker_id,
                restricted=slot.lane is ConsumerOperationLaneV2.SNAPSHOT,
            )
            self._shared_credit.release(self._worker_id)
        self._slots[channel] = _Slot(channel)

    def dispose(self, serial: int) -> None:
        self._require_thread()
        slot = self._find(serial)
        if slot.phase == "DELIVERABLE":
            self._release(slot)
        else:
            slot.abandoned = True  # no fabricated cancellation; wait authoritative read

    def fail(self) -> None:
        self._require_thread()
        self.failed = True
        if self._shared_credit is not None:
            self._shared_credit.fence()
            assert self._shared_read_slots is not None
            self._shared_read_slots.fence()
        self.close_transport()

    def close_transport(self) -> None:
        for channel in self._slots:
            channel.close()

    def stop(self) -> None:
        self._require_thread()
        if self.failed or self.outstanding:
            raise OwnerProtocolError("owner read rendezvous not drained")
        if self._shared_read_slots is not None:
            assert self._worker_id is not None
            self._shared_read_slots.require_worker_drained(self._worker_id)
        self.shutdown_verified = True
