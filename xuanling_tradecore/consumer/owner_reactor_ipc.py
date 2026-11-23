"""Reactor-owned, nonblocking inherited WRITE transport. No second job queue.

Requests which already borrowed the original fair-ready WRITE credit may be
streamed into the inherited socket while the owner executes them strictly one
at a time.  The kernel socket is the only request-frame buffer: this port keeps
only bounded correlation metadata and at most one partially-sent packet.
Completed, undelivered replies continue to borrow the original queue's credit.
There are no worker threads, reconnects or automatic replays.
"""

from __future__ import annotations

import socket
import struct
from collections import deque
from dataclasses import dataclass
from hashlib import sha256
from threading import get_ident
from time import monotonic_ns
from typing import Callable, Generic, TypeVar, cast

from .base import ConsumerSession
from .fair_queue_v2 import BoundedFairOperationQueueV2
from .frontend_shared_credit import FrontendSharedCreditV1
from .owner_ipc_v1 import (
    MAX_OWNER_PACKET_BYTES,
    OwnerAuthorityV1,
    OwnerCreditLedgerV1,
    OwnerEventV1,
    OwnerIdentityV1,
    OwnerPeerV1,
    OwnerProtocolError,
    OwnerReadMessageV1,
    OwnerReadyV1,
    OwnerRequestV1,
    decode_owner_packet,
    encode_owner_packet,
)
from .unix_v2 import ConsumerUnixPeerV2
from .wire_v2 import DecodedFrameV2, MessageKind, decode_frame

_T = TypeVar("_T")
_HEADER = struct.Struct("!4sII")
_TURN_PACKET_BATCH = 4
_TURN_IO_BYTES = 65_536
_TURN_READ_SYSCALLS = _TURN_PACKET_BATCH * 4


def owner_request_fields(frame: DecodedFrameV2) -> tuple[str, str]:
    payload = frame.payload.get("payload")
    if not isinstance(payload, dict):
        raise OwnerProtocolError("owner request payload missing")
    body = cast(dict[str, object], payload)
    if frame.payload.get("operation") == "SubmitSignal":
        key, strategy = body.get("strategy_order_id"), body.get("strategy_id")
    elif frame.payload.get("operation") == "CancelOrder":
        key, strategy = body.get("oms_order_id"), "__ACCOUNT_SCOPE__"
    else:
        raise OwnerProtocolError("owner WRITE operation required")
    if type(key) is not str or type(strategy) is not str or not key or not strategy:
        raise OwnerProtocolError("owner business routing identity missing")
    return key, strategy


def reactor_owner_request(
    *,
    ready: OwnerReadyV1,
    serial: int,
    session: ConsumerSession,
    account_id: str,
    peer: ConsumerUnixPeerV2 | None,
    raw: bytes,
    deadline_ns: int,
) -> OwnerRequestV1:
    if peer is None:
        raise OwnerProtocolError("original kernel peer missing")
    frame = decode_frame(raw)
    key, strategy = owner_request_fields(frame)
    operation = frame.payload["operation"]
    if operation not in ("SubmitSignal", "CancelOrder"):
        raise OwnerProtocolError("owner WRITE required")
    return OwnerRequestV1(
        OwnerIdentityV1(
            ready.generation,
            serial,
            OwnerAuthorityV1(
                session.consumer_id, account_id, strategy, ready.scope_sha256, session.binding_epoch
            ),
            str(frame.payload["request_id"]),
            key,
            sha256(raw).digest(),
            OwnerPeerV1(peer.pid, peer.uid, peer.gid),
        ),
        operation,  # pyright: ignore[reportArgumentType]
        deadline_ns,
        raw,
    )


def reactor_owner_read_request(
    *,
    ready: OwnerReadyV1,
    serial: int,
    session: ConsumerSession,
    account_id: str,
    peer: ConsumerUnixPeerV2 | None,
    raw: bytes,
    deadline_ns: int,
) -> OwnerReadMessageV1:
    if peer is None:
        raise OwnerProtocolError("original read kernel peer missing")
    frame = decode_frame(raw)
    return OwnerReadMessageV1(
        OwnerIdentityV1(
            ready.generation,
            serial,
            OwnerAuthorityV1(
                session.consumer_id,
                account_id,
                "__READ_SCOPE__",
                ready.scope_sha256,
                session.binding_epoch,
            ),
            str(frame.payload["request_id"]),
            "READ:" + str(frame.payload.get("operation")),
            sha256(raw).digest(),
            OwnerPeerV1(peer.pid, peer.uid, peer.gid),
        ),
        "REQUEST",
        frame=raw,
        deadline_ns=deadline_ns,
    )


@dataclass(frozen=True)
class _Association:
    correlation: int
    authority: OwnerAuthorityV1
    deadline_ns: int


class ReactorOwnerPortV1(Generic[_T]):
    """All methods except containment close belong to the creating reactor.

    A single send buffer and a single packet assembler are bounded by the
    existing wire maximum. The <=128-entry metadata table stores no request
    frames.  Fully sent packets may wait in the bounded kernel socket, but the
    owner protocol and ledger still permit only one STARTED request and require
    terminal replies in serial order. Socket loss or even zero-progress send
    expiry fences all work; neither is proof that an order was not executed.
    """

    def __init__(
        self,
        channel: socket.socket,
        *,
        ready: OwnerReadyV1,
        ready_queue: BoundedFairOperationQueueV2[_T],
        shared_credit: FrontendSharedCreditV1 | None = None,
        worker_id: int | None = None,
        diagnostic_sent: Callable[[int, int, int], None] | None = None,
        handoff_limit: int = 128,
    ) -> None:
        if (shared_credit is None) != (worker_id is None):
            raise OwnerProtocolError(
                "shared frontend credit and worker identity must be supplied together"
            )
        if type(handoff_limit) is not int or not 1 <= handoff_limit <= 128:
            raise OwnerProtocolError("owner handoff limit must be in 1..128")
        self.channel, self.ready = channel, ready
        self._thread = get_ident()
        self._ledger = OwnerCreditLedgerV1(
            generation=ready.generation,
            scope_sha256=ready.scope_sha256,
            owner_pid=ready.owner_pid,
            ready_queue=ready_queue,
        )
        self._ledger.accept_ready(ready)
        self._associations: dict[int, _Association] = {}
        self._shared_credit = shared_credit
        self._handoff_limit = handoff_limit
        self._worker_id = worker_id
        self._shared_serials: set[int] = set()
        self._wire_order: deque[int] = deque()
        # At most four already-authorized packets may wait for the next
        # bounded reactor flush.  This is not a second business queue: every
        # entry has already borrowed the original fair-ready WRITE credit and
        # remains in the original ledger until the public response is sent.
        self._output: deque[bytes] = deque()
        self._diagnostic_sent = diagnostic_sent
        self._diagnostic_output_serials: deque[int] | None = (
            deque() if diagnostic_sent is not None else None
        )
        self._offset = 0
        self._turn_offers = 0
        self._input = bytearray()
        self._expected = _HEADER.size
        self.shutdown_verified = False
        channel.setblocking(False)

    def _require_thread(self) -> None:
        if get_ident() != self._thread:
            raise OwnerProtocolError("owner port belongs to one reactor")

    @property
    def outstanding(self) -> int:
        return self._ledger.outstanding

    @property
    def failed(self) -> bool:
        return self._ledger.failed

    @property
    def full(self) -> bool:
        return (
            self.failed
            or (
                self._shared_credit is not None
                and not self._shared_credit.maybe_available
            )
            or self._turn_offers >= _TURN_PACKET_BATCH
            or len(self._output) >= _TURN_PACKET_BATCH
            or self.outstanding >= self._handoff_limit
        )

    @property
    def wants_write(self) -> bool:
        return bool(self._output)

    @property
    def turn_batch_ready(self) -> bool:
        return len(self._output) >= _TURN_PACKET_BATCH

    @property
    def turn_offer_budget_exhausted(self) -> bool:
        """A flushed batch still consumes this turn's four-offer budget."""
        return self._turn_offers >= _TURN_PACKET_BATCH

    def begin_turn(self) -> None:
        """Open exactly one four-packet WRITE handoff budget for this turn."""
        self._require_thread()
        self._turn_offers = 0

    def offer(self, request: OwnerRequestV1) -> bool:
        self._require_thread()
        if self.full:
            return False
        frame = decode_frame(request.request_frame)
        packet = encode_owner_packet(request)
        shared, worker_id = self._shared_credit, self._worker_id
        shared_acquired = False
        if shared is not None:
            assert worker_id is not None
            if not shared.try_acquire(worker_id):
                return False
            shared_acquired = True
        try:
            if not self._ledger.reserve(request):
                if shared_acquired:
                    assert shared is not None and worker_id is not None
                    shared.release(worker_id)
                return False
        except BaseException:
            if shared_acquired:
                assert shared is not None and worker_id is not None
                shared.release(worker_id)
            raise
        if shared_acquired:
            self._shared_serials.add(request.identity.serial)
        self._associations[request.identity.serial] = _Association(
            frame.header.correlation_id, request.identity.authority, request.deadline_ns
        )
        self._wire_order.append(request.identity.serial)
        self._output.append(packet)
        if self._diagnostic_output_serials is not None:
            self._diagnostic_output_serials.append(request.identity.serial)
        self._turn_offers += 1
        return True

    def expire(self, *, now_ns: int | None = None) -> None:
        self._require_thread()
        now = monotonic_ns() if now_ns is None else now_ns
        if self._wire_order and now >= self._associations[self._wire_order[0]].deadline_ns:
            self.fail()
            raise OwnerProtocolError("owner execution/reply deadline expired; outcome unknown")

    def write_ready(self, *, maximum_bytes: int = _TURN_IO_BYTES) -> None:
        """Send one bounded vector containing at most four ordered packets.

        Linux uses one ``sendmsg`` syscall.  Platforms without ``sendmsg``
        use one bounded ``send`` over a temporary joined view for tests.  A
        short write advances exact packet boundaries and never releases a
        ledger credit.
        """
        self.expire()
        if not 1 <= maximum_bytes <= _TURN_IO_BYTES:
            raise OwnerProtocolError("invalid owner reactor send budget")
        if not self._output:
            return
        try:
            remaining = maximum_bytes
            views: list[memoryview] = []
            for index, packet in enumerate(self._output):
                start = self._offset if index == 0 else 0
                available = len(packet) - start
                if available <= 0:
                    raise OwnerProtocolError("owner send packet cursor invalid")
                take = min(remaining, available)
                views.append(memoryview(packet)[start : start + take])
                remaining -= take
                if remaining == 0:
                    break
            try:
                diagnostic_before = monotonic_ns() if self._diagnostic_sent is not None else 0
                if hasattr(self.channel, "sendmsg"):
                    sent = self.channel.sendmsg(views)
                else:  # pragma: no cover - Windows-only compatibility path
                    sent = self.channel.send(b"".join(views))
                diagnostic_after = monotonic_ns() if self._diagnostic_sent is not None else 0
            except BlockingIOError:
                return
            if sent <= 0:
                raise OwnerProtocolError("owner send made no progress")
            remaining_sent = sent
            while remaining_sent:
                packet = self._output[0]
                available = len(packet) - self._offset
                if remaining_sent < available:
                    self._offset += remaining_sent
                    remaining_sent = 0
                    continue
                remaining_sent -= available
                self._output.popleft()
                if self._diagnostic_output_serials is not None:
                    assert self._diagnostic_sent is not None
                    serial = self._diagnostic_output_serials.popleft()
                    self._diagnostic_sent(serial, diagnostic_before, diagnostic_after)
                self._offset = 0
        except BaseException:
            self.fail()
            raise

    def _read_batch(
        self,
        *,
        maximum_bytes: int = _TURN_IO_BYTES,
        maximum_events: int = _TURN_PACKET_BATCH,
        include_started: bool,
    ) -> tuple[OwnerEventV1, ...]:
        """Read up to four terminal replies under one 64 KiB turn budget.

        Every recv remains capped at the current packet boundary, so no second
        packet is hidden inside the assembler.  STARTED and terminal packets
        can nevertheless be consumed in the same reactor turn.  The syscall,
        byte and terminal-event limits prevent a busy owner from monopolizing
        the public socket reactor.
        """
        self.expire()
        if (
            not 1 <= maximum_bytes <= _TURN_IO_BYTES
            or not 1 <= maximum_events <= _TURN_PACKET_BATCH
        ):
            raise OwnerProtocolError("invalid owner reactor receive budget")
        delivered: list[OwnerEventV1] = []
        remaining_bytes = maximum_bytes
        try:
            for _ in range(_TURN_READ_SYSCALLS):
                if remaining_bytes == 0 or len(delivered) >= maximum_events:
                    break
                try:
                    part = self.channel.recv(
                        min(remaining_bytes, self._expected - len(self._input))
                    )
                except BlockingIOError:
                    break
                if not part:
                    raise OwnerProtocolError("owner channel EOF; outcome unknown")
                remaining_bytes -= len(part)
                self._input.extend(part)
                if len(self._input) != self._expected:
                    continue
                if self._expected == _HEADER.size:
                    magic, length, _crc = _HEADER.unpack(self._input)
                    if magic != b"TCO1" or not 0 < length <= MAX_OWNER_PACKET_BYTES:
                        raise OwnerProtocolError("owner reactor packet outside fixed budget")
                    self._expected += length
                    continue
                event = decode_owner_packet(bytes(self._input))
                self._input.clear()
                self._expected = _HEADER.size
                if (
                    type(event) is not OwnerEventV1
                    or not self._wire_order
                    or event.identity.serial != self._wire_order[0]
                ):
                    raise OwnerProtocolError("owner reactor unsolicited/wrong-direction event")
                self._ledger.observe(event)
                if event.phase == "STARTED":
                    if include_started:
                        delivered.append(event)
                    continue
                association = self._associations[event.identity.serial]
                result = self._ledger.result(
                    event.identity.serial, current_authority=association.authority
                )
                if result.phase == "COMPLETED":
                    self._validate_response(result, association)
                self._wire_order.popleft()
                delivered.append(result)
            return tuple(delivered)
        except BaseException:
            self.fail()
            raise

    def read_ready_batch(
        self,
        *,
        maximum_bytes: int = _TURN_IO_BYTES,
        maximum_events: int = _TURN_PACKET_BATCH,
    ) -> tuple[OwnerEventV1, ...]:
        """Read terminal replies; STARTED remains an internal state transition."""

        return self._read_batch(
            maximum_bytes=maximum_bytes,
            maximum_events=maximum_events,
            include_started=False,
        )

    def read_protocol_batch(
        self,
        *,
        maximum_bytes: int = _TURN_IO_BYTES,
        maximum_events: int = _TURN_PACKET_BATCH,
    ) -> tuple[OwnerEventV1, ...]:
        """Read verified STARTED and terminal events for a trusted broker.

        Public frontends continue to use :meth:`read_ready_batch`.  A central
        multi-frontend broker must forward the real STARTED transition so each
        worker's own ledger can validate the later terminal event; fabricating
        STARTED from a terminal reply would erase the unknown-outcome fence.
        """

        return self._read_batch(
            maximum_bytes=maximum_bytes,
            maximum_events=maximum_events,
            include_started=True,
        )

    def read_ready(self, *, maximum_bytes: int = _TURN_IO_BYTES) -> OwnerEventV1 | None:
        """Compatibility wrapper for single-result callers and unit fixtures."""
        events = self.read_ready_batch(maximum_bytes=maximum_bytes, maximum_events=1)
        return events[0] if events else None

    @staticmethod
    def _validate_response(event: OwnerEventV1, association: _Association) -> None:
        response = decode_frame(event.response_frame)
        if (
            response.header.message_kind not in {MessageKind.RESPONSE, MessageKind.ERROR}
            or response.header.correlation_id != association.correlation
            or response.payload.get("request_id") != event.identity.request_id
        ):
            raise OwnerProtocolError("owner reactor response association invalid")
        if response.header.message_kind is MessageKind.RESPONSE:
            body = response.payload.get("payload")
            if not isinstance(body, dict):
                raise OwnerProtocolError("owner reactor receipt payload missing")
            receipt = cast(dict[str, object], body)
            lsn = receipt.get("journal_lsn")
            if (
                type(lsn) is not int
                or not 0 < lsn <= event.source_lsn
                or receipt.get("journal_id") != event.journal_id
                or receipt.get("consumer_request_id") != event.identity.request_id
            ):
                raise OwnerProtocolError("owner reactor receipt exceeds published fence")

    def acknowledge_delivery(self, serial: int, *, current_authority: OwnerAuthorityV1) -> None:
        self.validate_delivery(serial, current_authority=current_authority)
        self.dispose(serial)

    def validate_delivery(self, serial: int, *, current_authority: OwnerAuthorityV1) -> None:
        self._ledger.result(serial, current_authority=current_authority)

    def dispose(self, serial: int) -> None:
        self._require_thread()
        self._ledger.release(serial)
        del self._associations[serial]
        if self._shared_credit is not None:
            if serial not in self._shared_serials:
                self._shared_credit.fence()
                raise OwnerProtocolError("owner shared credit association missing")
            self._shared_serials.remove(serial)
            assert self._worker_id is not None
            self._shared_credit.release(self._worker_id)

    def fail(self) -> None:
        self._ledger.fail()
        if self._shared_credit is not None:
            self._shared_credit.fence()
        self.close_transport()

    def close_transport(self) -> None:
        """Emergency containment only; never sets shutdown_verified."""
        self.channel.close()

    def stop(self) -> None:
        self._require_thread()
        if self.shutdown_verified:
            return
        self._ledger.stop()
        self._ledger.require_drained()
        if self._shared_credit is not None:
            assert self._worker_id is not None
            if self._shared_serials:
                self._shared_credit.fence()
                raise OwnerProtocolError("owner shared credit associations not drained")
        self.shutdown_verified = True
        # Keep inherited channel open until launcher sends control STOP and
        # verifies the child's managed exit; EOF first would fence the owner.
