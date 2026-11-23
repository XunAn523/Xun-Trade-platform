"""Private spawn-owner protocol, not a public API or a second OMS.

Only bounded immutable bytes cross the inherited socketpair: never closures,
resolvers, Book objects, pickle or credentials. CRC detects framing damage; it
does NOT authenticate a network peer. The transport must be inherited by the
trusted spawned owner, never exposed on a public listener.

This module does not start an OMS or assert that a source commit happened.
The future unique owner must supply verified source positions and revalidate
the current authorization/risk state before starting each business operation.
"""

from __future__ import annotations

import re
import select
import socket
import struct
import sys
import zlib
from dataclasses import dataclass
from hashlib import sha256
from threading import get_ident
from time import monotonic_ns, thread_time_ns
from typing import Generic, Literal, TypeVar

import msgspec

from .fair_queue_v2 import BoundedFairOperationQueueV2, ConsumerOperationLaneV2
from .wire_v2 import MAX_FRAME_BYTES

MAX_OWNER_PACKET_BYTES = MAX_FRAME_BYTES + 8192
_HEADER = struct.Struct("!4sII")
_MAGIC = b"TCO1"
_U64_MAX = (1 << 64) - 1
_GENERATION = re.compile(r"[0-9a-f]{32}\Z")
_SCOPE = re.compile(r"[0-9a-f]{64}\Z")
_T = TypeVar("_T")


class OwnerProtocolError(ValueError):
    """No success receipt may be derived from an unverified owner message."""


def _uint(value: int, *, positive: bool = True) -> None:
    if type(value) is not int or not (1 if positive else 0) <= value <= _U64_MAX:
        raise OwnerProtocolError("owner integer outside protocol range")


def _text(value: str, maximum: int = 256) -> None:
    if type(value) is not str or not value or len(value.encode("utf-8")) > maximum:
        raise OwnerProtocolError("owner identifier outside protocol range")


def _generation(value: str) -> None:
    if type(value) is not str or not _GENERATION.fullmatch(value):
        raise OwnerProtocolError("invalid owner boot generation")


def _digest(value: bytes) -> None:
    if type(value) is not bytes or len(value) != 32:
        raise OwnerProtocolError("invalid owner content digest")


class OwnerAuthorityV1(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    consumer_id: str
    account_id: str
    strategy_id: str
    scope_sha256: str
    authorization_epoch: int

    def __post_init__(self) -> None:
        for value in (self.consumer_id, self.account_id, self.strategy_id):
            _text(value)
        if type(self.scope_sha256) is not str or not _SCOPE.fullmatch(self.scope_sha256):
            raise OwnerProtocolError("invalid owner authorization scope")
        _uint(self.authorization_epoch)


class OwnerPeerV1(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Original public SO_PEERCRED, supplied only by the trusted frontend."""

    pid: int
    uid: int
    gid: int

    def __post_init__(self) -> None:
        _uint(self.pid)
        _uint(self.uid, positive=False)
        _uint(self.gid, positive=False)
        if self.pid > 2**31 - 1 or max(self.uid, self.gid) > 2**32 - 1:
            raise OwnerProtocolError("owner original peer outside Linux ucred range")


class OwnerIdentityV1(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    generation: str
    serial: int
    authority: OwnerAuthorityV1
    request_id: str
    business_key: str
    request_sha256: bytes
    peer: OwnerPeerV1 | None = None

    def __post_init__(self) -> None:
        _generation(self.generation)
        _uint(self.serial)
        if type(self.authority) is not OwnerAuthorityV1:
            raise OwnerProtocolError("missing owner authority")
        _text(self.request_id)
        _text(self.business_key, 1024)
        _digest(self.request_sha256)
        if self.peer is not None and type(self.peer) is not OwnerPeerV1:
            raise OwnerProtocolError("invalid owner original peer")


class OwnerRequestV1(msgspec.Struct, tag="request", frozen=True, forbid_unknown_fields=True):
    identity: OwnerIdentityV1
    operation: Literal["SubmitSignal", "CancelOrder"]
    deadline_ns: int
    request_frame: bytes

    def __post_init__(self) -> None:
        if type(self.identity) is not OwnerIdentityV1 or self.operation not in (
            "SubmitSignal",
            "CancelOrder",
        ):
            raise OwnerProtocolError("invalid owner submission")
        _uint(self.deadline_ns)
        if (
            type(self.request_frame) is not bytes
            or not 1 <= len(self.request_frame) <= MAX_FRAME_BYTES
            or sha256(self.request_frame).digest() != self.identity.request_sha256
        ):
            raise OwnerProtocolError("owner request bytes/digest mismatch")


class OwnerCancelV1(msgspec.Struct, tag="cancel", frozen=True, forbid_unknown_fields=True):
    identity: OwnerIdentityV1

    def __post_init__(self) -> None:
        if type(self.identity) is not OwnerIdentityV1:
            raise OwnerProtocolError("invalid owner cancellation")


class OwnerReadyV1(msgspec.Struct, tag="ready", frozen=True, forbid_unknown_fields=True):
    generation: str
    owner_pid: int
    scope_sha256: str
    journal_id: str
    recovered_lsn: int
    source_prefix_sha256: bytes
    trace_name: str = ""

    def __post_init__(self) -> None:
        _generation(self.generation)
        _uint(self.owner_pid)
        if type(self.scope_sha256) is not str or not _SCOPE.fullmatch(self.scope_sha256):
            raise OwnerProtocolError("invalid recovered owner scope")
        _text(self.journal_id, 128)
        _uint(self.recovered_lsn, positive=False)
        _digest(self.source_prefix_sha256)
        if type(self.trace_name) is not str or (
            self.trace_name
            and not re.fullmatch(r"consumer-[0-9a-f]{32}-[0-9a-f]{32}\.jsonl", self.trace_name)
        ):
            raise OwnerProtocolError("invalid owner trace basename")


class OwnerStopV1(msgspec.Struct, tag="stop", frozen=True, forbid_unknown_fields=True):
    generation: str
    serial: int

    def __post_init__(self) -> None:
        _generation(self.generation)
        _uint(self.serial)


class OwnerStoppedV1(msgspec.Struct, tag="stopped", frozen=True, forbid_unknown_fields=True):
    generation: str
    serial: int
    owner_pid: int
    journal_id: str
    final_lsn: int
    managed_exit_sha256: bytes

    def __post_init__(self) -> None:
        _generation(self.generation)
        _uint(self.serial)
        _uint(self.owner_pid)
        _text(self.journal_id, 128)
        _uint(self.final_lsn, positive=False)
        _digest(self.managed_exit_sha256)


class OwnerEventV1(msgspec.Struct, tag="event", frozen=True, forbid_unknown_fields=True):
    identity: OwnerIdentityV1
    phase: Literal["STARTED", "COMPLETED", "NOT_STARTED", "UNKNOWN"]
    observed_ns: int
    response_frame: bytes = b""
    journal_id: str = ""
    source_lsn: int = 0
    source_prefix_sha256: bytes = b""
    reason: Literal["", "CANCELLED", "DEADLINE", "STOPPED", "PRECHECK"] = ""
    source_anchor: Literal["CURRENT_VERIFIED_PREFIX", "RECOVERED_PREFIX"] = (
        "CURRENT_VERIFIED_PREFIX"
    )

    def __post_init__(self) -> None:
        if type(self.identity) is not OwnerIdentityV1:
            raise OwnerProtocolError("invalid owner event identity")
        _uint(self.observed_ns)
        _uint(self.source_lsn, positive=False)
        if self.phase == "COMPLETED":
            if (
                type(self.response_frame) is not bytes
                or not 1 <= len(self.response_frame) <= MAX_FRAME_BYTES
                or self.reason
            ):
                raise OwnerProtocolError("invalid owner completed response")
            _text(self.journal_id, 128)
            _digest(self.source_prefix_sha256)
            if self.source_anchor not in ("CURRENT_VERIFIED_PREFIX", "RECOVERED_PREFIX"):
                raise OwnerProtocolError("invalid owner source anchor kind")
        elif self.phase in ("STARTED", "NOT_STARTED", "UNKNOWN"):
            if (
                self.response_frame != b""
                or self.journal_id
                or self.source_lsn
                or self.source_prefix_sha256 != b""
                or (self.phase == "NOT_STARTED") != bool(self.reason)
                or self.reason not in ("", "CANCELLED", "DEADLINE", "STOPPED", "PRECHECK")
                or self.source_anchor != "CURRENT_VERIFIED_PREFIX"
            ):
                raise OwnerProtocolError("invalid owner non-completion event")
        else:
            raise OwnerProtocolError("invalid owner phase")


class OwnerReadMessageV1(msgspec.Struct, frozen=True, forbid_unknown_fields=True, tag=True):
    identity: OwnerIdentityV1
    phase: Literal["REQUEST", "PREPARED", "CHECK", "GRANT", "RELEASE", "RELEASED"]
    frame: bytes = b""
    response_sha256: bytes = b""
    deadline_ns: int = 0

    def __post_init__(self) -> None:
        if type(self.frame) is not bytes or len(self.frame) > MAX_FRAME_BYTES:
            raise OwnerProtocolError("owner read frame outside fixed budget")
        if self.phase == "REQUEST":
            _uint(self.deadline_ns)
            if (
                not self.frame
                or sha256(self.frame).digest() != self.identity.request_sha256
                or self.response_sha256
            ):
                raise OwnerProtocolError("invalid owner read request")
        elif self.phase in ("PREPARED", "GRANT"):
            _digest(self.response_sha256)
            if (
                self.deadline_ns
                or (self.phase == "PREPARED" and not self.frame)
                or (self.frame and sha256(self.frame).digest() != self.response_sha256)
            ):
                raise OwnerProtocolError("invalid owner read reply")
        elif self.phase == "CHECK":
            _digest(self.response_sha256)
            if self.frame or self.deadline_ns:
                raise OwnerProtocolError("invalid owner read check")
        elif self.phase in ("RELEASE", "RELEASED"):
            if self.frame or self.response_sha256 or self.deadline_ns:
                raise OwnerProtocolError("invalid owner read release acknowledgement")
        else:
            raise OwnerProtocolError("invalid owner read phase")


OwnerMessageV1 = (
    OwnerRequestV1
    | OwnerCancelV1
    | OwnerReadyV1
    | OwnerEventV1
    | OwnerStopV1
    | OwnerStoppedV1
    | OwnerReadMessageV1
)
_DECODER = msgspec.msgpack.Decoder(OwnerMessageV1)


def encode_owner_packet(message: OwnerMessageV1) -> bytes:
    if type(message) not in (
        OwnerRequestV1,
        OwnerCancelV1,
        OwnerReadyV1,
        OwnerEventV1,
        OwnerStopV1,
        OwnerStoppedV1,
        OwnerReadMessageV1,
    ):
        raise OwnerProtocolError("unsupported owner message")
    payload = msgspec.msgpack.encode(message)
    if not 0 < len(payload) <= MAX_OWNER_PACKET_BYTES:
        raise OwnerProtocolError("owner packet exceeds fixed budget")
    return _HEADER.pack(_MAGIC, len(payload), zlib.crc32(payload)) + payload


def decode_owner_packet(packet: bytes) -> OwnerMessageV1:
    if type(packet) is not bytes or not _HEADER.size < len(packet) <= (
        _HEADER.size + MAX_OWNER_PACKET_BYTES
    ):
        raise OwnerProtocolError("owner packet truncated/oversized")
    magic, size, crc = _HEADER.unpack_from(packet)
    payload = packet[_HEADER.size :]
    if magic != _MAGIC or size != len(payload) or zlib.crc32(payload) != crc:
        raise OwnerProtocolError("owner packet framing/CRC mismatch")
    try:
        return _DECODER.decode(payload)
    except (msgspec.DecodeError, ValueError, TypeError) as error:
        raise OwnerProtocolError("owner packet schema invalid") from error


def send_owner_packet(
    channel: socket.socket,
    message: OwnerMessageV1,
    *,
    deadline_ns: int,
    diagnostic: dict[str, object] | None = None,
) -> None:
    """Send once, with a finite deadline. Partial send failures are UNKNOWN."""
    _uint(deadline_ns)
    if diagnostic is not None:
        diagnostic["completed_packet_encode_started_ns"] = monotonic_ns()
    packet = encode_owner_packet(message)
    if diagnostic is not None:
        diagnostic["completed_packet_encode_finished_ns"] = monotonic_ns()
    remaining = deadline_ns - monotonic_ns()
    if not 0 < remaining <= 60_000_000_000:
        raise OwnerProtocolError("owner send deadline expired/outside 60 seconds")
    if diagnostic is not None:
        diagnostic["completed_timeout_get_started_ns"] = monotonic_ns()
    previous_timeout = channel.gettimeout()
    if diagnostic is not None:
        diagnostic["completed_timeout_get_finished_ns"] = monotonic_ns()
    try:
        if diagnostic is not None:
            diagnostic["completed_timeout_set_started_ns"] = monotonic_ns()
            diagnostic["completed_timeout_set_cpu_started_ns"] = thread_time_ns()
        channel.settimeout(remaining / 1_000_000_000)
        if diagnostic is not None:
            diagnostic["completed_timeout_set_cpu_finished_ns"] = thread_time_ns()
            diagnostic["completed_timeout_set_finished_ns"] = monotonic_ns()
        if diagnostic is not None:
            diagnostic["completed_socket_send_started_ns"] = monotonic_ns()
            diagnostic["completed_socket_send_cpu_started_ns"] = thread_time_ns()
        channel.sendall(packet)
        if diagnostic is not None:
            diagnostic["completed_socket_send_cpu_finished_ns"] = thread_time_ns()
            diagnostic["completed_socket_send_finished_ns"] = monotonic_ns()
    finally:
        channel.settimeout(previous_timeout)


def send_owner_packet_nonblocking_candidate(
    channel: socket.socket, message: OwnerMessageV1, *, deadline_ns: int
) -> None:
    """Opt-in Linux candidate: one bounded send, never mutate socket mode or retry a message.

    A partial packet followed by any failure is UNKNOWN, exactly as for
    sendall. The caller must fence its owner and must never replay the packet.
    """
    if sys.platform != "linux" or not hasattr(socket, "MSG_DONTWAIT"):
        raise OwnerProtocolError("nonblocking candidate requires Linux MSG_DONTWAIT")
    _uint(deadline_ns)
    if channel.gettimeout() is not None:
        raise OwnerProtocolError("nonblocking candidate requires an un-timed owned socket")
    packet = encode_owner_packet(message)
    view = memoryview(packet)
    offset = 0
    while offset < len(packet):
        remaining = deadline_ns - monotonic_ns()
        if not 0 < remaining <= 60_000_000_000:
            raise OwnerProtocolError("owner send deadline expired/outside 60 seconds")
        try:
            sent = channel.send(view[offset:], socket.MSG_DONTWAIT)
        except BlockingIOError:
            _, writable, _ = select.select([], [channel], [], remaining / 1_000_000_000)
            if not writable:
                raise TimeoutError("owner candidate send deadline expired")
            continue
        if sent <= 0:
            raise OwnerProtocolError("owner candidate socket closed during send")
        offset += sent


def receive_owner_packet(channel: socket.socket, *, deadline_ns: int) -> OwnerMessageV1:
    """One bounded frame; EOF/timeout always raises, never means NOT_STARTED.

    The caller exclusively owns this socket and must fence outstanding writes
    on ANY failure. No reconnect, retry or trust of partially decoded content.
    """
    _uint(deadline_ns)
    if deadline_ns - monotonic_ns() > 60_000_000_000:
        raise OwnerProtocolError("owner receive deadline exceeds 60 seconds")
    previous_timeout = channel.gettimeout()

    def read(size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            remaining = deadline_ns - monotonic_ns()
            if remaining <= 0:
                raise TimeoutError("owner receive deadline expired")
            channel.settimeout(remaining / 1_000_000_000)
            part = channel.recv(min(65536, size - len(data)))
            if not part:
                raise OwnerProtocolError("owner channel EOF/truncated")
            data.extend(part)
        return bytes(data)

    try:
        header = read(_HEADER.size)
        magic, size, _ = _HEADER.unpack(header)
        if magic != _MAGIC or not 0 < size <= MAX_OWNER_PACKET_BYTES:
            raise OwnerProtocolError("owner header outside fixed budget")
        return decode_owner_packet(header + read(size))
    finally:
        channel.settimeout(previous_timeout)


@dataclass
class _Pending:
    identity: OwnerIdentityV1
    deadline_ns: int
    phase: str = "QUEUED"
    started_ns: int = 0
    cancel_requested: bool = False
    terminal: OwnerEventV1 | None = None


class OwnerCreditLedgerV1(Generic[_T]):
    """Single-reactor ledger borrowing existing ready WRITE credits.

    Completed-but-undelivered entries keep their credit. Local cancellation or
    socket loss NEVER releases an unresolved entry as if it hadn't executed.
    This is a bounded correlation table, not another request/frame queue.
    """

    def __init__(
        self,
        *,
        generation: str,
        scope_sha256: str,
        owner_pid: int,
        ready_queue: BoundedFairOperationQueueV2[_T],
        maximum_inflight: int = 128,
    ) -> None:
        _generation(generation)
        _uint(owner_pid)
        if type(scope_sha256) is not str or not _SCOPE.fullmatch(scope_sha256):
            raise OwnerProtocolError("invalid expected owner scope")
        if type(maximum_inflight) is not int or not 1 <= maximum_inflight <= 128:
            raise OwnerProtocolError("invalid owner credit budget")
        self._thread = get_ident()
        self._generation, self._scope, self._pid = generation, scope_sha256, owner_pid
        self._queue, self._maximum = ready_queue, maximum_inflight
        self._pending: dict[int, _Pending] = {}
        self._last_serial = 0
        self._ready: OwnerReadyV1 | None = None
        self._source_lsn = 0
        self._source_digest: bytes = b""
        self._completion_anchor: str | None = None
        self._running: int | None = None
        self._failed = self._stopped = False

    def _require_thread(self) -> None:
        if get_ident() != self._thread:
            raise OwnerProtocolError("owner ledger belongs to one reactor")

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def outstanding(self) -> int:
        return len(self._pending)

    def accept_ready(self, ready: OwnerReadyV1) -> None:
        self._require_thread()
        if type(ready) is not OwnerReadyV1 or (
            self._failed
            or self._stopped
            or self._ready is not None
            or (ready.generation, ready.scope_sha256, ready.owner_pid)
            != (self._generation, self._scope, self._pid)
        ):
            self.fail()
            raise OwnerProtocolError("owner readiness identity mismatch/repeated")
        self._ready, self._source_lsn = ready, ready.recovered_lsn
        self._source_digest = ready.source_prefix_sha256

    def reserve(self, request: OwnerRequestV1) -> bool:
        self._require_thread()
        identity = request.identity
        if self._failed or self._stopped or self._ready is None:
            return False
        if (
            identity.generation != self._generation
            or identity.authority.scope_sha256 != self._scope
            or identity.serial <= self._last_serial
        ):
            raise OwnerProtocolError("owner request scope/serial mismatch")
        if len(self._pending) >= self._maximum or not self._queue.reserve(
            ConsumerOperationLaneV2.WRITE
        ):
            return False
        self._pending[identity.serial] = _Pending(identity, request.deadline_ns)
        self._last_serial = identity.serial
        return True

    def request_cancel(self, serial: int) -> OwnerCancelV1 | None:
        self._require_thread()
        pending = self._pending[serial]
        if self._failed or pending.phase != "QUEUED" or pending.cancel_requested:
            return None
        pending.cancel_requested = True
        return OwnerCancelV1(pending.identity)  # request, NOT an acknowledgement

    def observe(self, event: OwnerEventV1) -> None:
        self._require_thread()
        try:
            if type(event) is not OwnerEventV1:
                raise OwnerProtocolError("unexpected owner message kind")
            self._observe(event)
        except OwnerProtocolError:
            self.fail()
            raise

    def _observe(self, event: OwnerEventV1) -> None:
        pending = self._pending.get(event.identity.serial)
        if self._failed or pending is None or event.identity != pending.identity:
            raise OwnerProtocolError("unrelated/stale owner response")
        if pending.terminal is not None:
            raise OwnerProtocolError("duplicate terminal owner response")
        if event.phase == "STARTED":
            first_queued = next(
                (serial for serial, value in self._pending.items() if value.phase == "QUEUED"),
                None,
            )
            if (
                pending.phase != "QUEUED"
                or self._running is not None
                or event.identity.serial != first_queued
                or event.observed_ns >= pending.deadline_ns
            ):
                raise OwnerProtocolError("owner start reordered/repeated/expired")
            pending.phase, pending.started_ns = "STARTED", event.observed_ns
            self._running = event.identity.serial
        elif event.phase == "COMPLETED":
            if (
                pending.phase != "STARTED"
                or self._running != event.identity.serial
                or event.observed_ns < pending.started_ns
                or self._ready is None
                or event.journal_id != self._ready.journal_id
                or event.source_lsn < self._source_lsn
                or (
                    event.source_anchor == "RECOVERED_PREFIX"
                    and event.source_prefix_sha256 != self._ready.source_prefix_sha256
                )
                or (
                    self._completion_anchor is not None
                    and event.source_anchor != self._completion_anchor
                )
                or (
                    event.source_lsn == self._source_lsn
                    and event.source_prefix_sha256 != self._source_digest
                )
            ):
                raise OwnerProtocolError("owner completion has no matching execution/source")
            self._source_lsn, self._running = event.source_lsn, None
            self._source_digest = event.source_prefix_sha256
            self._completion_anchor = event.source_anchor
            pending.phase, pending.terminal = event.phase, event
        elif event.phase == "NOT_STARTED":
            if (
                pending.phase != "QUEUED"
                or (event.reason == "CANCELLED" and not pending.cancel_requested)
                or (event.reason == "DEADLINE" and event.observed_ns < pending.deadline_ns)
            ):
                raise OwnerProtocolError("unproven owner pre-start cancellation")
            pending.phase, pending.terminal = event.phase, event
        else:
            self.fail()  # UNKNOWN is never an automatic retry or negative receipt

    def result(self, serial: int, *, current_authority: OwnerAuthorityV1) -> OwnerEventV1:
        self._require_thread()
        pending = self._pending[serial]
        if (
            self._failed
            or pending.terminal is None
            or current_authority != pending.identity.authority
        ):
            raise OwnerProtocolError("owner delivery unresolved/revoked/fenced")
        return pending.terminal

    def release(self, serial: int) -> None:
        self._require_thread()
        if self._pending[serial].terminal is None:
            raise OwnerProtocolError("cannot release unresolved owner credit")
        self._queue.release(ConsumerOperationLaneV2.WRITE)
        del self._pending[serial]

    def stop(self) -> None:
        self._require_thread()
        self._stopped = True

    def fail(self) -> None:
        self._require_thread()
        self._failed = self._stopped = True
        self._running = None
        for pending in self._pending.values():
            if pending.terminal is None:
                pending.phase = "UNKNOWN"
                pending.terminal = OwnerEventV1(pending.identity, "UNKNOWN", monotonic_ns())

    def require_drained(self) -> None:
        self._require_thread()
        if self._failed or self._pending:
            raise OwnerProtocolError("owner shutdown has unresolved/unknown work")
