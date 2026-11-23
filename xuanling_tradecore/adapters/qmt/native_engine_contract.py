"""Fixed binary ABI shared by the Edge and the QMT embedded Native Engine.

The QMT model receives a Python 3.6 compatible copy of these constants during
deployment.  The ABI intentionally uses only fixed-size structs and raw bytes:
JSON, pickle and Python object sharing are forbidden on the SPSC rings.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from hashlib import sha256


class QmtNativeContractError(RuntimeError):
    """A native ring header, frame, CRC or sequence invariant failed."""


RING_MAGIC = b"TCQMT001"
# Revision 1 existed with both unaligned and aligned counters. Reject both;
# guessing from counters would accept an empty legacy ring ambiguously.
RING_ABI_VERSION = 2
RING_HEADER_SIZE = 128
RING_SLOT_SIZE = 512
RING_COMMIT_OFFSET = RING_SLOT_SIZE - 8
RING_BODY_SIZE = RING_COMMIT_OFFSET
RING_SCOPE_DIGEST_SIZE = 32
RING_GENERATION_SIZE = 16
RING_DIGEST_SIZE = 32

_HEADER = struct.Struct("<8sHH32s16sII4xQQQQ")
_SLOT_PREFIX = struct.Struct("<QHHI32s32sQQQQI")
_COMMIT = struct.Struct("<Q")
RING_PAYLOAD_SIZE = RING_BODY_SIZE - _SLOT_PREFIX.size


class QmtNativeFrameKind(IntEnum):
    """Fixed command/event frame kinds; unknown values are rejected."""

    SUBMIT = 1
    CANCEL = 2
    QUERY = 3
    CALL_STARTED = 10
    CALL_RETURNED = 11
    ORDER_CALLBACK = 20
    TRADE_CALLBACK = 21
    CANCEL_CALLBACK = 22
    QUERY_ROW = 23
    QUERY_COMPLETE = 24
    FAULT = 30
    HEALTH = 31


@dataclass(frozen=True)
class QmtRingHeaderV1:
    """Header value API retained for compatibility; wire revision is ABI 2."""

    scope_digest: bytes
    generation: bytes
    slot_size: int
    capacity: int
    producer_sequence: int = 0
    consumer_sequence: int = 0
    producer_heartbeat_ns: int = 0
    consumer_heartbeat_ns: int = 0

    def __post_init__(self) -> None:
        if len(self.scope_digest) != RING_SCOPE_DIGEST_SIZE:
            raise QmtNativeContractError("QMT ring scope digest length is invalid")
        if len(self.generation) != RING_GENERATION_SIZE:
            raise QmtNativeContractError("QMT ring generation length is invalid")
        if self.slot_size != RING_SLOT_SIZE:
            raise QmtNativeContractError("QMT ring slot size is invalid")
        if self.capacity <= 1:
            raise QmtNativeContractError("QMT ring capacity must exceed one slot")
        for value in (
            self.producer_sequence,
            self.consumer_sequence,
            self.producer_heartbeat_ns,
            self.consumer_heartbeat_ns,
        ):
            if value < 0:
                raise QmtNativeContractError("QMT ring counters must be nonnegative")

    @property
    def mapping_size(self) -> int:
        return RING_HEADER_SIZE + self.slot_size * self.capacity

    def pack(self) -> bytes:
        payload = bytearray(RING_HEADER_SIZE)
        _HEADER.pack_into(
            payload,
            0,
            RING_MAGIC,
            RING_ABI_VERSION,
            0,
            self.scope_digest,
            self.generation,
            self.slot_size,
            self.capacity,
            self.producer_sequence,
            self.consumer_sequence,
            self.producer_heartbeat_ns,
            self.consumer_heartbeat_ns,
        )
        return bytes(payload)

    @classmethod
    def unpack(cls, raw: bytes) -> "QmtRingHeaderV1":
        if len(raw) < RING_HEADER_SIZE:
            raise QmtNativeContractError("QMT ring header is truncated")
        (
            magic,
            version,
            reserved,
            scope_digest,
            generation,
            slot_size,
            capacity,
            producer_sequence,
            consumer_sequence,
            producer_heartbeat_ns,
            consumer_heartbeat_ns,
        ) = _HEADER.unpack_from(raw)
        if magic != RING_MAGIC or version != RING_ABI_VERSION or reserved != 0:
            raise QmtNativeContractError("QMT ring header ABI does not match")
        return cls(
            scope_digest=scope_digest,
            generation=generation,
            slot_size=slot_size,
            capacity=capacity,
            producer_sequence=producer_sequence,
            consumer_sequence=consumer_sequence,
            producer_heartbeat_ns=producer_heartbeat_ns,
            consumer_heartbeat_ns=consumer_heartbeat_ns,
        )


@dataclass(frozen=True)
class QmtNativeFrameV1:
    """One committed-ring body before its final commit-sequence store."""

    sequence: int
    kind: QmtNativeFrameKind
    request_digest: bytes
    intent_digest: bytes
    timestamps_ns: tuple[int, int, int, int]
    payload: bytes
    flags: int = 0

    def __post_init__(self) -> None:
        if self.sequence < 0 or self.flags < 0 or self.flags > 0xFFFF:
            raise QmtNativeContractError("QMT native frame sequence or flags are invalid")
        if (
            len(self.request_digest) != RING_DIGEST_SIZE
            or len(self.intent_digest) != RING_DIGEST_SIZE
        ):
            raise QmtNativeContractError("QMT native frame digest length is invalid")
        if len(self.payload) > RING_PAYLOAD_SIZE:
            raise QmtNativeContractError("QMT native frame payload exceeds fixed slot capacity")
        if len(self.timestamps_ns) != 4 or any(value < 0 for value in self.timestamps_ns):
            raise QmtNativeContractError("QMT native frame timestamps are invalid")

    def encode_uncommitted(self) -> bytes:
        body = bytearray(RING_BODY_SIZE)
        _SLOT_PREFIX.pack_into(
            body,
            0,
            self.sequence,
            int(self.kind),
            self.flags,
            len(self.payload),
            self.request_digest,
            self.intent_digest,
            *self.timestamps_ns,
            0,
        )
        body[_SLOT_PREFIX.size : _SLOT_PREFIX.size + len(self.payload)] = self.payload
        crc = zlib.crc32(body) & 0xFFFFFFFF
        _SLOT_PREFIX.pack_into(
            body,
            0,
            self.sequence,
            int(self.kind),
            self.flags,
            len(self.payload),
            self.request_digest,
            self.intent_digest,
            *self.timestamps_ns,
            crc,
        )
        return bytes(body) + (b"\x00" * _COMMIT.size)

    @classmethod
    def decode_committed(cls, raw: bytes, *, expected_sequence: int) -> "QmtNativeFrameV1":
        if len(raw) != RING_SLOT_SIZE:
            raise QmtNativeContractError("QMT native frame slot size is invalid")
        commit_sequence = _COMMIT.unpack_from(raw, RING_COMMIT_OFFSET)[0]
        if commit_sequence != expected_sequence:
            raise QmtNativeContractError("QMT native frame commit sequence does not match")
        (
            sequence,
            raw_kind,
            flags,
            payload_length,
            request_digest,
            intent_digest,
            first_timestamp,
            second_timestamp,
            third_timestamp,
            fourth_timestamp,
            expected_crc,
        ) = _SLOT_PREFIX.unpack_from(raw)
        if sequence != expected_sequence or payload_length > RING_PAYLOAD_SIZE:
            raise QmtNativeContractError("QMT native frame sequence or payload length is invalid")
        body = bytearray(raw[:RING_BODY_SIZE])
        _SLOT_PREFIX.pack_into(
            body,
            0,
            sequence,
            raw_kind,
            flags,
            payload_length,
            request_digest,
            intent_digest,
            first_timestamp,
            second_timestamp,
            third_timestamp,
            fourth_timestamp,
            0,
        )
        if zlib.crc32(body) & 0xFFFFFFFF != expected_crc:
            raise QmtNativeContractError("QMT native frame CRC does not match")
        try:
            kind = QmtNativeFrameKind(raw_kind)
        except ValueError as error:
            raise QmtNativeContractError("QMT native frame kind is unsupported") from error
        payload_start = _SLOT_PREFIX.size
        return cls(
            sequence=sequence,
            kind=kind,
            request_digest=request_digest,
            intent_digest=intent_digest,
            timestamps_ns=(first_timestamp, second_timestamp, third_timestamp, fourth_timestamp),
            payload=bytes(raw[payload_start : payload_start + payload_length]),
            flags=flags,
        )


def qmt_native_abi_digest() -> str:
    """Return the manifest-pinned ABI digest without host-specific data."""

    payload = b"|".join(
        (
            RING_MAGIC,
            str(RING_ABI_VERSION).encode("ascii"),
            str(RING_HEADER_SIZE).encode("ascii"),
            str(RING_SLOT_SIZE).encode("ascii"),
            str(_HEADER.format).encode("ascii"),
            str(_SLOT_PREFIX.format).encode("ascii"),
            ",".join(f"{item.name}:{int(item)}" for item in QmtNativeFrameKind).encode("ascii"),
        )
    )
    return sha256(payload).hexdigest()
