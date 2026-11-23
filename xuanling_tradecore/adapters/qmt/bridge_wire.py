"""Length-prefixed canonical wire frames for the QMT mTLS bridge.

This is a Linux/Edge transport envelope, not the QMT local ring ABI. Network
payloads are canonical UTF-8 JSON with a binary fixed header; local rings never
use this module or JSON.
"""

from __future__ import annotations

import json
import socket
import struct
from dataclasses import dataclass
from enum import IntEnum
from hashlib import sha256
from typing import Mapping, cast


class QmtBridgeWireError(RuntimeError):
    """A bridge wire frame is malformed, oversized, or incompatible."""


WIRE_MAGIC = b"TCQB"
WIRE_VERSION = 1
WIRE_MAX_PAYLOAD = 1 << 20
_WIRE_HEADER = struct.Struct("!4sHHI32s")


class QmtBridgeWireKind(IntEnum):
    HELLO = 1
    COMMAND = 2
    ACCEPTED = 3
    EVENT = 4
    ACK = 5
    HEALTH = 6
    REJECTED = 7


@dataclass(frozen=True)
class QmtBridgeWireFrame:
    """One versioned, checksummed bridge record without a socket dependency."""

    kind: QmtBridgeWireKind
    payload: bytes

    def __post_init__(self) -> None:
        if len(self.payload) > WIRE_MAX_PAYLOAD:
            raise QmtBridgeWireError("QMT bridge wire payload exceeds the bounded limit")

    def encode(self) -> bytes:
        digest = sha256(self.payload).digest()
        return (
            _WIRE_HEADER.pack(WIRE_MAGIC, WIRE_VERSION, int(self.kind), len(self.payload), digest)
            + self.payload
        )

    @classmethod
    def decode(cls, raw: bytes) -> "QmtBridgeWireFrame":
        if len(raw) < _WIRE_HEADER.size:
            raise QmtBridgeWireError("QMT bridge wire frame is truncated")
        magic, version, raw_kind, payload_size, digest = _WIRE_HEADER.unpack_from(raw)
        if magic != WIRE_MAGIC or version != WIRE_VERSION:
            raise QmtBridgeWireError("QMT bridge wire ABI does not match")
        if payload_size > WIRE_MAX_PAYLOAD or len(raw) != _WIRE_HEADER.size + payload_size:
            raise QmtBridgeWireError("QMT bridge wire payload length is invalid")
        payload = raw[_WIRE_HEADER.size :]
        if sha256(payload).digest() != digest:
            raise QmtBridgeWireError("QMT bridge wire payload hash does not match")
        try:
            kind = QmtBridgeWireKind(raw_kind)
        except ValueError as error:
            raise QmtBridgeWireError("QMT bridge wire kind is unsupported") from error
        return cls(kind=kind, payload=payload)


def send_wire_frame(connection: socket.socket, frame: QmtBridgeWireFrame) -> None:
    """Write exactly one bounded frame to an already-authenticated stream."""

    try:
        connection.sendall(frame.encode())
    except OSError as error:
        raise QmtBridgeWireError("QMT bridge wire send failed") from error


def receive_wire_frame(connection: socket.socket) -> QmtBridgeWireFrame:
    """Read one frame without trusting a peer-provided length or kind."""

    header = _receive_exact(connection, _WIRE_HEADER.size)
    magic, version, _raw_kind, payload_size, _digest = _WIRE_HEADER.unpack(header)
    if magic != WIRE_MAGIC or version != WIRE_VERSION:
        raise QmtBridgeWireError("QMT bridge wire ABI does not match")
    if payload_size > WIRE_MAX_PAYLOAD:
        raise QmtBridgeWireError("QMT bridge wire payload length is invalid")
    payload = _receive_exact(connection, payload_size)
    return QmtBridgeWireFrame.decode(header + payload)


def canonical_wire_payload(value: Mapping[str, object]) -> bytes:
    """Encode a finite primitive mapping deterministically for the mTLS layer."""

    _validate_wire_value(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def decode_canonical_wire_payload(raw: bytes) -> Mapping[str, object]:
    try:
        decoded_object = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QmtBridgeWireError("QMT bridge wire payload is not UTF-8 JSON") from error
    if not isinstance(decoded_object, dict):
        raise QmtBridgeWireError("QMT bridge wire payload is not an object")
    decoded_fields = cast(dict[object, object], decoded_object)
    if not all(isinstance(key, str) for key in decoded_fields):
        raise QmtBridgeWireError("QMT bridge wire payload is not a text-keyed object")
    value = cast(Mapping[str, object], decoded_fields)
    _validate_wire_value(value)
    if canonical_wire_payload(value) != raw:
        raise QmtBridgeWireError("QMT bridge wire payload is not canonical")
    return value


def _validate_wire_value(value: object) -> None:
    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, list):
        for item in cast(list[object], value):
            _validate_wire_value(item)
        return
    if isinstance(value, Mapping):
        fields = cast(Mapping[object, object], value)
        for key, item in fields.items():
            if not isinstance(key, str):
                raise QmtBridgeWireError("QMT bridge wire object has a non-text key")
            _validate_wire_value(item)
        return
    raise QmtBridgeWireError("QMT bridge wire payload contains an unsupported value")


def _receive_exact(connection: socket.socket, size: int) -> bytes:
    received = bytearray()
    while len(received) < size:
        try:
            chunk = connection.recv(size - len(received))
        except OSError as error:
            raise QmtBridgeWireError("QMT bridge wire receive failed") from error
        if not chunk:
            raise QmtBridgeWireError("QMT bridge wire peer closed mid-frame")
        received.extend(chunk)
    return bytes(received)
