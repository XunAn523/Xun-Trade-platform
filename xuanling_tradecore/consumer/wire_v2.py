"""Strict ConsumerGateway V2 binary framing and versioned contract loader.

The fixed header is network byte order and contains no business or identity
text. JSON is decoded only after a complete frame, header validation, size
validation, and CRC verification have succeeded.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from enum import IntEnum
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, cast

import msgspec
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from ..domain import ConsumerApiVersion, ConsumerWireContractV2
from ..domain._validation import DomainValidationError, require_nonnegative
from .payload_v2 import canonical_payload_bytes

MAGIC = b"XTC2"
HEADER_VERSION = 1
API_MAJOR = 2
API_MINOR = 0
PAYLOAD_SCHEMA_VERSION = 2
HEADER_SIZE_BYTES = 32
MAX_PAYLOAD_BYTES = 1_048_576
MAX_FRAME_BYTES = HEADER_SIZE_BYTES + MAX_PAYLOAD_BYTES
_HEADER = struct.Struct(">4sBBHHHHHQII")
_SUPPORTED_FLAGS = 0
_JSON_DECODER = msgspec.json.Decoder()


class ConsumerWireErrorCode(str):
    """Stable wire error-code constants shared with the contract catalog."""

    FRAME_INCOMPLETE = "FRAME_INCOMPLETE"
    BAD_MAGIC = "BAD_MAGIC"
    UNSUPPORTED_HEADER_VERSION = "UNSUPPORTED_HEADER_VERSION"
    UNSUPPORTED_API_VERSION = "UNSUPPORTED_API_VERSION"
    FRAME_TOO_LARGE = "FRAME_TOO_LARGE"
    INVALID_FLAGS = "INVALID_FLAGS"
    PAYLOAD_CRC_MISMATCH = "PAYLOAD_CRC_MISMATCH"
    INVALID_UTF8 = "INVALID_UTF8"
    INVALID_JSON = "INVALID_JSON"
    SCHEMA_VALIDATION_FAILED = "SCHEMA_VALIDATION_FAILED"
    UNKNOWN_CRITICAL_FIELD = "UNKNOWN_CRITICAL_FIELD"
    EVENT_SCHEMA_INCOMPATIBLE = "EVENT_SCHEMA_INCOMPATIBLE"
    UPGRADE_REQUIRED = "UPGRADE_REQUIRED"
    CURSOR_INVALID = "CURSOR_INVALID"
    CURSOR_EXPIRED = "CURSOR_EXPIRED"
    JOURNAL_GAP = "JOURNAL_GAP"
    CURSOR_RECOVERY_UNAVAILABLE = "CURSOR_RECOVERY_UNAVAILABLE"
    STREAM_RESET_MISMATCH = "STREAM_RESET_MISMATCH"
    AUDIT_EVIDENCE_UNAVAILABLE = "AUDIT_EVIDENCE_UNAVAILABLE"
    EVENT_BACKPRESSURE = "EVENT_BACKPRESSURE"


class ConsumerWireError(ValueError):
    """Fail-closed frame or payload rejection with a stable public code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class MessageKind(IntEnum):
    REQUEST = 1
    RESPONSE = 2
    EVENT = 3
    ERROR = 4


@dataclass(frozen=True)
class FrameHeaderV2:
    message_kind: MessageKind
    flags: int
    api_major: int
    api_minor: int
    schema_version: int
    correlation_id: int
    payload_length: int
    payload_crc32: int

    def __post_init__(self) -> None:
        require_nonnegative(self.flags, "wire flags")
        require_nonnegative(self.correlation_id, "wire correlation_id")
        require_nonnegative(self.payload_length, "wire payload_length")
        require_nonnegative(self.payload_crc32, "wire payload_crc32")
        if self.flags != _SUPPORTED_FLAGS:
            raise ConsumerWireError(
                ConsumerWireErrorCode.INVALID_FLAGS,
                "Consumer V2 has no negotiated non-zero frame flags",
            )
        if (self.api_major, self.api_minor) != (API_MAJOR, API_MINOR):
            raise ConsumerWireError(
                ConsumerWireErrorCode.UNSUPPORTED_API_VERSION,
                "unsupported Consumer API version",
            )
        if self.schema_version != PAYLOAD_SCHEMA_VERSION:
            raise ConsumerWireError(
                ConsumerWireErrorCode.UNSUPPORTED_API_VERSION,
                "unsupported Consumer payload schema version",
            )
        if self.payload_length > MAX_PAYLOAD_BYTES:
            raise ConsumerWireError(
                ConsumerWireErrorCode.FRAME_TOO_LARGE,
                "Consumer frame payload exceeds the fixed maximum",
            )


@dataclass(frozen=True)
class DecodedFrameV2:
    header: FrameHeaderV2
    payload: Mapping[str, object]


def encode_frame(
    message_kind: MessageKind,
    correlation_id: int,
    payload: Mapping[str, object],
) -> bytes:
    """Encode one canonical UTF-8 JSON payload in a strict V2 frame."""

    require_nonnegative(correlation_id, "wire correlation_id")
    if correlation_id > (1 << 64) - 1:
        raise DomainValidationError("wire correlation_id exceeds uint64")
    try:
        payload_bytes = canonical_payload_bytes(payload)
    except (TypeError, ValueError) as exc:
        raise ConsumerWireError(
            ConsumerWireErrorCode.INVALID_JSON,
            "payload cannot be represented as canonical JSON",
        ) from exc
    if len(payload_bytes) > MAX_PAYLOAD_BYTES:
        raise ConsumerWireError(
            ConsumerWireErrorCode.FRAME_TOO_LARGE,
            "Consumer frame payload exceeds the fixed maximum",
        )
    checksum = zlib.crc32(payload_bytes)
    header = _HEADER.pack(
        MAGIC,
        HEADER_VERSION,
        int(message_kind),
        _SUPPORTED_FLAGS,
        API_MAJOR,
        API_MINOR,
        PAYLOAD_SCHEMA_VERSION,
        0,
        correlation_id,
        len(payload_bytes),
        checksum,
    )
    return header + payload_bytes


def decode_frame(frame: object) -> DecodedFrameV2:
    """Decode exactly one complete frame; trailing bytes are rejected."""

    if not isinstance(frame, bytes):
        raise ConsumerWireError(
            ConsumerWireErrorCode.FRAME_INCOMPLETE,
            "frame must be immutable bytes",
        )
    if len(frame) < HEADER_SIZE_BYTES:
        raise ConsumerWireError(
            ConsumerWireErrorCode.FRAME_INCOMPLETE,
            "fixed Consumer V2 header is incomplete",
        )
    (
        magic,
        header_version,
        message_kind_raw,
        flags,
        api_major,
        api_minor,
        schema_version,
        reserved,
        correlation_id,
        payload_length,
        payload_crc32,
    ) = _HEADER.unpack_from(frame, 0)
    if magic != MAGIC:
        raise ConsumerWireError(ConsumerWireErrorCode.BAD_MAGIC, "invalid Consumer V2 magic")
    if header_version != HEADER_VERSION or reserved != 0:
        raise ConsumerWireError(
            ConsumerWireErrorCode.UNSUPPORTED_HEADER_VERSION,
            "unsupported Consumer V2 header version or reserved bits",
        )
    try:
        message_kind = MessageKind(message_kind_raw)
    except ValueError as exc:
        raise ConsumerWireError(
            ConsumerWireErrorCode.UNSUPPORTED_HEADER_VERSION,
            "unknown Consumer V2 message kind",
        ) from exc
    header = FrameHeaderV2(
        message_kind=message_kind,
        flags=flags,
        api_major=api_major,
        api_minor=api_minor,
        schema_version=schema_version,
        correlation_id=correlation_id,
        payload_length=payload_length,
        payload_crc32=payload_crc32,
    )
    expected_size = HEADER_SIZE_BYTES + header.payload_length
    if len(frame) != expected_size:
        code = (
            ConsumerWireErrorCode.FRAME_INCOMPLETE
            if len(frame) < expected_size
            else ConsumerWireErrorCode.INVALID_JSON
        )
        raise ConsumerWireError(code, "frame length does not equal declared payload length")
    # Decode directly from the immutable frame buffer.  The former slices
    # copied both the fixed header and the complete payload for every UDS
    # request, even though struct, crc32 and msgspec all accept a buffer.
    payload_view = memoryview(frame)[HEADER_SIZE_BYTES:]
    if zlib.crc32(payload_view) != header.payload_crc32:
        raise ConsumerWireError(
            ConsumerWireErrorCode.PAYLOAD_CRC_MISMATCH,
            "payload checksum does not match the fixed header",
        )
    try:
        decoded = _JSON_DECODER.decode(payload_view)
    except msgspec.DecodeError as exc:
        # Keep the public error taxonomy stable without decoding valid input
        # twice. UTF-8 classification is needed only on the rejected path.
        try:
            bytes(payload_view).decode("utf-8")
        except UnicodeDecodeError as unicode_exc:
            raise ConsumerWireError(
                ConsumerWireErrorCode.INVALID_UTF8,
                "payload is not valid UTF-8",
            ) from unicode_exc
        raise ConsumerWireError(
            ConsumerWireErrorCode.INVALID_JSON,
            "payload is not valid JSON",
        ) from exc
    if not isinstance(decoded, dict):
        raise ConsumerWireError(
            ConsumerWireErrorCode.INVALID_JSON,
            "payload root must be a JSON object",
        )
    return DecodedFrameV2(header=header, payload=cast(Mapping[str, object], decoded))


def validate_payload(
    payload: Mapping[str, object],
    schema: Mapping[str, object],
    *,
    compiled_validator: Draft202012Validator | None = None,
) -> None:
    """Validate an already framed payload against Draft 2020-12."""

    try:
        validator = compiled_validator or Draft202012Validator(cast(dict[str, Any], schema))
        validator.validate(  # pyright: ignore[reportUnknownMemberType] # jsonschema exposes dynamic validator protocols without complete typing.
            dict(payload)
        )
    except ValidationError as exc:
        code = (
            ConsumerWireErrorCode.UNKNOWN_CRITICAL_FIELD
            if exc.validator in {"additionalProperties", "unevaluatedProperties"}
            else ConsumerWireErrorCode.SCHEMA_VALIDATION_FAILED
        )
        raise ConsumerWireError(
            code,
            "payload does not satisfy the Consumer V2 schema",
        ) from exc


def _tree_hash(
    paths: tuple[Path, ...],
    root: Path,
    *,
    normalize_text: bool = False,
) -> str:
    digest = sha256()
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        content = path.read_bytes()
        if normalize_text:
            content = content.replace(b"\r\n", b"\n")
        digest.update(struct.pack(">I", len(relative)))
        digest.update(relative)
        digest.update(struct.pack(">Q", len(content)))
        digest.update(content)
    return digest.hexdigest()


def load_contract_identity(contract_root: Path) -> ConsumerWireContractV2:
    """Verify repository contract assets and return their immutable identity."""

    root = contract_root.resolve()
    hash_path = root / "schema-hashes.json"
    try:
        declared_raw = json.loads(hash_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DomainValidationError("Consumer V2 schema-hashes.json is unavailable") from exc
    if not isinstance(declared_raw, dict):
        raise DomainValidationError("Consumer V2 schema hashes must be a JSON object")
    declared = cast(dict[str, object], declared_raw)
    schema_paths = tuple((root / "schemas").glob("*.json"))
    golden_paths = tuple((root / "golden").glob("*.bin"))
    compatibility_path = root / "compatibility-matrix.json"
    if not schema_paths or not golden_paths or not compatibility_path.is_file():
        raise DomainValidationError("Consumer V2 contract assets are incomplete")
    actual = {
        "schema_hash": _tree_hash(schema_paths, root, normalize_text=True),
        "golden_vectors_hash": _tree_hash(golden_paths, root),
        "compatibility_matrix_hash": sha256(
            compatibility_path.read_bytes().replace(b"\r\n", b"\n")
        ).hexdigest(),
    }
    if any(declared.get(key) != value for key, value in actual.items()):
        raise DomainValidationError("Consumer V2 contract asset hash mismatch")
    return ConsumerWireContractV2(
        api_version=ConsumerApiVersion(API_MAJOR, API_MINOR),
        schema_hash=actual["schema_hash"],
        golden_vectors_hash=actual["golden_vectors_hash"],
        compatibility_matrix_hash=actual["compatibility_matrix_hash"],
        header_size_bytes=HEADER_SIZE_BYTES,
        max_frame_bytes=MAX_FRAME_BYTES,
    )
