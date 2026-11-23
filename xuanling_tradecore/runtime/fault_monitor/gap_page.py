"""Fixed-layout, payload-free monitoring gap page encoding.

The page is deliberately a value codec.  A Linux supervisor may map and
update it through a separately reviewed atomic/native binding, while normal
TradeCore modules can validate evidence without sharing mutable memory.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from ...domain._validation import DomainValidationError, require_nonnegative, require_positive

_MAGIC = b"XTFG"
_VERSION = 1
_LAYOUT = struct.Struct(">4sHHQQQQQQ")
RUNTIME_FAULT_GAP_PAGE_V1_BYTES = _LAYOUT.size


@dataclass(frozen=True)
class RuntimeFaultGapPageV1:
    """The fixed supervisor evidence envelope, containing no business bytes."""

    process_epoch: int
    active: bool
    first_sequence: int
    last_sequence: int
    dropped_count: int
    severity_bitmap: int
    heartbeat_monotonic_ns: int

    def __post_init__(self) -> None:
        require_positive(self.process_epoch, "fault gap process epoch")
        for value, name in (
            (self.first_sequence, "fault gap first sequence"),
            (self.last_sequence, "fault gap last sequence"),
            (self.dropped_count, "fault gap dropped count"),
            (self.severity_bitmap, "fault gap severity bitmap"),
            (self.heartbeat_monotonic_ns, "fault gap heartbeat"),
        ):
            require_nonnegative(value, name)
        if self.active and (self.dropped_count == 0 or self.first_sequence > self.last_sequence):
            raise DomainValidationError("active fault gap page has invalid gap range")
        if not self.active and any(
            (self.first_sequence, self.last_sequence, self.dropped_count, self.severity_bitmap)
        ):
            raise DomainValidationError("inactive fault gap page retains gap state")

    def encode(self) -> bytes:
        return _LAYOUT.pack(
            _MAGIC,
            _VERSION,
            int(self.active),
            self.process_epoch,
            self.first_sequence,
            self.last_sequence,
            self.dropped_count,
            self.severity_bitmap,
            self.heartbeat_monotonic_ns,
        )

    @classmethod
    def decode(cls, value: bytes) -> RuntimeFaultGapPageV1:
        if len(value) != _LAYOUT.size:
            raise DomainValidationError("fault gap page has invalid fixed size")
        magic, version, active, epoch, first, last, count, bitmap, heartbeat = _LAYOUT.unpack(value)
        if magic != _MAGIC or version != _VERSION or active not in {0, 1}:
            raise DomainValidationError("fault gap page has unsupported layout")
        return cls(epoch, bool(active), first, last, count, bitmap, heartbeat)
