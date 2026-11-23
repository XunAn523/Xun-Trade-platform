"""Bounded, private, fixed-record consumer evidence with chained segment hashes.

No request/response body, account, security, price, token or exception text is
accepted. Only timing/count/status and digests cross this writer API. RAM does
not grow with request count. Raw records, not HDR, remain the measurement source.
"""

from __future__ import annotations

import json
import os
import struct
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json, open_private

# seq, planned, call, connect, sent, decoded, validated, window_end, journal_lsn;
# operation, group, result, count; request-id digest, order/result-set digest.
RECORD = struct.Struct("<9Q4I32s32s")
SCHEMA = "consumer-longrun-fixed-record-v1"


@dataclass(frozen=True)
class ConsumerTimingRecord:
    sequence: int
    planned_ns: int
    call_ns: int
    connect_ns: int
    sent_ns: int
    decoded_ns: int
    validated_ns: int
    window_end_ns: int
    journal_lsn: int
    operation: int
    group: int
    result: int
    count: int
    request_digest: bytes
    result_digest: bytes

    def encode(self) -> bytes:
        values = (
            self.sequence,
            self.planned_ns,
            self.call_ns,
            self.connect_ns,
            self.sent_ns,
            self.decoded_ns,
            self.validated_ns,
            self.window_end_ns,
            self.journal_lsn,
            self.operation,
            self.group,
            self.result,
            self.count,
        )
        if (
            any(type(v) is not int or v < 0 for v in values)
            or any(v >= 2**64 for v in values[:9])
            or any(v >= 2**32 for v in values[9:])
            or type(self.request_digest) is not bytes
            or len(self.request_digest) != 32
            or type(self.result_digest) is not bytes
            or len(self.result_digest) != 32
        ):
            raise ValueError("invalid bounded evidence record")
        times = (
            self.planned_ns,
            self.call_ns,
            self.connect_ns,
            self.sent_ns,
            self.decoded_ns,
            self.validated_ns,
        )
        present = tuple(v for v in times if v)
        if present != tuple(sorted(present)):
            raise ValueError("evidence times are reversed")
        if self.result == 0 and any(not v for v in times):
            raise ValueError("successful evidence needs every measured timestamp")
        return RECORD.pack(*values, self.request_digest, self.result_digest)


class ConsumerEvidenceWriter:
    def __init__(
        self,
        *,
        root: Path,
        segment_records: int = 400_000,
        maximum_bytes: int = 64 * 1024**3,
        maximum_segments: int = 8192,
    ) -> None:
        if (
            any(
                type(v) is not int or v < 1
                for v in (segment_records, maximum_bytes, maximum_segments)
            )
            or segment_records * RECORD.size > maximum_bytes
            or maximum_segments > 8192
        ):
            raise ValueError("evidence budget is invalid")
        root.mkdir(mode=0o700, exist_ok=False)
        self._root, self._limit, self._quota, self._max_segments = (
            root,
            segment_records,
            maximum_bytes,
            maximum_segments,
        )
        self._bytes = self._total = self._count = 0
        self._entries: list[dict[str, object]] = []
        self._chain = "0" * 64
        self._hash = sha256()
        self._file = None
        self._closed = self._failed = False
        self._open()

    def _open(self) -> None:
        if len(self._entries) >= self._max_segments:
            raise ValueError("evidence segment manifest budget exhausted")
        path = self._root / f"raw-{len(self._entries):06d}.bin"
        self._file = os.fdopen(
            open_private(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY), "wb", buffering=1024**2
        )
        self._hash, self._count = sha256(), 0

    def append(self, record: ConsumerTimingRecord) -> None:
        if self._closed or self._failed or self._file is None:
            raise ValueError("evidence writer is not usable")
        encoded = record.encode()
        try:
            if self._bytes + len(encoded) > self._quota:
                raise ValueError("evidence disk quota exhausted")
            self._file.write(encoded)
            self._hash.update(encoded)
            self._bytes += len(encoded)
            self._total += 1
            self._count += 1
            if self._count == self._limit:
                self._seal()
                self._open()
        except BaseException:
            self._failed = True
            raise

    def _seal(self) -> None:
        if self._file is None:
            return
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        except BaseException:
            self._failed = True
            raise
        finally:
            self._file.close()
            self._file = None
        digest = self._hash.hexdigest()
        self._chain = sha256(
            bytes.fromhex(self._chain) + bytes.fromhex(digest) + struct.pack("<Q", self._count)
        ).hexdigest()
        self._entries.append(
            {
                "file": f"raw-{len(self._entries):06d}.bin",
                "records": self._count,
                "bytes": self._count * RECORD.size,
                "sha256": digest,
                "chain": self._chain,
            }
        )
        self._publish(False)

    def _publish(self, closed: bool) -> None:
        atomic_runtime_json(
            self._root / "manifest.json",
            {
                "schema": SCHEMA,
                "record_bytes": RECORD.size,
                "records": self._total,
                "bytes": self._bytes,
                "closed": closed,
                "failed": self._failed,
                "chain": self._chain,
                "segments": self._entries,
            },
        )

    def close(self) -> None:
        if not self._closed:
            try:
                self._seal()
                self._publish(True)
            finally:
                self._closed = True


def verify_consumer_evidence(root: Path) -> tuple[int, int]:
    """Offline streaming integrity verification, not a throughput/SLO verdict."""
    manifest = root / "manifest.json"
    if root.is_symlink() or manifest.is_symlink() or manifest.stat().st_size > 8 * 1024**2:
        raise ValueError("unsafe/oversized evidence manifest")
    raw = json.loads(manifest.read_text())
    if (
        raw.get("schema") != SCHEMA
        or raw.get("record_bytes") != RECORD.size
        or raw.get("closed") is not True
        or raw.get("failed") is not False
        or not isinstance(raw.get("segments"), list)
        or len(raw["segments"]) > 8192
    ):
        raise ValueError("incomplete evidence manifest")
    expected = {"manifest.json", *(f"raw-{n:06d}.bin" for n in range(len(raw["segments"])))}
    if {p.name for p in root.iterdir()} != expected:
        raise ValueError("evidence directory has missing/unregistered files")
    chain, count, size = "0" * 64, 0, 0
    for index, entry in enumerate(raw["segments"]):
        name = f"raw-{index:06d}.bin"
        if entry.get("file") != name:
            raise ValueError("evidence segment order/path invalid")
        path = root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("evidence segment missing/unsafe")
        digest, bytes_read = sha256(), 0
        with path.open("rb") as source:
            while chunk := source.read(1024**2):
                digest.update(chunk)
                bytes_read += len(chunk)
        if (
            bytes_read % RECORD.size
            or bytes_read != entry["bytes"]
            or digest.hexdigest() != entry["sha256"]
        ):
            raise ValueError("evidence segment integrity mismatch")
        records = bytes_read // RECORD.size
        chain = sha256(
            bytes.fromhex(chain) + digest.digest() + struct.pack("<Q", records)
        ).hexdigest()
        if entry["records"] != records or entry["chain"] != chain:
            raise ValueError("evidence chain mismatch")
        count += records
        size += bytes_read
    if (count, size, chain) != (raw["records"], raw["bytes"], raw["chain"]):
        raise ValueError("evidence total mismatch")
    return count, size
