"""File-backed mmap fact journal for the non-dispatching Consumer UDS service.

The Consumer data plane needs a real journal boundary even while its benchmark
profile deliberately stops before EMS or an Adapter.  This module stores the
complete canonical ``OmsJournalFact`` in a fixed-size, file-backed mmap frame.
The frame body, CRC and final commit word make a torn tail explicit; there is
never an implicit fallback to an in-memory journal.

The commit word is the last hot-path store.  No ``flush``, ``msync`` or
``fsync`` occurs per request: this is a process-crash fence backed by the OS
page cache, not a power-loss durability claim.
"""

from __future__ import annotations

import json
import mmap
import os
import struct
import threading
import zlib
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Final, cast

import msgspec

from .order_book import (
    OmsFactJournal,
    OmsFactKind,
    OmsJournalCommit,
    OmsJournalFact,
    OmsOrderBookError,
)


class MmapOmsFactJournalError(OmsOrderBookError):
    """The file-backed Consumer fact fence cannot safely accept another fact."""


class MmapOmsFactJournalFull(MmapOmsFactJournalError):
    """The fixed preallocated Consumer fact journal has reached capacity."""


class MmapOmsFactJournalRecoveryRequired(MmapOmsFactJournalError):
    """A non-empty file has an untrusted tail and must not be appended."""


_FILE_MAGIC: Final = b"TCOMSFJ1"
_FRAME_MAGIC: Final = b"TCF1"
_VERSION: Final = 1
_HEADER_BYTES: Final = 4_096
_FRAME_BYTES: Final = 4_096
_COMMIT_BYTES: Final = 8
_COMMIT_XOR: Final = 0x71C43AF29B6E10D5
_FILE_HEADER = struct.Struct("<8sIIQ")
_FRAME_HEADER = struct.Struct("<4sIQQHHI")
_CRC = struct.Struct("<I")
_COMMIT = struct.Struct("<Q")
_BODY_BYTES: Final = _FRAME_BYTES - _COMMIT_BYTES
_MAX_PAYLOAD_BYTES: Final = _BODY_BYTES - _FRAME_HEADER.size - _CRC.size
_FACT_ENCODER = msgspec.json.Encoder(order="deterministic")
_RECOVERY_OBJECT_DECODER = msgspec.json.Decoder(type=dict[str, object])


def _reject_non_json_constant(_value: str) -> object:
    raise ValueError("non-JSON numeric constant")


@dataclass(frozen=True)
class MmapOmsFactRecordV1:
    """One fully verified fact recovered from a committed mmap frame."""

    lsn: int
    batch_id: int
    ordinal: int
    batch_size: int
    fact: OmsJournalFact


@dataclass(frozen=True)
class MmapOmsFactJournalScanV1:
    """Strict scan result; an untrusted tail never becomes a valid prefix."""

    records: tuple[MmapOmsFactRecordV1, ...]
    clean: bool
    first_untrusted_lsn: int | None = None


class MmapOmsFactJournalV1(OmsFactJournal):
    """Single-process append-only fact journal with an explicit mmap commit fence.

    ``append_batch`` first materializes every body, then commits each frame in
    sequence.  A process death during that final sequence leaves a detectable
    partial batch; a subsequent process must not append to it.  The Consumer
    benchmark service uses a new namespace on every launch, so this condition
    fails closed rather than pretending that the in-memory order projection was
    restored.
    """

    def __init__(self, *, path: Path, journal_id: str, capacity: int) -> None:
        if not journal_id or len(journal_id) > 128:
            raise MmapOmsFactJournalError("mmap OMS journal_id is invalid")
        if not 1 <= capacity <= 1_000_000:
            raise MmapOmsFactJournalError("mmap OMS journal capacity is invalid")
        self._path = Path(path)
        self._journal_id = journal_id
        self._capacity = capacity
        self._lock = threading.RLock()
        self._closed = False
        self._append_permitted = True
        self._next_lsn = 1
        self._next_batch_id = 1
        self._file, self._mapped, created = self._open()
        if not created:
            scan = self.scan()
            if not scan.clean:
                self._append_permitted = False
            elif scan.records:
                self._next_lsn = scan.records[-1].lsn + 1
                self._next_batch_id = scan.records[-1].batch_id + 1

    @property
    def journal_id(self) -> str:
        return self._journal_id

    @property
    def path(self) -> Path:
        return self._path

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def recovery_required(self) -> bool:
        return not self._append_permitted

    @property
    def profile_hash(self) -> str:
        return sha256(b"MmapOmsFactJournalV1|4096|TCF1|commit-last").hexdigest()

    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        """Commit a complete logical fact batch or permanently close admission.

        The normal return means every frame in the batch received its final
        commit word.  On an I/O/programming failure after body materialization,
        reusing this file could turn a torn batch into a misleading history, so
        future append attempts fail closed.
        """

        batch = tuple(facts)
        if not batch:
            raise MmapOmsFactJournalError("mmap OMS fact batch cannot be empty")
        if len({fact.fact_id for fact in batch}) != len(batch):
            raise MmapOmsFactJournalError("mmap OMS fact batch repeats a fact id")
        with self._lock:
            self._require_appendable()
            if self._next_lsn + len(batch) - 1 > self._capacity:
                raise MmapOmsFactJournalFull("mmap OMS fact journal capacity is exhausted")
            batch_id = self._next_batch_id
            prepared = tuple(
                self.encode_uncommitted_frame(
                    lsn=self._next_lsn + ordinal,
                    batch_id=batch_id,
                    ordinal=ordinal,
                    batch_size=len(batch),
                    fact=fact,
                )
                for ordinal, fact in enumerate(batch)
            )
            try:
                for lsn, body in prepared:
                    offset = _HEADER_BYTES + ((lsn - 1) * _FRAME_BYTES)
                    # Scan has already proved this next frame empty. Write only
                    # the materialized bytes; untouched padding remains zero.
                    # A crash before the fixed final commit word still leaves a
                    # detectable uncommitted tail and fails recovery closed.
                    self._mapped[offset : offset + len(body)] = body
                for lsn, _body in prepared:
                    offset = _HEADER_BYTES + ((lsn - 1) * _FRAME_BYTES)
                    # Final store: intentionally no synchronous flush on the
                    # Consumer hot path.
                    self._mapped[offset + _BODY_BYTES : offset + _FRAME_BYTES] = _COMMIT.pack(
                        lsn ^ _COMMIT_XOR
                    )
            except BaseException as exc:
                self._append_permitted = False
                raise MmapOmsFactJournalRecoveryRequired(
                    "mmap OMS fact journal append was interrupted; recovery is required"
                ) from exc
            first_lsn = self._next_lsn
            self._next_lsn += len(batch)
            self._next_batch_id += 1
            return OmsJournalCommit(
                journal_id=self._journal_id,
                first_lsn=first_lsn,
                final_lsn=self._next_lsn - 1,
                fact_count=len(batch),
            )

    def read_committed_batch(self, commit: OmsJournalCommit) -> tuple[MmapOmsFactRecordV1, ...]:
        """Read one exact committed batch for an off-hot-path execution worker.

        The supplied receipt is not authority by itself: every frame is
        revalidated, including CRC, commit word, ordinal and batch identity.
        This method neither scans the entire journal nor runs in admission.
        """

        with self._lock:
            self._require_appendable()
            if (
                commit.journal_id != self._journal_id
                or commit.stream_id is not None
                or commit.first_lsn < 1
                or commit.final_lsn >= self._next_lsn
                or commit.final_lsn > self._capacity
            ):
                raise MmapOmsFactJournalError("OMS journal receipt is outside this committed file")
            records: list[MmapOmsFactRecordV1] = []
            for lsn in range(commit.first_lsn, commit.final_lsn + 1):
                offset = _HEADER_BYTES + ((lsn - 1) * _FRAME_BYTES)
                record = self.decode_committed_frame(
                    self._mapped[offset : offset + _FRAME_BYTES], lsn
                )
                if record is None:
                    self._append_permitted = False
                    raise MmapOmsFactJournalRecoveryRequired(
                        "committed OMS batch contains an untrusted frame"
                    )
                records.append(record)
            if (
                len(records) != commit.fact_count
                or any(item.batch_id != records[0].batch_id for item in records)
                or any(item.batch_size != commit.fact_count for item in records)
                or tuple(item.ordinal for item in records) != tuple(range(commit.fact_count))
            ):
                raise MmapOmsFactJournalError("OMS receipt does not identify one complete batch")
            return tuple(records)

    def scan(self) -> MmapOmsFactJournalScanV1:
        """Verify every committed frame and reject a torn/non-contiguous tail."""

        self._require_open()
        records: list[MmapOmsFactRecordV1] = []
        expected_lsn = 1
        saw_empty = False
        for index in range(self._capacity):
            offset = _HEADER_BYTES + (index * _FRAME_BYTES)
            frame = self._mapped[offset : offset + _FRAME_BYTES]
            empty = frame == b"\0" * _FRAME_BYTES
            if saw_empty:
                if not empty:
                    return MmapOmsFactJournalScanV1(
                        tuple(records), clean=False, first_untrusted_lsn=expected_lsn
                    )
                expected_lsn += 1
                continue
            if empty:
                saw_empty = True
                expected_lsn += 1
                continue
            record = self.decode_committed_frame(frame, expected_lsn)
            if record is None:
                return MmapOmsFactJournalScanV1(
                    tuple(records), clean=False, first_untrusted_lsn=expected_lsn
                )
            records.append(record)
            expected_lsn += 1
        if records and not self._complete_batch_prefix(records):
            return MmapOmsFactJournalScanV1(
                tuple(records), clean=False, first_untrusted_lsn=records[-1].lsn
            )
        return MmapOmsFactJournalScanV1(tuple(records), clean=True)

    def close(self) -> None:
        if self._closed:
            return
        self._mapped.close()
        self._file.close()
        self._closed = True

    def _open(self) -> tuple[BinaryIO, mmap.mmap, bool]:
        parent = self._path.parent
        if not parent.is_dir() or parent.is_symlink():
            raise MmapOmsFactJournalError("mmap OMS journal parent is unsafe")
        flags = os.O_RDWR | os.O_CREAT
        descriptor: int | None = None
        try:
            descriptor = os.open(self._path, flags, 0o600)
            file_size = _HEADER_BYTES + (self._capacity * _FRAME_BYTES)
            existing_size = os.fstat(descriptor).st_size
            if existing_size == 0:
                os.ftruncate(descriptor, file_size)
                mapped = mmap.mmap(descriptor, file_size, access=mmap.ACCESS_WRITE)
                mapped[: _FILE_HEADER.size] = _FILE_HEADER.pack(
                    _FILE_MAGIC, _VERSION, _FRAME_BYTES, self._capacity
                )
                created = True
            elif existing_size == file_size:
                mapped = mmap.mmap(descriptor, file_size, access=mmap.ACCESS_WRITE)
                try:
                    magic, version, frame_bytes, capacity = _FILE_HEADER.unpack(
                        mapped[: _FILE_HEADER.size]
                    )
                except struct.error as exc:
                    mapped.close()
                    raise MmapOmsFactJournalError("mmap OMS journal header is malformed") from exc
                if (magic, version, frame_bytes, capacity) != (
                    _FILE_MAGIC,
                    _VERSION,
                    _FRAME_BYTES,
                    self._capacity,
                ):
                    mapped.close()
                    raise MmapOmsFactJournalError("mmap OMS journal header does not match profile")
                created = False
            else:
                os.close(descriptor)
                raise MmapOmsFactJournalError("mmap OMS journal file size does not match profile")
        except Exception:
            try:
                if descriptor is not None:
                    os.close(descriptor)
            except OSError:
                pass
            raise
        assert descriptor is not None
        return os.fdopen(descriptor, "r+b", closefd=True), mapped, created

    @staticmethod
    def encode_uncommitted_frame(
        *,
        lsn: int,
        batch_id: int,
        ordinal: int,
        batch_size: int,
        fact: OmsJournalFact,
    ) -> tuple[int, bytes]:
        encoded: dict[str, object] = {
            "account_id": fact.account_id,
            "attributes": dict(fact.attributes),
            "execution_id": fact.execution_id,
            "fact_id": fact.fact_id,
            "kind": fact.kind.value,
            "oms_order_id": fact.oms_order_id,
        }
        if fact.recovery_payload is not None:
            try:
                recovery_object: object = _RECOVERY_OBJECT_DECODER.decode(fact.recovery_payload)
            except (ValueError, UnicodeError, OverflowError):
                # Compatibility for canonical escaped lone surrogates / large
                # integers. Decode the WHOLE UTF-8 object; msgspec.Raw skipping
                # would accept invalid UTF-8 in nested values. Never use Raw as
                # a validation shortcut. No NaN/Infinity or UTF-16/BOM embedding.
                try:
                    recovery_object = json.loads(
                        fact.recovery_payload.decode("utf-8"),
                        parse_constant=_reject_non_json_constant,
                    )
                except (ValueError, UnicodeError) as exc:
                    raise MmapOmsFactJournalError("recovery payload is not valid JSON") from exc
            if not isinstance(recovery_object, dict):
                raise MmapOmsFactJournalError("recovery payload must be an object")
            encoded["recovery_payload"] = msgspec.Raw(fact.recovery_payload)
        payload = _FACT_ENCODER.encode(encoded)
        if len(payload) > _MAX_PAYLOAD_BYTES:
            raise MmapOmsFactJournalError("mmap OMS fact payload exceeds one fixed frame")
        header = _FRAME_HEADER.pack(
            _FRAME_MAGIC, _VERSION, lsn, batch_id, ordinal, batch_size, len(payload)
        )
        checksum = zlib.crc32(payload, zlib.crc32(header)) & 0xFFFFFFFF
        crc = _CRC.pack(checksum)
        body = header + payload + crc
        return lsn, body

    @staticmethod
    def decode_committed_frame(frame: bytes, expected_lsn: int) -> MmapOmsFactRecordV1 | None:
        if len(frame) != _FRAME_BYTES:
            return None
        try:
            magic, version, lsn, batch_id, ordinal, batch_size, payload_length = (
                _FRAME_HEADER.unpack(frame[: _FRAME_HEADER.size])
            )
        except struct.error:
            return None
        if (
            magic != _FRAME_MAGIC
            or version != _VERSION
            or lsn != expected_lsn
            or not 1 <= batch_id
            or not 1 <= batch_size
            or ordinal >= batch_size
            or payload_length > _MAX_PAYLOAD_BYTES
        ):
            return None
        payload_start = _FRAME_HEADER.size
        payload_end = payload_start + payload_length
        expected_crc = _CRC.unpack(frame[payload_end : payload_end + _CRC.size])[0]
        if zlib.crc32(frame[:payload_end]) & 0xFFFFFFFF != expected_crc:
            return None
        commit = _COMMIT.unpack(frame[_BODY_BYTES:])[0]
        if commit != (lsn ^ _COMMIT_XOR):
            return None
        try:
            raw_object: object = json.loads(frame[payload_start:payload_end].decode("utf-8"))
            if not isinstance(raw_object, dict):
                return None
            raw = cast(dict[str, object], raw_object)
            fact_id = raw.get("fact_id")
            kind = raw.get("kind")
            account_id = raw.get("account_id")
            oms_order_id = raw.get("oms_order_id")
            execution_id = raw.get("execution_id")
            attributes_object = raw.get("attributes")
            recovery_object = raw.get("recovery_payload")
            attributes = (
                cast(dict[object, object], attributes_object)
                if isinstance(attributes_object, dict)
                else None
            )
            if (
                not isinstance(fact_id, str)
                or not isinstance(kind, str)
                or not isinstance(account_id, str)
                or (oms_order_id is not None and not isinstance(oms_order_id, str))
                or (execution_id is not None and not isinstance(execution_id, str))
                or attributes is None
                or (recovery_object is not None and not isinstance(recovery_object, dict))
                or any(
                    not isinstance(key, str) or not isinstance(value, str)
                    for key, value in attributes.items()
                )
            ):
                return None
            fact = OmsJournalFact(
                fact_id=fact_id,
                kind=OmsFactKind(kind),
                account_id=account_id,
                oms_order_id=oms_order_id,
                execution_id=execution_id,
                attributes=cast(dict[str, str], attributes),
                recovery_payload=(
                    json.dumps(
                        recovery_object, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                    ).encode("ascii")
                    if recovery_object is not None
                    else None
                ),
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            OmsOrderBookError,
        ):
            return None
        return MmapOmsFactRecordV1(lsn, batch_id, ordinal, batch_size, fact)

    @staticmethod
    def _complete_batch_prefix(records: list[MmapOmsFactRecordV1]) -> bool:
        last = records[-1]
        if last.ordinal != last.batch_size - 1:
            return False
        batch = [record for record in records if record.batch_id == last.batch_id]
        return len(batch) == last.batch_size and tuple(record.ordinal for record in batch) == tuple(
            range(last.batch_size)
        )

    def _require_open(self) -> None:
        if self._closed:
            raise MmapOmsFactJournalError("mmap OMS fact journal is closed")

    def _require_appendable(self) -> None:
        self._require_open()
        if not self._append_permitted:
            raise MmapOmsFactJournalRecoveryRequired(
                "mmap OMS fact journal has an untrusted recovery tail"
            )
