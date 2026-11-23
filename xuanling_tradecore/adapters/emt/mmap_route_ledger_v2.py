"""Companion mmap Route Ledger for EMT namespace recovery.

This append-only file is written beside the existing pre-native Journal.  It
stores the minimum owner/strategy route fact which the older 512-byte
``MmapPreNativeJournal`` intentionally does not carry.  Like that Journal, it
is a process-crash fence only: frames commit with a final word and the writer
never calls ``flush``/``fsync`` on the order path.
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
from typing import cast

from .namespace_recovery import (
    EmtNamespaceRecoveryError,
    OrderClientIdNamespaceDirectoryV1,
    RecoveryRehydratedFactV1,
    RouteLedgerFactV2,
    RouteLedgerV2,
)


class MmapRouteLedgerError(RuntimeError):
    """The companion route ledger cannot safely be appended or recovered."""


class MmapRouteLedgerFull(MmapRouteLedgerError):
    """The preallocated route-ledger capacity is exhausted."""


@dataclass(frozen=True)
class MmapRouteLedgerNativeAcceptanceTargetV1:
    """One verified, exact target suitable for a bounded recovery cancel.

    This type intentionally carries only the two native routing scalars.  It
    is derived from a committed local ledger and must not be logged or used to
    widen a cleanup beyond that one acceptance.
    """

    order_client_id: int
    order_emt_id: int

    def __post_init__(self) -> None:
        if type(self.order_client_id) is not int or self.order_client_id <= 0:
            raise MmapRouteLedgerError("route-ledger cleanup client id is invalid")
        if type(self.order_emt_id) is not int or self.order_emt_id <= 0:
            raise MmapRouteLedgerError("route-ledger cleanup native id is invalid")


_HEADER_SIZE = 4_096
_FRAME_SIZE = 2_048
_MAGIC = b"TCRTLV2\x00"
_FRAME_MAGIC = b"TCRL"
_VERSION = 2
_COMMIT_XOR = 0x9E3779B97F4A7C15
_HEADER = struct.Struct("<8sIIQQ")
_FRAME_PREFIX = struct.Struct("<4sHHQI")
_CRC = struct.Struct("<I")
_COMMIT = struct.Struct("<Q")
_CRC_OFFSET = _FRAME_SIZE - _CRC.size - _COMMIT.size
_COMMIT_OFFSET = _FRAME_SIZE - _COMMIT.size
_PAYLOAD_CAPACITY = _CRC_OFFSET - _FRAME_PREFIX.size
_PRE_DISPATCH = 1
_NATIVE_ACCEPTED = 2
_RECOVERY_BATCH_BEGIN = 3
_RECOVERY_REHYDRATED = 4
_RECOVERY_BATCH_COMMIT = 5
_RECORD_KINDS = frozenset(
    {
        _PRE_DISPATCH,
        _NATIVE_ACCEPTED,
        _RECOVERY_BATCH_BEGIN,
        _RECOVERY_REHYDRATED,
        _RECOVERY_BATCH_COMMIT,
    }
)


@dataclass(frozen=True)
class MmapRouteLedgerRecordV2:
    """One verified committed route-ledger entry, retained only during recovery."""

    sequence: int
    kind: int
    payload: dict[str, object]


class MmapRouteLedgerV2:
    """Single-writer, fixed-frame route facts required for R2 rehydration."""

    def __init__(
        self,
        *,
        path: Path,
        capacity: int,
        directory: OrderClientIdNamespaceDirectoryV1,
        journal_epoch: int = 1,
    ) -> None:
        if not 1 <= capacity <= 1_000_000:
            raise ValueError("route ledger capacity is invalid")
        if journal_epoch < 1:
            raise ValueError("route ledger epoch is invalid")
        self._path = Path(path)
        self._capacity = capacity
        self._directory = directory
        self._journal_epoch = journal_epoch
        self._owner_thread_id: int | None = None
        self._closed = False
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._open_or_initialize()
        try:
            self._mapped = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_WRITE)
            records = self.scan_records()
            self._next_sequence = len(records) + 1
            self._append_permitted = self._tail_is_zero_after(len(records))
        except BaseException:
            self._file.close()
            raise

    @property
    def directory(self) -> OrderClientIdNamespaceDirectoryV1:
        return self._directory

    @property
    def path(self) -> Path:
        return self._path

    @property
    def journal_epoch(self) -> int:
        return self._journal_epoch

    @property
    def recovery_required(self) -> bool:
        return not self._append_permitted

    def append_pre_dispatch(self, fact: RouteLedgerFactV2) -> None:
        self._directory.lookup(fact.order_client_id)
        self._append(_PRE_DISPATCH, {"fact": _fact_payload(fact)})

    def append_native_accepted(self, *, order_client_id: int, order_emt_id: int) -> None:
        self._directory.lookup(order_client_id)
        if type(order_emt_id) is not int or order_emt_id <= 0:
            raise MmapRouteLedgerError("route-ledger native order id is invalid")
        self._append(
            _NATIVE_ACCEPTED,
            {"order_client_id": order_client_id, "order_emt_id": order_emt_id},
        )

    def append_rehydration_batch(self, facts: tuple[RecoveryRehydratedFactV1, ...]) -> None:
        """Append a crash-safe all-or-nothing R2 projection batch.

        Individual frames are committed in order, but the reader accepts none
        of them until the matching commit frame verifies the count and digest.
        A process death half way through therefore blocks recovery rather than
        manufacturing a partially rebound in-memory route map.
        """

        if not facts:
            raise MmapRouteLedgerError("recovery rehydration batch cannot be empty")
        batch_id = facts[0].recovery_run_id
        if any(fact.recovery_run_id != batch_id for fact in facts):
            raise MmapRouteLedgerError("recovery rehydration batch ids differ")
        payloads = tuple(_recovery_fact_payload(fact) for fact in facts)
        canonical = _canonical_recovery_batch(payloads)
        batch_hash = _sha256_hex(canonical)
        if self._next_sequence + len(payloads) + 1 > self._capacity:
            raise MmapRouteLedgerFull("route ledger lacks capacity for recovery batch")
        self._append(
            _RECOVERY_BATCH_BEGIN,
            {"batch_id": batch_id, "fact_count": len(payloads), "facts_hash": batch_hash},
        )
        for payload in payloads:
            self._append(_RECOVERY_REHYDRATED, {"batch_id": batch_id, "fact": payload})
        self._append(
            _RECOVERY_BATCH_COMMIT,
            {"batch_id": batch_id, "fact_count": len(payloads), "facts_hash": batch_hash},
        )

    def load_route_ledger(self) -> RouteLedgerV2:
        """Rebuild routes from a clean, contiguous committed prefix only."""

        if self.recovery_required:
            raise MmapRouteLedgerError("route ledger has an untrusted tail")
        ledger = RouteLedgerV2(self._directory)
        pending_recovery: tuple[str, int, str, list[RecoveryRehydratedFactV1]] | None = None
        for record in self.scan_records():
            if record.kind == _PRE_DISPATCH:
                try:
                    raw = record.payload["fact"]
                    if not isinstance(raw, dict):
                        raise TypeError("route fact is not an object")
                    fact = _fact_from_payload(cast(dict[str, object], raw))
                except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                    raise MmapRouteLedgerError(
                        "route-ledger pre-dispatch record is invalid"
                    ) from exc
                ledger.record_pre_dispatch(fact)
            elif record.kind == _NATIVE_ACCEPTED:
                try:
                    client_id = record.payload["order_client_id"]
                    native_id = record.payload["order_emt_id"]
                    if type(client_id) is not int or type(native_id) is not int:
                        raise TypeError("route acceptance scalar is invalid")
                    ledger.rehydrate_native_acceptance(
                        order_client_id=client_id,
                        order_emt_id=native_id,
                    )
                except (KeyError, TypeError, EmtNamespaceRecoveryError) as exc:
                    raise MmapRouteLedgerError("route-ledger acceptance record is invalid") from exc
            elif record.kind == _RECOVERY_BATCH_BEGIN:
                if pending_recovery is not None:
                    raise MmapRouteLedgerError("route-ledger recovery batches overlap")
                try:
                    batch_id = _payload_text(record.payload, "batch_id")
                    fact_count = _payload_positive_int(record.payload, "fact_count")
                    facts_hash = _payload_hash(record.payload, "facts_hash")
                except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                    raise MmapRouteLedgerError(
                        "route-ledger recovery batch begin is invalid"
                    ) from exc
                pending_recovery = (batch_id, fact_count, facts_hash, [])
            elif record.kind == _RECOVERY_REHYDRATED:
                if pending_recovery is None:
                    raise MmapRouteLedgerError("route-ledger recovery fact has no batch")
                try:
                    batch_id = _payload_text(record.payload, "batch_id")
                    raw = record.payload["fact"]
                    if not isinstance(raw, dict):
                        raise TypeError("recovery rehydrated fact is not an object")
                    fact = _recovery_fact_from_payload(cast(dict[str, object], raw))
                except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                    raise MmapRouteLedgerError("route-ledger recovery fact is invalid") from exc
                expected_id, expected_count, expected_hash, facts = pending_recovery
                if batch_id != expected_id or fact.recovery_run_id != expected_id:
                    raise MmapRouteLedgerError("route-ledger recovery fact batch id conflicts")
                if len(facts) >= expected_count:
                    raise MmapRouteLedgerError("route-ledger recovery batch exceeds declared count")
                facts.append(fact)
                pending_recovery = (expected_id, expected_count, expected_hash, facts)
            elif record.kind == _RECOVERY_BATCH_COMMIT:
                if pending_recovery is None:
                    raise MmapRouteLedgerError("route-ledger recovery commit has no batch")
                try:
                    batch_id = _payload_text(record.payload, "batch_id")
                    fact_count = _payload_positive_int(record.payload, "fact_count")
                    facts_hash = _payload_hash(record.payload, "facts_hash")
                except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                    raise MmapRouteLedgerError(
                        "route-ledger recovery batch commit is invalid"
                    ) from exc
                expected_id, expected_count, expected_hash, facts = pending_recovery
                if (
                    batch_id != expected_id
                    or fact_count != expected_count
                    or facts_hash != expected_hash
                    or len(facts) != expected_count
                    or _sha256_hex(
                        _canonical_recovery_batch(
                            tuple(_recovery_fact_payload(item) for item in facts)
                        )
                    )
                    != expected_hash
                ):
                    raise MmapRouteLedgerError("route-ledger recovery batch commit conflicts")
                for fact in facts:
                    ledger.apply_recovery_rehydrated(fact)
                pending_recovery = None
            else:  # The decoder only admits constants above; retain a hard gate.
                raise MmapRouteLedgerError("route-ledger record kind is invalid")
        if pending_recovery is not None:
            raise MmapRouteLedgerError("route-ledger recovery batch is incomplete")
        return ledger

    def scan_records(self) -> tuple[MmapRouteLedgerRecordV2, ...]:
        """Read the contiguous valid prefix; a torn tail is never interpreted."""

        self._require_open()
        records: list[MmapRouteLedgerRecordV2] = []
        for sequence in range(1, self._capacity + 1):
            offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
            frame = self._mapped[offset : offset + _FRAME_SIZE]
            if frame == b"\x00" * _FRAME_SIZE:
                break
            decoded = self._decode_frame(frame, sequence)
            if decoded is None:
                break
            records.append(decoded)
        return tuple(records)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._mapped.close()
        self._file.close()

    def _open_or_initialize(self):
        exists = self._path.exists()
        descriptor = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        file = os.fdopen(descriptor, "r+b", buffering=0)
        expected_size = _HEADER_SIZE + (self._capacity * _FRAME_SIZE)
        try:
            if not exists or os.fstat(file.fileno()).st_size == 0:
                file.truncate(expected_size)
                file.seek(0)
                file.write(
                    _HEADER.pack(_MAGIC, _VERSION, _FRAME_SIZE, self._capacity, self._journal_epoch)
                )
                file.flush()
                return file
            file.seek(0)
            header = file.read(_HEADER.size)
            if len(header) != _HEADER.size:
                raise MmapRouteLedgerError("route ledger header is truncated")
            magic, version, frame_size, capacity, epoch = _HEADER.unpack(header)
            if (
                magic != _MAGIC
                or version != _VERSION
                or frame_size != _FRAME_SIZE
                or capacity != self._capacity
                or epoch != self._journal_epoch
                or os.fstat(file.fileno()).st_size != expected_size
            ):
                raise MmapRouteLedgerError("route ledger header does not match its configuration")
            return file
        except BaseException:
            file.close()
            raise

    def _append(self, kind: int, payload: dict[str, object]) -> None:
        self._require_open()
        if kind not in _RECORD_KINDS:
            raise MmapRouteLedgerError("route ledger record kind is invalid")
        if not self._append_permitted:
            raise MmapRouteLedgerError("route ledger requires recovery before append")
        if self._next_sequence > self._capacity:
            raise MmapRouteLedgerFull("route ledger capacity is exhausted")
        thread_id = threading.get_ident()
        if self._owner_thread_id is None:
            self._owner_thread_id = thread_id
        elif self._owner_thread_id != thread_id:
            raise MmapRouteLedgerError("route ledger has another writer thread")
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        if len(encoded) > _PAYLOAD_CAPACITY:
            raise MmapRouteLedgerError("route ledger payload exceeds fixed frame capacity")
        sequence = self._next_sequence
        prefix = _FRAME_PREFIX.pack(_FRAME_MAGIC, _VERSION, kind, sequence, len(encoded))
        frame = bytearray(_FRAME_SIZE)
        frame[: _FRAME_PREFIX.size] = prefix
        frame[_FRAME_PREFIX.size : _FRAME_PREFIX.size + len(encoded)] = encoded
        _CRC.pack_into(frame, _CRC_OFFSET, zlib.crc32(frame[:_CRC_OFFSET]) & 0xFFFFFFFF)
        offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
        self._mapped[offset : offset + _CRC_OFFSET + _CRC.size] = frame[: _CRC_OFFSET + _CRC.size]
        _COMMIT.pack_into(frame, _COMMIT_OFFSET, sequence ^ _COMMIT_XOR)
        self._mapped[offset + _COMMIT_OFFSET : offset + _FRAME_SIZE] = frame[_COMMIT_OFFSET:]
        self._next_sequence += 1

    def _decode_frame(self, frame: bytes, expected_sequence: int) -> MmapRouteLedgerRecordV2 | None:
        try:
            magic, version, kind, sequence, length = _FRAME_PREFIX.unpack_from(frame)
            stored_crc = _CRC.unpack_from(frame, _CRC_OFFSET)[0]
            commit = _COMMIT.unpack_from(frame, _COMMIT_OFFSET)[0]
        except struct.error:
            return None
        if (
            magic != _FRAME_MAGIC
            or version != _VERSION
            or kind not in _RECORD_KINDS
            or sequence != expected_sequence
            or not 0 < length <= _PAYLOAD_CAPACITY
            or commit != (sequence ^ _COMMIT_XOR)
            or stored_crc != (zlib.crc32(frame[:_CRC_OFFSET]) & 0xFFFFFFFF)
        ):
            return None
        try:
            payload = json.loads(
                frame[_FRAME_PREFIX.size : _FRAME_PREFIX.size + length].decode("ascii")
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        return MmapRouteLedgerRecordV2(sequence, kind, cast(dict[str, object], payload))

    def _tail_is_zero_after(self, committed_count: int) -> bool:
        """Reject torn and noncontiguous content instead of ever reusing it."""

        for sequence in range(committed_count + 1, self._capacity + 1):
            offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
            if self._mapped[offset : offset + _FRAME_SIZE] != b"\x00" * _FRAME_SIZE:
                return False
        return True

    def _require_open(self) -> None:
        if self._closed:
            raise MmapRouteLedgerError("route ledger is closed")


def load_single_native_acceptance_target_v1(
    path: Path,
) -> MmapRouteLedgerNativeAcceptanceTargetV1:
    """Read one fully committed acceptance target without opening a writer.

    The recovery command accepts only a ledger containing exactly one
    pre-dispatch fact and exactly one matching native-acceptance fact *or* one
    complete, digest-verified ``RECOVERY_REHYDRATED`` batch.  A torn frame,
    nonzero tail, duplicate, incomplete batch, or conflicting mapping fails
    closed.  This is deliberately read-only so a cleanup process cannot
    repair or append to the crashed process's journal before it has reconciled
    the target.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise MmapRouteLedgerError("route-ledger cleanup source is unreadable") from exc
    expected_header_size = _HEADER_SIZE + (_FRAME_SIZE * 64)
    if len(raw) != expected_header_size:
        raise MmapRouteLedgerError("route-ledger cleanup source size is invalid")
    try:
        magic, version, frame_size, capacity, epoch = _HEADER.unpack_from(raw)
    except struct.error as exc:
        raise MmapRouteLedgerError("route-ledger cleanup header is invalid") from exc
    if (
        magic != _MAGIC
        or version != _VERSION
        or frame_size != _FRAME_SIZE
        or capacity != 64
        or epoch < 1
    ):
        raise MmapRouteLedgerError("route-ledger cleanup header does not match F2")

    pre_dispatch_ids: set[int] = set()
    native_ids: dict[int, int] = {}
    recovered_ids: dict[int, int] = {}
    pending_recovery: tuple[str, int, str, list[RecoveryRehydratedFactV1]] | None = None
    completed_recovery_batches = 0
    committed = 0
    for sequence in range(1, capacity + 1):
        offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
        frame = raw[offset : offset + _FRAME_SIZE]
        if frame == b"\x00" * _FRAME_SIZE:
            break
        record = _decode_route_ledger_frame(frame, sequence)
        if record is None:
            raise MmapRouteLedgerError("route-ledger cleanup source has a torn frame")
        committed += 1
        if record.kind == _PRE_DISPATCH:
            try:
                fact_raw = record.payload["fact"]
                if not isinstance(fact_raw, dict):
                    raise TypeError
                fact = _fact_from_payload(cast(dict[str, object], fact_raw).copy())
            except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                raise MmapRouteLedgerError("route-ledger cleanup pre-dispatch is invalid") from exc
            pre_dispatch_ids.add(fact.order_client_id)
        elif record.kind == _NATIVE_ACCEPTED:
            try:
                client_id = record.payload["order_client_id"]
                native_id = record.payload["order_emt_id"]
            except KeyError as exc:
                raise MmapRouteLedgerError("route-ledger cleanup acceptance is invalid") from exc
            if (
                type(client_id) is not int
                or client_id <= 0
                or type(native_id) is not int
                or native_id <= 0
                or client_id in native_ids
            ):
                raise MmapRouteLedgerError("route-ledger cleanup acceptance is ambiguous")
            native_ids[client_id] = native_id
        elif record.kind == _RECOVERY_BATCH_BEGIN:
            if pending_recovery is not None or completed_recovery_batches:
                raise MmapRouteLedgerError("route-ledger cleanup recovery batches are ambiguous")
            try:
                pending_recovery = (
                    _payload_text(record.payload, "batch_id"),
                    _payload_positive_int(record.payload, "fact_count"),
                    _payload_hash(record.payload, "facts_hash"),
                    [],
                )
            except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                raise MmapRouteLedgerError(
                    "route-ledger cleanup recovery begin is invalid"
                ) from exc
        elif record.kind == _RECOVERY_REHYDRATED:
            if pending_recovery is None:
                raise MmapRouteLedgerError("route-ledger cleanup recovery fact has no begin")
            try:
                batch_id = _payload_text(record.payload, "batch_id")
                raw_fact = record.payload["fact"]
                if not isinstance(raw_fact, dict):
                    raise TypeError("route-ledger cleanup recovery fact is not an object")
                fact = _recovery_fact_from_payload(cast(dict[str, object], raw_fact))
            except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                raise MmapRouteLedgerError("route-ledger cleanup recovery fact is invalid") from exc
            expected_id, expected_count, expected_hash, facts = pending_recovery
            if (
                batch_id != expected_id
                or fact.recovery_run_id != expected_id
                or len(facts) >= expected_count
            ):
                raise MmapRouteLedgerError("route-ledger cleanup recovery fact conflicts")
            facts.append(fact)
            pending_recovery = (expected_id, expected_count, expected_hash, facts)
        elif record.kind == _RECOVERY_BATCH_COMMIT:
            if pending_recovery is None:
                raise MmapRouteLedgerError("route-ledger cleanup recovery commit has no begin")
            try:
                batch_id = _payload_text(record.payload, "batch_id")
                fact_count = _payload_positive_int(record.payload, "fact_count")
                facts_hash = _payload_hash(record.payload, "facts_hash")
            except (KeyError, TypeError, ValueError, EmtNamespaceRecoveryError) as exc:
                raise MmapRouteLedgerError(
                    "route-ledger cleanup recovery commit is invalid"
                ) from exc
            expected_id, expected_count, expected_hash, facts = pending_recovery
            if (
                batch_id != expected_id
                or fact_count != expected_count
                or facts_hash != expected_hash
                or len(facts) != expected_count
                or _sha256_hex(
                    _canonical_recovery_batch(tuple(_recovery_fact_payload(fact) for fact in facts))
                )
                != expected_hash
            ):
                raise MmapRouteLedgerError("route-ledger cleanup recovery commit conflicts")
            for fact in facts:
                existing = recovered_ids.get(fact.order_client_id)
                if existing is not None and existing != fact.order_emt_id:
                    raise MmapRouteLedgerError("route-ledger cleanup recovery target conflicts")
                recovered_ids[fact.order_client_id] = fact.order_emt_id
            pending_recovery = None
            completed_recovery_batches += 1
    tail_offset = _HEADER_SIZE + (committed * _FRAME_SIZE)
    if raw[tail_offset:] != b"\x00" * (len(raw) - tail_offset):
        raise MmapRouteLedgerError("route-ledger cleanup source tail is untrusted")
    if pending_recovery is not None:
        raise MmapRouteLedgerError("route-ledger cleanup recovery batch is incomplete")
    for client_id, native_id in native_ids.items():
        rehydrated = recovered_ids.get(client_id)
        if rehydrated is not None and rehydrated != native_id:
            raise MmapRouteLedgerError("route-ledger cleanup native and recovery targets conflict")
    effective_ids = dict(native_ids)
    for client_id, native_id in recovered_ids.items():
        effective_ids.setdefault(client_id, native_id)
    if len(pre_dispatch_ids) != 1 or set(effective_ids) != pre_dispatch_ids:
        raise MmapRouteLedgerError("route-ledger cleanup source has no unique accepted target")
    client_id = next(iter(pre_dispatch_ids))
    return MmapRouteLedgerNativeAcceptanceTargetV1(client_id, effective_ids[client_id])


def _decode_route_ledger_frame(
    frame: bytes, expected_sequence: int
) -> MmapRouteLedgerRecordV2 | None:
    try:
        magic, version, kind, sequence, length = _FRAME_PREFIX.unpack_from(frame)
        stored_crc = _CRC.unpack_from(frame, _CRC_OFFSET)[0]
        commit = _COMMIT.unpack_from(frame, _COMMIT_OFFSET)[0]
    except struct.error:
        return None
    if (
        magic != _FRAME_MAGIC
        or version != _VERSION
        or kind not in _RECORD_KINDS
        or sequence != expected_sequence
        or not 0 < length <= _PAYLOAD_CAPACITY
        or commit != (sequence ^ _COMMIT_XOR)
        or stored_crc != (zlib.crc32(frame[:_CRC_OFFSET]) & 0xFFFFFFFF)
    ):
        return None
    try:
        payload = json.loads(
            frame[_FRAME_PREFIX.size : _FRAME_PREFIX.size + length].decode("ascii")
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return MmapRouteLedgerRecordV2(expected_sequence, kind, cast(dict[str, object], payload))


def _fact_payload(fact: RouteLedgerFactV2) -> dict[str, object]:
    return {
        "scope": {
            "account_id": fact.scope.account_id,
            "trading_day": fact.scope.trading_day.value,
            "emt_client_id": fact.scope.emt_client_id,
        },
        "order_client_id": fact.order_client_id,
        "namespace_id": fact.namespace_id,
        "namespace_manifest_hash": fact.namespace_manifest_hash,
        "owner_system_id": fact.owner_system_id,
        "adapter_instance_id": fact.adapter_instance_id,
        "strategy_id": fact.strategy_id,
        "execution_id": fact.execution_id,
        "journal_epoch": fact.journal_epoch,
        "frame_sequence": fact.frame_sequence,
        "route_payload_hash": fact.route_payload_hash,
        "call_started": fact.call_started,
        "order_emt_id": fact.order_emt_id,
    }


def _fact_from_payload(raw: dict[str, object]) -> RouteLedgerFactV2:
    values = dict(raw)
    scope_raw = values.pop("scope")
    if not isinstance(scope_raw, dict):
        raise ValueError("scope is invalid")
    scope_values = cast(dict[str, object], scope_raw)
    from ...domain import TradingDayEpoch
    from .namespace_recovery import EmtOrderClientNamespaceScope

    scope = EmtOrderClientNamespaceScope(
        account_id=_required_text(scope_values, "account_id"),
        trading_day=TradingDayEpoch(_required_text(scope_values, "trading_day")),
        emt_client_id=_required_int(scope_values, "emt_client_id"),
    )
    return RouteLedgerFactV2(
        scope=scope,
        order_client_id=_required_int(values, "order_client_id"),
        namespace_id=_required_text(values, "namespace_id"),
        namespace_manifest_hash=_required_text(values, "namespace_manifest_hash"),
        owner_system_id=_required_text(values, "owner_system_id"),
        adapter_instance_id=_required_text(values, "adapter_instance_id"),
        strategy_id=_required_text(values, "strategy_id"),
        execution_id=_required_text(values, "execution_id"),
        journal_epoch=_required_int(values, "journal_epoch"),
        frame_sequence=_required_int(values, "frame_sequence"),
        route_payload_hash=_required_text(values, "route_payload_hash"),
        call_started=_required_bool(values, "call_started"),
        order_emt_id=_optional_positive_int(values, "order_emt_id"),
    )


def _required_text(values: dict[str, object], name: str) -> str:
    value = values.pop(name)
    if not isinstance(value, str):
        raise ValueError(f"{name} is invalid")
    return value


def _required_int(values: dict[str, object], name: str) -> int:
    value = values.pop(name)
    if type(value) is not int:
        raise ValueError(f"{name} is invalid")
    return value


def _required_bool(values: dict[str, object], name: str) -> bool:
    value = values.pop(name)
    if type(value) is not bool:
        raise ValueError(f"{name} is invalid")
    return value


def _optional_positive_int(values: dict[str, object], name: str) -> int | None:
    value = values.pop(name)
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} is invalid")
    return value


def _recovery_fact_payload(fact: RecoveryRehydratedFactV1) -> dict[str, object]:
    return {
        "recovery_run_id": fact.recovery_run_id,
        "namespace_manifest_hash": fact.namespace_manifest_hash,
        "first_scan_digest": fact.first_scan_digest,
        "second_scan_digest": fact.second_scan_digest,
        "order_client_id": fact.order_client_id,
        "order_emt_id": fact.order_emt_id,
        "execution_id": fact.execution_id,
        "route_payload_hash": fact.route_payload_hash,
        "journal_epoch": fact.journal_epoch,
        "frame_sequence": fact.frame_sequence,
    }


def _recovery_fact_from_payload(raw: dict[str, object]) -> RecoveryRehydratedFactV1:
    values = dict(raw)
    expected = {
        "recovery_run_id",
        "namespace_manifest_hash",
        "first_scan_digest",
        "second_scan_digest",
        "order_client_id",
        "order_emt_id",
        "execution_id",
        "route_payload_hash",
        "journal_epoch",
        "frame_sequence",
    }
    if set(values) != expected:
        raise ValueError("recovery rehydrated fact fields differ")
    return RecoveryRehydratedFactV1(
        recovery_run_id=_required_text(values, "recovery_run_id"),
        namespace_manifest_hash=_required_text(values, "namespace_manifest_hash"),
        first_scan_digest=_required_text(values, "first_scan_digest"),
        second_scan_digest=_required_text(values, "second_scan_digest"),
        order_client_id=_required_int(values, "order_client_id"),
        order_emt_id=_required_int(values, "order_emt_id"),
        execution_id=_required_text(values, "execution_id"),
        route_payload_hash=_required_text(values, "route_payload_hash"),
        journal_epoch=_required_int(values, "journal_epoch"),
        frame_sequence=_required_int(values, "frame_sequence"),
    )


def _canonical_recovery_batch(payloads: tuple[dict[str, object], ...]) -> bytes:
    return json.dumps(
        list(payloads), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _sha256_hex(value: bytes) -> str:
    return sha256(value).hexdigest()


def _payload_text(payload: dict[str, object], name: str) -> str:
    value = payload[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} is invalid")
    return value


def _payload_positive_int(payload: dict[str, object], name: str) -> int:
    value = payload[name]
    if type(value) is not int or value <= 0:
        raise TypeError(f"{name} is invalid")
    return value


def _payload_hash(payload: dict[str, object], name: str) -> str:
    value = _payload_text(payload, name)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} is invalid")
    return value
