"""Single-writer mmap write-ahead journal for the EMT native fast path.

This journal deliberately records only the fixed identity needed to reconcile
an unknown native outcome.  It is a *process-crash* recovery fence: frames are
mapped from a real file and are not private Python heap objects, but callers do
not wait for ``flush``/``fsync``.  It consequently does not claim durability
across host power loss or storage failure.

The writer is intentionally single-threaded.  A complete frame body and CRC
are written first; the final eight-byte commit word is written last.  Recovery
accepts only a contiguous prefix of valid, committed frames.  A torn tail can
therefore never be interpreted as authorization to call a venue.
"""

from __future__ import annotations

import mmap
import os
import struct
import threading
import zlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path


class MmapPreNativeJournalError(RuntimeError):
    """The local write-ahead journal cannot safely record a native action."""


class MmapPreNativeJournalFull(MmapPreNativeJournalError):
    """The preallocated journal has no reusable frame capacity."""


_HEADER_SIZE = 4_096
_FRAME_SIZE = 512
_FILE_MAGIC = b"TCMMJNL1"
_FRAME_MAGIC = b"TCMF"
_VERSION = 1
_COMMIT_XOR = 0xA5C37D9E1254B60F
_HEADER = struct.Struct("<8sIIQQ")
_BODY = struct.Struct("<4sHHQQ64s96s96s64s64s32s")
_CRC = struct.Struct("<I")
_COMMIT = struct.Struct("<Q")
_PAYLOAD_SIZE = _FRAME_SIZE - _COMMIT.size


class MmapJournalRecordKind:
    """Fixed on-disk kinds; integers keep frames deterministic and compact."""

    ORDER_PRE_NATIVE = 1
    ORDER_NATIVE_ACCEPTED = 2
    ORDER_CALLBACK_OBSERVED = 3
    CANCEL_STARTED = 4
    CANCEL_NATIVE_ACCEPTED = 5
    CANCEL_REJECTED = 6
    ORDER_CALL_STARTED = 7


@dataclass(frozen=True)
class MmapPreNativeRecord:
    """One fully committed, payload-free recovery marker."""

    sequence: int
    kind: int
    monotonic_ns: int
    account_id: str
    execution_id: str
    command_id: str
    payload_hash: str
    client_order_ref: str
    native_order_id: str


@dataclass(frozen=True)
class MmapCallbackObservedAppend:
    """One callback frame prepared for an ordered single-writer batch.

    The type deliberately has no mutable payload and carries the same fixed
    identity fields as :meth:`append_callback_observed`.  A batch is merely a
    more efficient way for one already-bound writer to commit independent
    callback facts; it never creates a second writer or weakens the commit
    word/recovery-prefix invariant.
    """

    monotonic_ns: int
    account_id: str
    execution_id: str
    transition_id: str
    payload_hash: str
    client_order_ref: str
    native_order_id: str


class MmapJournalRecoveryStatus(str, Enum):
    """Result of a strict, read-only prefix scan used during restart."""

    CLEAN = "CLEAN"
    UNTRUSTED_TAIL = "UNTRUSTED_TAIL"
    NONCONTIGUOUS_TAIL = "NONCONTIGUOUS_TAIL"


@dataclass(frozen=True)
class MmapJournalRecoveryScan:
    """The only safe recovery view of one pre-native mmap file.

    ``records`` contains the contiguous, committed prefix only.  A non-clean
    scan does not make a torn frame disappear: it identifies the first
    untrusted position so the recovery coordinator can keep admission closed
    and request broker reconciliation.
    """

    journal_epoch: int
    capacity: int
    records: tuple[MmapPreNativeRecord, ...]
    status: MmapJournalRecoveryStatus
    first_untrusted_sequence: int | None = None

    @property
    def trusted_position(self) -> int:
        return len(self.records)

    @property
    def clean(self) -> bool:
        return self.status is MmapJournalRecoveryStatus.CLEAN


@dataclass(frozen=True)
class MmapJournalPreparation:
    """Payload-free evidence that a Journal was prepared before C0.

    Preparation deliberately only makes mapped pages resident and caches a
    static account field.  It never flushes dirty pages, changes a frame, or
    weakens the single-writer ownership rule.
    """

    page_size: int
    page_count: int
    madvise_willneed_requested: bool
    static_account_cached: bool
    preloaded_fixed_field_count: int


class MmapPreNativeJournal:
    """Append fixed recovery frames through one process-local writer thread."""

    def __init__(self, *, path: Path, capacity: int, journal_epoch: int = 1) -> None:
        if not 1 <= capacity <= 1_000_000:
            raise ValueError("mmap Journal capacity is invalid")
        if journal_epoch < 1:
            raise ValueError("mmap Journal epoch is invalid")
        self._path = Path(path)
        self._capacity = capacity
        self._journal_epoch = journal_epoch
        # The journal is constructed during session composition but its one
        # writer is the native dispatch lane, which starts later.  Bind that
        # lane on its first append and reject every subsequent writer thread.
        self._owner_thread_id: int | None = None
        self._closed = False
        self._account_field_cache: dict[str, bytes] = {}
        self._fixed_field_cache: dict[tuple[str, str, int, bool], bytes] = {}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._open_or_initialize()
        try:
            self._mapped = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_WRITE)
            recovery_scan = self.scan_recovery_prefix()
            self._next_sequence = recovery_scan.trusted_position + 1
            # A process that sees an untrusted tail must never reuse that
            # position. F1 recovery may read its trusted prefix and produce a
            # reconciliation plan, but only an explicit file-rotation flow in
            # a later activation package may create a new writable journal.
            self._append_permitted = recovery_scan.clean
        except BaseException:
            self._file.close()
            raise

    @property
    def path(self) -> Path:
        return self._path

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def journal_epoch(self) -> int:
        """Return the stable, file-header epoch used by MergeManifestV2."""

        return self._journal_epoch

    @property
    def frame_size(self) -> int:
        return _FRAME_SIZE

    @property
    def recovery_required(self) -> bool:
        """Whether this reopened file has an unsafe tail that blocks append."""

        return not self._append_permitted

    @property
    def profile_hash(self) -> str:
        """Return a stable hash for the fixed V1 frame/header layout."""

        return sha256(b"MmapJournalProfileV1|header:<8sIIQQ|frame:512|body:TCMF/v1").hexdigest()

    def prepare_for_measurement(
        self,
        *,
        account_id: str,
        execution_ids: Iterable[str] = (),
        command_ids: Iterable[str] = (),
        payload_hashes: Iterable[str] = (),
        client_order_refs: Iterable[str] = (),
    ) -> MmapJournalPreparation:
        """Pre-touch every mapped page before a benchmark C0 release.

        ``madvise(MADV_WILLNEED)`` is advisory.  The explicit read of one byte
        per page establishes residency before measurement even when the kernel
        ignores that hint.  This is a setup-only operation and is forbidden
        after C0; in particular it performs no ``flush``/``msync``/``fsync``.
        """

        self._require_open()
        self._fixed_account_id(account_id)
        account_cached = True
        # These immutable values are fully known while composing a controlled
        # burst.  Pre-encoding them moves ASCII validation, ``encode`` and
        # fixed-width padding out of C1->C2 without caching payload contents.
        # The cache is bounded, journal-local, and optional for normal service
        # calls; unknown values keep the same fail-closed validation path.
        for value in execution_ids:
            self._fixed_field(value, 96, "execution_id")
        for value in command_ids:
            self._fixed_field(value, 96, "command_id")
        for value in payload_hashes:
            self._fixed_field(value, 64, "payload_hash")
        for value in client_order_refs:
            self._fixed_field(value, 64, "client_order_ref")
        page_size = _page_size()
        madvise_willneed_requested = False
        advise = getattr(self._mapped, "madvise", None)
        willneed = getattr(mmap, "MADV_WILLNEED", None)
        if callable(advise) and isinstance(willneed, int):
            try:
                advise(willneed)
                madvise_willneed_requested = True
            except (OSError, ValueError):
                # The explicit page reads below retain the preparation
                # guarantee on platforms/filesystems without MADV_WILLNEED.
                pass
        mapped_size = len(self._mapped)
        for offset in range(0, mapped_size, page_size):
            _ = self._mapped[offset]
        return MmapJournalPreparation(
            page_size=page_size,
            page_count=(mapped_size + page_size - 1) // page_size,
            madvise_willneed_requested=madvise_willneed_requested,
            static_account_cached=account_cached,
            preloaded_fixed_field_count=len(self._account_field_cache)
            + len(self._fixed_field_cache),
        )

    def append_pre_native(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        command_id: str,
        payload_hash: str,
        client_order_ref: str,
    ) -> MmapPreNativeRecord:
        return self._append(
            kind=MmapJournalRecordKind.ORDER_PRE_NATIVE,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=command_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id="",
        )

    def append_native_accepted(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        command_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        return self._append(
            kind=MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=command_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id=native_order_id,
        )

    def append_call_started(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        command_id: str,
        payload_hash: str,
        client_order_ref: str,
    ) -> MmapPreNativeRecord:
        """Commit the last local fact immediately before the native SDK call."""

        return self._append(
            kind=MmapJournalRecordKind.ORDER_CALL_STARTED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=command_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id="",
        )

    def append_callback_observed(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        transition_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        """Append one proven order/trade callback before C6/C7 delivery."""

        return self._append(
            kind=MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=transition_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id=native_order_id,
        )

    def append_callback_observed_batch(
        self,
        entries: tuple[MmapCallbackObservedAppend, ...],
        *,
        on_before_commit: Callable[[MmapPreNativeRecord], None] | None = None,
        on_committed: Callable[[MmapPreNativeRecord], None] | None = None,
    ) -> tuple[MmapPreNativeRecord, ...]:
        """Commit up to 32 distinct callback facts through one mmap writer.

        All frame bodies are materialized before any commit word is written.
        Commit words are then written in sequence order, preserving the
        recoverable contiguous-prefix rule if the process stops mid-batch.
        Optional hooks are observational only and cannot make a committed
        frame fail or alter the writer's sequence.
        """

        if not 1 <= len(entries) <= 32:
            raise MmapPreNativeJournalError("mmap callback batch length is outside 1..32")
        if len({entry.client_order_ref for entry in entries}) != len(entries):
            raise MmapPreNativeJournalError("mmap callback batch repeats an ordered route")
        prepared = tuple(
            (
                MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED,
                entry.monotonic_ns,
                entry.account_id,
                entry.execution_id,
                entry.transition_id,
                entry.payload_hash,
                entry.client_order_ref,
                entry.native_order_id,
            )
            for entry in entries
        )
        return self._append_batch(
            prepared,
            on_before_commit=on_before_commit,
            on_committed=on_committed,
        )

    def append_cancel_started(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        cancel_request_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        """Append the local cancel-before-native marker."""

        return self._append(
            kind=MmapJournalRecordKind.CANCEL_STARTED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=cancel_request_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id=native_order_id,
        )

    def append_cancel_native_accepted(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        cancel_request_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        """Append the local cancel native receipt marker."""

        return self._append(
            kind=MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=cancel_request_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id=native_order_id,
        )

    def append_cancel_rejected(
        self,
        *,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        transition_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        """Append one proven cancel-error callback marker."""

        return self._append(
            kind=MmapJournalRecordKind.CANCEL_REJECTED,
            monotonic_ns=monotonic_ns,
            account_id=account_id,
            execution_id=execution_id,
            command_id=transition_id,
            payload_hash=payload_hash,
            client_order_ref=client_order_ref,
            native_order_id=native_order_id,
        )

    def records(self) -> tuple[MmapPreNativeRecord, ...]:
        """Read the valid committed prefix; reject malformed nonzero frames."""

        self._require_open()
        records: list[MmapPreNativeRecord] = []
        for index in range(self._capacity):
            offset = _HEADER_SIZE + (index * _FRAME_SIZE)
            frame = self._mapped[offset : offset + _FRAME_SIZE]
            if frame == b"\0" * _FRAME_SIZE:
                break
            record = self._decode_frame(frame, expected_sequence=index + 1)
            if record is None:
                break
            records.append(record)
        return tuple(records)

    def scan_recovery_prefix(self) -> MmapJournalRecoveryScan:
        """Strictly scan this file without silently accepting an unsafe tail.

        The normal ``records()`` API retains its historical prefix-only
        compatibility.  Recovery code must use this API instead: any torn,
        CRC-invalid or non-contiguous frame is an explicit gate condition,
        never a reason to reuse or invent a sequence position.
        """

        self._require_open()
        records: list[MmapPreNativeRecord] = []
        saw_empty = False
        for index in range(self._capacity):
            sequence = index + 1
            offset = _HEADER_SIZE + (index * _FRAME_SIZE)
            frame = self._mapped[offset : offset + _FRAME_SIZE]
            is_empty = frame == b"\0" * _FRAME_SIZE
            if saw_empty:
                if not is_empty:
                    return MmapJournalRecoveryScan(
                        journal_epoch=self._journal_epoch,
                        capacity=self._capacity,
                        records=tuple(records),
                        status=MmapJournalRecoveryStatus.NONCONTIGUOUS_TAIL,
                        first_untrusted_sequence=sequence,
                    )
                continue
            if is_empty:
                saw_empty = True
                continue
            record = self._decode_frame(frame, expected_sequence=sequence)
            if record is None:
                return MmapJournalRecoveryScan(
                    journal_epoch=self._journal_epoch,
                    capacity=self._capacity,
                    records=tuple(records),
                    status=MmapJournalRecoveryStatus.UNTRUSTED_TAIL,
                    first_untrusted_sequence=sequence,
                )
            records.append(record)
        return MmapJournalRecoveryScan(
            journal_epoch=self._journal_epoch,
            capacity=self._capacity,
            records=tuple(records),
            status=MmapJournalRecoveryStatus.CLEAN,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._mapped.close()
        self._file.close()
        self._closed = True

    def _append(
        self,
        *,
        kind: int,
        monotonic_ns: int,
        account_id: str,
        execution_id: str,
        command_id: str,
        payload_hash: str,
        client_order_ref: str,
        native_order_id: str,
    ) -> MmapPreNativeRecord:
        return self._append_batch(
            (
                (
                    kind,
                    monotonic_ns,
                    account_id,
                    execution_id,
                    command_id,
                    payload_hash,
                    client_order_ref,
                    native_order_id,
                ),
            )
        )[0]

    def _append_batch(
        self,
        entries: tuple[tuple[int, int, str, str, str, str, str, str], ...],
        *,
        on_before_commit: Callable[[MmapPreNativeRecord], None] | None = None,
        on_committed: Callable[[MmapPreNativeRecord], None] | None = None,
    ) -> tuple[MmapPreNativeRecord, ...]:
        """Encode bodies once, then advance one contiguous commit prefix."""

        self._require_owner()
        if not entries:
            raise MmapPreNativeJournalError("mmap Journal batch cannot be empty")
        if self._next_sequence + len(entries) - 1 > self._capacity:
            raise MmapPreNativeJournalFull("mmap Journal frame capacity is exhausted")
        prepared: list[tuple[int, bytes, MmapPreNativeRecord]] = []
        for offset_index, (
            kind,
            monotonic_ns,
            account_id,
            execution_id,
            command_id,
            payload_hash,
            client_order_ref,
            native_order_id,
        ) in enumerate(entries):
            if kind not in {
                MmapJournalRecordKind.ORDER_PRE_NATIVE,
                MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
                MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED,
                MmapJournalRecordKind.CANCEL_STARTED,
                MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED,
                MmapJournalRecordKind.CANCEL_REJECTED,
                MmapJournalRecordKind.ORDER_CALL_STARTED,
            }:
                raise MmapPreNativeJournalError("mmap Journal record kind is invalid")
            if monotonic_ns < 0:
                raise MmapPreNativeJournalError("mmap Journal monotonic time is invalid")
            sequence = self._next_sequence + offset_index
            body = _BODY.pack(
                _FRAME_MAGIC,
                _VERSION,
                kind,
                sequence,
                monotonic_ns,
                self._fixed_account_id(account_id),
                self._fixed_field(execution_id, 96, "execution_id"),
                self._fixed_field(command_id, 96, "command_id"),
                self._fixed_field(payload_hash, 64, "payload_hash"),
                self._fixed_field(client_order_ref, 64, "client_order_ref"),
                self._fixed_field(native_order_id, 32, "native_order_id", allow_empty=True),
            )
            crc = _CRC.pack(zlib.crc32(body) & 0xFFFFFFFF)
            uncommitted = body + crc + (b"\0" * (_PAYLOAD_SIZE - len(body) - len(crc)))
            record = MmapPreNativeRecord(
                sequence=sequence,
                kind=kind,
                monotonic_ns=monotonic_ns,
                account_id=account_id,
                execution_id=execution_id,
                command_id=command_id,
                payload_hash=payload_hash,
                client_order_ref=client_order_ref,
                native_order_id=native_order_id,
            )
            prepared.append((sequence, uncommitted, record))
        for sequence, uncommitted, _record in prepared:
            offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
            self._mapped[offset : offset + _PAYLOAD_SIZE] = uncommitted
        committed: list[MmapPreNativeRecord] = []
        for sequence, _uncommitted, record in prepared:
            if on_before_commit is not None:
                try:
                    on_before_commit(record)
                except BaseException:
                    pass
            offset = _HEADER_SIZE + ((sequence - 1) * _FRAME_SIZE)
            # Commit is intentionally the final store. No ``flush`` occurs on
            # the hot path: this is an OS-page-cache process-crash fence only.
            self._mapped[offset + _PAYLOAD_SIZE : offset + _FRAME_SIZE] = _COMMIT.pack(
                sequence ^ _COMMIT_XOR
            )
            self._next_sequence = sequence + 1
            committed.append(record)
            if on_committed is not None:
                try:
                    on_committed(record)
                except BaseException:
                    pass
        return tuple(committed)

    def _open_or_initialize(self):
        expected_size = _HEADER_SIZE + (self._capacity * _FRAME_SIZE)
        existed = self._path.exists()
        descriptor = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        file = os.fdopen(descriptor, "r+b", buffering=0)
        try:
            size = os.fstat(file.fileno()).st_size
            if not existed or size == 0:
                os.ftruncate(file.fileno(), expected_size)
                header = _HEADER.pack(
                    _FILE_MAGIC, _VERSION, _FRAME_SIZE, self._capacity, self._journal_epoch
                )
                file.write(header + (b"\0" * (_HEADER_SIZE - len(header))))
                return file
            if size != expected_size:
                raise MmapPreNativeJournalError("mmap Journal file size does not match capacity")
            header = file.read(_HEADER.size)
            magic, version, frame_size, capacity, stored_epoch = _HEADER.unpack(header)
            if (
                magic != _FILE_MAGIC
                or version != _VERSION
                or frame_size != _FRAME_SIZE
                or capacity != self._capacity
            ):
                raise MmapPreNativeJournalError("mmap Journal header is incompatible")
            # ``0`` was emitted by early V1 writers before the reserved word
            # became an explicit epoch.  Interpret it as epoch 1 so existing
            # files remain readable, but never permit an accidental epoch hop.
            stored_epoch = stored_epoch or 1
            if stored_epoch != self._journal_epoch:
                raise MmapPreNativeJournalError("mmap Journal epoch is incompatible")
            return file
        except BaseException:
            file.close()
            raise

    def _decode_frame(self, frame: bytes, *, expected_sequence: int) -> MmapPreNativeRecord | None:
        return _decode_mmap_pre_native_frame(frame, expected_sequence=expected_sequence)

    def _require_owner(self) -> None:
        self._require_open()
        if not self._append_permitted:
            raise MmapPreNativeJournalError(
                "mmap Journal has an untrusted tail and requires reconciliation"
            )
        thread_id = threading.get_ident()
        if self._owner_thread_id is None:
            self._owner_thread_id = thread_id
            return
        if thread_id != self._owner_thread_id:
            raise MmapPreNativeJournalError("mmap Journal has more than one writer thread")

    def _fixed_account_id(self, account_id: str) -> bytes:
        cached = self._account_field_cache.get(account_id)
        if cached is not None:
            return cached
        encoded = _fixed_ascii(account_id, 64, "account_id")
        # A Journal is account-pinned.  Keep this defensive cap nevertheless,
        # so an accidental dynamic account source cannot turn a hot-path cache
        # into unbounded state.
        if len(self._account_field_cache) < 16:
            self._account_field_cache[account_id] = encoded
        return encoded

    def _fixed_field(
        self, value: str, maximum: int, name: str, *, allow_empty: bool = False
    ) -> bytes:
        key = (name, value, maximum, allow_empty)
        cached = self._fixed_field_cache.get(key)
        if cached is not None:
            return cached
        encoded = _fixed_ascii(value, maximum, name, allow_empty=allow_empty)
        # A single 2,000-order acceptance window has four relevant dynamic
        # fields per order.  This cap covers that window yet prevents malformed
        # callers from turning the fast-path cache into unbounded memory.
        if len(self._fixed_field_cache) < 8_192:
            self._fixed_field_cache[key] = encoded
        return encoded

    def _require_open(self) -> None:
        if self._closed:
            raise MmapPreNativeJournalError("mmap Journal is closed")


def scan_mmap_pre_native_journal_path(path: Path) -> MmapJournalRecoveryScan:
    """Read a pre-native Journal without ever opening it for write access.

    Reconciliation must not reuse the normal writer constructor merely to
    inspect an historical file: doing so would require an ``O_RDWR`` handle
    and makes an otherwise read-only operation harder to audit.  This helper
    validates the exact V1 header, every contiguous committed frame, and the
    remaining zero tail from immutable bytes.
    """

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise MmapPreNativeJournalError("mmap Journal recovery source is unreadable") from exc
    if len(raw) < _HEADER_SIZE:
        raise MmapPreNativeJournalError("mmap Journal recovery source is truncated")
    try:
        magic, version, frame_size, capacity, journal_epoch = _HEADER.unpack_from(raw)
    except struct.error as exc:
        raise MmapPreNativeJournalError("mmap Journal recovery header is invalid") from exc
    if (
        magic != _FILE_MAGIC
        or version != _VERSION
        or frame_size != _FRAME_SIZE
        or not 1 <= capacity <= 1_000_000
    ):
        raise MmapPreNativeJournalError("mmap Journal recovery header is incompatible")
    expected_size = _HEADER_SIZE + (capacity * _FRAME_SIZE)
    if len(raw) != expected_size:
        raise MmapPreNativeJournalError("mmap Journal recovery source size is invalid")
    records: list[MmapPreNativeRecord] = []
    saw_empty = False
    for index in range(capacity):
        sequence = index + 1
        offset = _HEADER_SIZE + (index * _FRAME_SIZE)
        frame = raw[offset : offset + _FRAME_SIZE]
        is_empty = frame == b"\0" * _FRAME_SIZE
        if saw_empty:
            if not is_empty:
                return MmapJournalRecoveryScan(
                    journal_epoch=journal_epoch or 1,
                    capacity=capacity,
                    records=tuple(records),
                    status=MmapJournalRecoveryStatus.NONCONTIGUOUS_TAIL,
                    first_untrusted_sequence=sequence,
                )
            continue
        if is_empty:
            saw_empty = True
            continue
        record = _decode_mmap_pre_native_frame(frame, expected_sequence=sequence)
        if record is None:
            return MmapJournalRecoveryScan(
                journal_epoch=journal_epoch or 1,
                capacity=capacity,
                records=tuple(records),
                status=MmapJournalRecoveryStatus.UNTRUSTED_TAIL,
                first_untrusted_sequence=sequence,
            )
        records.append(record)
    return MmapJournalRecoveryScan(
        journal_epoch=journal_epoch or 1,
        capacity=capacity,
        records=tuple(records),
        status=MmapJournalRecoveryStatus.CLEAN,
    )


def _decode_mmap_pre_native_frame(
    frame: bytes, *, expected_sequence: int
) -> MmapPreNativeRecord | None:
    if len(frame) != _FRAME_SIZE:
        return None
    try:
        commit = _COMMIT.unpack(frame[_PAYLOAD_SIZE:])[0]
    except struct.error:
        return None
    if commit == 0:
        return None
    body = frame[: _BODY.size]
    stored_crc = _CRC.unpack(frame[_BODY.size : _BODY.size + _CRC.size])[0]
    if zlib.crc32(body) & 0xFFFFFFFF != stored_crc:
        return None
    (
        magic,
        version,
        kind,
        sequence,
        monotonic_ns,
        account_id,
        execution_id,
        command_id,
        payload_hash,
        client_order_ref,
        native_order_id,
    ) = _BODY.unpack(body)
    if (
        magic != _FRAME_MAGIC
        or version != _VERSION
        or kind
        not in {
            MmapJournalRecordKind.ORDER_PRE_NATIVE,
            MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
            MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED,
            MmapJournalRecordKind.CANCEL_STARTED,
            MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED,
            MmapJournalRecordKind.CANCEL_REJECTED,
            MmapJournalRecordKind.ORDER_CALL_STARTED,
        }
        or sequence != expected_sequence
        or commit != (sequence ^ _COMMIT_XOR)
    ):
        return None
    return MmapPreNativeRecord(
        sequence=sequence,
        kind=kind,
        monotonic_ns=monotonic_ns,
        account_id=_decode_ascii(account_id),
        execution_id=_decode_ascii(execution_id),
        command_id=_decode_ascii(command_id),
        payload_hash=_decode_ascii(payload_hash),
        client_order_ref=_decode_ascii(client_order_ref),
        native_order_id=_decode_ascii(native_order_id),
    )


def _fixed_ascii(value: str, maximum: int, name: str, *, allow_empty: bool = False) -> bytes:
    if (not value and not allow_empty) or not value.isascii():
        raise MmapPreNativeJournalError(f"mmap Journal {name} is invalid")
    encoded = value.encode("ascii")
    if len(encoded) > maximum:
        raise MmapPreNativeJournalError(f"mmap Journal {name} is too long")
    return encoded.ljust(maximum, b"\0")


def _decode_ascii(value: bytes) -> str:
    return value.split(b"\0", 1)[0].decode("ascii")


def _page_size() -> int:
    try:
        size = int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, OSError, ValueError):
        size = getattr(mmap, "PAGESIZE", 4_096)
    return size if size > 0 else 4_096
