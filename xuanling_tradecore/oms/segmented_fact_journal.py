"""Single-writer mmap segments with prepared successors and crash-checked positions.

The append path never creates files, flushes mappings, hashes complete segments or
writes metadata. A prepared successor is activated with a commit-last mmap header.
The manifest already owns that successor before it can enter the ready queue.
"""

from __future__ import annotations

import json
import mmap
import os
import re
import shutil
import struct
import threading
import uuid
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from queue import Empty, Full, Queue
from time import monotonic_ns, sleep
from typing import Callable, Iterator, Protocol, cast

from ..persistence.runtime_files import (
    ExclusiveRuntimeLease,
    atomic_runtime_json,
    open_private,
    require_private_runtime_file,
)
from .mmap_fact_journal_v1 import (
    MmapOmsFactJournalError,
    MmapOmsFactJournalFull,
    MmapOmsFactJournalRecoveryRequired,
    MmapOmsFactJournalV1,
    MmapOmsFactRecordV1,
)
from .order_book import OmsFactJournal, OmsJournalCommit, OmsJournalFact

FRAME_BYTES = 4096
HEADER_BYTES = 4096
_STATIC = struct.Struct("<8sIIQ32s")
_POSITION = struct.Struct("<QQQ")
_CRC = struct.Struct("<I")
_WORD = struct.Struct("<Q")
_ACTIVATION_OFFSET = 128
_XOR = 0x4D2A519F39E770C1
_FILE_NAME = re.compile(r"segment-([0-9]{12})\.mmap\Z")


@dataclass(frozen=True)
class SegmentedJournalStats:
    journal_id: str
    next_lsn: int
    active_segment: int
    prepared_segments: int
    sealed_waiting: int
    catalog_segments: int
    allocated_bytes: int
    rotations: int
    maintenance_failed: bool
    archived_segments: int = 0
    archived_bytes: int = 0
    sealed_segments: int = 0
    seal_work_ns: int = 0
    maximum_seal_ns: int = 0
    process_prepared_segments: int = 0
    prepare_work_ns: int = 0
    maximum_prepare_ns: int = 0
    commit_timing_failed: bool = False


@dataclass(frozen=True)
class SegmentedJournalReadBound:
    journal_id: str
    profile_hash: str
    segment_frames: int
    active_segment: int
    active_frames: int
    committed_lsn: int
    next_batch_id: int


class _Segment:
    def __init__(
        self,
        path: Path,
        *,
        index: int,
        frames: int,
        journal_id: str,
        create: bool,
        prefault: bool = True,
        read_only: bool = False,
        stage_event: Callable[[str, str, int], None] | None = None,
        diagnostic_paced_prefault_pages: int = 0,
    ) -> None:
        self.path, self.index, self.frames = path, index, frames
        self._stage_event = stage_event
        if type(
            diagnostic_paced_prefault_pages
        ) is not int or diagnostic_paced_prefault_pages not in (0, 64, 256):
            raise ValueError("invalid maintenance diagnostic pacing")
        self.first_lsn = self.first_batch_id = self.predecessor = self.count = 0
        self.next_batch_id = 0
        self._closed = False
        if create and read_only:
            raise MmapOmsFactJournalError("cannot create a read-only segment")
        flags = (os.O_RDONLY if read_only else os.O_RDWR) | (
            os.O_CREAT | os.O_EXCL if create else 0
        )
        fd = open_private(path, flags)
        size = HEADER_BYTES + frames * FRAME_BYTES
        self._static = _STATIC.pack(
            b"TCSEG001", 1, FRAME_BYTES, index, sha256(journal_id.encode()).digest()
        )
        try:
            if create:
                with self._stage("ftruncate"):
                    os.ftruncate(fd, size)
                if hasattr(os, "posix_fallocate"):
                    with self._stage("fallocate"):
                        os.posix_fallocate(fd, 0, size)
            if os.fstat(fd).st_size != size:
                raise MmapOmsFactJournalRecoveryRequired("segment size mismatch")
            self._map = mmap.mmap(
                fd, size, access=mmap.ACCESS_READ if read_only else mmap.ACCESS_WRITE
            )
            if create:
                self._map[: _STATIC.size] = self._static
                if prefault:
                    with self._stage("prefault"):
                        for offset in range(HEADER_BYTES, size, FRAME_BYTES):
                            self._map[offset] = 0
                            if (
                                diagnostic_paced_prefault_pages
                                and ((offset - HEADER_BYTES) // FRAME_BYTES + 1)
                                % diagnostic_paced_prefault_pages
                                == 0
                            ):
                                sleep(0.001)
                with self._stage("prepare_flush"):
                    self._map.flush()
                with self._stage("prepare_fsync"):
                    os.fsync(fd)
            elif self._map[: _STATIC.size] != self._static:
                raise MmapOmsFactJournalRecoveryRequired("segment identity/header mismatch")
            self._read_activation()
        except BaseException:
            if hasattr(self, "_map"):
                self._map.close()
            os.close(fd)
            raise
        self._fd = fd

    @contextmanager
    def _stage(self, name: str):
        """Emit bounded maintenance-only timestamps; never on fact append."""
        event = self._stage_event
        if event is not None:
            event(name, "BEGIN", monotonic_ns())
        try:
            yield
        except BaseException:
            if event is not None:
                event(name, "FAILED", monotonic_ns())
            raise
        else:
            if event is not None:
                event(name, "END", monotonic_ns())

    def _read_activation(self) -> None:
        start = _ACTIVATION_OFFSET
        body = self._map[start : start + _POSITION.size]
        crc_at = start + _POSITION.size
        checksum = _CRC.unpack(self._map[crc_at : crc_at + 4])[0]
        word = _WORD.unpack(self._map[crc_at + 4 : crc_at + 12])[0]
        if body == b"\0" * _POSITION.size and checksum == word == 0:
            return
        first, batch, previous = _POSITION.unpack(body)
        if (
            first < 1
            or batch < 1
            or previous >= self.index
            or checksum != zlib.crc32(self._static + body) & 0xFFFFFFFF
            or word != (first ^ batch ^ self.index ^ _XOR)
        ):
            raise MmapOmsFactJournalRecoveryRequired("segment activation is torn or invalid")
        self.first_lsn, self.first_batch_id, self.predecessor = first, batch, previous
        self.next_batch_id = batch

    def activate(self, first_lsn: int, batch_id: int, predecessor: int) -> None:
        if self.first_lsn or self.count:
            raise MmapOmsFactJournalRecoveryRequired("segment cannot be activated twice")
        body = _POSITION.pack(first_lsn, batch_id, predecessor)
        start = _ACTIVATION_OFFSET
        self._map[start : start + len(body)] = body
        crc_at = start + len(body)
        self._map[crc_at : crc_at + 4] = _CRC.pack(zlib.crc32(self._static + body) & 0xFFFFFFFF)
        self._map[crc_at + 4 : crc_at + 12] = _WORD.pack(first_lsn ^ batch_id ^ self.index ^ _XOR)
        self.first_lsn, self.first_batch_id = first_lsn, batch_id
        self.next_batch_id, self.predecessor = batch_id, predecessor

    def append_prepared(self, prepared: tuple[tuple[int, bytes], ...], batch: int) -> None:
        for ordinal, (_position, body) in enumerate(prepared):
            offset = HEADER_BYTES + (self.count + ordinal) * FRAME_BYTES
            self._map[offset : offset + len(body)] = body
        for ordinal, (position, _body) in enumerate(prepared):
            offset = HEADER_BYTES + (self.count + ordinal + 1) * FRAME_BYTES - 8
            self._map[offset : offset + 8] = _WORD.pack(position ^ 0x71C43AF29B6E10D5)
        self.count += len(prepared)
        self.next_batch_id = batch + 1

    def records(self) -> Iterator[MmapOmsFactRecordV1]:
        count, ordinal, batch_size = 0, 0, 0
        pending: list[MmapOmsFactRecordV1] = []
        batch = self.first_batch_id
        empty_tail = False
        for index in range(self.frames):
            start = HEADER_BYTES + index * FRAME_BYTES
            frame = self._map[start : start + FRAME_BYTES]
            if frame == b"\0" * FRAME_BYTES:
                empty_tail = True
                continue
            if empty_tail or self.first_lsn == 0:
                raise MmapOmsFactJournalRecoveryRequired("segment has a hole or unactivated facts")
            record = MmapOmsFactJournalV1.decode_committed_frame(frame, self.first_lsn + index)
            if (
                record is None
                or record.batch_id != batch
                or record.ordinal != ordinal
                or (ordinal != 0 and record.batch_size != batch_size)
            ):
                raise MmapOmsFactJournalRecoveryRequired("segment has an incomplete/corrupt batch")
            batch_size = record.batch_size
            count += 1
            ordinal += 1
            pending.append(record)
            if ordinal == batch_size:
                batch += 1
                ordinal = 0
                yield from pending
                pending.clear()
        if ordinal:
            raise MmapOmsFactJournalRecoveryRequired("segment ends inside a batch")
        self.count, self.next_batch_id = count, batch

    def seal_cut(self) -> tuple[int, int, int, int, int]:
        return (
            self.first_lsn,
            self.first_batch_id,
            self.predecessor,
            self.count,
            self.next_batch_id,
        )

    def seal(self, *, expected: tuple[int, int, int, int, int] | None = None) -> str:
        # Called only by the maintenance worker, never from append_batch.
        with self._stage("seal_validate"):
            for _record in self.records():
                pass
            if expected is not None and self.seal_cut() != expected:
                raise MmapOmsFactJournalRecoveryRequired("segment seal cut changed")
        with self._stage("seal_flush"):
            self._map.flush()
        with self._stage("seal_fsync"):
            os.fsync(self._fd)
        with self._stage("seal_digest"):
            digest = self.digest()
        self.close()
        return digest

    def digest(self) -> str:
        digest = sha256()
        for offset in range(0, len(self._map), 1024 * 1024):
            digest.update(self._map[offset : offset + 1024 * 1024])
        return digest.hexdigest()

    def close(self) -> None:
        if not self._closed:
            self._map.close()
            os.close(self._fd)
            self._closed = True


class SegmentSealer(Protocol):
    def __call__(self, index: int, cut: tuple[int, int, int, int, int]) -> str: ...

    def close(self) -> None: ...


class SegmentedOmsFactJournal(OmsFactJournal):
    """Bounded mapped working set; immutable closed segments remain on disk.

    Until the archive lifecycle is installed, disk_budget_bytes is a hard gate,
    not an implicit licence to delete old evidence. Ready-capacity exhaustion is
    an explicit retryable pre-commit failure, never synchronous file creation.
    """

    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        segment_frames: int = 65_536,
        spare_segments: int = 2,
        disk_budget_bytes: int = 32 * 1024**3,
        prefault: bool = True,
        archive_root: Path | None = None,
        archive_budget_bytes: int = 2 * 1024**4,
        segment_sealer: SegmentSealer | None = None,
        segment_preparer: Callable[[int, bool], None] | None = None,
    ) -> None:
        cache_advice = os.environ.get("TRADECORE_DIAGNOSTIC_SEALED_CACHE_EVICT", "0")
        if cache_advice not in {"0", "1"}:
            raise MmapOmsFactJournalError("invalid sealed cache diagnostic setting")
        if cache_advice == "1" and (
            not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED")
        ):
            raise MmapOmsFactJournalError("sealed cache advice is unavailable")
        self._diagnostic_evict_sealed_cache = cache_advice == "1"
        if (
            not journal_id
            or len(journal_id) > 128
            or type(segment_frames) is not int
            or not 2 <= segment_frames <= 1_000_000
            or type(spare_segments) is not int
            or not 2 <= spare_segments <= 8
            or (segment_preparer is not None and segment_sealer is None)
        ):
            raise MmapOmsFactJournalError("invalid segmented journal identity/capacity")
        self.path = Path(root)
        self.path.mkdir(mode=0o700, parents=False, exist_ok=True)
        if self.path.is_symlink() or not self.path.is_dir():
            raise MmapOmsFactJournalError("unsafe segmented journal root")
        if os.name == "posix" and (
            self.path.stat().st_uid != os.geteuid() or self.path.stat().st_mode & 0o077
        ):
            raise MmapOmsFactJournalError("segmented journal root must be owner-only")
        self._file_bytes = HEADER_BYTES + segment_frames * FRAME_BYTES
        if type(archive_budget_bytes) is not int or archive_budget_bytes < self._file_bytes:
            raise MmapOmsFactJournalError("archive capacity is invalid")
        self.archive_path = archive_root
        self._archive_budget = archive_budget_bytes
        if archive_root is not None:
            archive_root.mkdir(mode=0o700, parents=False, exist_ok=True)
            if (
                archive_root.is_symlink()
                or not archive_root.is_dir()
                or archive_root.resolve() == self.path.resolve()
                or (
                    os.name == "posix"
                    and (
                        archive_root.stat().st_uid != os.geteuid()
                        or archive_root.stat().st_mode & 0o077
                    )
                )
            ):
                raise MmapOmsFactJournalError("archive root must be private and separate")
        if (
            type(disk_budget_bytes) is not int
            or disk_budget_bytes < (spare_segments + 1) * self._file_bytes
        ):
            raise MmapOmsFactJournalError("disk budget cannot hold active and spare segments")
        self._journal_id, self._frames, self._spares = journal_id, segment_frames, spare_segments
        self._segment_sealer = segment_sealer
        self._segment_preparer = segment_preparer
        self._commit_observer: Callable[[int, int, int, int, int], bool] | None = None
        self._commit_timing_failed = False
        self._process_prepared_segments = self._prepare_work_ns = self._maximum_prepare_ns = 0
        self._budget, self._prefault = disk_budget_bytes, prefault
        self._lock, self._catalog_lock = threading.RLock(), threading.Lock()
        self._catalog_publish_lock = threading.RLock()
        self._archive_lock = threading.Lock()
        self._ready: Queue[_Segment] = Queue(spare_segments)
        self._sealed: Queue[_Segment] = Queue(spare_segments + 1)
        self._wake, self._stop = threading.Event(), threading.Event()
        self._catalog: dict[int, dict[str, object]] = {}
        self._orphan_bytes = sum(path.stat().st_size for path in self.path.glob("orphan-*.mmap"))
        self._next_index, self._rotations = 1, 0
        self._sealed_segments = self._seal_work_ns = self._maximum_seal_ns = 0
        self._failed = self._closed = False
        self._append_started = False
        self._manifest = self.path / "segments.json"
        self._lease = ExclusiveRuntimeLease(self.path / "writer.lock")
        opened: list[_Segment] = []
        try:
            if self._manifest.exists():
                self._load_catalog()
                active: _Segment | None = None
                expected_lsn = expected_batch = 1
                predecessor = 0
                for index in sorted(self._catalog):
                    segment = self._open_segment(index)
                    opened.append(segment)
                    for _record in segment.records():
                        pass
                    expected_hash = self._catalog[index].get("sha256")
                    if expected_hash is not None and expected_hash != segment.digest():
                        raise MmapOmsFactJournalRecoveryRequired("sealed segment digest mismatch")
                    if segment.first_lsn:
                        if (
                            segment.first_lsn != expected_lsn
                            or segment.first_batch_id != expected_batch
                            or segment.predecessor != predecessor
                        ):
                            raise MmapOmsFactJournalRecoveryRequired(
                                "segment chain is discontinuous"
                            )
                        if active is not None:
                            active.close()
                        active = segment
                        predecessor = index
                        expected_lsn += segment.count
                        expected_batch = segment.next_batch_id
                    elif self._ready.qsize() < self._spares:
                        self._ready.put_nowait(segment)
                    else:
                        segment.close()
                if active is None:
                    raise MmapOmsFactJournalRecoveryRequired("manifest has no activated segment")
                self._active = active
                self._next_lsn, self._next_batch = expected_lsn, expected_batch
            else:
                if any(self.path.glob("segment-*.mmap")):
                    raise MmapOmsFactJournalRecoveryRequired("segment files lack their manifest")
                self._active = self._prepare_segment()
                opened.append(self._active)
                self._active.activate(1, 1, 0)
                self._next_lsn = self._next_batch = 1
            while self._ready.qsize() < self._spares:
                segment = self._prepare_segment()
                opened.append(segment)
                self._ready.put_nowait(segment)
        except BaseException:
            for segment in opened:
                segment.close()
            self._lease.close()
            raise
        self._worker = threading.Thread(
            target=self._maintain, name="oms-segment-maintenance", daemon=True
        )
        self._worker.start()

    @property
    def journal_id(self) -> str:
        return self._journal_id

    def bind_commit_observer(self, observer: Callable[[int, int, int, int, int], bool]) -> None:
        """Install optional evidence before live appends; never a fact authority."""
        with self._lock:
            if (
                self._closed
                or self._append_started
                or self._commit_observer is not None
                or not callable(observer)
            ):
                raise MmapOmsFactJournalError("commit observer must bind once before appends")
            self._commit_observer = observer

    @property
    def commit_timing_failed(self) -> bool:
        return self._commit_timing_failed

    @property
    def profile_hash(self) -> str:
        return sha256(b"SegmentedOmsFactJournal|TCSEG001|4096|commit-last").hexdigest()

    def _open_segment(self, index: int) -> _Segment:
        return _Segment(
            self.segment_read_path(index),
            index=index,
            frames=self._frames,
            journal_id=self._journal_id,
            create=False,
            read_only=self._catalog.get(index, {}).get("tier") == "archive",
        )

    def segment_read_path(self, index: int) -> Path:
        """Resolve source authority location, including verified archived segments.

        Readers that race hot-copy removal resolve again after FileNotFound.
        Existing mappings remain valid until their independent reader closes.
        """
        with self._catalog_lock:
            row = self._catalog.get(index)
            archived = row is not None and row.get("tier") == "archive"
        if archived:
            if self.archive_path is None:
                raise MmapOmsFactJournalRecoveryRequired("archive storage is unavailable")
            return self.archive_path / f"segment-{index:012d}.mmap"
        return self.path / f"segment-{index:012d}.mmap"

    def archive_sealed(self, index: int) -> bool:
        """Copy, fsync, verify and publish archive before removing a HOT DUPLICATE.

        Never deletes the authoritative raw archive. No compression assumptions,
        checkpoint truncation or trading recovery claim. One maintenance owner,
        whole sealed segments only; physical source is retained in full.
        """
        if self.archive_path is None:
            raise MmapOmsFactJournalError("archive storage was not configured")
        if type(index) is not int or index < 1:
            raise MmapOmsFactJournalError("archive index is invalid")
        with self._archive_lock:
            bound = self.read_bound()
            with self._catalog_lock:
                row = dict(self._catalog.get(index, {}))
                archived_count = sum(r.get("tier") == "archive" for r in self._catalog.values())
            digest = row.get("sha256")
            if index >= bound.active_segment:
                raise MmapOmsFactJournalError("only completed predecessor segments may archive")
            if row.get("tier") == "archive":
                return False
            if not isinstance(digest, str):
                raise MmapOmsFactJournalError("segment sealing has not completed")
            if (archived_count + 1) * self._file_bytes > self._archive_budget:
                raise MmapOmsFactJournalFull("archive quota is exhausted")
            archive_bytes = 0
            for path in self.archive_path.iterdir():
                if path.is_symlink() or not path.is_file():
                    raise MmapOmsFactJournalRecoveryRequired("archive contains an unsafe entry")
                archive_bytes += path.stat().st_size
            # Failed partial copies and pre-publication duplicates occupy real
            # space too; never give them an unbudgeted second allowance.
            if archive_bytes + self._file_bytes > self._archive_budget:
                raise MmapOmsFactJournalFull("archive temporary-copy quota is exhausted")
            source = self.path / f"segment-{index:012d}.mmap"
            target = self.archive_path / source.name
            temporary = self.archive_path / ("copy-" + uuid.uuid4().hex + ".partial")
            try:
                # Open via owner-only O_NOFOLLOW, not arbitrary shutil paths.
                with os.fdopen(open_private(source, os.O_RDONLY), "rb") as inp:
                    with os.fdopen(
                        open_private(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb"
                    ) as out:
                        shutil.copyfileobj(inp, out, length=1024**2)
                        out.flush()
                        os.fsync(out.fileno())
                archived = _Segment(
                    temporary,
                    index=index,
                    frames=self._frames,
                    journal_id=self._journal_id,
                    create=False,
                    read_only=True,
                )
                try:
                    if archived.digest() != digest:
                        raise MmapOmsFactJournalRecoveryRequired("archive copy digest mismatch")
                    for _record in archived.records():
                        pass
                finally:
                    archived.close()
                if target.exists() or target.is_symlink():
                    existing = _Segment(
                        target,
                        index=index,
                        frames=self._frames,
                        journal_id=self._journal_id,
                        create=False,
                        read_only=True,
                    )
                    try:
                        if existing.digest() != digest:
                            raise MmapOmsFactJournalRecoveryRequired("existing archive disagrees")
                    finally:
                        existing.close()
                    temporary.unlink()
                else:
                    temporary.replace(target)
                self._sync_directory(self.archive_path)
                with self._catalog_publish_lock:
                    with self._catalog_lock:
                        self._catalog[index]["tier"] = "archive"
                    try:
                        self._publish_catalog()
                    except BaseException:
                        with self._catalog_lock:
                            self._catalog[index] = row
                        raise
                # Metadata now points to the verified/fsynced archive. The old
                # hot file is only a duplicate; failure preserves extra bytes.
                source.unlink()
                self._sync_directory(self.path)
                self._wake.set()
                return True
            except BaseException:
                self._failed = True
                # Keep partials/copies for explicit diagnostic maintenance;
                # never erase source or infer coverage from their existence.
                raise

    @staticmethod
    def _sync_directory(path: Path) -> None:
        if os.name == "posix":
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def read_bound(self) -> SegmentedJournalReadBound:
        """Copy a whole-batch committed fence; never scan/open files under this lock."""
        with self._lock:
            if self._closed or self._failed:
                raise MmapOmsFactJournalRecoveryRequired("journal writer is closed/failed")
            return SegmentedJournalReadBound(
                self._journal_id,
                self.profile_hash,
                self._frames,
                self._active.index,
                self._active.count,
                self._next_lsn - 1,
                self._next_batch,
            )

    def _load_catalog(self) -> None:
        if self._manifest.is_symlink() or self._manifest.stat().st_size > 16 * 1024**2:
            raise MmapOmsFactJournalRecoveryRequired("unsafe/oversized segment manifest")
        raw: object = json.loads(self._manifest.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise MmapOmsFactJournalRecoveryRequired("segment manifest is not an object")
        value = cast(dict[str, object], raw)
        entries = value.get("segments")
        if (
            set(value) != {"schema_version", "journal_id", "segment_frames", "segments"}
            or value["schema_version"] != 1
            or value["journal_id"] != self._journal_id
            or value["segment_frames"] != self._frames
            or not isinstance(entries, list)
        ):
            raise MmapOmsFactJournalRecoveryRequired("segment manifest profile mismatch")
        for entry in cast(list[object], entries):
            if not isinstance(entry, dict):
                raise MmapOmsFactJournalRecoveryRequired("segment manifest entry is invalid")
            row = cast(dict[str, object], entry)
            index = row.get("index")
            if (
                type(index) is not int
                or not 1 <= index <= 999_999_999_999
                or index in self._catalog
                or set(row) - {"index", "sha256", "tier"}
                or row.get("tier", "hot") not in {"hot", "archive"}
                or (
                    row.get("tier") == "archive"
                    and (self.archive_path is None or not isinstance(row.get("sha256"), str))
                )
            ):
                raise MmapOmsFactJournalRecoveryRequired(
                    "segment manifest repeats/invalidates index"
                )
            self._catalog[index] = dict(row)
        if not self._catalog:
            raise MmapOmsFactJournalRecoveryRequired("segment manifest is empty")
        expected = {
            f"segment-{index:012d}.mmap"
            for index, row in self._catalog.items()
            if row.get("tier") != "archive"
        }
        actual = {path.name for path in self.path.glob("segment-*.mmap")}
        if expected - actual:
            raise MmapOmsFactJournalRecoveryRequired("missing registered segment file")
        archived_duplicates = {
            f"segment-{index:012d}.mmap"
            for index, row in self._catalog.items()
            if row.get("tier") == "archive"
        }
        # A crash after archive publication may leave its old HOT duplicate.
        # Preserve and account it, do not relabel it as an empty prepared file.
        self._orphan_bytes += sum(
            (self.path / name).stat().st_size for name in actual & archived_duplicates
        )
        for name in actual - expected - archived_duplicates:
            match = _FILE_NAME.fullmatch(name)
            if match is None:
                raise MmapOmsFactJournalRecoveryRequired("unregistered segment filename")
            segment = self._open_segment(int(match.group(1)))
            try:
                if segment.first_lsn or any(segment.records()):
                    raise MmapOmsFactJournalRecoveryRequired("unregistered activated segment")
            finally:
                segment.close()
            # A prepared file cannot enter the writer queue before manifest publication.
            # Preserve verified empty leftovers from a killed preparer, never erase them.
            target = self.path / f"orphan-{uuid.uuid4().hex}.mmap"
            (self.path / name).rename(target)
            self._orphan_bytes += target.stat().st_size
        self._next_index = max(self._catalog) + 1

    def _publish_catalog(self) -> None:
        with self._catalog_publish_lock:
            with self._catalog_lock:
                entries = [dict(self._catalog[index]) for index in sorted(self._catalog)]
            atomic_runtime_json(
                self._manifest,
                {
                    "schema_version": 1,
                    "journal_id": self._journal_id,
                    "segment_frames": self._frames,
                    "segments": entries,
                },
            )

    def _prepare_segment(self) -> _Segment:
        with self._catalog_lock:
            hot = sum(row.get("tier") != "archive" for row in self._catalog.values())
        if (hot + 1) * self._file_bytes + self._orphan_bytes > self._budget:
            raise MmapOmsFactJournalFull("sealed journal disk budget is exhausted")
        index = self._next_index
        self._next_index += 1
        target = self.path / f"segment-{index:012d}.mmap"
        started = monotonic_ns()
        if self._segment_preparer is not None:
            self._segment_preparer(index, self._prefault)
            require_private_runtime_file(target)
        segment = _Segment(
            target,
            index=index,
            frames=self._frames,
            journal_id=self._journal_id,
            create=self._segment_preparer is None,
            prefault=self._prefault,
        )
        if self._segment_preparer is not None:
            if segment.first_lsn or segment.first_batch_id or segment.predecessor or segment.count:
                segment.close()
                raise MmapOmsFactJournalRecoveryRequired("prepared segment was already activated")
            self._process_prepared_segments += 1
        elapsed = monotonic_ns() - started
        self._prepare_work_ns += elapsed
        self._maximum_prepare_ns = max(self._maximum_prepare_ns, elapsed)
        with self._catalog_lock:
            self._catalog[index] = {"index": index}
        try:
            self._publish_catalog()
        except BaseException:
            segment.close()
            raise
        return segment

    def append_batch(self, facts: tuple[OmsJournalFact, ...]) -> OmsJournalCommit:
        batch = tuple(facts)
        if (
            not batch
            or len(batch) > min(self._frames, 65_535)
            or len({fact.fact_id for fact in batch}) != len(batch)
        ):
            raise MmapOmsFactJournalError("batch is empty, too large or repeats a fact identity")
        with self._lock:
            if self._closed or self._failed:
                raise MmapOmsFactJournalRecoveryRequired("segmented writer is closed or failed")
            self._append_started = True
            # Validate and encode the whole batch before activating a successor
            # or writing any body; a bad caller payload must not poison a writer.
            prepared = tuple(
                MmapOmsFactJournalV1.encode_uncommitted_frame(
                    lsn=self._next_lsn + ordinal,
                    batch_id=self._next_batch,
                    ordinal=ordinal,
                    batch_size=len(batch),
                    fact=fact,
                )
                for ordinal, fact in enumerate(batch)
            )
            if self._active.count + len(batch) > self._frames:
                if self._sealed.full():
                    raise MmapOmsFactJournalFull("sealed segment maintenance is backpressured")
                try:
                    successor = self._ready.get_nowait()
                except Empty as exc:
                    raise MmapOmsFactJournalFull(
                        "no prepared successor; retry before commit"
                    ) from exc
                previous = self._active
                try:
                    successor.activate(self._next_lsn, self._next_batch, previous.index)
                except BaseException:
                    self._failed = True
                    successor.close()
                    raise
                self._active = successor
                self._sealed.put_nowait(previous)
                self._rotations += 1
                self._wake.set()
            first = self._next_lsn
            observer = self._commit_observer
            before = monotonic_ns() if observer is not None else 0
            try:
                self._active.append_prepared(prepared, self._next_batch)
            except BaseException:
                self._failed = True
                raise
            after = monotonic_ns() if observer is not None else 0
            self._next_lsn += len(batch)
            self._next_batch += 1
            if observer is not None:
                try:
                    if observer(first, self._next_lsn - 1, len(batch), before, after) is not True:
                        self._commit_timing_failed = True
                except BaseException:
                    # The commit already happened. Monitoring cannot make it
                    # appear absent/retryable or prevent memory state publication.
                    self._commit_timing_failed = True
            return OmsJournalCommit(self._journal_id, first, self._next_lsn - 1, len(batch))

    def _maintain(self) -> None:
        try:
            while not self._stop.is_set() or not self._sealed.empty():
                self._wake.wait(0.05)
                self._wake.clear()
                while True:
                    try:
                        segment = self._sealed.get_nowait()
                    except Empty:
                        break
                    try:
                        started = monotonic_ns()
                        digest = (
                            segment.seal()
                            if self._segment_sealer is None
                            else self._segment_sealer(segment.index, segment.seal_cut())
                        )
                        elapsed = monotonic_ns() - started
                        self._sealed_segments += 1
                        self._seal_work_ns += elapsed
                        self._maximum_seal_ns = max(self._maximum_seal_ns, elapsed)
                        with self._catalog_lock:
                            self._catalog[segment.index]["sha256"] = digest
                        self._publish_catalog()
                    finally:
                        segment.close()
                        self._sealed.task_done()
                    if self._diagnostic_evict_sealed_cache:
                        # Advisory only: the sealed file remains authoritative.
                        # Close the writer mapping before asking the kernel to
                        # reclaim clean cache; never advise active/spare files.
                        with self._archive_lock:
                            with self._catalog_lock:
                                archived = self._catalog[segment.index].get("tier") == "archive"
                            if not archived:
                                fd = open_private(segment.path, os.O_RDONLY)
                                try:
                                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                                finally:
                                    os.close(fd)
                while not self._stop.is_set() and not self._ready.full():
                    try:
                        segment = self._prepare_segment()
                    except MmapOmsFactJournalFull:
                        break
                    try:
                        self._ready.put_nowait(segment)
                    except Full:
                        segment.close()
        except BaseException:
            self._failed = True

    def startup_records(self) -> Iterator[MmapOmsFactRecordV1]:
        """Scan under the writer lease, before any live append is attempted.

        The lock spans iteration so no caller can append during bootstrap. A
        separate closed-writer reader cannot provide this exclusivity guarantee.
        """
        with self._lock:
            if self._closed or self._failed or self._append_started:
                raise MmapOmsFactJournalError("startup scan must precede live appends")
            with self._catalog_lock:
                indexes = sorted(self._catalog)
            for index in indexes:
                segment = self._open_segment(index)
                try:
                    yield from segment.records()
                finally:
                    segment.close()

    def records(self) -> Iterator[MmapOmsFactRecordV1]:
        """Read a quiescent/closed journal; never expose partial batches."""
        if not self._closed:
            raise MmapOmsFactJournalError("record iteration requires a closed writer")
        for index in sorted(self._catalog):
            segment = self._open_segment(index)
            try:
                yield from segment.records()
            finally:
                segment.close()

    def stats(self) -> SegmentedJournalStats:
        with self._lock:
            with self._catalog_lock:
                archived = sum(row.get("tier") == "archive" for row in self._catalog.values())
            return SegmentedJournalStats(
                self._journal_id,
                self._next_lsn,
                self._active.index,
                self._ready.qsize(),
                self._sealed.qsize(),
                len(self._catalog),
                (len(self._catalog) - archived) * self._file_bytes + self._orphan_bytes,
                self._rotations,
                self._failed,
                archived,
                archived * self._file_bytes,
                self._sealed_segments,
                self._seal_work_ns,
                self._maximum_seal_ns,
                self._process_prepared_segments,
                self._prepare_work_ns,
                self._maximum_prepare_ns,
                self._commit_timing_failed,
            )

    def close(self) -> None:
        with self._lock:
            if getattr(self, "_resources_closed", False):
                return
            self._closed = True
        self._stop.set()
        self._wake.set()
        self._worker.join(timeout=30)
        if self._worker.is_alive():
            self._failed = True
            raise MmapOmsFactJournalRecoveryRequired("segment maintenance did not drain")
        if self._segment_sealer is not None:
            # The worker and its child must both drain before the writer lease
            # can be released. A failed close remains retryable by the owner.
            self._segment_sealer.close()
        self._active.close()
        for queue in (self._ready, self._sealed):
            while True:
                try:
                    queue.get_nowait().close()
                except Empty:
                    break
        self._lease.close()
        self._resources_closed = True
