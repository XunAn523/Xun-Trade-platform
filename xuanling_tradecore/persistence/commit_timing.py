"""Bounded source commit timing evidence, never a trading durability fence.

Six uint64 values per row: sequence, first/final LSN, before/after commit, facts.
No business payload, account, token, order identity or exception text is stored.
Failure invalidates measurement; it must never turn a committed order into a
retryable pre-commit failure. Source Journal remains the sole fact authority.
"""

from __future__ import annotations

import json
import os
import struct
from hashlib import sha256
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Thread
from time import monotonic_ns, time_ns
from typing import cast

from .clock_identity import monotonic_clock_identity
from .runtime_files import atomic_runtime_json, open_private, require_private_runtime_file

RECORD = struct.Struct("<6Q")
SCHEMA = "source-commit-timing-v1"


class _BatchDrainQueue(Queue[tuple[int, ...]]):
    """Bounded FIFO; take only already-available evidence without waiting for a batch."""

    def get_available(self, maximum: int, timeout: float) -> tuple[tuple[int, ...], ...]:
        if not 1 <= maximum <= 64 or timeout < 0:
            raise ValueError("invalid evidence batch budget")
        with self.not_empty:
            if not self.not_empty.wait_for(self._qsize, timeout):
                raise Empty
            rows = tuple(self._get() for _ in range(min(maximum, self._qsize())))
            self.not_full.notify_all()
            return rows


class CommitTimingWriter:
    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        first_lsn: int,
        maximum_bytes: int,
        capacity: int = 65536,
        segment_records: int = 250000,
        producer_model: str | None = None,
    ) -> None:
        if (
            any(
                type(v) is not int or v < 1
                for v in (first_lsn, maximum_bytes, capacity, segment_records)
            )
            or capacity > 65536
            or segment_records > 1000000
            or maximum_bytes < RECORD.size
            or not journal_id
            or len(journal_id) > 128
            or (
                producer_model is not None
                and (type(producer_model) is not str or not 1 <= len(producer_model) <= 64)
            )
        ):
            raise ValueError("invalid commit timing evidence budget")
        batch_drain = os.environ.get("TRADECORE_DIAGNOSTIC_COMMIT_BATCH_DRAIN", "0")
        if batch_drain not in {"0", "1"}:
            raise ValueError("invalid commit timing batch diagnostic setting")
        root.mkdir(mode=0o700, exist_ok=False)
        self._root, self._journal, self._first = root, journal_id, first_lsn
        self._limit, self._segment_records = maximum_bytes, segment_records
        self._queue: Queue[tuple[int, ...]] = (
            _BatchDrainQueue(capacity) if batch_drain == "1" else Queue(capacity)
        )
        self._stop = Event()
        self._submitted = self._written = self._dropped = self._bytes = 0
        self._expected = first_lsn
        self._last_after = 0
        self._failed = self._closed = False
        self._segments: list[dict[str, object]] = []
        self._chain = "0" * 64
        self._anchor = {"monotonic_ns": monotonic_ns(), "wall_ns": time_ns()}
        self._clock_identity = monotonic_clock_identity()
        self._producer_model = producer_model
        self._publish()
        target = self._run_batched if batch_drain == "1" else self._run
        self._thread = Thread(target=target, name="source-commit-evidence", daemon=True)
        self._thread.start()

    def observe(self, first: int, last: int, count: int, before: int, after: int) -> bool:
        """Called only by the serialized Journal owner after the commit word.

        No file IO, encoding, waiting, authority decision or caller retry here.
        Sequence/LSN coverage includes a dropped row so loss cannot be hidden.
        """
        self._submitted += 1
        valid = (
            all(type(v) is int and 0 < v < 2**64 for v in (first, last, count, before, after))
            and first == self._expected
            and last - first + 1 == count
            and before <= after
            and before >= self._last_after
        )
        self._expected = last + 1 if type(last) is int else self._expected
        if valid:
            self._last_after = after
        if not valid or self._failed or self._closed:
            self._dropped += 1
            self._failed = True
            return False
        try:
            self._queue.put_nowait((self._submitted, first, last, before, after, count))
            return True
        except Full:
            self._dropped += 1
            self._failed = True
            return False

    def stats(self) -> dict[str, int | bool]:
        return {
            "submitted": self._submitted,
            "written": self._written,
            "dropped": self._dropped,
            "failed": self._failed,
            "closed": self._closed,
            "queue_depth": self._queue.qsize(),
            "bytes": self._bytes,
            "maximum_bytes": self._limit,
            "first_lsn": self._first,
            "observed_final_lsn": self._expected - 1,
        }

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def manifest_path(self) -> Path:
        return self._root / "manifest.json"

    def _publish(self) -> None:
        atomic_runtime_json(
            self._root / "manifest.json",
            {
                "schema": SCHEMA,
                "journal_id": self._journal,
                "record_bytes": RECORD.size,
                "clock": "time.monotonic_ns",
                "clock_identity_sha256": self._clock_identity,
                "producer_model": self._producer_model,
                "anchor": self._anchor,
                "formal_qualification": False,
                "chain": self._chain,
                "segments": self._segments,
                **self.stats(),
            },
        )

    def _run(self) -> None:
        output = None
        digest = sha256()
        count = 0
        last_publish = 0

        def seal() -> None:
            nonlocal output, count, digest
            if output is None:
                return
            output.flush()
            os.fsync(output.fileno())
            output.close()
            output = None
            checksum = digest.hexdigest()
            self._chain = sha256(
                bytes.fromhex(self._chain) + digest.digest() + struct.pack("<Q", count)
            ).hexdigest()
            self._segments.append(
                {
                    "file": f"commits-{len(self._segments):06d}.bin",
                    "records": count,
                    "bytes": count * RECORD.size,
                    "sha256": checksum,
                    "chain": self._chain,
                }
            )
            count, digest = 0, sha256()

        try:
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    row = self._queue.get(timeout=0.1)
                except Empty:
                    if output is not None:
                        output.flush()
                    if monotonic_ns() - last_publish >= 1_000_000_000:
                        self._publish()
                        last_publish = monotonic_ns()
                    continue
                if self._bytes + RECORD.size > self._limit or len(self._segments) >= 4096:
                    self._failed = True
                    self._dropped += 1
                    continue
                if output is None:
                    path = self._root / f"commits-{len(self._segments):06d}.bin"
                    output = os.fdopen(
                        open_private(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY),
                        "wb",
                        buffering=1024**2,
                    )
                encoded = RECORD.pack(*row)
                output.write(encoded)
                digest.update(encoded)
                self._written += 1
                self._bytes += RECORD.size
                count += 1
                if count == self._segment_records:
                    seal()
                    self._publish()
            seal()
        except Exception:
            self._failed = True
        finally:
            if output is not None:
                try:
                    output.close()
                except Exception:
                    self._failed = True
            self._closed = True
            try:
                self._publish()
            except Exception:
                self._failed = True  # Missing/stale manifest cannot prove close.

    def _run_batched(self) -> None:
        """Experimental maintenance worker; source/manifest semantics mirror _run."""
        output = None
        digest = sha256()
        count = 0
        last_publish = 0
        queue = cast(_BatchDrainQueue, self._queue)

        def seal() -> None:
            nonlocal output, count, digest
            if output is None:
                return
            output.flush()
            os.fsync(output.fileno())
            output.close()
            output = None
            checksum = digest.hexdigest()
            self._chain = sha256(
                bytes.fromhex(self._chain) + digest.digest() + struct.pack("<Q", count)
            ).hexdigest()
            self._segments.append(
                {
                    "file": f"commits-{len(self._segments):06d}.bin",
                    "records": count,
                    "bytes": count * RECORD.size,
                    "sha256": checksum,
                    "chain": self._chain,
                }
            )
            count, digest = 0, sha256()

        try:
            while not self._stop.is_set() or not queue.empty():
                try:
                    rows = queue.get_available(64, 0.1)
                except Empty:
                    if output is not None:
                        output.flush()
                    if monotonic_ns() - last_publish >= 1_000_000_000:
                        self._publish()
                        last_publish = monotonic_ns()
                    continue
                for row in rows:
                    if self._bytes + RECORD.size > self._limit or len(self._segments) >= 4096:
                        self._failed = True
                        self._dropped += 1
                        continue
                    if output is None:
                        path = self._root / f"commits-{len(self._segments):06d}.bin"
                        output = os.fdopen(
                            open_private(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY),
                            "wb",
                            buffering=1024**2,
                        )
                    encoded = RECORD.pack(*row)
                    output.write(encoded)
                    digest.update(encoded)
                    self._written += 1
                    self._bytes += RECORD.size
                    count += 1
                    if count == self._segment_records:
                        seal()
                        self._publish()
            seal()
        except Exception:
            self._failed = True
        finally:
            if output is not None:
                try:
                    output.close()
                except Exception:
                    self._failed = True
            self._closed = True
            try:
                self._publish()
            except Exception:
                self._failed = True

    def close(self) -> None:
        self._stop.set()
        self._thread.join(10)
        if self._thread.is_alive():
            self._failed = True
            raise RuntimeError("source timing evidence did not drain")


def verify_commit_timing(root: Path) -> tuple[int, int]:
    """Stream closed evidence integrity and LSN/timing coverage; not an SLO pass."""
    manifest = root / "manifest.json"
    if root.is_symlink() or manifest.is_symlink() or manifest.stat().st_size > 4 * 1024**2:
        raise ValueError("unsafe commit timing manifest")
    require_private_runtime_file(manifest)
    raw_value: object = json.loads(manifest.read_bytes())
    if not isinstance(raw_value, dict):
        raise ValueError("invalid commit timing manifest")
    raw = cast(dict[str, object], raw_value)
    segments_raw = raw.get("segments")
    if (
        raw.get("schema") != SCHEMA
        or raw.get("record_bytes") != RECORD.size
        or raw.get("closed") is not True
        or raw.get("failed") is not False
        or raw.get("dropped") != 0
        or not isinstance(segments_raw, list)
    ):
        raise ValueError("incomplete commit timing evidence")
    segments = cast(list[object], segments_raw)
    if len(segments) > 4096 or any(not isinstance(entry, dict) for entry in segments):
        raise ValueError("invalid commit timing segments")
    for field in ("first_lsn", "observed_final_lsn", "submitted", "written", "dropped", "bytes"):
        value = raw.get(field)
        if type(value) is not int or value < 0:
            raise ValueError("invalid commit timing counts")
    first_lsn = cast(int, raw["first_lsn"])
    if first_lsn < 1 or raw.get("clock") != "time.monotonic_ns":
        raise ValueError("invalid commit timing origin/clock")
    expected_files = {"manifest.json", *(f"commits-{n:06d}.bin" for n in range(len(segments)))}
    if {p.name for p in root.iterdir()} != expected_files:
        raise ValueError("unregistered/missing commit timing evidence")
    lsn, sequence, size, chain = first_lsn, 0, 0, "0" * 64
    last_after = 0
    for index, value in enumerate(segments):
        entry = cast(dict[str, object], value)
        name = f"commits-{index:06d}.bin"
        path = root / name
        records_expected = entry.get("records")
        if (
            entry.get("file") != name
            or path.is_symlink()
            or not path.is_file()
            or type(records_expected) is not int
            or not 1 <= records_expected <= 1000000
            or path.stat().st_size != records_expected * RECORD.size
        ):
            raise ValueError("unsafe commit timing segment")
        digest, records = sha256(), 0
        with path.open("rb") as source:
            while body := source.read(RECORD.size):
                if len(body) != RECORD.size:
                    raise ValueError("truncated commit timing row")
                seq, first, last, before, after, facts = RECORD.unpack(body)
                if (
                    seq != sequence + 1
                    or first != lsn
                    or last - first + 1 != facts
                    or not 0 < before <= after
                    or facts < 1
                    or before < last_after
                ):
                    raise ValueError("commit timing coverage is invalid")
                sequence, lsn = seq, last + 1
                last_after = after
                records += 1
                size += RECORD.size
                digest.update(body)
        chain = sha256(
            bytes.fromhex(chain) + digest.digest() + struct.pack("<Q", records)
        ).hexdigest()
        if (entry.get("records"), entry.get("bytes"), entry.get("sha256"), entry.get("chain")) != (
            records,
            records * RECORD.size,
            digest.hexdigest(),
            chain,
        ):
            raise ValueError("commit timing integrity mismatch")
    if (sequence, sequence, size, lsn - 1, chain) != (
        raw["submitted"],
        raw["written"],
        raw["bytes"],
        raw["observed_final_lsn"],
        raw["chain"],
    ):
        raise ValueError("commit timing total mismatch")
    return sequence, lsn - 1
