"""One bounded metadata-only owner for full Journal segment verification.

The parent retains the writer lease. The child can only seal a named segment
under its fixed private Journal root; it cannot append facts or call an SDK.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import re
import socket
import struct
from hashlib import sha256
from pathlib import Path
from threading import Lock
from time import monotonic, monotonic_ns
from typing import Literal

import msgspec

from ..oms.mmap_fact_journal_v1 import MmapOmsFactJournalRecoveryRequired
from .runtime_files import require_private_runtime_file

_LIMIT = 4096
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
SealCut = tuple[int, int, int, int, int]


class _Request(msgspec.Struct, forbid_unknown_fields=True):
    serial: int
    index: int
    cut: SealCut
    operation: Literal["seal", "prepare"] = "seal"
    prefault: bool = True


class _Response(msgspec.Struct, forbid_unknown_fields=True):
    serial: int
    index: int
    cut: SealCut
    digest: str
    operation: Literal["seal", "prepare"] = "seal"


def _receive(channel: socket.socket, deadline: float | None = None) -> bytes | None:
    def read(size: int) -> bytes:
        result = bytearray()
        while len(result) < size:
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("segment control deadline expired")
                channel.settimeout(remaining)
            block = channel.recv(size - len(result))
            if not block:
                if not result:
                    return b""
                raise ValueError("segment control truncated")
            result.extend(block)
        return bytes(result)

    header = read(4)
    if not header:
        return None
    size = struct.unpack("!I", header)[0]
    if not 0 < size <= _LIMIT:
        raise ValueError("segment control exceeds bound")
    body = read(size)
    if len(body) != size:
        raise ValueError("segment control truncated")
    return body


def _send(channel: socket.socket, value: _Request | _Response) -> None:
    encoded = msgspec.msgpack.encode(value)
    if not 0 < len(encoded) <= _LIMIT:
        raise ValueError("segment control exceeds bound")
    channel.sendall(struct.pack("!I", len(encoded)) + encoded)


def _run(channel: socket.socket, root: Path, journal_id: str, frames: int, cpu: int | None):
    # Imported inside the child to keep bootstrap dependencies acyclic.
    from ..oms.segmented_fact_journal import _Segment  # pyright: ignore[reportPrivateUsage]

    diagnostic = None
    try:
        if cpu is not None:
            os.sched_setaffinity(0, {cpu})
        pacing = os.environ.get("TRADECORE_DIAGNOSTIC_PACED_PREFAULT", "0")
        if pacing not in {"0", "1", "64"}:
            raise ValueError("invalid prefault diagnostic pacing")
        paced_prefault_pages = {"0": 0, "1": 256, "64": 64}[pacing]
        diagnostic_dir = os.environ.get("TRADECORE_DIAGNOSTIC_SEGMENT_TRACE_DIR")
        if diagnostic_dir is not None:
            from ..consumer.request_trace_v2 import BoundedRequestTraceWriterV2

            trace_root = Path(diagnostic_dir)
            if not trace_root.is_absolute() or trace_root.is_symlink() or not trace_root.is_dir():
                raise ValueError("segment diagnostic directory unavailable")
            diagnostic = BoundedRequestTraceWriterV2(
                trace_root / f"segment-maintenance-{os.getpid()}.jsonl",
                journal_id=journal_id,
                capacity=4096,
                max_bytes=16 * 1024 * 1024,
                rotation_bytes=4 * 1024 * 1024,
                record_schema="consumer-stage-trace-v1",
            )
        while (body := _receive(channel)) is not None:
            request = msgspec.msgpack.decode(body, type=_Request)
            if request.serial < 1 or request.index < 1:
                raise ValueError("invalid segment identity")
            metadata = root.stat()
            if (
                root.is_symlink()
                or not root.is_dir()
                or (
                    os.name == "posix"
                    and (metadata.st_uid != os.geteuid() or metadata.st_mode & 0o077)
                )
            ):
                raise ValueError("segment root is unsafe")
            target = root / f"segment-{request.index:012d}.mmap"
            preparing = request.operation == "prepare"
            if preparing:
                if request.cut != (0, 0, 0, 0, 0):
                    raise ValueError("new segment cannot carry an activated cut")
            else:
                require_private_runtime_file(target)

            def emit(phase: str, state: str, at_ns: int) -> None:
                if diagnostic is not None:
                    diagnostic.submit(
                        {
                            "scope": "candidate_segment_maintenance",
                            "journal_id_sha256": sha256(journal_id.encode()).hexdigest(),
                            "pid": os.getpid(),
                            "serial": request.serial,
                            "segment_index": request.index,
                            "operation": request.operation,
                            "phase": phase,
                            "state": state,
                            "at_ns": at_ns,
                            "allocated_bytes": 4096 + frames * 4096,
                            "prefault_pages": frames if request.prefault and preparing else 0,
                        }
                    )

            emit("operation", "BEGIN", monotonic_ns())
            try:
                segment = _Segment(
                    target,
                    index=request.index,
                    frames=frames,
                    journal_id=journal_id,
                    create=preparing,
                    prefault=request.prefault,
                    stage_event=emit if diagnostic is not None else None,
                    diagnostic_paced_prefault_pages=paced_prefault_pages,
                )
                try:
                    # A spare acknowledgement is NOT a sealed source digest.
                    # The parent still verifies its identity and activation.
                    digest = "0" * 64 if preparing else segment.seal(expected=request.cut)
                finally:
                    segment.close()
            except BaseException:
                emit("operation", "FAILED", monotonic_ns())
                raise
            else:
                emit("operation", "END", monotonic_ns())
            _send(
                channel,
                _Response(request.serial, request.index, request.cut, digest, request.operation),
            )
    except Exception:
        # No partial success or exception/business text crosses the channel.
        # EOF is a sticky parent-side failure; the raw source stays retained.
        pass
    finally:
        if diagnostic is not None:
            diagnostic.close()
        channel.close()


class ProcessSegmentSealer:
    def __init__(
        self,
        *,
        root: Path,
        journal_id: str,
        frames: int,
        cpu: int | None = None,
        timeout_seconds: float = 20,
    ) -> None:
        if (
            not root.is_absolute()
            or not journal_id
            or len(journal_id) > 128
            or type(frames) is not int
            or not 2 <= frames <= 1_000_000
            or (cpu is not None and (type(cpu) is not int or cpu < 0))
            or type(timeout_seconds) not in (int, float)
            or not 0.01 <= timeout_seconds <= 20
        ):
            raise ValueError("segment sealer configuration invalid")
        self._channel, child = socket.socketpair()
        self._lock = Lock()
        self._serial = 0
        self._timeout = timeout_seconds
        self._closed = self._failed = False
        self._process = mp.get_context("spawn").Process(
            target=_run,
            args=(child, root, journal_id, frames, cpu),
            name="journal-segment-sealer",
        )
        try:
            self._process.start()
        except BaseException:
            self._channel.close()
            raise
        finally:
            child.close()

    def __call__(self, index: int, cut: SealCut) -> str:
        return self._request(index, cut, "seal", True)

    def prepare(self, index: int, prefault: bool) -> None:
        if type(index) is not int or index < 1 or type(prefault) is not bool:
            raise ValueError("invalid new segment identity/profile")
        self._request(index, (0, 0, 0, 0, 0), "prepare", prefault)

    def _request(
        self, index: int, cut: SealCut, operation: Literal["seal", "prepare"], prefault: bool
    ) -> str:
        with self._lock:
            if self._closed or self._failed or not self._process.is_alive():
                raise MmapOmsFactJournalRecoveryRequired("segment sealer unavailable")
            try:
                self._serial += 1
                request = _Request(self._serial, index, cut, operation, prefault)
                deadline = monotonic() + self._timeout
                self._channel.settimeout(self._timeout)
                _send(self._channel, request)
                body = _receive(self._channel, deadline)
                if body is None:
                    raise ValueError("segment sealer closed")
                response = msgspec.msgpack.decode(body, type=_Response)
                if (
                    response.serial != request.serial
                    or response.index != index
                    or response.cut != cut
                    or response.operation != operation
                    or _DIGEST.fullmatch(response.digest) is None
                    or (operation == "prepare" and response.digest != "0" * 64)
                ):
                    raise ValueError("segment sealer response mismatched")
                return response.digest
            except Exception:
                self._failed = True
                raise MmapOmsFactJournalRecoveryRequired("segment seal was not verified") from None

    def close(self) -> None:
        if self._closed:
            return
        if not self._lock.acquire(timeout=self._timeout + 1):
            raise MmapOmsFactJournalRecoveryRequired("segment sealer did not drain")
        try:
            self._channel.close()
            self._process.join(timeout=3)
            forced = self._process.is_alive()
            if forced:
                self._process.terminate()
                self._process.join(timeout=1)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(timeout=1)
            if forced or self._process.is_alive():
                raise MmapOmsFactJournalRecoveryRequired("segment sealer required forced stop")
            self._closed = True
        finally:
            self._lock.release()

    @property
    def shutdown_verified(self) -> bool:
        return self._closed and not self._failed and self._process.exitcode == 0
