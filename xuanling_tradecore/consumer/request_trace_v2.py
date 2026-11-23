"""Optional bounded, redacted request evidence; never a trading durability fence."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Thread
from time import monotonic_ns, time_ns
from typing import Callable, Generator, Mapping

import msgspec

from ..persistence.clock_identity import monotonic_clock_identity
from ..persistence.rotating_request_trace import RotatingRequestTraceFile

_CURRENT: ContextVar[dict[str, object] | None] = ContextVar("consumer_request_trace", default=None)


@contextmanager
def request_trace_scope(record: dict[str, object] | None) -> Generator[None, None, None]:
    token = _CURRENT.set(record)
    try:
        yield
    finally:
        _CURRENT.reset(token)


def mark_request_stage(name: str) -> None:
    record = _CURRENT.get()
    if record is not None:
        record[name] = monotonic_ns()


def mark_response_kind(kind: str) -> None:
    record = _CURRENT.get()
    if record is not None:
        record["response_kind"] = kind


def mark_request_value(name: str, value: int | str | bool) -> None:
    """Attach one bounded non-business diagnostic value to the active trace."""

    record = _CURRENT.get()
    if record is not None:
        record[name] = value


def mark_request_failure(stage: str, code: str) -> None:
    """Record a stable failure classification without exception text or payload."""

    record = _CURRENT.get()
    if record is not None:
        record["failure_stage"] = stage
        record["failure_code"] = code


class BoundedRequestTraceWriterV2:
    """One producer, bounded queue and file, one asynchronous JSON writer.

    No payload, account, cursor, raw request ID or exception text is recorded.
    Status is atomically replaced after flush. Any drop/failure invalidates full
    coverage. Flush is evidence visibility, NOT fsync/checkpoint durability.
    """

    def __init__(
        self,
        path: Path,
        *,
        journal_id: str,
        capacity: int = 65_536,
        max_bytes: int = 512 * 1024 * 1024,
        rotation_bytes: int | None = None,
        rotation_interval_ns: int = 60_000_000_000,
        record_schema: str = "consumer-server-request-trace-v1",
    ) -> None:
        if (
            capacity < 1
            or max_bytes < 1
            or record_schema
            not in {
                "consumer-server-request-trace-v1",
                "consumer-stage-trace-v1",
                "consumer-owner-ipc-trace-v1",
            }
        ):
            raise ValueError("trace limits must be positive")
        # Exclusive creation prevents accidental replacement of earlier evidence.
        self._file = (
            path.open("xb")
            if rotation_bytes is None
            else RotatingRequestTraceFile(
                path,
                journal_id=journal_id,
                segment_bytes=rotation_bytes,
                interval_ns=rotation_interval_ns,
            )
        )
        if rotation_bytes is None:
            path.chmod(0o600)
        self.path = path
        self._queue: Queue[dict[str, object]] = Queue(capacity)
        self._stop = Event()
        self._max_bytes = max_bytes
        self._written_bytes = 0
        self._submitted = self._written = self._queue_dropped = self._capacity_dropped = 0
        self._peak_queue = 0
        self._failed = False
        self._drained = False
        self._closed = False
        self._status_provider: Callable[[], Mapping[str, object]] | None = None
        self._status_provider_failed = False
        self._metadata = {
            "schema_version": record_schema,
            "journal_id": journal_id,
            "pid": os.getpid(),
            "clock": "time.monotonic_ns",
            "clock_identity_sha256": monotonic_clock_identity(),
            "anchor_monotonic_ns": monotonic_ns(),
            "anchor_wall_ns": time_ns(),
            "queue_capacity": capacity,
            "max_bytes": max_bytes,
            "scope": "request-reactor complete frames; pre-admission rejects use ingress counters"
            if record_schema == "consumer-server-request-trace-v1"
            else "trusted inherited owner IPC; NOT public socket ingress"
            if record_schema == "consumer-owner-ipc-trace-v1"
            else "inclusive internal stage observations; not request outcomes",
        }
        self._encoder = msgspec.json.Encoder(order="deterministic")
        self._publish_status()
        self._thread = Thread(target=self._run, name="consumer-trace-writer", daemon=True)
        self._thread.start()

    def set_status_provider(self, provider: Callable[[], Mapping[str, object]]) -> None:
        """Add bounded runtime diagnostics to the asynchronous status sidecar.

        The provider is invoked only by the trace writer thread. Request
        producers therefore never perform status serialization or filesystem
        work, and a diagnostic failure cannot fail request processing.
        """

        if self._closed or self._status_provider is not None:
            raise RuntimeError("trace status provider is already fixed")
        self._status_provider = provider

    def submit(self, record: dict[str, object]) -> None:
        self._submitted += 1
        if self._failed or self._closed:
            self._queue_dropped += 1
            return
        try:
            payload = dict(record)
            if isinstance(self._file, RotatingRequestTraceFile):
                payload["trace_sequence"] = self._submitted
            self._queue.put_nowait(payload)
            self._peak_queue = max(self._peak_queue, self._queue.qsize())
        except Full:
            self._queue_dropped += 1

    def _publish_status(self) -> None:
        diagnostics: dict[str, object] = {}
        provider = self._status_provider
        if provider is not None:
            try:
                diagnostics = dict(provider())
            except Exception:  # diagnostics must never stop the evidence writer
                self._status_provider_failed = True
        status = {
            **self._metadata,
            "submitted": self._submitted,
            "written": self._written,
            "queue_dropped": self._queue_dropped,
            "capacity_dropped": self._capacity_dropped,
            "writer_failed": self._failed,
            "queue_depth": self._queue.qsize(),
            "peak_queue": self._peak_queue,
            "written_bytes": self._written_bytes,
            "updated_monotonic_ns": monotonic_ns(),
            "closed": self._closed,
            "drain_complete": self._drained,
            "status_provider_failed": self._status_provider_failed,
            **diagnostics,
            "trace_storage": self._file.stats()
            if isinstance(self._file, RotatingRequestTraceFile)
            else {"mode": "single"},
        }
        target = self.path.with_suffix(".status.json")
        temporary = target.with_suffix(".tmp")
        # dumps uses the compact C encoder; dump iterates/writes thousands of
        # tiny Python chunks for bounded GC/cgroup diagnostics. The sidecar
        # bytes and visibility semantics stay unchanged, with one buffered write.
        encoded_status = json.dumps(status, separators=(",", ":"))
        with temporary.open("w", encoding="utf-8") as output:
            temporary.chmod(0o600)
            output.write(encoded_status)
        temporary.replace(target)

    def _run(self) -> None:
        try:
            last_status_ns = 0
            while not self._stop.is_set() or not self._queue.empty():
                deadline = monotonic_ns() + 100_000_000
                try:
                    batch = [self._queue.get(timeout=0.1)]
                except Empty:
                    batch = []
                for _ in range(255):
                    try:
                        if self._stop.is_set():
                            batch.append(self._queue.get_nowait())
                        else:
                            remaining = max(0, deadline - monotonic_ns()) / 1e9
                            batch.append(self._queue.get(timeout=remaining))
                    except Empty:
                        break
                if isinstance(self._file, RotatingRequestTraceFile):
                    for record in batch:
                        size = self._file.append_record(
                            record, maximum_remaining=self._max_bytes - self._written_bytes
                        )
                        if not size:
                            self._capacity_dropped += 1
                            continue
                        self._written += 1
                        self._written_bytes += size
                else:
                    encoded: list[bytes] = []
                    batch_bytes = 0
                    for record in batch:
                        line = self._encoder.encode(record) + b"\n"
                        if self._written_bytes + batch_bytes + len(line) > self._max_bytes:
                            self._capacity_dropped += 1
                            continue
                        encoded.append(line)
                        self._written += 1
                        batch_bytes += len(line)
                    if encoded:
                        self._file.write(b"".join(encoded))
                        self._written_bytes += batch_bytes
                self._file.flush()
                if monotonic_ns() - last_status_ns >= 250_000_000:
                    self._publish_status()
                    last_status_ns = monotonic_ns()
        except (OSError, TypeError, ValueError):
            self._failed = True
        finally:
            self._closed = True
            try:
                self._file.close()
            except (OSError, ValueError):
                self._failed = True
            finally:
                self._drained = not self._failed and self._queue.empty()
                try:
                    self._publish_status()
                except OSError:
                    self._failed = True
                    self._drained = False  # stale/missing status cannot attest complete coverage

    def close(self, *, timeout_seconds: float = 10.0) -> None:
        if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 60:
            raise ValueError("trace drain timeout must be finite and bounded")
        self._stop.set()
        self._thread.join(timeout=timeout_seconds)
        if self._thread.is_alive():
            self._failed = True
            # Status may still be the writer's previous on-disk sample.
            raise RuntimeError("request trace drain timed out")

    @property
    def coverage_verified(self) -> bool:
        return (
            self._closed
            and self._drained
            and not self._failed
            and not self._status_provider_failed
            and self._queue_dropped == self._capacity_dropped == 0
            and self._submitted == self._written
        )
