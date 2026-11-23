"""Restricted durable evidence sink for the independent fault supervisor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..domain._validation import require_nonnegative
from .fault_monitor.gap_page import RuntimeFaultGapPageV1
from .fault_supervisor import RuntimeFaultGapEvidenceV1


class RuntimeFaultEvidenceStoreV1:
    """Atomically write payload-free fault evidence below a dedicated root."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("fault evidence root must be absolute")
        self._root = root

    def persist(self, evidence: RuntimeFaultGapEvidenceV1) -> None:
        name = (
            f"gap-{evidence.process_epoch}-{evidence.first_sequence}-{evidence.last_sequence}.json"
        )
        self._write_json(
            name,
            {
                "dropped_count": evidence.dropped_count,
                "first_sequence": evidence.first_sequence,
                "last_sequence": evidence.last_sequence,
                "observed_monotonic_ns": evidence.observed_monotonic_ns,
                "process_epoch": evidence.process_epoch,
                "schema_version": 1,
                "severity_bitmap": evidence.severity_bitmap,
            },
        )

    def persist_heartbeat(self, heartbeat_monotonic_ns: int) -> None:
        self._write_json(
            "fault-supervisor-heartbeat.v1.json",
            {
                "monotonic_ns": require_nonnegative(heartbeat_monotonic_ns, "supervisor heartbeat"),
                "schema_version": 1,
            },
        )

    def _write_json(self, name: str, document: dict[str, int]) -> None:
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
        target = self._root / name
        temporary = self._root / f".{name}.{os.getpid()}.tmp"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                raise OSError("fault evidence write was incomplete")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, target)


class RuntimeFaultGapPageFileReaderV1:
    """Read exactly one externally-owned fixed gap page without parsing payloads."""

    def __init__(self, path: Path, *, page_bytes: int) -> None:
        if not path.is_absolute():
            raise ValueError("fault gap page path must be absolute")
        self._path = path
        self._page_bytes = require_nonnegative(page_bytes, "fault gap page byte size")
        if self._page_bytes == 0:
            raise ValueError("fault gap page byte size must be positive")

    def read(self) -> bytes:
        descriptor = os.open(self._path, os.O_RDONLY)
        try:
            result = os.read(descriptor, self._page_bytes + 1)
        finally:
            os.close(descriptor)
        if len(result) != self._page_bytes:
            raise ValueError("fault gap page file has an invalid fixed size")
        return result


class RuntimeFaultGapPageFileInitializerV1:
    """Create one startup page without replacing a persisted active-gap witness."""

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("fault gap page path must be absolute")
        self._path = path

    def initialize(self, page: RuntimeFaultGapPageV1) -> bool:
        """Write an inactive startup page once, or validate and preserve an existing page."""

        if not self._path.parent.is_dir():
            raise ValueError("fault gap page parent directory is unavailable")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self._path, flags, 0o600)
        except FileExistsError:
            RuntimeFaultGapPageV1.decode(
                RuntimeFaultGapPageFileReaderV1(self._path, page_bytes=len(page.encode())).read()
            )
            return False
        try:
            encoded = page.encode()
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                raise OSError("fault gap page initialization was incomplete")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return True
