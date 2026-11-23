"""Deployment-only runtime checks for the Windows QMT Edge.

The Edge store itself remains usable in offline contract tests.  A real Windows
service calls :func:`require_supported_qmt_edge_sqlite` before opening its
database so an old bundled SQLite cannot silently become a production WAL
runtime.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .edge_config import QmtEdgeConfigV1
from .edge_store import QmtEdgeStore, QmtEdgeStoreError
from .native_engine_client import QmtNativeEngineClient
from .native_engine_contract import QmtRingHeaderV1
from .windows_ring import QmtNativeAtomicLibrary, QmtNativeRing, QmtNativeRingSnapshot

# SQLite documents the WAL-reset correction in 3.44.6, 3.50.7, and 3.51.3;
# the first two are official backports, not vendor exceptions.  Keep the
# accepted branches explicit so deployment cannot infer safety from the host
# Python version or from an arbitrary newer-looking patch release.
# https://www.sqlite.org/wal.html
_QMT_EDGE_SQLITE_WAL_FIXED_RANGES: tuple[
    tuple[tuple[int, int, int], tuple[int, int, int] | None], ...
] = (
    ((3, 44, 6), (3, 45, 0)),
    ((3, 50, 7), (3, 51, 0)),
    ((3, 51, 3), None),
)

# Public compatibility marker: this is the earliest accepted release, not a
# simple numerical floor.  Callers must use ``require_supported_qmt_edge_sqlite``
# because intermediate branches such as 3.49.x are intentionally rejected.
MIN_QMT_EDGE_SQLITE_VERSION = _QMT_EDGE_SQLITE_WAL_FIXED_RANGES[0][0]


def _has_official_wal_reset_fix(version: tuple[int, int, int]) -> bool:
    return any(
        version >= lower and (upper is None or version < upper)
        for lower, upper in _QMT_EDGE_SQLITE_WAL_FIXED_RANGES
    )


def require_supported_qmt_edge_sqlite(
    version: Sequence[int] = sqlite3.sqlite_version_info,
) -> tuple[int, int, int]:
    """Return the approved SQLite version or fail before opening Edge WAL.

    Only SQLite releases carrying SQLite's official WAL-reset correction are
    accepted.  No caller may bypass the gate by changing ``synchronous`` or
    disabling WAL.
    """

    normalized = tuple(int(part) for part in version[:3])
    if len(normalized) != 3 or any(part < 0 for part in normalized):
        raise QmtEdgeStoreError("QMT Edge SQLite runtime version is malformed")
    if not _has_official_wal_reset_fix(normalized):
        current = ".".join(str(part) for part in normalized)
        raise QmtEdgeStoreError(
            "QMT Edge requires an official WAL-reset-fixed SQLite build "
            "(3.44.6-3.44.x, 3.50.7-3.50.x, or 3.51.3+); "
            f"got {current}"
        )
    return normalized


@dataclass(frozen=True)
class QmtEdgeRuntimeSnapshot:
    """Payload-free readiness view for one locally opened Edge scope."""

    mode: str
    mutation_allowed: bool
    sqlite_version: tuple[int, int, int]
    command_ring: QmtNativeRingSnapshot
    event_ring: QmtNativeRingSnapshot


class QmtEdgeRuntime:
    """Compose the local Edge state machine and its two SPSC rings.

    This runtime deliberately has no TCP listener and no QMT API object.  It is
    the only Windows Edge process permitted to own the SQLite writer and the
    producer/consumer sides of the command/event rings.  The separately loaded
    QMT model owns the reciprocal sides.
    """

    def __init__(
        self,
        *,
        config: QmtEdgeConfigV1,
        store: QmtEdgeStore,
        command_ring: QmtNativeRing,
        event_ring: QmtNativeRing,
        sqlite_version: tuple[int, int, int],
    ) -> None:
        self.config = config
        self.store = store
        self.command_ring = command_ring
        self.event_ring = event_ring
        self.native_client = QmtNativeEngineClient(
            store=store, command_ring=command_ring, event_ring=event_ring
        )
        self._sqlite_version = sqlite_version

    @classmethod
    def open(cls, config: QmtEdgeConfigV1) -> "QmtEdgeRuntime":
        """Open a scope after all deployment prerequisites have been checked."""

        if not config.is_disarmed and (config.mode != "ARMED" or not config.allow_mutation):
            raise QmtEdgeStoreError("QMT Edge runtime mutation state is invalid")
        config.require_native_assets()
        sqlite_version = require_supported_qmt_edge_sqlite()
        native_library = QmtNativeAtomicLibrary(config.atomic_dll_path)
        header = QmtRingHeaderV1(
            scope_digest=config.scope_digest,
            generation=config.generation,
            slot_size=512,
            capacity=config.ring_capacity,
        )
        command_ring = cls._open_or_create_ring(
            config.command_ring_path, header=header, native_library=native_library
        )
        try:
            event_ring = cls._open_or_create_ring(
                config.event_ring_path, header=header, native_library=native_library
            )
        except BaseException:
            command_ring.close()
            raise
        try:
            config.database_path.parent.mkdir(parents=True, exist_ok=True)
            store = QmtEdgeStore(config.database_path, synchronous=config.sqlite_synchronous)
        except BaseException:
            command_ring.close()
            event_ring.close()
            raise
        return cls(
            config=config,
            store=store,
            command_ring=command_ring,
            event_ring=event_ring,
            sqlite_version=sqlite_version,
        )

    def snapshot(self) -> QmtEdgeRuntimeSnapshot:
        return QmtEdgeRuntimeSnapshot(
            mode=self.config.mode,
            mutation_allowed=self.config.allow_mutation,
            sqlite_version=self._sqlite_version,
            command_ring=self.command_ring.snapshot(),
            event_ring=self.event_ring.snapshot(),
        )

    def close(self) -> None:
        """Close local resources without changing any native request state."""

        try:
            self.store.close()
        finally:
            try:
                self.command_ring.close()
            finally:
                self.event_ring.close()

    @staticmethod
    def _open_or_create_ring(
        path: Path, *, header: QmtRingHeaderV1, native_library: QmtNativeAtomicLibrary
    ) -> QmtNativeRing:
        if path.exists():
            return QmtNativeRing.open_file(
                path,
                expected_scope_digest=header.scope_digest,
                expected_generation=header.generation,
                native_library=native_library,
            )
        return QmtNativeRing.create_file(path, header, native_library=native_library)
