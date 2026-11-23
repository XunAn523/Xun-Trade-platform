"""Bounded MPSC callback ring with an optional native C backend.

The C backend uses a fixed-size ring while Python holds the GIL, which gives
multiple vendor callback threads a single safe producer boundary.  Source is
shipped in ``native/fastspi_ring.c`` and is optional at build time; deployments
that require native throughput must set ``require_native_fastspi=True``.
"""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from threading import Lock
from time import perf_counter_ns
from typing import Any

try:  # Built opportunistically by setup.py; absence is an explicit fallback.
    from ._fastspi import (
        NativeMpscRing,  # type: ignore[import-not-found]  # optional compiled extension
    )
except ImportError:  # pragma: no cover - depends on local compiler availability.
    NativeMpscRing = None  # type: ignore[assignment,misc]  # native extension is optional at runtime


class FastSpiUnavailable(RuntimeError):
    """A deployment required the native ring but no extension was built."""


class _PythonFallbackRing:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._items: deque[Any] = deque()
        self._lock = Lock()

    def try_push(self, item: Any) -> bool:
        with self._lock:
            if len(self._items) >= self._capacity:
                return False
            self._items.append(item)
            return True

    def pop(self) -> Any | None:
        with self._lock:
            return self._items.popleft() if self._items else None

    @property
    def depth(self) -> int:
        with self._lock:
            return len(self._items)


class FastSpiMpscRing:
    """Small minimal-fact callback ring used by ``CallbackPipeline``.

    A fallback is functional but intentionally observable via
    :attr:`native_available`; it must not be accepted as a high-throughput
    production performance result.
    """

    def __init__(self, capacity: int, *, require_native: bool = False) -> None:
        if capacity <= 0:
            raise ValueError("callback ring capacity must be positive")
        if NativeMpscRing is None:
            if require_native:
                raise FastSpiUnavailable(
                    "native fastspi extension is unavailable; install with a C compiler"
                )
            self._backend: Any = _PythonFallbackRing(capacity)
            self._native_available = False
        else:
            self._backend = NativeMpscRing(capacity)
            self._native_available = True

    @property
    def native_available(self) -> bool:
        return self._native_available

    def try_push(self, item: Any) -> int | None:
        """Add a dataclass-like minimal callback fact and return enqueue time."""

        enqueued_ns = perf_counter_ns()
        try:
            stored = replace(item, ring_enqueue_ns=enqueued_ns)
        except TypeError as exc:
            raise TypeError("fastspi items must be dataclass callback records") from exc
        return enqueued_ns if self._backend.try_push(stored) else None

    def pop(self) -> Any | None:
        return self._backend.pop()

    @property
    def depth(self) -> int:
        return int(self._backend.depth)
