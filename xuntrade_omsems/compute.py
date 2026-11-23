"""Bounded deterministic compute with an explicit diagnostic Python backend.

Set XUNTRADE_BACKEND=native in deployment to reject a missing native extension.
The backend is selected once at process start; native runtime failures propagate.
"""

from __future__ import annotations

import os
from hashlib import blake2s
from typing import Sequence

MAX_BATCH_SIZE = 1_000_000
MAX_INT64 = (1 << 63) - 1
MIN_INT64 = -(1 << 63)
_MODE = os.environ.get("XUNTRADE_BACKEND", "native")
if _MODE not in {"auto", "native", "python"}:
    raise RuntimeError("XUNTRADE_BACKEND must be auto, native or python")

_native = None
_native_import_error: str | None = None
if _MODE != "python":
    try:
        from . import _native as _loaded_native
    except ImportError as exc:
        _native_import_error = str(exc)
        if _MODE == "native":
            raise RuntimeError("required XunTradeOMSEMS C++ engine is unavailable") from exc
    else:
        _native = _loaded_native


def backend_info() -> dict[str, object]:
    if _native is not None:
        return {**_native.backend_info(), "mode": _MODE}
    return {
        "engine": "python",
        "crc32c": "python-table",
        "api_version": 1,
        "maximum_workers": 1,
        "mode": _MODE,
        "native_import_error": _native_import_error,
    }


def require_native() -> None:
    if _native is None:
        raise RuntimeError("this profile requires the XunTradeOMSEMS C++ engine")


def _crc_table() -> tuple[int, ...]:
    values = []
    for seed in range(256):
        value = seed
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
        values.append(value)
    return tuple(values)


_CRC_TABLE = _crc_table()


def crc32c_python(data: bytes) -> int:
    """Reference implementation for diagnostics and equivalence tests."""
    if type(data) is not bytes:
        raise TypeError("crc32c requires immutable bytes")
    checksum = 0xFFFFFFFF
    for value in data:
        checksum = _CRC_TABLE[(checksum ^ value) & 0xFF] ^ (checksum >> 8)
    return checksum ^ 0xFFFFFFFF


# Bind once: journal calls avoid an extra Python dispatch on every frame.
crc32c = _native.crc32c if _native is not None else crc32c_python


def _count(value: int, maximum: int, label: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{label} must be builtin int")
    if not 1 <= value <= maximum:
        raise ValueError(f"{label} must be in [1, {maximum}]")


def _batch(values: Sequence[object]) -> tuple[object, ...]:
    if type(values) not in (tuple, list):
        raise TypeError("batch must be a list or tuple")
    if len(values) > MAX_BATCH_SIZE:
        raise ValueError("batch exceeds 1000000 rows")
    return tuple(values)


def session_shards(
    execution_ids: Sequence[str], shard_count: int, *, workers: int = 1
) -> tuple[int, ...]:
    """Compute v1 shard indices; EMS still enforces ownership and routing epochs."""
    _count(shard_count, 32, "shard_count")
    _count(workers, 32, "workers")
    if _native is not None:
        return _native.session_shards(execution_ids, shard_count, workers)
    ids = _batch(execution_ids)
    result = []
    for value in ids:
        if type(value) is not str:
            raise TypeError("execution IDs must be builtin str")
        if not 1 <= len(value) <= 256:
            raise ValueError("execution ID length must be in [1, 256]")
        digest = blake2s(value.encode("utf-8"), digest_size=16, person=b"tcshard1").digest()
        result.append(int.from_bytes(digest, "big") % shard_count)
    return tuple(result)


def assess_orders(
    orders: Sequence[tuple[int, int, int, int]], *, workers: int = 1
) -> tuple[tuple[int, int], ...]:
    """Assess independent (quantity, price_minor, available_minor, limit_minor).

    Results are (notional_minor, code): 0 accepted, 1 invalid, 2 overflow,
    3 order limit, 4 cash limit. Invalid/overflow results carry zero notional.
    Each row is an independent immutable snapshot. Acceptance is arithmetic
    eligibility only, never a cash reservation or permission to submit.
    """
    _count(workers, 32, "workers")
    if _native is not None:
        return _native.assess_orders(orders, workers)
    rows = _batch(orders)
    result = []
    for row in rows:
        if type(row) is not tuple or len(row) != 4:
            raise TypeError("each order must be a tuple of four int64 values")
        if any(type(value) is not int for value in row):
            raise TypeError("values must be builtin int (bool is forbidden)")
        if any(value < MIN_INT64 or value > MAX_INT64 for value in row):
            raise OverflowError("int value does not fit int64")
        quantity, price, available, limit = row
        if quantity <= 0 or price <= 0 or available < 0 or limit < 0:
            result.append((0, 1))
        elif quantity > MAX_INT64 // price:
            result.append((0, 2))
        else:
            notional = quantity * price
            result.append((notional, 3 if notional > limit else 4 if notional > available else 0))
    return tuple(result)
