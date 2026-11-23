"""Immutable, private, stream-built Consumer snapshots with bounded page reads.

Build/hash/fsync are maintenance work. Opening and cold reads belong to a read
lane, never the sole reactor. No whole-history list or mmap is kept by a pin.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Iterable, cast

import msgspec

from ..domain import ConsumerEventPositionV2, ConsumerOrderEvent
from .runtime_files import atomic_runtime_json, open_private, require_private_runtime_file

_ENTRY = struct.Struct("<QIII")
_DECODER = msgspec.json.Decoder(ConsumerOrderEvent)


def build_frozen_consumer_snapshot(
    *,
    root: Path,
    scope: tuple[str, str, str],
    account_id: str,
    position: ConsumerEventPositionV2,
    orders: Iterable[ConsumerOrderEvent],
) -> None:
    if position.stream_id.redis_seq or position.fact_ordinal:
        raise ValueError("frozen mmap snapshot requires scalar LSN positions")
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    if root.is_symlink():
        raise ValueError("unsafe frozen snapshot root")
    data_digest, index_digest = sha256(), sha256()
    count, offset, previous = 0, 0, ""
    with (
        os.fdopen(
            open_private(root / "orders.bin", os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb"
        ) as data,
        os.fdopen(
            open_private(root / "positions.bin", os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb"
        ) as index,
    ):
        for event in orders:
            if (
                event.oms_order_id <= previous
                or (
                    event.consumer_trace.consumer_id,
                    event.consumer_trace.consumer_account_id,
                    event.journal_id,
                )
                != scope
                or event.account_id != account_id
                or event.consumer_trace.account_id != account_id
                or event.journal_lsn > position.stream_id.redis_ms
            ):
                raise ValueError("snapshot input is unordered or outside scope/watermark")
            encoded = msgspec.json.encode(event)
            if len(encoded) > 65536:
                raise ValueError("snapshot row exceeds bounded read capacity")
            prefix = struct.pack("<QII", offset, len(encoded), zlib.crc32(encoded))
            entry = prefix + struct.pack("<I", zlib.crc32(prefix))
            data.write(encoded)
            index.write(entry)
            data_digest.update(encoded)
            index_digest.update(entry)
            offset += len(encoded)
            count += 1
            previous = event.oms_order_id
        for output in (data, index):
            output.flush()
            os.fsync(output.fileno())
    atomic_runtime_json(
        root / "snapshot.json",
        {
            "schema_version": 1,
            "scope": scope,
            "account_id": account_id,
            "position": position.to_external_id(),
            "order_count": count,
            "data_bytes": offset,
            "data_sha256": data_digest.hexdigest(),
            "index_sha256": index_digest.hexdigest(),
        },
    )


def _digest_file(path: Path) -> str:
    value = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


class FrozenConsumerSnapshot:
    retained_bytes = 32 * 1024  # conservative private handle/metadata pin budget, no data cache

    def __init__(self, root: Path) -> None:
        if root.is_symlink() or (
            os.name == "posix"
            and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        ):
            raise ValueError("frozen snapshot root is unsafe")
        metadata = root / "snapshot.json"
        require_private_runtime_file(metadata)
        if metadata.stat().st_size > 4096:
            raise ValueError("snapshot manifest is oversized")
        raw: object = json.loads(metadata.read_text())
        if not isinstance(raw, dict):
            raise ValueError("snapshot manifest is invalid")
        value = cast(dict[str, object], raw)
        if (
            set(value)
            != {
                "schema_version",
                "scope",
                "account_id",
                "position",
                "order_count",
                "data_bytes",
                "data_sha256",
                "index_sha256",
            }
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise ValueError("snapshot manifest schema is invalid")
        scope, count, size = value["scope"], value["order_count"], value["data_bytes"]
        if (
            not isinstance(scope, list)
            or len(cast(list[object], scope)) != 3
            or any(type(v) is not str or not v for v in cast(list[object], scope))
            or type(count) is not int
            or count < 0
            or type(size) is not int
            or size < 0
        ):
            raise ValueError("snapshot manifest scope/capacity is invalid")
        self.scope = cast(tuple[str, str, str], tuple(cast(list[str], scope)))
        if not isinstance(value["account_id"], str) or not value["account_id"]:
            raise ValueError("snapshot account binding is missing")
        self.account_id = value["account_id"]
        self.order_count = count
        self.snapshot_position = ConsumerEventPositionV2.from_external_id(value["position"])
        for name, expected in [
            ("orders.bin", value["data_sha256"]),
            ("positions.bin", value["index_sha256"]),
        ]:
            path = root / name
            require_private_runtime_file(path)
            if _digest_file(path) != expected:
                raise ValueError("snapshot checksum mismatch")
        if (root / "orders.bin").stat().st_size != size or (
            root / "positions.bin"
        ).stat().st_size != count * _ENTRY.size:
            raise ValueError("snapshot files do not match manifest capacity")
        self._data = os.fdopen(open_private(root / "orders.bin", os.O_RDONLY), "rb", buffering=0)
        try:
            self._index = os.fdopen(
                open_private(root / "positions.bin", os.O_RDONLY), "rb", buffering=0
            )
        except BaseException:
            self._data.close()
            raise
        self._lock = Lock()
        self._closed = False

    def read_orders(self, start: int, end: int) -> tuple[ConsumerOrderEvent, ...]:
        if not 0 <= start <= end <= self.order_count or end - start > 200:
            raise ValueError("snapshot page range is invalid")
        with self._lock:
            if self._closed:
                raise ValueError("snapshot pin expired")
            self._index.seek(start * _ENTRY.size)
            entries = self._index.read((end - start) * _ENTRY.size)
            if len(entries) != (end - start) * _ENTRY.size:
                raise ValueError("snapshot index is truncated")
            result: list[ConsumerOrderEvent] = []
            for n in range(end - start):
                entry = entries[n * _ENTRY.size : (n + 1) * _ENTRY.size]
                offset, length, checksum, index_crc = _ENTRY.unpack(entry)
                if zlib.crc32(entry[:-4]) != index_crc or not 0 < length <= 65536:
                    raise ValueError("snapshot index frame is corrupt")
                self._data.seek(offset)
                encoded = self._data.read(length)
                if len(encoded) != length or zlib.crc32(encoded) != checksum:
                    raise ValueError("snapshot data frame is corrupt")
                event = _DECODER.decode(encoded)
                if (
                    event.consumer_trace.consumer_id,
                    event.consumer_trace.consumer_account_id,
                    event.journal_id,
                ) != self.scope:
                    raise ValueError("snapshot row scope changed")
                if (
                    event.account_id != self.account_id
                    or event.consumer_trace.account_id != self.account_id
                    or event.journal_lsn > self.snapshot_position.stream_id.redis_ms
                ):
                    raise ValueError("snapshot row account/watermark changed")
                if result and result[-1].oms_order_id >= event.oms_order_id:
                    raise ValueError("snapshot order sort is corrupt")
                result.append(event)
            return tuple(result)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._index.close()
                self._data.close()
                self._closed = True
