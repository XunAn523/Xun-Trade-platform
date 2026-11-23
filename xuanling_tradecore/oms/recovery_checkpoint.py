"""Strict checkpoint DTOs and atomic two-generation storage for a replay worker.

Checkpoint encoding and fsync belong to a quiescent replay/maintenance process,
never the live admission thread. The state reducer owns semantic validation.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from hashlib import sha256
from pathlib import Path
from typing import cast

import msgspec

from ..persistence.runtime_files import (
    atomic_runtime_json,
    open_private,
    require_private_runtime_file,
)
from .order_book import OmsOrderBookError


class OrderCheckpointRow(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    admission_seed: bytes
    state: str
    order_revision: int
    filled_quantity: int
    trade_keys: tuple[str, ...]
    reservation_revision: int


class EventCheckpointRow(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    event_id: str
    kind: str
    account_id: str
    oms_order_id: str
    execution_id: str
    order_state: str
    order_revision: int
    journal_lsn: int
    source_key: str


class OmsCheckpointState(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    schema_version: int
    journal_id: str
    watermark: int
    orders: tuple[OrderCheckpointRow, ...]
    events: tuple[EventCheckpointRow, ...]


def decode_checkpoint(encoded: bytes, *, maximum_bytes: int = 256 * 1024**2) -> OmsCheckpointState:
    if not encoded or len(encoded) > maximum_bytes:
        raise OmsOrderBookError("OMS checkpoint encoded capacity is invalid")
    try:
        state = msgspec.json.decode(encoded, type=OmsCheckpointState)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise OmsOrderBookError("OMS checkpoint schema is invalid") from exc
    if state.schema_version != 1 or not state.journal_id or state.watermark < 1:
        raise OmsOrderBookError("OMS checkpoint scope/watermark is invalid")
    return state


class JournalCheckpointStore:
    """Keep two published recovery generations; a bad newest payload may fall back.

    Fallback is only a verified local checkpoint, not permission to skip missing
    Journal suffixes. Old files are retained until a retention package owns cleanup.
    """

    def __init__(self, root: Path, *, maximum_bytes: int = 256 * 1024**2) -> None:
        self.root = root
        self.maximum_bytes = maximum_bytes
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
        if (
            root.is_symlink()
            or not root.is_dir()
            or (
                os.name == "posix"
                and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
            )
        ):
            raise OmsOrderBookError("OMS checkpoint directory is unsafe")
        self._manifest = root / "checkpoints.json"

    def _entries(self) -> list[dict[str, object]]:
        if not self._manifest.exists():
            if self._manifest.is_symlink():
                raise OmsOrderBookError("checkpoint manifest symlink is invalid")
            return []
        try:
            require_private_runtime_file(self._manifest)
            if self._manifest.stat().st_size > 4096:
                raise ValueError("checkpoint manifest size")
            raw: object = json.loads(self._manifest.read_text())
            if not isinstance(raw, dict):
                raise ValueError("checkpoint manifest root")
            data = cast(dict[str, object], raw)
            rows = data.get("generations")
            if (
                set(data) != {"schema_version", "generations"}
                or data["schema_version"] != 1
                or not isinstance(rows, list)
                or not 1 <= len(cast(list[object], rows)) <= 2
            ):
                raise ValueError("checkpoint manifest fields")
            result: list[dict[str, object]] = []
            for row in cast(list[object], rows):
                if not isinstance(row, dict):
                    raise ValueError("checkpoint generation")
                item = cast(dict[str, object], row)
                name, digest = item.get("file"), item.get("sha256")
                if (
                    set(item) != {"file", "sha256", "watermark", "journal_id"}
                    or not isinstance(name, str)
                    or not re.fullmatch(r"checkpoint-[0-9a-f]{32}\.json", name)
                    or not isinstance(digest, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", digest)
                    or type(item["watermark"]) is not int
                    or item["watermark"] < 1
                    or not isinstance(item["journal_id"], str)
                ):
                    raise ValueError("checkpoint generation fields")
                result.append(item)
            return result
        except (ValueError, OSError) as exc:
            raise OmsOrderBookError("checkpoint manifest cannot be verified") from exc

    def publish(self, encoded: bytes) -> Path:
        state = decode_checkpoint(encoded, maximum_bytes=self.maximum_bytes)
        previous = self._entries()
        if previous and (
            previous[-1]["journal_id"] != state.journal_id
            or cast(int, previous[-1]["watermark"]) >= state.watermark
        ):
            raise OmsOrderBookError("checkpoint publication scope/watermark regressed")
        name = f"checkpoint-{uuid.uuid4().hex}.json"
        path = self.root / name
        descriptor = open_private(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        row: dict[str, object] = {
            "file": name,
            "sha256": sha256(encoded).hexdigest(),
            "watermark": state.watermark,
            "journal_id": state.journal_id,
        }
        atomic_runtime_json(
            self._manifest, {"schema_version": 1, "generations": (previous + [row])[-2:]}
        )
        return path

    def load_latest(self, *, expected_journal_id: str) -> bytes:
        rows = self._entries()
        if not rows or any(row["journal_id"] != expected_journal_id for row in rows):
            raise OmsOrderBookError("no verified checkpoint for the runtime identity")
        for row in reversed(rows):
            path = self.root / cast(str, row["file"])
            try:
                require_private_runtime_file(path)
                if path.stat().st_size > self.maximum_bytes:
                    continue
                encoded = path.read_bytes()
                if sha256(encoded).hexdigest() != row["sha256"]:
                    continue
                state = decode_checkpoint(encoded, maximum_bytes=self.maximum_bytes)
                if state.journal_id == expected_journal_id and state.watermark == row["watermark"]:
                    return encoded
            except (ValueError, OSError):
                continue
        raise OmsOrderBookError("all checkpoint generations are untrusted")
