"""DISARMED-only atomic persistence for account-scoped consumer cursor keys."""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path
from typing import cast

from .cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2, CursorTokenError
from .stream_registry_v2 import (
    CursorStreamRefV2,
    CursorStreamRegistryError,
    CursorStreamRegistryV2,
    CursorStreamResetV2,
)


class ConsumerControlRootError(RuntimeError):
    """Control-root state cannot be safely read or committed."""


class AccountCursorControlRootV2:
    """Persist one cursor key ring below a pre-authorized account control root."""

    _KEY_RING = "cursor-key-ring-v2.json"
    _STREAM_REGISTRY = "cursor-stream-registry-v2.json"

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ConsumerControlRootError("account control root must be an absolute Path")
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def initialize(self) -> None:
        """Create the root with owner-only access or verify an existing root."""

        self._path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._verify_directory(self._path)

    def write_cursor_key_ring(self, ring: CursorKeyRingV2) -> None:
        """Atomically replace protected key material after root verification."""

        self.initialize()
        payload = {
            "schema_version": 1,
            "current": _key_payload(ring.current_key),
            "retired": [_key_payload(key) for key in ring.retired_keys],
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
        self._atomic_replace(self._path / self._KEY_RING, encoded)

    def read_cursor_key_ring(self) -> CursorKeyRingV2:
        """Read a complete owner-only key-ring file without fallback defaults."""

        self.initialize()
        path = self._path / self._KEY_RING
        self._verify_regular_file(path)
        try:
            raw = json.loads(path.read_text(encoding="ascii"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ConsumerControlRootError("cursor key ring is unreadable") from error
        if not isinstance(raw, dict):
            raise ConsumerControlRootError("cursor key ring schema is invalid")
        payload = cast(dict[str, object], raw)
        if set(payload) != {"schema_version", "current", "retired"}:
            raise ConsumerControlRootError("cursor key ring schema is invalid")
        if payload["schema_version"] != 1 or not isinstance(payload["retired"], list):
            raise ConsumerControlRootError("cursor key ring version is invalid")
        try:
            current = _key_from_payload(payload["current"], current=True)
            retired = tuple(
                _key_from_payload(item, current=False)
                for item in cast(list[object], payload["retired"])
            )
            return CursorKeyRingV2(current=current, retired=retired)
        except (CursorTokenError, TypeError, ValueError) as error:
            raise ConsumerControlRootError("cursor key ring contents are invalid") from error

    def write_stream_registry(self, registry: CursorStreamRegistryV2) -> None:
        """Atomically persist the complete, already-validated generation graph."""

        self.initialize()
        payload = {
            "schema_version": 1,
            "current": _stream_payload(registry.current),
            "streams": [_stream_payload(stream) for stream in registry.streams],
            "resets": [
                {
                    "reset_context": reset.reset_context,
                    "source": _stream_payload(reset.source),
                    "target": _stream_payload(reset.target),
                }
                for reset in registry.resets
            ],
        }
        self._atomic_replace(
            self._path / self._STREAM_REGISTRY,
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
                "ascii"
            ),
        )

    def read_stream_registry(self) -> CursorStreamRegistryV2:
        """Read a complete registry; malformed graphs remain unavailable, never expired."""

        self.initialize()
        path = self._path / self._STREAM_REGISTRY
        self._verify_regular_file(path)
        try:
            raw = json.loads(path.read_text(encoding="ascii"))
            if not isinstance(raw, dict):
                raise ValueError("registry root")
            payload = cast(dict[str, object], raw)
            if set(payload) != {"schema_version", "current", "streams", "resets"}:
                raise ValueError("registry fields")
            if (
                payload["schema_version"] != 1
                or not isinstance(payload["streams"], list)
                or not isinstance(payload["resets"], list)
            ):
                raise ValueError("registry version")
            streams = tuple(
                _stream_from_payload(item) for item in cast(list[object], payload["streams"])
            )
            resets = tuple(
                _reset_from_payload(item) for item in cast(list[object], payload["resets"])
            )
            return CursorStreamRegistryV2(
                current=_stream_from_payload(payload["current"]), streams=streams, resets=resets
            )
        except (
            OSError,
            UnicodeError,
            json.JSONDecodeError,
            CursorStreamRegistryError,
            ValueError,
        ) as error:
            raise ConsumerControlRootError(
                "cursor stream registry is invalid or unavailable"
            ) from error

    def _atomic_replace(self, path: Path, encoded: bytes) -> None:
        if path.parent != self._path or path.name not in {self._KEY_RING, self._STREAM_REGISTRY}:
            raise ConsumerControlRootError("control-root write target is invalid")
        temporary = self._path / ("." + path.name + ".tmp")
        if temporary.exists() or temporary.is_symlink():
            raise ConsumerControlRootError("control-root temporary path already exists")
        descriptor = -1
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                descriptor = -1
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            self._fsync_directory()
            self._verify_regular_file(path)
        except OSError as error:
            raise ConsumerControlRootError("atomic cursor key ring write failed") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def _fsync_directory(self) -> None:
        if os.name == "nt":
            return
        descriptor = os.open(self._path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _verify_directory(path: Path) -> None:
        if path.is_symlink() or not path.is_dir():
            raise ConsumerControlRootError("account control root is not a real directory")
        if os.name == "nt":
            return
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise ConsumerControlRootError("account control root permissions are too broad")

    @staticmethod
    def _verify_regular_file(path: Path) -> None:
        if path.is_symlink() or not path.is_file():
            raise ConsumerControlRootError("cursor key ring is not a regular file")
        if os.name == "nt":
            return
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise ConsumerControlRootError("cursor key ring permissions are too broad")


def _key_payload(key: CursorSigningKeyV2) -> dict[str, object]:
    return {
        "key_id": key.key_id,
        "secret_b64": base64.b64encode(key.secret).decode("ascii"),
        "verify_until_wall_ns": key.verify_until_wall_ns,
    }


def _stream_payload(stream: CursorStreamRefV2) -> dict[str, object]:
    return {"journal_id": stream.journal_id, "stream_generation": stream.stream_generation}


def _stream_from_payload(value: object) -> CursorStreamRefV2:
    if not isinstance(value, dict):
        raise ValueError("stream payload")
    payload = cast(dict[str, object], value)
    if set(payload) != {"journal_id", "stream_generation"}:
        raise ValueError("stream payload fields")
    journal_id, generation = payload["journal_id"], payload["stream_generation"]
    if (
        not isinstance(journal_id, str)
        or not isinstance(generation, int)
        or isinstance(generation, bool)
    ):
        raise ValueError("stream payload types")
    return CursorStreamRefV2(journal_id, generation)


def _reset_from_payload(value: object) -> CursorStreamResetV2:
    if not isinstance(value, dict):
        raise ValueError("reset payload")
    payload = cast(dict[str, object], value)
    if set(payload) != {"reset_context", "source", "target"} or not isinstance(
        payload["reset_context"], str
    ):
        raise ValueError("reset payload fields")
    return CursorStreamResetV2(
        payload["reset_context"],
        _stream_from_payload(payload["source"]),
        _stream_from_payload(payload["target"]),
    )


def _key_from_payload(value: object, *, current: bool) -> CursorSigningKeyV2:
    if not isinstance(value, dict):
        raise ValueError("cursor key payload fields are invalid")
    payload = cast(dict[str, object], value)
    if set(payload) != {
        "key_id",
        "secret_b64",
        "verify_until_wall_ns",
    }:
        raise ValueError("cursor key payload fields are invalid")
    key_id = payload["key_id"]
    secret_b64 = payload["secret_b64"]
    deadline = payload["verify_until_wall_ns"]
    if not isinstance(key_id, str) or not isinstance(secret_b64, str):
        raise ValueError("cursor key payload types are invalid")
    if current and deadline is not None:
        raise ValueError("current cursor key has a verify deadline")
    if not current and (not isinstance(deadline, int) or isinstance(deadline, bool)):
        raise ValueError("retired cursor key has no verify deadline")
    try:
        secret = base64.b64decode(secret_b64.encode("ascii"), validate=True)
    except (ValueError, UnicodeError) as error:
        raise ValueError("cursor key secret is not base64") from error
    return CursorSigningKeyV2(
        key_id=key_id,
        secret=secret,
        verify_until_wall_ns=cast(int | None, deadline),
    )
