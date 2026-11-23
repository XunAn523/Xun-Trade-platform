"""Atomic, scope-bound persistent identity and cursor keys for a Consumer runtime."""

from __future__ import annotations

import base64
import json
import os
import secrets
import uuid
from dataclasses import replace
from pathlib import Path
from threading import Lock
from typing import cast

from ..persistence.runtime_files import (
    ExclusiveRuntimeLease,
    atomic_runtime_json,
    require_private_runtime_file,
)
from .control_root_v2 import AccountCursorControlRootV2, ConsumerControlRootError
from .cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from .snapshot_cursor_v2 import SnapshotCursorCodecV2, SnapshotCursorSigningKeyV2
from .stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2


class PersistentConsumerRuntimeIdentity:
    """Kernel-exclusive lease and one atomic envelope; never regenerate on corruption."""

    def __init__(
        self,
        *,
        root: Path,
        scope_sha256: str,
        create: bool = False,
        cursor_lifetime_ns: int = 3_600_000_000_000,
    ) -> None:
        if (
            len(scope_sha256) != 64
            or any(c not in "0123456789abcdef" for c in scope_sha256)
            or type(cursor_lifetime_ns) is not int
            or cursor_lifetime_ns <= 0
        ):
            raise ConsumerControlRootError("runtime identity scope/lifetime is invalid")
        self._control = AccountCursorControlRootV2(root)
        self._control.initialize()
        if os.name == "posix" and root.stat().st_uid != os.geteuid():
            raise ConsumerControlRootError("runtime identity root has another owner")
        self._lease = ExclusiveRuntimeLease(root / "runtime-owner.lock")
        self._path = root / "runtime-identity.json"
        self._scope, self._lifetime = scope_sha256, cursor_lifetime_ns
        self._closed = False
        self._failed = False
        self._rotation_lock = Lock()
        self._codecs: tuple[CursorKeyRingV2, SnapshotCursorCodecV2] | None = None
        try:
            if self._path.exists() or self._path.is_symlink():
                self._load()
            else:
                if not create or set(path.name for path in root.iterdir()) - {"runtime-owner.lock"}:
                    raise ConsumerControlRootError(
                        "runtime identity is missing; initialization required"
                    )
                self.journal_id = "consumer-" + uuid.uuid4().hex
                self.event_key = CursorSigningKeyV2(
                    "event-" + uuid.uuid4().hex, secrets.token_bytes(32)
                )
                self.event_retired: tuple[CursorSigningKeyV2, ...] = ()
                self.snapshot_key = SnapshotCursorSigningKeyV2(
                    "snapshot-" + uuid.uuid4().hex, secrets.token_bytes(32)
                )
                self.snapshot_retired: tuple[SnapshotCursorSigningKeyV2, ...] = ()
                self._persist()
            self._codecs = self._build_codecs()
        except BaseException:
            self._lease.close()
            raise

    @property
    def registry(self) -> CursorStreamRegistryV2:
        stream = CursorStreamRefV2(self.journal_id, 1)
        return CursorStreamRegistryV2(stream, (stream,))

    def event_codec(self) -> CursorKeyRingV2:
        self._require_usable()
        if self._codecs is not None:
            return self._codecs[0]
        return CursorKeyRingV2(current=self.event_key, retired=self.event_retired)

    def snapshot_codec(self) -> SnapshotCursorCodecV2:
        self._require_usable()
        if self._codecs is not None:
            return self._codecs[1]
        return SnapshotCursorCodecV2(self.snapshot_key, retired_keys=self.snapshot_retired)

    def _build_codecs(self) -> tuple[CursorKeyRingV2, SnapshotCursorCodecV2]:
        return (
            CursorKeyRingV2(current=self.event_key, retired=self.event_retired),
            SnapshotCursorCodecV2(self.snapshot_key, retired_keys=self.snapshot_retired),
        )

    def _load(self) -> None:
        try:
            require_private_runtime_file(self._path)
            if self._path.stat().st_size > 65_536:
                raise ConsumerControlRootError("runtime identity file is oversized")
            raw: object = json.loads(self._path.read_text(encoding="ascii"))
            if not isinstance(raw, dict):
                raise ValueError("identity root")
            data = cast(dict[str, object], raw)
            if (
                set(data)
                != {
                    "schema_version",
                    "scope_sha256",
                    "cursor_lifetime_ns",
                    "journal_id",
                    "event_keys",
                    "snapshot_keys",
                }
                or type(data["schema_version"]) is not int
                or data["schema_version"] != 1
                or data["scope_sha256"] != self._scope
                or data["cursor_lifetime_ns"] != self._lifetime
                or not isinstance(data["journal_id"], str)
            ):
                raise ValueError("identity scope/schema mismatch")
            self.journal_id = data["journal_id"]
            if (
                not self.journal_id.startswith("consumer-")
                or len(self.journal_id) != 41
                or any(c not in "0123456789abcdef" for c in self.journal_id[9:])
            ):
                raise ValueError("journal identity")
            events, snapshots = _keys(data["event_keys"]), _keys(data["snapshot_keys"])
            self.event_key = CursorSigningKeyV2(*events[0])
            self.event_retired = tuple(CursorSigningKeyV2(*row) for row in events[1:])
            self.snapshot_key = SnapshotCursorSigningKeyV2(*snapshots[0])
            self.snapshot_retired = tuple(SnapshotCursorSigningKeyV2(*row) for row in snapshots[1:])
            self.event_codec()
            self.snapshot_codec()
        except (ValueError, KeyError, TypeError, OSError, UnicodeError) as exc:
            raise ConsumerControlRootError("runtime identity cannot be verified") from exc

    def _persist(self) -> None:
        atomic_runtime_json(
            self._path,
            {
                "schema_version": 1,
                "scope_sha256": self._scope,
                "cursor_lifetime_ns": self._lifetime,
                "journal_id": self.journal_id,
                "event_keys": [
                    _key_payload(k.key_id, k.secret, k.verify_until_wall_ns)
                    for k in (self.event_key, *self.event_retired)
                ],
                "snapshot_keys": [
                    _key_payload(k.key_id, k.secret, k.verify_until_wall_ns)
                    for k in (self.snapshot_key, *self.snapshot_retired)
                ],
            },
        )

    def rotate(self, *, now_wall_ns: int, grace_ns: int) -> None:
        if not self._rotation_lock.acquire(blocking=False):
            raise ConsumerControlRootError("another key rotation is in progress")
        try:
            self._rotate(now_wall_ns=now_wall_ns, grace_ns=grace_ns)
        finally:
            self._rotation_lock.release()

    def _rotate(self, *, now_wall_ns: int, grace_ns: int) -> None:
        self._require_usable()
        if (
            self._closed
            or type(now_wall_ns) is not int
            or now_wall_ns < 0
            or type(grace_ns) is not int
            or grace_ns < self._lifetime
        ):
            raise ConsumerControlRootError("key rotation requires a complete verification grace")
        deadline = now_wall_ns + grace_ns
        events = tuple(
            key
            for key in self.event_retired
            if key.verify_until_wall_ns is not None and key.verify_until_wall_ns > now_wall_ns
        )
        snapshots = tuple(
            key
            for key in self.snapshot_retired
            if key.verify_until_wall_ns is not None and key.verify_until_wall_ns > now_wall_ns
        )
        if len(events) >= 8 or len(snapshots) >= 8:
            raise ConsumerControlRootError("unexpired retired key capacity reached")
        previous = self.event_key, self.event_retired, self.snapshot_key, self.snapshot_retired
        self.event_retired = (*events, replace(self.event_key, verify_until_wall_ns=deadline))
        self.snapshot_retired = (
            *snapshots,
            replace(self.snapshot_key, verify_until_wall_ns=deadline),
        )
        self.event_key = CursorSigningKeyV2("event-" + uuid.uuid4().hex, secrets.token_bytes(32))
        self.snapshot_key = SnapshotCursorSigningKeyV2(
            "snapshot-" + uuid.uuid4().hex, secrets.token_bytes(32)
        )
        try:
            self._persist()
            # Readers see ONE immutable, fully constructed generation, only
            # after durable publication. They do not wait on fsync or observe
            # the temporary current/retired attribute updates above.
            self._codecs = self._build_codecs()
        except BaseException:
            (self.event_key, self.event_retired, self.snapshot_key, self.snapshot_retired) = (
                previous
            )
            self._failed = True
            raise

    def _require_usable(self) -> None:
        if self._closed or self._failed:
            raise ConsumerControlRootError("runtime identity is closed or requires recovery")

    def close(self) -> None:
        self._closed = True
        self._lease.close()


def _key_payload(key_id: str, secret: bytes, deadline: int | None) -> dict[str, object]:
    return {
        "key_id": key_id,
        "secret_b64": base64.b64encode(secret).decode("ascii"),
        "verify_until_wall_ns": deadline,
    }


def _keys(value: object) -> list[tuple[str, bytes, int | None]]:
    if not isinstance(value, list) or not 1 <= len(cast(list[object], value)) <= 9:
        raise ValueError("key ring size")
    result: list[tuple[str, bytes, int | None]] = []
    for raw in cast(list[object], value):
        if not isinstance(raw, dict):
            raise ValueError("key ring entry")
        row = cast(dict[str, object], raw)
        if set(row) != {"key_id", "secret_b64", "verify_until_wall_ns"}:
            raise ValueError("key ring fields")
        key_id, encoded, deadline = row["key_id"], row["secret_b64"], row["verify_until_wall_ns"]
        if (
            not isinstance(key_id, str)
            or not isinstance(encoded, str)
            or (deadline is not None and type(deadline) is not int)
        ):
            raise ValueError("key ring types")
        result.append((key_id, base64.b64decode(encoded, validate=True), deadline))
    return result
