"""Signed, account-scoped cursors for immutable Consumer snapshot pages."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Mapping, cast

from ..domain._validation import require_nonnegative, require_positive, require_text

_PREFIX = "cts2_"
_VERSION = 1
_MAC_BYTES = 32


class SnapshotCursorErrorV2(ValueError):
    """A page cursor is malformed, unauthenticated, expired, or out of scope."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SnapshotCursorClaimsV2:
    consumer_id: str
    consumer_account_id: str
    authorization_epoch: int
    snapshot_id: str
    journal_id: str
    stream_generation: int
    snapshot_position: str
    next_index: int
    page_size: int
    issued_at_wall_ns: int
    expires_at_wall_ns: int
    key_id: str

    def __post_init__(self) -> None:
        for name in (
            "consumer_id",
            "consumer_account_id",
            "snapshot_id",
            "journal_id",
            "snapshot_position",
            "key_id",
        ):
            require_text(getattr(self, name), f"snapshot cursor {name}", max_length=128)
        require_positive(self.authorization_epoch, "snapshot cursor authorization_epoch")
        require_positive(self.stream_generation, "snapshot cursor stream_generation")
        require_nonnegative(self.next_index, "snapshot cursor next_index")
        require_positive(self.page_size, "snapshot cursor page_size")
        require_nonnegative(self.issued_at_wall_ns, "snapshot cursor issued_at_wall_ns")
        require_positive(self.expires_at_wall_ns, "snapshot cursor expires_at_wall_ns")
        if self.expires_at_wall_ns <= self.issued_at_wall_ns:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor lifetime is invalid")


@dataclass(frozen=True)
class SnapshotCursorSigningKeyV2:
    key_id: str
    secret: bytes
    verify_until_wall_ns: int | None = None

    def __post_init__(self) -> None:
        if self.verify_until_wall_ns is not None:
            require_positive(self.verify_until_wall_ns, "snapshot key verify deadline")
        require_text(self.key_id, "snapshot cursor key_id", max_length=64)
        if len(self.secret) < 32:
            raise SnapshotCursorErrorV2(
                "CURSOR_INVALID", "snapshot cursor key requires at least 256 bits"
            )


class SnapshotCursorCodecV2:
    """Issue and verify canonical HMAC-SHA256 page cursors."""

    def __init__(
        self,
        key: SnapshotCursorSigningKeyV2,
        *,
        maximum_cached_tokens: int = 4_096,
        retired_keys: tuple[SnapshotCursorSigningKeyV2, ...] = (),
    ) -> None:
        require_positive(maximum_cached_tokens, "snapshot cursor cache capacity")
        if (
            key.verify_until_wall_ns is not None
            or len(retired_keys) > 8
            or any(item.verify_until_wall_ns is None for item in retired_keys)
            or len({item.key_id for item in (key, *retired_keys)}) != len(retired_keys) + 1
        ):
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot key ring is invalid")
        self._key = key
        self._keys = {item.key_id: item for item in (key, *retired_keys)}
        self._maximum_cached_tokens = maximum_cached_tokens
        self._verified: OrderedDict[str, SnapshotCursorClaimsV2] = OrderedDict()
        self._cache_lock = Lock()

    @property
    def key_id(self) -> str:
        return self._key.key_id

    def issue(self, claims: SnapshotCursorClaimsV2) -> str:
        if claims.key_id != self._key.key_id:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor key is not current")
        payload = _canonical_json(_claims_mapping(claims))
        signature = hmac.digest(self._key.secret, payload, "sha256")
        token = _PREFIX + _b64(payload + signature)
        self._remember(token, claims)
        return token

    def verify(self, token: object, *, evaluated_wall_ns: int) -> SnapshotCursorClaimsV2:
        require_nonnegative(evaluated_wall_ns, "snapshot cursor evaluation time")
        if not isinstance(token, str) or len(token) > 4_096 or not token.startswith(_PREFIX):
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor prefix is invalid")
        with self._cache_lock:
            cached = self._verified.get(token)
            if cached is not None:
                self._require_verifying_key(cached.key_id, evaluated_wall_ns)
                if evaluated_wall_ns >= cached.expires_at_wall_ns:
                    del self._verified[token]
                    raise SnapshotCursorErrorV2("SNAPSHOT_EXPIRED", "snapshot cursor has expired")
                self._verified.move_to_end(token)
                return cached
        raw = _unb64(token[len(_PREFIX) :])
        if len(raw) <= _MAC_BYTES:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor is truncated")
        if _PREFIX + _b64(raw) != token:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor is not canonical")
        payload, signature = raw[:-_MAC_BYTES], raw[-_MAC_BYTES:]
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor is invalid") from exc
        if not isinstance(decoded, dict):
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor is not canonical")
        decoded_mapping = cast(Mapping[str, object], decoded)
        if _canonical_json(decoded_mapping) != payload:
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor is not canonical")
        claims = _claims_from_mapping(decoded_mapping)
        verifying_key = self._require_verifying_key(claims.key_id, evaluated_wall_ns)
        if not hmac.compare_digest(signature, hmac.digest(verifying_key.secret, payload, "sha256")):
            raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor signature is invalid")
        if evaluated_wall_ns >= claims.expires_at_wall_ns:
            raise SnapshotCursorErrorV2("SNAPSHOT_EXPIRED", "snapshot cursor has expired")
        self._remember(token, claims)
        return claims

    def _require_verifying_key(self, key_id: str, now: int) -> SnapshotCursorSigningKeyV2:
        key = self._keys.get(key_id)
        if key is None or (
            key.verify_until_wall_ns is not None and now >= key.verify_until_wall_ns
        ):
            raise SnapshotCursorErrorV2(
                "CURSOR_INVALID", "snapshot cursor key is unknown or retired"
            )
        return key

    def _remember(self, token: str, claims: SnapshotCursorClaimsV2) -> None:
        with self._cache_lock:
            self._verified[token] = claims
            self._verified.move_to_end(token)
            while len(self._verified) > self._maximum_cached_tokens:
                self._verified.popitem(last=False)


def _claims_mapping(claims: SnapshotCursorClaimsV2) -> Mapping[str, object]:
    return {
        "v": _VERSION,
        "cid": claims.consumer_id,
        "aid": claims.consumer_account_id,
        "epoch": claims.authorization_epoch,
        "sid": claims.snapshot_id,
        "jid": claims.journal_id,
        "gen": claims.stream_generation,
        "pos": claims.snapshot_position,
        "next": claims.next_index,
        "size": claims.page_size,
        "iat": claims.issued_at_wall_ns,
        "exp": claims.expires_at_wall_ns,
        "kid": claims.key_id,
    }


def _claims_from_mapping(value: Mapping[str, object]) -> SnapshotCursorClaimsV2:
    expected = {
        "v",
        "cid",
        "aid",
        "epoch",
        "sid",
        "jid",
        "gen",
        "pos",
        "next",
        "size",
        "iat",
        "exp",
        "kid",
    }
    if set(value) != expected or value.get("v") != _VERSION:
        raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor claims are invalid")
    texts = tuple(value[name] for name in ("cid", "aid", "sid", "jid", "pos", "kid"))
    numbers = tuple(value[name] for name in ("epoch", "gen", "next", "size", "iat", "exp"))
    if any(not isinstance(item, str) for item in texts) or any(
        type(item) is not int for item in numbers
    ):
        raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor claim types are invalid")
    return SnapshotCursorClaimsV2(
        cast(str, texts[0]),
        cast(str, texts[1]),
        cast(int, numbers[0]),
        cast(str, texts[2]),
        cast(str, texts[3]),
        cast(int, numbers[1]),
        cast(str, texts[4]),
        cast(int, numbers[2]),
        cast(int, numbers[3]),
        cast(int, numbers[4]),
        cast(int, numbers[5]),
        cast(str, texts[5]),
    )


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    if not value or "=" in value:
        raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor base64 is invalid")
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, UnicodeError) as exc:
        raise SnapshotCursorErrorV2("CURSOR_INVALID", "snapshot cursor base64 is invalid") from exc


def snapshot_cursor_digest_v2(token: str) -> str:
    """Return a safe evidence identifier without exposing the bearer token."""

    return hashlib.sha256(token.encode("ascii")).hexdigest()
