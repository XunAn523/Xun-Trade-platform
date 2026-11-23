"""Versioned HMAC ConsumerEventCursorV2 tokens with canonical binary positions.

This is a pure key-ring primitive.  Control-root persistence and socket
transport are separate M2-C concerns; no cursor secret is serialized here.
"""

from __future__ import annotations

import base64
import hmac
import struct
from dataclasses import dataclass
from typing import Mapping

from ..domain import ConsumerEventCursorV2, ConsumerEventPositionV2
from ..domain._validation import DomainValidationError, require_nonnegative, require_text

_PREFIX = "ct2_"
_VERSION = 2
_LEGACY_VERSION = 1
_MAC_BYTES = 32
_UINT64 = struct.Struct(">Q")


class CursorTokenError(DomainValidationError):
    """A cursor token cannot be authenticated or decoded canonically."""

    def __init__(self, message: str, *, code: str = "CURSOR_INVALID") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CursorSigningKeyV2:
    """One in-memory HMAC key; material is never included in cursor claims."""

    key_id: str
    secret: bytes
    verify_until_wall_ns: int | None = None

    def __post_init__(self) -> None:
        require_text(self.key_id, "cursor key_id", max_length=64)
        if len(self.secret) < 32:
            raise CursorTokenError("cursor HMAC secret must contain at least 256 bits")
        if self.verify_until_wall_ns is not None:
            require_nonnegative(self.verify_until_wall_ns, "cursor key verify_until_wall_ns")


class CursorKeyRingV2:
    """Issue with exactly one current key and verify current/retired keys."""

    def __init__(
        self,
        *,
        current: CursorSigningKeyV2,
        retired: tuple[CursorSigningKeyV2, ...] = (),
    ) -> None:
        retired = tuple(retired)
        if current.verify_until_wall_ns is not None:
            raise CursorTokenError("current cursor key cannot have a verify deadline")
        all_keys = (current,) + retired
        if len({key.key_id for key in all_keys}) != len(all_keys):
            raise CursorTokenError("cursor key ring repeats a key_id")
        if any(key.verify_until_wall_ns is None for key in retired):
            raise CursorTokenError("retired cursor key requires a verify deadline")
        self._current = current
        self._retired = retired
        self._keys: Mapping[str, CursorSigningKeyV2] = {key.key_id: key for key in all_keys}

    @property
    def current_key(self) -> CursorSigningKeyV2:
        """Return current material only for the protected control-root writer."""

        return self._current

    @property
    def retired_keys(self) -> tuple[CursorSigningKeyV2, ...]:
        """Return retired material only for the protected control-root writer."""

        return self._retired

    def issue(
        self,
        *,
        consumer_id: str,
        consumer_account_id: str,
        journal_id: str,
        stream_generation: int,
        after_position: ConsumerEventPositionV2 | None,
        authorization_epoch: int,
        issued_at_wall_ns: int,
        expires_at_wall_ns: int,
        high_watermark_position: ConsumerEventPositionV2 | None = None,
    ) -> str:
        """Return a canonical signed token using the current key only."""

        claims = ConsumerEventCursorV2(
            consumer_id=consumer_id,
            consumer_account_id=consumer_account_id,
            journal_id=journal_id,
            stream_generation=stream_generation,
            after_position=after_position,
            authorization_epoch=authorization_epoch,
            issued_at_wall_ns=issued_at_wall_ns,
            expires_at_wall_ns=expires_at_wall_ns,
            cursor_key_id=self._current.key_id,
            high_watermark_position=high_watermark_position,
        )
        payload = _encode_claims(claims)
        return _PREFIX + _b64(payload + hmac.digest(self._current.secret, payload, "sha256"))

    def verify(self, token: object, *, evaluated_wall_ns: int) -> ConsumerEventCursorV2:
        """Authenticate a canonical token and enforce token/key expiration."""

        require_nonnegative(evaluated_wall_ns, "cursor evaluation time")
        if not isinstance(token, str) or not token.startswith(_PREFIX):
            raise CursorTokenError("cursor token prefix is invalid")
        encoded = token[len(_PREFIX) :]
        raw = _unb64(encoded)
        if len(raw) <= _MAC_BYTES:
            raise CursorTokenError("cursor token is truncated")
        payload, signature = raw[:-_MAC_BYTES], raw[-_MAC_BYTES:]
        claims = _decode_claims(payload)
        if _PREFIX + _b64(raw) != token:
            raise CursorTokenError("cursor token encoding is not canonical")
        key = self._keys.get(claims.cursor_key_id)
        if key is None or not hmac.compare_digest(
            signature, hmac.digest(key.secret, payload, "sha256")
        ):
            raise CursorTokenError("cursor token signature is invalid")
        if evaluated_wall_ns >= claims.expires_at_wall_ns:
            raise CursorTokenError("cursor token has expired", code="CURSOR_EXPIRED")
        if key.verify_until_wall_ns is not None and evaluated_wall_ns >= key.verify_until_wall_ns:
            raise CursorTokenError("cursor signing key has expired", code="CURSOR_EXPIRED")
        return claims


def _encode_claims(claims: ConsumerEventCursorV2) -> bytes:
    parts = [bytes((_VERSION,))]
    for value in (
        claims.consumer_id,
        claims.consumer_account_id,
        claims.journal_id,
        claims.cursor_key_id,
    ):
        encoded = value.encode("utf-8")
        if len(encoded) > 255:
            raise CursorTokenError("cursor text claim exceeds binary token bound")
        parts.append(bytes((len(encoded),)))
        parts.append(encoded)
    parts.append(_UINT64.pack(claims.stream_generation))
    parts.append(
        b"\x00" if claims.after_position is None else b"\x01" + claims.after_position.to_bytes()
    )
    parts.append(
        b"\x00"
        if claims.high_watermark_position is None
        else b"\x01" + claims.high_watermark_position.to_bytes()
    )
    parts.extend(
        _UINT64.pack(value)
        for value in (
            claims.authorization_epoch,
            claims.issued_at_wall_ns,
            claims.expires_at_wall_ns,
        )
    )
    return b"".join(parts)


def _decode_claims(payload: bytes) -> ConsumerEventCursorV2:
    if not payload or payload[0] not in {_LEGACY_VERSION, _VERSION}:
        raise CursorTokenError("cursor token version is unsupported")
    token_version = payload[0]
    offset = 1
    values: list[str] = []
    for _ in range(4):
        if offset >= len(payload):
            raise CursorTokenError("cursor token text claim is truncated")
        length = payload[offset]
        offset += 1
        end = offset + length
        try:
            values.append(payload[offset:end].decode("utf-8"))
        except UnicodeDecodeError as error:
            raise CursorTokenError("cursor token text claim is not UTF-8") from error
        if end > len(payload):
            raise CursorTokenError("cursor token text claim is truncated")
        offset = end
    if offset + 8 + 1 + 24 > len(payload):
        raise CursorTokenError("cursor token numeric claims are truncated")
    stream_generation = _UINT64.unpack(payload[offset : offset + 8])[0]
    offset += 8
    position_marker = payload[offset]
    offset += 1
    after_position: ConsumerEventPositionV2 | None
    after_position, offset = _decode_position(payload, offset, position_marker)
    high_watermark_position: ConsumerEventPositionV2 | None = None
    if token_version == _VERSION:
        if offset >= len(payload):
            raise CursorTokenError("cursor token high watermark is truncated")
        high_watermark_marker = payload[offset]
        offset += 1
        high_watermark_position, offset = _decode_position(
            payload,
            offset,
            high_watermark_marker,
        )
    if offset + 24 != len(payload):
        raise CursorTokenError("cursor token has trailing or truncated numeric claims")
    authorization_epoch, issued_at, expires_at = struct.unpack(">QQQ", payload[offset:])
    return ConsumerEventCursorV2(
        consumer_id=values[0],
        consumer_account_id=values[1],
        journal_id=values[2],
        stream_generation=stream_generation,
        after_position=after_position,
        authorization_epoch=authorization_epoch,
        issued_at_wall_ns=issued_at,
        expires_at_wall_ns=expires_at,
        cursor_key_id=values[3],
        high_watermark_position=high_watermark_position,
    )


def _decode_position(
    payload: bytes,
    offset: int,
    marker: int,
) -> tuple[ConsumerEventPositionV2 | None, int]:
    if marker == 0:
        return None, offset
    if marker != 1 or offset + 20 > len(payload):
        raise CursorTokenError("cursor token position is truncated or invalid")
    return (
        ConsumerEventPositionV2.from_external_id("ce2_" + _b64(payload[offset : offset + 20])),
        offset + 20,
    )


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    if not value or "=" in value:
        raise CursorTokenError("cursor token base64url is invalid")
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, UnicodeError) as error:
        raise CursorTokenError("cursor token base64url is invalid") from error
