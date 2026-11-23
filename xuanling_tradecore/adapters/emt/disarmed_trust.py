"""Independent trust records for locally serialized EMT DISARMED profiles.

The profile contains its declared public keys so an offline artifact can
detect accidental corruption.  A party that might create a native session
must instead use this separately supplied public-key and revocation material;
otherwise replacing a profile and its embedded key would amount to self-trust.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast

from .disarmed_profile import EmtDisarmedProfile, EmtDisarmedProfileError
from .scope_ownership import EmtEd25519ScopeOwnershipVerifier


@dataclass(frozen=True)
class EmtDisarmedSignerTrust:
    """One independently supplied active signer and its revocation snapshot."""

    signer_id: str
    public_key: bytes
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    revocation_authority: str
    revoked_signer_ids: frozenset[str]

    def __post_init__(self) -> None:
        _text(self.signer_id, "EMT DISARMED signer id")
        if len(self.public_key) != 32:
            raise EmtDisarmedProfileError("EMT DISARMED signer public key must contain 32 bytes")
        if self.valid_from_wall_ns < 0 or self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise EmtDisarmedProfileError("EMT DISARMED signer validity is invalid")
        if self.revocation_authority != self.signer_id:
            raise EmtDisarmedProfileError("EMT DISARMED revocation authority does not match signer")

    def verifier_at(self, *, now_wall_ns: int) -> EmtEd25519ScopeOwnershipVerifier:
        """Return the external verifier only while its signer record is active."""

        if not self.valid_from_wall_ns <= now_wall_ns < self.valid_until_wall_ns:
            raise EmtDisarmedProfileError("EMT DISARMED signer record is not active")
        if self.signer_id in self.revoked_signer_ids:
            raise EmtDisarmedProfileError("EMT DISARMED signer is revoked")
        return EmtEd25519ScopeOwnershipVerifier({self.signer_id: self.public_key})

    def verify_profile(self, profile: EmtDisarmedProfile, *, now_wall_ns: int) -> None:
        """Reject self-signed replacement before a profile can be used further."""

        if profile.trusted_public_keys != {self.signer_id: self.public_key}:
            raise EmtDisarmedProfileError(
                "EMT DISARMED profile declared public keys do not match external trust"
            )
        profile.verify_with(self.verifier_at(now_wall_ns=now_wall_ns))


def load_emt_disarmed_signer_trust(
    *, public_key_path: Path, revocations_path: Path
) -> EmtDisarmedSignerTrust:
    """Load one non-secret public-key record and its local revocation snapshot."""

    public = _read_object(public_key_path, "EMT DISARMED public-key record")
    _require_exact_keys(
        public,
        {
            "schema_version",
            "signer_id",
            "algorithm",
            "public_key_b64",
            "valid_from_wall_ns",
            "valid_until_wall_ns",
        },
        "EMT DISARMED public-key record",
    )
    if public.get("schema_version") != 1 or public.get("algorithm") != "Ed25519":
        raise EmtDisarmedProfileError("EMT DISARMED public-key record is unsupported")
    signer_id = _text(public.get("signer_id"), "EMT DISARMED signer id")
    revocations = _read_object(revocations_path, "EMT DISARMED revocation record")
    _require_exact_keys(
        revocations,
        {"schema_version", "authority", "revoked_signer_ids", "generated_wall_ns"},
        "EMT DISARMED revocation record",
    )
    if revocations.get("schema_version") != 1:
        raise EmtDisarmedProfileError("EMT DISARMED revocation record is unsupported")
    revoked = _text_set(revocations.get("revoked_signer_ids"), "EMT revoked signer ids")
    return EmtDisarmedSignerTrust(
        signer_id=signer_id,
        public_key=_public_key(public.get("public_key_b64")),
        valid_from_wall_ns=_wall_ns(public.get("valid_from_wall_ns"), "EMT signer valid_from"),
        valid_until_wall_ns=_wall_ns(public.get("valid_until_wall_ns"), "EMT signer valid_until"),
        revocation_authority=_text(revocations.get("authority"), "EMT revocation authority"),
        revoked_signer_ids=revoked,
    )


def _read_object(path: Path, field_name: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EmtDisarmedProfileError(f"{field_name} is unreadable") from exc
    if not isinstance(value, dict):
        raise EmtDisarmedProfileError(f"{field_name} must be an object")
    return cast(Mapping[str, object], value)


def _public_key(value: object) -> bytes:
    try:
        public_key = base64.b64decode(_text(value, "EMT signer public key"), validate=True)
    except (ValueError, binascii.Error) as exc:
        raise EmtDisarmedProfileError("EMT signer public key is not base64") from exc
    if len(public_key) != 32:
        raise EmtDisarmedProfileError("EMT signer public key must contain 32 bytes")
    return public_key


def _text_set(value: object, field_name: str) -> frozenset[str]:
    if not isinstance(value, list):
        raise EmtDisarmedProfileError(f"{field_name} must be an array")
    raw_values = cast(list[object], value)
    values = tuple(_text(item, field_name) for item in raw_values)
    if len(set(values)) != len(values):
        raise EmtDisarmedProfileError(f"{field_name} must not repeat a signer")
    return frozenset(values)


def _wall_ns(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise EmtDisarmedProfileError(f"{field_name} must be a non-negative integer")
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EmtDisarmedProfileError(f"{field_name} is required")
    return value


def _require_exact_keys(value: Mapping[str, object], expected: set[str], field_name: str) -> None:
    if set(value) != expected:
        raise EmtDisarmedProfileError(f"{field_name} has unknown or missing fields")
