"""Signed, always-DISARMED EMT scope profile serialization.

This profile has no credentials and cannot express a READY transition.  It
binds exact signed scope claims to their trusted public keys before any native
EMT session factory is considered.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence, cast

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ...domain import AdapterClientScopeOwnership
from .scope_ownership import (
    EmtEd25519ScopeOwnershipVerifier,
    EmtScopeOwnershipSignatureError,
    EmtSignedScopeOwnership,
    canonical_scope_ownership_payload,
)


class EmtDisarmedProfileError(ValueError):
    """A local EMT profile is malformed, untrusted, or not DISARMED."""


@dataclass(frozen=True)
class EmtDisarmedProfile:
    """Verified signed ownership claims for a profile that remains DISARMED."""

    profile_id: str
    signed_scope_ownerships: tuple[EmtSignedScopeOwnership, ...]
    trusted_public_keys: Mapping[str, bytes]
    schema_version: int = 1
    approved: bool = False
    lifecycle: str = "DISARMED"

    def __post_init__(self) -> None:
        _require_text(self.profile_id, "EMT DISARMED profile_id")
        if self.schema_version != 1:
            raise EmtDisarmedProfileError("EMT DISARMED profile schema_version must be 1")
        if self.approved is not False or self.lifecycle != "DISARMED":
            raise EmtDisarmedProfileError("EMT profile builder permits DISARMED only")
        if not self.signed_scope_ownerships:
            raise EmtDisarmedProfileError("EMT DISARMED profile requires signed scope ownerships")
        try:
            verifier = EmtEd25519ScopeOwnershipVerifier(self.trusted_public_keys)
        except EmtScopeOwnershipSignatureError as exc:
            raise EmtDisarmedProfileError("EMT DISARMED profile has invalid trusted keys") from exc
        ownership_keys: set[tuple[str, str, str, str]] = set()
        for signed in self.signed_scope_ownerships:
            try:
                ownership = verifier.verify(signed)
            except EmtScopeOwnershipSignatureError as exc:
                raise EmtDisarmedProfileError(
                    "EMT DISARMED profile contains an invalid signed scope ownership"
                ) from exc
            key = (
                ownership.venue_id,
                ownership.account_id,
                ownership.adapter_instance_id,
                ownership.adapter_client_scope,
            )
            if key in ownership_keys:
                raise EmtDisarmedProfileError(
                    "EMT DISARMED profile repeats an account/client-scope ownership"
                )
            ownership_keys.add(key)

    def as_dict(self) -> dict[str, object]:
        """Return a non-secret, canonical JSON-ready DISARMED profile.

        ``trusted_public_keys_b64`` is a signed-profile declaration that makes
        an offline file self-verifying.  It is *not* a runtime trust anchor:
        callers that could open a controlled session must verify the claim
        again with independently supplied control-plane keys.
        """

        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "approval": {"approved": False, "lifecycle": "DISARMED"},
            "trusted_public_keys_b64": {
                signer_id: base64.b64encode(public_key).decode("ascii")
                for signer_id, public_key in sorted(self.trusted_public_keys.items())
            },
            "signed_scope_ownerships": [
                {
                    "ownership": asdict(signed.ownership),
                    "certificate_reference": signed.certificate_reference,
                    "revocation_reference": signed.revocation_reference,
                    "signature_b64": signed.signature_b64,
                }
                for signed in self.signed_scope_ownerships
            ],
        }

    def verify_with(self, verifier: EmtEd25519ScopeOwnershipVerifier) -> None:
        """Verify every claim against independently supplied trusted keys.

        A profile's embedded public keys make accidental corruption visible,
        but cannot establish who is trusted.  This explicit second check is
        therefore required on every path that could progress beyond local
        DISARMED inspection.
        """

        for signed in self.signed_scope_ownerships:
            verifier.verify(signed)


def sign_disarmed_profile(
    *,
    profile_id: str,
    ownerships: Sequence[AdapterClientScopeOwnership],
    private_keys: Mapping[str, Ed25519PrivateKey],
    certificate_references: Mapping[str, str],
    revocation_references: Mapping[str, str],
) -> EmtDisarmedProfile:
    """Create an always-DISARMED profile without serializing private keys."""

    signed_scope_ownerships: list[EmtSignedScopeOwnership] = []
    trusted_public_keys: dict[str, bytes] = {}
    for ownership in ownerships:
        try:
            private_key = private_keys[ownership.signer_id]
            certificate_reference = certificate_references[ownership.signer_id]
            revocation_reference = revocation_references[ownership.signer_id]
        except KeyError as exc:
            raise EmtDisarmedProfileError(
                f"EMT DISARMED profile lacks signing material for {ownership.signer_id}"
            ) from exc
        signature = private_key.sign(
            canonical_scope_ownership_payload(
                ownership,
                certificate_reference=certificate_reference,
                revocation_reference=revocation_reference,
            )
        )
        signed_scope_ownerships.append(
            EmtSignedScopeOwnership(
                ownership=ownership,
                certificate_reference=certificate_reference,
                revocation_reference=revocation_reference,
                signature_b64=base64.b64encode(signature).decode("ascii"),
            )
        )
        trusted_public_keys[ownership.signer_id] = private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
    return EmtDisarmedProfile(
        profile_id=profile_id,
        signed_scope_ownerships=tuple(signed_scope_ownerships),
        trusted_public_keys=trusted_public_keys,
    )


def load_emt_disarmed_profile(path: Path) -> EmtDisarmedProfile:
    """Load and cryptographically verify a local non-secret DISARMED profile."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EmtDisarmedProfileError("EMT DISARMED profile is unreadable") from exc
    root = _mapping(raw, "EMT DISARMED profile")
    _require_exact_keys(
        root,
        {
            "schema_version",
            "profile_id",
            "approval",
            "trusted_public_keys_b64",
            "signed_scope_ownerships",
        },
        "EMT DISARMED profile",
    )
    approval = _mapping(root.get("approval"), "EMT DISARMED profile approval")
    _require_exact_keys(
        approval,
        {"approved", "lifecycle"},
        "EMT DISARMED profile approval",
    )
    if approval.get("approved") is not False or approval.get("lifecycle") != "DISARMED":
        raise EmtDisarmedProfileError("EMT profile builder permits DISARMED only")
    raw_keys = _mapping(root.get("trusted_public_keys_b64"), "EMT trusted public keys")
    trusted_public_keys = {
        _text(signer_id, "EMT trusted signer id"): _public_key_from_b64(value)
        for signer_id, value in raw_keys.items()
    }
    raw_signed = _sequence(root.get("signed_scope_ownerships"), "EMT signed scope ownerships")
    signed_scope_ownerships = tuple(_signed_scope_ownership(value) for value in raw_signed)
    schema_version = root.get("schema_version")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise EmtDisarmedProfileError("EMT DISARMED profile schema_version must be an integer")
    return EmtDisarmedProfile(
        profile_id=_text(root.get("profile_id"), "EMT DISARMED profile_id"),
        signed_scope_ownerships=signed_scope_ownerships,
        trusted_public_keys=trusted_public_keys,
        schema_version=schema_version,
    )


def _signed_scope_ownership(value: object) -> EmtSignedScopeOwnership:
    raw = _mapping(value, "EMT signed scope ownership")
    _require_exact_keys(
        raw,
        {"ownership", "certificate_reference", "revocation_reference", "signature_b64"},
        "EMT signed scope ownership",
    )
    return EmtSignedScopeOwnership(
        ownership=adapter_client_scope_ownership_from_dict(
            _mapping(raw.get("ownership"), "EMT scope ownership")
        ),
        certificate_reference=_text(raw.get("certificate_reference"), "EMT certificate reference"),
        revocation_reference=_text(raw.get("revocation_reference"), "EMT revocation reference"),
        signature_b64=_text(raw.get("signature_b64"), "EMT scope ownership signature"),
    )


def adapter_client_scope_ownership_from_dict(
    raw: Mapping[str, object],
) -> AdapterClientScopeOwnership:
    """Parse one ownership object without loading a profile or private key."""

    _require_exact_keys(
        raw,
        {
            "ownership_id",
            "version",
            "venue_id",
            "account_id",
            "adapter_instance_id",
            "adapter_client_scope",
            "owner_instance_id",
            "config_source_hash",
            "signer_id",
            "valid_from_wall_ns",
            "valid_until_wall_ns",
            "uniqueness_probe_hash",
            "uniqueness_probe_succeeded",
            "revoked_at_wall_ns",
        },
        "EMT scope ownership",
    )
    revoked_at_wall_ns = raw.get("revoked_at_wall_ns")
    if revoked_at_wall_ns is not None and (
        not isinstance(revoked_at_wall_ns, int) or isinstance(revoked_at_wall_ns, bool)
    ):
        raise EmtDisarmedProfileError("EMT scope ownership revoked_at_wall_ns must be an integer")
    return AdapterClientScopeOwnership(
        ownership_id=_text(raw.get("ownership_id"), "EMT scope ownership_id"),
        version=_integer(raw.get("version"), "EMT scope ownership version"),
        venue_id=_text(raw.get("venue_id"), "EMT scope ownership venue_id"),
        account_id=_text(raw.get("account_id"), "EMT scope ownership account_id"),
        adapter_instance_id=_text(
            raw.get("adapter_instance_id"), "EMT scope ownership adapter_instance_id"
        ),
        adapter_client_scope=_text(
            raw.get("adapter_client_scope"), "EMT scope ownership adapter_client_scope"
        ),
        owner_instance_id=_text(
            raw.get("owner_instance_id"), "EMT scope ownership owner_instance_id"
        ),
        config_source_hash=_text(
            raw.get("config_source_hash"), "EMT scope ownership config_source_hash"
        ),
        signer_id=_text(raw.get("signer_id"), "EMT scope ownership signer_id"),
        valid_from_wall_ns=_integer(
            raw.get("valid_from_wall_ns"), "EMT scope ownership valid_from_wall_ns"
        ),
        valid_until_wall_ns=_integer(
            raw.get("valid_until_wall_ns"), "EMT scope ownership valid_until_wall_ns"
        ),
        uniqueness_probe_hash=_text(
            raw.get("uniqueness_probe_hash"), "EMT scope ownership uniqueness_probe_hash"
        ),
        uniqueness_probe_succeeded=_boolean(
            raw.get("uniqueness_probe_succeeded"), "EMT scope ownership uniqueness_probe_succeeded"
        ),
        revoked_at_wall_ns=revoked_at_wall_ns,
    )


def _public_key_from_b64(value: object) -> bytes:
    encoded = _text(value, "EMT trusted public key")
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise EmtDisarmedProfileError("EMT trusted public key is not base64") from exc
    if len(decoded) != 32:
        raise EmtDisarmedProfileError("EMT trusted public key must contain 32 bytes")
    return decoded


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise EmtDisarmedProfileError(f"{field_name} must be an object")
    return cast(Mapping[str, object], value)


def _require_exact_keys(value: Mapping[str, object], expected: set[str], field_name: str) -> None:
    actual = set(value)
    if actual != expected:
        raise EmtDisarmedProfileError(f"{field_name} has unknown or missing fields")


def _sequence(value: object, field_name: str) -> Sequence[object]:
    if not isinstance(value, list) or not value:
        raise EmtDisarmedProfileError(f"{field_name} must be a non-empty array")
    return cast(Sequence[object], value)


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EmtDisarmedProfileError(f"{field_name} is required")
    return value


def _integer(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise EmtDisarmedProfileError(f"{field_name} must be an integer")
    return value


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise EmtDisarmedProfileError(f"{field_name} must be boolean")
    return value


def _require_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EmtDisarmedProfileError(f"{field_name} is required")
