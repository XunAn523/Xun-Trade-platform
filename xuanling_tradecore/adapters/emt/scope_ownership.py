"""Signed EMT client-scope ownership claims for controlled profile binding.

The signature binds the exact ownership namespace plus the certificate and
revocation references used by the control plane.  This module deliberately
does not fetch a revocation endpoint or load a private key: the caller supplies
trusted public keys and a release process supplies the signed claim.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import asdict, dataclass
from typing import Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ...domain import AdapterClientScopeOwnership


class EmtScopeOwnershipSignatureError(RuntimeError):
    """A signed EMT client-scope ownership claim is invalid or untrusted."""


@dataclass(frozen=True)
class EmtSignedScopeOwnership:
    """A detached Ed25519 signature over one exact client-scope claim."""

    ownership: AdapterClientScopeOwnership
    certificate_reference: str
    revocation_reference: str
    signature_b64: str

    def __post_init__(self) -> None:
        _require_text(self.certificate_reference, "EMT certificate reference")
        _require_text(self.revocation_reference, "EMT revocation reference")
        _require_text(self.signature_b64, "EMT scope ownership signature")

    def canonical_payload(self) -> bytes:
        """Return stable signed bytes without credentials or native objects."""

        return canonical_scope_ownership_payload(
            self.ownership,
            certificate_reference=self.certificate_reference,
            revocation_reference=self.revocation_reference,
        )

    def signature_bytes(self) -> bytes:
        try:
            signature = base64.b64decode(self.signature_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EmtScopeOwnershipSignatureError(
                "EMT scope ownership signature is not base64"
            ) from exc
        if len(signature) != 64:
            raise EmtScopeOwnershipSignatureError(
                "EMT scope ownership signature is not an Ed25519 signature"
            )
        return signature


def canonical_scope_ownership_payload(
    ownership: AdapterClientScopeOwnership,
    *,
    certificate_reference: str,
    revocation_reference: str,
) -> bytes:
    """Serialize the exact signed claim deterministically."""

    _require_text(certificate_reference, "EMT certificate reference")
    _require_text(revocation_reference, "EMT revocation reference")
    payload = {
        "schema_version": 1,
        "algorithm": "Ed25519",
        "certificate_reference": certificate_reference,
        "revocation_reference": revocation_reference,
        "ownership": asdict(ownership),
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


class EmtEd25519ScopeOwnershipVerifier:
    """Verify claims against a control-plane supplied signer-id/key set."""

    def __init__(self, trusted_public_keys: Mapping[str, bytes]) -> None:
        if not trusted_public_keys:
            raise EmtScopeOwnershipSignatureError("at least one EMT scope signer is required")
        keys: dict[str, Ed25519PublicKey] = {}
        for signer_id, raw_key in trusted_public_keys.items():
            _require_text(signer_id, "EMT scope signer id")
            if len(raw_key) != 32:
                raise EmtScopeOwnershipSignatureError(
                    "EMT scope signer public key must be exactly 32 bytes"
                )
            try:
                keys[signer_id] = Ed25519PublicKey.from_public_bytes(raw_key)
            except ValueError as exc:
                raise EmtScopeOwnershipSignatureError(
                    "EMT scope signer public key is invalid"
                ) from exc
        self._keys = keys

    def verify(self, signed: EmtSignedScopeOwnership) -> AdapterClientScopeOwnership:
        """Return the verified claim or fail closed without remote I/O."""

        signer_id = signed.ownership.signer_id
        try:
            public_key = self._keys[signer_id]
        except KeyError as exc:
            raise EmtScopeOwnershipSignatureError(
                "EMT scope ownership signer is not trusted"
            ) from exc
        try:
            public_key.verify(signed.signature_bytes(), signed.canonical_payload())
        except InvalidSignature as exc:
            raise EmtScopeOwnershipSignatureError(
                "EMT scope ownership signature does not verify"
            ) from exc
        return signed.ownership


def _require_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EmtScopeOwnershipSignatureError(f"{field_name} is required")
