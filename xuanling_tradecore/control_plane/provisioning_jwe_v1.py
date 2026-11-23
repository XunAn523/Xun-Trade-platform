"""Strict compact-JWE credential envelope support for Provisioning V1.

Only RFC 7516 compact JWE with RSA-OAEP-256 and A256GCM is accepted.  The
module has no transport or persistence dependency and never includes received
ciphertext/plaintext in an exception message.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from os import urandom
from typing import Mapping, cast

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..domain._validation import require_positive, require_text
from .provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ControlPrincipalV1,
    CredentialClaimsV1,
    ProvisioningErrorV1,
)

_HEADER = {
    "alg": "RSA-OAEP-256",
    "enc": "A256GCM",
    "typ": "tradecore-broker-credential+jwe",
    "cty": "application/json",
}
_SCHEMA = "TradeCoreBrokerCredentialV1"
_RFC3339_UTC = re.compile(
    r"(?P<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.(?P<fraction>[0-9]{1,9}))?Z\Z"
)


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    if not value or "=" in value:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "JWE encoding is invalid")
    try:
        padding_width = -len(value) % 4
        result = base64.b64decode(
            value + "=" * padding_width,
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError, UnicodeEncodeError) as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "JWE encoding is invalid") from exc
    if _b64url_encode(result) != value:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "JWE encoding is not canonical")
    return result


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def parse_rfc3339_wall_ns_v1(value: object) -> int:
    if not isinstance(value, str):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential time is invalid")
    match = _RFC3339_UTC.fullmatch(value)
    if match is None:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential time is invalid")
    try:
        parsed = datetime.fromisoformat(match.group("whole") + "+00:00")
    except ValueError as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential time is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential time must be UTC")
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return int(parsed.timestamp()) * 1_000_000_000 + int(fraction or "0")


def format_rfc3339_wall_ns_v1(value: int) -> str:
    require_positive(value, "credential time")
    seconds, nanos = divmod(value, 1_000_000_000)
    rendered = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{rendered}.{nanos:09d}Z"


def _object(value: object, *, label: str, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", f"credential {label} is invalid")
    untyped = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in untyped):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", f"credential {label} is invalid")
    result = cast(dict[str, object], untyped)
    if set(result) != set(fields):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", f"credential {label} is invalid")
    return result


@dataclass(frozen=True)
class CredentialRecipientV1:
    """Public recipient material returned by the controlled recipient endpoint."""

    target: str
    key_id: str
    public_key: rsa.RSAPublicKey = field(repr=False)
    not_after_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.target, "credential recipient target", max_length=256)
        require_text(self.key_id, "credential recipient key ID", max_length=256)
        require_positive(self.not_after_wall_ns, "credential recipient expiry")
        if self.public_key.key_size < 2_048:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "recipient key is too small")


@dataclass(frozen=True)
class VerifiedCredentialEnvelopeV1:
    """Validated JWE result; material is redacted from representation."""

    claims: CredentialClaimsV1
    material: BrokerCredentialMaterialV1 = field(repr=False)
    envelope_digest: str
    profile_not_after_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.envelope_digest, "credential envelope digest", max_length=64)
        if len(self.envelope_digest) != 64:
            raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential envelope digest is invalid")
        require_positive(self.profile_not_after_wall_ns, "credential profile expiry")


def encrypt_credential_envelope_v1(
    *,
    recipient: CredentialRecipientV1,
    claims: CredentialClaimsV1,
    identity: AdapterProfileIdentityV1,
    material: BrokerCredentialMaterialV1,
    profile_not_after_wall_ns: int,
) -> str:
    """Construct a canonical compact JWE for an authorized provisioning client."""

    if claims.audience != recipient.target or claims.recipient_key_id != recipient.key_id:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential claims do not match recipient")
    if profile_not_after_wall_ns < claims.expires_at_wall_ns:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "profile expiry precedes envelope expiry")
    header = dict(_HEADER)
    header["kid"] = recipient.key_id
    protected = _b64url_encode(_canonical_json(header))
    payload: Mapping[str, object] = {
        "credential_schema": _SCHEMA,
        "jti": claims.jti,
        "iss": claims.issuer,
        "aud": claims.audience,
        "iat": format_rfc3339_wall_ns_v1(claims.issued_at_wall_ns),
        "nbf": format_rfc3339_wall_ns_v1(claims.not_before_wall_ns),
        "exp": format_rfc3339_wall_ns_v1(claims.expires_at_wall_ns),
        "profile_id": identity.profile_id,
        "adapter_kind": identity.adapter_kind.value,
        "adapter_instance_id": identity.adapter_instance_id,
        "credential": {
            "broker_account": material.broker_account,
            "password": material.password,
            "client_id": material.client_id,
            "password_type": material.password_type,
        },
        "connection": {
            "kind": material.connection.kind,
            "host": material.connection.host,
            "port": material.connection.port,
            "server_name": material.connection.server_name,
        },
        "not_after": format_rfc3339_wall_ns_v1(profile_not_after_wall_ns),
    }
    cek, iv = urandom(32), urandom(12)
    encrypted_key = recipient.public_key.encrypt(
        cek,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    encrypted = AESGCM(cek).encrypt(iv, _canonical_json(payload), protected.encode("ascii"))
    ciphertext, tag = encrypted[:-16], encrypted[-16:]
    return ".".join(
        (
            _b64url_encode(_canonical_json(header)),
            _b64url_encode(encrypted_key),
            _b64url_encode(iv),
            _b64url_encode(ciphertext),
            _b64url_encode(tag),
        )
    )


def decrypt_credential_envelope_v1(
    *,
    envelope: str,
    private_key: rsa.RSAPrivateKey,
    expected_recipient: CredentialRecipientV1,
    expected_principal: ControlPrincipalV1,
    expected_identity: AdapterProfileIdentityV1,
    expected_profile_not_after_wall_ns: int,
    now_wall_ns: int,
) -> VerifiedCredentialEnvelopeV1:
    """Decrypt and strictly bind a JWE to its mTLS peer and outer request."""

    if private_key.key_size < 2_048:
        raise ProvisioningErrorV1("CONTROL_RECIPIENT", "recipient private key is invalid")
    if len(envelope) > 60_000:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential envelope is invalid")
    parts = envelope.split(".")
    if len(parts) != 5:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential envelope is invalid")
    protected_raw, encrypted_key_raw, iv_raw, ciphertext_raw, tag_raw = (
        _b64url_decode(part) for part in parts
    )
    try:
        header_object = _object(
            json.loads(protected_raw.decode("ascii")),
            label="header",
            fields=frozenset({"alg", "enc", "typ", "cty", "kid"}),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential header is invalid") from exc
    if (
        any(header_object.get(key) != value for key, value in _HEADER.items())
        or header_object.get("kid") != expected_recipient.key_id
    ):
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential header is not accepted")
    if len(iv_raw) != 12 or len(tag_raw) != 16:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential cipher shape is invalid")
    try:
        cek = private_key.decrypt(
            encrypted_key_raw,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
        plaintext = AESGCM(cek).decrypt(
            iv_raw,
            ciphertext_raw + tag_raw,
            parts[0].encode("ascii"),
        )
    except Exception as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential encryption is invalid") from exc
    try:
        raw = _object(
            json.loads(plaintext.decode("utf-8")),
            label="payload",
            fields=frozenset(
                {
                    "credential_schema",
                    "jti",
                    "iss",
                    "aud",
                    "iat",
                    "nbf",
                    "exp",
                    "profile_id",
                    "adapter_kind",
                    "adapter_instance_id",
                    "credential",
                    "connection",
                    "not_after",
                }
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential payload is invalid") from exc
    if raw["credential_schema"] != _SCHEMA:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential payload is invalid")
    try:
        adapter_kind = AdapterKindV1(raw["adapter_kind"])
    except (TypeError, ValueError) as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential adapter kind is invalid") from exc
    for value, expected, label in (
        (raw["profile_id"], expected_identity.profile_id, "profile"),
        (raw["adapter_instance_id"], expected_identity.adapter_instance_id, "instance"),
        (adapter_kind, expected_identity.adapter_kind, "adapter"),
        (raw["iss"], expected_principal.principal_id, "issuer"),
        (raw["aud"], expected_recipient.target, "audience"),
    ):
        if value != expected:
            raise ProvisioningErrorV1("CONTROL_ENVELOPE", f"credential {label} does not match")
    claims = CredentialClaimsV1(
        jti=cast(str, raw["jti"]),
        issuer=cast(str, raw["iss"]),
        audience=cast(str, raw["aud"]),
        issued_at_wall_ns=parse_rfc3339_wall_ns_v1(raw["iat"]),
        not_before_wall_ns=parse_rfc3339_wall_ns_v1(raw["nbf"]),
        expires_at_wall_ns=parse_rfc3339_wall_ns_v1(raw["exp"]),
        recipient_key_id=expected_recipient.key_id,
    )
    if not claims.not_before_wall_ns <= now_wall_ns < claims.expires_at_wall_ns:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential envelope is not current")
    profile_not_after_wall_ns = parse_rfc3339_wall_ns_v1(raw["not_after"])
    if profile_not_after_wall_ns != expected_profile_not_after_wall_ns:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "profile expiry does not match")
    credential = _object(
        raw["credential"],
        label="credential",
        fields=frozenset({"broker_account", "password", "client_id", "password_type"}),
    )
    connection = _object(
        raw["connection"],
        label="connection",
        fields=frozenset({"kind", "host", "port", "server_name"}),
    )
    try:
        material = BrokerCredentialMaterialV1(
            broker_account=cast(str, credential["broker_account"]),
            password=cast(str, credential["password"]),
            client_id=cast(str | None, credential["client_id"]),
            password_type=cast(str, credential["password_type"]),
            connection=BrokerConnectionV1(
                kind=cast(str, connection["kind"]),
                host=cast(str, connection["host"]),
                port=cast(int, connection["port"]),
                server_name=cast(str | None, connection["server_name"]),
            ),
        )
    except (TypeError, ProvisioningErrorV1) as exc:
        raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential payload is invalid") from exc
    return VerifiedCredentialEnvelopeV1(
        claims=claims,
        material=material,
        envelope_digest=sha256(envelope.encode("ascii")).hexdigest(),
        profile_not_after_wall_ns=profile_not_after_wall_ns,
    )
