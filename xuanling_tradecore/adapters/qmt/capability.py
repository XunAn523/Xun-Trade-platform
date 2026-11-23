"""Signed QMT capability evidence and fail-closed activation gates.

This module never opens a QMT session.  It turns the P0 probe result into a
small, signed, scope-bound statement that the production adapter can verify
before it is even allowed to request mutation at Windows Edge.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from hashlib import sha256
from typing import AbstractSet, Mapping, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ...domain._validation import require_nonnegative, require_text
from ..mapping import ActivatedVenueMapping


class QmtCapabilityError(RuntimeError):
    """A QMT capability manifest or activation is invalid."""


_REQUIRED_CAPABILITIES = frozenset(
    {
        "embedded_python_36",
        "run_time",
        "quick_trade_2",
        "single_native_caller",
        "native_mapping_interlocked",
        "route_identity_echo",
        "order_callback",
        "query_complete",
    }
)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise QmtCapabilityError(f"QMT capability {name} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class QmtCapabilityManifestV1:
    """The signed, payload-free result of one reviewed QMT P0 probe."""

    manifest_id: str
    signer_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    mapping_manifest_hash: str
    edge_instance_digest: str
    qmt_build_digest: str
    embedded_python_version: str
    native_abi_digest: str
    route_binding_digest: str
    evidence_digest: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int | None
    capabilities: frozenset[str]
    signature: str

    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise QmtCapabilityError("QMT capability schema version is unsupported")
        for name in (
            "manifest_id",
            "signer_id",
            "adapter_instance_id",
            "adapter_client_scope",
            "embedded_python_version",
            "signature",
        ):
            require_text(getattr(self, name), f"QMT capability {name}", max_length=256)
        for name in (
            "mapping_manifest_hash",
            "edge_instance_digest",
            "qmt_build_digest",
            "native_abi_digest",
            "route_binding_digest",
            "evidence_digest",
        ):
            _sha256(getattr(self, name), name)
        require_nonnegative(self.valid_from_wall_ns, "QMT capability valid_from_wall_ns")
        if self.valid_until_wall_ns is not None:
            require_nonnegative(self.valid_until_wall_ns, "QMT capability valid_until_wall_ns")
            if self.valid_until_wall_ns <= self.valid_from_wall_ns:
                raise QmtCapabilityError("QMT capability validity interval is invalid")
        normalized = frozenset(self.capabilities)
        if not normalized or not all(value for value in normalized):
            raise QmtCapabilityError("QMT capability set is invalid")
        object.__setattr__(self, "capabilities", normalized)

    @property
    def manifest_digest(self) -> str:
        return sha256(self.canonical_payload()).hexdigest()

    def canonical_payload(self) -> bytes:
        """Return the exact detached-signature payload."""

        payload = {
            "adapter_client_scope": self.adapter_client_scope,
            "adapter_instance_id": self.adapter_instance_id,
            "capabilities": sorted(self.capabilities),
            "edge_instance_digest": self.edge_instance_digest,
            "embedded_python_version": self.embedded_python_version,
            "evidence_digest": self.evidence_digest,
            "manifest_id": self.manifest_id,
            "mapping_manifest_hash": self.mapping_manifest_hash,
            "native_abi_digest": self.native_abi_digest,
            "qmt_build_digest": self.qmt_build_digest,
            "route_binding_digest": self.route_binding_digest,
            "schema_version": self.schema_version,
            "signer_id": self.signer_id,
            "valid_from_wall_ns": self.valid_from_wall_ns,
            "valid_until_wall_ns": self.valid_until_wall_ns,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def is_valid_at(self, now_wall_ns: int) -> bool:
        require_nonnegative(now_wall_ns, "QMT capability activation time")
        return now_wall_ns >= self.valid_from_wall_ns and (
            self.valid_until_wall_ns is None or now_wall_ns < self.valid_until_wall_ns
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> "QmtCapabilityManifestV1":
        try:
            decoded_object = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise QmtCapabilityError("QMT capability manifest is not UTF-8 JSON") from error
        if not isinstance(decoded_object, dict):
            raise QmtCapabilityError("QMT capability manifest is not an object")
        decoded_fields = cast(dict[object, object], decoded_object)
        if not all(isinstance(key, str) for key in decoded_fields):
            raise QmtCapabilityError("QMT capability manifest has non-text field names")
        decoded = cast(dict[str, object], decoded_fields)
        expected = {
            "adapter_client_scope",
            "adapter_instance_id",
            "capabilities",
            "edge_instance_digest",
            "embedded_python_version",
            "evidence_digest",
            "manifest_id",
            "mapping_manifest_hash",
            "native_abi_digest",
            "qmt_build_digest",
            "route_binding_digest",
            "schema_version",
            "signature",
            "signer_id",
            "valid_from_wall_ns",
            "valid_until_wall_ns",
        }
        if set(decoded) != expected:
            raise QmtCapabilityError("QMT capability manifest fields differ from V1")
        capabilities_object = decoded["capabilities"]
        if not isinstance(capabilities_object, list):
            raise QmtCapabilityError("QMT capability manifest capabilities must be a text array")
        capability_values = cast(list[object], capabilities_object)
        if not all(isinstance(item, str) for item in capability_values):
            raise QmtCapabilityError("QMT capability manifest capabilities must be a text array")
        capabilities = cast(list[str], capability_values)
        optional_until_object = decoded["valid_until_wall_ns"]
        optional_until: int | None
        if optional_until_object is None:
            optional_until = None
        elif isinstance(optional_until_object, int):
            optional_until = optional_until_object
        else:
            raise QmtCapabilityError(
                "QMT capability valid_until_wall_ns must be an integer or null"
            )
        return cls(
            manifest_id=_text_field(decoded, "manifest_id"),
            signer_id=_text_field(decoded, "signer_id"),
            adapter_instance_id=_text_field(decoded, "adapter_instance_id"),
            adapter_client_scope=_text_field(decoded, "adapter_client_scope"),
            mapping_manifest_hash=_text_field(decoded, "mapping_manifest_hash"),
            edge_instance_digest=_text_field(decoded, "edge_instance_digest"),
            qmt_build_digest=_text_field(decoded, "qmt_build_digest"),
            embedded_python_version=_text_field(decoded, "embedded_python_version"),
            native_abi_digest=_text_field(decoded, "native_abi_digest"),
            route_binding_digest=_text_field(decoded, "route_binding_digest"),
            evidence_digest=_text_field(decoded, "evidence_digest"),
            valid_from_wall_ns=_integer_field(decoded, "valid_from_wall_ns"),
            valid_until_wall_ns=optional_until,
            capabilities=frozenset(capabilities),
            signature=_text_field(decoded, "signature"),
            schema_version=_integer_field(decoded, "schema_version"),
        )


class QmtEd25519CapabilityVerifierV1:
    """Verify one capability manifest against a restricted trust set."""

    def __init__(
        self, trusted_public_keys: Mapping[str, bytes], *, revoked_signer_ids: AbstractSet[str]
    ) -> None:
        self._keys: dict[str, Ed25519PublicKey] = {}
        for signer_id, raw_key in trusted_public_keys.items():
            require_text(signer_id, "QMT capability signer id", max_length=256)
            if len(raw_key) != 32:
                raise QmtCapabilityError("QMT capability Ed25519 public key must contain 32 bytes")
            self._keys[signer_id] = Ed25519PublicKey.from_public_bytes(raw_key)
        self._revoked = frozenset(revoked_signer_ids)

    def verify(self, manifest: QmtCapabilityManifestV1) -> None:
        if manifest.signer_id in self._revoked:
            raise QmtCapabilityError("QMT capability signer is revoked")
        key = self._keys.get(manifest.signer_id)
        if key is None:
            raise QmtCapabilityError("QMT capability signer is not trusted")
        try:
            signature = base64.b64decode(manifest.signature, validate=True)
        except (ValueError, TypeError) as error:
            raise QmtCapabilityError("QMT capability signature is not base64") from error
        try:
            key.verify(signature, manifest.canonical_payload())
        except InvalidSignature as error:
            raise QmtCapabilityError("QMT capability signature verification failed") from error


@dataclass(frozen=True)
class QmtCapabilityActivation:
    """Verified P0 evidence bound to one mapping and Edge instance.

    The activation is still deliberately disarmed.  It only proves that a
    caller may construct a production bridge; a separate runtime mutation
    window remains outside this object and must be approved at execution time.
    """

    manifest: QmtCapabilityManifestV1

    @classmethod
    def activate(
        cls,
        *,
        manifest: QmtCapabilityManifestV1,
        verifier: QmtEd25519CapabilityVerifierV1,
        mapping: ActivatedVenueMapping,
        expected_edge_instance_digest: str,
        now_wall_ns: int,
    ) -> "QmtCapabilityActivation":
        verifier.verify(manifest)
        if not manifest.is_valid_at(now_wall_ns):
            raise QmtCapabilityError("QMT capability manifest is outside its validity interval")
        ownership = mapping.manifest.scope_ownership
        binding = mapping.manifest.route_token_binding
        if ownership is None or binding is None:
            raise QmtCapabilityError("QMT mapping lacks scope or route-token binding")
        if (
            manifest.adapter_instance_id != ownership.adapter_instance_id
            or manifest.adapter_client_scope != ownership.adapter_client_scope
            or manifest.mapping_manifest_hash != mapping.manifest.manifest_hash
            or manifest.route_binding_digest != binding.manifest_hash
            or manifest.edge_instance_digest
            != _sha256(expected_edge_instance_digest, "expected_edge_instance_digest")
        ):
            raise QmtCapabilityError("QMT capability manifest belongs to another adapter profile")
        if manifest.embedded_python_version != "3.6":
            raise QmtCapabilityError("QMT capability manifest does not prove embedded Python 3.6")
        missing = _REQUIRED_CAPABILITIES - manifest.capabilities
        if missing:
            raise QmtCapabilityError(
                f"QMT capability manifest lacks required probe results: {','.join(sorted(missing))}"
            )
        return cls(manifest=manifest)


def _text_field(value: Mapping[str, object], name: str) -> str:
    field = value[name]
    if not isinstance(field, str):
        raise QmtCapabilityError(f"QMT capability manifest {name} must be text")
    return field


def _integer_field(value: Mapping[str, object], name: str) -> int:
    field = value[name]
    if not isinstance(field, int):
        raise QmtCapabilityError(f"QMT capability manifest {name} must be an integer")
    return field
