"""Signed, payload-free identity witnesses for EMT public-stream replay.

The EMT worker writes an immutable *proposal* after a controlled order is
accepted, reconciled and cleaned up. A separate offline control-plane step
signs that proposal with the reviewed R1 signer. A RESTART-only observer must
verify that signature before using the digest as evidence. Neither document
stores account numbers or native order identifiers.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Mapping, cast

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ...domain import TradingDayEpoch
from ...domain._validation import require_nonnegative, require_positive, require_text


class EmtReplayWitnessErrorV1(ValueError):
    """A replay witness proposal or signature is malformed or untrusted."""


def _require_hash(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise EmtReplayWitnessErrorV1(f"{label} must be a lowercase SHA-256 hash")


def _canonical_bytes(document: Mapping[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def _identity_digest(
    *,
    account_id: str,
    adapter_instance_id: str,
    adapter_client_scope: str,
    trading_day: TradingDayEpoch,
    order_client_id: int,
    order_emt_id: int,
) -> str:
    require_text(account_id, "replay witness account_id")
    require_text(adapter_instance_id, "replay adapter_instance_id")
    require_text(adapter_client_scope, "replay adapter_client_scope")
    require_positive(order_client_id, "replay witness order_client_id")
    require_positive(order_emt_id, "replay witness order_emt_id")
    return sha256(
        _canonical_bytes(
            {
                "account_id": account_id,
                "adapter_client_scope": adapter_client_scope,
                "adapter_instance_id": adapter_instance_id,
                "order_client_id": order_client_id,
                "order_emt_id": order_emt_id,
                "schema_version": 1,
                "trading_day": trading_day.value,
            }
        )
    ).hexdigest()


@dataclass(frozen=True)
class EmtReplayWitnessV1:
    """Unsigned, write-once proposal for one broker callback identity."""

    witness_id: str
    account_config_profile: str
    adapter_instance_id: str
    adapter_client_scope: str
    trading_day: TradingDayEpoch
    namespace_release_hash: str
    namespace_manifest_hash: str
    identity_digest: str
    issued_at_wall_ns: int
    expires_at_wall_ns: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.witness_id, "replay witness_id"),
            (self.account_config_profile, "replay account_config_profile"),
            (self.adapter_instance_id, "replay adapter_instance_id"),
            (self.adapter_client_scope, "replay adapter_client_scope"),
        ):
            require_text(value, label, max_length=128)
        for value, label in (
            (self.namespace_release_hash, "replay namespace_release_hash"),
            (self.namespace_manifest_hash, "replay namespace_manifest_hash"),
            (self.identity_digest, "replay identity_digest"),
        ):
            _require_hash(value, label)
        require_nonnegative(self.issued_at_wall_ns, "replay issued_at_wall_ns")
        require_positive(self.expires_at_wall_ns, "replay expires_at_wall_ns")
        if self.expires_at_wall_ns <= self.issued_at_wall_ns:
            raise EmtReplayWitnessErrorV1("replay witness validity interval is invalid")

    def _fields(self) -> dict[str, object]:
        return {
            "account_config_profile": self.account_config_profile,
            "adapter_client_scope": self.adapter_client_scope,
            "adapter_instance_id": self.adapter_instance_id,
            "expires_at_wall_ns": self.expires_at_wall_ns,
            "identity_digest": self.identity_digest,
            "issued_at_wall_ns": self.issued_at_wall_ns,
            "namespace_manifest_hash": self.namespace_manifest_hash,
            "namespace_release_hash": self.namespace_release_hash,
            "trading_day": self.trading_day.value,
            "witness_id": self.witness_id,
        }

    @property
    def content_sha256(self) -> str:
        return sha256(_canonical_bytes(self._fields())).hexdigest()

    def active_at(self, wall_ns: int) -> bool:
        return self.issued_at_wall_ns <= wall_ns < self.expires_at_wall_ns

    def matches_callback(
        self,
        *,
        account_id: str,
        order_client_id: int,
        order_emt_id: int,
        observed_at_wall_ns: int,
    ) -> bool:
        if not self.active_at(observed_at_wall_ns):
            return False
        return self.identity_digest == _identity_digest(
            account_id=account_id,
            adapter_instance_id=self.adapter_instance_id,
            adapter_client_scope=self.adapter_client_scope,
            trading_day=self.trading_day,
            order_client_id=order_client_id,
            order_emt_id=order_emt_id,
        )

    def as_proposal_document(self) -> dict[str, object]:
        document = {"schema_version": 1, **self._fields()}
        document["content_sha256"] = self.content_sha256
        return document

    def as_public_document(self) -> dict[str, object]:
        """Compatibility spelling for the unsigned proposal document only."""

        return self.as_proposal_document()


@dataclass(frozen=True)
class EmtSignedReplayWitnessV1:
    """Offline-signed proposal that an independent RESTART observer may trust."""

    witness: EmtReplayWitnessV1
    signer_id: str
    certificate_reference: str
    revocation_reference: str
    signature_b64: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.signer_id, "replay witness signer_id"),
            (self.certificate_reference, "replay witness certificate_reference"),
            (self.revocation_reference, "replay witness revocation_reference"),
        ):
            require_text(value, label, max_length=512)
        try:
            signature = base64.b64decode(self.signature_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EmtReplayWitnessErrorV1("replay witness signature is not base64") from exc
        if len(signature) != 64:
            raise EmtReplayWitnessErrorV1("replay witness signature is not Ed25519")

    def canonical_payload(self) -> bytes:
        return _canonical_bytes(
            {
                "certificate_reference": self.certificate_reference,
                "revocation_reference": self.revocation_reference,
                "schema_version": 1,
                "signer_id": self.signer_id,
                "witness_content_sha256": self.witness.content_sha256,
            }
        )

    @property
    def content_sha256(self) -> str:
        return sha256(
            _canonical_bytes(
                {
                    "certificate_reference": self.certificate_reference,
                    "revocation_reference": self.revocation_reference,
                    "schema_version": 1,
                    "signature_b64": self.signature_b64,
                    "signer_id": self.signer_id,
                    "witness": self.witness.as_proposal_document(),
                }
            )
        ).hexdigest()

    def as_public_document(self) -> dict[str, object]:
        document: dict[str, object] = {
            "schema_version": 1,
            "witness": self.witness.as_proposal_document(),
            "signer_id": self.signer_id,
            "certificate_reference": self.certificate_reference,
            "revocation_reference": self.revocation_reference,
            "signature_b64": self.signature_b64,
        }
        document["content_sha256"] = self.content_sha256
        return document


def issue_emt_replay_witness_v1(
    *,
    witness_id: str,
    account_config_profile: str,
    account_id: str,
    adapter_instance_id: str,
    adapter_client_scope: str,
    trading_day: TradingDayEpoch,
    namespace_release_hash: str,
    namespace_manifest_hash: str,
    order_client_id: int,
    order_emt_id: int,
    issued_at_wall_ns: int,
    expires_at_wall_ns: int,
) -> EmtReplayWitnessV1:
    """Make a digest-only proposal after a controlled native acceptance."""

    return EmtReplayWitnessV1(
        witness_id=witness_id,
        account_config_profile=account_config_profile,
        adapter_instance_id=adapter_instance_id,
        adapter_client_scope=adapter_client_scope,
        trading_day=trading_day,
        namespace_release_hash=namespace_release_hash,
        namespace_manifest_hash=namespace_manifest_hash,
        identity_digest=_identity_digest(
            account_id=account_id,
            adapter_instance_id=adapter_instance_id,
            adapter_client_scope=adapter_client_scope,
            trading_day=trading_day,
            order_client_id=order_client_id,
            order_emt_id=order_emt_id,
        ),
        issued_at_wall_ns=issued_at_wall_ns,
        expires_at_wall_ns=expires_at_wall_ns,
    )


def sign_emt_replay_witness_v1(
    witness: EmtReplayWitnessV1,
    *,
    signer_id: str,
    certificate_reference: str,
    revocation_reference: str,
    private_key: Ed25519PrivateKey,
) -> EmtSignedReplayWitnessV1:
    """Sign a reviewed proposal with an offline R1 Ed25519 private key."""

    unsigned = EmtSignedReplayWitnessV1(
        witness=witness,
        signer_id=signer_id,
        certificate_reference=certificate_reference,
        revocation_reference=revocation_reference,
        signature_b64=base64.b64encode(b"\x00" * 64).decode("ascii"),
    )
    return EmtSignedReplayWitnessV1(
        witness=witness,
        signer_id=signer_id,
        certificate_reference=certificate_reference,
        revocation_reference=revocation_reference,
        signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
            "ascii"
        ),
    )


def write_emt_replay_witness_proposal_v1(path: Path, witness: EmtReplayWitnessV1) -> None:
    """Create one unsigned proposal; it remains unusable as replay proof."""

    _write_new_json(path, witness.as_proposal_document())


def write_signed_emt_replay_witness_v1(path: Path, witness: EmtSignedReplayWitnessV1) -> None:
    """Create one signed public witness without replacing prior evidence."""

    _write_new_json(path, witness.as_public_document())


def load_emt_replay_witness_proposal_v1(path: Path) -> EmtReplayWitnessV1:
    """Load and hash-verify a proposal before offline signing."""

    return _proposal_from_document(_read_document(path, "replay witness proposal"))


def load_signed_emt_replay_witness_v1(path: Path) -> EmtSignedReplayWitnessV1:
    """Load and hash-verify a signed witness; signature verification is external."""

    raw = _read_document(path, "signed replay witness")
    expected = {
        "schema_version",
        "witness",
        "signer_id",
        "certificate_reference",
        "revocation_reference",
        "signature_b64",
        "content_sha256",
    }
    if set(raw) != expected or raw.get("schema_version") != 1:
        raise EmtReplayWitnessErrorV1("signed replay witness schema is invalid")
    nested = raw.get("witness")
    if not isinstance(nested, Mapping):
        raise EmtReplayWitnessErrorV1("signed replay witness proposal is invalid")
    witness = _proposal_from_document(cast(Mapping[str, object], nested))
    try:
        signed = EmtSignedReplayWitnessV1(
            witness=witness,
            signer_id=_text_field(raw, "signer_id"),
            certificate_reference=_text_field(raw, "certificate_reference"),
            revocation_reference=_text_field(raw, "revocation_reference"),
            signature_b64=_text_field(raw, "signature_b64"),
        )
    except (TypeError, ValueError) as exc:
        raise EmtReplayWitnessErrorV1("signed replay witness fields are invalid") from exc
    if raw.get("content_sha256") != signed.content_sha256:
        raise EmtReplayWitnessErrorV1("signed replay witness content hash does not match")
    return signed


# Backward-compatible aliases for proposal-only control-plane code. Verification
# code must import the explicitly signed loader above.
write_emt_replay_witness_v1 = write_emt_replay_witness_proposal_v1
load_emt_replay_witness_v1 = load_emt_replay_witness_proposal_v1


def _proposal_from_document(raw: Mapping[str, object]) -> EmtReplayWitnessV1:
    expected = {
        "schema_version",
        "witness_id",
        "account_config_profile",
        "adapter_instance_id",
        "adapter_client_scope",
        "trading_day",
        "namespace_release_hash",
        "namespace_manifest_hash",
        "identity_digest",
        "issued_at_wall_ns",
        "expires_at_wall_ns",
        "content_sha256",
    }
    if set(raw) != expected or raw.get("schema_version") != 1:
        raise EmtReplayWitnessErrorV1("replay witness proposal schema is invalid")
    try:
        witness = EmtReplayWitnessV1(
            witness_id=_text_field(raw, "witness_id"),
            account_config_profile=_text_field(raw, "account_config_profile"),
            adapter_instance_id=_text_field(raw, "adapter_instance_id"),
            adapter_client_scope=_text_field(raw, "adapter_client_scope"),
            trading_day=TradingDayEpoch(_text_field(raw, "trading_day")),
            namespace_release_hash=_text_field(raw, "namespace_release_hash"),
            namespace_manifest_hash=_text_field(raw, "namespace_manifest_hash"),
            identity_digest=_text_field(raw, "identity_digest"),
            issued_at_wall_ns=_integer_field(raw, "issued_at_wall_ns"),
            expires_at_wall_ns=_integer_field(raw, "expires_at_wall_ns"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise EmtReplayWitnessErrorV1("replay witness proposal fields are invalid") from exc
    if raw.get("content_sha256") != witness.content_sha256:
        raise EmtReplayWitnessErrorV1("replay witness proposal content hash does not match")
    return witness


def _read_document(path: Path, label: str) -> Mapping[str, object]:
    try:
        parsed = json.loads(Path(path).read_text(encoding="ascii"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EmtReplayWitnessErrorV1(f"{label} cannot be read") from exc
    if not isinstance(parsed, Mapping):
        raise EmtReplayWitnessErrorV1(f"{label} schema is invalid")
    return cast(Mapping[str, object], parsed)


def _write_new_json(path: Path, document: Mapping[str, object]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="ascii") as handle:
        json.dump(document, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _text_field(document: Mapping[str, object], name: str) -> str:
    value = document.get(name)
    if type(value) is not str:
        raise EmtReplayWitnessErrorV1(f"replay witness {name} is invalid")
    return value


def _integer_field(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if type(value) is not int:
        raise EmtReplayWitnessErrorV1(f"replay witness {name} is invalid")
    return value
