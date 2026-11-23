"""Build public R1 namespace-release material outside an EMT runtime.

This utility has two deliberately separate operations:

* ``build-trust`` converts one reviewed public signer and revocation record
  into the strict, public-only trust document consumed by the runtime; and
* ``sign-release`` converts a non-secret allocation request into one immutable
  signed release using an ignored offline private-key file.

Neither operation imports an EMT SDK, reads account credentials, opens a
session, or writes runtime release state.  The signer input is intentionally
outside Git and the resulting documents contain no private material.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence, cast

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.acceptance.r1_release_preflight import NamespaceTrustV1, load_namespace_trust_v1
from tools.performance.build_emt_disarmed_profile import load_private_keys
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    OrderClientIdNamespaceLeaseV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
)
from xuanling_tradecore.domain import TradingDayEpoch


class R1NamespaceReleaseMaterialError(ValueError):
    """A public release request or offline signing input is unsafe."""


@dataclass(frozen=True)
class LeaseRequestV1:
    """Unsigned allocation data for one own or foreign namespace lease."""

    namespace_id: str
    owner_system_id: str
    adapter_instance_id: str
    range_start: int
    range_end: int
    manifest_version: int


@dataclass(frozen=True)
class ReleaseRequestV1:
    """Non-secret, reviewable input for one signed R1 namespace release."""

    release_id: str
    release_epoch: int
    release_manifest_hash: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    signer_id: str
    certificate_reference: str
    revocation_reference: str
    scope: EmtOrderClientNamespaceScope
    own: LeaseRequestV1
    foreign: tuple[LeaseRequestV1, ...]


def build_namespace_trust(
    *, public_key_path: Path, revocations_path: Path, output: Path
) -> NamespaceTrustV1:
    """Build an exact public-only R1 trust document without overwriting output."""

    if output.exists():
        raise R1NamespaceReleaseMaterialError("R1 namespace trust output already exists")
    public = _read_object(public_key_path, "R1 namespace public key")
    expected_public_fields = {
        "schema_version",
        "signer_id",
        "algorithm",
        "public_key_b64",
        "valid_from_wall_ns",
        "valid_until_wall_ns",
    }
    if set(public) != expected_public_fields or public.get("schema_version") != 1:
        raise R1NamespaceReleaseMaterialError("R1 namespace public key fields differ")
    if public.get("algorithm") != "Ed25519":
        raise R1NamespaceReleaseMaterialError("R1 namespace public key algorithm differs")
    signer_id = _text(public, "signer_id")
    public_key_b64 = _text(public, "public_key_b64")
    try:
        public_key = base64.b64decode(public_key_b64, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise R1NamespaceReleaseMaterialError("R1 namespace public key is not base64") from exc
    if len(public_key) != 32:
        raise R1NamespaceReleaseMaterialError("R1 namespace public key is not Ed25519")
    _validity(public, "R1 namespace public key")

    revocations = _read_object(revocations_path, "R1 namespace revocations")
    if set(revocations) != {
        "schema_version",
        "authority",
        "revoked_signer_ids",
        "generated_wall_ns",
    }:
        raise R1NamespaceReleaseMaterialError("R1 namespace revocation fields differ")
    if revocations.get("schema_version") != 1 or not isinstance(revocations.get("authority"), str):
        raise R1NamespaceReleaseMaterialError("R1 namespace revocation schema differs")
    revoked = revocations.get("revoked_signer_ids")
    if not isinstance(revoked, list) or any(
        not isinstance(item, str) or not item for item in revoked
    ):
        raise R1NamespaceReleaseMaterialError("R1 namespace revocations are invalid")
    if len(set(revoked)) != len(revoked):
        raise R1NamespaceReleaseMaterialError("R1 namespace revocations repeat signer ids")
    if signer_id in revoked:
        raise R1NamespaceReleaseMaterialError("R1 namespace signer is revoked")
    document = {
        "schema_version": 1,
        "trusted_public_keys": [{"signer_id": signer_id, "public_key_b64": public_key_b64}],
        "revoked_signer_ids": sorted(revoked),
    }
    _write_new_json(output, document)
    return load_namespace_trust_v1(output)


def load_release_request(path: Path) -> ReleaseRequestV1:
    """Load one exact, non-template R1 namespace allocation request."""

    document = _read_object(path, "R1 namespace release request")
    expected = {
        "schema_version",
        "template",
        "release_id",
        "release_epoch",
        "release_manifest_hash",
        "valid_from_wall_ns",
        "valid_until_wall_ns",
        "signer_id",
        "certificate_reference",
        "revocation_reference",
        "scope",
        "own",
        "foreign",
    }
    if set(document) != expected or document.get("schema_version") != 1:
        raise R1NamespaceReleaseMaterialError("R1 namespace release request fields differ")
    if document.get("template") is not False:
        raise R1NamespaceReleaseMaterialError("R1 namespace release request is a template")
    scope = _scope(_object(document, "scope"))
    own = _lease_request(_object(document, "own"), "R1 namespace own lease")
    foreign_raw = document.get("foreign")
    if not isinstance(foreign_raw, list):
        raise R1NamespaceReleaseMaterialError("R1 namespace foreign leases are invalid")
    foreign = tuple(
        _lease_request(
            _object_item(item, "R1 namespace foreign lease"), "R1 namespace foreign lease"
        )
        for item in foreign_raw
    )
    release_manifest_hash = _hash(document, "release_manifest_hash")
    valid_from_wall_ns, valid_until_wall_ns = _validity(document, "R1 namespace release request")
    try:
        return ReleaseRequestV1(
            release_id=_text(document, "release_id"),
            release_epoch=_positive_int(document, "release_epoch"),
            release_manifest_hash=release_manifest_hash,
            valid_from_wall_ns=valid_from_wall_ns,
            valid_until_wall_ns=valid_until_wall_ns,
            signer_id=_text(document, "signer_id"),
            certificate_reference=_text(document, "certificate_reference"),
            revocation_reference=_text(document, "revocation_reference"),
            scope=scope,
            own=own,
            foreign=foreign,
        )
    except ValueError as exc:
        raise R1NamespaceReleaseMaterialError("R1 namespace release request is invalid") from exc


def sign_namespace_release(
    *, request_path: Path, namespace_trust_path: Path, private_keys_path: Path, output: Path
) -> SignedNamespaceReleaseV1:
    """Sign one immutable release after trust and signer-material cross-checks."""

    if output.exists():
        raise R1NamespaceReleaseMaterialError("R1 namespace release output already exists")
    request = load_release_request(request_path)
    trust = load_namespace_trust_v1(namespace_trust_path)
    if request.signer_id in trust.revoked_signer_ids:
        raise R1NamespaceReleaseMaterialError("R1 namespace release signer is revoked")
    try:
        private_key = load_private_keys(private_keys_path)[request.signer_id]
    except (KeyError, ValueError) as exc:
        raise R1NamespaceReleaseMaterialError(
            "R1 namespace release private signer is unavailable"
        ) from exc
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    expected_public_key = trust.public_keys.get(request.signer_id)
    if expected_public_key != public_key:
        raise R1NamespaceReleaseMaterialError("R1 namespace release signer does not match trust")
    release = _sign_request(request, private_key=private_key, trust=trust)
    _write_new_json(output, release.as_dict())
    return release


def _sign_request(
    request: ReleaseRequestV1,
    *,
    private_key: Ed25519PrivateKey,
    trust: NamespaceTrustV1,
) -> SignedNamespaceReleaseV1:
    def signed_lease(item: LeaseRequestV1) -> SignedOrderClientIdNamespaceLeaseV1:
        unsigned = SignedOrderClientIdNamespaceLeaseV1(
            lease=OrderClientIdNamespaceLeaseV1(
                scope=request.scope,
                namespace_id=item.namespace_id,
                owner_system_id=item.owner_system_id,
                adapter_instance_id=item.adapter_instance_id,
                range_start=item.range_start,
                range_end=item.range_end,
                manifest_version=item.manifest_version,
                valid_from_wall_ns=request.valid_from_wall_ns,
                valid_until_wall_ns=request.valid_until_wall_ns,
                release_manifest_hash=request.release_manifest_hash,
                signer_id=request.signer_id,
            ),
            certificate_reference=request.certificate_reference,
            revocation_reference=request.revocation_reference,
            signature_b64="AA==",
        )
        return replace(
            unsigned,
            signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
                "ascii"
            ),
        )

    unsigned = SignedNamespaceReleaseV1(
        release_id=request.release_id,
        release_epoch=request.release_epoch,
        release_manifest_hash=request.release_manifest_hash,
        trust_anchor_hash=trust.trust_anchor_hash,
        valid_from_wall_ns=request.valid_from_wall_ns,
        valid_until_wall_ns=request.valid_until_wall_ns,
        own=signed_lease(request.own),
        foreign=tuple(signed_lease(item) for item in request.foreign),
        signer_id=request.signer_id,
        certificate_reference=request.certificate_reference,
        revocation_reference=request.revocation_reference,
        signature_b64="AA==",
    )
    return replace(
        unsigned,
        signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
            "ascii"
        ),
    )


def _read_object(path: Path, name: str) -> Mapping[str, object]:
    try:
        raw = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise R1NamespaceReleaseMaterialError(f"{name} is unreadable") from exc
    return _object_item(raw, name)


def _object(document: Mapping[str, object], name: str) -> Mapping[str, object]:
    try:
        return _object_item(document[name], name)
    except KeyError as exc:
        raise R1NamespaceReleaseMaterialError(f"{name} is missing") from exc


def _object_item(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise R1NamespaceReleaseMaterialError(f"{name} is not an object")
    return cast(Mapping[str, object], value)


def _text(document: Mapping[str, object], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value.strip():
        raise R1NamespaceReleaseMaterialError(f"R1 namespace {name} is invalid")
    return value


def _positive_int(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if type(value) is not int or value <= 0:
        raise R1NamespaceReleaseMaterialError(f"R1 namespace {name} is invalid")
    return value


def _nonnegative_int(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if type(value) is not int or value < 0:
        raise R1NamespaceReleaseMaterialError(f"R1 namespace {name} is invalid")
    return value


def _hash(document: Mapping[str, object], name: str) -> str:
    value = _text(document, name)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise R1NamespaceReleaseMaterialError(f"R1 namespace {name} is invalid")
    return value


def _validity(document: Mapping[str, object], name: str) -> tuple[int, int]:
    start = _nonnegative_int(document, "valid_from_wall_ns")
    end = _positive_int(document, "valid_until_wall_ns")
    if end <= start:
        raise R1NamespaceReleaseMaterialError(f"{name} validity interval is invalid")
    return start, end


def _scope(document: Mapping[str, object]) -> EmtOrderClientNamespaceScope:
    expected = {"account_id", "trading_day", "emt_client_id"}
    if set(document) != expected:
        raise R1NamespaceReleaseMaterialError("R1 namespace scope fields differ")
    try:
        return EmtOrderClientNamespaceScope(
            _text(document, "account_id"),
            TradingDayEpoch(_text(document, "trading_day")),
            _nonnegative_int(document, "emt_client_id"),
        )
    except ValueError as exc:
        raise R1NamespaceReleaseMaterialError("R1 namespace scope is invalid") from exc


def _lease_request(document: Mapping[str, object], name: str) -> LeaseRequestV1:
    expected = {
        "namespace_id",
        "owner_system_id",
        "adapter_instance_id",
        "range_start",
        "range_end",
        "manifest_version",
    }
    if set(document) != expected:
        raise R1NamespaceReleaseMaterialError(f"{name} fields differ")
    start, end = _positive_int(document, "range_start"), _positive_int(document, "range_end")
    if end < start:
        raise R1NamespaceReleaseMaterialError(f"{name} range is invalid")
    return LeaseRequestV1(
        namespace_id=_text(document, "namespace_id"),
        owner_system_id=_text(document, "owner_system_id"),
        adapter_instance_id=_text(document, "adapter_instance_id"),
        range_start=start,
        range_end=end,
        manifest_version=_positive_int(document, "manifest_version"),
    )


def _write_new_json(path: Path, document: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except OSError as exc:
        raise R1NamespaceReleaseMaterialError(
            "R1 namespace release output cannot be created"
        ) from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    trust = commands.add_parser("build-trust", help="build strict public R1 namespace trust")
    trust.add_argument("--public-key", type=Path, required=True)
    trust.add_argument("--revocations", type=Path, required=True)
    trust.add_argument("--output", type=Path, required=True)
    release = commands.add_parser("sign-release", help="sign one reviewed R1 namespace release")
    release.add_argument("--request", type=Path, required=True)
    release.add_argument("--namespace-trust", type=Path, required=True)
    release.add_argument("--private-keys", type=Path, required=True)
    release.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "build-trust":
        material = build_namespace_trust(
            public_key_path=args.public_key, revocations_path=args.revocations, output=args.output
        )
        print(json.dumps({"trust_anchor_hash": material.trust_anchor_hash}))
    else:
        signed = sign_namespace_release(
            request_path=args.request,
            namespace_trust_path=args.namespace_trust,
            private_keys_path=args.private_keys,
            output=args.output,
        )
        print(
            json.dumps({"release_hash": signed.content_hash, "release_epoch": signed.release_epoch})
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
