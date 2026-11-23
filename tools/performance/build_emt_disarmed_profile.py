"""Build a signed EMT profile that is permanently DISARMED at creation time."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
from pathlib import Path
from typing import Mapping, Sequence

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters.emt import (
    EmtDisarmedProfileError,
    adapter_client_scope_ownership_from_dict,
    sign_disarmed_profile,
)
from xuanling_tradecore.domain import AdapterClientScopeOwnership


def load_signing_request(
    path: Path,
) -> tuple[
    str,
    tuple[AdapterClientScopeOwnership, ...],
    Mapping[str, str],
    Mapping[str, str],
]:
    """Load a non-secret request whose only admissible lifecycle is DISARMED."""

    root = _read_object(path, "EMT DISARMED signing request")
    if root.get("schema_version") != 1:
        raise EmtDisarmedProfileError("EMT DISARMED signing request schema_version must be 1")
    if root.get("template") is True:
        raise EmtDisarmedProfileError(
            "EMT DISARMED signing request is a template; replace every scope placeholder first"
        )
    approval = _object(root.get("approval"), "EMT DISARMED signing request approval")
    if approval.get("approved") is not False or approval.get("lifecycle") != "DISARMED":
        raise EmtDisarmedProfileError("EMT profile builder permits DISARMED only")
    raw_ownerships = root.get("scope_ownerships")
    if not isinstance(raw_ownerships, list) or not raw_ownerships:
        raise EmtDisarmedProfileError("EMT DISARMED signing request needs scope_ownerships")
    ownerships = tuple(
        adapter_client_scope_ownership_from_dict(_object(value, "EMT scope ownership"))
        for value in raw_ownerships
    )
    return (
        _text(root.get("profile_id"), "EMT DISARMED profile_id"),
        ownerships,
        _string_mapping(root.get("certificate_references"), "EMT certificate references"),
        _string_mapping(root.get("revocation_references"), "EMT revocation references"),
    )


def load_private_keys(path: Path) -> Mapping[str, Ed25519PrivateKey]:
    """Load raw Ed25519 private keys from an ignored local secret file."""

    raw = _read_object(path, "EMT scope private-key file")
    keys: dict[str, Ed25519PrivateKey] = {}
    for signer_id, encoded in raw.items():
        try:
            private_bytes = base64.b64decode(_text(encoded, "EMT private key"), validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EmtDisarmedProfileError("EMT private key is not base64") from exc
        if len(private_bytes) != 32:
            raise EmtDisarmedProfileError("EMT Ed25519 private key must contain 32 bytes")
        keys[_text(signer_id, "EMT signer id")] = Ed25519PrivateKey.from_private_bytes(
            private_bytes
        )
    if not keys:
        raise EmtDisarmedProfileError("EMT scope private-key file must not be empty")
    return keys


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an EMT signed DISARMED profile")
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--private-keys", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    profile_id, ownerships, certificate_references, revocation_references = load_signing_request(
        args.request
    )
    profile = sign_disarmed_profile(
        profile_id=profile_id,
        ownerships=ownerships,
        private_keys=load_private_keys(args.private_keys),
        certificate_references=certificate_references,
        revocation_references=revocation_references,
    )
    if args.output.exists():
        raise EmtDisarmedProfileError("EMT profile builder refuses to replace output")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as file:
        json.dump(profile.as_dict(), file, ensure_ascii=False, indent=2)
        file.write("\n")
    print(f"EMT_DISARMED_PROFILE={profile.profile_id}")
    return 0


def _read_object(path: Path, field_name: str) -> Mapping[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EmtDisarmedProfileError(f"{field_name} is unreadable") from exc
    return _object(raw, field_name)


def _object(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise EmtDisarmedProfileError(f"{field_name} must be an object")
    return value


def _string_mapping(value: object, field_name: str) -> Mapping[str, str]:
    raw = _object(value, field_name)
    return {
        _text(key, f"{field_name} signer id"): _text(item, field_name) for key, item in raw.items()
    }


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EmtDisarmedProfileError(f"{field_name} is required")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
