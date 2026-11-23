"""Initialize local-only signing material and a three-scope DISARMED template."""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
from hashlib import sha256
from pathlib import Path
from typing import Sequence

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

_DEFAULT_SIGNER_ID = "emt-sim-disarmed-signer-v1"
_DEFAULT_PROFILE_ID = "emt-sim-three-scope-disarmed-v1"
_SCOPE_DESCRIPTORS = (
    ("ordinary", "7001"),
    ("credit", "7002"),
    ("option", "7003"),
)


class EmtDisarmedBootstrapError(ValueError):
    """The local-only three-scope material cannot be safely initialized."""


def build_bootstrap_artifacts(
    *,
    signer_id: str,
    profile_id: str,
    private_keys_path: Path,
    public_key_path: Path,
    revocations_path: Path,
    request_path: Path,
    now_wall_ns: int,
) -> None:
    """Write new secret/public/template files once; never replace existing files."""

    _require_text(signer_id, "EMT signer id")
    _require_text(profile_id, "EMT DISARMED profile id")
    if now_wall_ns <= 0:
        raise EmtDisarmedBootstrapError("EMT bootstrap wall time must be positive")
    paths = (private_keys_path, public_key_path, revocations_path, request_path)
    if len({path.resolve() for path in paths}) != len(paths):
        raise EmtDisarmedBootstrapError("EMT bootstrap output paths must be distinct")
    existing = tuple(path for path in paths if path.exists())
    if existing:
        raise EmtDisarmedBootstrapError("EMT bootstrap refuses to replace existing local material")

    private_key = Ed25519PrivateKey.generate()
    private_bytes = private_key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public_bytes = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    valid_until_wall_ns = now_wall_ns + 7 * 24 * 60 * 60 * 1_000_000_000
    certificate_reference = f"local://{public_key_path.as_posix()}"
    revocation_reference = f"local://{revocations_path.as_posix()}"
    private_payload = {signer_id: base64.b64encode(private_bytes).decode("ascii")}
    public_payload = {
        "schema_version": 1,
        "signer_id": signer_id,
        "algorithm": "Ed25519",
        "public_key_b64": base64.b64encode(public_bytes).decode("ascii"),
        "valid_from_wall_ns": now_wall_ns,
        "valid_until_wall_ns": valid_until_wall_ns,
    }
    revocations_payload = {
        "schema_version": 1,
        "authority": signer_id,
        "revoked_signer_ids": [],
        "generated_wall_ns": now_wall_ns,
    }
    request_payload = {
        "schema_version": 1,
        "template": True,
        "profile_id": profile_id,
        "approval": {"approved": False, "lifecycle": "DISARMED"},
        "scope_ownerships": [
            _scope_template(
                kind=kind,
                client_scope=client_scope,
                signer_id=signer_id,
                valid_from_wall_ns=now_wall_ns,
                valid_until_wall_ns=valid_until_wall_ns,
            )
            for kind, client_scope in _SCOPE_DESCRIPTORS
        ],
        "certificate_references": {signer_id: certificate_reference},
        "revocation_references": {signer_id: revocation_reference},
        "instructions": (
            "Replace every __REPLACE_*__ token from ignored local account configuration, "
            "run the uniqueness probe, set template=false, then invoke build_emt_disarmed_profile.py."
        ),
    }
    _write_private_json(private_keys_path, private_payload)
    try:
        _write_json(public_key_path, public_payload)
        _write_json(revocations_path, revocations_payload)
        _write_json(request_path, request_payload)
    except Exception:
        private_keys_path.unlink(missing_ok=True)
        public_key_path.unlink(missing_ok=True)
        revocations_path.unlink(missing_ok=True)
        request_path.unlink(missing_ok=True)
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Initialize local-only EMT test signer material and a three-scope DISARMED template"
    )
    parser.add_argument("--signer-id", default=_DEFAULT_SIGNER_ID)
    parser.add_argument("--profile-id", default=_DEFAULT_PROFILE_ID)
    parser.add_argument("--private-keys", type=Path, required=True)
    parser.add_argument("--public-key", type=Path, required=True)
    parser.add_argument("--revocations", type=Path, required=True)
    parser.add_argument("--request-template", type=Path, required=True)
    args = parser.parse_args(argv)
    build_bootstrap_artifacts(
        signer_id=args.signer_id,
        profile_id=args.profile_id,
        private_keys_path=args.private_keys,
        public_key_path=args.public_key,
        revocations_path=args.revocations,
        request_path=args.request_template,
        now_wall_ns=time.time_ns(),
    )
    print(f"EMT_DISARMED_PROFILE_TEMPLATE={args.profile_id}")
    return 0


def _scope_template(
    *,
    kind: str,
    client_scope: str,
    signer_id: str,
    valid_from_wall_ns: int,
    valid_until_wall_ns: int,
) -> dict[str, object]:
    descriptor = f"{kind}:{client_scope}:{signer_id}".encode("utf-8")
    pending_hash = sha256(b"pending:" + descriptor).hexdigest()
    return {
        "ownership_id": f"emt-sim-{kind}-client-{client_scope}-v1",
        "version": 1,
        "venue_id": "EMT",
        "account_id": f"__REPLACE_{kind.upper()}_ACCOUNT_ID__",
        "adapter_instance_id": "emt-sim-linux-x86_64-v1",
        "adapter_client_scope": client_scope,
        "owner_instance_id": "fengyu",
        "config_source_hash": pending_hash,
        "signer_id": signer_id,
        "valid_from_wall_ns": valid_from_wall_ns,
        "valid_until_wall_ns": valid_until_wall_ns,
        "uniqueness_probe_hash": pending_hash,
        "uniqueness_probe_succeeded": False,
        "revoked_at_wall_ns": None,
    }


def _write_private_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.write("\n")


def _require_text(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EmtDisarmedBootstrapError(f"{field_name} is required")


if __name__ == "__main__":
    raise SystemExit(main())
