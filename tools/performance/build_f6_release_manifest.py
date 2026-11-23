"""Build and verify a signed, non-secret F6 EMT-only release manifest.

The private key is supplied only from an ignored local file.  This tool does
not deploy code, configure a host, or start an EMT session.
"""

from __future__ import annotations

import argparse
import base64
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Mapping, Sequence, cast

from tools.performance.build_emt_disarmed_profile import load_private_keys
from xuanling_tradecore.adapters.emt import EmtDisarmedProfileError, load_emt_disarmed_signer_trust
from xuanling_tradecore.runtime import (
    Ed25519ReleaseManifestVerifierV1,
    ReleaseManifestActivationErrorV1,
    SignedReleaseManifestV1,
    load_signed_release_manifest_v1_json,
)


class F6ReleaseManifestToolError(ValueError):
    """A release manifest request, signature, or trust record is invalid."""


def load_unsigned_release_request(path: Path) -> SignedReleaseManifestV1:
    """Load the exact V1 payload without a signature for offline signing."""

    document = _read_object(path, "F6 release manifest request")
    if "signature" in document:
        raise F6ReleaseManifestToolError("F6 release manifest request must not include signature")
    candidate = dict(document)
    candidate["signature"] = "unsigned-placeholder"
    try:
        return load_signed_release_manifest_v1_json(json.dumps(candidate, sort_keys=True))
    except ReleaseManifestActivationErrorV1 as exc:
        raise F6ReleaseManifestToolError(str(exc)) from exc


def sign_release_manifest(
    *, request: SignedReleaseManifestV1, private_keys_path: Path
) -> SignedReleaseManifestV1:
    """Sign canonical bytes with the request's declared local signer identity."""

    try:
        private_key = load_private_keys(private_keys_path)[request.signer_id]
    except KeyError as exc:
        raise F6ReleaseManifestToolError("F6 release signer private key is unavailable") from exc
    signature = base64.b64encode(private_key.sign(request.canonical_bytes())).decode("ascii")
    return replace(request, signature=signature)


def verify_release_manifest(
    *,
    manifest: SignedReleaseManifestV1,
    public_key_path: Path,
    revocations_path: Path,
    now_wall_ns: int,
) -> None:
    """Verify against an independently supplied active public-key/revocation record."""

    if now_wall_ns < 0:
        raise F6ReleaseManifestToolError("F6 release verification time must be nonnegative")
    trust = load_emt_disarmed_signer_trust(
        public_key_path=public_key_path,
        revocations_path=revocations_path,
    )
    try:
        trust.verifier_at(now_wall_ns=now_wall_ns)
    except EmtDisarmedProfileError as exc:
        raise F6ReleaseManifestToolError("F6 release signer trust is not active") from exc
    if manifest.signer_id != trust.signer_id:
        raise F6ReleaseManifestToolError("F6 release manifest signer differs from trust record")
    verifier = Ed25519ReleaseManifestVerifierV1({trust.signer_id: trust.public_key})
    if not verifier.verify(
        signer_id=manifest.signer_id,
        payload=manifest.canonical_bytes(),
        signature=manifest.signature,
    ):
        raise F6ReleaseManifestToolError("F6 release manifest signature verification failed")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the offline signer or verifier without exposing secret material."""

    parser = argparse.ArgumentParser(description="Build or verify an F6 release manifest")
    commands = parser.add_subparsers(dest="command", required=True)
    sign = commands.add_parser("sign", help="sign one non-secret release request")
    sign.add_argument("--request", type=Path, required=True)
    sign.add_argument("--private-keys", type=Path, required=True)
    sign.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify", help="verify one signed release manifest")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--public-key", type=Path, required=True)
    verify.add_argument("--revocations", type=Path, required=True)
    verify.add_argument("--now-wall-ns", type=int, default=None)
    args = parser.parse_args(argv)
    if args.command == "sign":
        request = load_unsigned_release_request(args.request)
        signed = sign_release_manifest(request=request, private_keys_path=args.private_keys)
        if args.output.exists():
            raise F6ReleaseManifestToolError("F6 release manifest signer refuses to replace output")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(signed.as_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"F6_RELEASE_MANIFEST_SHA256={signed.content_hash}")
        print(f"F6_RELEASE_EPOCH={signed.release_epoch}")
        return 0
    raw = args.manifest.read_text(encoding="utf-8")
    try:
        manifest = load_signed_release_manifest_v1_json(raw)
    except ReleaseManifestActivationErrorV1 as exc:
        raise F6ReleaseManifestToolError(str(exc)) from exc
    now_wall_ns = time.time_ns() if args.now_wall_ns is None else args.now_wall_ns
    verify_release_manifest(
        manifest=manifest,
        public_key_path=args.public_key,
        revocations_path=args.revocations,
        now_wall_ns=now_wall_ns,
    )
    print(f"F6_RELEASE_MANIFEST_SHA256={manifest.content_hash}")
    print("F6_RELEASE_SIGNATURE=VALID")
    return 0


def _read_object(path: Path, field_name: str) -> Mapping[str, object]:
    try:
        raw = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise F6ReleaseManifestToolError(f"{field_name} is unreadable") from exc
    if not isinstance(raw, dict):
        raise F6ReleaseManifestToolError(f"{field_name} must be an object")
    return cast(Mapping[str, object], raw)


if __name__ == "__main__":
    raise SystemExit(main())
