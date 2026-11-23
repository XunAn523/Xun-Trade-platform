"""Offline F6 release-manifest signer/verifier tests."""

from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.performance.build_f6_release_manifest import (
    F6ReleaseManifestToolError,
    load_unsigned_release_request,
    sign_release_manifest,
    verify_release_manifest,
)

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


def _request() -> dict[str, object]:
    return {
        "manifest_id": "f6-emt-r0-1",
        "release_epoch": 1,
        "release_kind": "NORMAL",
        "artifact_hash": HASH_A,
        "emt_sdk_hash": HASH_B,
        "native_extension_hash": HASH_C,
        "elf_abi_hash": HASH_D,
        "systemd_unit_hash": HASH_E,
        "cgroup_policy_hash": HASH_F,
        "host_health_policy_hash": HASH_A,
        "journal_policy_hash": HASH_B,
        "control_root_policy_hash": HASH_C,
        "archive_policy_hash": HASH_D,
        "bundle_trust_policy_hash": HASH_E,
        "account_profile_hashes": {"emt-account-a": HASH_F},
        "signer_id": "f6-release-signer-1",
        "valid_from_wall_ns": 10,
        "valid_until_wall_ns": 100,
        "rollback_from_artifact_hash": None,
        "rollback_approval_hash": None,
    }


class BuildF6ReleaseManifestTests(unittest.TestCase):
    def test_signs_and_verifies_only_with_independent_active_trust(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        private_raw = private_key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        public_raw = private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path = root / "request.json"
            private_path = root / "private.json"
            public_path = root / "public.json"
            revocations_path = root / "revocations.json"
            request_path.write_text(json.dumps(_request()), encoding="utf-8")
            private_path.write_text(
                json.dumps({"f6-release-signer-1": base64.b64encode(private_raw).decode("ascii")}),
                encoding="utf-8",
            )
            public_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "signer_id": "f6-release-signer-1",
                        "algorithm": "Ed25519",
                        "public_key_b64": base64.b64encode(public_raw).decode("ascii"),
                        "valid_from_wall_ns": 10,
                        "valid_until_wall_ns": 100,
                    }
                ),
                encoding="utf-8",
            )
            revocations_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "authority": "f6-release-signer-1",
                        "revoked_signer_ids": [],
                        "generated_wall_ns": 10,
                    }
                ),
                encoding="utf-8",
            )
            unsigned = load_unsigned_release_request(request_path)
            signed = sign_release_manifest(request=unsigned, private_keys_path=private_path)
            verify_release_manifest(
                manifest=signed,
                public_key_path=public_path,
                revocations_path=revocations_path,
                now_wall_ns=20,
            )
            with self.assertRaisesRegex(F6ReleaseManifestToolError, "not active"):
                verify_release_manifest(
                    manifest=signed,
                    public_key_path=public_path,
                    revocations_path=revocations_path,
                    now_wall_ns=100,
                )

    def test_request_rejects_embedded_signature(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "request.json"
            request = _request()
            request["signature"] = "not-allowed"
            path.write_text(json.dumps(request), encoding="utf-8")
            with self.assertRaisesRegex(F6ReleaseManifestToolError, "must not include"):
                load_unsigned_release_request(path)


if __name__ == "__main__":
    unittest.main()
