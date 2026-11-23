from __future__ import annotations

import base64
import json
import tempfile
import unittest
from hashlib import sha256
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.acceptance.r1_namespace_release_material import (
    R1NamespaceReleaseMaterialError,
    build_namespace_trust,
    sign_namespace_release,
)
from tools.acceptance.r1_release_preflight import Settings as PreflightSettings
from tools.acceptance.r1_release_preflight import run as run_preflight


class R1NamespaceReleaseMaterialTests(unittest.TestCase):
    def _write_control_plane(self, root: Path) -> tuple[Path, Path, Path, str]:
        private_key = Ed25519PrivateKey.generate()
        signer_id = "r1-release-material-test"
        private_bytes = private_key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        public_bytes = private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        public_path = root / "public.json"
        public_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "signer_id": signer_id,
                    "algorithm": "Ed25519",
                    "public_key_b64": base64.b64encode(public_bytes).decode("ascii"),
                    "valid_from_wall_ns": 1,
                    "valid_until_wall_ns": 9_999_999_999,
                }
            ),
            encoding="utf-8",
        )
        revocations_path = root / "revocations.json"
        revocations_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "authority": "test",
                    "revoked_signer_ids": [],
                    "generated_wall_ns": 1,
                }
            ),
            encoding="utf-8",
        )
        private_path = root / "private.json"
        private_path.write_text(
            json.dumps({signer_id: base64.b64encode(private_bytes).decode("ascii")}),
            encoding="utf-8",
        )
        return public_path, revocations_path, private_path, signer_id

    def _request(self, root: Path, signer_id: str) -> Path:
        request = root / "request.json"
        request.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "template": False,
                    "release_id": "r1-material-test-release",
                    "release_epoch": 1,
                    "release_manifest_hash": sha256(b"reviewed-runtime").hexdigest(),
                    "valid_from_wall_ns": 1,
                    "valid_until_wall_ns": 9_999_999_999,
                    "signer_id": signer_id,
                    "certificate_reference": "test://certificate",
                    "revocation_reference": "test://revocations",
                    "scope": {
                        "account_id": "logical-test-account",
                        "trading_day": "2026-09-11",
                        "emt_client_id": 17,
                    },
                    "own": {
                        "namespace_id": "tradecore-a",
                        "owner_system_id": "tradecore-a",
                        "adapter_instance_id": "adapter-a",
                        "range_start": 1000,
                        "range_end": 1999,
                        "manifest_version": 1,
                    },
                    "foreign": [
                        {
                            "namespace_id": "tradecore-b",
                            "owner_system_id": "tradecore-b",
                            "adapter_instance_id": "adapter-b",
                            "range_start": 2000,
                            "range_end": 2999,
                            "manifest_version": 1,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return request

    def test_builds_immutable_public_trust_and_signed_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, revocations, private, signer_id = self._write_control_plane(root)
            trust_path = root / "trust.json"
            trust = build_namespace_trust(
                public_key_path=public, revocations_path=revocations, output=trust_path
            )
            self.assertEqual(1, len(trust.public_keys))
            release_path = root / "release.json"
            release = sign_namespace_release(
                request_path=self._request(root, signer_id),
                namespace_trust_path=trust_path,
                private_keys_path=private,
                output=release_path,
            )
            self.assertEqual(1, release.release_epoch)
            report = run_preflight(
                PreflightSettings(
                    namespace_release=release_path,
                    namespace_trust=trust_path,
                    expected_runtime_release_hash=sha256(b"reviewed-runtime").hexdigest(),
                    expected_account_id="logical-test-account",
                    expected_trading_day="2026-09-11",
                    expected_emt_client_id=17,
                    output=root / "preflight.json",
                ),
                now_wall_ns=2,
            )
            self.assertEqual("prepared", report["preflight_status"])
            self.assertNotIn("private", release_path.read_text(encoding="utf-8").lower())

    def test_rejects_a_private_key_that_does_not_match_public_trust(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, revocations, _private, signer_id = self._write_control_plane(root)
            other = Ed25519PrivateKey.generate()
            other_private = other.private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
            private = root / "other-private.json"
            private.write_text(
                json.dumps({signer_id: base64.b64encode(other_private).decode("ascii")}),
                encoding="utf-8",
            )
            trust_path = root / "trust.json"
            build_namespace_trust(
                public_key_path=public, revocations_path=revocations, output=trust_path
            )
            with self.assertRaisesRegex(R1NamespaceReleaseMaterialError, "does not match trust"):
                sign_namespace_release(
                    request_path=self._request(root, signer_id),
                    namespace_trust_path=trust_path,
                    private_keys_path=private,
                    output=root / "release.json",
                )

    def test_rejects_template_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, revocations, private, signer_id = self._write_control_plane(root)
            trust_path = root / "trust.json"
            build_namespace_trust(
                public_key_path=public, revocations_path=revocations, output=trust_path
            )
            request = self._request(root, signer_id)
            raw = json.loads(request.read_text(encoding="utf-8"))
            raw["template"] = True
            request.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(R1NamespaceReleaseMaterialError, "template"):
                sign_namespace_release(
                    request_path=request,
                    namespace_trust_path=trust_path,
                    private_keys_path=private,
                    output=root / "release.json",
                )


if __name__ == "__main__":
    unittest.main()
