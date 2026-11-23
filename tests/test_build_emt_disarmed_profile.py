"""Tests for the local secret-to-non-secret EMT DISARMED profile builder."""

from __future__ import annotations

import base64
import json
import unittest
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.performance.build_emt_disarmed_profile import (
    load_signing_request,
    main,
)
from xuanling_tradecore.adapters.emt import (
    EmtDisarmedProfileError,
    load_builtin_emt_legacy_sim_manifest,
    load_emt_disarmed_profile,
)


class BuildEmtDisarmedProfileTests(unittest.TestCase):
    def _request(self) -> dict[str, object]:
        manifest = load_builtin_emt_legacy_sim_manifest()
        assert manifest.scope_ownership is not None
        signer_id = manifest.scope_ownership.signer_id
        return {
            "schema_version": 1,
            "profile_id": "emt-sim-disarmed-v1",
            "approval": {"approved": False, "lifecycle": "DISARMED"},
            "scope_ownerships": [asdict(manifest.scope_ownership)],
            "certificate_references": {signer_id: "test://emt-signer-public-key"},
            "revocation_references": {signer_id: "test://emt-signer-revocations"},
        }

    def test_builder_writes_a_verified_profile_without_private_key(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        manifest = load_builtin_emt_legacy_sim_manifest()
        assert manifest.scope_ownership is not None
        raw_key = private_key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            request_path = root / "request.json"
            keys_path = root / "keys.json"
            output_path = root / "profile.json"
            request_path.write_text(json.dumps(self._request()), encoding="utf-8")
            keys_path.write_text(
                json.dumps(
                    {manifest.scope_ownership.signer_id: base64.b64encode(raw_key).decode()}
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                0,
                main(
                    [
                        "--request",
                        str(request_path),
                        "--private-keys",
                        str(keys_path),
                        "--output",
                        str(output_path),
                    ]
                ),
            )
            rendered = output_path.read_text(encoding="utf-8")
            self.assertNotIn(base64.b64encode(raw_key).decode(), rendered)
            self.assertEqual(
                "emt-sim-disarmed-v1", load_emt_disarmed_profile(output_path).profile_id
            )

    def test_builder_rejects_any_request_that_is_not_disarmed(self) -> None:
        request = self._request()
        request["approval"] = {"approved": True, "lifecycle": "DISARMED"}
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "request.json"
            path.write_text(json.dumps(request), encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "DISARMED only"):
                load_signing_request(path)

    def test_builder_refuses_to_replace_an_existing_profile(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        manifest = load_builtin_emt_legacy_sim_manifest()
        assert manifest.scope_ownership is not None
        raw_key = private_key.private_bytes(
            serialization.Encoding.Raw,
            serialization.PrivateFormat.Raw,
            serialization.NoEncryption(),
        )
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            request_path = root / "request.json"
            keys_path = root / "keys.json"
            output_path = root / "profile.json"
            request_path.write_text(json.dumps(self._request()), encoding="utf-8")
            keys_path.write_text(
                json.dumps(
                    {manifest.scope_ownership.signer_id: base64.b64encode(raw_key).decode()}
                ),
                encoding="utf-8",
            )
            output_path.write_text("preserve-this-profile", encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "refuses to replace"):
                main(
                    [
                        "--request",
                        str(request_path),
                        "--private-keys",
                        str(keys_path),
                        "--output",
                        str(output_path),
                    ]
                )
            self.assertEqual("preserve-this-profile", output_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
