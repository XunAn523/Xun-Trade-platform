"""Acceptance tests for the redacted external-trust DISARMED profile verifier."""

from __future__ import annotations

import base64
import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from tools.performance.bootstrap_emt_disarmed_profile import build_bootstrap_artifacts
from tools.performance.build_emt_disarmed_profile import main as build_profile
from tools.performance.materialize_emt_disarmed_profile_request import (
    materialize_disarmed_profile_request,
)
from tools.performance.verify_emt_disarmed_profile import (
    main,
    verify_three_scope_disarmed_profile,
)
from xuanling_tradecore.adapters.emt import (
    EmtDisarmedProfileError,
    adapter_client_scope_ownership_from_dict,
    load_emt_disarmed_profile,
)
from xuanling_tradecore.adapters.emt.scope_ownership import canonical_scope_ownership_payload


class VerifyEmtDisarmedProfileTests(unittest.TestCase):
    def _profile_paths(self, root: Path) -> tuple[Path, Path, Path]:
        self.now_wall_ns = time.time_ns()
        private_path = root / "private.json"
        public_path = root / "public.json"
        revocations_path = root / "revocations.json"
        template_path = root / "template.json"
        request_path = root / "request.json"
        accounts_path = root / "accounts.json"
        profile_path = root / "profile.json"
        build_bootstrap_artifacts(
            signer_id="emt-sim-disarmed-signer-v1",
            profile_id="emt-sim-three-scope-disarmed-v1",
            private_keys_path=private_path,
            public_key_path=public_path,
            revocations_path=revocations_path,
            request_path=template_path,
            now_wall_ns=self.now_wall_ns - 1,
        )
        accounts_path.write_text(
            json.dumps(
                {
                    "gateway": {"host": "127.0.0.1", "port": 19088},
                    "ordinary": {"account": "ordinary-account", "password": "ordinary-password"},
                    "credit": {"account": "credit-account", "password": "credit-password"},
                    "option": {"account": "option-account", "password": "option-password"},
                }
            ),
            encoding="utf-8",
        )
        materialize_disarmed_profile_request(
            template_path=template_path,
            accounts_path=accounts_path,
            output_path=request_path,
            now_wall_ns=self.now_wall_ns,
        )
        self.assertEqual(
            0,
            build_profile(
                [
                    "--request",
                    str(request_path),
                    "--private-keys",
                    str(private_path),
                    "--output",
                    str(profile_path),
                ]
            ),
        )
        return profile_path, public_path, revocations_path

    def test_reports_a_verified_three_scope_profile_without_sensitive_values(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile_path, public_path, revocations_path = self._profile_paths(root)
            report_path = root / "acceptance.json"
            self.assertEqual(
                0,
                main(
                    [
                        "--profile",
                        str(profile_path),
                        "--public-key",
                        str(public_path),
                        "--revocations",
                        str(revocations_path),
                        "--output",
                        str(report_path),
                    ]
                ),
            )
            rendered = report_path.read_text(encoding="utf-8")
            report = json.loads(rendered)
            self.assertEqual("EMT_THREE_SCOPE_DISARMED_PROFILE_ACCEPTED", report["result"])
            self.assertEqual(3, report["scope_count"])
            self.assertFalse(report["activation_permitted"])
            self.assertNotIn("ordinary-account", rendered)
            self.assertNotIn("credit-account", rendered)
            self.assertNotIn("option-account", rendered)
            self.assertNotIn("ordinary-password", rendered)
            self.assertNotIn("private", rendered.lower())

    def test_rekeyed_self_verified_profile_is_rejected_by_external_public_key(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile_path, public_path, revocations_path = self._profile_paths(root)
            raw = json.loads(profile_path.read_text(encoding="utf-8"))
            attacker = Ed25519PrivateKey.generate()
            raw["trusted_public_keys_b64"]["emt-sim-disarmed-signer-v1"] = base64.b64encode(
                attacker.public_key().public_bytes(
                    serialization.Encoding.Raw,
                    serialization.PublicFormat.Raw,
                )
            ).decode("ascii")
            for signed in raw["signed_scope_ownerships"]:
                signed_ownership = signed["ownership"]
                parsed = adapter_client_scope_ownership_from_dict(signed_ownership)
                signature = attacker.sign(
                    canonical_scope_ownership_payload(
                        parsed,
                        certificate_reference=signed["certificate_reference"],
                        revocation_reference=signed["revocation_reference"],
                    )
                )
                signed["signature_b64"] = base64.b64encode(signature).decode("ascii")
            profile_path.write_text(json.dumps(raw), encoding="utf-8")
            load_emt_disarmed_profile(profile_path)
            with self.assertRaisesRegex(EmtDisarmedProfileError, "do not match external trust"):
                verify_three_scope_disarmed_profile(
                    profile_path=profile_path,
                    public_key_path=public_path,
                    revocations_path=revocations_path,
                    now_wall_ns=self.now_wall_ns,
                )

    def test_revoked_signer_or_existing_report_is_rejected(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile_path, public_path, revocations_path = self._profile_paths(root)
            revocations = json.loads(revocations_path.read_text(encoding="utf-8"))
            revocations["revoked_signer_ids"] = ["emt-sim-disarmed-signer-v1"]
            revocations_path.write_text(json.dumps(revocations), encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "signer is revoked"):
                verify_three_scope_disarmed_profile(
                    profile_path=profile_path,
                    public_key_path=public_path,
                    revocations_path=revocations_path,
                    now_wall_ns=self.now_wall_ns,
                )
            report_path = root / "existing.json"
            report_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "refuses to replace"):
                main(
                    [
                        "--profile",
                        str(profile_path),
                        "--public-key",
                        str(public_path),
                        "--revocations",
                        str(revocations_path),
                        "--output",
                        str(report_path),
                    ]
                )


if __name__ == "__main__":
    unittest.main()
