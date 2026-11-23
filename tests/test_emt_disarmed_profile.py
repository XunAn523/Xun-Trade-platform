"""Tests for signed local EMT profiles that can never leave DISARMED."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters import AccountTradingCapabilities, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtDisarmedProfileError,
    EmtEd25519ScopeOwnershipVerifier,
    load_builtin_emt_legacy_sim_manifest,
    load_emt_disarmed_profile,
    sign_disarmed_profile,
)
from xuanling_tradecore.domain import TradingDayEpoch


class _NoOpFactory:
    def open(self, *, callback_ingress):
        del callback_ingress
        raise AssertionError("profile selection must happen before a session opens")


class EmtDisarmedProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        manifest = load_builtin_emt_legacy_sim_manifest()
        assert manifest.scope_ownership is not None
        self.ownership = manifest.scope_ownership
        self.private_key = Ed25519PrivateKey.generate()

    def _profile(self):
        signer_id = self.ownership.signer_id
        return sign_disarmed_profile(
            profile_id="emt-sim-disarmed-v1",
            ownerships=(self.ownership,),
            private_keys={signer_id: self.private_key},
            certificate_references={signer_id: "test://emt-signer-public-key"},
            revocation_references={signer_id: "test://emt-signer-revocations"},
        )

    def _external_verifier(self) -> EmtEd25519ScopeOwnershipVerifier:
        return EmtEd25519ScopeOwnershipVerifier(
            {
                self.ownership.signer_id: self.private_key.public_key().public_bytes(
                    serialization.Encoding.Raw,
                    serialization.PublicFormat.Raw,
                )
            }
        )

    def test_signed_profile_round_trips_without_private_key(self) -> None:
        profile = self._profile()
        rendered = profile.as_dict()
        self.assertFalse(rendered["approval"]["approved"])
        self.assertEqual("DISARMED", rendered["approval"]["lifecycle"])
        self.assertNotIn("private", json.dumps(rendered).lower())

        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "profile.json"
            path.write_text(json.dumps(rendered), encoding="utf-8")
            loaded = load_emt_disarmed_profile(path)

        self.assertEqual(profile.profile_id, loaded.profile_id)
        self.assertEqual(profile.signed_scope_ownerships, loaded.signed_scope_ownerships)

    def test_approved_or_tampered_profile_fails_closed(self) -> None:
        profile = self._profile()
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "profile.json"
            rendered = profile.as_dict()
            rendered["approval"]["approved"] = True
            path.write_text(json.dumps(rendered), encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "DISARMED only"):
                load_emt_disarmed_profile(path)

            rendered = profile.as_dict()
            rendered["signed_scope_ownerships"][0]["signature_b64"] = "A" * 88
            path.write_text(json.dumps(rendered), encoding="utf-8")
            with self.assertRaisesRegex(EmtDisarmedProfileError, "invalid signed"):
                load_emt_disarmed_profile(path)

    def test_duplicate_account_client_scope_is_rejected(self) -> None:
        duplicate = replace(self.ownership, ownership_id="another-ownership-id")
        signer_id = self.ownership.signer_id
        with self.assertRaisesRegex(EmtDisarmedProfileError, "repeats"):
            sign_disarmed_profile(
                profile_id="emt-sim-disarmed-v1",
                ownerships=(self.ownership, duplicate),
                private_keys={signer_id: self.private_key},
                certificate_references={signer_id: "test://emt-signer-public-key"},
                revocation_references={signer_id: "test://emt-signer-revocations"},
            )

    def test_profile_selects_only_its_exact_mapping_claim(self) -> None:
        profile = self._profile()
        manifest = load_builtin_emt_legacy_sim_manifest()
        mapping = EmtAdapterProfile(manifest).activate(
            runtime=RuntimeVenueProfile(
                fingerprint=manifest.fingerprint,
                capabilities=manifest.capabilities,
                account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
                scope_ownership=manifest.scope_ownership,
                route_token_binding=manifest.route_token_binding,
                trusted_scope_signer_ids=frozenset({self.ownership.signer_id}),
                route_token_probe_succeeded=True,
            ),
            activated_at_unix_s=0,
        )
        adapter = EmtAdapterProfile.create_controlled_read_only_adapter_from_disarmed_profile(
            mapping,
            disarmed_profile=profile,
            session_factory=_NoOpFactory(),
            expected_api_version="2.27.0",
            expected_trading_day=TradingDayEpoch("2026-09-03"),
            session_epoch=1,
            scope_ownership_verifier=self._external_verifier(),
        )
        self.assertEqual("emt-sim-instance-1", adapter.adapter_instance_id)

    def test_profile_cannot_make_its_own_runtime_signer_trusted(self) -> None:
        profile = self._profile()
        wrong_private_key = Ed25519PrivateKey.generate()
        wrong_public_key = wrong_private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        manifest = load_builtin_emt_legacy_sim_manifest()
        mapping = EmtAdapterProfile(manifest).activate(
            runtime=RuntimeVenueProfile(
                fingerprint=manifest.fingerprint,
                capabilities=manifest.capabilities,
                account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
                scope_ownership=manifest.scope_ownership,
                route_token_binding=manifest.route_token_binding,
                trusted_scope_signer_ids=frozenset({self.ownership.signer_id}),
                route_token_probe_succeeded=True,
            ),
            activated_at_unix_s=0,
        )
        with self.assertRaisesRegex(Exception, "does not verify"):
            EmtAdapterProfile.create_controlled_read_only_adapter_from_disarmed_profile(
                mapping,
                disarmed_profile=profile,
                session_factory=_NoOpFactory(),
                expected_api_version="2.27.0",
                expected_trading_day=TradingDayEpoch("2026-09-03"),
                session_epoch=1,
                scope_ownership_verifier=EmtEd25519ScopeOwnershipVerifier(
                    {self.ownership.signer_id: wrong_public_key}
                ),
            )


if __name__ == "__main__":
    unittest.main()
