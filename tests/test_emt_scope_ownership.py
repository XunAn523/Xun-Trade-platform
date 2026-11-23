"""Tests for cryptographically bound EMT controlled-profile ownership."""

from __future__ import annotations

import base64
import unittest
from dataclasses import replace

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtEd25519ScopeOwnershipVerifier,
    EmtScopeOwnershipSignatureError,
    EmtSignedScopeOwnership,
    canonical_scope_ownership_payload,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import TradingDayEpoch


class _NoOpFactory:
    def open(self, *, callback_ingress):
        del callback_ingress
        raise AssertionError("scope verification must complete before a session opens")


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _signed_claim(private_key: Ed25519PrivateKey) -> EmtSignedScopeOwnership:
    ownership = _mapping().manifest.scope_ownership
    assert ownership is not None
    certificate_reference = "test://emt-control-plane/signer-1-public-key"
    revocation_reference = "test://emt-control-plane/revocations-v1"
    signature = private_key.sign(
        canonical_scope_ownership_payload(
            ownership,
            certificate_reference=certificate_reference,
            revocation_reference=revocation_reference,
        )
    )
    return EmtSignedScopeOwnership(
        ownership=ownership,
        certificate_reference=certificate_reference,
        revocation_reference=revocation_reference,
        signature_b64=base64.b64encode(signature).decode("ascii"),
    )


class EmtScopeOwnershipTests(unittest.TestCase):
    def test_exact_signed_ownership_binds_a_controlled_profile(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        signed = _signed_claim(private_key)
        verifier = EmtEd25519ScopeOwnershipVerifier({"tradecore-release-signer-1": public_key})

        adapter = EmtAdapterProfile.create_controlled_read_only_adapter(
            _mapping(),
            session_factory=_NoOpFactory(),
            expected_api_version="2.27.0",
            expected_trading_day=TradingDayEpoch("2026-09-03"),
            session_epoch=1,
            signed_scope_ownership=signed,
            scope_ownership_verifier=verifier,
        )

        self.assertEqual("emt-sim-instance-1", adapter.adapter_instance_id)

    def test_tampered_claim_or_untrusted_signer_fails_before_session_factory(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        signed = _signed_claim(private_key)
        verifier = EmtEd25519ScopeOwnershipVerifier({"tradecore-release-signer-1": public_key})
        with self.assertRaisesRegex(EmtScopeOwnershipSignatureError, "does not verify"):
            verifier.verify(replace(signed, revocation_reference="test://changed"))
        with self.assertRaisesRegex(EmtScopeOwnershipSignatureError, "not trusted"):
            EmtEd25519ScopeOwnershipVerifier({"another-signer": public_key}).verify(signed)


if __name__ == "__main__":
    unittest.main()
