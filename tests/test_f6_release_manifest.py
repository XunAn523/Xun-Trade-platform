"""F6 release-manifest activation and anti-rollback contracts."""

from __future__ import annotations

import base64
import json
import os
import stat
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.domain import AccountLifecycle, BundleTrustPolicy
from xuanling_tradecore.runtime import (
    Ed25519ReleaseManifestVerifierV1,
    FileReleaseStateStoreV1,
    ReleaseActivationCoordinatorV1,
    ReleaseBootstrapApprovalV1,
    ReleaseManifestActivationErrorV1,
    ReleaseManifestKindV1,
    SignedReleaseManifestV1,
    load_signed_release_manifest_v1_json,
)

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64


class _Verifier:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.calls: list[tuple[str, bytes, str]] = []

    def verify(self, *, signer_id: str, payload: bytes, signature: str) -> bool:
        self.calls.append((signer_id, payload, signature))
        return self.accepted


def _manifest(
    *,
    epoch: int = 1,
    artifact_hash: str = HASH_A,
    signature: str = "detached-signature",
    kind: ReleaseManifestKindV1 = ReleaseManifestKindV1.NORMAL,
    rollback_from_artifact_hash: str | None = None,
    rollback_approval_hash: str | None = None,
) -> SignedReleaseManifestV1:
    return SignedReleaseManifestV1(
        manifest_id=f"emt-r0-{epoch}",
        release_epoch=epoch,
        release_kind=kind,
        artifact_hash=artifact_hash,
        emt_sdk_hash=HASH_B,
        native_extension_hash=HASH_C,
        elf_abi_hash=HASH_D,
        systemd_unit_hash=HASH_E,
        cgroup_policy_hash=HASH_F,
        host_health_policy_hash=HASH_A,
        journal_policy_hash=HASH_B,
        control_root_policy_hash=HASH_C,
        archive_policy_hash=HASH_D,
        bundle_trust_policy_hash=HASH_E,
        account_profile_hashes={"emt-account-a": HASH_F, "emt-account-b": HASH_A},
        signer_id="release-signer-1",
        valid_from_wall_ns=10,
        valid_until_wall_ns=100,
        signature=signature,
        rollback_from_artifact_hash=rollback_from_artifact_hash,
        rollback_approval_hash=rollback_approval_hash,
    )


def _policy() -> BundleTrustPolicy:
    return BundleTrustPolicy(
        policy_id="release-policy-1",
        policy_epoch=1,
        trusted_signer_ids=frozenset({"release-signer-1"}),
        revoked_signer_ids=frozenset(),
        allowed_bundle_types=frozenset({"release_manifest"}),
    )


def _lifecycles() -> dict[str, AccountLifecycle]:
    return {
        "emt-account-a": AccountLifecycle.DISARMED,
        "emt-account-b": AccountLifecycle.DISARMED,
    }


def _approval(manifest: SignedReleaseManifestV1) -> ReleaseBootstrapApprovalV1:
    return ReleaseBootstrapApprovalV1(
        approval_id="bootstrap-approval-1",
        manifest_hash=manifest.content_hash,
        approver_id="release-operator-1",
        approved_at_wall_ns=20,
    )


class ReleaseManifestActivationTests(unittest.TestCase):
    def _coordinator(self, path: Path, verifier: _Verifier) -> ReleaseActivationCoordinatorV1:
        return ReleaseActivationCoordinatorV1(
            trust_policy=_policy(), verifier=verifier, store=FileReleaseStateStoreV1(path)
        )

    def test_initial_release_requires_matching_approval_and_persists_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "control" / "release-state-v1.json"
            manifest = _manifest()
            verifier = _Verifier()
            coordinator = self._coordinator(path, verifier)
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "bootstrap approval"):
                coordinator.activate(manifest, account_lifecycles=_lifecycles(), now_wall_ns=20)
            state = coordinator.activate(
                manifest,
                account_lifecycles=_lifecycles(),
                now_wall_ns=20,
                bootstrap_approval=_approval(manifest),
            )
            self.assertEqual(1, state.release_epoch)
            self.assertEqual(manifest.content_hash, state.release_manifest_hash)
            self.assertEqual(state, FileReleaseStateStoreV1(path).load())
            self.assertEqual(2, len(verifier.calls))
            if os.name != "nt":
                self.assertEqual(0, stat.S_IMODE(path.stat().st_mode) & 0o077)

    def test_same_epoch_is_idempotent_but_content_or_lower_epoch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "release-state-v1.json"
            manifest = _manifest()
            coordinator = self._coordinator(path, _Verifier())
            first = coordinator.activate(
                manifest,
                account_lifecycles=_lifecycles(),
                now_wall_ns=20,
                bootstrap_approval=_approval(manifest),
            )
            self.assertEqual(
                first,
                coordinator.activate(manifest, account_lifecycles=_lifecycles(), now_wall_ns=21),
            )
            altered = replace(manifest, native_extension_hash=HASH_E)
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "different manifest"):
                coordinator.activate(altered, account_lifecycles=_lifecycles(), now_wall_ns=21)
            upgraded = _manifest(epoch=2, artifact_hash=HASH_B)
            coordinator.activate(upgraded, account_lifecycles=_lifecycles(), now_wall_ns=21)
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "epoch rollback"):
                coordinator.activate(manifest, account_lifecycles=_lifecycles(), now_wall_ns=22)

    def test_higher_epoch_and_audited_rollback_are_monotonic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "release-state-v1.json"
            coordinator = self._coordinator(path, _Verifier())
            initial = _manifest(epoch=1, artifact_hash=HASH_A)
            coordinator.activate(
                initial,
                account_lifecycles=_lifecycles(),
                now_wall_ns=20,
                bootstrap_approval=_approval(initial),
            )
            upgraded = _manifest(epoch=2, artifact_hash=HASH_B)
            coordinator.activate(upgraded, account_lifecycles=_lifecycles(), now_wall_ns=21)
            rollback = _manifest(
                epoch=3,
                artifact_hash=HASH_A,
                kind=ReleaseManifestKindV1.ROLLBACK_RELEASE,
                rollback_from_artifact_hash=HASH_B,
                rollback_approval_hash=HASH_C,
            )
            state = coordinator.activate(rollback, account_lifecycles=_lifecycles(), now_wall_ns=22)
            self.assertEqual((3, HASH_A), (state.release_epoch, state.artifact_hash))
            wrong_source = replace(rollback, release_epoch=4, rollback_from_artifact_hash=HASH_B)
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "active artifact"):
                coordinator.activate(wrong_source, account_lifecycles=_lifecycles(), now_wall_ns=23)

    def test_rejects_live_account_bad_signature_and_account_set_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "release-state-v1.json"
            manifest = _manifest()
            coordinator = self._coordinator(path, _Verifier())
            live = _lifecycles()
            live["emt-account-a"] = AccountLifecycle.READY
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "every account"):
                coordinator.activate(
                    manifest,
                    account_lifecycles=live,
                    now_wall_ns=20,
                    bootstrap_approval=_approval(manifest),
                )
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "account set"):
                coordinator.activate(
                    manifest,
                    account_lifecycles={"emt-account-a": AccountLifecycle.DISARMED},
                    now_wall_ns=20,
                    bootstrap_approval=_approval(manifest),
                )
            failing = self._coordinator(path, _Verifier(accepted=False))
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "signature"):
                failing.activate(
                    manifest,
                    account_lifecycles=_lifecycles(),
                    now_wall_ns=20,
                    bootstrap_approval=_approval(manifest),
                )

    def test_file_store_rejects_malformed_state_instead_of_resetting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "release-state-v1.json"
            path.write_text('{"schema_version":1}', encoding="utf-8")
            with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "unreadable"):
                FileReleaseStateStoreV1(path).load()

    def test_manifest_enforces_explicit_rollback_shape(self) -> None:
        with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "must not carry"):
            _manifest(rollback_from_artifact_hash=HASH_A)
        with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "requires source"):
            _manifest(kind=ReleaseManifestKindV1.ROLLBACK_RELEASE)

    def test_ed25519_verifier_and_exact_json_loader_reject_tampering(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        unsigned = _manifest(signature="placeholder")
        signature = base64.b64encode(private_key.sign(unsigned.canonical_bytes())).decode("ascii")
        manifest = replace(unsigned, signature=signature)
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        verifier = Ed25519ReleaseManifestVerifierV1({manifest.signer_id: public_key})
        self.assertTrue(
            verifier.verify(
                signer_id=manifest.signer_id,
                payload=manifest.canonical_bytes(),
                signature=manifest.signature,
            )
        )
        loaded = load_signed_release_manifest_v1_json(json.dumps(manifest.as_dict()))
        self.assertEqual(manifest, loaded)
        tampered = replace(manifest, artifact_hash=HASH_B)
        self.assertFalse(
            verifier.verify(
                signer_id=tampered.signer_id,
                payload=tampered.canonical_bytes(),
                signature=tampered.signature,
            )
        )
        document = manifest.as_dict()
        document["unexpected"] = "reject"
        with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "fields differ"):
            load_signed_release_manifest_v1_json(json.dumps(document))


if __name__ == "__main__":
    unittest.main()
