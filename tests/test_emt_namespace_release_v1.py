from __future__ import annotations

import base64
import json
import tempfile
import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseActivationErrorV1,
    NamespaceReleaseBootstrapApprovalV1,
    OrderClientIdNamespaceLeaseV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
    load_signed_namespace_release_v1_json,
    namespace_trust_anchor_hash_v1,
)
from xuanling_tradecore.domain import TradingDayEpoch

_NOW = 1_000_000
_DAY = TradingDayEpoch("2026-09-10")
_RUNTIME_RELEASE = sha256(b"runtime-release").hexdigest()


def _scope() -> EmtOrderClientNamespaceScope:
    return EmtOrderClientNamespaceScope("sim-account", _DAY, 101)


def _signed_lease(
    private_key: Ed25519PrivateKey,
    *,
    namespace_id: str,
    owner: str,
    start: int,
    end: int,
) -> SignedOrderClientIdNamespaceLeaseV1:
    unsigned = SignedOrderClientIdNamespaceLeaseV1(
        lease=OrderClientIdNamespaceLeaseV1(
            scope=_scope(),
            namespace_id=namespace_id,
            owner_system_id=owner,
            adapter_instance_id=f"adapter-{owner}",
            range_start=start,
            range_end=end,
            manifest_version=1,
            valid_from_wall_ns=0,
            valid_until_wall_ns=_NOW + 1,
            release_manifest_hash=_RUNTIME_RELEASE,
            signer_id="test-signer",
        ),
        certificate_reference="test-certificate",
        revocation_reference="test-revocation",
        signature_b64="AA==",
    )
    return replace(
        unsigned,
        signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
            "ascii"
        ),
    )


def _release(
    private_key: Ed25519PrivateKey,
    *,
    epoch: int = 1,
    release_hash: str = _RUNTIME_RELEASE,
    own_start: int = 1000,
    own_end: int = 1999,
    foreign_start: int = 2000,
    foreign_end: int = 2999,
) -> tuple[SignedNamespaceReleaseV1, dict[str, bytes]]:
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    keys = {"test-signer": public_key}
    unsigned = SignedNamespaceReleaseV1(
        release_id="r1-sim-release",
        release_epoch=epoch,
        release_manifest_hash=release_hash,
        trust_anchor_hash=namespace_trust_anchor_hash_v1(keys, revoked_signer_ids=frozenset()),
        valid_from_wall_ns=0,
        valid_until_wall_ns=_NOW + 1,
        own=_signed_lease(
            private_key,
            namespace_id="tradecore-a",
            owner="tradecore-a",
            start=own_start,
            end=own_end,
        ),
        foreign=(
            _signed_lease(
                private_key,
                namespace_id="system-b",
                owner="system-b",
                start=foreign_start,
                end=foreign_end,
            ),
        ),
        signer_id="test-signer",
        certificate_reference="test-certificate",
        revocation_reference="test-revocation",
        signature_b64="AA==",
    )
    return (
        replace(
            unsigned,
            signature_b64=base64.b64encode(private_key.sign(unsigned.canonical_payload())).decode(
                "ascii"
            ),
        ),
        keys,
    )


class NamespaceReleaseTests(unittest.TestCase):
    def _coordinator(
        self, path: Path, keys: dict[str, bytes]
    ) -> NamespaceReleaseActivationCoordinatorV1:
        return NamespaceReleaseActivationCoordinatorV1(
            verifier=EmtOrderClientNamespaceVerifier(keys, revoked_signer_ids=frozenset()),
            state_store=FileNamespaceReleaseStateStoreV1(path),
        )

    def test_signed_release_activates_before_runtime_and_round_trips(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        release, keys = _release(private_key)
        loaded = load_signed_namespace_release_v1_json(json.dumps(release.as_dict()))
        self.assertEqual(release, loaded)
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = self._coordinator(Path(temporary) / "namespace-state.json", keys)
            activated = coordinator.activate(
                loaded,
                expected_scope=_scope(),
                expected_release_manifest_hash=_RUNTIME_RELEASE,
                expected_trust_anchor_hash=namespace_trust_anchor_hash_v1(
                    keys, revoked_signer_ids=frozenset()
                ),
                now_wall_ns=_NOW,
                bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
                    approval_id="bootstrap-1",
                    release_hash=loaded.content_hash,
                    approver_id="test-operator",
                    approved_at_wall_ns=_NOW,
                ),
            )
            self.assertEqual(1000, activated.directory.own.range_start)
            self.assertEqual(999, activated.state.high_watermark)
            restarted = coordinator.activate(
                loaded,
                expected_scope=_scope(),
                expected_release_manifest_hash=_RUNTIME_RELEASE,
                expected_trust_anchor_hash=namespace_trust_anchor_hash_v1(
                    keys, revoked_signer_ids=frozenset()
                ),
                now_wall_ns=_NOW,
            )
            self.assertEqual(activated.state, restarted.state)
            self.assertEqual(activated.directory.manifest_hash, restarted.directory.manifest_hash)

    def test_release_state_rejects_rollback_and_preserves_high_watermark(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        release, keys = _release(private_key)
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = self._coordinator(Path(temporary) / "namespace-state.json", keys)
            activated = coordinator.activate(
                release,
                expected_scope=_scope(),
                expected_release_manifest_hash=_RUNTIME_RELEASE,
                expected_trust_anchor_hash=release.trust_anchor_hash,
                now_wall_ns=_NOW,
                bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
                    approval_id="bootstrap-1",
                    release_hash=release.content_hash,
                    approver_id="test-operator",
                    approved_at_wall_ns=_NOW,
                ),
            )
            coordinator.checkpoint_high_watermark(activated, high_watermark=1100)
            newer, _keys = _release(private_key, epoch=2, own_start=1101, own_end=1999)
            activated_newer = coordinator.activate(
                newer,
                expected_scope=_scope(),
                expected_release_manifest_hash=_RUNTIME_RELEASE,
                expected_trust_anchor_hash=newer.trust_anchor_hash,
                now_wall_ns=_NOW,
            )
            self.assertEqual(1100, activated_newer.state.high_watermark)
            with self.assertRaisesRegex(NamespaceReleaseActivationErrorV1, "rollback"):
                coordinator.activate(
                    release,
                    expected_scope=_scope(),
                    expected_release_manifest_hash=_RUNTIME_RELEASE,
                    expected_trust_anchor_hash=release.trust_anchor_hash,
                    now_wall_ns=_NOW,
                )
            activated_newer = coordinator.checkpoint_high_watermark(
                activated_newer, high_watermark=1101
            )
            with self.assertRaisesRegex(NamespaceReleaseActivationErrorV1, "rollback"):
                coordinator.checkpoint_high_watermark(activated_newer, high_watermark=1100)

    def test_successor_release_rejects_reuse_of_recorded_client_id_prefix(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        release, keys = _release(private_key)
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = self._coordinator(Path(temporary) / "namespace-state.json", keys)
            activated = coordinator.activate(
                release,
                expected_scope=_scope(),
                expected_release_manifest_hash=_RUNTIME_RELEASE,
                expected_trust_anchor_hash=release.trust_anchor_hash,
                now_wall_ns=_NOW,
                bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
                    approval_id="bootstrap-1",
                    release_hash=release.content_hash,
                    approver_id="test-operator",
                    approved_at_wall_ns=_NOW,
                ),
            )
            coordinator.checkpoint_high_watermark(activated, high_watermark=1100)
            successor, _keys = _release(private_key, epoch=2)
            with self.assertRaisesRegex(
                NamespaceReleaseActivationErrorV1, "reuses the recorded own id prefix"
            ):
                coordinator.activate(
                    successor,
                    expected_scope=_scope(),
                    expected_release_manifest_hash=_RUNTIME_RELEASE,
                    expected_trust_anchor_hash=successor.trust_anchor_hash,
                    now_wall_ns=_NOW,
                )

    def test_release_rejects_wrong_runtime_hash_and_revoked_signer(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        release, keys = _release(private_key)
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = self._coordinator(Path(temporary) / "namespace-state.json", keys)
            with self.assertRaisesRegex(NamespaceReleaseActivationErrorV1, "runtime release"):
                coordinator.activate(
                    release,
                    expected_scope=_scope(),
                    expected_release_manifest_hash=sha256(b"other").hexdigest(),
                    expected_trust_anchor_hash=release.trust_anchor_hash,
                    now_wall_ns=_NOW,
                )
            revoked = NamespaceReleaseActivationCoordinatorV1(
                verifier=EmtOrderClientNamespaceVerifier(
                    keys, revoked_signer_ids=frozenset({"test-signer"})
                ),
                state_store=FileNamespaceReleaseStateStoreV1(Path(temporary) / "other.json"),
            )
            with self.assertRaisesRegex(NamespaceReleaseActivationErrorV1, "verification failed"):
                revoked.activate(
                    release,
                    expected_scope=_scope(),
                    expected_release_manifest_hash=_RUNTIME_RELEASE,
                    expected_trust_anchor_hash=release.trust_anchor_hash,
                    now_wall_ns=_NOW,
                )


if __name__ == "__main__":
    unittest.main()
