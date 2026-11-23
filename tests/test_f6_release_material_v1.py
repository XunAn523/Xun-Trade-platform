"""F6 release material must be exact before a detached signature is trusted."""

from __future__ import annotations

import unittest
from dataclasses import replace

from xuanling_tradecore.runtime import ReleaseManifestActivationErrorV1, ReleaseMaterialV1


def _digest(letter: str) -> str:
    return letter * 64


def _material() -> ReleaseMaterialV1:
    return ReleaseMaterialV1(
        artifact_hash=_digest("a"),
        emt_sdk_hash=_digest("b"),
        native_extension_hash=_digest("c"),
        elf_abi_hash=_digest("d"),
        systemd_unit_hash=_digest("e"),
        cgroup_policy_hash=_digest("f"),
        host_health_policy_hash=_digest("1"),
        journal_policy_hash=_digest("2"),
        control_root_policy_hash=_digest("3"),
        archive_policy_hash=_digest("4"),
        bundle_trust_policy_hash=_digest("5"),
        account_profile_hashes={"emt-sim-three-scope": _digest("6")},
    )


class ReleaseMaterialTests(unittest.TestCase):
    def test_builds_exact_unsigned_payload_and_rejects_asset_drift(self) -> None:
        material = _material()
        manifest = material.unsigned_manifest(
            manifest_id="f6-r0-sim-1",
            release_epoch=1,
            signer_id="release-signer-1",
            valid_from_wall_ns=1,
            valid_until_wall_ns=2,
        )
        self.assertEqual(manifest.signature, "unsigned-placeholder")
        material.require_matches(manifest)
        with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "native_extension_hash"):
            material.require_matches(replace(manifest, native_extension_hash=_digest("7")))

    def test_requires_nonempty_hashed_profile_mapping(self) -> None:
        with self.assertRaisesRegex(ReleaseManifestActivationErrorV1, "at least one"):
            ReleaseMaterialV1(
                artifact_hash=_digest("a"),
                emt_sdk_hash=_digest("b"),
                native_extension_hash=_digest("c"),
                elf_abi_hash=_digest("d"),
                systemd_unit_hash=_digest("e"),
                cgroup_policy_hash=_digest("f"),
                host_health_policy_hash=_digest("1"),
                journal_policy_hash=_digest("2"),
                control_root_policy_hash=_digest("3"),
                archive_policy_hash=_digest("4"),
                bundle_trust_policy_hash=_digest("5"),
                account_profile_hashes={},
            )


if __name__ == "__main__":
    unittest.main()
