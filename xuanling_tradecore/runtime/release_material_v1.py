"""Hash-bound material for the non-hot-path F6 R0 release manifest.

The collector that obtains these hashes runs before an EMT session exists.  It
is intentionally separate from the execution runtime: a bad host or an
unexpected asset can only prevent release activation, never delay C0--C7.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..domain._validation import freeze_text_mapping, require_sha256, require_text
from .release_manifest_v1 import (
    ReleaseManifestActivationErrorV1,
    ReleaseManifestKindV1,
    SignedReleaseManifestV1,
)


@dataclass(frozen=True)
class ReleaseMaterialV1:
    """All host and release identities which a signed F6 manifest must bind."""

    artifact_hash: str
    emt_sdk_hash: str
    native_extension_hash: str
    elf_abi_hash: str
    systemd_unit_hash: str
    cgroup_policy_hash: str
    host_health_policy_hash: str
    journal_policy_hash: str
    control_root_policy_hash: str
    archive_policy_hash: str
    bundle_trust_policy_hash: str
    account_profile_hashes: Mapping[str, str]

    def __post_init__(self) -> None:
        for field_name in (
            "artifact_hash",
            "emt_sdk_hash",
            "native_extension_hash",
            "elf_abi_hash",
            "systemd_unit_hash",
            "cgroup_policy_hash",
            "host_health_policy_hash",
            "journal_policy_hash",
            "control_root_policy_hash",
            "archive_policy_hash",
            "bundle_trust_policy_hash",
        ):
            require_sha256(getattr(self, field_name), f"release material {field_name}")
        frozen_profiles = freeze_text_mapping(
            self.account_profile_hashes, "release material profiles"
        )
        if not frozen_profiles:
            raise ReleaseManifestActivationErrorV1(
                "release material requires at least one account profile hash"
            )
        for label, digest in frozen_profiles.items():
            require_text(label, "release material profile label", max_length=256)
            require_sha256(digest, "release material account profile hash")
        object.__setattr__(self, "account_profile_hashes", frozen_profiles)

    def unsigned_manifest(
        self,
        *,
        manifest_id: str,
        release_epoch: int,
        signer_id: str,
        valid_from_wall_ns: int,
        valid_until_wall_ns: int,
        release_kind: ReleaseManifestKindV1 = ReleaseManifestKindV1.NORMAL,
        rollback_from_artifact_hash: str | None = None,
        rollback_approval_hash: str | None = None,
    ) -> SignedReleaseManifestV1:
        """Build the exact canonical payload for offline signing.

        The temporary signature is only a parser placeholder.  Callers must
        remove it when serializing a signing request and replace it with a
        detached signature before verification or activation.
        """

        return SignedReleaseManifestV1(
            manifest_id=manifest_id,
            release_epoch=release_epoch,
            release_kind=release_kind,
            artifact_hash=self.artifact_hash,
            emt_sdk_hash=self.emt_sdk_hash,
            native_extension_hash=self.native_extension_hash,
            elf_abi_hash=self.elf_abi_hash,
            systemd_unit_hash=self.systemd_unit_hash,
            cgroup_policy_hash=self.cgroup_policy_hash,
            host_health_policy_hash=self.host_health_policy_hash,
            journal_policy_hash=self.journal_policy_hash,
            control_root_policy_hash=self.control_root_policy_hash,
            archive_policy_hash=self.archive_policy_hash,
            bundle_trust_policy_hash=self.bundle_trust_policy_hash,
            account_profile_hashes=self.account_profile_hashes,
            signer_id=signer_id,
            valid_from_wall_ns=valid_from_wall_ns,
            valid_until_wall_ns=valid_until_wall_ns,
            signature="unsigned-placeholder",
            rollback_from_artifact_hash=rollback_from_artifact_hash,
            rollback_approval_hash=rollback_approval_hash,
        )

    def require_matches(self, manifest: SignedReleaseManifestV1) -> None:
        """Reject any signature-valid manifest for different measured material."""

        mismatches = [
            field_name
            for field_name in (
                "artifact_hash",
                "emt_sdk_hash",
                "native_extension_hash",
                "elf_abi_hash",
                "systemd_unit_hash",
                "cgroup_policy_hash",
                "host_health_policy_hash",
                "journal_policy_hash",
                "control_root_policy_hash",
                "archive_policy_hash",
                "bundle_trust_policy_hash",
            )
            if getattr(manifest, field_name) != getattr(self, field_name)
        ]
        if dict(manifest.account_profile_hashes) != dict(self.account_profile_hashes):
            mismatches.append("account_profile_hashes")
        if mismatches:
            raise ReleaseManifestActivationErrorV1(
                "release manifest material mismatch: " + ",".join(mismatches)
            )
