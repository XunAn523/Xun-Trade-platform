"""Signed R0 release activation with durable anti-rollback state.

This module is deliberately outside the execution hot path.  A release is
verified before an EMT session can be created; it neither imports an Adapter
nor opens a network connection.  The persisted high-watermark prevents a
previous artifact from being silently reintroduced after a restart.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping, Protocol, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ..domain import AccountLifecycle, BundleTrustPolicy, ReleaseState
from ..domain._validation import (
    DomainValidationError,
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)


class ReleaseManifestActivationErrorV1(RuntimeError):
    """A release cannot be verified, persisted, or safely activated."""


class ReleaseManifestKindV1(str, Enum):
    """A normal release or an audited, monotonic rollback release."""

    NORMAL = "NORMAL"
    ROLLBACK_RELEASE = "ROLLBACK_RELEASE"


@dataclass(frozen=True)
class SignedReleaseManifestV1:
    """Non-secret, signed identity for one EMT-only runtime release.

    All hashes identify already-built assets and policies.  The manifest never
    contains credentials, endpoints, account balances, or an Adapter handle.
    """

    manifest_id: str
    release_epoch: int
    release_kind: ReleaseManifestKindV1
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
    signer_id: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    signature: str
    rollback_from_artifact_hash: str | None = None
    rollback_approval_hash: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("manifest_id", self.manifest_id),
            ("signer_id", self.signer_id),
            ("signature", self.signature),
        ):
            require_text(value, f"release manifest {name}", max_length=256)
        require_positive(self.release_epoch, "release manifest epoch")
        require_nonnegative(self.valid_from_wall_ns, "release manifest valid_from_wall_ns")
        require_nonnegative(self.valid_until_wall_ns, "release manifest valid_until_wall_ns")
        if self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise ReleaseManifestActivationErrorV1("release manifest validity interval is invalid")
        for name, value in (
            ("artifact_hash", self.artifact_hash),
            ("emt_sdk_hash", self.emt_sdk_hash),
            ("native_extension_hash", self.native_extension_hash),
            ("elf_abi_hash", self.elf_abi_hash),
            ("systemd_unit_hash", self.systemd_unit_hash),
            ("cgroup_policy_hash", self.cgroup_policy_hash),
            ("host_health_policy_hash", self.host_health_policy_hash),
            ("journal_policy_hash", self.journal_policy_hash),
            ("control_root_policy_hash", self.control_root_policy_hash),
            ("archive_policy_hash", self.archive_policy_hash),
            ("bundle_trust_policy_hash", self.bundle_trust_policy_hash),
        ):
            require_sha256(value, f"release manifest {name}")
        profiles = freeze_text_mapping(
            self.account_profile_hashes,
            "release manifest account_profile_hashes",
            value_max_length=64,
        )
        if not profiles:
            raise ReleaseManifestActivationErrorV1(
                "release manifest requires at least one account profile"
            )
        for account_id, profile_hash in profiles.items():
            require_text(account_id, "release manifest account profile account_id", max_length=128)
            require_sha256(profile_hash, "release manifest account profile hash")
        object.__setattr__(self, "account_profile_hashes", profiles)
        rollback_fields = (self.rollback_from_artifact_hash, self.rollback_approval_hash)
        if self.release_kind is ReleaseManifestKindV1.NORMAL:
            if any(value is not None for value in rollback_fields):
                raise ReleaseManifestActivationErrorV1(
                    "normal release must not carry rollback references"
                )
        elif self.release_kind is ReleaseManifestKindV1.ROLLBACK_RELEASE:
            if any(value is None for value in rollback_fields):
                raise ReleaseManifestActivationErrorV1(
                    "rollback release requires source artifact and approval hashes"
                )
            assert self.rollback_from_artifact_hash is not None
            assert self.rollback_approval_hash is not None
            require_sha256(
                self.rollback_from_artifact_hash,
                "release manifest rollback_from_artifact_hash",
            )
            require_sha256(self.rollback_approval_hash, "release manifest rollback_approval_hash")
        else:
            raise ReleaseManifestActivationErrorV1("release manifest kind is unsupported")

    def canonical_bytes(self) -> bytes:
        """Return the signature payload with a deterministic map order."""

        payload = {
            "account_profile_hashes": dict(sorted(self.account_profile_hashes.items())),
            "archive_policy_hash": self.archive_policy_hash,
            "artifact_hash": self.artifact_hash,
            "bundle_trust_policy_hash": self.bundle_trust_policy_hash,
            "cgroup_policy_hash": self.cgroup_policy_hash,
            "control_root_policy_hash": self.control_root_policy_hash,
            "elf_abi_hash": self.elf_abi_hash,
            "emt_sdk_hash": self.emt_sdk_hash,
            "host_health_policy_hash": self.host_health_policy_hash,
            "journal_policy_hash": self.journal_policy_hash,
            "manifest_id": self.manifest_id,
            "native_extension_hash": self.native_extension_hash,
            "release_epoch": self.release_epoch,
            "release_kind": self.release_kind.value,
            "rollback_approval_hash": self.rollback_approval_hash,
            "rollback_from_artifact_hash": self.rollback_from_artifact_hash,
            "signer_id": self.signer_id,
            "systemd_unit_hash": self.systemd_unit_hash,
            "valid_from_wall_ns": self.valid_from_wall_ns,
            "valid_until_wall_ns": self.valid_until_wall_ns,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @property
    def content_hash(self) -> str:
        """Return the immutable manifest identity used by ``ReleaseState``."""

        return sha256(self.canonical_bytes()).hexdigest()

    def as_dict(self) -> dict[str, object]:
        """Return the complete non-secret serialized release manifest."""

        return {
            "account_profile_hashes": dict(sorted(self.account_profile_hashes.items())),
            "archive_policy_hash": self.archive_policy_hash,
            "artifact_hash": self.artifact_hash,
            "bundle_trust_policy_hash": self.bundle_trust_policy_hash,
            "cgroup_policy_hash": self.cgroup_policy_hash,
            "control_root_policy_hash": self.control_root_policy_hash,
            "elf_abi_hash": self.elf_abi_hash,
            "emt_sdk_hash": self.emt_sdk_hash,
            "host_health_policy_hash": self.host_health_policy_hash,
            "journal_policy_hash": self.journal_policy_hash,
            "manifest_id": self.manifest_id,
            "native_extension_hash": self.native_extension_hash,
            "release_epoch": self.release_epoch,
            "release_kind": self.release_kind.value,
            "rollback_approval_hash": self.rollback_approval_hash,
            "rollback_from_artifact_hash": self.rollback_from_artifact_hash,
            "signature": self.signature,
            "signer_id": self.signer_id,
            "systemd_unit_hash": self.systemd_unit_hash,
            "valid_from_wall_ns": self.valid_from_wall_ns,
            "valid_until_wall_ns": self.valid_until_wall_ns,
        }


class ReleaseManifestSignatureVerifierV1(Protocol):
    """Offline verifier backed by the pre-installed release trust root."""

    def verify(self, *, signer_id: str, payload: bytes, signature: str) -> bool:
        """Verify the detached release-manifest signature."""
        ...


class Ed25519ReleaseManifestVerifierV1:
    """Offline Ed25519 verifier over the exact canonical release payload."""

    def __init__(self, trusted_public_keys: Mapping[str, bytes]) -> None:
        if not trusted_public_keys:
            raise ReleaseManifestActivationErrorV1(
                "release manifest verifier requires trusted public keys"
            )
        keys: dict[str, bytes] = {}
        for signer_id, public_key in trusted_public_keys.items():
            require_text(signer_id, "release manifest signer_id", max_length=256)
            if len(public_key) != 32:
                raise ReleaseManifestActivationErrorV1(
                    "release manifest Ed25519 public key must contain 32 bytes"
                )
            keys[signer_id] = public_key
        self._trusted_public_keys = keys

    def verify(self, *, signer_id: str, payload: bytes, signature: str) -> bool:
        try:
            public_key = self._trusted_public_keys[signer_id]
            raw_signature = base64.b64decode(signature, validate=True)
            Ed25519PublicKey.from_public_bytes(public_key).verify(raw_signature, payload)
        except (KeyError, ValueError, binascii.Error, InvalidSignature):
            return False
        return True


_MANIFEST_FIELDS_V1 = frozenset(
    {
        "account_profile_hashes",
        "archive_policy_hash",
        "artifact_hash",
        "bundle_trust_policy_hash",
        "cgroup_policy_hash",
        "control_root_policy_hash",
        "elf_abi_hash",
        "emt_sdk_hash",
        "host_health_policy_hash",
        "journal_policy_hash",
        "manifest_id",
        "native_extension_hash",
        "release_epoch",
        "release_kind",
        "rollback_approval_hash",
        "rollback_from_artifact_hash",
        "signature",
        "signer_id",
        "systemd_unit_hash",
        "valid_from_wall_ns",
        "valid_until_wall_ns",
    }
)


def load_signed_release_manifest_v1_json(payload: str) -> SignedReleaseManifestV1:
    """Load an exact, non-secret F6 release manifest JSON document."""

    try:
        raw = cast(object, json.loads(payload))
    except json.JSONDecodeError as exc:
        raise ReleaseManifestActivationErrorV1("release manifest is not JSON") from exc
    if not isinstance(raw, dict):
        raise ReleaseManifestActivationErrorV1("release manifest must be an object")
    document = cast(Mapping[str, object], raw)
    if frozenset(document) != _MANIFEST_FIELDS_V1:
        raise ReleaseManifestActivationErrorV1("release manifest fields differ from V1")
    try:
        release_kind = ReleaseManifestKindV1(_text_field(document, "release_kind"))
    except ValueError as exc:
        raise ReleaseManifestActivationErrorV1("release manifest kind is unsupported") from exc
    return SignedReleaseManifestV1(
        manifest_id=_text_field(document, "manifest_id"),
        release_epoch=_positive_int_field(document, "release_epoch"),
        release_kind=release_kind,
        artifact_hash=_text_field(document, "artifact_hash"),
        emt_sdk_hash=_text_field(document, "emt_sdk_hash"),
        native_extension_hash=_text_field(document, "native_extension_hash"),
        elf_abi_hash=_text_field(document, "elf_abi_hash"),
        systemd_unit_hash=_text_field(document, "systemd_unit_hash"),
        cgroup_policy_hash=_text_field(document, "cgroup_policy_hash"),
        host_health_policy_hash=_text_field(document, "host_health_policy_hash"),
        journal_policy_hash=_text_field(document, "journal_policy_hash"),
        control_root_policy_hash=_text_field(document, "control_root_policy_hash"),
        archive_policy_hash=_text_field(document, "archive_policy_hash"),
        bundle_trust_policy_hash=_text_field(document, "bundle_trust_policy_hash"),
        account_profile_hashes=_hash_mapping_field(document, "account_profile_hashes"),
        signer_id=_text_field(document, "signer_id"),
        valid_from_wall_ns=_nonnegative_int_field(document, "valid_from_wall_ns"),
        valid_until_wall_ns=_nonnegative_int_field(document, "valid_until_wall_ns"),
        signature=_text_field(document, "signature"),
        rollback_from_artifact_hash=_optional_text_field(document, "rollback_from_artifact_hash"),
        rollback_approval_hash=_optional_text_field(document, "rollback_approval_hash"),
    )


def _text_field(document: Mapping[str, object], field_name: str) -> str:
    value = document[field_name]
    if not isinstance(value, str):
        raise ReleaseManifestActivationErrorV1(f"release manifest {field_name} must be text")
    return value


def _optional_text_field(document: Mapping[str, object], field_name: str) -> str | None:
    value = document[field_name]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ReleaseManifestActivationErrorV1(
            f"release manifest {field_name} must be text or null"
        )
    return value


def _positive_int_field(document: Mapping[str, object], field_name: str) -> int:
    value = document[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReleaseManifestActivationErrorV1(
            f"release manifest {field_name} must be a positive integer"
        )
    return value


def _nonnegative_int_field(document: Mapping[str, object], field_name: str) -> int:
    value = document[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReleaseManifestActivationErrorV1(
            f"release manifest {field_name} must be a nonnegative integer"
        )
    return value


def _hash_mapping_field(document: Mapping[str, object], field_name: str) -> Mapping[str, str]:
    value = document[field_name]
    if not isinstance(value, dict):
        raise ReleaseManifestActivationErrorV1(f"release manifest {field_name} must be an object")
    result: dict[str, str] = {}
    for key, item in cast(Mapping[object, object], value).items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ReleaseManifestActivationErrorV1(
                f"release manifest {field_name} must map text to text"
            )
        result[key] = item
    return result


@dataclass(frozen=True)
class ReleaseBootstrapApprovalV1:
    """An auditable explicit approval needed when no durable release state exists."""

    approval_id: str
    manifest_hash: str
    approver_id: str
    approved_at_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.approval_id, "release bootstrap approval_id", max_length=256)
        require_sha256(self.manifest_hash, "release bootstrap manifest_hash")
        require_text(self.approver_id, "release bootstrap approver_id", max_length=256)
        require_nonnegative(self.approved_at_wall_ns, "release bootstrap approved_at_wall_ns")


class ReleaseStateStoreV1(Protocol):
    """Durable global high-watermark storage for release activation."""

    def load(self) -> ReleaseState | None:
        """Return the most recently accepted release, if initialization is proven."""

    def commit(self, state: ReleaseState) -> None:
        """Durably commit the new high watermark before release assets are loaded."""


class FileReleaseStateStoreV1:
    """Atomically-replaced, fsync-backed local release-state file.

    This is called only while all accounts are DISARMED.  It deliberately
    rejects malformed state instead of treating it as a fresh installation.
    """

    _SCHEMA_VERSION = 1

    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> ReleaseState | None:
        if not self._path.exists():
            return None
        try:
            document = cast(object, json.loads(self._path.read_text(encoding="utf-8")))
            if not isinstance(document, dict):
                raise ValueError("release state is not an object")
            fields = cast(dict[str, object], document)
            if set(fields) != {
                "artifact_hash",
                "release_epoch",
                "release_manifest_hash",
                "schema_version",
            }:
                raise ValueError("release state fields differ")
            if fields["schema_version"] != self._SCHEMA_VERSION:
                raise ValueError("release state schema version differs")
            release_epoch = fields["release_epoch"]
            artifact_hash = fields["artifact_hash"]
            release_manifest_hash = fields["release_manifest_hash"]
            if (
                isinstance(release_epoch, bool)
                or not isinstance(release_epoch, int)
                or not isinstance(artifact_hash, str)
                or not isinstance(release_manifest_hash, str)
            ):
                raise ValueError("release state field types differ")
            return ReleaseState(
                release_epoch=release_epoch,
                artifact_hash=artifact_hash,
                release_manifest_hash=release_manifest_hash,
            )
        except (DomainValidationError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ReleaseManifestActivationErrorV1("release state is unreadable") from exc

    def commit(self, state: ReleaseState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            {
                "artifact_hash": state.artifact_hash,
                "release_epoch": state.release_epoch,
                "release_manifest_hash": state.release_manifest_hash,
                "schema_version": self._SCHEMA_VERSION,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        temporary_path: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="wb", prefix=".release-state-", dir=self._path.parent, delete=False
            ) as temporary:
                temporary.write(encoded)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, self._path)
            temporary_path = None
            if os.name != "nt":
                os.chmod(self._path, 0o600)
                self._fsync_parent_directory()
        except OSError as exc:
            raise ReleaseManifestActivationErrorV1("release state commit failed") from exc
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def _fsync_parent_directory(self) -> None:
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        try:
            descriptor = os.open(self._path.parent, os.O_RDONLY | directory_flag)
        except OSError as exc:
            raise ReleaseManifestActivationErrorV1(
                "release state parent directory cannot be synced"
            ) from exc
        try:
            os.fsync(descriptor)
        except OSError as exc:
            raise ReleaseManifestActivationErrorV1(
                "release state parent directory sync failed"
            ) from exc
        finally:
            os.close(descriptor)


class ReleaseActivationCoordinatorV1:
    """Activate only a trusted, monotonic release while every account is DISARMED."""

    def __init__(
        self,
        *,
        trust_policy: BundleTrustPolicy,
        verifier: ReleaseManifestSignatureVerifierV1,
        store: ReleaseStateStoreV1,
    ) -> None:
        self._trust_policy = trust_policy
        self._verifier = verifier
        self._store = store

    def activate(
        self,
        manifest: SignedReleaseManifestV1,
        *,
        account_lifecycles: Mapping[str, AccountLifecycle],
        now_wall_ns: int,
        bootstrap_approval: ReleaseBootstrapApprovalV1 | None = None,
    ) -> ReleaseState:
        """Verify then durably advance the release high watermark.

        Asset loading, SDK import, session creation, and order admission must
        occur *after* this method returns.  A same-epoch same-hash invocation
        is an idempotent restart; all other rollback attempts are rejected.
        """

        require_nonnegative(now_wall_ns, "release activation time")
        self._require_disarmed_account_set(manifest, account_lifecycles)
        self._require_trusted_signature(manifest, now_wall_ns)
        previous = self._store.load()
        manifest_hash = manifest.content_hash
        if previous is None:
            self._require_bootstrap_approval(manifest_hash, bootstrap_approval)
            if manifest.release_kind is ReleaseManifestKindV1.ROLLBACK_RELEASE:
                raise ReleaseManifestActivationErrorV1(
                    "rollback release requires an existing release state"
                )
            next_state = ReleaseState(
                release_epoch=manifest.release_epoch,
                artifact_hash=manifest.artifact_hash,
                release_manifest_hash=manifest_hash,
            )
            self._store.commit(next_state)
            return next_state
        if manifest.release_epoch < previous.release_epoch:
            raise ReleaseManifestActivationErrorV1("release epoch rollback is rejected")
        if manifest.release_epoch == previous.release_epoch:
            if manifest_hash != previous.release_manifest_hash:
                raise ReleaseManifestActivationErrorV1(
                    "same release epoch has a different manifest"
                )
            return previous
        if manifest.release_kind is ReleaseManifestKindV1.ROLLBACK_RELEASE:
            assert manifest.rollback_from_artifact_hash is not None
            if manifest.rollback_from_artifact_hash != previous.artifact_hash:
                raise ReleaseManifestActivationErrorV1(
                    "rollback release does not reference the active artifact"
                )
        next_state = ReleaseState(
            release_epoch=manifest.release_epoch,
            artifact_hash=manifest.artifact_hash,
            release_manifest_hash=manifest_hash,
        )
        self._store.commit(next_state)
        return next_state

    def _require_disarmed_account_set(
        self,
        manifest: SignedReleaseManifestV1,
        account_lifecycles: Mapping[str, AccountLifecycle],
    ) -> None:
        declared = set(manifest.account_profile_hashes)
        observed = set(account_lifecycles)
        if declared != observed:
            raise ReleaseManifestActivationErrorV1(
                "release manifest account set differs from runtime account set"
            )
        if any(state is not AccountLifecycle.DISARMED for state in account_lifecycles.values()):
            raise ReleaseManifestActivationErrorV1(
                "release activation requires every account to be DISARMED"
            )

    def _require_trusted_signature(
        self, manifest: SignedReleaseManifestV1, now_wall_ns: int
    ) -> None:
        if "release_manifest" not in self._trust_policy.allowed_bundle_types:
            raise ReleaseManifestActivationErrorV1(
                "trust policy does not authorize release manifests"
            )
        if manifest.signer_id not in self._trust_policy.trusted_signer_ids:
            raise ReleaseManifestActivationErrorV1("release manifest signer is not trusted")
        if manifest.signer_id in self._trust_policy.revoked_signer_ids:
            raise ReleaseManifestActivationErrorV1("release manifest signer is revoked")
        if not manifest.valid_from_wall_ns <= now_wall_ns < manifest.valid_until_wall_ns:
            raise ReleaseManifestActivationErrorV1(
                "release manifest is outside its validity interval"
            )
        if not self._verifier.verify(
            signer_id=manifest.signer_id,
            payload=manifest.canonical_bytes(),
            signature=manifest.signature,
        ):
            raise ReleaseManifestActivationErrorV1("release manifest signature verification failed")

    @staticmethod
    def _require_bootstrap_approval(
        manifest_hash: str, approval: ReleaseBootstrapApprovalV1 | None
    ) -> None:
        if approval is None or approval.manifest_hash != manifest_hash:
            raise ReleaseManifestActivationErrorV1(
                "initial release activation requires matching bootstrap approval"
            )
