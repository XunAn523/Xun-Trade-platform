"""M5 signed local decision-bundle activation with durable anti-rollback state.

The component intentionally has no network client: a production OpenFeature
provider is represented by an already verified local snapshot.  It can only be
activated while its account is DISARMED.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping, Protocol, cast

from ..domain import AccountLifecycle, BundleTrustPolicy
from ..domain._validation import (
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)


class BundleActivationErrorV1(RuntimeError):
    """A bundle cannot become active without a complete trusted lineage."""


@dataclass(frozen=True)
class SignedDecisionBundleV1:
    """One account-scoped, locally stored complete decision bundle."""

    bundle_id: str
    account_id: str
    bundle_epoch: int
    signer_id: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    mapping_hash: str
    capability_hash: str
    risk_rule_hash: str
    retention_hash: str
    feature_flags: Mapping[str, str]
    signature: str

    def __post_init__(self) -> None:
        for name, value in (
            ("bundle_id", self.bundle_id),
            ("account_id", self.account_id),
            ("signer_id", self.signer_id),
            ("signature", self.signature),
        ):
            require_text(value, f"bundle {name}", max_length=256)
        require_positive(self.bundle_epoch, "bundle epoch")
        require_nonnegative(self.valid_from_wall_ns, "bundle valid_from_wall_ns")
        require_nonnegative(self.valid_until_wall_ns, "bundle valid_until_wall_ns")
        if self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise BundleActivationErrorV1("bundle validity interval is invalid")
        for name, value in (
            ("mapping_hash", self.mapping_hash),
            ("capability_hash", self.capability_hash),
            ("risk_rule_hash", self.risk_rule_hash),
            ("retention_hash", self.retention_hash),
        ):
            require_sha256(value, f"bundle {name}")
        flags = freeze_text_mapping(self.feature_flags, "bundle feature_flags", max_items=64)
        for flag, value in flags.items():
            if value not in {"enabled", "disabled"}:
                raise BundleActivationErrorV1(
                    f"bundle feature flag {flag} must be enabled or disabled"
                )
        object.__setattr__(self, "feature_flags", flags)

    def canonical_bytes(self) -> bytes:
        payload = {
            "account_id": self.account_id,
            "bundle_epoch": self.bundle_epoch,
            "bundle_id": self.bundle_id,
            "capability_hash": self.capability_hash,
            "feature_flags": dict(sorted(self.feature_flags.items())),
            "mapping_hash": self.mapping_hash,
            "retention_hash": self.retention_hash,
            "risk_rule_hash": self.risk_rule_hash,
            "signer_id": self.signer_id,
            "valid_from_wall_ns": self.valid_from_wall_ns,
            "valid_until_wall_ns": self.valid_until_wall_ns,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @property
    def content_hash(self) -> str:
        return sha256(self.canonical_bytes()).hexdigest()


class BundleSignatureVerifierV1(Protocol):
    def verify(self, *, signer_id: str, payload: bytes, signature: str) -> bool:
        """Verify the detached signature through the configured offline trust chain."""

        ...


@dataclass(frozen=True)
class BundleActivationRecordV1:
    account_id: str
    bundle_epoch: int
    content_hash: str
    activated_at_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.account_id, "bundle activation account_id")
        require_positive(self.bundle_epoch, "bundle activation epoch")
        require_sha256(self.content_hash, "bundle activation content_hash")
        require_nonnegative(self.activated_at_wall_ns, "bundle activation time")


class BundleActivationStoreV1(Protocol):
    def load(self, account_id: str) -> BundleActivationRecordV1 | None:
        """Load the durable highest accepted bundle identity for one account."""

    def commit(self, record: BundleActivationRecordV1) -> None:
        """Persist a monotonic successful activation before it is exposed."""


class FileBundleActivationStoreV1:
    """Small atomically-replaced local state file for account bundle high watermarks."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self, account_id: str) -> BundleActivationRecordV1 | None:
        require_text(account_id, "bundle activation account_id")
        if not self._path.exists():
            return None
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            item = raw.get(account_id)
            if item is None:
                return None
            return BundleActivationRecordV1(
                account_id=account_id,
                bundle_epoch=item["bundle_epoch"],
                content_hash=item["content_hash"],
                activated_at_wall_ns=item["activated_at_wall_ns"],
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise BundleActivationErrorV1("bundle activation store is unreadable") from exc

    def commit(self, record: BundleActivationRecordV1) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        records: dict[str, dict[str, object]] = {}
        if self._path.exists():
            try:
                loaded = cast(object, json.loads(self._path.read_text(encoding="utf-8")))
                if not isinstance(loaded, dict):
                    raise ValueError("not an object")
                loaded_items = cast(dict[object, object], loaded)
                for account_id, item in loaded_items.items():
                    if not isinstance(account_id, str) or not isinstance(item, dict):
                        raise ValueError("invalid account bundle state")
                    item_fields = cast(dict[object, object], item)
                    parsed: dict[str, object] = {}
                    for field in ("activated_at_wall_ns", "bundle_epoch", "content_hash"):
                        if field not in item_fields:
                            raise ValueError("incomplete account bundle state")
                        parsed[field] = item_fields[field]
                    records[account_id] = parsed
            except (OSError, ValueError, TypeError) as exc:
                raise BundleActivationErrorV1("bundle activation store is unreadable") from exc
        records[record.account_id] = {
            "activated_at_wall_ns": record.activated_at_wall_ns,
            "bundle_epoch": record.bundle_epoch,
            "content_hash": record.content_hash,
        }
        encoded = json.dumps(records, sort_keys=True, separators=(",", ":")).encode("utf-8")
        try:
            with NamedTemporaryFile(
                mode="wb", prefix=".bundle-activation-", dir=self._path.parent, delete=False
            ) as temporary:
                temporary.write(encoded)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, self._path)
        except OSError as exc:
            raise BundleActivationErrorV1("bundle activation store commit failed") from exc


class LocalDecisionBundleProviderV1:
    """The local OpenFeature-like provider; unknown flags are fail-closed."""

    def __init__(self) -> None:
        self._active: dict[str, SignedDecisionBundleV1] = {}

    def install(self, bundle: SignedDecisionBundleV1) -> None:
        self._active[bundle.account_id] = bundle

    def active_bundle(self, account_id: str) -> SignedDecisionBundleV1:
        try:
            return self._active[account_id]
        except KeyError as exc:
            raise BundleActivationErrorV1("account has no active local decision bundle") from exc

    def enabled(self, *, account_id: str, flag: str) -> bool:
        require_text(flag, "feature flag")
        bundle = self.active_bundle(account_id)
        try:
            return bundle.feature_flags[flag] == "enabled"
        except KeyError as exc:
            raise BundleActivationErrorV1("unknown feature flag is fail-closed") from exc


class BundleActivationCoordinatorV1:
    """Verify and atomically activate only newer trusted bundles while DISARMED."""

    def __init__(
        self,
        *,
        trust_policy: BundleTrustPolicy,
        verifier: BundleSignatureVerifierV1,
        store: BundleActivationStoreV1,
        provider: LocalDecisionBundleProviderV1,
    ) -> None:
        self._policy = trust_policy
        self._verifier = verifier
        self._store = store
        self._provider = provider

    def activate(
        self,
        bundle: SignedDecisionBundleV1,
        *,
        account_lifecycle: AccountLifecycle,
        now_wall_ns: int,
    ) -> BundleActivationRecordV1:
        if account_lifecycle is not AccountLifecycle.DISARMED:
            raise BundleActivationErrorV1("bundle activation requires a DISARMED account")
        if now_wall_ns < 0:
            raise BundleActivationErrorV1("bundle activation time must be nonnegative")
        if "decision_bundle" not in self._policy.allowed_bundle_types:
            raise BundleActivationErrorV1("trust policy does not authorize decision bundles")
        if bundle.signer_id not in self._policy.trusted_signer_ids:
            raise BundleActivationErrorV1("bundle signer is not trusted")
        if bundle.signer_id in self._policy.revoked_signer_ids:
            raise BundleActivationErrorV1("bundle signer is revoked")
        if not bundle.valid_from_wall_ns <= now_wall_ns < bundle.valid_until_wall_ns:
            raise BundleActivationErrorV1("bundle is outside its validity interval")
        if not self._verifier.verify(
            signer_id=bundle.signer_id,
            payload=bundle.canonical_bytes(),
            signature=bundle.signature,
        ):
            raise BundleActivationErrorV1("bundle signature verification failed")
        previous = self._store.load(bundle.account_id)
        content_hash = bundle.content_hash
        if previous is not None:
            if bundle.bundle_epoch < previous.bundle_epoch:
                raise BundleActivationErrorV1("bundle epoch rollback is rejected")
            if bundle.bundle_epoch == previous.bundle_epoch:
                if previous.content_hash != content_hash:
                    raise BundleActivationErrorV1("same bundle epoch has different content")
                self._provider.install(bundle)
                return previous
        record = BundleActivationRecordV1(
            account_id=bundle.account_id,
            bundle_epoch=bundle.bundle_epoch,
            content_hash=content_hash,
            activated_at_wall_ns=now_wall_ns,
        )
        self._store.commit(record)
        self._provider.install(bundle)
        return record
