"""Signed, SDK-free R1 namespace release activation.

The release document is deliberately read and verified before an EMT SDK is
imported, a native session is opened, or an order id can be allocated.  It
contains only public control-plane material: signed range leases, references,
hashes and validity.  Private keys remain with the offline release signer.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping, cast

from ...domain import TradingDayEpoch
from ...domain._validation import require_nonnegative, require_positive, require_text
from .namespace_recovery import (
    EmtNamespaceRecoveryError,
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    OrderClientIdNamespaceDirectoryV1,
    OrderClientIdNamespaceLeaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
)


class NamespaceReleaseActivationErrorV1(RuntimeError):
    """A namespace release is missing, altered, stale, or unsafe to activate."""


def _require_hash(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise NamespaceReleaseActivationErrorV1(f"{name} must be a lowercase SHA-256 hash")


@dataclass(frozen=True)
class SignedNamespaceReleaseV1:
    """The complete public namespace map for one EMT account/day/client scope."""

    release_id: str
    release_epoch: int
    release_manifest_hash: str
    trust_anchor_hash: str
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    own: SignedOrderClientIdNamespaceLeaseV1
    foreign: tuple[SignedOrderClientIdNamespaceLeaseV1, ...]
    signer_id: str
    certificate_reference: str
    revocation_reference: str
    signature_b64: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.release_id, "release_id"),
            (self.signer_id, "signer_id"),
            (self.certificate_reference, "certificate_reference"),
            (self.revocation_reference, "revocation_reference"),
            (self.signature_b64, "signature"),
        ):
            require_text(value, f"namespace release {name}")
        require_positive(self.release_epoch, "namespace release epoch")
        require_nonnegative(self.valid_from_wall_ns, "namespace release valid_from_wall_ns")
        require_positive(self.valid_until_wall_ns, "namespace release valid_until_wall_ns")
        if self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release validity interval is invalid"
            )
        _require_hash(self.release_manifest_hash, "namespace release release_manifest_hash")
        _require_hash(self.trust_anchor_hash, "namespace release trust_anchor_hash")
        foreign = tuple(self.foreign)
        if any(item.lease.scope != self.own.lease.scope for item in foreign):
            raise NamespaceReleaseActivationErrorV1("namespace release mixes EMT scopes")
        object.__setattr__(self, "foreign", foreign)

    @property
    def scope(self) -> EmtOrderClientNamespaceScope:
        return self.own.lease.scope

    def active_at(self, wall_ns: int) -> bool:
        return self.valid_from_wall_ns <= wall_ns < self.valid_until_wall_ns

    def canonical_payload(self) -> bytes:
        return json.dumps(
            {
                "certificate_reference": self.certificate_reference,
                "foreign": [_signed_lease_document(item) for item in self.foreign],
                "own": _signed_lease_document(self.own),
                "release_epoch": self.release_epoch,
                "release_id": self.release_id,
                "release_manifest_hash": self.release_manifest_hash,
                "revocation_reference": self.revocation_reference,
                "schema_version": 1,
                "signer_id": self.signer_id,
                "trust_anchor_hash": self.trust_anchor_hash,
                "valid_from_wall_ns": self.valid_from_wall_ns,
                "valid_until_wall_ns": self.valid_until_wall_ns,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")

    @property
    def content_hash(self) -> str:
        return sha256(
            self.canonical_payload() + b"\x00" + self.signature_b64.encode("ascii")
        ).hexdigest()

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "release_id": self.release_id,
            "release_epoch": self.release_epoch,
            "release_manifest_hash": self.release_manifest_hash,
            "trust_anchor_hash": self.trust_anchor_hash,
            "valid_from_wall_ns": self.valid_from_wall_ns,
            "valid_until_wall_ns": self.valid_until_wall_ns,
            "own": _signed_lease_document(self.own),
            "foreign": [_signed_lease_document(item) for item in self.foreign],
            "signer_id": self.signer_id,
            "certificate_reference": self.certificate_reference,
            "revocation_reference": self.revocation_reference,
            "signature_b64": self.signature_b64,
        }


@dataclass(frozen=True)
class NamespaceReleaseBootstrapApprovalV1:
    """Explicit audit record required when a scope has no local state yet."""

    approval_id: str
    release_hash: str
    approver_id: str
    approved_at_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.approval_id, "namespace bootstrap approval_id")
        _require_hash(self.release_hash, "namespace bootstrap release_hash")
        require_text(self.approver_id, "namespace bootstrap approver_id")
        require_nonnegative(self.approved_at_wall_ns, "namespace bootstrap approved_at_wall_ns")


@dataclass(frozen=True)
class NamespaceReleaseStateV1:
    """Durable non-secret activation state for exactly one R1 scope."""

    scope: EmtOrderClientNamespaceScope
    release_epoch: int
    release_hash: str
    namespace_manifest_hash: str
    release_manifest_hash: str
    trust_anchor_hash: str
    high_watermark: int

    def __post_init__(self) -> None:
        require_positive(self.release_epoch, "namespace release state epoch")
        for value, name in (
            (self.release_hash, "release_hash"),
            (self.namespace_manifest_hash, "namespace_manifest_hash"),
            (self.release_manifest_hash, "release_manifest_hash"),
            (self.trust_anchor_hash, "trust_anchor_hash"),
        ):
            _require_hash(value, f"namespace release state {name}")
        if type(self.high_watermark) is not int or self.high_watermark < 0:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release state high_watermark is invalid"
            )


class FileNamespaceReleaseStateStoreV1:
    """Atomically replaced state file, called outside the native order lane."""

    _SCHEMA_VERSION = 1

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def load(self) -> NamespaceReleaseStateV1 | None:
        if not self._path.exists():
            return None
        try:
            raw = cast(object, json.loads(self._path.read_text(encoding="utf-8")))
            if not isinstance(raw, dict):
                raise ValueError("namespace release state is not an object")
            document = cast(Mapping[str, object], raw)
            if set(document) != {
                "high_watermark",
                "namespace_manifest_hash",
                "release_epoch",
                "release_hash",
                "release_manifest_hash",
                "schema_version",
                "scope",
                "trust_anchor_hash",
            }:
                raise ValueError("namespace release state fields differ")
            if document["schema_version"] != self._SCHEMA_VERSION:
                raise ValueError("namespace release state schema differs")
            scope = _scope_from_document(
                _object(document["scope"], "namespace release state scope")
            )
            return NamespaceReleaseStateV1(
                scope=scope,
                release_epoch=_positive_int(document, "release_epoch"),
                release_hash=_text(document, "release_hash"),
                namespace_manifest_hash=_text(document, "namespace_manifest_hash"),
                release_manifest_hash=_text(document, "release_manifest_hash"),
                trust_anchor_hash=_text(document, "trust_anchor_hash"),
                high_watermark=_nonnegative_int(document, "high_watermark"),
            )
        except (
            OSError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            EmtNamespaceRecoveryError,
        ) as exc:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release state is unreadable"
            ) from exc

    def commit(self, state: NamespaceReleaseStateV1) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            {
                "schema_version": self._SCHEMA_VERSION,
                "scope": _scope_document(state.scope),
                "release_epoch": state.release_epoch,
                "release_hash": state.release_hash,
                "namespace_manifest_hash": state.namespace_manifest_hash,
                "release_manifest_hash": state.release_manifest_hash,
                "trust_anchor_hash": state.trust_anchor_hash,
                "high_watermark": state.high_watermark,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        temporary_path: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="wb", prefix=".namespace-release-", dir=self._path.parent, delete=False
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
            raise NamespaceReleaseActivationErrorV1(
                "namespace release state commit failed"
            ) from exc
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def _fsync_parent_directory(self) -> None:
        descriptor = os.open(self._path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        except OSError as exc:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release state parent sync failed"
            ) from exc
        finally:
            os.close(descriptor)


@dataclass(frozen=True)
class ActivatedNamespaceReleaseV1:
    """The only directory an R1 session composition may pass to the adapter."""

    directory: OrderClientIdNamespaceDirectoryV1
    state: NamespaceReleaseStateV1


class NamespaceReleaseActivationCoordinatorV1:
    """Verify the signed namespace publication before SDK/session/order setup."""

    def __init__(
        self,
        *,
        verifier: EmtOrderClientNamespaceVerifier,
        state_store: FileNamespaceReleaseStateStoreV1,
    ) -> None:
        self._verifier = verifier
        self._state_store = state_store

    def activate(
        self,
        release: SignedNamespaceReleaseV1,
        *,
        expected_scope: EmtOrderClientNamespaceScope,
        expected_release_manifest_hash: str,
        expected_trust_anchor_hash: str,
        now_wall_ns: int,
        bootstrap_approval: NamespaceReleaseBootstrapApprovalV1 | None = None,
    ) -> ActivatedNamespaceReleaseV1:
        """Verify and persist a monotonic publication while still DISARMED.

        This method performs control-plane file I/O and must finish before an
        SDK import/session creation/order allocator becomes reachable.  It is
        never invoked in C0--C7.
        """

        require_nonnegative(now_wall_ns, "namespace release activation time")
        _require_hash(expected_release_manifest_hash, "expected release manifest hash")
        _require_hash(expected_trust_anchor_hash, "expected trust anchor hash")
        if release.scope != expected_scope:
            raise NamespaceReleaseActivationErrorV1("namespace release has another EMT scope")
        if release.release_manifest_hash != expected_release_manifest_hash:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release binds another runtime release"
            )
        if release.trust_anchor_hash != expected_trust_anchor_hash:
            raise NamespaceReleaseActivationErrorV1("namespace release binds another trust anchor")
        if not release.active_at(now_wall_ns):
            raise NamespaceReleaseActivationErrorV1(
                "namespace release is outside validity interval"
            )
        try:
            self._verifier.verify_detached(
                signer_id=release.signer_id,
                signature_b64=release.signature_b64,
                payload=release.canonical_payload(),
            )
            directory = OrderClientIdNamespaceDirectoryV1(
                own=release.own,
                foreign=release.foreign,
                verifier=self._verifier,
                wall_ns=now_wall_ns,
            )
        except EmtNamespaceRecoveryError as exc:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release verification failed"
            ) from exc
        if directory.manifest_hash != _directory_hash(release):
            raise NamespaceReleaseActivationErrorV1(
                "namespace release directory hash is inconsistent"
            )

        previous = self._state_store.load()
        release_hash = release.content_hash
        if previous is None:
            if bootstrap_approval is None or bootstrap_approval.release_hash != release_hash:
                raise NamespaceReleaseActivationErrorV1(
                    "initial namespace release activation requires matching bootstrap approval"
                )
            state = NamespaceReleaseStateV1(
                scope=directory.scope,
                release_epoch=release.release_epoch,
                release_hash=release_hash,
                namespace_manifest_hash=directory.manifest_hash,
                release_manifest_hash=release.release_manifest_hash,
                trust_anchor_hash=release.trust_anchor_hash,
                high_watermark=directory.own.range_start - 1,
            )
            self._state_store.commit(state)
            return ActivatedNamespaceReleaseV1(directory, state)
        if previous.scope != directory.scope:
            raise NamespaceReleaseActivationErrorV1("namespace release state has another scope")
        if release.release_epoch < previous.release_epoch:
            raise NamespaceReleaseActivationErrorV1("namespace release epoch rollback is rejected")
        if (
            release.release_epoch == previous.release_epoch
            and release_hash != previous.release_hash
        ):
            raise NamespaceReleaseActivationErrorV1(
                "same namespace release epoch has a different release"
            )
        if previous.high_watermark > directory.own.range_end:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release range would roll back the recorded high watermark"
            )
        if release.release_epoch == previous.release_epoch:
            return ActivatedNamespaceReleaseV1(directory, previous)
        # A later publication must never reclaim an id that an earlier
        # publication may already have handed to EMT.  Query rows carry only
        # ``order_client_id``; they do not carry a namespace-release epoch.
        # Reusing the recorded prefix would therefore make an old venue order
        # indistinguishable from a new local route during R2 reconciliation.
        #
        # This is deliberately a control-plane gate, before SDK/session
        # setup.  ``high_watermark`` advances only through the durable
        # checkpoint, so its prefix is the minimum history that a successor
        # must retire from its own lease.  The successor's signed directory
        # must classify that prefix as foreign (or otherwise fail directory
        # validation); it cannot be reissued as ``OWN``.
        if directory.own.range_start <= previous.high_watermark:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release successor reuses the recorded own id prefix"
            )
        state = NamespaceReleaseStateV1(
            scope=directory.scope,
            release_epoch=release.release_epoch,
            release_hash=release_hash,
            namespace_manifest_hash=directory.manifest_hash,
            release_manifest_hash=release.release_manifest_hash,
            trust_anchor_hash=release.trust_anchor_hash,
            high_watermark=max(previous.high_watermark, directory.own.range_start - 1),
        )
        self._state_store.commit(state)
        return ActivatedNamespaceReleaseV1(directory, state)

    def checkpoint_high_watermark(
        self, activated: ActivatedNamespaceReleaseV1, *, high_watermark: int
    ) -> ActivatedNamespaceReleaseV1:
        """Durably advance a recovered allocation watermark outside the hot path.

        Journal recovery remains the per-order authority.  This checkpoint is
        an anti-rollback control-plane checkpoint made after a recovery or an
        operator-controlled rollover, never per C0--C7 order.
        """

        current = self._state_store.load()
        if current is None or current != activated.state:
            raise NamespaceReleaseActivationErrorV1("namespace release state changed unexpectedly")
        own = activated.directory.own
        if (
            type(high_watermark) is not int
            or not own.range_start - 1 <= high_watermark <= own.range_end
        ):
            raise NamespaceReleaseActivationErrorV1(
                "namespace release high watermark is outside lease"
            )
        if high_watermark < current.high_watermark:
            raise NamespaceReleaseActivationErrorV1(
                "namespace release high watermark rollback is rejected"
            )
        updated = NamespaceReleaseStateV1(
            scope=current.scope,
            release_epoch=current.release_epoch,
            release_hash=current.release_hash,
            namespace_manifest_hash=current.namespace_manifest_hash,
            release_manifest_hash=current.release_manifest_hash,
            trust_anchor_hash=current.trust_anchor_hash,
            high_watermark=high_watermark,
        )
        self._state_store.commit(updated)
        return ActivatedNamespaceReleaseV1(activated.directory, updated)


def namespace_trust_anchor_hash_v1(
    trusted_public_keys: Mapping[str, bytes], *, revoked_signer_ids: frozenset[str]
) -> str:
    """Return the public-key/revocation identity bound into a release file."""

    if not trusted_public_keys:
        raise NamespaceReleaseActivationErrorV1("namespace trust anchor requires a public key")
    rows: list[dict[str, object]] = []
    for signer_id, raw_key in sorted(trusted_public_keys.items()):
        require_text(signer_id, "namespace trust anchor signer_id")
        if len(raw_key) != 32:
            raise NamespaceReleaseActivationErrorV1("namespace trust anchor key is invalid")
        rows.append({"signer_id": signer_id, "public_key_hex": raw_key.hex()})
    encoded = json.dumps(
        {"public_keys": rows, "revoked_signer_ids": sorted(revoked_signer_ids)},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return sha256(encoded).hexdigest()


def load_signed_namespace_release_v1_json(payload: str) -> SignedNamespaceReleaseV1:
    """Load an exact public R1 namespace release document."""

    try:
        raw = cast(object, json.loads(payload))
    except json.JSONDecodeError as exc:
        raise NamespaceReleaseActivationErrorV1("namespace release is not JSON") from exc
    if not isinstance(raw, dict):
        raise NamespaceReleaseActivationErrorV1("namespace release is not an object")
    document = cast(Mapping[str, object], raw)
    if set(document) != {
        "certificate_reference",
        "foreign",
        "own",
        "release_epoch",
        "release_id",
        "release_manifest_hash",
        "revocation_reference",
        "schema_version",
        "signature_b64",
        "signer_id",
        "trust_anchor_hash",
        "valid_from_wall_ns",
        "valid_until_wall_ns",
    }:
        raise NamespaceReleaseActivationErrorV1("namespace release fields differ from V1")
    if document["schema_version"] != 1:
        raise NamespaceReleaseActivationErrorV1("namespace release schema is unsupported")
    foreign_raw = document["foreign"]
    if not isinstance(foreign_raw, list):
        raise NamespaceReleaseActivationErrorV1("namespace release foreign must be a list")
    foreign = cast(list[object], foreign_raw)
    return SignedNamespaceReleaseV1(
        release_id=_text(document, "release_id"),
        release_epoch=_positive_int(document, "release_epoch"),
        release_manifest_hash=_text(document, "release_manifest_hash"),
        trust_anchor_hash=_text(document, "trust_anchor_hash"),
        valid_from_wall_ns=_nonnegative_int(document, "valid_from_wall_ns"),
        valid_until_wall_ns=_positive_int(document, "valid_until_wall_ns"),
        own=_signed_lease_from_document(_object(document["own"], "namespace release own")),
        foreign=tuple(
            _signed_lease_from_document(_object(item, "namespace release foreign"))
            for item in foreign
        ),
        signer_id=_text(document, "signer_id"),
        certificate_reference=_text(document, "certificate_reference"),
        revocation_reference=_text(document, "revocation_reference"),
        signature_b64=_text(document, "signature_b64"),
    )


def _signed_lease_document(signed: SignedOrderClientIdNamespaceLeaseV1) -> dict[str, object]:
    lease = signed.lease
    return {
        "lease": {
            "scope": _scope_document(lease.scope),
            "namespace_id": lease.namespace_id,
            "owner_system_id": lease.owner_system_id,
            "adapter_instance_id": lease.adapter_instance_id,
            "range_start": lease.range_start,
            "range_end": lease.range_end,
            "manifest_version": lease.manifest_version,
            "valid_from_wall_ns": lease.valid_from_wall_ns,
            "valid_until_wall_ns": lease.valid_until_wall_ns,
            "release_manifest_hash": lease.release_manifest_hash,
            "signer_id": lease.signer_id,
        },
        "certificate_reference": signed.certificate_reference,
        "revocation_reference": signed.revocation_reference,
        "signature_b64": signed.signature_b64,
    }


def _signed_lease_from_document(
    document: Mapping[str, object],
) -> SignedOrderClientIdNamespaceLeaseV1:
    if set(document) != {"lease", "certificate_reference", "revocation_reference", "signature_b64"}:
        raise NamespaceReleaseActivationErrorV1("namespace signed lease fields differ")
    lease_document = _object(document["lease"], "namespace lease")
    if set(lease_document) != {
        "scope",
        "namespace_id",
        "owner_system_id",
        "adapter_instance_id",
        "range_start",
        "range_end",
        "manifest_version",
        "valid_from_wall_ns",
        "valid_until_wall_ns",
        "release_manifest_hash",
        "signer_id",
    }:
        raise NamespaceReleaseActivationErrorV1("namespace lease fields differ")
    return SignedOrderClientIdNamespaceLeaseV1(
        lease=OrderClientIdNamespaceLeaseV1(
            scope=_scope_from_document(_object(lease_document["scope"], "namespace lease scope")),
            namespace_id=_text(lease_document, "namespace_id"),
            owner_system_id=_text(lease_document, "owner_system_id"),
            adapter_instance_id=_text(lease_document, "adapter_instance_id"),
            range_start=_positive_int(lease_document, "range_start"),
            range_end=_positive_int(lease_document, "range_end"),
            manifest_version=_positive_int(lease_document, "manifest_version"),
            valid_from_wall_ns=_nonnegative_int(lease_document, "valid_from_wall_ns"),
            valid_until_wall_ns=_positive_int(lease_document, "valid_until_wall_ns"),
            release_manifest_hash=_text(lease_document, "release_manifest_hash"),
            signer_id=_text(lease_document, "signer_id"),
        ),
        certificate_reference=_text(document, "certificate_reference"),
        revocation_reference=_text(document, "revocation_reference"),
        signature_b64=_text(document, "signature_b64"),
    )


def _directory_hash(release: SignedNamespaceReleaseV1) -> str:
    hashes = sorted(item.manifest_hash for item in (release.own,) + release.foreign)
    return sha256(b"\x00".join(item.encode("ascii") for item in hashes)).hexdigest()


def _scope_document(scope: EmtOrderClientNamespaceScope) -> dict[str, object]:
    return {
        "account_id": scope.account_id,
        "trading_day": scope.trading_day.value,
        "emt_client_id": scope.emt_client_id,
    }


def _scope_from_document(document: Mapping[str, object]) -> EmtOrderClientNamespaceScope:
    if set(document) != {"account_id", "trading_day", "emt_client_id"}:
        raise ValueError("namespace scope fields differ")
    return EmtOrderClientNamespaceScope(
        account_id=_text(document, "account_id"),
        trading_day=TradingDayEpoch(_text(document, "trading_day")),
        emt_client_id=_positive_or_zero_int(document, "emt_client_id"),
    )


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} is not an object")
    return cast(Mapping[str, object], value)


def _text(document: Mapping[str, object], name: str) -> str:
    value = document[name]
    if not isinstance(value, str):
        raise ValueError(f"{name} is not text")
    return value


def _positive_int(document: Mapping[str, object], name: str) -> int:
    value = document[name]
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} is not positive")
    return value


def _positive_or_zero_int(document: Mapping[str, object], name: str) -> int:
    value = document[name]
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} is not nonnegative")
    return value


def _nonnegative_int(document: Mapping[str, object], name: str) -> int:
    return _positive_or_zero_int(document, name)
