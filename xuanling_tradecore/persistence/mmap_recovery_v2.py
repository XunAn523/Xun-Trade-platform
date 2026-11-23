"""Fail-closed recovery and merge proof for ``MmapJournalProfileV1``.

This module is deliberately outside the native call and callback writer paths.
It reads already-committed fixed mmap frames, produces a sealed merge manifest,
and closes admission when any proof is incomplete.  It never sends, resends,
or cancels a broker instruction.  A clean recovery result remains ``DISARMED``
until an external reconciliation and a separately approved activation flow.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Final

from .mmap_journal_contracts import JournalAdmissionClosedError, JournalIntegrityError
from .mmap_pre_native_journal import (
    MmapJournalRecordKind,
    MmapJournalRecoveryScan,
    MmapJournalRecoveryStatus,
    MmapPreNativeJournal,
    MmapPreNativeRecord,
)

_SCHEMA_VERSION: Final = 2
_SHA256_LENGTH: Final = 64


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def _text(value: str, name: str, *, maximum: int = 128) -> str:
    if not value or not value.isascii() or len(value) > maximum:
        raise JournalIntegrityError(f"{name} is invalid")
    return value


def _hash(value: str, name: str) -> str:
    _text(value, name, maximum=_SHA256_LENGTH)
    if len(value) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise JournalIntegrityError(f"{name} must be a lower-case SHA-256")
    return value


class MmapRecoveryGateReasonV2(str, Enum):
    """One-way recovery conditions that must prevent native submission."""

    JOURNAL_INTEGRITY = "JOURNAL_INTEGRITY"
    ROUTE_CONFLICT = "ROUTE_CONFLICT"
    CALLBACK_GAP = "CALLBACK_GAP"
    CONTROL_ROOT = "CONTROL_ROOT"
    MANIFEST_DRIFT = "MANIFEST_DRIFT"
    CAPACITY = "CAPACITY"
    PROJECTOR_LAG = "PROJECTOR_LAG"
    WRITER_COLLISION = "WRITER_COLLISION"


class MmapReconciliationKindV2(str, Enum):
    """Safe classifications; none is an instruction to resend automatically."""

    PRE_NATIVE = "PRE_NATIVE"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    NATIVE_ACCEPTED = "NATIVE_ACCEPTED"
    CALLBACK_PENDING = "CALLBACK_PENDING"
    CANCEL_UNKNOWN = "CANCEL_UNKNOWN"
    CANCEL_ACCEPTED = "CANCEL_ACCEPTED"


@dataclass(frozen=True)
class MmapRecoveryGateEventV2:
    reason: MmapRecoveryGateReasonV2
    scope_id: str
    reference: str

    def __post_init__(self) -> None:
        _text(self.scope_id, "recovery gate scope")
        _text(self.reference, "recovery gate reference", maximum=256)


@dataclass(frozen=True)
class MmapRecoveryGateSnapshotV2:
    """Current gate state.  ``DISARMED`` is intentionally never ``READY``."""

    runtime_state: str
    events: tuple[MmapRecoveryGateEventV2, ...]
    manual_reconciliation_required: bool

    @property
    def admission_closed(self) -> bool:
        return bool(self.events) or self.runtime_state == "DISARMED"


class MmapRecoveryGateV2:
    """One-way gate used by recovery and non-hot-path health probes."""

    def __init__(self) -> None:
        self._events: list[MmapRecoveryGateEventV2] = []
        self._event_keys: set[tuple[MmapRecoveryGateReasonV2, str, str]] = set()

    def close(self, *, reason: MmapRecoveryGateReasonV2, scope_id: str, reference: str) -> None:
        event = MmapRecoveryGateEventV2(reason=reason, scope_id=scope_id, reference=reference)
        key = (event.reason, event.scope_id, event.reference)
        if key not in self._event_keys:
            self._event_keys.add(key)
            self._events.append(event)

    def snapshot(self) -> MmapRecoveryGateSnapshotV2:
        return MmapRecoveryGateSnapshotV2(
            runtime_state="DISARMED",
            events=tuple(self._events),
            manual_reconciliation_required=True,
        )

    def require_submit(self, *, scope_id: str) -> None:
        """Reject new side effects until a later, external activation flow.

        F1 deliberately does not implement such an activation flow.  This
        method is a callable fail-closed adapter seam, not an arming API.
        """

        _text(scope_id, "recovery submit scope")
        raise JournalAdmissionClosedError(
            "mmap recovery remains DISARMED; manual reconciliation and activation are required"
        )


class MmapWriterOwnershipRegistryV2:
    """Startup-time writer lease registry for scope/shard mmap ownership.

    It is intentionally not a locking primitive in C0--C7.  The journal's
    writer-thread check stays on the file writer itself; this registry makes a
    duplicate composed owner visible to host health before any lane starts.
    """

    def __init__(self, *, gate: MmapRecoveryGateV2) -> None:
        self._gate = gate
        self._owners: dict[tuple[str, str], str] = {}

    def claim(self, *, scope_id: str, shard_id: str, writer_identity: str) -> None:
        key = (_text(scope_id, "writer scope"), _text(shard_id, "writer shard"))
        owner = _text(writer_identity, "writer identity")
        existing = self._owners.get(key)
        if existing is None:
            self._owners[key] = owner
            return
        if existing != owner:
            self._gate.close(
                reason=MmapRecoveryGateReasonV2.WRITER_COLLISION,
                scope_id=key[0],
                reference=f"{key[1]}:writer-identity-collision",
            )
            raise JournalAdmissionClosedError("mmap scope/shard already has another writer owner")


@dataclass(frozen=True)
class MmapJournalShardInputV2:
    """One stable scope/shard mapping supplied to an off-path recovery run."""

    scope_id: str
    shard_id: str
    journal: MmapPreNativeJournal
    route_directory_digest: str
    writer_identity: str

    def __post_init__(self) -> None:
        _text(self.scope_id, "manifest scope")
        _text(self.shard_id, "manifest shard")
        _hash(self.route_directory_digest, "route directory digest")
        _text(self.writer_identity, "manifest writer identity")


@dataclass(frozen=True)
class MmapJournalShardManifestV2:
    """Payload-free trusted-prefix description of one mmap shard."""

    scope_id: str
    shard_id: str
    journal_epoch: int
    profile_hash: str
    frame_hash: str
    contiguous_position: int
    capacity: int
    route_directory_digest: str
    writer_identity_digest: str
    generated: bool
    sealed: bool

    def __post_init__(self) -> None:
        _text(self.scope_id, "manifest scope")
        _text(self.shard_id, "manifest shard")
        _hash(self.profile_hash, "manifest profile hash")
        _hash(self.frame_hash, "manifest frame hash")
        _hash(self.route_directory_digest, "manifest route directory digest")
        _hash(self.writer_identity_digest, "manifest writer identity digest")
        if self.journal_epoch < 1 or self.contiguous_position < 0 or self.capacity < 1:
            raise JournalIntegrityError("manifest Journal position or capacity is invalid")
        if self.contiguous_position > self.capacity:
            raise JournalIntegrityError("manifest Journal position exceeds capacity")

    @property
    def key(self) -> tuple[str, str]:
        return (self.scope_id, self.shard_id)

    def payload(self) -> dict[str, object]:
        return {
            "scope_id": self.scope_id,
            "shard_id": self.shard_id,
            "journal_epoch": self.journal_epoch,
            "profile_hash": self.profile_hash,
            "frame_hash": self.frame_hash,
            "contiguous_position": self.contiguous_position,
            "capacity": self.capacity,
            "route_directory_digest": self.route_directory_digest,
            "writer_identity_digest": self.writer_identity_digest,
            "generated": self.generated,
            "sealed": self.sealed,
        }


@dataclass(frozen=True)
class MergeManifestV2:
    """Canonical, hash-chained merge manifest for all recovered shards."""

    shards: tuple[MmapJournalShardManifestV2, ...]
    generated: bool
    sealed: bool
    previous_manifest_hash: str | None = None
    schema_version: int = _SCHEMA_VERSION
    manifest_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != _SCHEMA_VERSION:
            raise JournalIntegrityError("MergeManifestV2 schema version is invalid")
        if not self.shards:
            raise JournalIntegrityError("MergeManifestV2 requires at least one shard")
        ordered = tuple(sorted(self.shards, key=lambda item: item.key))
        if ordered != self.shards or len({item.key for item in ordered}) != len(ordered):
            raise JournalIntegrityError("MergeManifestV2 shards are not canonically ordered")
        if self.sealed and not self.generated:
            raise JournalIntegrityError("a sealed MergeManifestV2 must be generated")
        if self.previous_manifest_hash is not None:
            _hash(self.previous_manifest_hash, "previous manifest hash")
        object.__setattr__(
            self, "manifest_hash", sha256(_canonical_json(self.payload())).hexdigest()
        )

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "generated": self.generated,
            "sealed": self.sealed,
            "previous_manifest_hash": self.previous_manifest_hash,
            "shards": [item.payload() for item in self.shards],
        }

    def encoded(self) -> bytes:
        return _canonical_json({**self.payload(), "manifest_hash": self.manifest_hash})

    @classmethod
    def decode(cls, encoded: bytes) -> "MergeManifestV2":
        """Verify and decode a previously sealed on-disk manifest."""

        try:
            raw = json.loads(encoded.decode("ascii"))
            stored_hash = raw.pop("manifest_hash")
            shards = tuple(
                MmapJournalShardManifestV2(
                    scope_id=item["scope_id"],
                    shard_id=item["shard_id"],
                    journal_epoch=item["journal_epoch"],
                    profile_hash=item["profile_hash"],
                    frame_hash=item["frame_hash"],
                    contiguous_position=item["contiguous_position"],
                    capacity=item["capacity"],
                    route_directory_digest=item["route_directory_digest"],
                    writer_identity_digest=item["writer_identity_digest"],
                    generated=item["generated"],
                    sealed=item["sealed"],
                )
                for item in raw["shards"]
            )
            result = cls(
                shards=shards,
                generated=raw["generated"],
                sealed=raw["sealed"],
                previous_manifest_hash=raw["previous_manifest_hash"],
                schema_version=raw["schema_version"],
            )
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise JournalIntegrityError("MergeManifestV2 encoding is malformed") from exc
        if stored_hash != result.manifest_hash:
            raise JournalIntegrityError("MergeManifestV2 hash does not match its content")
        return result


class MergeManifestV2Store:
    """Atomically persist a sealed manifest outside all execution paths."""

    def __init__(self, *, path: Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def write(self, manifest: MergeManifestV2) -> None:
        if not manifest.generated or not manifest.sealed:
            raise JournalIntegrityError("only a generated and sealed manifest may be persisted")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_name(f".{self._path.name}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as file:
                file.write(manifest.encoded())
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self._path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def read(self) -> MergeManifestV2:
        try:
            return MergeManifestV2.decode(self._path.read_bytes())
        except OSError as exc:
            raise JournalIntegrityError("MergeManifestV2 cannot be read") from exc


@dataclass(frozen=True)
class MmapRecoveredRouteV2:
    """Minimal identity association reconstructed from committed mmap facts."""

    scope_id: str
    execution_id: str
    client_order_ref: str
    command_id: str
    payload_hash: str
    native_order_id: str | None
    shard_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.scope_id, "recovered route scope")
        _text(self.execution_id, "recovered execution id")
        _text(self.client_order_ref, "recovered client order reference")
        _text(self.command_id, "recovered command id")
        _hash(self.payload_hash, "recovered route payload hash")
        if self.native_order_id is not None:
            _text(self.native_order_id, "recovered native order id")
        if not self.shard_ids or tuple(sorted(set(self.shard_ids))) != self.shard_ids:
            raise JournalIntegrityError("recovered route shard IDs are invalid")


@dataclass(frozen=True)
class MmapReconciliationItemV2:
    kind: MmapReconciliationKindV2
    scope_id: str
    execution_id: str
    client_order_ref: str
    source_positions: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _text(self.scope_id, "reconciliation scope")
        _text(self.execution_id, "reconciliation execution")
        _text(self.client_order_ref, "reconciliation client reference")
        if not self.source_positions:
            raise JournalIntegrityError("reconciliation item requires source positions")


@dataclass(frozen=True)
class MmapJournalCapacityPolicyV2:
    """Explicit non-hot-path stop-new thresholds for a preallocated shard."""

    stop_new_at_percent: int = 80
    maximum_projector_lag_records: int = 1_000

    def __post_init__(self) -> None:
        if not 1 <= self.stop_new_at_percent <= 100:
            raise ValueError("Journal stop-new percent is invalid")
        if self.maximum_projector_lag_records < 0:
            raise ValueError("projector lag bound is invalid")


@dataclass(frozen=True)
class MmapRecoveryResultV2:
    manifest: MergeManifestV2
    scans: Mapping[tuple[str, str], MmapJournalRecoveryScan]
    recovered_routes: tuple[MmapRecoveredRouteV2, ...]
    reconciliation_items: tuple[MmapReconciliationItemV2, ...]
    gate: MmapRecoveryGateSnapshotV2

    def __post_init__(self) -> None:
        object.__setattr__(self, "scans", MappingProxyType(dict(self.scans)))


class MmapRecoveryCoordinatorV2:
    """Build a deterministic merge/reconciliation view without venue I/O."""

    def __init__(self, *, capacity_policy: MmapJournalCapacityPolicyV2 | None = None) -> None:
        self._capacity_policy = capacity_policy or MmapJournalCapacityPolicyV2()

    def recover(
        self,
        shards: Iterable[MmapJournalShardInputV2],
        *,
        previous_manifest: MergeManifestV2 | None = None,
        projector_positions: Mapping[tuple[str, str], int] | None = None,
        callback_stream_complete: bool = True,
        control_root_complete: bool = True,
    ) -> MmapRecoveryResultV2:
        """Scan, merge and classify facts; never reopen execution admission."""

        inputs = tuple(sorted(shards, key=lambda item: (item.scope_id, item.shard_id)))
        if not inputs or len({(item.scope_id, item.shard_id) for item in inputs}) != len(inputs):
            raise JournalIntegrityError("recovery scope/shard inputs are invalid")
        gate = MmapRecoveryGateV2()
        scans: dict[tuple[str, str], MmapJournalRecoveryScan] = {}
        shard_manifests: list[MmapJournalShardManifestV2] = []
        for item in inputs:
            key = (item.scope_id, item.shard_id)
            try:
                scan = item.journal.scan_recovery_prefix()
            except Exception as exc:
                gate.close(
                    reason=MmapRecoveryGateReasonV2.JOURNAL_INTEGRITY,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:header-or-file:{type(exc).__name__}",
                )
                scan = MmapJournalRecoveryScan(
                    journal_epoch=item.journal.journal_epoch,
                    capacity=item.journal.capacity,
                    records=(),
                    status=MmapJournalRecoveryStatus.UNTRUSTED_TAIL,
                    first_untrusted_sequence=1,
                )
            scans[key] = scan
            if not scan.clean:
                gate.close(
                    reason=MmapRecoveryGateReasonV2.JOURNAL_INTEGRITY,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:untrusted-sequence:{scan.first_untrusted_sequence}",
                )
            if (
                scan.trusted_position * 100
                >= item.journal.capacity * self._capacity_policy.stop_new_at_percent
            ):
                gate.close(
                    reason=MmapRecoveryGateReasonV2.CAPACITY,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:position:{scan.trusted_position}",
                )
            projector_position = (projector_positions or {}).get(key, 0)
            if projector_position < 0 or projector_position > scan.trusted_position:
                gate.close(
                    reason=MmapRecoveryGateReasonV2.PROJECTOR_LAG,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:invalid-projector-position",
                )
            elif (
                scan.trusted_position - projector_position
                > self._capacity_policy.maximum_projector_lag_records
            ):
                gate.close(
                    reason=MmapRecoveryGateReasonV2.PROJECTOR_LAG,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:lag:{scan.trusted_position - projector_position}",
                )
            shard_manifests.append(
                MmapJournalShardManifestV2(
                    scope_id=item.scope_id,
                    shard_id=item.shard_id,
                    journal_epoch=item.journal.journal_epoch,
                    profile_hash=item.journal.profile_hash,
                    frame_hash=_records_hash(scan.records),
                    contiguous_position=scan.trusted_position,
                    capacity=item.journal.capacity,
                    route_directory_digest=item.route_directory_digest,
                    writer_identity_digest=sha256(item.writer_identity.encode("ascii")).hexdigest(),
                    generated=scan.clean,
                    sealed=scan.clean,
                )
            )
        self._check_manifest_drift(gate, shard_manifests, previous_manifest)
        if not callback_stream_complete:
            gate.close(
                reason=MmapRecoveryGateReasonV2.CALLBACK_GAP,
                scope_id="runtime",
                reference="callback-stream-incomplete",
            )
        if not control_root_complete:
            gate.close(
                reason=MmapRecoveryGateReasonV2.CONTROL_ROOT,
                scope_id="runtime",
                reference="control-root-incomplete",
            )
        routes, items = self._rebuild_routes(gate, scans)
        manifest = MergeManifestV2(
            shards=tuple(shard_manifests),
            generated=not gate.snapshot().events,
            sealed=not gate.snapshot().events,
            previous_manifest_hash=(previous_manifest.manifest_hash if previous_manifest else None),
        )
        return MmapRecoveryResultV2(
            manifest=manifest,
            scans=scans,
            recovered_routes=routes,
            reconciliation_items=items,
            gate=gate.snapshot(),
        )

    @staticmethod
    def _check_manifest_drift(
        gate: MmapRecoveryGateV2,
        current: Iterable[MmapJournalShardManifestV2],
        previous: MergeManifestV2 | None,
    ) -> None:
        if previous is None:
            return
        old = {item.key: item for item in previous.shards}
        for item in current:
            prior = old.get(item.key)
            if prior is None:
                continue
            if (
                item.journal_epoch != prior.journal_epoch
                or item.profile_hash != prior.profile_hash
                or item.route_directory_digest != prior.route_directory_digest
                or item.contiguous_position < prior.contiguous_position
            ):
                gate.close(
                    reason=MmapRecoveryGateReasonV2.MANIFEST_DRIFT,
                    scope_id=item.scope_id,
                    reference=f"{item.shard_id}:epoch-profile-route-or-position",
                )

    @staticmethod
    def _rebuild_routes(
        gate: MmapRecoveryGateV2,
        scans: Mapping[tuple[str, str], MmapJournalRecoveryScan],
    ) -> tuple[tuple[MmapRecoveredRouteV2, ...], tuple[MmapReconciliationItemV2, ...]]:
        grouped: dict[tuple[str, str, str], list[tuple[str, MmapPreNativeRecord]]] = defaultdict(
            list
        )
        by_client: dict[tuple[str, str], tuple[str, str, str]] = {}
        by_execution: dict[tuple[str, str], tuple[str, str, str]] = {}
        for (scope_id, shard_id), scan in scans.items():
            for record in scan.records:
                route_key = (scope_id, record.execution_id, record.client_order_ref)
                for index, value in (
                    (by_client, record.client_order_ref),
                    (by_execution, record.execution_id),
                ):
                    existing = index.get((scope_id, value))
                    if existing is not None and existing != route_key:
                        gate.close(
                            reason=MmapRecoveryGateReasonV2.ROUTE_CONFLICT,
                            scope_id=scope_id,
                            reference=f"{shard_id}:identity-conflict:{record.sequence}",
                        )
                    index[(scope_id, value)] = route_key
                grouped[route_key].append((shard_id, record))
        routes: list[MmapRecoveredRouteV2] = []
        items: list[MmapReconciliationItemV2] = []
        for (scope_id, execution_id, client_order_ref), facts in sorted(grouped.items()):
            action_facts = [
                item
                for _, item in facts
                if item.kind
                in {
                    MmapJournalRecordKind.ORDER_PRE_NATIVE,
                    MmapJournalRecordKind.ORDER_CALL_STARTED,
                    MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
                }
            ]
            reference = action_facts[0] if action_facts else facts[0][1]
            conflicting = any(
                item.command_id != reference.command_id
                or item.payload_hash != reference.payload_hash
                for item in action_facts
            )
            native_ids = {
                item.native_order_id
                for _, item in facts
                if item.native_order_id
                and item.kind
                in {
                    MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
                    MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED,
                }
            }
            if conflicting or len(native_ids) > 1:
                gate.close(
                    reason=MmapRecoveryGateReasonV2.ROUTE_CONFLICT,
                    scope_id=scope_id,
                    reference=f"{client_order_ref}:command-payload-or-native-id",
                )
                continue
            kinds = {item.kind for _, item in facts}
            has_pre_native = MmapJournalRecordKind.ORDER_PRE_NATIVE in kinds
            has_call_started = MmapJournalRecordKind.ORDER_CALL_STARTED in kinds
            has_accepted = MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED in kinds
            has_callback = MmapJournalRecordKind.ORDER_CALLBACK_OBSERVED in kinds
            has_cancel_started = MmapJournalRecordKind.CANCEL_STARTED in kinds
            has_cancel_accepted = MmapJournalRecordKind.CANCEL_NATIVE_ACCEPTED in kinds
            has_cancel_rejected = MmapJournalRecordKind.CANCEL_REJECTED in kinds
            if (
                has_callback or has_cancel_started or has_cancel_accepted or has_cancel_rejected
            ) and not has_pre_native:
                gate.close(
                    reason=MmapRecoveryGateReasonV2.CALLBACK_GAP,
                    scope_id=scope_id,
                    reference=f"{client_order_ref}:callback-or-cancel-without-pre-native",
                )
                continue
            routes.append(
                MmapRecoveredRouteV2(
                    scope_id=scope_id,
                    execution_id=execution_id,
                    client_order_ref=client_order_ref,
                    command_id=reference.command_id,
                    payload_hash=reference.payload_hash,
                    native_order_id=next(iter(native_ids), None),
                    shard_ids=tuple(sorted({shard_id for shard_id, _ in facts})),
                )
            )
            position_set = tuple(sorted((shard_id, item.sequence) for shard_id, item in facts))
            if has_cancel_started and not (has_cancel_accepted or has_cancel_rejected):
                kind = MmapReconciliationKindV2.CANCEL_UNKNOWN
            elif has_cancel_accepted and not has_cancel_rejected:
                kind = MmapReconciliationKindV2.CANCEL_ACCEPTED
            elif has_call_started and not has_accepted:
                kind = MmapReconciliationKindV2.SUBMIT_UNKNOWN
            elif has_pre_native and not has_call_started:
                kind = MmapReconciliationKindV2.PRE_NATIVE
            elif has_accepted and not has_callback:
                kind = MmapReconciliationKindV2.CALLBACK_PENDING
            else:
                kind = MmapReconciliationKindV2.NATIVE_ACCEPTED
            items.append(
                MmapReconciliationItemV2(
                    kind=kind,
                    scope_id=scope_id,
                    execution_id=execution_id,
                    client_order_ref=client_order_ref,
                    source_positions=position_set,
                )
            )
        return tuple(routes), tuple(items)


def _records_hash(records: Iterable[MmapPreNativeRecord]) -> str:
    """Hash only fixed recovery identities; raw SDK payloads never enter a manifest."""

    return sha256(
        _canonical_json(
            [
                {
                    "sequence": item.sequence,
                    "kind": item.kind,
                    "monotonic_ns": item.monotonic_ns,
                    "account_id": item.account_id,
                    "execution_id": item.execution_id,
                    "command_id": item.command_id,
                    "payload_hash": item.payload_hash,
                    "client_order_ref": item.client_order_ref,
                    "native_order_id": item.native_order_id,
                }
                for item in records
            ]
        )
    ).hexdigest()
