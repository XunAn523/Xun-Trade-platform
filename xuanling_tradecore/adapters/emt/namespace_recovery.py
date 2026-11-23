"""EMT order-client-id namespace ownership and authoritative recovery.

This module is deliberately SDK-free. It contains the signed namespace
control plane, local route facts, and fail-closed R2 reconciliation. It never
sends, cancels, or retries an order.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import asdict, dataclass, replace
from enum import Enum
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Mapping, Protocol

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from ...adapters.identity import (
    ClientOrderRefAllocationError,
    ClientOrderRefAllocationRequest,
    ClientOrderRefAllocator,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
    ClientOrderRefScope,
)
from ...domain import TradingDayEpoch
from ...domain._validation import require_nonnegative, require_positive, require_text
from ...oms.identity_journal import ClientOrderRefJournal
from .identity import EMT_CLIENT_ORDER_ID_MAX, EmtClientOrderIdAllocator, EmtClientOrderRefScope


class EmtNamespaceRecoveryError(RuntimeError):
    """A namespace claim, route fact, or recovery result is unsafe."""


def _require_hash(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise EmtNamespaceRecoveryError(f"{name} must be a lowercase SHA-256 hash")


@dataclass(frozen=True)
class EmtOrderClientNamespaceScope:
    """The broker-visible namespace within which a uint32 is unique."""

    account_id: str
    trading_day: TradingDayEpoch
    emt_client_id: int

    def __post_init__(self) -> None:
        require_text(self.account_id, "EMT namespace account_id")
        if type(self.emt_client_id) is not int or not 0 <= self.emt_client_id <= 255:
            raise EmtNamespaceRecoveryError("EMT namespace client_id must be uint8")


@dataclass(frozen=True)
class OrderClientIdNamespaceLeaseV1:
    """One static, non-overlapping order-client-id range for a system."""

    scope: EmtOrderClientNamespaceScope
    namespace_id: str
    owner_system_id: str
    adapter_instance_id: str
    range_start: int
    range_end: int
    manifest_version: int
    valid_from_wall_ns: int
    valid_until_wall_ns: int
    release_manifest_hash: str
    signer_id: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.namespace_id, "namespace_id"),
            (self.owner_system_id, "owner_system_id"),
            (self.adapter_instance_id, "adapter_instance_id"),
            (self.signer_id, "signer_id"),
        ):
            require_text(value, f"EMT namespace {name}")
        if not 1 <= self.range_start <= self.range_end <= EMT_CLIENT_ORDER_ID_MAX:
            raise EmtNamespaceRecoveryError("EMT namespace range is outside uint32")
        require_positive(self.manifest_version, "EMT namespace manifest_version")
        require_nonnegative(self.valid_from_wall_ns, "EMT namespace valid_from_wall_ns")
        require_positive(self.valid_until_wall_ns, "EMT namespace valid_until_wall_ns")
        if self.valid_until_wall_ns <= self.valid_from_wall_ns:
            raise EmtNamespaceRecoveryError("EMT namespace validity interval is invalid")
        _require_hash(self.release_manifest_hash, "EMT namespace release_manifest_hash")

    def contains(self, order_client_id: int) -> bool:
        return self.range_start <= order_client_id <= self.range_end

    def active_at(self, wall_ns: int) -> bool:
        return self.valid_from_wall_ns <= wall_ns < self.valid_until_wall_ns


@dataclass(frozen=True)
class SignedOrderClientIdNamespaceLeaseV1:
    """A detached signature over one namespace lease, never a private key."""

    lease: OrderClientIdNamespaceLeaseV1
    certificate_reference: str
    revocation_reference: str
    signature_b64: str

    def __post_init__(self) -> None:
        for value, name in (
            (self.certificate_reference, "certificate_reference"),
            (self.revocation_reference, "revocation_reference"),
            (self.signature_b64, "signature"),
        ):
            require_text(value, f"EMT namespace {name}")

    def canonical_payload(self) -> bytes:
        lease = self.lease
        payload = {
            "algorithm": "Ed25519",
            "certificate_reference": self.certificate_reference,
            "lease": {
                "adapter_instance_id": lease.adapter_instance_id,
                "emt_client_id": lease.scope.emt_client_id,
                "manifest_version": lease.manifest_version,
                "namespace_id": lease.namespace_id,
                "owner_system_id": lease.owner_system_id,
                "range_end": lease.range_end,
                "range_start": lease.range_start,
                "release_manifest_hash": lease.release_manifest_hash,
                "scope_account_id": lease.scope.account_id,
                "scope_trading_day": lease.scope.trading_day.value,
                "signer_id": lease.signer_id,
                "valid_from_wall_ns": lease.valid_from_wall_ns,
                "valid_until_wall_ns": lease.valid_until_wall_ns,
            },
            "revocation_reference": self.revocation_reference,
            "schema_version": 1,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )

    def signature_bytes(self) -> bytes:
        try:
            value = base64.b64decode(self.signature_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EmtNamespaceRecoveryError("EMT namespace signature is not base64") from exc
        if len(value) != 64:
            raise EmtNamespaceRecoveryError("EMT namespace signature is not Ed25519")
        return value

    @property
    def manifest_hash(self) -> str:
        return sha256(self.canonical_payload() + b"\x00" + self.signature_bytes()).hexdigest()


class EmtOrderClientNamespaceVerifier:
    """Offline trust verifier for signed namespace ranges."""

    def __init__(
        self,
        trusted_public_keys: Mapping[str, bytes],
        *,
        revoked_signer_ids: frozenset[str],
    ) -> None:
        if not trusted_public_keys:
            raise EmtNamespaceRecoveryError("EMT namespace trust requires a public key")
        keys: dict[str, Ed25519PublicKey] = {}
        for signer_id, raw_key in trusted_public_keys.items():
            require_text(signer_id, "EMT namespace signer_id")
            if len(raw_key) != 32:
                raise EmtNamespaceRecoveryError("EMT namespace signer key must be 32 bytes")
            try:
                keys[signer_id] = Ed25519PublicKey.from_public_bytes(raw_key)
            except ValueError as exc:
                raise EmtNamespaceRecoveryError("EMT namespace signer key is invalid") from exc
        self._keys = keys
        self._revoked = frozenset(revoked_signer_ids)

    def verify(
        self,
        signed: SignedOrderClientIdNamespaceLeaseV1,
        *,
        wall_ns: int,
    ) -> OrderClientIdNamespaceLeaseV1:
        if signed.lease.signer_id in self._revoked:
            raise EmtNamespaceRecoveryError("EMT namespace signer is revoked")
        if not signed.lease.active_at(wall_ns):
            raise EmtNamespaceRecoveryError("EMT namespace lease is outside validity interval")
        self.verify_detached(
            signer_id=signed.lease.signer_id,
            signature_b64=signed.signature_b64,
            payload=signed.canonical_payload(),
        )
        return signed.lease

    def verify_detached(self, *, signer_id: str, signature_b64: str, payload: bytes) -> None:
        """Verify a non-secret release envelope with the same offline trust set."""

        require_text(signer_id, "EMT namespace signer_id")
        require_text(signature_b64, "EMT namespace signature")
        if signer_id in self._revoked:
            raise EmtNamespaceRecoveryError("EMT namespace signer is revoked")
        try:
            signature = base64.b64decode(signature_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise EmtNamespaceRecoveryError("EMT namespace signature is not base64") from exc
        if len(signature) != 64:
            raise EmtNamespaceRecoveryError("EMT namespace signature is not Ed25519")
        try:
            public_key = self._keys[signer_id]
        except KeyError as exc:
            raise EmtNamespaceRecoveryError("EMT namespace signer is not trusted") from exc
        try:
            public_key.verify(signature, payload)
        except InvalidSignature as exc:
            raise EmtNamespaceRecoveryError("EMT namespace signature does not verify") from exc


class NamespaceOwnership(str, Enum):
    OWN = "OWN"
    FOREIGN_RESERVED = "FOREIGN_RESERVED"
    UNKNOWN_NAMESPACE = "UNKNOWN_NAMESPACE"


@dataclass(frozen=True)
class NamespaceLookup:
    ownership: NamespaceOwnership
    lease: OrderClientIdNamespaceLeaseV1 | None


class OrderClientIdNamespaceDirectoryV1:
    """Immutable, validated own/foreign range map used without remote I/O."""

    def __init__(
        self,
        *,
        own: SignedOrderClientIdNamespaceLeaseV1,
        foreign: tuple[SignedOrderClientIdNamespaceLeaseV1, ...] = (),
        verifier: EmtOrderClientNamespaceVerifier,
        wall_ns: int,
    ) -> None:
        verified_own = verifier.verify(own, wall_ns=wall_ns)
        verified_foreign = tuple(verifier.verify(item, wall_ns=wall_ns) for item in foreign)
        leases = (verified_own,) + verified_foreign
        if len({lease.namespace_id for lease in leases}) != len(leases):
            raise EmtNamespaceRecoveryError("EMT namespace directory repeats namespace_id")
        if any(lease.scope != verified_own.scope for lease in leases):
            raise EmtNamespaceRecoveryError("EMT namespace directory mixes broker scopes")
        if any(lease.owner_system_id == verified_own.owner_system_id for lease in verified_foreign):
            raise EmtNamespaceRecoveryError("EMT foreign namespace repeats own system owner")
        ordered = tuple(sorted(leases, key=lambda item: (item.range_start, item.range_end)))
        for left, right in zip(ordered, ordered[1:]):
            if left.range_end >= right.range_start:
                raise EmtNamespaceRecoveryError("EMT namespace ranges overlap")
        self._own = verified_own
        self._leases = ordered
        hashes = sorted(item.manifest_hash for item in (own,) + foreign)
        self._manifest_hash = sha256(
            b"\x00".join(item.encode("ascii") for item in hashes)
        ).hexdigest()

    @property
    def own(self) -> OrderClientIdNamespaceLeaseV1:
        return self._own

    @property
    def manifest_hash(self) -> str:
        return self._manifest_hash

    @property
    def scope(self) -> EmtOrderClientNamespaceScope:
        return self._own.scope

    def lookup(self, order_client_id: int) -> NamespaceLookup:
        # EMT documents order_client_id as user-defined and permits callers
        # to omit it.  The Python callback then legitimately reports zero.
        # Zero cannot belong to any signed range (which begins at one), so it
        # remains an unknown, recovery-blocking identity rather than causing a
        # complete authoritative account scan to lose its first row.
        if type(order_client_id) is not int or not 0 <= order_client_id <= EMT_CLIENT_ORDER_ID_MAX:
            raise EmtNamespaceRecoveryError("EMT order_client_id is outside uint32")
        for lease in self._leases:
            if lease.contains(order_client_id):
                ownership = (
                    NamespaceOwnership.OWN
                    if lease == self._own
                    else NamespaceOwnership.FOREIGN_RESERVED
                )
                return NamespaceLookup(ownership, lease)
        return NamespaceLookup(NamespaceOwnership.UNKNOWN_NAMESPACE, None)


class ExecutionNamespaceFenceV1:
    """Local process fence for one verified R1 namespace manifest.

    This is intentionally *not* presented as a distributed lock.  It prevents
    two TradeCore processes under the same control root from silently using a
    namespace at once; cross-host exclusivity continues to depend on the
    signed, non-overlapping published range manifest.
    """

    def __init__(
        self,
        *,
        runtime_control_root: Path,
        directory: OrderClientIdNamespaceDirectoryV1,
    ) -> None:
        self._root = Path(runtime_control_root)
        self._directory = directory
        payload = {
            "account_id": directory.scope.account_id,
            "trading_day": directory.scope.trading_day.value,
            "emt_client_id": directory.scope.emt_client_id,
            "namespace_id": directory.own.namespace_id,
            "namespace_manifest_hash": directory.manifest_hash,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        self._scope_hash = sha256(encoded.encode("ascii")).hexdigest()
        self._path = self._root / f"{self._scope_hash}.lock"
        self._acquired = False

    @property
    def scope_hash(self) -> str:
        return self._scope_hash

    def acquire(self, *, instance_id: str) -> None:
        require_text(instance_id, "namespace fence instance_id")
        if self._acquired:
            raise EmtNamespaceRecoveryError("namespace fence is already acquired")
        self._root.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "schema_version": 1,
                "scope_hash": self._scope_hash,
                "owner_system_id": self._directory.own.owner_system_id,
                "adapter_instance_id": self._directory.own.adapter_instance_id,
                "namespace_manifest_hash": self._directory.manifest_hash,
                "instance_id": instance_id,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        try:
            descriptor = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError as exc:
            raise EmtNamespaceRecoveryError(
                "namespace fence already exists; recovery/manual review is required"
            ) from exc
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                self._path.unlink(missing_ok=True)
            finally:
                raise
        self._acquired = True

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            self._path.unlink()
        except FileNotFoundError as exc:
            raise EmtNamespaceRecoveryError("namespace fence disappeared before release") from exc
        finally:
            self._acquired = False


@dataclass(frozen=True)
class RouteLedgerFactV2:
    """One PreDispatch route fact durable before the EMT native call."""

    scope: EmtOrderClientNamespaceScope
    order_client_id: int
    namespace_id: str
    namespace_manifest_hash: str
    owner_system_id: str
    adapter_instance_id: str
    strategy_id: str
    execution_id: str
    journal_epoch: int
    frame_sequence: int
    route_payload_hash: str
    call_started: bool = True
    order_emt_id: int | None = None

    def __post_init__(self) -> None:
        if not 1 <= self.order_client_id <= EMT_CLIENT_ORDER_ID_MAX:
            raise EmtNamespaceRecoveryError("route ledger order_client_id is invalid")
        for value, name in (
            (self.namespace_id, "namespace_id"),
            (self.owner_system_id, "owner_system_id"),
            (self.adapter_instance_id, "adapter_instance_id"),
            (self.strategy_id, "strategy_id"),
            (self.execution_id, "execution_id"),
        ):
            require_text(value, f"route ledger {name}")
        _require_hash(self.namespace_manifest_hash, "route ledger namespace_manifest_hash")
        _require_hash(self.route_payload_hash, "route ledger route_payload_hash")
        require_positive(self.journal_epoch, "route ledger journal_epoch")
        require_positive(self.frame_sequence, "route ledger frame_sequence")
        if type(self.call_started) is not bool:
            raise EmtNamespaceRecoveryError("route ledger call_started must be boolean")
        if self.order_emt_id is not None:
            require_positive(self.order_emt_id, "route ledger order_emt_id")


@dataclass(frozen=True)
class RecoveryRehydratedFactV1:
    """One recovery-only native-id projection backed by two EMT scans.

    The fact deliberately contains only routing identities and digests.  It
    carries neither account credentials nor market/order payload.  A writer
    must commit the complete batch containing this fact before its matching
    route can be projected into memory.
    """

    recovery_run_id: str
    namespace_manifest_hash: str
    first_scan_digest: str
    second_scan_digest: str
    order_client_id: int
    order_emt_id: int
    execution_id: str
    route_payload_hash: str
    journal_epoch: int
    frame_sequence: int

    def __post_init__(self) -> None:
        require_text(self.recovery_run_id, "recovery rehydrated recovery_run_id")
        require_text(self.execution_id, "recovery rehydrated execution_id")
        for value, name in (
            (self.namespace_manifest_hash, "namespace_manifest_hash"),
            (self.first_scan_digest, "first_scan_digest"),
            (self.second_scan_digest, "second_scan_digest"),
            (self.route_payload_hash, "route_payload_hash"),
        ):
            _require_hash(value, f"recovery rehydrated {name}")
        if (
            type(self.order_client_id) is not int
            or not 1 <= self.order_client_id <= EMT_CLIENT_ORDER_ID_MAX
        ):
            raise EmtNamespaceRecoveryError("recovery rehydrated order_client_id is invalid")
        require_positive(self.order_emt_id, "recovery rehydrated order_emt_id")
        require_positive(self.journal_epoch, "recovery rehydrated journal_epoch")
        require_positive(self.frame_sequence, "recovery rehydrated frame_sequence")


class RecoveryRehydrationFactSinkV1(Protocol):
    """Durable, batch-atomic sink required before R2 projection."""

    def append_rehydration_batch(self, facts: tuple[RecoveryRehydratedFactV1, ...]) -> None:
        """Commit every fact or leave no projectable recovery mutation."""

        ...


class RouteLedgerV2:
    """Route facts rebuilt only from committed PreDispatch Journal evidence."""

    def __init__(self, directory: OrderClientIdNamespaceDirectoryV1) -> None:
        self._directory = directory
        self._by_client_id: dict[int, RouteLedgerFactV2] = {}
        self._by_execution_id: dict[str, RouteLedgerFactV2] = {}
        self._lock = RLock()

    def record_pre_dispatch(self, fact: RouteLedgerFactV2) -> None:
        self._validate_fact(fact)
        with self._lock:
            existing_client = self._by_client_id.get(fact.order_client_id)
            existing_execution = self._by_execution_id.get(fact.execution_id)
            if existing_client is not None or existing_execution is not None:
                if existing_client == fact and existing_execution == fact:
                    return
                raise EmtNamespaceRecoveryError("route ledger contains a conflicting route")
            self._by_client_id[fact.order_client_id] = fact
            self._by_execution_id[fact.execution_id] = fact

    def route_for_client_id(self, order_client_id: int) -> RouteLedgerFactV2 | None:
        with self._lock:
            return self._by_client_id.get(order_client_id)

    def rehydrate_native_acceptance(
        self,
        *,
        order_client_id: int,
        order_emt_id: int,
    ) -> RouteLedgerFactV2:
        require_positive(order_emt_id, "rehydrated EMT order_emt_id")
        with self._lock:
            existing = self._by_client_id.get(order_client_id)
            if existing is None:
                raise EmtNamespaceRecoveryError("cannot rehydrate an unknown route")
            if existing.order_emt_id is not None and existing.order_emt_id != order_emt_id:
                raise EmtNamespaceRecoveryError("rehydrated EMT order id conflicts with route")
            rehydrated = replace(existing, order_emt_id=order_emt_id)
            self._by_client_id[order_client_id] = rehydrated
            self._by_execution_id[rehydrated.execution_id] = rehydrated
            return rehydrated

    def apply_recovery_rehydrated(self, fact: RecoveryRehydratedFactV1) -> RouteLedgerFactV2:
        """Apply one already-durable recovery fact idempotently.

        This is intentionally separate from ``rehydrate_native_acceptance``:
        a restart may only use the former after the mmap reader has verified a
        complete recovery batch and its scan/route bindings.
        """

        if fact.namespace_manifest_hash != self._directory.manifest_hash:
            raise EmtNamespaceRecoveryError(
                "recovery rehydrated fact has another namespace manifest"
            )
        with self._lock:
            existing = self._by_client_id.get(fact.order_client_id)
            if existing is None:
                raise EmtNamespaceRecoveryError("recovery rehydrated fact has no local route")
            if (
                existing.execution_id != fact.execution_id
                or existing.route_payload_hash != fact.route_payload_hash
                or existing.journal_epoch != fact.journal_epoch
                or existing.frame_sequence != fact.frame_sequence
            ):
                raise EmtNamespaceRecoveryError("recovery rehydrated fact conflicts with route")
        return self.rehydrate_native_acceptance(
            order_client_id=fact.order_client_id,
            order_emt_id=fact.order_emt_id,
        )

    def facts(self) -> tuple[RouteLedgerFactV2, ...]:
        with self._lock:
            return tuple(sorted(self._by_client_id.values(), key=lambda item: item.order_client_id))

    def _validate_fact(self, fact: RouteLedgerFactV2) -> None:
        own = self._directory.own
        if fact.scope != self._directory.scope:
            raise EmtNamespaceRecoveryError("route ledger fact has another namespace scope")
        if (
            fact.namespace_id != own.namespace_id
            or fact.namespace_manifest_hash != self._directory.manifest_hash
            or fact.owner_system_id != own.owner_system_id
            or fact.adapter_instance_id != own.adapter_instance_id
            or not own.contains(fact.order_client_id)
        ):
            raise EmtNamespaceRecoveryError("route ledger fact is outside own namespace")


@dataclass(frozen=True)
class EmtAuthoritativeOrderV1:
    """Minimal normalized order evidence retained from an EMT query callback."""

    order_client_id: int
    order_emt_id: int
    order_status: int
    filled_qty: int
    leaves_qty: int

    def __post_init__(self) -> None:
        require_nonnegative(self.order_client_id, "authoritative order_client_id")
        require_positive(self.order_emt_id, "authoritative order_emt_id")
        require_nonnegative(self.order_status, "authoritative order_status")
        require_nonnegative(self.filled_qty, "authoritative filled_qty")
        require_nonnegative(self.leaves_qty, "authoritative leaves_qty")

    @property
    def digest(self) -> str:
        return sha256(
            json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()


@dataclass(frozen=True)
class EmtAuthoritativeScanV1:
    """One complete query pass containing no account, instrument, or price data."""

    scan_id: str
    orders: tuple[EmtAuthoritativeOrderV1, ...]
    complete: bool

    def __post_init__(self) -> None:
        require_text(self.scan_id, "authoritative scan_id")
        orders = tuple(self.orders)
        if len({(item.order_client_id, item.order_emt_id) for item in orders}) != len(orders):
            raise EmtNamespaceRecoveryError("authoritative scan repeats EMT order identity")
        object.__setattr__(self, "orders", orders)

    @property
    def digest(self) -> str:
        values = "\x00".join(
            item.digest
            for item in sorted(
                self.orders, key=lambda item: (item.order_client_id, item.order_emt_id)
            )
        )
        return sha256(values.encode("ascii")).hexdigest()


class EmtAuthoritativeOrderObserverV1(Protocol):
    """Explicit recovery/owned-target sink for five permitted EMT order scalars."""

    def on_authoritative_order_row(
        self,
        *,
        query_kind: str,
        order: EmtAuthoritativeOrderV1,
    ) -> None:
        """Accept one normalized order row outside ordinary query operation."""

        ...

    def on_authoritative_order_terminal(
        self,
        *,
        query_kind: str,
        error_id: int | None,
        page_has_more: bool = False,
    ) -> None:
        """Close the one expected full-order query pass."""

        ...


class EmtAuthoritativeOrderCollectorV1:
    """Bounded, recovery-only collector for one complete EMT order scan.

    The collector intentionally retains no account, symbol, price, side,
    credential, or native object.  It is opt-in and cannot make a normal
    read-only query into a recovery scan by itself.
    """

    def __init__(self, *, scan_id: str) -> None:
        require_text(scan_id, "authoritative collector scan_id")
        self._scan_id = scan_id
        self._orders: dict[tuple[int, int], EmtAuthoritativeOrderV1] = {}
        self._completed = False
        self._failed = False
        self._lock = RLock()

    def on_authoritative_order_row(
        self,
        *,
        query_kind: str,
        order: EmtAuthoritativeOrderV1,
    ) -> None:
        if query_kind not in {"orders", "orders_by_page", "unfinished_orders"}:
            raise EmtNamespaceRecoveryError(
                "authoritative order evidence requires an approved orders query"
            )
        with self._lock:
            if self._completed:
                raise EmtNamespaceRecoveryError("authoritative recovery scan is already complete")
            identity = (order.order_client_id, order.order_emt_id)
            existing = self._orders.get(identity)
            if existing is not None and existing != order:
                self._failed = True
                raise EmtNamespaceRecoveryError(
                    "authoritative recovery scan has conflicting EMT order identity"
                )
            self._orders[identity] = order

    def on_authoritative_order_terminal(
        self,
        *,
        query_kind: str,
        error_id: int | None,
        page_has_more: bool = False,
    ) -> None:
        with self._lock:
            if (
                query_kind not in {"orders", "orders_by_page", "unfinished_orders"}
                or self._completed
                or error_id not in (None, 0)
                or (query_kind in {"orders", "unfinished_orders"} and page_has_more)
            ):
                self._failed = True
                raise EmtNamespaceRecoveryError(
                    "authoritative recovery query terminal is incomplete"
                )
            if query_kind in {"orders", "unfinished_orders"} or not page_has_more:
                self._completed = True

    def seal(self) -> EmtAuthoritativeScanV1:
        with self._lock:
            return EmtAuthoritativeScanV1(
                scan_id=self._scan_id,
                orders=tuple(self._orders.values()),
                complete=self._completed and not self._failed,
            )


class EmtAuthoritativeOrderScanSeriesCollectorV1:
    """Rotate explicitly between complete scans without retaining raw rows.

    A caller starts the next scan only after receiving the prior query's
    terminal event.  This makes an accidental overlap or a late callback a
    fail-closed error instead of silently merging two snapshots.
    """

    def __init__(self) -> None:
        self._current: EmtAuthoritativeOrderCollectorV1 | None = None
        self._lock = RLock()

    def begin(self, *, scan_id: str) -> None:
        with self._lock:
            if self._current is not None:
                raise EmtNamespaceRecoveryError(
                    "authoritative scan series already has an active scan"
                )
            self._current = EmtAuthoritativeOrderCollectorV1(scan_id=scan_id)

    def on_authoritative_order_row(
        self,
        *,
        query_kind: str,
        order: EmtAuthoritativeOrderV1,
    ) -> None:
        with self._lock:
            collector = self._require_current()
            collector.on_authoritative_order_row(query_kind=query_kind, order=order)

    def on_authoritative_order_terminal(
        self,
        *,
        query_kind: str,
        error_id: int | None,
        page_has_more: bool = False,
    ) -> None:
        with self._lock:
            collector = self._require_current()
            collector.on_authoritative_order_terminal(
                query_kind=query_kind,
                error_id=error_id,
                page_has_more=page_has_more,
            )

    def seal_current(self) -> EmtAuthoritativeScanV1:
        with self._lock:
            collector = self._require_current()
            scan = collector.seal()
            self._current = None
            return scan

    def _require_current(self) -> EmtAuthoritativeOrderCollectorV1:
        if self._current is None:
            raise EmtNamespaceRecoveryError("authoritative scan series has no active scan")
        return self._current


class RecoveryOrderClassification(str, Enum):
    OWN_MATCHED = "OWN_MATCHED"
    OWN_R2_ORPHAN = "OWN_R2_ORPHAN"
    FOREIGN_RESERVED = "FOREIGN_RESERVED"
    OWN_UNRESOLVED = "OWN_UNRESOLVED"
    UNKNOWN_NAMESPACE = "UNKNOWN_NAMESPACE"
    NAMESPACE_COLLISION = "NAMESPACE_COLLISION"


class RecoveryDispositionV1(str, Enum):
    RECOVERED_DISARMED = "RECOVERED_DISARMED"
    RECOVERY_BLOCKED = "RECOVERY_BLOCKED"


@dataclass(frozen=True)
class ReconciledOrderV1:
    order: EmtAuthoritativeOrderV1
    classification: RecoveryOrderClassification
    execution_id: str | None
    strategy_id: str | None


@dataclass(frozen=True)
class EmtAuthoritativeRecoveryResultV1:
    """Recovery conclusion with stability scoped to owned and unknown rows.

    Reserved foreign rows may change while another system trades.  Their drift
    is retained as evidence, but is never projected or acted on by this
    system, so it cannot by itself invalidate an otherwise stable owned
    recovery projection.
    """

    disposition: RecoveryDispositionV1
    stable_double_scan: bool
    callback_gaps_closed: bool
    reconciled_orders: tuple[ReconciledOrderV1, ...]
    rehydrated_execution_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    foreign_drift_detected: bool
    foreign_drift_order_count: int


class EmtAuthoritativeRecoveryReconcilerV1:
    """Compare stable EMT scans with local routes and rehydrate R2 only."""

    def __init__(
        self,
        *,
        directory: OrderClientIdNamespaceDirectoryV1,
        ledger: RouteLedgerV2,
        rehydration_sink: RecoveryRehydrationFactSinkV1 | None = None,
    ) -> None:
        self._directory = directory
        self._ledger = ledger
        self._rehydration_sink = rehydration_sink

    def reconcile(
        self,
        *,
        first_scan: EmtAuthoritativeScanV1,
        second_scan: EmtAuthoritativeScanV1,
        callback_gaps_closed: bool,
    ) -> EmtAuthoritativeRecoveryResultV1:
        first_orders = {
            (item.order_client_id, item.order_emt_id): item for item in first_scan.orders
        }
        second_orders = {
            (item.order_client_id, item.order_emt_id): item for item in second_scan.orders
        }

        def partition(
            orders: dict[tuple[int, int], EmtAuthoritativeOrderV1],
        ) -> tuple[
            dict[tuple[int, int], EmtAuthoritativeOrderV1],
            dict[tuple[int, int], EmtAuthoritativeOrderV1],
            dict[tuple[int, int], EmtAuthoritativeOrderV1],
        ]:
            own: dict[tuple[int, int], EmtAuthoritativeOrderV1] = {}
            foreign: dict[tuple[int, int], EmtAuthoritativeOrderV1] = {}
            unknown: dict[tuple[int, int], EmtAuthoritativeOrderV1] = {}
            for identity, order in orders.items():
                ownership = self._directory.lookup(order.order_client_id).ownership
                if ownership is NamespaceOwnership.OWN:
                    own[identity] = order
                elif ownership is NamespaceOwnership.FOREIGN_RESERVED:
                    foreign[identity] = order
                else:
                    unknown[identity] = order
            return own, foreign, unknown

        first_own, first_foreign, first_unknown = partition(first_orders)
        second_own, second_foreign, second_unknown = partition(second_orders)
        foreign_drift_order_count = sum(
            first_foreign.get(identity) != second_foreign.get(identity)
            for identity in set(first_foreign) | set(second_foreign)
        )
        stable = (
            first_scan.complete
            and second_scan.complete
            and first_own == second_own
            and first_unknown == second_unknown
        )
        reasons: list[str] = []
        if not first_scan.complete or not second_scan.complete:
            reasons.append("AUTHORITATIVE_SCAN_INCOMPLETE")
        if not stable:
            reasons.append("AUTHORITATIVE_DOUBLE_SCAN_UNSTABLE")
        if first_unknown or second_unknown:
            reasons.append("UNKNOWN_NAMESPACE")
        if not callback_gaps_closed:
            reasons.append("CALLBACK_GAP_OPEN")
        reconciled: list[ReconciledOrderV1] = []
        rehydrate_candidates: list[tuple[RouteLedgerFactV2, EmtAuthoritativeOrderV1]] = []
        own_client_ids = {
            client_id
            for client_id, _native_id in second_own
            if sum(candidate_id == client_id for candidate_id, _candidate_native_id in second_own)
            > 1
        }
        for order in sorted(
            second_scan.orders, key=lambda item: (item.order_client_id, item.order_emt_id)
        ):
            lookup = self._directory.lookup(order.order_client_id)
            if lookup.ownership is NamespaceOwnership.FOREIGN_RESERVED:
                reconciled.append(
                    ReconciledOrderV1(
                        order, RecoveryOrderClassification.FOREIGN_RESERVED, None, None
                    )
                )
                continue
            if lookup.ownership is NamespaceOwnership.UNKNOWN_NAMESPACE:
                reconciled.append(
                    ReconciledOrderV1(
                        order, RecoveryOrderClassification.UNKNOWN_NAMESPACE, None, None
                    )
                )
                continue
            route = self._ledger.route_for_client_id(order.order_client_id)
            if route is None or not route.call_started:
                reconciled.append(
                    ReconciledOrderV1(order, RecoveryOrderClassification.OWN_UNRESOLVED, None, None)
                )
                reasons.append("OWN_ROUTE_UNRESOLVED")
                continue
            if order.order_client_id in own_client_ids and route.order_emt_id is None:
                reconciled.append(
                    ReconciledOrderV1(
                        order,
                        RecoveryOrderClassification.NAMESPACE_COLLISION,
                        route.execution_id,
                        route.strategy_id,
                    )
                )
                reasons.append("NATIVE_ORDER_ID_CONFLICT")
                continue
            if route.order_emt_id is not None and route.order_emt_id != order.order_emt_id:
                reconciled.append(
                    ReconciledOrderV1(
                        order,
                        RecoveryOrderClassification.NAMESPACE_COLLISION,
                        route.execution_id,
                        route.strategy_id,
                    )
                )
                reasons.append("NATIVE_ORDER_ID_CONFLICT")
                continue
            if route.order_emt_id is None:
                classification = RecoveryOrderClassification.OWN_R2_ORPHAN
                rehydrate_candidates.append((route, order))
            else:
                classification = RecoveryOrderClassification.OWN_MATCHED
            reconciled.append(
                ReconciledOrderV1(order, classification, route.execution_id, route.strategy_id)
            )
        rehydrated: list[str] = []
        if rehydrate_candidates and self._rehydration_sink is None:
            reasons.append("RECOVERY_REHYDRATION_SINK_MISSING")
        if not reasons and rehydrate_candidates:
            recovery_run_id = sha256(
                "\x00".join(
                    (
                        self._directory.manifest_hash,
                        first_scan.digest,
                        second_scan.digest,
                        *(
                            f"{route.execution_id}:{order.order_client_id}:{order.order_emt_id}"
                            for route, order in rehydrate_candidates
                        ),
                    )
                ).encode("utf-8")
            ).hexdigest()
            facts = tuple(
                RecoveryRehydratedFactV1(
                    recovery_run_id=recovery_run_id,
                    namespace_manifest_hash=self._directory.manifest_hash,
                    first_scan_digest=first_scan.digest,
                    second_scan_digest=second_scan.digest,
                    order_client_id=order.order_client_id,
                    order_emt_id=order.order_emt_id,
                    execution_id=route.execution_id,
                    route_payload_hash=route.route_payload_hash,
                    journal_epoch=route.journal_epoch,
                    frame_sequence=route.frame_sequence,
                )
                for route, order in rehydrate_candidates
            )
            assert self._rehydration_sink is not None
            try:
                self._rehydration_sink.append_rehydration_batch(facts)
            except BaseException:
                # The sink itself must leave incomplete batches non-projectable.
                # A recovery conclusion may never outrun its durable evidence.
                reasons.append("RECOVERY_REHYDRATION_PERSIST_FAILED")
            else:
                for fact in facts:
                    self._ledger.apply_recovery_rehydrated(fact)
                    rehydrated.append(fact.execution_id)
        return EmtAuthoritativeRecoveryResultV1(
            RecoveryDispositionV1.RECOVERY_BLOCKED
            if reasons
            else RecoveryDispositionV1.RECOVERED_DISARMED,
            stable,
            callback_gaps_closed,
            tuple(reconciled),
            tuple(rehydrated),
            tuple(sorted(set(reasons))),
            foreign_drift_order_count > 0,
            foreign_drift_order_count,
        )


class NamespaceBoundEmtClientOrderIdAllocator(ClientOrderRefAllocator):
    """Journal-backed EMT allocator restricted to one verified range lease."""

    def __init__(
        self,
        *,
        emt_scope: EmtClientOrderRefScope,
        namespace: OrderClientIdNamespaceLeaseV1,
        journal: ClientOrderRefJournal,
    ) -> None:
        expected = EmtOrderClientNamespaceScope(
            emt_scope.account_id,
            emt_scope.trading_day,
            emt_scope.emt_client_id,
        )
        if namespace.scope != expected:
            raise EmtNamespaceRecoveryError("namespace lease has another EMT allocator scope")
        self._emt_scope = emt_scope
        self._namespace = namespace
        self._journal = journal
        self._recovered = False
        self._watermark = namespace.range_start - 1
        self._leases: dict[str, ClientOrderRefLease] = {}
        self._lock = RLock()

    @property
    def scope(self) -> ClientOrderRefScope:
        return self._emt_scope.canonical_scope

    @property
    def journal(self) -> ClientOrderRefJournal:
        """Expose the exact allocation authority to a checked runtime composition."""

        return self._journal

    def recover_from_journal(self) -> None:
        self.recover(self._journal.read_recovery(self.scope))

    def recover(self, recovery: ClientOrderRefRecoveryInput) -> None:
        with self._lock:
            if self._recovered:
                raise ClientOrderRefAllocationError("EMT namespace allocator is already recovered")
            if recovery != self._journal.read_recovery(self.scope):
                raise ClientOrderRefAllocationError(
                    "EMT namespace allocator recovery is not Journal-backed"
                )
            if not recovery.is_complete:
                raise ClientOrderRefAllocationError(
                    "EMT namespace allocator recovery is incomplete"
                )
            leases: dict[str, ClientOrderRefLease] = {}
            watermark = self._namespace.range_start - 1
            for lease in recovery.leases:
                value = EmtClientOrderIdAllocator.parse_client_order_ref(lease.client_order_ref)
                if lease.allocation_sequence != value or not self._namespace.contains(value):
                    raise ClientOrderRefAllocationError(
                        "recovered order_client_id is outside namespace"
                    )
                leases[lease.execution_id] = lease
                watermark = max(watermark, value)
            self._leases = leases
            self._watermark = watermark
            self._recovered = True

    def allocate(self, request: ClientOrderRefAllocationRequest) -> ClientOrderRefLease:
        with self._lock:
            if request.scope != self.scope:
                raise ClientOrderRefAllocationError("EMT namespace allocation has another scope")
            if not self._recovered:
                raise ClientOrderRefAllocationError(
                    "EMT namespace allocation requires complete Journal recovery"
                )
            existing = self._leases.get(request.execution_id)
            if existing is not None:
                return existing
            candidate_id = self._watermark + 1
            if candidate_id > self._namespace.range_end:
                raise ClientOrderRefAllocationError("EMT order_client_id namespace is exhausted")
            candidate = ClientOrderRefLease(
                execution_id=request.execution_id,
                scope=self.scope,
                client_order_ref=str(candidate_id),
                allocation_sequence=candidate_id,
            )
            committed = self._journal.append_lease(candidate)
            if committed.lease != candidate:
                raise ClientOrderRefAllocationError("EMT namespace Journal committed another lease")
            self._leases[candidate.execution_id] = candidate
            self._watermark = candidate_id
            return candidate


class VerifiedNamespaceBoundEmtClientOrderIdAllocator(NamespaceBoundEmtClientOrderIdAllocator):
    """Range allocator that accepts only a previously verified directory.

    ``NamespaceBoundEmtClientOrderIdAllocator`` remains a narrow recovery
    primitive.  Production composition uses this wrapper so an unsigned lease
    object cannot accidentally become an allocation authority.
    """

    def __init__(
        self,
        *,
        emt_scope: EmtClientOrderRefScope,
        directory: OrderClientIdNamespaceDirectoryV1,
        journal: ClientOrderRefJournal,
    ) -> None:
        super().__init__(emt_scope=emt_scope, namespace=directory.own, journal=journal)
        self._directory = directory

    @property
    def directory(self) -> OrderClientIdNamespaceDirectoryV1:
        """Return the verified range directory used by this allocator."""

        return self._directory
