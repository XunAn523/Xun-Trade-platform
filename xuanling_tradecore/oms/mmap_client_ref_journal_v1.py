"""Single-host, file-backed EMT client-reference allocation Journal.

One protected control root deterministically maps each exact scope to one
Journal and a cooperative kernel lock. This is a process-crash fence, not an
fsync-per-allocation, host-power-loss or cross-host uniqueness guarantee.
The signed namespace release and external anti-rollback state remain required.
"""

from __future__ import annotations

import json
import os
import stat
from hashlib import sha256
from pathlib import Path
from threading import RLock

try:
    import fcntl
except ImportError:  # Windows must not silently downgrade the lock.
    fcntl = None  # type: ignore[assignment]  # Windows has no POSIX lock; initialization must fail closed.

from ..adapters.identity import (
    ClientOrderRefAllocationError,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
    ClientOrderRefScope,
)
from .identity_journal import ClientOrderRefJournal, ClientOrderRefJournalCommit
from .mmap_fact_journal_v1 import MmapOmsFactJournalV1
from .order_book import OmsFactKind, OmsJournalCommit, OmsJournalFact


class MmapClientRefJournalErrorV1(ClientOrderRefAllocationError):
    """The exact client-reference scope cannot be safely opened or recovered."""


def _scope_payload(scope: ClientOrderRefScope) -> dict[str, str]:
    return {
        "account_id": scope.account_id,
        "trading_day": scope.trading_day.value,
        "venue_id": scope.venue_id,
        "adapter_instance_id": scope.adapter_instance_id,
        "adapter_client_scope": scope.adapter_client_scope,
    }


class MmapClientRefJournalV1(ClientOrderRefJournal):
    """Exactly one local scope with committed, monotonic allocation facts."""

    def __init__(
        self, *, root: Path, scope: ClientOrderRefScope, capacity: int, bootstrap: bool = False
    ) -> None:
        if fcntl is None or not hasattr(os, "O_NOFOLLOW"):
            raise MmapClientRefJournalErrorV1(
                "client reference Journal requires Linux file locking"
            )
        if (
            scope.venue_id != "EMT"
            or type(bootstrap) is not bool
            or type(capacity) is not int
            or not 1 <= capacity <= 1_000_000
        ):
            raise MmapClientRefJournalErrorV1("client reference Journal scope is invalid")
        root = Path(root)
        if not root.is_absolute() or root.is_symlink() or not root.is_dir():
            raise MmapClientRefJournalErrorV1("client reference control root is unsafe")
        metadata = root.stat()
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise MmapClientRefJournalErrorV1("client reference control root must be owner-private")
        encoded = json.dumps(_scope_payload(scope), sort_keys=True, separators=(",", ":")).encode()
        scope_hash = sha256(encoded).hexdigest()
        self._scope = scope
        self._journal_id = f"emt-client-refs-{scope_hash}"
        self._lock = RLock()
        self._failed = False
        self._closed = False
        self._records: dict[str, ClientOrderRefJournalCommit] = {}
        self._last_sequence = 0
        self._preparer_claimed = False
        self._path = root / f"{scope_hash}.mmap"
        anchor = root / f"{scope_hash}.anchor"
        lock_path = root / f"{scope_hash}.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        self._lock_fd = descriptor
        try:
            lock_stat = os.fstat(descriptor)
            if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_nlink != 1:
                raise MmapClientRefJournalErrorV1("client reference lock file is unsafe")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise MmapClientRefJournalErrorV1(
                    "client reference scope is already owned"
                ) from exc
            anchor_payload = json.dumps(
                {"schema": 1, "scope_hash": scope_hash, "capacity": capacity},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
            creating = False
            if anchor.exists() or anchor.is_symlink():
                self._check_regular(anchor)
                if (
                    anchor.stat().st_size != len(anchor_payload)
                    or anchor.read_bytes() != anchor_payload
                    or not self._path.exists()
                ):
                    raise MmapClientRefJournalErrorV1(
                        "client reference anchor or Journal is missing"
                    )
            else:
                if not bootstrap or self._path.exists() or self._path.is_symlink():
                    raise MmapClientRefJournalErrorV1(
                        "client reference bootstrap proof is required"
                    )
                self._create_anchor(anchor, anchor_payload)
                creating = True
            if self._path.exists() or self._path.is_symlink():
                self._check_regular(self._path)
                if not creating and self._path.stat().st_size == 0:
                    raise MmapClientRefJournalErrorV1("existing client reference Journal is empty")
            self._journal = MmapOmsFactJournalV1(
                path=self._path, journal_id=self._journal_id, capacity=capacity
            )
            try:
                self._load()
            except BaseException:
                self._journal.close()
                raise
        except BaseException:
            os.close(descriptor)
            self._closed = True
            raise

    @property
    def journal_id(self) -> str:
        return self._journal_id

    def claim_single_preparer(self) -> None:
        """Bind this open allocation scope to exactly one process-local preparer."""

        with self._lock:
            self._require_open(self._scope)
            if self._preparer_claimed:
                raise MmapClientRefJournalErrorV1("client reference preparer is already claimed")
            self._preparer_claimed = True

    def read_recovery(self, scope: ClientOrderRefScope) -> ClientOrderRefRecoveryInput:
        with self._lock:
            self._require_open(scope)
            for record in self._records.values():
                self._verify_record(record)
            leases = tuple(record.lease for record in self._records.values())
            return ClientOrderRefRecoveryInput(
                recovery_evidence_id=f"{self._journal_id}:recovery",
                journal_id=self._journal_id,
                scope=scope,
                committed_allocation_sequence=self._last_sequence,
                leases=leases,
                is_complete=True,
            )

    def append_lease(self, lease: ClientOrderRefLease) -> ClientOrderRefJournalCommit:
        with self._lock:
            self._require_open(lease.scope)
            existing = self._records.get(lease.execution_id)
            if existing is not None:
                if existing.lease != lease:
                    raise MmapClientRefJournalErrorV1("execution already owns another reference")
                self._verify_record(existing)
                return existing
            self._validate_lease(lease)
            if lease.allocation_sequence <= self._last_sequence:
                raise MmapClientRefJournalErrorV1("reference sequence does not advance")
            fact = OmsJournalFact(
                fact_id=f"client-ref:{sha256(lease.execution_id.encode()).hexdigest()}",
                kind=OmsFactKind.CLIENT_ORDER_REF_ALLOCATED,
                account_id=lease.scope.account_id,
                oms_order_id=None,
                execution_id=lease.execution_id,
                attributes={
                    **_scope_payload(lease.scope),
                    "client_order_ref": lease.client_order_ref,
                    "allocation_sequence": str(lease.allocation_sequence),
                    "schema": "1",
                },
            )
            try:
                commit = self._journal.append_batch((fact,))
            except Exception:
                self._failed = True
                raise
            result = ClientOrderRefJournalCommit(self._journal_id, commit.final_lsn, lease)
            self._records[lease.execution_id] = result
            self._last_sequence = lease.allocation_sequence
            return result

    def read_committed_lease(
        self, scope: ClientOrderRefScope, execution_id: str
    ) -> ClientOrderRefLease | None:
        with self._lock:
            self._require_open(scope)
            record = self._records.get(execution_id)
            if record is None:
                return None
            self._verify_record(record)
            return record.lease

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._journal.close()
            finally:
                os.close(self._lock_fd)
                self._closed = True

    def _load(self) -> None:
        scan = self._journal.scan()
        if not scan.clean:
            raise MmapClientRefJournalErrorV1("client reference Journal has an untrusted tail")
        for row in scan.records:
            if row.ordinal != 0 or row.batch_size != 1:
                raise MmapClientRefJournalErrorV1("client reference batch is invalid")
            lease = self._decode(row.fact)
            if (
                lease.execution_id in self._records
                or lease.allocation_sequence <= self._last_sequence
            ):
                raise MmapClientRefJournalErrorV1("client reference Journal repeats or regresses")
            self._records[lease.execution_id] = ClientOrderRefJournalCommit(
                self._journal_id, row.lsn, lease
            )
            self._last_sequence = lease.allocation_sequence

    def _verify_record(self, record: ClientOrderRefJournalCommit) -> None:
        try:
            rows = self._journal.read_committed_batch(
                OmsJournalCommit(
                    self._journal_id, record.commit_sequence, record.commit_sequence, 1
                )
            )
            if self._decode(rows[0].fact) != record.lease:
                raise MmapClientRefJournalErrorV1("client reference fact changed")
        except Exception:
            self._failed = True
            raise

    def _decode(self, fact: OmsJournalFact) -> ClientOrderRefLease:
        attrs = dict(fact.attributes)
        expected = {**_scope_payload(self._scope), "schema": "1"}
        if (
            fact.kind is not OmsFactKind.CLIENT_ORDER_REF_ALLOCATED
            or fact.account_id != self._scope.account_id
            or fact.oms_order_id is not None
            or fact.execution_id is None
            or fact.recovery_payload is not None
            or set(attrs) != set(expected) | {"client_order_ref", "allocation_sequence"}
            or any(attrs[key] != value for key, value in expected.items())
        ):
            raise MmapClientRefJournalErrorV1("client reference fact scope is invalid")
        text = attrs["allocation_sequence"]
        if not text.isascii() or not text.isdigit() or str(int(text)) != text:
            raise MmapClientRefJournalErrorV1("client reference fact sequence is invalid")
        lease = ClientOrderRefLease(
            fact.execution_id, self._scope, attrs["client_order_ref"], int(text)
        )
        self._validate_lease(lease)
        if fact.fact_id != f"client-ref:{sha256(lease.execution_id.encode()).hexdigest()}":
            raise MmapClientRefJournalErrorV1("client reference fact identity is invalid")
        return lease

    @staticmethod
    def _validate_lease(lease: ClientOrderRefLease) -> None:
        if (
            not lease.client_order_ref.isascii()
            or not lease.client_order_ref.isdigit()
            or str(lease.allocation_sequence) != lease.client_order_ref
            or lease.allocation_sequence > 2**32 - 1
        ):
            raise MmapClientRefJournalErrorV1("EMT reference is not a canonical nonzero uint32")

    def _require_open(self, scope: ClientOrderRefScope) -> None:
        if self._closed or self._failed or scope != self._scope or self._journal.recovery_required:
            raise MmapClientRefJournalErrorV1("client reference scope is closed or mismatched")

    @staticmethod
    def _check_regular(path: Path) -> None:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
            raise MmapClientRefJournalErrorV1("client reference control file is unsafe")

    @staticmethod
    def _create_anchor(path: Path, payload: bytes) -> None:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            # Preserve incomplete anchor as a fail-closed recovery signal.
            raise
