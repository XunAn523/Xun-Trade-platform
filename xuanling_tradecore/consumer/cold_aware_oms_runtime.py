"""Exclusive, bounded-memory rebuild of hot/cold OMS coverage from retained Journal.

This correctness-first startup composition deliberately rebuilds disposable read
and cold indexes in a NEW bounded generation. It neither trusts stale negative
proofs nor claims incremental cold-index recovery / 60-second RTO. No listener,
Adapter or LONG_RUNNING gate is enabled here.
"""

from __future__ import annotations

import heapq
import json
import os
import re
import uuid
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast

import msgspec

from ..oms.journal_tail import SegmentedJournalTail
from ..oms.order_book import OmsOrderBookError
from ..oms.recovery import OmsRecoverySeedV1
from ..oms.recovery_checkpoint import (
    EventCheckpointRow,
    OmsCheckpointState,
    OrderCheckpointRow,
    decode_checkpoint,
)
from ..oms.retained_order_book import RetainedOrderBook
from ..oms.segmented_fact_journal import SegmentedOmsFactJournal
from ..persistence.cold_intent_index import ColdIntentIndex
from ..persistence.order_projection_store import OrderProjectionStore
from ..persistence.process_checkpoint_validator import ProcessCheckpointValidator
from ..persistence.process_segment_sealer import ProcessSegmentSealer
from ..persistence.runtime_files import (
    atomic_runtime_json,
    open_private,
    require_private_runtime_file,
)
from .runtime_identity import PersistentConsumerRuntimeIdentity
from .runtime_profile import ConsumerLongRunningPolicy

_GENERATION = re.compile(r"recovery-[0-9a-f]{32}\Z")


@dataclass(frozen=True)
class ColdAwareRecoveryStats:
    watermark: int
    active_orders: int
    cold_orders: int
    normalized_events: int
    recent_events: int
    accounts: int
    generation: str


def _private_directory(root: Path, *, create: bool) -> None:
    if create:
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
    if (
        root.is_symlink()
        or not root.is_dir()
        or (
            os.name == "posix"
            and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)
        )
    ):
        raise OmsOrderBookError("cold recovery directory is missing/unsafe")


class ColdAwareOmsRuntime:
    def __init__(
        self,
        *,
        root: Path,
        scope_sha256: str,
        policy: ConsumerLongRunningPolicy,
        initialize: bool = False,
        maximum_order_events: int = 64,
        maximum_accounts: int = 64,
        maximum_active_checkpoint_bytes: int = 64 * 1024**2,
        maximum_database_bytes: int = 8 * 1024**3,
        maximum_generations: int = 8,
        process_cold_validation: bool = False,
        cold_owner_cpu: int | None = None,
        segment_owner_cpu: int | None = None,
        scoped_cold_lookup_epochs: bool = False,
    ) -> None:
        if (
            type(initialize) is not bool
            or type(process_cold_validation) is not bool
            or type(scoped_cold_lookup_epochs) is not bool
            or type(maximum_order_events) is not int
            or not 3 <= maximum_order_events <= 1024
            or type(maximum_accounts) is not int
            or not 1 <= maximum_accounts <= 1024
            or type(maximum_active_checkpoint_bytes) is not int
            or not 262144 <= maximum_active_checkpoint_bytes <= 256 * 1024**2
            or type(maximum_database_bytes) is not int
            or not 1024**2 <= maximum_database_bytes <= 1024**4
            or type(maximum_generations) is not int
            or not 1 <= maximum_generations <= 64
        ):
            raise OmsOrderBookError("cold recovery budget is invalid")
        _private_directory(root, create=initialize)
        if initialize and any(root.iterdir()):
            raise OmsOrderBookError("cold recovery initialization requires an empty root")
        self._closed = False
        self._journal: SegmentedOmsFactJournal | None = None
        self._cold: ColdIntentIndex | None = None
        self._validator: ProcessCheckpointValidator | None = None
        self._sealer: ProcessSegmentSealer | None = None
        self.identity = PersistentConsumerRuntimeIdentity(
            root=root / "identity", scope_sha256=scope_sha256, create=initialize
        )
        store: OrderProjectionStore | None = None
        tail: SegmentedJournalTail | None = None
        try:
            if not initialize and not (root / "journal" / "segments.json").is_file():
                raise OmsOrderBookError("cold recovery lost its source Journal manifest")
            if process_cold_validation:
                self._sealer = ProcessSegmentSealer(
                    root=root / "journal",
                    journal_id=self.identity.journal_id,
                    frames=policy.segment_frames,
                    cpu=segment_owner_cpu,
                )
            journal = SegmentedOmsFactJournal(
                root=root / "journal",
                journal_id=self.identity.journal_id,
                segment_frames=policy.segment_frames,
                spare_segments=policy.spare_segments,
                disk_budget_bytes=policy.journal_disk_budget_bytes,
                segment_sealer=self._sealer,
                segment_preparer=self._sealer.prepare if self._sealer is not None else None,
            )
            self._journal = self.journal = journal
            watermark = journal.read_bound().committed_lsn
            self._validate_previous(root / "coverage.json", scope_sha256, watermark)
            recoveries = root / "recoveries"
            _private_directory(recoveries, create=True)
            count = 0
            for previous in recoveries.iterdir():
                if not _GENERATION.fullmatch(previous.name):
                    raise OmsOrderBookError("unknown recovery generation entry")
                _private_directory(previous, create=False)
                count += 1
                if count >= maximum_generations:
                    raise OmsOrderBookError(
                        "recovery generation capacity exhausted; explicit maintenance required"
                    )
            self.generation_root = recoveries / ("recovery-" + uuid.uuid4().hex)
            _private_directory(self.generation_root, create=True)
            self.projection_root = self.generation_root / "projection"
            store = OrderProjectionStore(
                self.projection_root,
                journal_id=journal.journal_id,
                profile_hash=journal.profile_hash,
                initialize=True,
                maximum_database_bytes=maximum_database_bytes,
                maximum_order_events=maximum_order_events,
            )
            tail = SegmentedJournalTail(journal, maximum_batch_facts=256)
            # Bind the original verified replay pass, not a second scan or a
            # hash of mutable coverage metadata. Startup-only, no hot-path work.
            prefix = sha256(
                msgspec.json.encode(
                    (
                        "oms-owner-recovered-prefix-v1",
                        journal.journal_id,
                        journal.profile_hash,
                        watermark,
                    )
                )
            )
            while tail.cursor.next_lsn <= watermark:
                batch = tail.read_next()
                if batch is None or batch.after.next_lsn - 1 > watermark:
                    raise OmsOrderBookError("startup cut is not a complete Journal prefix")
                store.apply(batch)
                prefix.update(bytes.fromhex(batch.sha256))
            tail.close()
            tail = None
            facts, orders, events = store.recovery_counts()
            if facts != watermark or journal.read_bound().committed_lsn != watermark:
                raise OmsOrderBookError("source changed during quiescent cold recovery")
            if process_cold_validation:
                self._validator = ProcessCheckpointValidator(
                    journal.journal_id,
                    database_path=self.generation_root / "cold" / "intents.sqlite",
                    maximum_database_bytes=maximum_database_bytes,
                    maximum_batch=256,
                    cpu=cold_owner_cpu,
                )
            cold = ColdIntentIndex(
                root=self.generation_root / "cold",
                journal_id=journal.journal_id,
                initialize=True,
                filter_bytes=policy.cold_filter_bytes,
                maximum_database_bytes=maximum_database_bytes,
                checkpoint_validator=self._validator,
                archive_committer=self._validator.commit if self._validator is not None else None,
                archive_close=self._validator.close if self._validator is not None else None,
                maximum_transaction_archives=4 if self._validator is not None else 1,
                maximum_transaction_checkpoints=256,
                scoped_lookup_epochs=scoped_cold_lookup_epochs,
            )
            self._cold = self.cold_index = cold
            active_rows: list[OrderCheckpointRow] = []
            active_events: list[EventCheckpointRow] = []
            recent: list[tuple[int, EventCheckpointRow]] = []
            account_counts: dict[str, int] = {}
            active_bytes = cold_count = seen_orders = seen_events = 0
            terminal_digest = sha256()
            last = ""
            while page := store.recovery_order_page(last, watermark=watermark):
                for order_id, encoded in page:
                    state = decode_checkpoint(encoded, maximum_bytes=262144)
                    if (
                        state.journal_id != journal.journal_id
                        or state.watermark > watermark
                        or len(state.orders) != 1
                    ):
                        raise OmsOrderBookError("recovery checkpoint scope is invalid")
                    initial = OmsRecoverySeedV1.decode_and_verify(
                        state.orders[0].admission_seed
                    ).order
                    if initial.order.oms_order_id != order_id:
                        raise OmsOrderBookError("recovery checkpoint order identity mismatch")
                    account_id = initial.order.account_id
                    if account_id not in account_counts:
                        if len(account_counts) >= maximum_accounts:
                            raise OmsOrderBookError("recovery account capacity exhausted")
                        account_counts[account_id] = 0
                    account_counts[account_id] += len(state.events)
                    seen_orders += 1
                    seen_events += len(state.events)
                    for event in state.events:
                        heapq.heappush(recent, (event.journal_lsn, event))
                        if len(recent) > policy.replay_cache_events:
                            heapq.heappop(recent)
                    if state.orders[0].state in {"FILLED", "CANCELED", "REJECTED"}:
                        receipt = cold.archive((encoded,)).result(timeout=30)
                        if not cold.validate(receipt) or receipt.digests != (
                            sha256(encoded).digest(),
                        ):
                            raise OmsOrderBookError("cold recovery commit receipt is unavailable")
                        terminal_digest.update(
                            msgspec.json.encode((order_id, sha256(encoded).hexdigest()))
                        )
                        cold_count += 1
                    else:
                        active_bytes += len(encoded)
                        if (
                            len(active_rows) >= policy.maximum_active_orders
                            or active_bytes > maximum_active_checkpoint_bytes
                        ):
                            raise OmsOrderBookError("active recovery checkpoint capacity exhausted")
                        active_rows.append(state.orders[0])
                        active_events.extend(state.events)
                    last = order_id
            if (seen_orders, seen_events) != (orders, events) or cold.stats().records != cold_count:
                raise OmsOrderBookError("hot/cold recovery coverage is incomplete")
            store.close()
            store = None
            active = (
                None
                if not watermark
                else msgspec.json.encode(
                    OmsCheckpointState(
                        1,
                        journal.journal_id,
                        watermark,
                        tuple(active_rows),
                        tuple(sorted(active_events, key=lambda row: row.journal_lsn)),
                    )
                )
            )
            if active is not None and len(active) > maximum_active_checkpoint_bytes:
                raise OmsOrderBookError("encoded active recovery checkpoint exceeds its budget")
            book = RetainedOrderBook(
                journal=journal,
                cold_index=cold,
                maximum_hot_orders=policy.maximum_active_orders,
                replay_cache_events=policy.replay_cache_events,
                maximum_order_events=maximum_order_events,
                maximum_accounts=maximum_accounts,
            )
            book.restore_cold_covered_checkpoint(
                active,
                expected_journal_id=journal.journal_id,
                event_counts=account_counts,
                recent_events=tuple(row for _, row in sorted(recent)),
                maximum_checkpoint_bytes=maximum_active_checkpoint_bytes,
            )
            if journal.read_bound().committed_lsn != watermark:
                raise OmsOrderBookError("source advanced before cold coverage publication")
            self._active_path = self.generation_root / "active-checkpoint.json"
            payload = b"null" if active is None else active
            with os.fdopen(
                open_private(self._active_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY), "wb"
            ) as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            self.stats = ColdAwareRecoveryStats(
                watermark,
                len(active_rows),
                cold_count,
                events,
                len(recent),
                len(account_counts),
                self.generation_root.name,
            )
            atomic_runtime_json(
                root / "coverage.json",
                {
                    "schema_version": 1,
                    "scope_sha256": scope_sha256,
                    "journal_id": journal.journal_id,
                    "watermark": watermark,
                    "generation": self.generation_root.name,
                    "active_orders": len(active_rows),
                    "cold_orders": cold_count,
                    "active_sha256": sha256(payload).hexdigest(),
                    "cold_sha256": terminal_digest.hexdigest(),
                },
            )
            self.order_book = book
            self.recovered_watermark = watermark
            self.recovered_source_sha256 = prefix.hexdigest()
        except BaseException:
            self._close_resources((tail, store))
            raise

    def _validate_previous(self, path: Path, scope: str, watermark: int) -> None:
        if not path.exists() and not path.is_symlink():
            return  # First startup may have crashed before publishing; source is still mandatory.
        require_private_runtime_file(path)
        if path.stat().st_size > 8192:
            raise OmsOrderBookError("recovery publication is oversized")
        data: object = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise OmsOrderBookError("recovery publication is invalid")
        item = cast(dict[str, object], data)
        position, name = item.get("watermark"), item.get("generation")
        if (
            set(item)
            != {
                "schema_version",
                "scope_sha256",
                "journal_id",
                "watermark",
                "generation",
                "active_orders",
                "cold_orders",
                "active_sha256",
                "cold_sha256",
            }
            or type(item.get("schema_version")) is not int
            or item.get("schema_version") != 1
            or item.get("scope_sha256") != scope
            or item.get("journal_id") != self.identity.journal_id
            or type(position) is not int
            or not 0 <= position <= watermark
            or not isinstance(name, str)
            or not _GENERATION.fullmatch(name)
        ):
            raise OmsOrderBookError("recovery publication scope/watermark is invalid")
        for key in ("active_orders", "cold_orders"):
            value = item[key]
            if type(value) is not int or not 0 <= value <= position:
                raise OmsOrderBookError("recovery publication counts are invalid")
        for key in ("active_sha256", "cold_sha256"):
            value = item[key]
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
                raise OmsOrderBookError("recovery publication digest is invalid")

    def checkpoint_recovered_state(self) -> Path:
        """Only the unchanged, quiescent startup cut, not an active writer checkpoint."""
        if (
            self._closed
            or not self.order_book.recovery_is_quiescent
            or self.journal.read_bound().committed_lsn != self.recovered_watermark
        ):
            raise OmsOrderBookError("active/closed recovery state cannot publish checkpoint")
        return self._active_path

    def close(self) -> None:
        if self._closed:
            return
        self._close_resources(())

    @property
    def shutdown_verified(self) -> bool:
        return (
            self._closed
            and (self._sealer is None or self._sealer.shutdown_verified)
            and (self._validator is None or self._validator.shutdown_verified)
        )

    def _close_resources(
        self, bootstrap: tuple[SegmentedJournalTail | OrderProjectionStore | None, ...]
    ) -> None:
        failed = False
        for resource in (*bootstrap, self._cold, self._validator, self._journal):
            if resource is not None:
                try:
                    resource.close()
                except BaseException:
                    failed = True
        # Journal.close owns the backend drain before releasing its lease.
        # If construction failed before a Journal existed, drain it here too.
        if self._journal is None and self._sealer is not None:
            try:
                self._sealer.close()
            except BaseException:
                failed = True
        if failed:
            # Keep the identity lease while an old worker might still own work.
            raise OmsOrderBookError("cold recovery cleanup did not drain") from None
        self.identity.close()
        self._closed = True
