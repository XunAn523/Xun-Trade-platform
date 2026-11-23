from __future__ import annotations

import base64
import mmap
import tempfile
import unittest
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters import ClientOrderRefAllocationRequest
from xuanling_tradecore.adapters.emt import (
    EmtAuthoritativeOrderScanSeriesCollectorV1,
    EmtAuthoritativeOrderV1,
    EmtAuthoritativeRecoveryReconcilerV1,
    EmtAuthoritativeScanV1,
    EmtNamespaceRecoveryError,
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    ExecutionNamespaceFenceV1,
    MmapRouteLedgerError,
    MmapRouteLedgerV2,
    NamespaceBoundEmtClientOrderIdAllocator,
    NamespaceOwnership,
    OrderClientIdNamespaceDirectoryV1,
    OrderClientIdNamespaceLeaseV1,
    RecoveryDispositionV1,
    RecoveryOrderClassification,
    RecoveryRehydratedFactV1,
    RouteLedgerFactV2,
    RouteLedgerV2,
    SignedOrderClientIdNamespaceLeaseV1,
    VerifiedNamespaceBoundEmtClientOrderIdAllocator,
    load_single_native_acceptance_target_v1,
)
from xuanling_tradecore.adapters.emt.identity import EmtClientOrderRefScope
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.oms.identity_journal import InMemoryClientOrderRefJournal

_NOW = 1_000_000
_DAY = TradingDayEpoch("2026-09-10")
_RELEASE_HASH = sha256(b"release").hexdigest()
_ROUTE_HASH = sha256(b"route").hexdigest()


def _scope() -> EmtOrderClientNamespaceScope:
    return EmtOrderClientNamespaceScope("sim-account", _DAY, 101)


def _signed(
    private_key: Ed25519PrivateKey,
    *,
    namespace_id: str,
    owner: str,
    start: int,
    end: int,
) -> SignedOrderClientIdNamespaceLeaseV1:
    lease = OrderClientIdNamespaceLeaseV1(
        scope=_scope(),
        namespace_id=namespace_id,
        owner_system_id=owner,
        adapter_instance_id=f"adapter-{owner}",
        range_start=start,
        range_end=end,
        manifest_version=1,
        valid_from_wall_ns=0,
        valid_until_wall_ns=_NOW + 1,
        release_manifest_hash=_RELEASE_HASH,
        signer_id="test-signer",
    )
    unsigned = SignedOrderClientIdNamespaceLeaseV1(
        lease=lease,
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


def _directory() -> tuple[OrderClientIdNamespaceDirectoryV1, Ed25519PrivateKey]:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    verifier = EmtOrderClientNamespaceVerifier(
        {"test-signer": public_key},
        revoked_signer_ids=frozenset(),
    )
    own = _signed(
        private_key, namespace_id="tradecore-a", owner="tradecore-a", start=1000, end=1999
    )
    foreign = _signed(private_key, namespace_id="system-b", owner="system-b", start=2000, end=2999)
    return (
        OrderClientIdNamespaceDirectoryV1(
            own=own,
            foreign=(foreign,),
            verifier=verifier,
            wall_ns=_NOW,
        ),
        private_key,
    )


def _route(
    directory: OrderClientIdNamespaceDirectoryV1,
    *,
    order_client_id: int = 1000,
    execution_id: str = "execution-a",
    order_emt_id: int | None = None,
) -> RouteLedgerFactV2:
    own = directory.own
    return RouteLedgerFactV2(
        scope=directory.scope,
        order_client_id=order_client_id,
        namespace_id=own.namespace_id,
        namespace_manifest_hash=directory.manifest_hash,
        owner_system_id=own.owner_system_id,
        adapter_instance_id=own.adapter_instance_id,
        strategy_id="strategy-a",
        execution_id=execution_id,
        journal_epoch=1,
        frame_sequence=1,
        route_payload_hash=_ROUTE_HASH,
        order_emt_id=order_emt_id,
    )


class _InMemoryRecoverySink:
    """Test sink that is atomic within this process by construction."""

    def __init__(self) -> None:
        self.batches: list[tuple[RecoveryRehydratedFactV1, ...]] = []

    def append_rehydration_batch(self, facts: tuple[RecoveryRehydratedFactV1, ...]) -> None:
        self.batches.append(facts)


class _FailingRecoverySink:
    """A failed durable append must leave the recovery projection untouched."""

    def append_rehydration_batch(self, facts: tuple[RecoveryRehydratedFactV1, ...]) -> None:
        del facts
        raise OSError("simulated durable sink failure")


class EmtNamespaceRecoveryTests(unittest.TestCase):
    def test_r2_rehydrates_only_own_order_and_preserves_foreign_order(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory))
        own_order = EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100)
        foreign_order = EmtAuthoritativeOrderV1(2000, 5002, 1, 0, 100)
        scan = EmtAuthoritativeScanV1("scan-a", (own_order, foreign_order), True)

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_InMemoryRecoverySink(),
        ).reconcile(first_scan=scan, second_scan=scan, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERED_DISARMED, result.disposition)
        self.assertEqual(("execution-a",), result.rehydrated_execution_ids)
        self.assertEqual(
            (
                RecoveryOrderClassification.OWN_R2_ORPHAN,
                RecoveryOrderClassification.FOREIGN_RESERVED,
            ),
            tuple(item.classification for item in result.reconciled_orders),
        )
        rehydrated = ledger.route_for_client_id(1000)
        assert rehydrated is not None
        self.assertEqual(5001, rehydrated.order_emt_id)
        self.assertIsNone(ledger.route_for_client_id(2000))

    def test_unknown_namespace_unstable_scan_and_callback_gap_block_recovery(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        first = EmtAuthoritativeScanV1("first", (), True)
        second = EmtAuthoritativeScanV1(
            "second",
            (EmtAuthoritativeOrderV1(3000, 7001, 1, 0, 1),),
            True,
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_InMemoryRecoverySink(),
        ).reconcile(first_scan=first, second_scan=second, callback_gaps_closed=False)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertIn("AUTHORITATIVE_DOUBLE_SCAN_UNSTABLE", result.reason_codes)
        self.assertIn("CALLBACK_GAP_OPEN", result.reason_codes)
        self.assertIn("UNKNOWN_NAMESPACE", result.reason_codes)
        self.assertEqual(
            RecoveryOrderClassification.UNKNOWN_NAMESPACE,
            result.reconciled_orders[0].classification,
        )

    def test_collision_and_unresolved_own_route_are_blocked(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory, order_emt_id=9001))
        collision = EmtAuthoritativeOrderV1(1000, 9002, 1, 0, 1)
        unresolved = EmtAuthoritativeOrderV1(1001, 9003, 1, 0, 1)
        scan = EmtAuthoritativeScanV1("complete", (collision, unresolved), True)

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_InMemoryRecoverySink(),
        ).reconcile(first_scan=scan, second_scan=scan, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertIn("NATIVE_ORDER_ID_CONFLICT", result.reason_codes)
        self.assertIn("OWN_ROUTE_UNRESOLVED", result.reason_codes)

    def test_duplicate_client_id_is_preserved_and_classified_as_a_namespace_collision(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory, order_emt_id=9001))
        scan = EmtAuthoritativeScanV1(
            "duplicate-client-id",
            (
                EmtAuthoritativeOrderV1(1000, 9001, 1, 0, 1),
                EmtAuthoritativeOrderV1(1000, 9002, 1, 0, 1),
            ),
            True,
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_InMemoryRecoverySink(),
        ).reconcile(first_scan=scan, second_scan=scan, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertIn("NATIVE_ORDER_ID_CONFLICT", result.reason_codes)
        self.assertEqual(
            (
                RecoveryOrderClassification.OWN_MATCHED,
                RecoveryOrderClassification.NAMESPACE_COLLISION,
            ),
            tuple(item.classification for item in result.reconciled_orders),
        )

    def test_foreign_only_drift_does_not_block_stable_owned_r2_rehydration(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory))
        own = EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100)
        first = EmtAuthoritativeScanV1(
            "first",
            (own, EmtAuthoritativeOrderV1(2000, 6001, 1, 0, 100)),
            True,
        )
        second = EmtAuthoritativeScanV1(
            "second",
            (own, EmtAuthoritativeOrderV1(2001, 6002, 1, 0, 100)),
            True,
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_InMemoryRecoverySink(),
        ).reconcile(first_scan=first, second_scan=second, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERED_DISARMED, result.disposition)
        self.assertTrue(result.stable_double_scan)
        self.assertTrue(result.foreign_drift_detected)
        self.assertEqual(2, result.foreign_drift_order_count)
        self.assertEqual(("execution-a",), result.rehydrated_execution_ids)

    def test_blocked_recovery_never_rehydrates_an_owned_route(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory))
        first = EmtAuthoritativeScanV1(
            "first", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
        )
        second = EmtAuthoritativeScanV1(
            "second",
            (
                EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),
                EmtAuthoritativeOrderV1(3000, 7001, 1, 0, 100),
            ),
            True,
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
        ).reconcile(first_scan=first, second_scan=second, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertEqual((), result.rehydrated_execution_ids)
        route = ledger.route_for_client_id(1000)
        assert route is not None
        self.assertIsNone(route.order_emt_id)

    def test_r2_candidate_without_a_durable_sink_is_blocked(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory))
        scan = EmtAuthoritativeScanV1(
            "complete", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
        ).reconcile(first_scan=scan, second_scan=scan, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertIn("RECOVERY_REHYDRATION_SINK_MISSING", result.reason_codes)
        route = ledger.route_for_client_id(1000)
        assert route is not None
        self.assertIsNone(route.order_emt_id)

    def test_failed_durable_rehydration_does_not_mutate_the_route_ledger(self) -> None:
        directory, _private_key = _directory()
        ledger = RouteLedgerV2(directory)
        ledger.record_pre_dispatch(_route(directory))
        scan = EmtAuthoritativeScanV1(
            "complete", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
        )

        result = EmtAuthoritativeRecoveryReconcilerV1(
            directory=directory,
            ledger=ledger,
            rehydration_sink=_FailingRecoverySink(),
        ).reconcile(first_scan=scan, second_scan=scan, callback_gaps_closed=True)

        self.assertEqual(RecoveryDispositionV1.RECOVERY_BLOCKED, result.disposition)
        self.assertIn("RECOVERY_REHYDRATION_PERSIST_FAILED", result.reason_codes)
        self.assertEqual((), result.rehydrated_execution_ids)
        route = ledger.route_for_client_id(1000)
        assert route is not None
        self.assertIsNone(route.order_emt_id)

    def test_namespace_allocator_never_leaves_its_verified_range(self) -> None:
        directory, _private_key = _directory()
        journal = InMemoryClientOrderRefJournal()
        scope = EmtClientOrderRefScope(
            account_id="sim-account",
            trading_day=_DAY,
            adapter_instance_id="adapter-tradecore-a",
            emt_client_id=101,
        )
        allocator = NamespaceBoundEmtClientOrderIdAllocator(
            emt_scope=scope,
            namespace=directory.own,
            journal=journal,
        )
        allocator.recover_from_journal()
        first = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        )
        second = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-2", scope.canonical_scope)
        )
        self.assertEqual(("1000", "1001"), (first.client_order_ref, second.client_order_ref))
        self.assertEqual(
            first,
            allocator.allocate(
                ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
            ),
        )

    def test_verified_namespace_allocator_takes_a_directory_not_a_raw_lease(self) -> None:
        directory, _private_key = _directory()
        scope = EmtClientOrderRefScope(
            account_id="sim-account",
            trading_day=_DAY,
            adapter_instance_id="adapter-tradecore-a",
            emt_client_id=101,
        )
        allocator = VerifiedNamespaceBoundEmtClientOrderIdAllocator(
            emt_scope=scope,
            directory=directory,
            journal=InMemoryClientOrderRefJournal(),
        )
        allocator.recover_from_journal()
        lease = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-verified", scope.canonical_scope)
        )
        self.assertEqual("1000", lease.client_order_ref)

    def test_directory_rejects_overlapping_ranges_before_session_start(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        verifier = EmtOrderClientNamespaceVerifier(
            {"test-signer": public_key},
            revoked_signer_ids=frozenset(),
        )
        own = _signed(private_key, namespace_id="a", owner="a", start=1000, end=1999)
        overlap = _signed(private_key, namespace_id="b", owner="b", start=1999, end=2999)
        with self.assertRaisesRegex(EmtNamespaceRecoveryError, "overlap"):
            OrderClientIdNamespaceDirectoryV1(
                own=own,
                foreign=(overlap,),
                verifier=verifier,
                wall_ns=_NOW,
            )

    def test_namespace_lookup_keeps_foreign_order_outside_local_route_ledger(self) -> None:
        directory, _private_key = _directory()
        self.assertIs(NamespaceOwnership.OWN, directory.lookup(1000).ownership)
        self.assertIs(NamespaceOwnership.FOREIGN_RESERVED, directory.lookup(2000).ownership)
        self.assertIs(NamespaceOwnership.UNKNOWN_NAMESPACE, directory.lookup(3000).ownership)
        self.assertIs(NamespaceOwnership.UNKNOWN_NAMESPACE, directory.lookup(0).ownership)

    def test_mmap_route_ledger_rebuilds_r2_fact_after_process_restart(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "route-ledger.mmap"
            writer = MmapRouteLedgerV2(path=path, capacity=4, directory=directory)
            writer.append_pre_dispatch(_route(directory))
            writer.close()

            reopened = MmapRouteLedgerV2(path=path, capacity=4, directory=directory)
            ledger = reopened.load_route_ledger()
            result = EmtAuthoritativeRecoveryReconcilerV1(
                directory=directory,
                ledger=ledger,
                rehydration_sink=reopened,
            ).reconcile(
                first_scan=EmtAuthoritativeScanV1(
                    "first", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
                ),
                second_scan=EmtAuthoritativeScanV1(
                    "second", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
                ),
                callback_gaps_closed=True,
            )
            self.assertEqual(RecoveryDispositionV1.RECOVERED_DISARMED, result.disposition)
            self.assertEqual(("execution-a",), result.rehydrated_execution_ids)
            reopened.close()

            recovered_reader = MmapRouteLedgerV2(path=path, capacity=4, directory=directory)
            recovered = recovered_reader.load_route_ledger().route_for_client_id(1000)
            assert recovered is not None
            self.assertEqual(5001, recovered.order_emt_id)
            recovered_reader.close()

    def test_read_only_cleanup_target_requires_one_committed_acceptance(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "route-ledger.mmap"
            writer = MmapRouteLedgerV2(path=path, capacity=64, directory=directory)
            writer.append_pre_dispatch(_route(directory))
            writer.append_native_accepted(order_client_id=1000, order_emt_id=5001)
            writer.close()

            target = load_single_native_acceptance_target_v1(path)
            self.assertEqual(1000, target.order_client_id)
            self.assertEqual(5001, target.order_emt_id)

    def test_read_only_cleanup_target_accepts_one_committed_r2_rehydration(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "route-ledger.mmap"
            writer = MmapRouteLedgerV2(path=path, capacity=64, directory=directory)
            writer.append_pre_dispatch(_route(directory))
            result = EmtAuthoritativeRecoveryReconcilerV1(
                directory=directory,
                ledger=writer.load_route_ledger(),
                rehydration_sink=writer,
            ).reconcile(
                first_scan=EmtAuthoritativeScanV1(
                    "first", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
                ),
                second_scan=EmtAuthoritativeScanV1(
                    "second", (EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),), True
                ),
                callback_gaps_closed=True,
            )
            self.assertEqual(RecoveryDispositionV1.RECOVERED_DISARMED, result.disposition)
            writer.close()

            target = load_single_native_acceptance_target_v1(path)
            self.assertEqual(1000, target.order_client_id)
            self.assertEqual(5001, target.order_emt_id)

    def test_mmap_route_ledger_torn_tail_closes_recovery(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "route-ledger.mmap"
            writer = MmapRouteLedgerV2(path=path, capacity=2, directory=directory)
            writer.append_pre_dispatch(_route(directory))
            writer.close()
            with path.open("r+b", buffering=0) as file:
                mapped = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
                mapped[4_096 + 2_048 - 8 : 4_096 + 2_048] = b"\0" * 8
                mapped.close()
            reopened = MmapRouteLedgerV2(path=path, capacity=2, directory=directory)
            self.assertTrue(reopened.recovery_required)
            with self.assertRaisesRegex(MmapRouteLedgerError, "untrusted"):
                reopened.load_route_ledger()
            reopened.close()

    def test_mmap_route_ledger_incomplete_rehydration_batch_blocks_recovery(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "route-ledger.mmap"
            writer = MmapRouteLedgerV2(path=path, capacity=4, directory=directory)
            writer.append_pre_dispatch(_route(directory))
            writer._append(  # noqa: SLF001 - deliberate physical crash boundary fixture.
                3,
                {
                    "batch_id": "a" * 64,
                    "fact_count": 1,
                    "facts_hash": "b" * 64,
                },
            )
            writer.close()

            reopened = MmapRouteLedgerV2(path=path, capacity=4, directory=directory)
            with self.assertRaisesRegex(MmapRouteLedgerError, "batch is incomplete"):
                reopened.load_route_ledger()
            reopened.close()

    def test_authoritative_scan_series_rejects_overlapping_scans(self) -> None:
        series = EmtAuthoritativeOrderScanSeriesCollectorV1()
        series.begin(scan_id="scan-a")
        with self.assertRaisesRegex(EmtNamespaceRecoveryError, "active"):
            series.begin(scan_id="scan-b")
        series.on_authoritative_order_terminal(query_kind="orders", error_id=0)
        self.assertTrue(series.seal_current().complete)

    def test_authoritative_scan_series_closes_only_on_last_orders_page(self) -> None:
        series = EmtAuthoritativeOrderScanSeriesCollectorV1()
        series.begin(scan_id="paged-scan")
        series.on_authoritative_order_row(
            query_kind="orders_by_page",
            order=EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),
        )
        series.on_authoritative_order_terminal(
            query_kind="orders_by_page", error_id=0, page_has_more=True
        )
        self.assertFalse(series.seal_current().complete)

        series.begin(scan_id="paged-scan-complete")
        series.on_authoritative_order_row(
            query_kind="orders_by_page",
            order=EmtAuthoritativeOrderV1(1000, 5001, 1, 0, 100),
        )
        series.on_authoritative_order_terminal(
            query_kind="orders_by_page", error_id=0, page_has_more=False
        )
        self.assertTrue(series.seal_current().complete)

    def test_namespace_fence_is_local_and_fail_closed_on_existing_claim(self) -> None:
        directory, _private_key = _directory()
        with tempfile.TemporaryDirectory() as temporary:
            first = ExecutionNamespaceFenceV1(
                runtime_control_root=Path(temporary), directory=directory
            )
            second = ExecutionNamespaceFenceV1(
                runtime_control_root=Path(temporary), directory=directory
            )
            first.acquire(instance_id="instance-a")
            with self.assertRaisesRegex(EmtNamespaceRecoveryError, "already exists"):
                second.acquire(instance_id="instance-b")
            first.release()
            second.acquire(instance_id="instance-b")
            second.release()


if __name__ == "__main__":
    unittest.main()
