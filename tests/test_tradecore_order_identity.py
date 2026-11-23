from __future__ import annotations

import ast
import inspect
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import (
    CanonicalVenueOrderIdentityLifecycle,
    ClientOrderRefAllocationError,
    ClientOrderRefAllocationRequest,
    ClientOrderRefAllocator,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
    VenueOrderIdentityLifecycle,
    canonical_venue_order_identity_hash,
    canonical_venue_order_target_hash,
)
from xuanling_tradecore.adapters.emt import (
    EMT_CLIENT_ORDER_ID_MAX,
    EmtClientOrderIdAllocator,
    EmtClientOrderRefScope,
)
from xuanling_tradecore.domain import TradingDayEpoch, VenueOrderIdentity

ROOT = Path(__file__).resolve().parents[1]
ADAPTERS_ROOT = ROOT / "xuanling_tradecore" / "adapters"


def _emt_scope(
    *,
    account_id: str = "account-1",
    trading_day: str = "2026-08-31",
    adapter_instance_id: str = "emt-sim-v1",
    emt_client_id: int = 7,
) -> EmtClientOrderRefScope:
    return EmtClientOrderRefScope(
        account_id=account_id,
        trading_day=TradingDayEpoch(trading_day),
        adapter_instance_id=adapter_instance_id,
        emt_client_id=emt_client_id,
    )


def _recovery(
    scope: EmtClientOrderRefScope,
    *,
    watermark: int = 0,
    leases: tuple[ClientOrderRefLease, ...] = (),
    is_complete: bool = True,
) -> ClientOrderRefRecoveryInput:
    return ClientOrderRefRecoveryInput(
        recovery_evidence_id="recovery-evidence-1",
        journal_id="journal-1",
        scope=scope.canonical_scope,
        committed_allocation_sequence=watermark,
        leases=leases,
        is_complete=is_complete,
    )


class TradeCoreOrderIdentityTests(unittest.TestCase):
    def test_emt_allocator_requires_complete_recovery_and_issues_monotonic_nonzero_uint32(
        self,
    ) -> None:
        scope = _emt_scope()
        allocator = EmtClientOrderIdAllocator(scope)
        self.assertIsInstance(allocator, ClientOrderRefAllocator)
        first_request = ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        with self.assertRaisesRegex(
            ClientOrderRefAllocationError, "requires complete Journal recovery"
        ):
            allocator.allocate(first_request)

        recovered_lease = ClientOrderRefLease(
            execution_id="recovered-execution",
            scope=scope.canonical_scope,
            client_order_ref="41",
            allocation_sequence=41,
        )
        allocator.recover(_recovery(scope, watermark=41, leases=(recovered_lease,)))
        first = allocator.allocate(first_request)
        second = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-2", scope.canonical_scope)
        )
        self.assertEqual(("42", 42), (first.client_order_ref, first.allocation_sequence))
        self.assertEqual(("43", 43), (second.client_order_ref, second.allocation_sequence))
        self.assertEqual(first, allocator.allocate(first_request))
        self.assertNotEqual("0", first.client_order_ref)

    def test_emt_allocator_rejects_incomplete_cross_scope_bad_and_exhausted_recovery(self) -> None:
        scope = _emt_scope()
        allocator = EmtClientOrderIdAllocator(scope)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "incomplete"):
            allocator.recover(_recovery(scope, is_complete=False))

        other_scope = _emt_scope(emt_client_id=8)
        allocator = EmtClientOrderIdAllocator(scope)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "another client order id scope"):
            allocator.recover(_recovery(other_scope))

        bad_lease = ClientOrderRefLease(
            execution_id="bad-execution",
            scope=scope.canonical_scope,
            client_order_ref="4",
            allocation_sequence=3,
        )
        allocator = EmtClientOrderIdAllocator(scope)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "does not equal"):
            allocator.recover(_recovery(scope, watermark=3, leases=(bad_lease,)))

        allocator = EmtClientOrderIdAllocator(scope)
        allocator.recover(_recovery(scope, watermark=EMT_CLIENT_ORDER_ID_MAX))
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "exhausted"):
            allocator.allocate(
                ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
            )

    def test_caller_cannot_supply_or_overwrite_native_identity_keys(self) -> None:
        self.assertNotIn(
            "client_order_ref",
            inspect.signature(ClientOrderRefAllocationRequest).parameters,
        )
        scope = _emt_scope()
        allocator = EmtClientOrderIdAllocator(scope)
        allocator.recover(_recovery(scope))
        lease = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        )
        lifecycle = CanonicalVenueOrderIdentityLifecycle(scope.canonical_scope)
        self.assertIsInstance(lifecycle, VenueOrderIdentityLifecycle)
        allocated = lifecycle.allocated(lease)
        observed = lifecycle.observe_venue_order_id(allocated, venue_order_id="10001")
        canceled = lifecycle.observe_venue_cancel_id(observed, venue_cancel_id="20001")
        self.assertEqual(
            ("1", "10001", "20001"),
            (
                canceled.client_order_ref,
                canceled.venue_order_id,
                canceled.venue_cancel_id,
            ),
        )
        self.assertEqual(
            observed, lifecycle.observe_venue_order_id(observed, venue_order_id="10001")
        )
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "cannot be overwritten"):
            lifecycle.observe_venue_order_id(observed, venue_order_id="10002")
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "another scope"):
            lifecycle.observe_venue_cancel_id(
                VenueOrderIdentity("EMT", "other-adapter", "7", client_order_ref="1"),
                venue_cancel_id="20001",
            )

    def test_recovery_input_rejects_duplicate_or_uncommitted_leases(self) -> None:
        scope = _emt_scope().canonical_scope
        first = ClientOrderRefLease("execution-1", scope, "1", 1)
        duplicate_ref = ClientOrderRefLease("execution-2", scope, "1", 2)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "repeats a client order ref"):
            ClientOrderRefRecoveryInput(
                "evidence-1", "journal-1", scope, 2, (first, duplicate_ref), True
            )
        future = ClientOrderRefLease("execution-3", scope, "3", 3)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "exceeds committed"):
            ClientOrderRefRecoveryInput("evidence-1", "journal-1", scope, 2, (future,), True)

    def test_cancel_route_identity_digest_is_versioned_and_field_complete(self) -> None:
        scope = _emt_scope()
        allocator = EmtClientOrderIdAllocator(scope)
        allocator.recover(_recovery(scope))
        lease = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        )
        lifecycle = CanonicalVenueOrderIdentityLifecycle(scope.canonical_scope)
        with_order = lifecycle.observe_venue_order_id(
            lifecycle.allocated(lease), venue_order_id="10001"
        )
        with_cancel = lifecycle.observe_venue_cancel_id(with_order, venue_cancel_id="20001")
        self.assertEqual(
            canonical_venue_order_identity_hash(with_cancel),
            canonical_venue_order_identity_hash(with_cancel),
        )
        self.assertNotEqual(
            canonical_venue_order_identity_hash(with_order),
            canonical_venue_order_identity_hash(with_cancel),
        )
        self.assertEqual(
            canonical_venue_order_target_hash(with_order),
            canonical_venue_order_target_hash(with_cancel),
        )

    def test_identity_contracts_do_not_import_sdk_or_legacy_gateway(self) -> None:
        # Order-identity purity applies to the vendor-neutral identity
        # contracts.  It does not prohibit QMT Edge's separate SQLite WAL
        # durability implementation under ``adapters/qmt``.
        forbidden_roots = {"emt_gateway", "vnemttrader", "qmt", "psycopg", "sqlite3"}
        for source in (ADAPTERS_ROOT / "base.py", ADAPTERS_ROOT / "identity.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported_names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported_names = (node.module,)
                else:
                    continue
                for imported_name in imported_names:
                    self.assertNotIn(
                        imported_name.split(".", maxsplit=1)[0],
                        forbidden_roots,
                        f"{source.relative_to(ROOT)} imports a forbidden dependency",
                    )


if __name__ == "__main__":
    unittest.main()
