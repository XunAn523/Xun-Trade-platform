"""M1-02 contract tests for Journal-backed venue-client order references."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import (
    ClientOrderRefAllocationError,
    ClientOrderRefAllocationRequest,
    ClientOrderRefLease,
    ClientOrderRefRecoveryInput,
)
from xuanling_tradecore.adapters.emt import (
    EmtClientOrderRefScope,
    JournalBackedEmtClientOrderIdAllocator,
)
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.oms import (
    InMemoryClientOrderRefJournal,
    JournalBackedVenueOrderIdentityLifecycle,
)

ROOT = Path(__file__).resolve().parents[1]


class _FailingLeaseJournal(InMemoryClientOrderRefJournal):
    def __init__(self) -> None:
        super().__init__("failing-journal")
        self.fail_next_append = True

    def append_lease(self, lease: ClientOrderRefLease):
        if self.fail_next_append:
            self.fail_next_append = False
            raise OSError("injected Journal append failure")
        return super().append_lease(lease)


def _scope(*, trading_day: str = "2026-08-31", emt_client_id: int = 7) -> EmtClientOrderRefScope:
    return EmtClientOrderRefScope(
        account_id="account-1",
        trading_day=TradingDayEpoch(trading_day),
        adapter_instance_id="emt-sim-v1",
        emt_client_id=emt_client_id,
    )


class TradeCoreJournalIdentityTests(unittest.TestCase):
    def test_emt_allocator_reads_exact_journal_recovery_and_never_reuses_committed_uint32(
        self,
    ) -> None:
        journal = InMemoryClientOrderRefJournal("identity-journal-1")
        scope = _scope()
        allocator = JournalBackedEmtClientOrderIdAllocator(scope, journal)
        request = ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        with self.assertRaisesRegex(
            ClientOrderRefAllocationError, "requires complete Journal recovery"
        ):
            allocator.allocate(request)

        allocator.recover_from_journal()
        first = allocator.allocate(request)
        self.assertEqual(("1", 1), (first.client_order_ref, first.allocation_sequence))
        self.assertEqual(first, allocator.allocate(request))
        self.assertEqual(1, len(journal.lease_records))

        restarted = JournalBackedEmtClientOrderIdAllocator(scope, journal)
        restarted.recover_from_journal()
        second = restarted.allocate(
            ClientOrderRefAllocationRequest("execution-2", scope.canonical_scope)
        )
        self.assertEqual(("2", 2), (second.client_order_ref, second.allocation_sequence))
        self.assertEqual(
            ("1", "2"), tuple(record.lease.client_order_ref for record in journal.lease_records)
        )

    def test_allocator_rejects_incomplete_or_non_journal_recovery_and_does_not_advance_on_append_failure(
        self,
    ) -> None:
        journal = _FailingLeaseJournal()
        scope = _scope()
        allocator = JournalBackedEmtClientOrderIdAllocator(scope, journal)
        journal.mark_recovery_complete(scope.canonical_scope, is_complete=False)
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "incomplete"):
            allocator.recover_from_journal()

        journal.mark_recovery_complete(scope.canonical_scope, is_complete=True)
        forged = ClientOrderRefRecoveryInput(
            recovery_evidence_id="forged-evidence",
            journal_id="forged-journal",
            scope=scope.canonical_scope,
            committed_allocation_sequence=0,
            leases=(),
            is_complete=True,
        )
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "configured Journal"):
            allocator.recover(forged)

        allocator.recover_from_journal()
        request = ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        with self.assertRaises(OSError):
            allocator.allocate(request)
        self.assertEqual((), journal.lease_records)
        first = allocator.allocate(request)
        self.assertEqual(("1", 1), (first.client_order_ref, first.allocation_sequence))

    def test_venue_identity_persists_monotonically_and_requires_its_committed_lease(self) -> None:
        journal = InMemoryClientOrderRefJournal("identity-journal-2")
        scope = _scope()
        allocator = JournalBackedEmtClientOrderIdAllocator(scope, journal)
        allocator.recover_from_journal()
        lease = allocator.allocate(
            ClientOrderRefAllocationRequest("execution-1", scope.canonical_scope)
        )
        lifecycle = JournalBackedVenueOrderIdentityLifecycle(
            scope.canonical_scope,
            lease_journal=journal,
            identity_journal=journal,
        )
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "exact committed"):
            lifecycle.allocated(
                ClientOrderRefLease(
                    execution_id="uncommitted-execution",
                    scope=scope.canonical_scope,
                    client_order_ref="2",
                    allocation_sequence=2,
                )
            )
        initial = lifecycle.allocated(lease)
        observed = lifecycle.observe_venue_order_id(initial, venue_order_id="order-1001")
        canceled = lifecycle.observe_venue_cancel_id(observed, venue_cancel_id="cancel-1001")
        self.assertEqual(
            ("1", "order-1001", "cancel-1001"),
            (canceled.client_order_ref, canceled.venue_order_id, canceled.venue_cancel_id),
        )
        self.assertEqual(3, len(journal.identity_records))
        restarted = JournalBackedVenueOrderIdentityLifecycle(
            scope.canonical_scope,
            lease_journal=journal,
            identity_journal=journal,
        )
        self.assertEqual(canceled, restarted.allocated(lease))
        with self.assertRaisesRegex(ClientOrderRefAllocationError, "does not match"):
            lifecycle.observe_venue_cancel_id(initial, venue_cancel_id="cancel-1002")

    def test_scope_is_account_day_and_emt_client_specific_without_cross_epoch_fallback(
        self,
    ) -> None:
        journal = InMemoryClientOrderRefJournal("identity-journal-3")
        first_scope = _scope(trading_day="2026-08-31", emt_client_id=7)
        next_day_scope = _scope(trading_day="2026-09-01", emt_client_id=7)
        first_allocator = JournalBackedEmtClientOrderIdAllocator(first_scope, journal)
        first_allocator.recover_from_journal()
        first = first_allocator.allocate(
            ClientOrderRefAllocationRequest("execution-first", first_scope.canonical_scope)
        )
        first_lifecycle = JournalBackedVenueOrderIdentityLifecycle(
            first_scope.canonical_scope,
            lease_journal=journal,
            identity_journal=journal,
        )
        first_lifecycle.allocated(first)

        next_allocator = JournalBackedEmtClientOrderIdAllocator(next_day_scope, journal)
        next_allocator.recover_from_journal()
        next_lease = next_allocator.allocate(
            ClientOrderRefAllocationRequest("execution-next", next_day_scope.canonical_scope)
        )
        self.assertEqual("1", next_lease.client_order_ref)
        self.assertIsNone(
            journal.read_venue_identity(next_day_scope.canonical_scope, first.client_order_ref)
        )
        self.assertEqual(
            2,
            len(journal.lease_records),
            "same numeric order_client_id is valid only in its new TradingDayEpoch scope",
        )

    def test_identity_journal_layer_has_no_sdk_database_or_legacy_import(self) -> None:
        forbidden = {"emt_gateway", "vnemttrader", "psycopg", "sqlite3", "redis"}
        paths = (
            ROOT / "xuanling_tradecore" / "oms" / "identity_journal.py",
            ROOT / "xuanling_tradecore" / "adapters" / "emt" / "identity.py",
        )
        for source in paths:
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    names = (node.module,)
                else:
                    continue
                self.assertFalse(
                    {name.split(".", 1)[0] for name in names} & forbidden,
                    f"{source.relative_to(ROOT)} imports a forbidden runtime layer",
                )


if __name__ == "__main__":
    unittest.main()
