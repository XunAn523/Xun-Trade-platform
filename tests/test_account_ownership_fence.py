from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.ems import (
    AccountFenceScope,
    AccountOwnershipFence,
    AccountOwnershipFenceContendedError,
    AccountOwnershipFenceIntegrityError,
    AccountOwnershipFenceUnsupportedError,
    canonical_account_scope_hash,
)


def _make_layout(base: Path, scope: AccountFenceScope) -> tuple[Path, Path]:
    control_root = base / "control"
    account_root = control_root / "accounts" / scope.scope_hash
    account_root.mkdir(parents=True)
    if sys.platform == "linux":
        control_root.chmod(0o700)
        (control_root / "accounts").chmod(0o700)
        account_root.chmod(0o700)
    journal_root = base / "journals" / "generation-1"
    journal_root.mkdir(parents=True)
    return control_root, journal_root


def _new_fence(
    control_root: Path,
    journal_root: Path,
    scope: AccountFenceScope,
    *,
    instance_id: str = "tradecore-test-1",
    generation: int = 1,
) -> AccountOwnershipFence:
    return AccountOwnershipFence(
        runtime_control_root=control_root,
        scope=scope,
        instance_id=instance_id,
        host_id="host-test-1",
        process_started_monotonic_ns=100,
        journal_id=f"journal-{generation}",
        journal_root=journal_root,
        stream_generation=generation,
    )


class AccountFenceScopeTests(unittest.TestCase):
    def test_hash_is_stable_scoped_and_delimiter_unambiguous(self) -> None:
        first = AccountFenceScope("account/a", "EMT", "scope:b")
        same = AccountFenceScope("account/a", "EMT", "scope:b")
        different_scope = AccountFenceScope("account/a", "EMT", "scope/b")
        different_venue = AccountFenceScope("account/a", "QMT", "scope:b")
        self.assertEqual(canonical_account_scope_hash(first), same.scope_hash)
        self.assertEqual(64, len(first.scope_hash))
        self.assertNotEqual(first.scope_hash, different_scope.scope_hash)
        self.assertNotEqual(first.scope_hash, different_venue.scope_hash)
        self.assertNotIn("account", first.scope_hash)

    def test_noncanonical_scope_values_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            AccountFenceScope(" account-1", "EMT", "7")
        with self.assertRaises(ValueError):
            AccountFenceScope("account-1", "emt", "7")
        with self.assertRaises(ValueError):
            AccountFenceScope("account-1", "EMT", "scope\n7")

    def test_non_linux_acquisition_is_explicitly_unsupported(self) -> None:
        if sys.platform == "linux":
            self.skipTest("Linux exercises the real flock contract below")
        scope = AccountFenceScope("account-1", "EMT", "7")
        with tempfile.TemporaryDirectory() as directory:
            control_root, journal_root = _make_layout(Path(directory), scope)
            fence = _new_fence(control_root, journal_root, scope)
            with self.assertRaises(AccountOwnershipFenceUnsupportedError):
                fence.acquire(acquired_monotonic_ns=101)


@unittest.skipUnless(sys.platform == "linux", "production ownership fence requires Linux")
class LinuxAccountOwnershipFenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.scope = AccountFenceScope("account-1", "EMT", "7")
        self.control_root, self.journal_root = _make_layout(self.base, self.scope)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_holder_gets_process_bound_proof_but_never_ready_authorization(self) -> None:
        fence = _new_fence(self.control_root, self.journal_root, self.scope)
        proof = fence.acquire(acquired_monotonic_ns=110)
        try:
            self.assertTrue(fence.held)
            self.assertEqual(os.getpid(), proof.owner_pid)
            self.assertTrue(proof.requires_replay_reconciliation_and_manual_ready)
            self.assertFalse(proof.authorizes_ready)
            self.assertEqual(proof, fence.require_held())
            diagnostic = json.loads(fence.lock_path.read_text(encoding="ascii"))
            self.assertEqual(self.scope.scope_hash, diagnostic["scope_hash"])
            self.assertEqual("journal-1", diagnostic["journal_id"])
            self.assertEqual(1, diagnostic["stream_generation"])
            self.assertEqual(
                "DISARMED_REPLAY_RECONCILIATION_MANUAL_READY_REQUIRED",
                diagnostic["startup_disposition"],
            )
        finally:
            fence.release()
        self.assertFalse(fence.held)
        with self.assertRaises(AccountOwnershipFenceIntegrityError):
            fence.require_held()

    def test_different_journal_generations_contend_on_the_same_stable_lock(self) -> None:
        holder = _new_fence(self.control_root, self.journal_root, self.scope)
        holder.acquire(acquired_monotonic_ns=110)
        original_diagnostic = holder.lock_path.read_bytes()
        second_journal = self.base / "journals" / "generation-2"
        second_journal.mkdir()
        contender = _new_fence(
            self.control_root,
            second_journal,
            self.scope,
            instance_id="tradecore-test-2",
            generation=2,
        )
        try:
            with self.assertRaises(AccountOwnershipFenceContendedError) as raised:
                contender.acquire(acquired_monotonic_ns=120)
            self.assertIsNotNone(raised.exception.incumbent)
            assert raised.exception.incumbent is not None
            self.assertEqual("tradecore-test-1", raised.exception.incumbent.instance_id)
            self.assertEqual(1, raised.exception.incumbent.stream_generation)
            self.assertEqual(original_diagnostic, holder.lock_path.read_bytes())
        finally:
            holder.release()

        successor = _new_fence(
            self.control_root,
            second_journal,
            self.scope,
            instance_id="tradecore-test-2",
            generation=2,
        )
        proof = successor.acquire(acquired_monotonic_ns=130)
        try:
            self.assertFalse(proof.authorizes_ready)
            self.assertEqual(2, json.loads(successor.lock_path.read_text())["stream_generation"])
        finally:
            successor.release()

    def test_second_process_is_rejected_without_overwriting_holder_diagnostic(self) -> None:
        holder = _new_fence(self.control_root, self.journal_root, self.scope)
        holder.acquire(acquired_monotonic_ns=110)
        original_diagnostic = holder.lock_path.read_bytes()
        script = """
import sys
from pathlib import Path
from xuanling_tradecore.ems import AccountFenceScope, AccountOwnershipFence, AccountOwnershipFenceContendedError
scope = AccountFenceScope('account-1', 'EMT', '7')
fence = AccountOwnershipFence(runtime_control_root=Path(sys.argv[1]), scope=scope, instance_id='child', host_id='host-test-1', process_started_monotonic_ns=100, journal_id='journal-2', journal_root=Path(sys.argv[2]), stream_generation=2)
try:
    fence.acquire(acquired_monotonic_ns=120)
except AccountOwnershipFenceContendedError as exc:
    print(exc.incumbent.instance_id if exc.incumbent else 'UNKNOWN')
    raise SystemExit(23)
raise SystemExit(0)
"""
        try:
            completed = subprocess.run(
                [sys.executable, "-c", script, str(self.control_root), str(self.journal_root)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(23, completed.returncode, completed.stderr)
            self.assertEqual("tradecore-test-1", completed.stdout.strip())
            self.assertEqual(original_diagnostic, holder.lock_path.read_bytes())
        finally:
            holder.release()

    def test_abnormal_holder_exit_releases_lock_but_successor_remains_disarmed(self) -> None:
        script = """
import sys
import time
from pathlib import Path
from xuanling_tradecore.ems import AccountFenceScope, AccountOwnershipFence
scope = AccountFenceScope('account-1', 'EMT', '7')
fence = AccountOwnershipFence(runtime_control_root=Path(sys.argv[1]), scope=scope, instance_id='holder-child', host_id='host-test-1', process_started_monotonic_ns=100, journal_id='journal-1', journal_root=Path(sys.argv[2]), stream_generation=1)
fence.acquire(acquired_monotonic_ns=110)
print(scope.scope_hash, flush=True)
time.sleep(30)
"""
        holder = subprocess.Popen(
            [sys.executable, "-c", script, str(self.control_root), str(self.journal_root)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert holder.stdout is not None
            self.assertEqual(self.scope.scope_hash, holder.stdout.readline().strip())
            contender = _new_fence(
                self.control_root,
                self.journal_root,
                self.scope,
                instance_id="contender",
            )
            with self.assertRaises(AccountOwnershipFenceContendedError):
                contender.acquire(acquired_monotonic_ns=120)
        finally:
            holder.kill()
            holder.communicate(timeout=5)

        successor = _new_fence(
            self.control_root,
            self.journal_root,
            self.scope,
            instance_id="successor",
        )
        proof = successor.acquire(acquired_monotonic_ns=130)
        try:
            self.assertFalse(proof.authorizes_ready)
            self.assertTrue(proof.requires_replay_reconciliation_and_manual_ready)
        finally:
            successor.release()

    def test_layout_permission_symlink_and_journal_overlap_fail_closed(self) -> None:
        account_root = self.control_root / "accounts" / self.scope.scope_hash
        account_root.chmod(0o755)
        with self.assertRaises(AccountOwnershipFenceIntegrityError):
            _new_fence(self.control_root, self.journal_root, self.scope).acquire(
                acquired_monotonic_ns=110
            )
        account_root.chmod(0o700)

        overlapping_journal = account_root / "journal-generation"
        overlapping_journal.mkdir()
        with self.assertRaises(AccountOwnershipFenceIntegrityError):
            _new_fence(self.control_root, overlapping_journal, self.scope).acquire(
                acquired_monotonic_ns=110
            )

        overlapping_journal.rmdir()
        account_root.rmdir()
        target = self.base / "redirected-account-root"
        target.mkdir(mode=0o700)
        account_root.symlink_to(target, target_is_directory=True)
        with self.assertRaises(AccountOwnershipFenceIntegrityError):
            _new_fence(self.control_root, self.journal_root, self.scope).acquire(
                acquired_monotonic_ns=110
            )
