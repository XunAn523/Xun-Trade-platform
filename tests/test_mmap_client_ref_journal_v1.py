from __future__ import annotations

import os
import subprocess
import sys

import pytest
from xuanling_tradecore.adapters.emt.identity import EmtClientOrderRefScope
from xuanling_tradecore.adapters.emt.namespace_recovery import (
    VerifiedNamespaceBoundEmtClientOrderIdAllocator,
)
from xuanling_tradecore.adapters.identity import (
    ClientOrderRefAllocationRequest,
    ClientOrderRefLease,
)
from xuanling_tradecore.oms.mmap_client_ref_journal_v1 import (
    MmapClientRefJournalErrorV1,
    MmapClientRefJournalV1,
)

from test_emt_namespace_recovery import _DAY, _directory

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux flock and mmap")


def _scope():
    return EmtClientOrderRefScope("sim-account", _DAY, "adapter-tradecore-a", 101)


def _open(root, *, bootstrap=False):
    return MmapClientRefJournalV1(
        root=root, scope=_scope().canonical_scope, capacity=8, bootstrap=bootstrap
    )


def test_signed_allocator_commits_recovers_and_never_reuses(tmp_path):
    os.chmod(tmp_path, 0o700)
    directory, _key = _directory()
    scope = _scope()
    journal = _open(tmp_path, bootstrap=True)
    allocator = VerifiedNamespaceBoundEmtClientOrderIdAllocator(
        emt_scope=scope, directory=directory, journal=journal
    )
    allocator.recover_from_journal()
    first = allocator.allocate(ClientOrderRefAllocationRequest("exec-1", scope.canonical_scope))
    assert first.client_order_ref == "1000"
    assert journal.read_committed_lease(scope.canonical_scope, "exec-1") == first
    with pytest.raises(MmapClientRefJournalErrorV1, match="already owned"):
        _open(tmp_path)
    journal.close()

    journal = _open(tmp_path)
    try:
        allocator = VerifiedNamespaceBoundEmtClientOrderIdAllocator(
            emt_scope=scope, directory=directory, journal=journal
        )
        allocator.recover_from_journal()
        assert (
            allocator.allocate(ClientOrderRefAllocationRequest("exec-1", scope.canonical_scope))
            == first
        )
        second = allocator.allocate(
            ClientOrderRefAllocationRequest("exec-2", scope.canonical_scope)
        )
        assert second.client_order_ref == "1001"
    finally:
        journal.close()


def test_bootstrap_missing_file_and_insecure_root_fail_closed(tmp_path):
    os.chmod(tmp_path, 0o700)
    with pytest.raises(MmapClientRefJournalErrorV1, match="bootstrap"):
        _open(tmp_path)
    journal = _open(tmp_path, bootstrap=True)
    journal.close()
    path = next(tmp_path.glob("*.mmap"))
    path.unlink()
    with pytest.raises(MmapClientRefJournalErrorV1, match="missing"):
        _open(tmp_path, bootstrap=True)
    other = tmp_path / "insecure"
    other.mkdir(mode=0o755)
    os.chmod(other, 0o755)
    with pytest.raises(MmapClientRefJournalErrorV1, match="owner-private"):
        _open(other, bootstrap=True)


def test_corrupt_commit_closes_recovery_and_never_reuses(tmp_path):
    os.chmod(tmp_path, 0o700)
    journal = _open(tmp_path, bootstrap=True)
    scope = _scope().canonical_scope
    journal.append_lease(ClientOrderRefLease("exec-1", scope, "1000", 1000))
    path = next(tmp_path.glob("*.mmap"))
    journal.close()
    with path.open("r+b", buffering=0) as output:
        output.seek(4096 + 4096 - 8)
        output.write(b"\0" * 8)
    with pytest.raises(MmapClientRefJournalErrorV1, match="untrusted tail"):
        _open(tmp_path)


def test_process_exit_keeps_committed_mapping_for_new_owner(tmp_path):
    os.chmod(tmp_path, 0o700)
    script = """
import os, sys
from pathlib import Path
from xuanling_tradecore.adapters.emt.identity import EmtClientOrderRefScope
from xuanling_tradecore.adapters.identity import ClientOrderRefLease
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.oms.mmap_client_ref_journal_v1 import MmapClientRefJournalV1
scope=EmtClientOrderRefScope("sim-account", TradingDayEpoch("2026-09-11"), "adapter-tradecore-a", 101).canonical_scope
journal=MmapClientRefJournalV1(root=Path(sys.argv[1]),scope=scope,capacity=8,bootstrap=True)
journal.append_lease(ClientOrderRefLease("child-exec",scope,"1000",1000))
os._exit(0)
"""
    # Use the same trading day as the signed fixture without guessing it.
    script = script.replace('TradingDayEpoch("2026-09-11")', f'TradingDayEpoch("{_DAY.value}")')
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)], timeout=15, check=False)
    assert result.returncode == 0
    journal = _open(tmp_path)
    try:
        recovery = journal.read_recovery(_scope().canonical_scope)
        assert recovery.is_complete and recovery.committed_allocation_sequence == 1000
        assert recovery.leases[0].execution_id == "child-exec"
    finally:
        journal.close()
