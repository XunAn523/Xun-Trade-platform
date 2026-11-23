from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from xuanling_tradecore.consumer.persistent_oms_runtime import PersistentOmsRuntime
from xuanling_tradecore.consumer.runtime_profile import ConsumerLongRunningPolicy
from xuanling_tradecore.oms import IntentAdmissionOutcome, OmsOrderBookError

from test_oms_committed_recovery import admit


def runtime(root: Path, initialize: bool = False) -> PersistentOmsRuntime:
    return PersistentOmsRuntime(
        root=root,
        scope_sha256="a" * 64,
        policy=ConsumerLongRunningPolicy(segment_frames=4),
        initialize=initialize,
    )


def test_full_restart_composes_identity_checkpoint_suffix_and_idempotency(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    original = runtime(root, True)
    journal_id = original.journal.journal_id
    first = admit(original.order_book)
    order_id = first.order.order.oms_order_id
    original.close()
    recovered = runtime(root)
    recovered.checkpoint_recovered_state()
    recovered.order_book.mark_working(oms_order_id=order_id)
    recovered.order_book.apply_trade(oms_order_id=order_id, trade_key="fill", quantity=50)
    expected = recovered.order_book.order(order_id)
    recovered.close()
    restarted = runtime(root)
    try:
        assert restarted.journal.journal_id == journal_id
        assert restarted.order_book.order(order_id) == expected
        assert restarted.recovered_watermark == 5
        assert admit(restarted.order_book).outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert (
            admit(restarted.order_book, 200).outcome is IntentAdmissionOutcome.IDEMPOTENCY_CONFLICT
        )
        with pytest.raises(ValueError, match="startup scan"):
            list(restarted.journal.startup_records())
    finally:
        restarted.close()


def test_subprocess_death_after_commit_before_response_recovers_same_id(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    code = """
from pathlib import Path
import os, sys
from test_persistent_oms_runtime import runtime
from test_oms_committed_recovery import admit
r = runtime(Path(sys.argv[1]), True)
admit(r.order_book)
os._exit(27)
"""
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [str(Path.cwd()), str(Path.cwd() / "tests"), os.environ.get("PYTHONPATH", "")]
        ),
    }
    process = subprocess.run([sys.executable, "-c", code, str(root)], env=env, check=False)
    assert process.returncode == 27
    recovered = runtime(root)
    try:
        assert recovered.recovered_watermark == 2
        retry = admit(recovered.order_book)
        assert retry.outcome is IntentAdmissionOutcome.IDEMPOTENT
        assert recovered.journal.stats().next_lsn == 3
    finally:
        recovered.close()


def test_missing_manifest_or_identity_is_not_silently_reset(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    with pytest.raises(OmsOrderBookError):
        runtime(root)
    runtime(root, True).close()
    with pytest.raises(OmsOrderBookError):
        runtime(root, True)
    (root / "journal" / "segments.json").unlink()
    with pytest.raises(OmsOrderBookError, match="lost"):
        runtime(root)


def test_bootstrap_holds_exclusive_runtime_lease_and_closes_on_error(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    first = runtime(root, True)
    with pytest.raises(OSError):
        runtime(root)
    first.close()
    recovered = runtime(root)
    recovered.close()
