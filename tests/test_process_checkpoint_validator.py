import os
import signal
import time

import pytest
from xuanling_tradecore.oms.cold_intent import ColdIntentIndexError
from xuanling_tradecore.persistence.process_checkpoint_validator import ProcessCheckpointValidator

from test_cold_intent_index import checkpoint, index


def test_exact_child_verification_rejects_active_then_publishes_real_full_commit(tmp_path):
    validator = ProcessCheckpointValidator("stream")
    store = index(tmp_path / "cold", checkpoint_validator=validator)
    key, terminal = checkpoint()
    _, active = checkpoint(False)
    try:
        with pytest.raises(ColdIntentIndexError):
            store.archive((active,)).result(timeout=8)
        assert store.stats().generation == 0
        assert store.stats().records == 0
        receipt = store.archive((terminal,)).result(timeout=8)
        assert store.validate(receipt) and receipt.keys == (key,)
        assert store.resolve(key).result(timeout=8).checkpoint == terminal
        for invalid in ((), (b"",), (b"x" * (256 * 1024 + 1),), (terminal,) * 129):
            with pytest.raises(ColdIntentIndexError):
                validator(invalid)
    finally:
        store.close()
        validator.close()
    assert validator._process.exitcode == 0


def test_dead_validator_never_returns_positive_or_negative_proof():
    validator = ProcessCheckpointValidator("stream")
    _, terminal = checkpoint()
    try:
        validator._process.terminate()
        validator._process.join(3)
        with pytest.raises(ColdIntentIndexError):
            validator((terminal,))
        assert validator._failed
        with pytest.raises(ColdIntentIndexError):
            validator((terminal,))
    finally:
        validator.close()


@pytest.mark.parametrize("scoped", (False, True))
def test_process_owns_full_sql_commit_parent_only_publishes_after_receipt(tmp_path, scoped):
    import sqlite3
    from unittest.mock import patch

    from xuanling_tradecore.persistence.cold_intent_index import ColdIntentIndex

    root = tmp_path / "sql-child"
    validator = ProcessCheckpointValidator("stream", database_path=root / "intents.sqlite")
    parent_sql = []
    connect = ColdIntentIndex._connect

    def observed(owner):
        db = connect(owner)
        db.set_trace_callback(parent_sql.append)
        return db

    with patch.object(ColdIntentIndex, "_connect", new=observed):
        store = index(
            root,
            checkpoint_validator=validator,
            archive_committer=validator.commit,
            archive_close=validator.close,
            scoped_lookup_epochs=scoped,
        )
    key, terminal = checkpoint()
    try:
        receipt = store.archive((terminal,)).result(timeout=8)
        assert receipt.generation == 1 and store.validate(receipt)
        assert store.resolve(key).result(timeout=8).checkpoint == terminal
        assert not any(sql.startswith(("INSERT", "UPDATE", "BEGIN")) for sql in parent_sql)
        with sqlite3.connect(str(root / "intents.sqlite")) as db:
            assert db.execute("SELECT generation,records FROM meta").fetchone() == (1, 1)
        store.archive((terminal,)).result(timeout=8)
        assert store.stats().generation == 2 and store.stats().records == 1
    finally:
        store.close()
    assert validator._closed and validator._process.exitcode == 0


@pytest.mark.parametrize("scoped", (False, True))
def test_lost_full_commit_reply_poisoned_owner_never_grants_absence(tmp_path, scoped):
    from unittest.mock import patch

    from xuanling_tradecore.persistence import process_checkpoint_validator as module

    root = tmp_path / "lost-reply"
    validator = ProcessCheckpointValidator("stream", database_path=root / "intents.sqlite")
    store = index(
        root,
        archive_committer=validator.commit,
        archive_close=validator.close,
        scoped_lookup_epochs=scoped,
    )
    key, terminal = checkpoint()
    receive = module._receive

    def lost(*args, **kwargs):
        assert receive(*args, **kwargs) is not None  # child already FULL committed
        return None

    try:
        with patch.object(module, "_receive", side_effect=lost):
            with pytest.raises(ColdIntentIndexError):
                store.archive((terminal,)).result(timeout=8)
        assert store.stats().failed
        with pytest.raises(ColdIntentIndexError):
            store.probe(key)
    finally:
        store.close()
    assert validator._closed


@pytest.mark.skipif(not hasattr(signal, "SIGSTOP"), reason="POSIX stopped-child send deadline")
def test_deadline_also_bounds_blocked_ipc_send_not_just_reply():
    validator = ProcessCheckpointValidator("stream")
    try:
        # Only this test's owned child, never an external process.
        os.kill(validator._process.pid, signal.SIGSTOP)
        start = time.monotonic()
        with pytest.raises(ColdIntentIndexError):
            validator((b"x" * (256 * 1024),) * 128)
        assert time.monotonic() - start < 7
        assert validator._failed
    finally:
        os.kill(validator._process.pid, signal.SIGCONT)
        validator.close()
