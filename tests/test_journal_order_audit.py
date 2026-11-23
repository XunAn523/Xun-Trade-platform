from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.consumer.oms_operations_v2 import OrderBookConsumerOperationsV2
from xuanling_tradecore.oms.journal_audit import verify_order_audit
from xuanling_tradecore.persistence.order_projection_store import (
    OrderProjectionError,
    OrderProjectionStore,
)

from test_order_projection_store import SCOPE, admit, create, drain, retry
from test_tradecore_oms import _bindings


@pytest.fixture
def audit_env(tmp_path):
    journal, book, store, tail = create(tmp_path)
    order_id = admit(book, "a").order.order.oms_order_id
    admit(book, "other")
    retry(lambda: book.mark_working(oms_order_id=order_id))
    retry(lambda: book.apply_trade(oms_order_id=order_id, trade_key="trade", quantity=40))
    retry(lambda: book.request_cancel(oms_order_id=order_id))
    retry(lambda: book.cancel(oms_order_id=order_id))
    admit(book, "a", quantity=200)  # conflict: not a lifecycle audit event
    drain(store, tail)
    plan = store.order_audit_plan(
        SCOPE, order_id, minimum_watermark=journal.read_bound().committed_lsn
    )
    try:
        yield journal, book, store, tail, order_id, plan
    finally:
        tail.close()
        store.close()
        journal.close()


def test_raw_journal_replay_matches_hot_audit_contract_and_ignores_unrelated_order(audit_env):
    journal, book, _, _, order_id, plan = audit_env
    before = journal.read_bound()
    ops = OrderBookConsumerOperationsV2(order_book=book, admission_context_for=lambda *_: None)
    expected = ops.get_order_audit_trail(account_binding=_bindings()[0], oms_order_id=order_id)
    actual = verify_order_audit(journal, plan, scope=SCOPE, oms_order_id=order_id)
    assert actual == expected and len(actual) == 5
    assert [e.journal_lsn for e in actual] == sorted({e.journal_lsn for e in actual})
    assert journal.read_bound() == before


@pytest.mark.parametrize(
    "tamper", ["omit", "duplicate", "digest", "position", "scope", "order", "checkpoint"]
)
def test_unverifiable_index_hints_never_become_audit_authority(audit_env, tamper):
    journal, _, _, _, order_id, plan = audit_env
    scope = SCOPE
    if tamper == "omit":
        plan = replace(plan, anchors=plan.anchors[:1] + plan.anchors[2:])
    elif tamper == "duplicate":
        plan = replace(plan, anchors=plan.anchors + (plan.anchors[-1],))
    elif tamper == "digest":
        plan = replace(plan, anchors=(replace(plan.anchors[0], sha256="0" * 64), *plan.anchors[1:]))
    elif tamper == "position":
        anchor = plan.anchors[0]
        plan = replace(
            plan,
            anchors=(
                replace(anchor, before=msgspec.structs.replace(anchor.before, frame=1)),
                *plan.anchors[1:],
            ),
        )
    elif tamper == "scope":
        scope = ("foreign", *SCOPE[1:])
    elif tamper == "order":
        order_id = "foreign"
    else:
        plan = replace(plan, checkpoint=plan.checkpoint[:-1])
    with pytest.raises(Exception):
        verify_order_audit(journal, plan, scope=scope, oms_order_id=order_id)


def test_audit_capacity_rejects_without_partial_history(audit_env):
    journal, _, store, _, order_id, plan = audit_env
    with pytest.raises(OrderProjectionError):
        store.order_audit_plan(SCOPE, order_id, minimum_watermark=0, maximum_batches=1)
    with pytest.raises(OrderProjectionError):
        store.order_audit_plan(SCOPE, order_id, minimum_watermark=0, maximum_facts=1)
    with pytest.raises(Exception):
        verify_order_audit(journal, plan, scope=SCOPE, oms_order_id=order_id, maximum_facts=1)
    assert store.order_audit_plan(("foreign", *SCOPE[1:]), order_id, minimum_watermark=0) is None
    with patch.object(journal, "read_bound", side_effect=OSError("closed source")):
        with pytest.raises(OSError):
            verify_order_audit(journal, plan, scope=SCOPE, oms_order_id=order_id)


@pytest.mark.parametrize("fault", ["crc", "missing"])
def test_original_journal_is_required_even_when_the_read_index_is_healthy(audit_env, fault):
    journal, _, _, _, order_id, plan = audit_env
    first = plan.anchors[0].before
    path = journal.path / f"segment-{first.segment:012d}.mmap"
    if fault == "missing":
        moved = path.with_suffix(".temporarily-missing")
        path.rename(moved)
        try:
            with pytest.raises(Exception):
                verify_order_audit(journal, plan, scope=SCOPE, oms_order_id=order_id)
        finally:
            moved.rename(path)
    else:
        with path.open("r+b", buffering=0) as output:
            offset = 4096 + first.frame * 4096 + 100
            output.seek(offset)
            original = output.read(1)
            output.seek(offset)
            output.write(bytes([original[0] ^ 1]))
            try:
                with pytest.raises(Exception):
                    verify_order_audit(journal, plan, scope=SCOPE, oms_order_id=order_id)
            finally:
                output.seek(offset)
                output.write(original)


def test_old_projection_schema_is_rejected_without_automatic_migration_or_wipe(audit_env):
    journal, _, store, _, order_id, _ = audit_env
    store._db.execute("PRAGMA user_version=1")
    store._db.commit()
    try:
        with pytest.raises(OrderProjectionError, match="schema"):
            OrderProjectionStore(
                store.path.parent,
                journal_id=journal.journal_id,
                profile_hash=journal.profile_hash,
                read_only=True,
            )
        assert store.path.exists()
    finally:
        store._db.execute("PRAGMA user_version=3")
        store._db.commit()
    assert store.lookup(SCOPE, order_id) is not None
