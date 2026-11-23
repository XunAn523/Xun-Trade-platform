from __future__ import annotations

import sys
from dataclasses import replace

import pytest
from xuanling_tradecore.consumer.emt_committed_cancel_v1 import (
    EmtCommittedCancelErrorV1,
    verify_emt_committed_cancel_v1,
)
from xuanling_tradecore.domain import OmsOrderState

from test_consumer_emt_submit_route_preparer_v1 import _composition

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux namespace Journal")


def test_cancel_proof_rejects_changed_identity_reason_and_wrong_batch(tmp_path):
    journal, refs, _preparer, _resolver, (order, _commit), _errors, operations, _fixture, _context = (
        _composition(tmp_path, include_operations=True)
    )
    try:
        event = operations._order_book.request_cancel(
            oms_order_id=order.order.oms_order_id,
            cancel_request_id="cancel-1",
            reason="operator request",
        )
        assert event is not None
        arguments = dict(
            journal=journal, event=event, order=order,
            cancel_request_id="cancel-1", reason="operator request",
        )
        verified = verify_emt_committed_cancel_v1(**arguments)
        assert verified.commit.final_lsn == event.journal_lsn
        assert verified.cancel_request_id == "cancel-1"
        for changes in (
            {"cancel_request_id": "different"},
            {"reason": "different"},
            {"event": replace(event, execution_id="other")},
            {"event": replace(event, journal_lsn=event.journal_lsn + 1)},
            {"event": replace(event, order_state=OmsOrderState.CANCELED)},
        ):
            with pytest.raises(EmtCommittedCancelErrorV1):
                verify_emt_committed_cancel_v1(**{**arguments, **changes})
    finally:
        refs.close()
        journal.close()
