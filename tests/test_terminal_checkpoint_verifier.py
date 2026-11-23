from unittest.mock import patch

import msgspec
import pytest
from xuanling_tradecore.oms import OrderBook
from xuanling_tradecore.oms.cold_intent import ColdIntentKey
from xuanling_tradecore.oms.order_book import OmsOrderBookError
from xuanling_tradecore.oms.recovery import OmsRecoverySeedV1
from xuanling_tradecore.oms.recovery_checkpoint import decode_checkpoint
from xuanling_tradecore.persistence.cold_intent_index import validate_terminal_checkpoint

from test_cold_intent_index import checkpoint
from test_oms_committed_recovery import RecordingJournal


def reference(encoded, journal="stream"):
    book = OrderBook(journal=RecordingJournal(), persist_recovery_seed=True)
    book.restore_recovery_checkpoint(encoded, expected_journal_id=journal)
    seed = OmsRecoverySeedV1.decode_and_verify(decode_checkpoint(encoded).orders[0].admission_seed)
    trace = seed.order.order.consumer_trace
    return ColdIntentKey(seed.order.order.account_id, trace.strategy_id, trace.strategy_order_id)


def test_one_original_canonical_decode_per_call_without_cross_call_cache():
    key, encoded = checkpoint()
    original = OmsRecoverySeedV1.decode_and_verify
    with patch.object(OmsRecoverySeedV1, "decode_and_verify", wraps=original) as decode:
        for n in range(1, 4):
            assert validate_terminal_checkpoint(encoded, "stream") == key
            assert decode.call_count == n
    assert reference(encoded) == key


@pytest.mark.parametrize(
    "case",
    (
        "state",
        "revision",
        "reservation_revision",
        "fill",
        "trade",
        "seed",
        "event_identity",
        "event_revision",
        "event_source",
        "missing_event",
        "journal",
    ),
)
def test_identity_capture_never_bypasses_original_restore_validation(case):
    _, encoded = checkpoint()
    state = decode_checkpoint(encoded)
    row, last = state.orders[0], state.events[-1]
    if case in {"state", "revision", "reservation_revision", "fill", "trade", "seed"}:
        fields = {
            "state": {"state": "SUBMIT_UNKNOWN"},
            "revision": {"order_revision": 88},
            "reservation_revision": {"reservation_revision": -1},
            "fill": {"filled_quantity": 1},
            "trade": {"trade_keys": ("unproved",)},
            "seed": {"admission_seed": b"{}"},
        }[case]
        state = msgspec.structs.replace(state, orders=(msgspec.structs.replace(row, **fields),))
    elif case == "missing_event":
        state = msgspec.structs.replace(state, events=state.events[:-1])
    elif case == "journal":
        state = msgspec.structs.replace(state, journal_id="foreign")
    else:
        fields = {
            "event_identity": {"oms_order_id": "foreign"},
            "event_revision": {"order_revision": 88},
            "event_source": {"source_key": "foreign"},
        }[case]
        state = msgspec.structs.replace(
            state, events=(*state.events[:-1], msgspec.structs.replace(last, **fields))
        )
    invalid = msgspec.json.encode(state)
    with pytest.raises((OmsOrderBookError, ValueError)):
        reference(invalid)
    with pytest.raises((OmsOrderBookError, ValueError)):
        validate_terminal_checkpoint(invalid, "stream")


def test_active_checkpoint_still_cannot_retire():
    _, encoded = checkpoint(False)
    with pytest.raises(OmsOrderBookError, match="cannot retire"):
        validate_terminal_checkpoint(encoded, "stream")
