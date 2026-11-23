import pytest
from consumer_uds_generator_fixture import JOURNAL, frozen_page, response


def test_controlled_fixture_uses_real_wire_order_payload_shape_but_no_durable_claim():
    page = frozen_page("2026-10-21")
    assert len(page["orders"]) == page["count"] == 100
    assert all(set(row) == {"event_id", "event_kind", "payload"} for row in page["orders"])
    assert all("oms_order_id" in row["payload"] for row in page["orders"])
    assert "NOT-A-DURABLE-JOURNAL" in page["journal_id"]
    receipt = response(
        {"operation": "SubmitSignal", "request_id": "r", "payload": {}}, serial=100, page=page
    )
    assert receipt["payload"]["journal_id"] == JOURNAL
    audit = response(
        {
            "operation": "GetOrderAuditTrail",
            "request_id": "a",
            "payload": {"oms_order_id": receipt["payload"]["oms_order_id"]},
        },
        serial=101,
        page=page,
    )
    assert audit["payload"]["audit_trail"][0]["journal_lsn"] == 100


@pytest.mark.parametrize("op", ["PollEvents", "CancelOrder", "InsertOrder", "Reset"])
def test_fixture_cannot_simulate_real_cursor_recovery_or_trading(op):
    with pytest.raises(ValueError):
        response({"operation": op, "request_id": "r", "payload": {}}, serial=1, page={})
