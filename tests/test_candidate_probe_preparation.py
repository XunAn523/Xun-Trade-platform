from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from consumer_uds_candidate_probe import wait_seed_projection


def completed(row):
    return {"status": "COMPLETED", "payload": {"audit_trail": [row]}}


def test_public_non_pinning_preparation_never_treats_hot_replay_as_sql_readiness():
    row = {"oms_order_id": "seed", "journal_id": "source", "journal_lsn": 12}
    replies = iter(
        [
            {"status": "COMPLETED", "payload": {"high_watermark_position": "999-0:0"}},
            completed(dict(row, oms_order_id="foreign")),
            completed(dict(row, journal_id="foreign")),
            completed(dict(row, journal_lsn=True)),
            completed(dict(row, journal_lsn=11)),
            completed(row),
        ]
    )
    calls = []

    class Client:
        def call(self, request):
            calls.append(request)
            return next(replies)

    with patch("consumer_uds_candidate_probe.asyncio.sleep"):
        asyncio.run(
            wait_seed_projection(Client(), base=1, order_id="seed", journal_id="source", lsn=12)
        )
    assert len(calls) == 6
    assert all(call["operation"] == "GetOrderAuditTrail" for call in calls)
    assert all(call["payload"] == {"oms_order_id": "seed"} for call in calls)


def test_failed_preparation_is_bounded_and_does_not_pin_or_start_a_measurement():
    calls = []

    class Client:
        def call(self, request):
            calls.append(request)
            return {"status": "ERROR"}

    with patch("consumer_uds_candidate_probe.asyncio.sleep"):
        with pytest.raises(ValueError, match="audit projection fence"):
            asyncio.run(
                wait_seed_projection(Client(), base=1, order_id="seed", journal_id="source", lsn=12)
            )
    assert len(calls) == 100
