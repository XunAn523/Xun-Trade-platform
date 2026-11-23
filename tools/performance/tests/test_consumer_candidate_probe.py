import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from tools.performance.consumer_uds_candidate_probe import (
    exchange,
    planned_tick_ns,
    request,
    run_window,
    tick_count,
    valid_trading_day,
    wait_until_ns,
)
from xuanling_tradecore.consumer.wire_v2 import (
    HEADER_SIZE_BYTES,
    MessageKind,
    decode_frame,
    encode_frame,
)
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1


@pytest.mark.parametrize("rate", [1, 10, 20, 1980, 2000])
def test_integer_release_budget_is_exact_without_rate_rounding(rate):
    assert sum(tick_count(n, rate) for n in range(100)) == rate
    assert sum(tick_count(n, rate) for n in range(3000)) == 30 * rate


def test_completion_aware_release_clock_keeps_exact_window_and_tail_budget():
    start = 10_000_000_000
    ticks = 6_000
    values = [planned_tick_ns(start, tick, ticks) for tick in range(ticks)]
    assert values[0] == start
    assert values[-1] == start + 60_000_000_000 - 60_000_000
    assert all(left < right for left, right in zip(values, values[1:]))
    assert max(right - left for left, right in zip(values, values[1:])) <= 10_000_000


@pytest.mark.parametrize(
    "values",
    [
        (0, 0, 100),
        (1, -1, 100),
        (1, 100, 100),
        (1, 0, 1),
    ],
)
def test_completion_aware_release_clock_rejects_invalid_boundaries(values):
    with pytest.raises(ValueError, match="completion-aware"):
        planned_tick_ns(*values)


@pytest.mark.parametrize(
    "value,expected",
    [("2026-10-26", True), ("20261026", False), ("2026-02-30", False), ("", False)],
)
def test_candidate_probe_rejects_non_iso_or_impossible_trading_days(value, expected):
    assert valid_trading_day(value) is expected


def test_early_timer_wakeup_does_not_release_before_observed_gate():
    with (
        patch(
            "tools.performance.consumer_uds_candidate_probe.time.monotonic_ns",
            side_effect=[90, 95, 100],
        ),
        patch("asyncio.sleep", new_callable=AsyncMock) as sleep,
    ):
        asyncio.run(wait_until_ns(100))
    assert sleep.await_count == 2


def test_prepared_public_requests_have_no_internal_book_or_reset_control():
    value = request("ReadOrderSnapshotPage", "test", {"page_cursor": "opaque"})
    assert set(value) == {"message_type", "schema_version", "request_id", "operation", "payload"}
    assert "reset" not in value and value["schema_version"] == 2


def test_candidate_probe_retains_a_finite_concurrency_ceiling(tmp_path):
    with pytest.raises(ValueError, match="bounded explicit no-Adapter scope"):
        asyncio.run(
            run_window(
                config=object(),
                output=tmp_path,
                duration=1,
                rate=1,
                workers=4_097,
                base=1,
                day="2026-10-24",
            )
        )


@pytest.mark.parametrize("correlation", [7, 8])
def test_transport_matches_correlation_and_preserves_measured_boundaries(correlation):
    frame = encode_frame(MessageKind.REQUEST, 7, request("ListAccounts", "r", {}))
    response = encode_frame(MessageKind.RESPONSE, correlation, {"status": "COMPLETED"})
    reader = SimpleNamespace(
        readexactly=AsyncMock(
            side_effect=[response[:HEADER_SIZE_BYTES], response[HEADER_SIZE_BYTES:]]
        )
    )
    writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
    with patch(
        "asyncio.open_unix_connection", new=AsyncMock(return_value=(reader, writer)), create=True
    ):
        result, times = asyncio.run(exchange("/fake.sock", frame))
    assert times[0] > 0 and list(times) == sorted(times)
    assert (result is not None) == (correlation == 7)
    writer.close.assert_called_once()


def test_post_send_timeout_is_not_reclassified_as_pre_send_failure():
    frame = encode_frame(MessageKind.REQUEST, 7, request("ListAccounts", "r", {}))
    reader = SimpleNamespace(readexactly=AsyncMock(side_effect=asyncio.TimeoutError))
    writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
    with patch(
        "asyncio.open_unix_connection", new=AsyncMock(return_value=(reader, writer)), create=True
    ):
        result, (connected, sent, decoded) = asyncio.run(exchange("/fake.sock", frame))
    assert result is None and connected > 0 and sent >= connected and decoded == 0


def test_hot_page_is_actually_read_before_window_and_not_counted_as_measured_qps(tmp_path):
    config = SimpleNamespace(
        socket_path=tmp_path / "fake",
        consumer_account_id="c",
        strategy_id="s",
        venue_id="NO_ADAPTER_BENCHMARK",
        adapter_instance_id="disabled-no-dispatch",
    )
    preparation = []
    projected = iter((90, 99, 100))
    position = ConsumerEventPositionV2(RedisStreamIdV1(100, 0), 0).to_external_id()
    page = dict(
        snapshot_id="snapshot",
        consumer_account_id="c",
        journal_id="source",
        stream_generation=1,
        snapshot_position=position,
        count=100,
        orders=[],
        has_more=False,
        next_page_cursor=None,
    )

    def control(value):
        preparation.append(value["operation"])
        if value["operation"] == "SubmitSignal":
            return {
                "status": "ACCEPTED",
                "payload": {"journal_lsn": 100, "oms_order_id": "seed", "journal_id": "source"},
            }
        if value["operation"] == "GetOrderAuditTrail":
            return {
                "status": "COMPLETED",
                "payload": {
                    "audit_trail": [
                        {
                            "oms_order_id": "seed",
                            "journal_id": "source",
                            "journal_lsn": next(projected),
                        }
                    ]
                },
            }
        if value["operation"] == "CreateOrderSnapshot":
            return {
                "status": "COMPLETED",
                "payload": {
                    "order_count": 100,
                    "page_cursor": "signed",
                    "snapshot_position": position,
                },
            }
        return {"status": "COMPLETED", "payload": page}

    async def rpc(_path, frame):
        value = decode_frame(frame).payload
        body = (
            {"journal_lsn": 1, "oms_order_id": "order"}
            if value["operation"] == "SubmitSignal"
            else page
        )
        now = time.monotonic_ns()
        return {
            "request_id": value["request_id"],
            "status": "ACCEPTED" if value["operation"] == "SubmitSignal" else "COMPLETED",
            "payload": body,
        }, (now, now, now)

    client = SimpleNamespace(call=control)
    with (
        patch(
            "tools.performance.consumer_uds_candidate_probe.ConsumerUnixClientV2",
            return_value=client,
        ),
        patch("tools.performance.consumer_uds_candidate_probe.exchange", side_effect=rpc),
    ):
        report = asyncio.run(
            run_window(
                config=config,
                output=tmp_path,
                duration=1,
                rate=1,
                workers=2,
                base=1,
                day="2026-10-20",
            )
        )
    assert preparation == ["SubmitSignal"] * 100 + ["GetOrderAuditTrail"] * 3 + [
        "CreateOrderSnapshot",
        "ReadOrderSnapshotPage",
    ]
    assert report["counts"]["ReadOrderSnapshotPage.completed"] == 1
    assert report["snapshot_preparation"]["public_page_warmups"] == 1
    assert report["generator_gc"]["disabled_during_window"]
    assert report["generator_gc"]["enabled_after"]
    assert report["outcome_ledger"]["conserved"], report["counts"]
    assert report["release_clock"]["completion_reserve_ns"] == 60_000_000
    assert report["release_clock"]["last_release_ns"] == report["end_ns"] - 60_000_000


@pytest.mark.parametrize("error,code", [(asyncio.TimeoutError, 106), (EOFError, 101)])
def test_post_send_failures_have_distinct_codes(error, code):
    frame = encode_frame(MessageKind.REQUEST, 7, request("ListAccounts", "r", {}))
    reader = SimpleNamespace(readexactly=AsyncMock(side_effect=error))
    writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
    with patch(
        "asyncio.open_unix_connection", new=AsyncMock(return_value=(reader, writer)), create=True
    ):
        result, progress = asyncio.run(exchange("/fake.sock", frame))
    assert result is None and progress.failure_code == code
    assert progress[1] > 0 and progress[2] == 0
