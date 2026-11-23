from __future__ import annotations

from collections import Counter
from types import SimpleNamespace
from unittest.mock import Mock, patch

from consumer_uds_burst_probe import MAX_BURST_COUNT
from consumer_uds_burst_probe import _valid_trading_day as burst_day_is_valid
from consumer_uds_hot_probe import (
    HOT_LATENCY_SLO_PROFILE,
    HOT_P95_LIMIT_MS,
    HOT_P99_LIMIT_MS,
    _call_one,
    _latency_slo_passed,
    _latency_summary,
    _percentile,
    hot_request,
)
from consumer_uds_hot_probe import _valid_trading_day as hot_day_is_valid


def test_hot_percentiles_are_nearest_rank_and_include_p999_only_at_10k() -> None:
    assert _percentile([], 0.99) is None
    assert _percentile([4.0, 1.0, 3.0, 2.0], 0.50) == 2.0
    assert _latency_summary([1.0] * 9_999)["p99_9"] is None
    values = [float(index) for index in range(1, 10_001)]
    summary = _latency_summary(values)
    assert summary == {
        "p50": 5_000.0,
        "p95": 9_500.0,
        "p99": 9_900.0,
        "p99_9": 9_990.0,
        "p100": 10_000.0,
    }


def test_burst_probe_exposes_5k_and_first_bounded_overload_width() -> None:
    assert MAX_BURST_COUNT == 16_384
    assert MAX_BURST_COUNT > 10_240 > 5_000


def test_zero_warmup_counter_matches_the_empty_observation() -> None:
    assert Counter() == Counter({"ACCEPTED": 0})


def test_hot_slo_is_the_frozen_multifrontend_contract() -> None:
    assert HOT_LATENCY_SLO_PROFILE == "fixed-multifrontend-fresh-request-v1"
    assert (HOT_P95_LIMIT_MS, HOT_P99_LIMIT_MS) == (1.50, 3.00)
    assert _latency_slo_passed({"p95": 1.50, "p99": 3.00}, 10_000, 10_000)
    assert not _latency_slo_passed({"p95": 1.500001, "p99": 3.00}, 10_000, 10_000)
    assert not _latency_slo_passed({"p95": 1.50, "p99": 3.000001}, 10_000, 10_000)
    assert not _latency_slo_passed({"p95": 1.00, "p99": 2.00}, 9_999, 10_000)


def test_consumer_probes_require_the_contract_iso_trading_day() -> None:
    for validator in (hot_day_is_valid, burst_day_is_valid):
        assert validator("2026-10-17")
        assert not validator("20261017")
        assert not validator("2026-02-30")


def test_hot_records_actual_start_and_validation_end_not_reconstructed_timestamps():
    client = Mock()
    client.call.return_value = dict(
        message_type="response",
        schema_version=2,
        request_id="r",
        status="ACCEPTED",
        payload=dict(journal_id="j", journal_lsn=10, oms_order_id="o"),
    )
    with patch("consumer_uds_hot_probe.monotonic_ns", side_effect=[123, 456]):
        result = _call_one(client, {"request_id": "r"})
    assert result.started_ns == 123 and result.finished_ns == 456 and result.latency_ns == 333
    assert result.status == "ACCEPTED" and result.journal_lsn == 10
    client.call.return_value["payload"]["journal_lsn"] = True
    assert _call_one(client, {"request_id": "r"}).status == "INVALID_RESPONSE"
    client.call.side_effect = TimeoutError("private password")
    result = _call_one(client, {"request_id": "r"})
    assert result.status == "TimeoutError" and result.journal_lsn is None
    assert "private" not in repr(result)


def test_persistent_hot_namespace_is_explicit_and_does_not_reuse_warmup_keys():
    config = SimpleNamespace(consumer_account_id="c", strategy_id="s")
    first = hot_request(
        config, run_id="r", sequence=0, phase="warmup", day="2026-10-21", candidate=True, base=100
    )
    measured = hot_request(
        config, run_id="r", sequence=0, phase="measure", day="2026-10-21", candidate=True, base=110
    )
    assert first["payload"]["strategy_order_id"] == "LR-100"
    assert measured["payload"]["strategy_order_id"] == "LR-110"
    assert measured["payload"]["strategy_id"] == "s-H"
