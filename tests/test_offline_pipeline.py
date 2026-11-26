import pytest
from tools.benchmarks.offline_pipeline import arrival_ns, distribution, run_pipeline


def test_percentiles_use_nearest_rank():
    result = distribution(list(range(1, 101)))
    assert result["p50_ns"] == 50
    assert result["p95_ns"] == 95
    assert result["p99_ns"] == 99


def test_offline_pipeline_accounts_for_every_order_and_durable_callback():
    result = run_pipeline(orders=12, batch_size=4, capacity=32)
    assert result["completed"] == result["fake_broker_calls"] == 12
    assert result["rejected"] == 0
    assert result["gateway_c0_c7"]["count"] == 12
    assert result["journal_flush"]["count"] >= 24


def test_backpressure_rejects_without_fake_broker_retry():
    result = run_pipeline(
        orders=300, batch_size=300, capacity=1, broker_delay_us=2000, deadline_ms=1
    )
    assert result["rejected"] > 0
    assert result["offered"] == result["rejected"] + result["completed"]
    assert result["fake_broker_calls"] == result["completed"]
    assert result["rejection_latency"]["count"] == result["rejected"]
    assert result["within_deadline_ratio_of_offered"] == result["within_deadline"] / 300
    assert result["within_deadline_ratio_of_offered"] <= result["completion_ratio"]


def test_arrivals_keep_fixed_schedule_even_when_previous_work_is_late():
    assert [arrival_ns(100, i, 4) for i in range(4)] == [100, 250000100, 500000100, 750000100]


def test_paced_pipeline_measures_schedule_lateness_and_completion_separately(tmp_path):
    result = run_pipeline(
        orders=6, batch_size=3, target_rate=100, deadline_ms=1000, journal_root=tmp_path
    )
    assert result["actual_offer_window_ns"] >= result["last_scheduled_arrival_ns"] == 50_000_000
    assert result["generator_lateness"]["count"] == 6
    assert result["completed"] + result["rejected"] == 6
    assert result["source"]["commit"]
    assert result["completion_ratio"] + result["rejection_ratio"] == 1
    assert result["completed_during_offer"] <= result["completed"]


@pytest.mark.parametrize("rate", [-1, float("inf"), float("nan")])
def test_invalid_rates_are_rejected_before_running(rate):
    with pytest.raises(ValueError, match="rate"):
        run_pipeline(target_rate=rate)
