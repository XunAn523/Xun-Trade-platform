from tools.benchmarks.offline_pipeline import distribution, run_pipeline


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
    result = run_pipeline(orders=300, batch_size=300, capacity=1, broker_delay_us=2000)
    assert result["rejected"] > 0
    assert result["offered"] == result["rejected"] + result["completed"]
    assert result["fake_broker_calls"] == result["completed"]
