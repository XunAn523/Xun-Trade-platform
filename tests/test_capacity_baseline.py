from tools.benchmarks.capacity_baseline import run_matrix, summarize


def test_matrix_runs_fixed_rate_and_burst_with_accounting_and_source(tmp_path):
    result = run_matrix(
        output=tmp_path, rates=[1000], orders=3, repeats=1, allow_dirty=True, timeout_seconds=30
    )
    assert result["status"] == "passed"
    assert result["finished_trials"] == result["expected_trials"] == 2
    assert [t["measurement"]["target_rate_per_second"] for t in result["trials"]] == [1000, 0]
    assert all(t["measurement"]["source"] == result["source"] for t in result["trials"])


def test_no_completions_cannot_look_like_zero_latency_success():
    measurement = {
        "target_rate_per_second": 200,
        "completed": 0,
        "rejected": 10,
        "throughput_per_second": 0,
        "completion_ratio": 0,
        "latency": {"p99_ns": 0},
        "generator_lateness": {"p99_ns": 0},
        "queue_wait": {"p99_ns": 0},
        "deadline_ms": 100,
        "within_deadline_ratio_of_offered": 0,
    }
    row = summarize([{"status": "passed", "measurement": measurement}])[0]
    assert row["per_trial_latency_p99_ms"] is None
    assert row["trials_with_completions"] == 0
    assert row["within_deadline_ratio_of_offered"]["max"] == 0
