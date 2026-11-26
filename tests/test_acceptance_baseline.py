import sys
from pathlib import Path

from tools.quality.run_acceptance import classify_skip, load_suite, run_bounded


def test_native_manifest_contains_all_expanded_emt_qmt_and_core_suites():
    files = load_suite("native_core")["files"]
    assert len(files) >= 23
    for name in (
        "test_oms_ems_sidecar_v1.py",
        "test_consumer_emt_submit_route_preparer_v1.py",
        "test_consumer_emt_submit_worker_v1.py",
        "test_consumer_emt_outcome_projection_v1.py",
        "test_qmt_capability_and_native_contract.py",
    ):
        assert "tests/" + name in files


def test_skip_prerequisites_are_explicit_and_unknown_skips_are_not_accepted():
    assert classify_skip("requires Linux namespace Journal", "native")[0] == "platform"
    assert classify_skip("PostgreSQL database not configured", "native")[0] == "postgres"
    assert classify_skip("native pool only", "python")[0] == "reference_not_applicable"
    assert classify_skip("native pool only", "native")[0] == "unclassified"
    assert classify_skip("unknown reason", "native")[0] == "unclassified"


def test_bounded_test_process_times_out_and_keeps_diagnostic_output(tmp_path):
    result = run_bounded(
        [sys.executable, "-u", "-c", "import time; print('started'); time.sleep(30)"],
        tmp_path / "timeout.log",
        0.3,
    )
    assert result["status"] == "timed_out"
    assert "started" in Path(result["log"]).read_text()


def test_failed_process_is_not_reported_as_passed(tmp_path):
    result = run_bounded([sys.executable, "-c", "raise SystemExit(2)"], tmp_path / "failed.log", 10)
    assert result["status"] == "failed"
    assert result["returncode"] == 2
