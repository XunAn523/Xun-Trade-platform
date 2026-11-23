from copy import deepcopy

import pytest
from tools.performance.consumer_uds_candidate_probe import diagnostic_checks


def valid():
    counts, histograms = {}, {}
    for op in ("SubmitSignal", "ReadOrderSnapshotPage"):
        for name in ("offered", "completed", "completed_in_window"):
            counts[f"{op}.{name}"] = 20_000
        for name in ("scheduled", "generator_queue"):
            histograms[f"{op}.{name}"] = {"count": 20_000, "p95_ns": 1000, "p99_ns": 2000}
    return dict(
        counts=counts, histograms=histograms, planned_each=20_000, unissued=0,
        transport_failed=False,
    )


def test_short_checks_accept_complete_window_without_claiming_formal_qualification():
    assert all(diagnostic_checks(**valid()).values())


@pytest.mark.parametrize("op", ("SubmitSignal", "ReadOrderSnapshotPage"))
def test_complete_drain_outside_window_is_not_a_pass(op):
    sample = valid()
    sample["counts"][f"{op}.completed_in_window"] -= 1
    checks = diagnostic_checks(**sample)
    assert checks[f"{op}.completed"]
    assert not checks[f"{op}.completed_in_window"]


@pytest.mark.parametrize("metric,bound", (("scheduled", 100_000_000), ("generator_queue", 10_000_001)))
def test_transport_success_cannot_hide_late_calls_or_release_queue(metric, bound):
    sample = valid()
    sample["histograms"][f"SubmitSignal.{metric}"]["p99_ns"] = bound
    assert not all(diagnostic_checks(**sample).values())


def test_missing_metrics_and_unreleased_work_fail_closed():
    sample = valid()
    for field in ("counts", "histograms"):
        missing = deepcopy(sample)
        missing[field] = {}
        assert not all(diagnostic_checks(**missing).values())
    sample["unissued"] = 1
    assert not diagnostic_checks(**sample)["all_released"]
