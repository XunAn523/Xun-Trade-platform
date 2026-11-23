import pytest
from perfbench.short_probe_contract import (
    OPERATIONS,
    RESULTS,
    common_start,
    common_window,
    compatible_clocks,
    outcome_ledger,
)


def complete_counts():
    return {
        f"{op}.{name}": 10
        for op in OPERATIONS
        for name in (
            "release_attempted",
            "enqueued",
            "offered",
            "result.0",
            "completed",
            "completed_in_window",
        )
    }


def test_complete_ledger_is_conserved():
    ledger = outcome_ledger(complete_counts(), planned_each=10)
    assert ledger["conserved"] and ledger["unissued"] == 0
    assert not ledger["transport_failed"] and not ledger["request_failed"]


def test_release_holes_are_not_transport_failures():
    counts = complete_counts()
    for op in OPERATIONS:
        for name in ("enqueued", "offered", "result.0", "completed", "completed_in_window"):
            counts[f"{op}.{name}"] = 6
        counts[f"{op}.release_attempted"] = 8
        counts[f"{op}.release_rejected"] = 2
    ledger = outcome_ledger(counts, planned_each=10)
    assert ledger["conserved"] and ledger["unissued"] == 8
    assert not ledger["transport_failed"]
    assert ledger["operations"][OPERATIONS[0]]["not_attempted"] == 2


@pytest.mark.parametrize("code", [code for code in RESULTS if code])
def test_failure_classes_preserve_count_conservation(code):
    counts = complete_counts()
    op = OPERATIONS[0]
    for name in ("result.0", "completed", "completed_in_window"):
        counts[f"{op}.{name}"] = 9
    counts[f"{op}.result.{code}"] = 1
    ledger = outcome_ledger(counts, planned_each=10)
    assert ledger["conserved"] and ledger["request_failed"]
    assert ledger["transport_failed"] == (code in {100, 101, 105, 106, 107})


def test_late_completion_is_not_in_window():
    counts = complete_counts()
    counts[f"{OPERATIONS[0]}.completed_in_window"] = 9
    counts[f"{OPERATIONS[0]}.completed_after_window"] = 1
    assert outcome_ledger(counts, planned_each=10)["conserved"]


@pytest.mark.parametrize(
    "field", ["SubmitSignal.result.999", "generator_unrecorded_error", "evidence_write_errors"]
)
def test_unknown_or_missing_evidence_fails_conservation(field):
    counts = complete_counts()
    counts[field] = 1
    assert not outcome_ledger(counts, planned_each=10)["conserved"]


@pytest.mark.parametrize("counts", [{1: 2}, {"x": True}, {"x": -1}])
def test_malformed_ledger_rejected(counts):
    with pytest.raises(ValueError):
        outcome_ledger(counts, planned_each=10)


@pytest.mark.parametrize("value", [True, 0, 999, 60_000_001_001])
def test_common_start_rejects_expired_or_unbounded_gate(value):
    with pytest.raises(ValueError):
        common_start(now_ns=1000, supplied_ns=value)


def test_common_gate_and_clock_identity():
    assert common_start(now_ns=1000, supplied_ns=2000) == 2000
    assert common_start(now_ns=1000, supplied_ns=None) == 10_000_001_000
    assert compatible_clocks([{}, {}]) is None
    assert compatible_clocks([{"clock_identity_sha256": "a" * 64}] * 2) == "a" * 64
    for reports in ([{}, {"clock_identity_sha256": "a" * 64}], [{"clock_identity_sha256": "bad"}]):
        with pytest.raises(ValueError):
            compatible_clocks(reports)


def test_shared_window_is_not_a_performance_or_source_certificate():
    report = dict(
        target_kind="TRADECORE_PUBLIC_UDS",
        tool_revision="c" * 40,
        start_ns=100,
        end_ns=1000,
        clock_identity_sha256="a" * 64,
        clock="time.monotonic_ns",
        formal_qualification=False,
    )
    merged = common_window(report, dict(report))
    assert merged["window_contract_valid"]
    assert merged["producer_join_status"] == "NOT_MEASURED"
    assert merged["performance_status"] == "NOT_EVALUATED"
    for field, value in (
        ("start_ns", 101),
        ("end_ns", 1001),
        ("clock_identity_sha256", "b" * 64),
        ("clock", "time.time_ns"),
        ("formal_qualification", True),
        ("tool_revision", "d" * 40),
        ("target_kind", "CONTROLLED_GENERATOR_FIXTURE"),
    ):
        with pytest.raises(ValueError):
            common_window(report, dict(report, **{field: value}))
