from __future__ import annotations

from tools.acceptance.emt_query_terminal_supervisor_v1 import (
    _missing_worker_report,
    _progress_phase,
    _write_report,
)


def test_missing_worker_report_is_explicitly_not_terminal_success() -> None:
    report = _missing_worker_report(
        api_name="QueryYesterdayAsset",
        profile="ordinary",
        worker_return_code=-6,
        worker_timed_out=False,
        progress_phase="before_report",
    )
    assert report["result"] == {
        "api": "QueryYesterdayAsset",
        "profile": "ordinary",
        "worker_report_persisted": False,
        "worker_return_code": -6,
        "worker_timed_out": False,
        "payload_free_progress_phase": "before_report",
        "terminal_verified": False,
        "failure_class": "native_worker_terminated_before_payload_free_report",
    }


def test_progress_phase_rejects_non_scalar_or_non_ascii_values(tmp_path) -> None:
    path = tmp_path / "progress.json"
    path.write_text('{"phase":"before_report"}', encoding="utf-8")
    assert _progress_phase(path) == "before_report"
    path.write_text('{"phase":{"unsafe":true}}', encoding="utf-8")
    assert _progress_phase(path) is None
    path.write_text('{"phase":"\u4e0d\u5b89\u5168"}', encoding="utf-8")
    assert _progress_phase(path) is None


def test_supervisor_report_never_overwrites_existing_evidence(tmp_path) -> None:
    output = tmp_path / "report.json"
    _write_report(output, {"result": {"terminal_verified": False}})
    try:
        _write_report(output, {"result": {"terminal_verified": True}})
    except Exception as exc:
        assert type(exc).__name__ == "EmtQueryTerminalSupervisorError"
    else:  # pragma: no cover - the property under test
        raise AssertionError("existing evidence was overwritten")
