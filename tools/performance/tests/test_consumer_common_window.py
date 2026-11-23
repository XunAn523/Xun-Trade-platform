import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from tools.performance import consumer_uds_common_window_probe as module


def test_client_cpu_grants_are_fixed_disjoint_and_bounded():
    module.validate_cpu_groups("1,5;2,6", "3,7", {1, 2, 3, 5, 6, 7})
    for groups, observer in (("1;1", "2"), ("1", "1"), ("1", "9"), ("1;;2", "3"), ("1.0", "2")):
        with pytest.raises(ValueError):
            module.validate_cpu_groups(groups, observer, {1, 2, 3})


def test_common_runner_has_one_gate_and_preserves_child_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(module.os, "sched_getaffinity", lambda _: {1, 2, 3}, raising=False)
    monkeypatch.setattr(module, "common_start", lambda **_: 10**10)
    processes = [Mock(returncode=0), Mock(returncode=1)]
    for child in processes:
        child.poll.return_value = child.returncode
        child.wait.return_value = child.returncode
    launcher = Mock(side_effect=processes)
    monkeypatch.setattr(module.subprocess, "Popen", launcher)
    base = dict(
        target_kind="TRADECORE_PUBLIC_UDS",
        start_ns=10**10,
        end_ns=11 * 10**9,
        formal_qualification=False,
        clock="time.monotonic_ns",
        clock_identity_sha256="a" * 64,
        tool_revision="b" * 40,
    )
    reports = [dict(base, failed=False), dict(base, failed=True)]
    monkeypatch.setattr(module, "read_report", lambda _: reports.pop(0))
    result = module.run(
        SimpleNamespace(
            cpu_groups="1;2",
            events_cpus="3",
            duration=1,
            rate=2,
            sequence_base=0,
            config=tmp_path / "config",
            output=tmp_path / "result",
            trading_day="2026-10-21",
        )
    )
    assert result["failed"] and result["child_exit_codes"] == [0, 1]
    assert result["window_contract_valid"] and not result["final_event_drain_performed"]
    assert result["performance_status"] == "NOT_EVALUATED"
    for call in launcher.call_args_list:
        args = call.args[0]
        assert args[args.index("--start-ns") + 1] == str(10**10)
        assert "--duration" in args and "--initialize" not in args
    event_command = launcher.call_args_list[1].args[0]
    assert event_command[event_command.index("--partition-concurrency") + 1] == "1"
    assert result["event_partition_concurrency"] == 1
    assert json.loads((tmp_path / "result" / "report.json").read_text())["failed"]


def test_common_runner_forwards_bounded_final_drain_without_claiming_source_authority(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(module.os, "sched_getaffinity", lambda _: {1, 2}, raising=False)
    monkeypatch.setattr(module, "common_start", lambda **_: 10**10)
    children = [Mock(returncode=0), Mock(returncode=0)]
    for child in children:
        child.poll.return_value = 0
        child.wait.return_value = 0
    launcher = Mock(side_effect=children)
    monkeypatch.setattr(module.subprocess, "Popen", launcher)
    base = dict(
        target_kind="TRADECORE_PUBLIC_UDS",
        start_ns=10**10,
        end_ns=11 * 10**9,
        formal_qualification=False,
        clock="time.monotonic_ns",
        clock_identity_sha256="a" * 64,
        tool_revision="b" * 40,
        failed=False,
    )
    reports = [base, dict(base, final_drain={"requested": True, "reached": True})]
    monkeypatch.setattr(module, "read_report", lambda _: reports.pop(0))
    result = module.run(
        SimpleNamespace(
            cpu_groups="1",
            events_cpus="2",
            duration=1,
            rate=1,
            sequence_base=0,
            config=tmp_path / "config",
            output=tmp_path / "result",
            trading_day="2026-10-21",
            drain_through_lsn=400,
            drain_timeout_seconds=7,
            partition_concurrency=2,
        )
    )
    event_command = launcher.call_args_list[1].args[0]
    assert event_command[event_command.index("--drain-through-lsn") + 1] == "400"
    assert event_command[event_command.index("--drain-timeout-seconds") + 1] == "7"
    assert event_command[event_command.index("--partition-concurrency") + 1] == "2"
    assert result["event_partition_concurrency"] == 2
    assert result["final_event_drain_performed"]
    assert result["final_event_drain_reached"]
    assert result["producer_join_status"] == "NOT_MEASURED"


def test_missing_child_report_terminates_only_launched_process(tmp_path, monkeypatch):
    monkeypatch.setattr(module.os, "sched_getaffinity", lambda _: {1, 2}, raising=False)
    child = Mock(returncode=-15)
    child.wait.side_effect = [module.subprocess.TimeoutExpired("owned", 1), -15]
    child.poll.return_value = None
    launcher = Mock(side_effect=[child, OSError("private error")])
    monkeypatch.setattr(module.subprocess, "Popen", launcher)
    result = module.run(
        SimpleNamespace(
            cpu_groups="1",
            events_cpus="2",
            duration=1,
            rate=1,
            sequence_base=0,
            config=tmp_path / "config",
            output=tmp_path / "result",
            trading_day="2026-10-21",
        )
    )
    assert result["failed"] and result["error_code"] == "COMMON_WINDOW_CHILD_OR_EVIDENCE_FAILED"
    child.terminate.assert_called_once()
    child.kill.assert_called_once()
    assert "private" not in json.dumps(result)
