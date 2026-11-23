from pathlib import Path

import pytest
from sample_consumer_resources import UNITS, cpu_ids, interrupts, run, task_stat


def test_candidate_unit_is_an_explicit_sampler_target():
    assert "tradecore-performance-candidate.service" in UNITS


def test_resource_cpu_budget_is_bounded_and_explicit():
    assert cpu_ids("64-66,160-162") == {64, 65, 66, 160, 161, 162}
    for value in ("", "-1", "1-0", "0-1000000000", "0-256", "1,a", "2.0"):
        with pytest.raises(ValueError):
            cpu_ids(value)


def test_task_stat_handles_spaces_and_parentheses_in_comm_without_shifted_fields():
    fields = ["0"] * 40
    fields[0] = "S"
    for index in (7, 9, 11, 12, 19, 36, 39):
        fields[index] = str(index + 1)
    result = task_stat("123 (a thread (worker)) " + " ".join(fields))
    assert result["pid"] == 123 and result["minor_faults"] == 8
    assert result["starttime_ticks"] == 20 and result["processor"] == 37
    assert result["block_io_delay_ticks"] == 40
    with pytest.raises(ValueError):
        task_stat("broken")


def test_interrupts_remain_actual_per_cpu_and_missing_is_not_zero():
    text = " CPU0 CPU1 CPU2\n 1: 10 20 30 device\n NMI: 1 2 3 extra\n ERR: 5\n"
    assert interrupts(text, {1, 2}) == {"1": {"1": 20, "NMI": 2}, "2": {"1": 30, "NMI": 3}}
    assert interrupts("", {1}) is None
    assert interrupts(text, {3}) is None


def test_sampler_cannot_traverse_foreign_cgroups_or_create_unbounded_run(tmp_path):
    with pytest.raises(ValueError):
        run(
            root=Path("/sys/fs/cgroup/system.slice/other.service"),
            output=tmp_path / "output",
            cpus={1},
            duration=0,
        )
    assert not (tmp_path / "output").exists()
