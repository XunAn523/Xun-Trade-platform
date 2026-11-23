import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest

SCRIPT = Path(__file__).parents[1] / "deploy/consumer-uds/cpu_partition.py"
spec = importlib.util.spec_from_file_location("cpu_partition", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture_tree(tmp_path):
    root, cpu = tmp_path / "cgroup", tmp_path / "cpu"
    target = root / module.SLICE
    target.mkdir(parents=True)
    cpu.mkdir()
    (cpu / "online").write_text("0-191")
    for number in module.parse_cpus(module.CPUSET):
        topology = cpu / f"cpu{number}" / "topology"
        topology.mkdir(parents=True)
        (topology / "thread_siblings_list").write_text(f"{number % 96},{number % 96 + 96}")
    for name, value in {
        "cpuset.cpus": module.CPUSET,
        "cpuset.cpus.effective": module.CPUSET,
        "cpuset.cpus.exclusive": "",
        "cpuset.cpus.exclusive.effective": module.CPUSET,
        "cpuset.cpus.partition": "member",
    }.items():
        (target / name).write_text(value)
    sibling = root / "system.slice"
    sibling.mkdir()
    (sibling / "cpuset.cpus").write_text("")
    (sibling / "cpuset.cpus.effective").write_text("0-63,80-159,176-191")
    return root, cpu, target, sibling


def test_apply_and_check_are_idempotent(tmp_path):
    root, cpu, target, _ = fixture_tree(tmp_path)
    assert module.apply(root, cpu)["partition"] == "root"
    assert module.apply(root, cpu)["exclusive_cpus"] == module.CPUSET
    assert module.verify(root, module.preflight(root, cpu))["irq_isolation_claimed"] is False
    assert (target / "cpuset.cpus.partition").read_text().strip() == "root"


@pytest.mark.parametrize("failure", ["offline", "smt", "configured", "foreign", "conflict"])
def test_preflight_fails_before_mutation(tmp_path, failure):
    root, cpu, target, sibling = fixture_tree(tmp_path)
    if failure == "offline":
        (cpu / "online").write_text("0-95")
    elif failure == "smt":
        (cpu / "cpu64/topology/thread_siblings_list").write_text("64,161,999")
    elif failure == "configured":
        (target / "cpuset.cpus").write_text("64-79")
    elif failure == "foreign":
        (sibling / "cpuset.cpus").write_text("64-65")
    else:
        (sibling / "cpuset.cpus.exclusive").write_text("64")
    with pytest.raises(ValueError):
        module.apply(root, cpu)
    assert (target / "cpuset.cpus.partition").read_text() == "member"


def test_verification_failure_restores_prior_controls(tmp_path):
    root, cpu, target, sibling = fixture_tree(tmp_path)
    (sibling / "cpuset.cpus.effective").write_text("0-191")
    with pytest.raises(ValueError, match="overlaps"):
        module.apply(root, cpu)
    assert (target / "cpuset.cpus.partition").read_text().strip() == "member"
    assert not (target / "cpuset.cpus.exclusive").read_text().strip()


def test_invalid_partition_is_not_silently_replaced(tmp_path):
    root, cpu, target, _ = fixture_tree(tmp_path)
    (target / "cpuset.cpus.partition").write_text("root invalid (test)")
    with pytest.raises(ValueError, match="invalid"):
        module.apply(root, cpu)


def test_application_exception_rolls_back(tmp_path):
    root, cpu, target, _ = fixture_tree(tmp_path)
    with patch.object(module, "verify", side_effect=OSError("test failure")):
        with pytest.raises(OSError):
            module.apply(root, cpu)
    assert (target / "cpuset.cpus.partition").read_text().strip() == "member"


def test_formal_units_require_partition_and_preserve_guards():
    units = SCRIPT.parent.parent / "systemd"
    consumer = (units / "xuanling-tradecore-consumer-uds.service").read_text()
    common = (units / "tradecore-production-slice.conf").read_text()
    for content in (consumer, common):
        assert "Requires=xuanling-tradecore-cpu-partition.service" in content
        assert "After=xuanling-tradecore-cpu-partition.service" in content
        assert "Slice=tradecore.slice" in content
        assert (
            "ExecStartPre=+/usr/bin/python3 -I /usr/local/libexec/tradecore-cpu-partition.py check"
            in content
        )
    helper = (units / "xuanling-tradecore-cpu-partition.service").read_text()
    assert "/usr/local/libexec/tradecore-cpu-partition.py apply" in helper
    assert "/opt/xuanling-tradecore/deploy/consumer-uds/cpu_partition.py" not in helper
    assert "MemoryHigh=4831838208" in consumer
    assert "MemoryMax=6G" in consumer
    assert "KillMode=mixed" in consumer
    assert "TimeoutStopSec=75" in consumer
    assert "--disarmed" in (units / "xuanling-tradecore.service").read_text()
