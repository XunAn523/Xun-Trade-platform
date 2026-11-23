from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.candidate_systemd import require_coordinated_candidate_unit


def test_gate_reads_only_own_unit_and_requires_parent_first_shutdown():
    root = Path("/sys/fs/cgroup/system.slice/tc-owned.service")
    with (
        patch("xuanling_tradecore.consumer.candidate_systemd.sys.platform", "linux"),
        patch.object(Path, "read_text", return_value="0::/system.slice/tc-owned.service\n"),
        patch("xuanling_tradecore.consumer.candidate_systemd.subprocess.run") as run,
    ):
        run.return_value = SimpleNamespace(stdout="mixed\n")
        require_coordinated_candidate_unit(root)
        assert run.call_args.args[0] == [
            "systemctl",
            "show",
            "tc-owned.service",
            "--property=KillMode",
            "--value",
        ]
        for wrong in ("control-group", "process", "", "none"):
            run.return_value = SimpleNamespace(stdout=wrong)
            with pytest.raises(ValueError, match="KillMode"):
                require_coordinated_candidate_unit(root)
        with pytest.raises(ValueError, match="another unit"):
            require_coordinated_candidate_unit(Path("/sys/fs/cgroup/system.slice/other.service"))
