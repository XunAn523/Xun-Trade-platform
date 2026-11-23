from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from xuanling_tradecore.consumer.candidate_helper import OwnedCandidateTracker, _members


def lease():
    owner = OwnedCandidateTracker.__new__(OwnedCandidateTracker)
    owner._tracker = SimpleNamespace(_pid=20, _fd=30, _lock=Lock())
    owner._pid, owner._fd, owner._parent = 20, 30, 10
    owner._identity = (1, 2, 3)
    owner._started_ns = 1
    owner._closed, owner._proof = False, None
    owner._cgroup_root = Path("/sys/fs/cgroup/test.service")
    return owner


def test_helper_close_uses_owned_eof_and_finite_wait_never_unbounded_stop():
    owner = lease()
    with (
        patch("xuanling_tradecore.consumer.candidate_helper._children", side_effect=[{20}, set()]),
        patch(
            "xuanling_tradecore.consumer.candidate_helper._members", side_effect=[{10, 20}, {10}]
        ),
        patch("xuanling_tradecore.consumer.candidate_helper.mp.active_children", return_value=[]),
        patch("xuanling_tradecore.consumer.candidate_helper._identity", return_value=(1, 2, 3)),
        patch("xuanling_tradecore.consumer.candidate_helper.os.close") as close,
        patch(
            "xuanling_tradecore.consumer.candidate_helper.os.waitpid",
            create=True,
            return_value=(20, 0),
        ) as wait,
        patch("xuanling_tradecore.consumer.candidate_helper.os.WNOHANG", 1, create=True),
        patch(
            "xuanling_tradecore.consumer.candidate_helper.os.WIFEXITED",
            create=True,
            return_value=True,
        ),
        patch(
            "xuanling_tradecore.consumer.candidate_helper.os.WEXITSTATUS",
            create=True,
            return_value=0,
        ),
        patch(
            "xuanling_tradecore.consumer.candidate_helper.monotonic_clock_identity",
            return_value="c" * 64,
        ),
    ):
        proof = owner.close()
        close.assert_called_once_with(30)
        wait.assert_called_once_with(20, 1)
        assert proof["exit_code"] == proof["remaining_descendants"] == 0
        assert proof["forced_kill"] is False and owner._tracker._pid is None
        assert owner.close() == proof  # already reaped, cannot wait/close unrelated reused PID/FD


@pytest.mark.parametrize("fault", ["child", "orphan_in_cgroup", "identity", "lock"])
def test_unproven_ownership_never_closes_pipe_or_kills_any_process(fault):
    owner = lease()
    if fault == "lock":
        owner._tracker._lock = Mock()
        owner._tracker._lock.acquire.return_value = False
    with (
        patch(
            "xuanling_tradecore.consumer.candidate_helper._children",
            return_value={20, 99} if fault == "child" else {20},
        ),
        patch(
            "xuanling_tradecore.consumer.candidate_helper._members",
            return_value={10, 20, 99} if fault == "orphan_in_cgroup" else {10, 20},
        ),
        patch("xuanling_tradecore.consumer.candidate_helper.mp.active_children", return_value=[]),
        patch(
            "xuanling_tradecore.consumer.candidate_helper._identity",
            return_value=(99, 2, 3) if fault == "identity" else (1, 2, 3),
        ),
        patch("xuanling_tradecore.consumer.candidate_helper.os.close") as close,
        patch("xuanling_tradecore.consumer.candidate_helper.os.kill") as kill,
    ):
        with pytest.raises(RuntimeError):
            owner.close()
        close.assert_not_called()
        kill.assert_not_called()
        assert owner._proof is None


def test_helper_wait_timeout_is_failed_not_a_successful_strong_certificate():
    owner = lease()
    with (
        patch("xuanling_tradecore.consumer.candidate_helper._children", return_value={20}),
        patch("xuanling_tradecore.consumer.candidate_helper._members", return_value={10, 20}),
        patch("xuanling_tradecore.consumer.candidate_helper.mp.active_children", return_value=[]),
        patch("xuanling_tradecore.consumer.candidate_helper._identity", return_value=(1, 2, 3)),
        patch("xuanling_tradecore.consumer.candidate_helper.os.close"),
        patch(
            "xuanling_tradecore.consumer.candidate_helper.os.waitpid",
            create=True,
            return_value=(0, 0),
        ),
        patch("xuanling_tradecore.consumer.candidate_helper.os.WNOHANG", 1, create=True),
        patch("xuanling_tradecore.consumer.candidate_helper.monotonic", side_effect=[0, 3]),
        patch("xuanling_tradecore.consumer.candidate_helper.os.kill") as kill,
    ):
        with pytest.raises(RuntimeError, match="two seconds"):
            owner.close()
        with pytest.raises(RuntimeError, match="previously failed"):
            owner.close()
        kill.assert_not_called()
        assert owner._proof is None


def test_owned_helper_is_never_started_in_embedded_or_wrong_interpreter_context():
    with (
        patch("xuanling_tradecore.consumer.candidate_helper.require_coordinated_candidate_unit"),
        patch("xuanling_tradecore.consumer.candidate_helper.sys.platform", "linux"),
        patch("xuanling_tradecore.consumer.candidate_helper.active_count", return_value=2),
    ):
        with pytest.raises(RuntimeError, match="standalone"):
            OwnedCandidateTracker(cgroup_root=Path("/sys/fs/cgroup/test.service"))


def test_existing_tracker_is_refused_without_borrowing_or_stopping_it():
    import os

    from xuanling_tradecore.consumer import candidate_helper as module

    tracker = SimpleNamespace(_pid=123, _fd=456, ensure_running=Mock(), _stop=Mock())
    with (
        patch.object(module, "require_coordinated_candidate_unit"),
        patch.object(module.sys, "platform", "linux"),
        patch.object(module.sys, "version_info", (3, 10, 21)),
        patch.object(module, "active_count", return_value=1),
        patch.object(module.mp, "active_children", return_value=[]),
        patch.object(
            module.mp, "resource_tracker", SimpleNamespace(_resource_tracker=tracker), create=True
        ),
        patch.object(module, "_children", return_value=set()),
        patch.object(module, "_members", return_value={os.getpid()}),
    ):
        with pytest.raises(RuntimeError, match="shared tracker"):
            OwnedCandidateTracker(cgroup_root=Path("/sys/fs/cgroup/test.service"))
        tracker.ensure_running.assert_not_called()
        tracker._stop.assert_not_called()


def test_cgroup_scan_includes_orphans_and_rejects_unexpected_nesting(tmp_path):
    (tmp_path / "cgroup.procs").write_text("10\n20\n")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "cgroup.procs").write_text("99\n")
    assert _members(tmp_path) == {10, 20, 99}
    extra = nested / "unexpected"
    extra.mkdir()
    (extra / "cgroup.procs").write_text("100\n")
    with pytest.raises(RuntimeError, match="nesting"):
        _members(tmp_path)
