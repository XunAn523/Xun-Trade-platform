from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.owner_thread_identity import (
    _start_ticks,
    owner_thread_snapshot,
    set_evidence_thread_affinity,
    write_owner_thread_snapshot,
)


def test_thread_stat_parser_rejects_invalid_linux_data() -> None:
    with patch.object(Path, "read_text", return_value="invalid"):
        with pytest.raises(ValueError):
            _start_ticks(1)


@pytest.mark.parametrize("value", ["", "-1", "1,1", "1,2,3", "65536", "64"])
def test_evidence_affinity_rejects_invalid_or_writer_overlap(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = "xuanling_tradecore.consumer.owner_thread_identity"
    monkeypatch.setattr(f"{module}.threading.enumerate", lambda: [
        SimpleNamespace(name="oms-inherited-write", native_id=1),
        SimpleNamespace(name="source-commit-evidence", native_id=2),
    ])
    monkeypatch.setattr(f"{module}._start_ticks", lambda tid: 100 + tid)
    monkeypatch.setattr(f"{module}.os.sched_getaffinity", lambda tid: {64}, raising=False)
    with pytest.raises(ValueError):
        set_evidence_thread_affinity(value)


def test_evidence_affinity_moves_only_named_evidence_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = "xuanling_tradecore.consumer.owner_thread_identity"
    monkeypatch.setattr(f"{module}.threading.enumerate", lambda: [
        SimpleNamespace(name="oms-inherited-write", native_id=1),
        SimpleNamespace(name="source-commit-evidence", native_id=2),
        SimpleNamespace(name="consumer-trace-writer", native_id=3),
        SimpleNamespace(name="consumer-mock-lifecycle", native_id=4),
    ])
    monkeypatch.setattr(f"{module}._start_ticks", lambda tid: 100 + tid)
    affinity = {tid: {64} for tid in range(1, 5)}
    monkeypatch.setattr(f"{module}.os.sched_getaffinity", lambda tid: affinity[tid], raising=False)

    def set_affinity(tid: int, cpus: set[int]) -> None:
        affinity[tid] = set(cpus)

    monkeypatch.setattr(f"{module}.os.sched_setaffinity", set_affinity, raising=False)
    set_evidence_thread_affinity("171,172")
    assert affinity == {1: {64}, 2: {171, 172}, 3: {171, 172}, 4: {64}}

    affinity = {tid: {64} for tid in range(1, 5)}

    def fail_second(tid: int, cpus: set[int]) -> None:
        if tid == 3:
            raise OSError("injected failure")
        affinity[tid] = set(cpus)

    monkeypatch.setattr(f"{module}.os.sched_setaffinity", fail_second)
    with pytest.raises(OSError):
        set_evidence_thread_affinity("171,172")
    assert affinity == {tid: {64} for tid in range(1, 5)}

    def clipped(tid: int, cpus: set[int]) -> None:
        affinity[tid] = {171} if cpus == {171, 172} else set(cpus)

    monkeypatch.setattr(f"{module}.os.sched_setaffinity", clipped)
    with pytest.raises(ValueError, match="clipped"):
        set_evidence_thread_affinity("171,172")
    assert affinity == {tid: {64} for tid in range(1, 5)}


@pytest.mark.skipif(
    os.name != "posix" or not Path("/proc/self/task").is_dir(), reason="Linux procfs required"
)
def test_snapshot_joins_named_live_thread_and_redacts_unknown_names(tmp_path: Path) -> None:
    started, done = threading.Event(), threading.Event()

    def worker() -> None:
        started.set()
        done.wait(5)

    known = threading.Thread(target=worker, name="oms-inherited-write")
    unknown = threading.Thread(target=worker, name="secret-example-name")
    known.start()
    assert started.wait(2)
    unknown.start()
    try:
        report = owner_thread_snapshot()
        assert report["pid"] == os.getpid() and report["complete"]
        rows = {row["native_id"]: row for row in report["threads"]}
        assert rows[known.native_id]["role"] == "oms-inherited-write"
        assert rows[known.native_id]["start_ticks"] == _start_ticks(known.native_id)
        assert rows[known.native_id]["cpus"] == sorted(os.sched_getaffinity(known.native_id))
        assert rows[unknown.native_id]["role"] == "OTHER"
        assert "secret-example-name" not in json.dumps(report)
        path = tmp_path / "threads.json"
        write_owner_thread_snapshot(path)
        assert json.loads(path.read_text())["schema"] == "consumer-owner-thread-startup-v1"
        assert path.stat().st_mode & 0o077 == 0
    finally:
        done.set()
        known.join(2)
        unknown.join(2)
