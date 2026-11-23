"""Negative acceptance, native lifecycle and SQLite/ring crash boundaries."""

from __future__ import annotations

import runpy
import sqlite3
import struct
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from xuanling_tradecore.adapters.qmt.edge_store import QmtEdgeIdempotencyConflict, QmtEdgeStore
from xuanling_tradecore.adapters.qmt.native_engine_client import (
    QmtNativeEngineClient,
    QmtNativeEngineClientError,
    QmtNativeEventPending,
)
from xuanling_tradecore.adapters.qmt.native_engine_contract import (
    QmtNativeFrameKind,
    QmtNativeFrameV1,
    QmtRingHeaderV1,
)
from xuanling_tradecore.adapters.qmt.probe_evidence import (
    QmtProbeEvidence,
    QmtProbeEvidenceError,
    decode_durable_native_frame,
)
from xuanling_tradecore.adapters.qmt.windows_ring import QmtNativeRing

from test_qmt_probe_deployment import DEPLOY, TestAtomic, load_engine


def health(sequence, *, payload=b"PROBE_INIT", qpc=None):
    return QmtNativeFrameV1(
        sequence,
        QmtNativeFrameKind.HEALTH,
        b"\x00" * 32,
        b"\x00" * 32,
        (qpc or sequence * 100_000_000, 0, 0, 0),
        payload,
    )


def timer(tick):
    return health(tick + 1, payload=b"PROBE_TIMER" + struct.pack("<Q", tick))


def observed(evidence):
    for frame in (health(1), timer(1), timer(2), timer(3)):
        evidence.observe(frame, received_ns=frame.timestamps_ns[0])


def test_timer_summary_preserves_observed_intervals_without_claiming_schedule():
    evidence = QmtProbeEvidence()
    observed(evidence)
    summary = evidence.finish(now_ns=500_000_000)
    assert summary["timer_samples"] == 3
    assert summary["observed_interval_ns"]["p99"] == 100_000_000
    assert summary["capability_accepted"] is False
    assert summary["scheduled_time_evidence"] == "UNAVAILABLE"


def test_no_frames_or_init_only_cannot_pass():
    evidence = QmtProbeEvidence()
    with pytest.raises(QmtProbeEvidenceError, match="sufficient"):
        evidence.finish(now_ns=1)
    evidence.observe(health(1), received_ns=100_000_000)
    with pytest.raises(QmtProbeEvidenceError, match="sufficient"):
        evidence.finish(now_ns=200_000_000)
    with pytest.raises(QmtProbeEvidenceError, match="stalled"):
        evidence.check_alive(6_000_000_000)


@pytest.mark.parametrize(
    "bad",
    [
        replace(health(2), kind=QmtNativeFrameKind.FAULT, payload=b"INIT_RuntimeError"),
        health(2, payload=b"ARMED_INIT"),
        timer(2),  # missing first timer
        health(2, payload=b"PROBE_TIMER"),
        health(2),  # duplicate init
        replace(timer(1), timestamps_ns=(1, 0, 0, 0)),
        replace(timer(1), timestamps_ns=(6_000_000_000, 0, 0, 0)),
    ],
)
def test_fault_gap_or_invalid_health_never_passes(bad):
    evidence = QmtProbeEvidence()
    evidence.observe(health(1), received_ns=100_000_000)
    with pytest.raises(QmtProbeEvidenceError):
        evidence.observe(bad, received_ns=200_000_000)


def test_stop_requires_quiet_and_rejects_post_stop_timer():
    evidence = QmtProbeEvidence(expect_stop=True)
    observed(evidence)
    with pytest.raises(QmtProbeEvidenceError, match="MODEL_STOP"):
        evidence.finish(now_ns=500_000_000)
    evidence.observe(health(5, payload=b"MODEL_STOP"), received_ns=500_000_000)
    with pytest.raises(QmtProbeEvidenceError, match="quiet"):
        evidence.finish(now_ns=600_000_000)
    assert evidence.finish(now_ns=1_600_000_000)["acceptance"] == "PROBE_INIT_TIMER_STOP"
    with pytest.raises(QmtProbeEvidenceError, match="after MODEL_STOP"):
        evidence.observe(timer(5), received_ns=1_700_000_000)


@pytest.fixture
def runtime(tmp_path):
    header = QmtRingHeaderV1(b"s" * 32, b"g" * 16, 512, 4)
    command = QmtNativeRing.create_file(
        tmp_path / "command.ring", header, test_only_unsafe_fallback=True
    )
    event = QmtNativeRing.create_file(
        tmp_path / "event.ring", header, test_only_unsafe_fallback=True
    )
    store = QmtEdgeStore(tmp_path / "edge.sqlite")
    client = QmtNativeEngineClient(store=store, command_ring=command, event_ring=event)
    try:
        yield SimpleNamespace(
            command=command, event=event, store=store, client=client, root=tmp_path, header=header
        )
    finally:
        command.close()
        event.close()
        store.close()


def test_sqlite_failure_does_not_release_native_event(runtime):
    runtime.event.publish(health(1))
    with patch.object(
        runtime.store, "append_outbox", side_effect=sqlite3.OperationalError("disk full")
    ):
        with pytest.raises(sqlite3.OperationalError):
            runtime.client.drain_one_event()
    assert runtime.event.peek() == health(1)
    record = runtime.client.drain_one_event()
    assert decode_durable_native_frame(record.payload) == health(1)
    assert runtime.event.snapshot().depth == 0


def test_crash_after_sqlite_commit_replays_same_delivery(runtime):
    runtime.event.publish(health(1))
    with patch.object(runtime.event, "release_consumed", side_effect=RuntimeError("crash")):
        with pytest.raises(RuntimeError, match="crash"):
            runtime.client.drain_one_event()
    first = runtime.store.pending_outbox()[0]
    assert runtime.event.snapshot().depth == 1
    # A new client is sufficient: all recovery state is in SQLite and the ring.
    restarted = QmtNativeEngineClient(
        store=runtime.store, command_ring=runtime.command, event_ring=runtime.event
    )
    assert restarted.drain_one_event() == first
    assert len(runtime.store.pending_outbox()) == 1


def test_new_ring_generation_cannot_conflict_with_old_init(runtime):
    runtime.event.publish(health(1))
    first = runtime.client.drain_one_event()
    other = QmtNativeRing.create_file(
        runtime.root / "fresh.ring",
        replace(runtime.header, generation=b"n" * 16),
        test_only_unsafe_fallback=True,
    )
    try:
        other.publish(health(1, qpc=200_000_000))
        second = QmtNativeEngineClient(
            store=runtime.store, command_ring=runtime.command, event_ring=other
        ).drain_one_event()
        assert first.event_id != second.event_id
        assert len(runtime.store.pending_outbox()) == 2
    finally:
        other.close()


def test_same_generation_sequence_cannot_replace_a_durable_fact(runtime):
    runtime.event.publish(health(1))
    with patch.object(runtime.event, "release_consumed", side_effect=RuntimeError("crash")):
        with pytest.raises(RuntimeError):
            runtime.client.drain_one_event()
    changed = replace(health(1), request_digest=b"r" * 32)
    raw = changed.encode_uncommitted()[:-8] + struct.pack("<Q", 1)
    runtime.event._mapping[128:640] = raw
    with pytest.raises(QmtEdgeIdempotencyConflict):
        runtime.client.drain_one_event()
    assert len(runtime.store.pending_outbox()) == 1
    assert runtime.event.snapshot().depth == 1


def test_empty_is_distinct_from_corrupt_event(runtime):
    with pytest.raises(QmtNativeEventPending):
        runtime.client.drain_one_event()
    runtime.event.publish(health(1))
    runtime.event._mapping[128 + 130] ^= 1
    with pytest.raises(QmtNativeEngineClientError) as caught:
        runtime.client.drain_one_event()
    assert not isinstance(caught.value, QmtNativeEventPending)
    assert runtime.event.snapshot().depth == 1


def test_real_embedded_functions_emit_ticks_stop_and_restart_without_native_calls(runtime):
    module = load_engine()["init"].__globals__
    clock = iter(range(100_000_000, 10_000_000_000, 100_000_000))
    module["_now"] = lambda: next(clock)
    module["_Atomic"] = lambda _: TestAtomic()
    module["_load_config"] = lambda: {
        "scope_digest_hex": (b"s" * 32).hex(),
        "generation_hex": (b"g" * 16).hex(),
        "command_ring_path": str(runtime.root / "command.ring"),
        "event_ring_path": str(runtime.root / "event.ring"),
        "atomic_dll_path": "test",
        "mode": "PROBE",
        "allow_mutation": False,
        "max_drain": 1,
        "qmt_timer_period": "100nMilliSecond",
        "qmt_timer_start_time": "test",
    }
    for name in ("passorder", "cancel", "get_trade_detail_data"):
        module[name] = Mock(side_effect=AssertionError("native call in PROBE"))
    context = SimpleNamespace(run_time=Mock())
    try:
        for _ in range(2):
            evidence = QmtProbeEvidence(expect_stop=True)
            module["init"](context)
            assert not module["G"].halted
            frame = decode_durable_native_frame(runtime.client.drain_one_event().payload)
            evidence.observe(frame, received_ns=frame.timestamps_ns[0])
            for tick in range(1, 4):
                module["tradecore_qmt_drain"](context)
                frame = decode_durable_native_frame(runtime.client.drain_one_event().payload)
                assert struct.unpack("<Q", frame.payload[11:])[0] == tick
                evidence.observe(frame, received_ns=frame.timestamps_ns[0])
            module["stop"](context)
            frame = decode_durable_native_frame(runtime.client.drain_one_event().payload)
            evidence.observe(frame, received_ns=frame.timestamps_ns[0])
            module["tradecore_qmt_drain"](context)
            assert runtime.event.snapshot().depth == 0
            assert evidence.finish(now_ns=frame.timestamps_ns[0] + 1_000_000_000)
        for name in ("passorder", "cancel", "get_trade_detail_data"):
            module[name].assert_not_called()
    finally:
        module["stop"](context)


@pytest.mark.parametrize(
    "failure",
    [
        QmtNativeEventPending("empty"),
        QmtNativeEngineClientError("corrupt"),
        sqlite3.OperationalError("disk full"),
    ],
)
def test_monitor_cli_never_reports_complete_for_empty_or_failed_drain(failure, capsys):
    main = runpy.run_path(str(DEPLOY / "qmt_edge_event_monitor.py"))["main"]
    config = SimpleNamespace(is_disarmed=True, native_policy=None, generation=b"g" * 16)
    config_type = Mock(load=Mock(return_value=config))
    ring = SimpleNamespace(snapshot=lambda: SimpleNamespace(depth=0))
    runtime = SimpleNamespace(
        event_ring=ring,
        command_ring=ring,
        native_client=SimpleNamespace(drain_one_event=Mock(side_effect=failure)),
        close=Mock(),
    )
    clock = iter(range(100))
    with (
        patch.dict(
            main.__globals__,
            {
                "QmtEdgeConfigV1": config_type,
                "QmtEdgeRuntime": Mock(open=Mock(return_value=runtime)),
                "time": SimpleNamespace(
                    monotonic=lambda: next(clock), monotonic_ns=lambda: 1, sleep=lambda _: None
                ),
            },
        ),
        patch("sys.argv", ["monitor", "--config", "edge.json", "--seconds", "10"]),
    ):
        assert main() == 2
    output = capsys.readouterr().out
    assert "MONITOR_BLOCKED" in output
    assert "MONITOR_COMPLETE" not in output
    runtime.close.assert_called_once()
