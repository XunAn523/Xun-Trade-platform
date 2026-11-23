import json

import pytest
from tools.performance.analyze_broker_handoff import _load_group, summarize
from xuanling_tradecore.persistence.rotating_request_trace import RotatingRequestTraceFile


def records():
    digest = "a" * 64
    frontend = [
        {
            "operation": "SubmitSignal",
            "request_id_sha256": digest,
            "owner_generation": "generation",
            "ipc_frontend_offered_ns": 10,
        }
    ]
    broker = [
        {
            "operation": "SubmitSignal",
            "request_id_sha256": digest,
            "generation": "generation",
            "serial": 1,
            "broker_outstanding_before": 0,
            "broker_frontend_received_ns": 11,
            "broker_owner_offered_ns": 12,
            "broker_final_send_before_ns": 13,
            "broker_final_send_after_ns": 16,
        }
    ]
    owner = [
        {
            "operation": "SubmitSignal",
            "request_id_sha256": digest,
            "generation": "generation",
            "serial": 1,
            "ipc_owner_received_ns": 15,
        }
    ]
    return frontend, broker, owner


def test_correlates_syscall_overlap_without_negative_latency():
    f, b, o = records()
    result = summarize(f, b, o, start_ns=1, end_ns=20)
    assert result["paired_window_requests"] == 1
    assert result["owner_received_before_send_return"] == 1
    assert result["intervals"]["send_after_to_owner_lower"]["p100_ns"] == 0
    assert result["intervals"]["frontend_to_owner"]["p100_ns"] == 5


def test_rejects_missing_or_duplicate_or_wrong_serial():
    f, b, o = records()
    with pytest.raises(ValueError, match="coverage"):
        summarize(f, b, [], start_ns=1, end_ns=20)
    with pytest.raises(ValueError, match="duplicate"):
        summarize(f + f, b, o, start_ns=1, end_ns=20)
    o[0]["serial"] = 2
    with pytest.raises(ValueError, match="serial"):
        summarize(f, b, o, start_ns=1, end_ns=20)


def test_rejects_empty_generation_and_boolean_serial():
    f, b, o = records()
    f[0]["owner_generation"] = ""
    with pytest.raises(ValueError, match="identity"):
        summarize(f, b, o, start_ns=1, end_ns=20)
    f, b, o = records()
    b[0]["serial"] = o[0]["serial"] = True
    with pytest.raises(ValueError, match="serial"):
        summarize(f, b, o, start_ns=1, end_ns=20)


def test_trace_reader_rejects_partial_status_and_missing_file_records(tmp_path):
    clock = "a" * 64
    status = {
        "clock_identity_sha256": clock,
        "closed": True,
        "drain_complete": True,
        "writer_failed": False,
        "queue_dropped": 0,
        "capacity_dropped": 0,
        "submitted": 1,
        "written": 1,
    }
    path = tmp_path / "broker-diagnostic-one.status.json"
    path.write_text(json.dumps(status))
    with pytest.raises(ValueError, match="file count"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock)
    (tmp_path / "broker-diagnostic-one.jsonl").write_text('{"operation":"SubmitSignal"}\n')
    assert len(_load_group(tmp_path, "broker-diagnostic-", 1, clock)) == 1
    status["queue_dropped"] = 1
    path.write_text(json.dumps(status))
    with pytest.raises(ValueError, match="incomplete"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock)


def test_strict_trace_reader_rejects_single_file_without_integrity_manifest(tmp_path):
    clock = "a" * 64
    status = {
        "clock_identity_sha256": clock,
        "closed": True,
        "drain_complete": True,
        "writer_failed": False,
        "queue_dropped": 0,
        "capacity_dropped": 0,
        "submitted": 1,
        "written": 1,
        "trace_storage": {"mode": "single"},
    }
    (tmp_path / "broker-diagnostic-one.status.json").write_text(json.dumps(status))
    (tmp_path / "broker-diagnostic-one.jsonl").write_text('{"operation":"SubmitSignal"}\n')
    assert len(_load_group(tmp_path, "broker-diagnostic-", 1, clock)) == 1
    with pytest.raises(ValueError, match="hash chain"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock, require_hash_chain=True)


def test_rotating_reader_checks_hash_chain_and_rejects_same_count_tampering(tmp_path):
    clock = "a" * 64
    path = tmp_path / "broker-diagnostic-one.jsonl"
    trace = RotatingRequestTraceFile(path, journal_id="journal", segment_bytes=256)
    trace.write(b'{"operation":"SubmitSignal","trace_sequence":1}\n')
    trace.close()
    status = {
        "clock_identity_sha256": clock,
        "journal_id": "journal",
        "closed": True,
        "drain_complete": True,
        "writer_failed": False,
        "queue_dropped": 0,
        "capacity_dropped": 0,
        "submitted": 1,
        "written": 1,
        "trace_storage": {"mode": "rotating"},
    }
    (tmp_path / "broker-diagnostic-one.status.json").write_text(json.dumps(status))
    assert len(_load_group(tmp_path, "broker-diagnostic-", 1, clock, require_hash_chain=True)) == 1
    path.write_bytes(b'{"operation":"SubmitSignaX","trace_sequence":1}\n')
    with pytest.raises(ValueError, match="integrity"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock, require_hash_chain=True)


def test_rotating_reader_rejects_unregistered_file_and_journal_mismatch(tmp_path):
    clock = "a" * 64
    path = tmp_path / "broker-diagnostic-one.jsonl"
    trace = RotatingRequestTraceFile(path, journal_id="journal", segment_bytes=256)
    trace.write(b'{"operation":"SubmitSignal","trace_sequence":1}\n')
    trace.close()
    status = {
        "clock_identity_sha256": clock,
        "journal_id": "journal",
        "closed": True,
        "drain_complete": True,
        "writer_failed": False,
        "queue_dropped": 0,
        "capacity_dropped": 0,
        "submitted": 1,
        "written": 1,
        "trace_storage": {"mode": "rotating"},
    }
    status_path = tmp_path / "broker-diagnostic-one.status.json"
    status_path.write_text(json.dumps(status))
    extra = tmp_path / "broker-diagnostic-one-000001.jsonl"
    extra.write_bytes(b"")
    with pytest.raises(ValueError, match="segment set"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock, require_hash_chain=True)
    extra.unlink()
    status["journal_id"] = "other"
    status_path.write_text(json.dumps(status))
    with pytest.raises(ValueError, match="manifest"):
        _load_group(tmp_path, "broker-diagnostic-", 1, clock, require_hash_chain=True)
