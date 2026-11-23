import json
import sqlite3
import time
from hashlib import sha256
from unittest.mock import Mock, patch

import pytest
from test_consumer_event_checkpoint import page, plan
from test_consumer_rolling_events_probe import config
from tools.performance.consumer_uds_rolling_events_probe import run_probe
from tools.performance.join_consumer_source_timing import PAIR, join_consumed
from xuanling_tradecore.consumer.mock_lifecycle import MOCK_PRODUCER_MODEL
from xuanling_tradecore.persistence.commit_timing import CommitTimingWriter
from xuanling_tradecore.persistence.runtime_files import atomic_runtime_json


def bundle(tmp_path, partial=False):
    source = tmp_path / "source"
    with patch(
        "xuanling_tradecore.persistence.commit_timing.monotonic_clock_identity",
        return_value="c" * 64,
    ):
        writer = CommitTimingWriter(
            root=source,
            journal_id="test-journal",
            first_lsn=1,
            maximum_bytes=4096,
            producer_model=MOCK_PRODUCER_MODEL,
        )
    started = time.monotonic_ns()
    for n in range(4):
        writer.observe(n * 2 + 1, n * 2 + 2, 2, started + n * 10, started + n * 10 + 1)
    writer.close()
    client = Mock()
    responses = [plan(), page((2, 4) if partial else (2, 4, 6, 8), more=partial)]

    def reply(request):
        payload = (
            responses.pop(0)
            if responses
            else dict(
                journal_id="test-journal",
                stream_generation=1,
                event_count=0,
                high_watermark_position=None,
                partitions=[],
            )
        )
        if partial and request["operation"] == "PollEvents":
            time.sleep(1.01)
        return dict(
            message_type="response",
            schema_version=2,
            status="COMPLETED",
            request_id=request["request_id"],
            payload=payload,
        )

    client.call.side_effect = reply
    with patch(
        "tools.performance.consumer_uds_rolling_events_probe.monotonic_clock_identity",
        return_value="c" * 64,
    ):
        report = run_probe(
            config=config(),
            output=tmp_path / "progress",
            checkpoint_root=tmp_path / "checkpoint",
            initialize=True,
            duration=1,
            client=client,
        )
    assert not report["failed"]
    return dict(
        source_root=source,
        checkpoint_root=tmp_path / "checkpoint",
        progress_root=tmp_path / "progress",
    )


def final_status(tmp_path):
    source = json.loads((tmp_path / "source/manifest.json").read_text())
    value = {
        "closed": True,
        "drain_complete": True,
        "writer_failed": False,
        "status_provider_failed": False,
        "candidate_only": True,
        "full_qualification": False,
        "queue_dropped": 0,
        "capacity_dropped": 0,
        "submitted": 100,
        "written": 100,
        "mock_producer_model": MOCK_PRODUCER_MODEL,
        "mock": dict(
            retired=2,
            filled=2,
            rejected=0,
            state="STOPPED",
            error_code=None,
            native_calls=0,
            pending_archives=0,
        ),
        "journal": dict(
            journal_id="test-journal",
            next_lsn=9,
            maintenance_failed=False,
            commit_timing_failed=False,
        ),
        "projector": dict(
            state="STOPPED", error_code=None, projected_lsn=8, observed_committed_lsn=8
        ),
        "source_commit_timing": source,
    }
    path = tmp_path / "final-status.json"
    atomic_runtime_json(path, value)
    return path


def lifecycle(
    tmp_path, status, *, schema="consumer-candidate-final-lifecycle-v2", frontend_count=2,
    read_roles=("owner-read-plane",), read_broker=True
):
    path = tmp_path / "final-lifecycle.json"
    topology = [
        {
            "role": role,
            "kind": "THREAD" if role == "public-acceptor" else "PROCESS",
            "pid": index + 10,
            "exit_code": 0,
            "shutdown_verified": True,
        }
        for index, role in enumerate(
            (
                "public-acceptor",
                *(f"public-frontend-{index}" for index in range(frontend_count)),
                "write-broker",
                *(("read-broker",) if read_broker else ()),
                "oms-owner",
                *read_roles,
            )
        )
    ]
    topology_hash = sha256(
        json.dumps(topology, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    atomic_runtime_json(
        path,
        {
            "schema": schema,
            "runtime_closed": True,
            "all_managed_workers_exited_zero": True,
            "all_owned_descendants_exited_zero": True,
            "owned_helper_exit_status": "VERIFIED",
            "observed_monotonic_ns": time.monotonic_ns(),
            "helper_exit": {
                "schema": "candidate-owned-helper-exit-v1",
                "helper_kind": "cpython310-resource-tracker",
                "pid": 2,
                "parent_pid": 1,
                "start_ticks": 1,
                "started_ns": 1,
                "exited_ns": 2,
                "ownership_verified": True,
                "forced_kill": False,
                "exit_code": 0,
                "remaining_descendants": 0,
                "clock_identity_sha256": "c" * 64,
            },
            "formal_qualification": False,
            "producer_model": MOCK_PRODUCER_MODEL,
            "journal_id": "test-journal",
            "final_lsn": 8,
            "trace_status_sha256": sha256(status.read_bytes()).hexdigest(),
            "source_timing_manifest_sha256": sha256(
                (tmp_path / "source/manifest.json").read_bytes()
            ).hexdigest(),
            **(
                {
                    "lifecycle_components": topology,
                    "lifecycle_components_sha256": topology_hash,
                }
                if schema == "consumer-multi-frontend-lifecycle-v1"
                else {}
            ),
        },
    )
    return path


def test_exact_consumed_event_pairs_preserve_raw_times_and_never_export_tokens(tmp_path):
    inputs = bundle(tmp_path)
    result = join_consumed(**inputs, output=tmp_path / "join")
    assert result["matched_events"] == 4 and result["checkpoint_pages"] == 1
    assert result["formal_qualification"] is False
    assert result["producer_closure_status"] == "NOT_EVALUATED"
    data = (tmp_path / "join/paired.bin").read_bytes()
    assert len(data) == PAIR.size * 4
    rows = list(PAIR.iter_unpack(data))
    assert [row[1] for row in rows] == [2, 4, 6, 8]
    assert all(row[2] <= row[3] <= row[4] for row in rows)
    assert b"private-" not in b"".join(p.read_bytes() for p in (tmp_path / "join").iterdir())


def test_partial_consumption_is_not_reported_as_all_producer_events_delivered(tmp_path):
    inputs = bundle(tmp_path, partial=True)
    result = join_consumed(**inputs, output=tmp_path / "join")
    assert result["matched_events"] == 2 and result["source_batches"] == 4
    assert result["coverage"] == "CONSUMED_EVENTS_ONLY"
    assert result["producer_closure_status"] == "NOT_EVALUATED"
    assert not result["formal_qualification"]
    with pytest.raises(ValueError):
        path = final_status(tmp_path)
        join_consumed(
            **inputs,
            output=tmp_path / "closure",
            final_status=path,
            final_lifecycle=lifecycle(tmp_path, path),
        )


def test_fresh_mock_closure_requires_all_source_positions_and_durable_frontier(tmp_path):
    inputs = bundle(tmp_path)
    path = final_status(tmp_path)
    result = join_consumed(
        **inputs,
        output=tmp_path / "join",
        final_status=path,
        final_lifecycle=lifecycle(tmp_path, path),
    )
    assert result["producer_closure_status"] == "MATCHED_FRESH_MOCK_SOURCE"
    assert result["closed_mock_orders"] == 2
    assert result["matched_events"] == 4 and not result["formal_qualification"]


def test_process_owner_lifecycle_can_close_the_same_verified_source(tmp_path):
    inputs = bundle(tmp_path)
    path = final_status(tmp_path)
    result = join_consumed(
        **inputs,
        output=tmp_path / "join",
        final_status=path,
        final_lifecycle=lifecycle(
            tmp_path, path, schema="consumer-process-owner-lifecycle-v1"
        ),
    )
    assert result["producer_closure_status"] == "MATCHED_FRESH_MOCK_SOURCE"
    assert result["closed_mock_orders"] == 2


def test_multi_frontend_lifecycle_closes_same_source_only_with_exact_topology(tmp_path):
    inputs = bundle(tmp_path)
    status = final_status(tmp_path)
    marker = lifecycle(
        tmp_path, status, schema="consumer-multi-frontend-lifecycle-v1"
    )
    result = join_consumed(
        **inputs,
        output=tmp_path / "join",
        final_status=status,
        final_lifecycle=marker,
    )
    assert result["producer_closure_status"] == "MATCHED_FRESH_MOCK_SOURCE"


@pytest.mark.parametrize(
    ("read_roles", "read_broker"),
    [
        (("owner-read-plane",), True),
        (("owner-read-plane",), False),
        (("owner-replay-plane", "owner-snapshot-plane"), False),
        (("owner-replay-plane", "owner-snapshot-plane"), True),
    ],
)
def test_eight_frontend_lifecycle_closes_production_sized_topology(
    tmp_path, read_roles, read_broker
):
    inputs = bundle(tmp_path)
    status = final_status(tmp_path)
    marker = lifecycle(
        tmp_path,
        status,
        schema="consumer-multi-frontend-lifecycle-v1",
        frontend_count=8,
        read_roles=read_roles,
        read_broker=read_broker,
    )
    result = join_consumed(
        **inputs,
        output=tmp_path / "join",
        final_status=status,
        final_lifecycle=marker,
    )
    assert result["producer_closure_status"] == "MATCHED_FRESH_MOCK_SOURCE"


@pytest.mark.parametrize(
    "read_roles",
    [
        (),
        ("owner-replay-plane",),
        ("owner-snapshot-plane",),
        ("owner-read-plane", "owner-replay-plane", "owner-snapshot-plane"),
    ],
)
def test_lifecycle_rejects_incomplete_or_mixed_read_planes(tmp_path, read_roles):
    inputs = bundle(tmp_path)
    status = final_status(tmp_path)
    marker = lifecycle(
        tmp_path, status, schema="consumer-multi-frontend-lifecycle-v1",
        frontend_count=8, read_roles=read_roles, read_broker=False,
    )
    with pytest.raises(ValueError, match="read-plane role closure"):
        join_consumed(
            **inputs, output=tmp_path / "rejected", final_status=status,
            final_lifecycle=marker,
        )


def test_multi_frontend_lifecycle_rejects_missing_role_even_with_rehashed_topology(tmp_path):
    inputs = bundle(tmp_path)
    status = final_status(tmp_path)
    marker = lifecycle(
        tmp_path, status, schema="consumer-multi-frontend-lifecycle-v1"
    )
    body = json.loads(marker.read_text())
    body["lifecycle_components"].pop()
    body["lifecycle_components_sha256"] = sha256(
        json.dumps(
            body["lifecycle_components"], separators=(",", ":"), sort_keys=True
        ).encode()
    ).hexdigest()
    atomic_runtime_json(marker, body)
    with pytest.raises(ValueError, match="topology|role closure"):
        join_consumed(
            **inputs,
            output=tmp_path / "rejected",
            final_status=status,
            final_lifecycle=marker,
        )


@pytest.mark.parametrize(
    "mutation", ["legacy", "shared", "forced", "timeout", "wrong_clock", "pid_reused"]
)
def test_managed_only_or_failed_helper_cannot_upgrade_to_full_runtime_closure(tmp_path, mutation):
    inputs = bundle(tmp_path)
    path = final_status(tmp_path)
    marker = lifecycle(tmp_path, path)
    body = json.loads(marker.read_text())
    if mutation == "legacy":
        body["schema"] = "consumer-candidate-final-lifecycle-v1"
        body["all_children_exited_zero"] = True
    elif mutation == "shared":
        body["owned_helper_exit_status"] = "NOT_VERIFIED"
    elif mutation == "forced":
        body["helper_exit"]["forced_kill"] = True
    elif mutation == "timeout":
        body["helper_exit"]["exit_code"] = None
    elif mutation == "wrong_clock":
        body["helper_exit"]["clock_identity_sha256"] = "f" * 64
    else:
        body["helper_exit"]["start_ticks"] = 0
    atomic_runtime_json(marker, body)
    with pytest.raises(ValueError, match="lifecycle/hash|helper exit"):
        join_consumed(
            **inputs, output=tmp_path / "rejected", final_status=path, final_lifecycle=marker
        )


@pytest.mark.parametrize(
    "section,key,value",
    (
        ("mock", "native_calls", 1),
        ("mock", "pending_archives", 1),
        ("projector", "projected_lsn", 6),
        ("journal", "maintenance_failed", True),
        ("source_commit_timing", "failed", True),
    ),
)
def test_bad_final_mock_status_cannot_upgrade_consumed_only_to_producer_closure(
    tmp_path, section, key, value
):
    inputs = bundle(tmp_path)
    path = final_status(tmp_path)
    status = json.loads(path.read_text())
    status[section][key] = value
    atomic_runtime_json(path, status)
    with pytest.raises(ValueError):
        join_consumed(
            **inputs,
            output=tmp_path / "join",
            final_status=path,
            final_lifecycle=lifecycle(tmp_path, path),
        )
    assert not (tmp_path / "join/report.json").exists()


def test_pre_runtime_status_alone_or_late_status_mutation_cannot_prove_shutdown(tmp_path):
    inputs = bundle(tmp_path)
    path = final_status(tmp_path)
    with pytest.raises(ValueError, match="both required"):
        join_consumed(**inputs, output=tmp_path / "missing", final_status=path)
    marker = lifecycle(tmp_path, path)
    status = json.loads(path.read_text())
    status["written"] -= 1
    atomic_runtime_json(path, status)
    with pytest.raises(ValueError, match="lifecycle/hash"):
        join_consumed(
            **inputs, output=tmp_path / "changed", final_status=path, final_lifecycle=marker
        )


def test_large_run_metadata_cannot_silently_enter_the_short_join(tmp_path):
    inputs = bundle(tmp_path)
    path = inputs["source_root"] / "manifest.json"
    value = json.loads(path.read_text())
    value["written"] = 1_000_001
    path.write_text(json.dumps(value))
    with patch(
        "tools.performance.join_consumer_source_timing.verify_commit_timing",
        side_effect=AssertionError,
    ):
        with pytest.raises(ValueError, match="capacity budget"):
            join_consumed(**inputs, output=tmp_path / "join")
    assert not (tmp_path / "join").exists()


@pytest.mark.parametrize("fault", ("clock", "journal", "progress", "source"))
def test_unknown_clock_identity_or_missing_raw_pair_cannot_be_interpolated(tmp_path, fault):
    inputs = bundle(tmp_path)
    if fault == "clock":
        path = inputs["progress_root"] / "report.json"
        value = json.loads(path.read_text())
        value["clock_identity_sha256"] = None
        path.write_text(json.dumps(value))
    elif fault == "journal":
        with sqlite3.connect(inputs["checkpoint_root"] / "consumer-progress.sqlite") as db:
            db.execute("UPDATE meta SET journal='other-stream'")
    elif fault == "progress":
        path = inputs["progress_root"] / "progress.jsonl"
        with path.open("r+b") as file:
            file.seek(0)
            file.write(b"X")
    else:
        path = next(inputs["source_root"].glob("*.bin"))
        with path.open("r+b") as file:
            file.seek(0)
            file.write(b"X")
    with pytest.raises(ValueError):
        join_consumed(**inputs, output=tmp_path / "join")
    assert not (tmp_path / "join/report.json").exists()
