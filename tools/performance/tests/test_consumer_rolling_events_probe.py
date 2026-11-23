import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_consumer_event_checkpoint import page, plan
from tools.performance.consumer_uds_rolling_events_probe import ObservedClient, run_probe
from xuanling_tradecore.persistence.rotating_request_trace import verify_closed_request_trace


def config():
    return SimpleNamespace(
        consumer_id="private-consumer",
        consumer_account_id="private-account",
        venue_id="NO_ADAPTER_BENCHMARK",
        adapter_instance_id="disabled-no-dispatch",
    )


def test_short_diagnostic_only_uses_public_calls_and_never_logs_tokens_or_event_payload(tmp_path):
    client = Mock()
    responses = [plan(), page((2, 4, 6, 8), more=False, token="private-exhausted-8")]

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
        return dict(
            message_type="response",
            schema_version=2,
            status="COMPLETED",
            request_id=request["request_id"],
            payload=payload,
        )

    client.call.side_effect = reply
    output = tmp_path / "result"
    report = run_probe(
        config=config(),
        output=output,
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        client=client,
    )
    assert not report["failed"] and report["formal_qualification"] is False
    assert report["new_unique_checkpointed_events"] == 4
    assert report["unique_checkpointed_in_window"] == 4
    assert verify_closed_request_trace(output / "progress.manifest.json") == report["requests"]
    text = b"".join(p.read_bytes() for p in output.glob("*.json*"))
    assert b"private-" not in text and b"not persisted" not in text
    requests = [call.args[0] for call in client.call.call_args_list]
    assert requests[2]["payload"]["after_cursor"] == "private-exhausted-8"


def test_four_partition_probe_checkpoints_unique_events_with_bounded_public_calls(tmp_path):
    client = Mock()
    issued = False

    def reply(request):
        nonlocal issued
        if request["operation"] == "CreateReplayPartitions":
            if issued:
                payload = dict(
                    journal_id="test-journal",
                    stream_generation=1,
                    event_count=0,
                    high_watermark_position=None,
                    partitions=[],
                )
            else:
                issued = True
                payload = plan()
                payload["partitions"] = [
                    {
                        "partition_id": index,
                        "after_position": None if index == 0 else f"ce2_{index * 2:027d}",
                        "high_watermark_position": f"ce2_{(index + 1) * 2:027d}",
                        "event_count": 1,
                        "cursor": f"private-part-{index}",
                    }
                    for index in range(4)
                ]
                # Use the canonical position helper from the checkpoint tests.
                for index, part in enumerate(payload["partitions"]):
                    part["after_position"] = None if index == 0 else plan(end=index * 2)[
                        "high_watermark_position"
                    ]
                    part["high_watermark_position"] = plan(end=(index + 1) * 2)[
                        "high_watermark_position"
                    ]
            return dict(
                message_type="response",
                schema_version=2,
                status="COMPLETED",
                request_id=request["request_id"],
                payload=payload,
            )
        index = int(request["payload"]["cursor"].rsplit("-", 1)[1])
        payload = page(
            ((index + 1) * 2,),
            end=(index + 1) * 2,
            more=False,
            token=f"private-high-{(index + 1) * 2}",
        )
        return dict(
            message_type="response",
            schema_version=2,
            status="COMPLETED",
            request_id=request["request_id"],
            payload=payload,
        )

    client.call.side_effect = reply
    report = run_probe(
        config=config(),
        output=tmp_path / "result",
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        client=client,
        partition_concurrency=4,
    )
    assert not report["failed"]
    assert report["partition_concurrency"] == 4
    assert report["new_unique_checkpointed_events"] == 4
    assert report["private_checkpoint"]["pending_partitions"] == 0
    poll_calls = [
        call.args[0] for call in client.call.call_args_list if call.args[0]["operation"] == "PollEvents"
    ]
    assert sorted(item["payload"]["cursor"] for item in poll_calls) == [
        f"private-part-{index}" for index in range(4)
    ]


def test_rpc_failure_is_preserved_as_failed_evidence_not_successful_checkpoint(tmp_path):
    client = Mock()
    client.call.side_effect = TimeoutError("do not export private-password or private-token")
    output = tmp_path / "result"
    report = run_probe(
        config=config(),
        output=output,
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        client=client,
    )
    assert report["failed"] and report["new_unique_checkpointed_events"] == 0
    row = json.loads((output / "progress.jsonl").read_text().splitlines()[0])
    assert row["result"] == "FAILED" and row["progress_confirmed_ns"] == 0
    assert row["response_returned_ns"] == 0 and row["failure_observed_ns"] > 0
    assert "private-" not in json.dumps(row)


def test_observer_does_not_invent_socket_or_persistence_boundaries():
    client = Mock()
    client.call.return_value = {}
    observed = ObservedClient(client)
    observed.call(
        {"operation": "PollEvents", "request_id": "unique", "payload": {"cursor": "private-token"}}
    )
    assert observed.last["response_returned_ns"] >= observed.last["call_started_ns"]
    assert observed.last["progress_confirmed_ns"] == 0
    assert "connect_ns" not in observed.last and "sent_ns" not in observed.last


def test_common_start_prevents_rpc_before_frozen_gate(tmp_path, monkeypatch):
    from tools.performance import consumer_uds_rolling_events_probe as module

    clock = [1_000_000_000]
    monkeypatch.setattr(module.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(
        module.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + max(1, int(seconds * 1e9))),
    )
    gate = clock[0] + 20_000_000
    client = Mock()

    def invoke(_):
        assert clock[0] >= gate
        raise TimeoutError

    client.call.side_effect = invoke
    report = run_probe(
        config=config(),
        output=tmp_path / "result",
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        start_ns=gate,
        client=client,
    )
    assert report["start_ns"] == gate and report["end_ns"] == gate + 1_000_000_000
    assert report["failed"]


@pytest.mark.parametrize("start", [True, 0, -1])
def test_bad_gate_does_not_initialize_evidence(tmp_path, start):
    with pytest.raises(ValueError):
        run_probe(
            config=config(),
            output=tmp_path / "result",
            checkpoint_root=tmp_path / "checkpoint",
            initialize=True,
            duration=1,
            start_ns=start,
        )
    assert not (tmp_path / "result").exists()


@pytest.mark.parametrize(
    "code,expected",
    (
        ("CURSOR_RECOVERY_UNAVAILABLE", "CURSOR_RECOVERY_UNAVAILABLE"),
        ("private-password", "UNRECOGNIZED_CODE"),
        ({"private": "password"}, "UNRECOGNIZED_CODE"),
        (["private-password"], "UNRECOGNIZED_CODE"),
    ),
)
def test_observer_only_exports_whitelisted_public_error_codes(code, expected):
    client = Mock()
    client.call.return_value = {"message_type": "error", "code": code}
    observed = ObservedClient(client)
    observed.call({"operation": "CreateReplayPartitions", "request_id": "unique"})
    assert observed.last["server_error_code"] == expected
    assert "private-" not in json.dumps(observed.last)


@pytest.mark.parametrize("duration", (0, 61, True))
def test_short_scope_cannot_silently_start_long_qualification(tmp_path, duration):
    with pytest.raises(ValueError):
        run_probe(
            config=config(),
            output=tmp_path / "result",
            checkpoint_root=tmp_path / "checkpoint",
            initialize=True,
            duration=duration,
        )
    assert not (tmp_path / "result").exists()


def test_final_drain_uses_only_signed_frontier_and_never_backfills_window_metrics(
    tmp_path, monkeypatch
):
    from tools.performance import consumer_uds_rolling_events_probe as module

    after_window = False
    original = module._drain_target_reached

    def check(checkpoint, target):
        nonlocal after_window
        after_window = True
        return original(checkpoint, target)

    monkeypatch.setattr(module, "_drain_target_reached", check)
    client = Mock()
    offered_plan = False

    def reply(request):
        nonlocal offered_plan
        if request["operation"] == "PollEvents":
            payload = page((2, 4, 6, 8), more=False, token="private-exhausted-8")
        elif after_window and not offered_plan:
            offered_plan = True
            payload = plan()
        else:
            payload = dict(
                journal_id="test-journal",
                stream_generation=1,
                event_count=0,
                high_watermark_position=None,
                partitions=[],
            )
        return dict(
            message_type="response",
            schema_version=2,
            status="COMPLETED",
            request_id=request["request_id"],
            payload=payload,
        )

    client.call.side_effect = reply
    output = tmp_path / "result"
    report = run_probe(
        config=config(),
        output=output,
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        client=client,
        drain_through_lsn=8,
        drain_timeout_seconds=1,
    )
    assert not report["failed"] and not report["formal_qualification"]
    assert report["final_drain"]["reached"] is True
    assert report["final_drain"]["target_is_authority"] is False
    assert report["final_drain"]["unique_after_window"] == 4
    assert report["unique_checkpointed_in_window"] == 0
    assert report["new_unique_checkpointed_events"] == 4
    assert "PollEvents.call" not in report["histograms"]
    assert report["histograms"]["FINAL_DRAIN.PollEvents.call"]["count"] == 1
    assert report["requests_after_window"] == 2
    assert (
        report["requests_started_in_window"] + report["requests_after_window"] == report["requests"]
    )
    assert report["final_drain"]["finished_ns"] >= report["end_ns"]
    assert verify_closed_request_trace(output / "progress.manifest.json") == report["requests"]
    text = b"".join(p.read_bytes() for p in output.glob("*.json*"))
    assert b"private-" not in text and b"not persisted" not in text
    for call in client.call.call_args_list:
        assert not {"start_lsn", "end_lsn", "drain_through_lsn"} & call.args[0]["payload"].keys()


def test_final_drain_timeout_cannot_pass_by_just_naming_a_high_lsn(tmp_path):
    client = Mock()
    client.call.side_effect = lambda r: dict(
        message_type="response",
        schema_version=2,
        status="COMPLETED",
        request_id=r["request_id"],
        payload=dict(
            journal_id="test-journal",
            stream_generation=1,
            event_count=0,
            high_watermark_position=None,
            partitions=[],
        ),
    )
    report = run_probe(
        config=config(),
        output=tmp_path / "result",
        checkpoint_root=tmp_path / "checkpoint",
        initialize=True,
        duration=1,
        client=client,
        drain_through_lsn=8,
        drain_timeout_seconds=1,
    )
    assert report["failed"] and report["error_code"] == "FINAL_EVENT_DRAIN_TIMEOUT"
    assert report["final_drain"]["reached"] is False
    assert report["unique_checkpointed_in_window"] == report["new_unique_checkpointed_events"] == 0


@pytest.mark.parametrize(
    "target,timeout", [(True, 1), (0, 1), (-1, 1), (2**63, 1), (8, 0), (8, 61), (8, True)]
)
def test_invalid_final_drain_does_not_create_output_or_checkpoint(tmp_path, target, timeout):
    with pytest.raises(ValueError):
        run_probe(
            config=config(),
            output=tmp_path / "result",
            checkpoint_root=tmp_path / "checkpoint",
            initialize=True,
            duration=1,
            drain_through_lsn=target,
            drain_timeout_seconds=timeout,
        )
    assert not (tmp_path / "result").exists()
    assert not (tmp_path / "checkpoint").exists()
