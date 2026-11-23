from __future__ import annotations

import gc
import json
import os
import socket
import time
from dataclasses import replace
from pathlib import Path
from threading import Thread

import pytest
from xuanling_tradecore.consumer import (
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
    ConsumerUnixSocketError,
)
from xuanling_tradecore.consumer.long_running_candidate import LongRunningConsumerCandidate
from xuanling_tradecore.consumer.runtime_health import RuntimeResourceSample
from xuanling_tradecore.consumer.runtime_profile import (
    ConsumerLongRunningPolicy,
    ConsumerRuntimeProfile,
)
from xuanling_tradecore.consumer.uds_server_v2 import (
    ConsumerUdsServerConfigV2,
    build_consumer_uds_service_v2,
)


def config(tmp_path):
    root = tmp_path / "r"
    root.mkdir(mode=0o700)
    return ConsumerUdsServerConfigV2(
        socket_path=root / "s",
        journal_root=root,
        journal_capacity=1024,
        peer_uid=os.geteuid() if hasattr(os, "geteuid") else 0,
        peer_gid=os.getegid() if hasattr(os, "getegid") else 0,
        consumer_id="test",
        login_name="test",
        legal_name="test",
        consumer_account_id="test",
        account_id="test",
        account_display_name="test",
        venue_id="NO_ADAPTER_BENCHMARK",
        adapter_instance_id="disabled-no-dispatch",
        strategy_id="test",
        runtime_profile=ConsumerRuntimeProfile.LONG_RUNNING,
        long_running_policy=ConsumerLongRunningPolicy(
            segment_frames=128,
            cold_filter_bytes=1024,
            maximum_active_orders=1000,
            replay_cache_events=32,
        ),
    )


def sample():
    return RuntimeResourceSample(
        time.monotonic_ns(), 1024**2, 6 * 1024**3, 10**12, 0, 0, 0, gc.isenabled()
    )


def build(c, initialize=True, **kw):
    return LongRunningConsumerCandidate(
        config=c,
        contract_root=Path("contracts/consumer/v2"),
        initialize=initialize,
        sample_resources=sample,
        **kw,
    )


def call(client, operation, payload, seq=0):
    return client.call(
        {
            "message_type": "request",
            "schema_version": 2,
            "operation": operation,
            "request_id": f"r-{operation}-{seq}",
            "payload": payload,
        }
    )


def signal(n):
    return {
        "consumer_account_id": "test",
        "strategy_id": "test-H",
        "strategy_order_id": f"LR-{n}",
        "trading_day": "2026-10-20",
        "symbol": "TEST",
        "market": "SIM",
        "side": "BUY",
        "price_type": "LIMIT",
        "business_type": "CASH",
        "quantity": 1,
        "protection_price_ticks": 1,
    }


@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="Linux-only persistent service")
def test_candidate_is_explicit_and_does_not_open_the_release_gate(tmp_path):
    c = config(tmp_path)
    with pytest.raises(ValueError, match="incomplete"):
        build_consumer_uds_service_v2(config=c, contract_root=Path("contracts/consumer/v2"))
    with pytest.raises(ConsumerUnixSocketError):
        build(replace(c, adapter_instance_id="live-adapter"))
    candidate = build(c)
    try:
        assert len(candidate.strategies) == 22
        assert candidate.runtime.recovered_watermark == 0
        assert not c.socket_path.exists()
    finally:
        candidate.close()
    reopened = build(c, False)
    reopened.close()


def test_candidate_rejects_non_linux_before_creating_persistent_data(tmp_path):
    from unittest.mock import patch

    c = config(tmp_path)
    with patch("xuanling_tradecore.consumer.long_running_candidate.sys.platform", "win32"):
        with pytest.raises(ConsumerUnixSocketError):
            build(c)
    assert list(c.journal_root.iterdir()) == []


@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="owned Linux candidate")
def test_dead_projector_cannot_publish_successful_final_runtime_marker(tmp_path):
    candidate = build(config(tmp_path))
    try:
        candidate.start()
        candidate.projector._process.terminate()
        candidate.projector._process.join(3)
        with pytest.raises(ConsumerUnixSocketError):
            candidate.close()
        assert not list(candidate.trace.path.parent.glob("*.lifecycle.json"))
    finally:
        candidate.close()


@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="Linux TID affinity proof")
@pytest.mark.parametrize("isolated", (False, True))
def test_background_layout_pins_actual_threads_and_restores_embedded_caller(tmp_path, isolated):
    original = os.sched_getaffinity(0)
    physical = {}
    for cpu in sorted(original):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = ((topology / "physical_package_id").read_text(), (topology / "core_id").read_text())
        physical.setdefault(key, cpu)
    assert len(physical) >= 8, "Linux affinity fixture needs eight granted physical cores"
    cpus = tuple(physical.values())[:8]
    c = config(tmp_path)
    candidate = build(
        c,
        role_cpus=cpus[:4],
        projection_read_cpus=cpus[4:6],
        background_cpus=cpus[6:],
        isolate_submit_execution=isolated,
    )
    worker = None
    try:
        assert os.sched_getaffinity(0) == original
        candidate.start()
        assert os.sched_getaffinity(0) == original
        worker = Thread(target=candidate.serve_forever)
        worker.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
        assert call(client, "ListAccounts", {}, "affinity")["status"] == "COMPLETED"
        assert os.sched_getaffinity(worker.native_id) == {cpus[0]}
        readers = candidate.service._deferred_reads
        assert readers is not None
        threads = (
            candidate.trace._thread,
            candidate.mock._thread,
            candidate.sampler._thread,
            candidate.projector._broker,
            candidate.runtime.cold_index._thread,
            candidate.runtime.journal._worker,
            *readers._threads,
        )
        for thread in threads:
            assert thread is not None and thread.native_id is not None
            assert os.sched_getaffinity(thread.native_id) == set(cpus[6:]), thread.name
        if isolated:
            owner = candidate.service._submit_lane._thread
            assert os.sched_getaffinity(owner.native_id) == set(cpus[6:])
        assert os.sched_getaffinity(candidate.projector._process.pid) == {cpus[1]}
        assert os.sched_getaffinity(candidate.runtime._validator._process.pid) == {cpus[2]}
        assert os.sched_getaffinity(candidate.runtime._sealer._process.pid) == {cpus[3]}
        for owner, cpu in zip(candidate.projection_reads._owners, cpus[4:6]):
            assert os.sched_getaffinity(owner.process.pid) == {cpu}
    finally:
        candidate.close()
        if worker:
            worker.join(5)
    assert os.sched_getaffinity(0) == original


@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="owned Linux candidate")
def test_paused_final_projector_closes_resources_but_never_publishes_lifecycle_success(tmp_path):
    candidate = build(config(tmp_path))
    try:
        candidate.start()
        candidate.projector.pause()
        deadline = time.monotonic() + 3
        while candidate.projector.health().state != "PAUSED" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert candidate.projector.health().state == "PAUSED"
        with pytest.raises(ConsumerUnixSocketError, match="exit"):
            candidate.close()
        assert candidate.runtime.shutdown_verified
        assert candidate.projector.shutdown_verified
        assert not list(candidate.trace.path.parent.glob("*.lifecycle.json"))
    finally:
        candidate.close()


@pytest.mark.skipif(
    not hasattr(socket, "SO_PEERCRED"), reason="real peer credentials require Linux"
)
@pytest.mark.parametrize("committed_window", [False, True])
@pytest.mark.parametrize("stage_diagnostics", [False, True])
@pytest.mark.parametrize("isolated", [False, True])
def test_real_uds_persistent_mock_cold_queries_snapshots_and_restart(
    tmp_path, committed_window, stage_diagnostics, isolated
):
    c = config(tmp_path)
    candidate = build(
        c,
        committed_replay_window=committed_window,
        stage_diagnostics=stage_diagnostics,
        fixed_reactor_budgets=stage_diagnostics,
        isolate_submit_execution=isolated,
    )
    worker = None
    ids = []
    try:
        candidate.start()
        worker = Thread(target=candidate.serve_forever)
        worker.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
        for n in range(10):
            result = call(client, "SubmitSignal", signal(n), n)
            assert result["status"] == "ACCEPTED", result
            ids.append(result["payload"]["oms_order_id"])
        end = time.monotonic() + 5
        while time.monotonic() < end:
            assert candidate.mock.health().state == "RUNNING", candidate.mock.health()
            if (
                candidate.mock.health().retired == 10
                and candidate.projector.health().projected_lsn
                == candidate.runtime.journal.read_bound().committed_lsn
            ):
                break
            time.sleep(0.01)
        assert candidate.mock.health().retired == 10
        assert (
            candidate.projector.health().projected_lsn
            == candidate.runtime.journal.read_bound().committed_lsn
        )
        assert candidate.runtime.order_book.reservations("test") == ()
        for n, order_id in enumerate(ids):
            got = call(client, "GetOrder", {"oms_order_id": order_id}, n)
            assert got.get("status") == "COMPLETED", got
            state = got["payload"]["order"]["payload"]["order_state"]
            assert state == ("REJECTED" if n % 5 == 4 else "FILLED")
        plan = call(
            client, "CreateOrderSnapshot", {"consumer_account_id": "test", "page_size": 100}
        )
        assert plan["status"] == "COMPLETED", plan
        assert plan["payload"]["order_count"] == 10
        page = call(
            client, "ReadOrderSnapshotPage", {"page_cursor": plan["payload"]["page_cursor"]}
        )
        assert page["status"] == "COMPLETED", page
        assert len(page["payload"]["orders"]) == 10
        # Repeated hot page is memory-only on socket owner, without the read
        # worker hop; pin expiry/close cleanup must never occur in this mode.
        from unittest.mock import patch

        before = candidate.service.ingress_stats().deferred_reads_completed
        with patch.object(
            candidate.snapshots,
            "_expire_locked",
            side_effect=AssertionError("cleanup reached inline page"),
        ):
            hot = call(
                client, "ReadOrderSnapshotPage", {"page_cursor": plan["payload"]["page_cursor"]}
            )
        assert hot["status"] == "COMPLETED", hot
        assert candidate.service.ingress_stats().deferred_reads_completed == before
        replay = call(
            client, "CreateReplayPartitions", {"consumer_account_id": "test", "partition_count": 2}
        )
        assert replay["status"] == "COMPLETED", replay
        consumed = 0
        token = None
        for part in replay["payload"]["partitions"]:
            token = part["cursor"]
            while True:
                events = call(client, "PollEvents", {"cursor": token, "limit": 100})
                assert events["status"] == "COMPLETED", events
                consumed += len(events["payload"]["events"])
                token = events["payload"]["next_cursor"]
                if not events["payload"]["has_more"]:
                    break
        assert consumed == 20 and token
        extra = call(client, "SubmitSignal", signal(10), 10)
        assert extra["status"] == "ACCEPTED", extra
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if (
                candidate.mock.health().retired == 11
                and candidate.projector.health().projected_lsn
                == candidate.runtime.journal.read_bound().committed_lsn
            ):
                break
            time.sleep(0.01)
        rolling = call(
            client,
            "CreateReplayPartitions",
            {
                "consumer_account_id": "test",
                "partition_count": 2,
                "after_cursor": token,
            },
        )
        assert rolling["status"] == "COMPLETED", rolling
        assert rolling["payload"]["event_count"] == 2
    finally:
        candidate.close()
        if worker:
            worker.join(timeout=5)
            assert not worker.is_alive()
    if stage_diagnostics:
        assert candidate.stage_trace.coverage_verified
        statuses = list((candidate.trace.path.parent / "stages").glob("*.status.json"))
        assert len(statuses) == 2
        for path in statuses:
            status = json.loads(path.read_text())
            assert status["closed"] and status["drain_complete"]
            assert status["queue_dropped"] == status["capacity_dropped"] == 0
            assert status["schema_version"] == "consumer-stage-trace-v1"
    recovered = build(
        c,
        False,
        committed_replay_window=committed_window,
        fixed_reactor_budgets=stage_diagnostics,
        isolate_submit_execution=isolated,
    )
    worker = None
    try:
        assert recovered.runtime.stats.cold_orders == 11
        assert recovered.runtime.stats.active_orders == 0
        recovered.start()
        worker = Thread(target=recovered.serve_forever)
        worker.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(c.socket_path))
        before = recovered.runtime.journal.read_bound().committed_lsn
        result = call(client, "SubmitSignal", signal(0), 99)
        assert result["status"] == "ACCEPTED", result
        assert result["payload"]["oms_order_id"] == ids[0]
        assert recovered.runtime.journal.read_bound().committed_lsn == before
        assert recovered.runtime.order_book.reservations("test") == ()
    finally:
        recovered.close()
        if worker:
            worker.join(timeout=5)
