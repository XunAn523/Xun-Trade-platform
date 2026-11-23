import os
import socket
import time
from pathlib import Path
from threading import Thread

import pytest
from perfbench.consumer_event_checkpoint import ConsumerEventCheckpoint, RollingConsumerEventProbe
from tools.performance.consumer_uds_rolling_events_probe import run_probe
from tools.performance.join_consumer_source_timing import join_consumed
from xuanling_tradecore.consumer import ConsumerUnixClientConfigV2, ConsumerUnixClientV2
from xuanling_tradecore.consumer.long_running_candidate import LongRunningConsumerCandidate
from xuanling_tradecore.consumer.runtime_health import RuntimeResourceSample
from xuanling_tradecore.consumer.runtime_profile import (
    ConsumerLongRunningPolicy,
    ConsumerRuntimeProfile,
)
from xuanling_tradecore.consumer.uds_server_v2 import ConsumerUdsServerConfigV2
from xuanling_tradecore.persistence.commit_timing import verify_commit_timing


@pytest.fixture
def read_owner_options(request):
    if not request.param:
        yield {}
        return
    allowed = os.sched_getaffinity(0)
    physical = {}
    for cpu in sorted(allowed):
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        physical.setdefault(
            ((root / "physical_package_id").read_text(), (root / "core_id").read_text()), cpu
        )
    if len(physical) < 4:
        pytest.skip("process read integration requires four already-allowed physical cores")
    cpus = tuple(physical.values())[:4]
    try:
        yield {"role_cpus": cpus[:3], "projection_read_cpus": cpus[3:]}
    finally:
        os.sched_setaffinity(0, allowed)


@pytest.mark.skipif(not hasattr(socket, "SO_PEERCRED"), reason="Linux UDS peer credentials")
@pytest.mark.parametrize("read_owner_options", (False, True), indirect=True)
def test_new_production_is_checkpointed_through_real_signed_suffix_after_resume(
    tmp_path, read_owner_options
):
    root = tmp_path / "server"
    root.mkdir(mode=0o700)
    config = ConsumerUdsServerConfigV2(
        socket_path=root / "s",
        journal_root=root,
        journal_capacity=128,
        peer_uid=os.geteuid(),
        peer_gid=os.getegid(),
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
            replay_cache_events=32,
            maximum_active_orders=1000,
        ),
    )
    candidate = LongRunningConsumerCandidate(
        config=config,
        contract_root=Path("contracts/consumer/v2"),
        initialize=True,
        sample_resources=lambda: RuntimeResourceSample(
            time.monotonic_ns(), 1024**2, 6 * 1024**3, 10**12, 0, 0, 0, True
        ),
        **read_owner_options,
    )
    ledger = ConsumerEventCheckpoint(
        root=tmp_path / "checkpoint", scope_sha256="a" * 64, initialize=True
    )
    runner = Thread(target=candidate.serve_forever)
    client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(config.socket_path, timeout_seconds=3))
    try:
        candidate.start()
        runner.start()

        def submit(start, end):
            for n in range(start, end):
                response = client.call(
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "request_id": f"producer-{n}",
                        "operation": "SubmitSignal",
                        "payload": {
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
                        },
                    }
                )
                assert response["status"] == "ACCEPTED"

        def wait_events(count):
            for attempt in range(200):
                response = client.call(
                    {
                        "message_type": "request",
                        "schema_version": 2,
                        "request_id": f"fence-{count}-{attempt}",
                        "operation": "CreateReplayPartitions",
                        "payload": {"consumer_account_id": "test", "partition_count": 4},
                    }
                )
                if (
                    response.get("status") == "COMPLETED"
                    and response["payload"]["event_count"] == count
                ):
                    return
                time.sleep(0.01)
            raise AssertionError("public production fence did not catch up")

        submit(0, 6)
        wait_events(12)  # Accepted + real mock terminal facts, not fixed seed replay.
        probe = RollingConsumerEventProbe(
            client=client, checkpoint=ledger, consumer_account_id="test"
        )
        assert probe.step() is None  # First frozen interval.
        assert probe.step().unique_events > 0
        ledger.close()  # Resume a partially consumed plan, without new BEGIN.
        ledger = ConsumerEventCheckpoint(
            root=tmp_path / "checkpoint", scope_sha256="a" * 64, initialize=False
        )
        probe = RollingConsumerEventProbe(
            client=client, checkpoint=ledger, consumer_account_id="test"
        )
        for _ in range(10):
            if not ledger.pending():
                break
            probe.step()
        assert ledger.stats()["unique_checkpointed_events"] == 12
        token, frontier = ledger.frontier()
        assert token and frontier
        # Publish more only AFTER the first signed frozen high watermark.
        submit(6, 11)
        wait_events(22)
        assert probe.step() is None  # Server issues a signed suffix, not BEGIN.
        for _ in range(10):
            if not ledger.pending():
                break
            probe.step()
        assert ledger.stats()["unique_checkpointed_events"] == 22
        assert ledger.stats()["pending_partitions"] == 0
        assert ledger.frontier()[1] != frontier
        assert probe.step() is None  # Empty suffix must not erase the frontier.
        assert ledger.stats()["unique_checkpointed_events"] == 22
        assert ledger.frontier()[0]
        report = run_probe(
            config=config,
            output=tmp_path / "public-observer",
            checkpoint_root=tmp_path / "observer-db",
            initialize=True,
            duration=1,
        )
        assert not report["failed"] and report["new_unique_checkpointed_events"] == 22
        assert report["performance_status"] == "NOT_EVALUATED"
        if candidate.projection_reads is not None:
            assert candidate.projection_reads.stats()["calls"] > 0
            assert not candidate.projection_reads.stats()["failed"]
    finally:
        ledger.close()
        candidate.service.request_stop()
        if runner.is_alive():
            runner.join(timeout=3)
        candidate.close()
    assert not runner.is_alive()
    (timing_root,) = (config.journal_root / "traces").glob("source-commits-*")
    assert verify_commit_timing(timing_root) == (22, 44)
    assert candidate.commit_timing.stats()["written"] == 22
    # Embedded pytest owns/shares its tracker; candidate.close must not stop it
    # or issue the CLI's stronger all-owned-OS-descendants certificate.
    assert not list((config.journal_root / "traces").glob("*.lifecycle.json"))
    assert list((config.journal_root / "traces").glob("*.managed-exit.json"))
    joined = join_consumed(
        source_root=timing_root,
        checkpoint_root=tmp_path / "observer-db",
        progress_root=tmp_path / "public-observer",
        output=tmp_path / "clock-join",
    )
    assert joined["matched_events"] == 22
    assert not joined["formal_qualification"]
    assert joined["producer_closure_status"] == "NOT_EVALUATED"
    assert joined["closed_mock_orders"] is None
