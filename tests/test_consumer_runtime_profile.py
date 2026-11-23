from dataclasses import replace
from pathlib import Path

import pytest
from xuanling_tradecore.consumer.runtime_profile import (
    REQUIRED_LONG_RUNNING_CAPABILITIES,
    ConsumerLongRunningPolicy,
    ConsumerRuntimePolicyError,
    fact_capacity_budget,
    require_long_running_capabilities,
)


def test_budget_counts_facts_instead_of_orders() -> None:
    budget = fact_capacity_budget(submit_tps=2000, facts_per_submit=2, duration_seconds=7200)
    assert budget == {
        "fact_rate": 4000,
        "required_frames": 28_800_000,
        "raw_bytes_per_second": 16_384_000,
        "raw_bytes": 117_964_800_000,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"segment_frames": True},
        {"segment_frames": 3},
        {"spare_segments": 1},
        {"memory_guard_bytes": 6 * 1024**3},
        {"journal_disk_budget_bytes": 1},
        {"trace_disk_budget_bytes": 1},
        {"maximum_active_orders": 0},
    ],
)
def test_invalid_or_unbounded_policy_fails_closed(change: dict[str, int]) -> None:
    with pytest.raises(ConsumerRuntimePolicyError):
        replace(ConsumerLongRunningPolicy(), **change)


def test_unknown_or_float_policy_is_rejected() -> None:
    for raw in ({"maximum_active_orders": 1.0}, {"ignore_memory_limit": 1}):
        with pytest.raises(ConsumerRuntimePolicyError):
            ConsumerLongRunningPolicy.from_mapping(raw)
    assert ConsumerLongRunningPolicy.from_mapping({}) == ConsumerLongRunningPolicy()


def test_capability_gate_requires_all_lifecycle_components() -> None:
    for missing in REQUIRED_LONG_RUNNING_CAPABILITIES:
        with pytest.raises(ConsumerRuntimePolicyError, match=missing):
            require_long_running_capabilities(REQUIRED_LONG_RUNNING_CAPABILITIES - {missing})
    require_long_running_capabilities(REQUIRED_LONG_RUNNING_CAPABILITIES)


def test_long_running_bootstrap_rejects_before_opening_any_journal(tmp_path: Path) -> None:
    from xuanling_tradecore.consumer.runtime_profile import ConsumerRuntimeProfile
    from xuanling_tradecore.consumer.uds_server_v2 import (
        ConsumerUdsServerConfigV2,
        build_consumer_uds_service_v2,
    )

    config = ConsumerUdsServerConfigV2(
        socket_path=tmp_path / "consumer.sock",
        journal_root=tmp_path / "missing-journal",
        journal_capacity=16,
        peer_uid=1000,
        peer_gid=1000,
        consumer_id="consumer",
        login_name="login",
        legal_name="name",
        consumer_account_id="account",
        account_id="account",
        account_display_name="test",
        venue_id="NO_ADAPTER_BENCHMARK",
        adapter_instance_id="disabled-no-dispatch",
        strategy_id="strategy",
        runtime_profile=ConsumerRuntimeProfile.LONG_RUNNING,
        long_running_policy=ConsumerLongRunningPolicy(),
    )
    with pytest.raises(ConsumerRuntimePolicyError, match="implementation incomplete"):
        build_consumer_uds_service_v2(config=config, contract_root=tmp_path / "missing-contract")
    assert not config.journal_root.exists()
    assert not config.socket_path.exists()
