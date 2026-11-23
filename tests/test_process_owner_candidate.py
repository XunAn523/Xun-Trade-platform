from __future__ import annotations

import pytest
from xuanling_tradecore.consumer.long_running_candidate import process_owner_trace_budgets
from xuanling_tradecore.consumer.process_owner_candidate import _cpus

from test_long_running_consumer_candidate import config


def test_process_owner_candidate_parses_exact_cpu_roles() -> None:
    assert _cpus("64,65,66,67", count=4) == (64, 65, 66, 67)


@pytest.mark.parametrize(
    ("value", "count"),
    (("64,64", None), ("64,65", 4), ("64,-1", None), ("64, 65", None), ("", None)),
)
def test_process_owner_candidate_rejects_ambiguous_cpu_roles(
    value: str, count: int | None
) -> None:
    with pytest.raises(ValueError, match="CPU IDs"):
        _cpus(value, count=count)


@pytest.mark.parametrize("diagnostics", (False, True))
def test_process_owner_trace_budget_moves_private_request_capacity_to_frontend(
    tmp_path, diagnostics
):
    candidate = config(tmp_path)
    policy = candidate.long_running_policy
    assert policy is not None

    source, stage, ipc, private, frontend = process_owner_trace_budgets(
        candidate, projection_commit_diagnostics=diagnostics
    )

    assert source + stage + ipc + private + frontend == policy.trace_disk_budget_bytes
    assert source == policy.trace_disk_budget_bytes // 4
    assert ipc == policy.trace_disk_budget_bytes // 8
    assert stage == (policy.trace_disk_budget_bytes // 8 if diagnostics else 0)
    assert private == policy.trace_segment_bytes
    assert frontend > private
