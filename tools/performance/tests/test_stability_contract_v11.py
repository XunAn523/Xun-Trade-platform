from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools.performance.run_stability_contract_v11 import StabilityContractError, load_plan


def _plan() -> dict[str, object]:
    return {
        "schema": "StabilityPlanV11",
        "profile_id": "test-v11",
        "workloads": [
            {"name": "OMS-W3", "command": ["echo", "oms"]},
            {"name": "EMS-E5", "command": ["echo", "ems"]},
            {"name": "STR-S5", "command": ["echo", "strategy"]},
        ],
        "faults": [{"name": "pause", "command": ["echo", "fault"]}],
    }


def test_stability_plan_requires_all_three_mixed_contracts_and_l2_faults(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(_plan()), encoding="utf-8")
    assert load_plan(path, level="L1")["profile_id"] == "test-v11"
    incomplete = _plan()
    incomplete["workloads"] = incomplete["workloads"][:2]
    path.write_text(json.dumps(incomplete), encoding="utf-8")
    with pytest.raises(StabilityContractError, match="OMS-W3"):
        load_plan(path, level="L1")
    no_fault = _plan()
    no_fault["faults"] = []
    path.write_text(json.dumps(no_fault), encoding="utf-8")
    with pytest.raises(StabilityContractError, match="fault"):
        load_plan(path, level="L2")
