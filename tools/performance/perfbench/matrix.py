"""Data-driven end-to-end performance matrix for the EMT gateway surface.

The official 86/69 catalog is the source of interface names.  This module
turns it into executable scenarios, including account profile, workload,
required timestamp paths, and sample-volume gates.  It deliberately contains
no account identifiers, symbols, order parameters, or credentials.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import Any

from .catalog import InterfaceSpec, all_specs


class MatrixError(ValueError):
    """Raised when a scenario or an evidence manifest is inconsistent."""


StagePath = tuple[str, str]


def _path_name(path: StagePath) -> str:
    return f"{path[0]}_to_{path[1]}"


_API_PATHS = {
    "LIFE": (("C0", "C1"), ("C1", "C3"), ("C3", "C4"), ("C0", "C4")),
    "CMD-H": (
        ("C0", "C1"),
        ("C1", "C2"),
        ("C2", "C3"),
        ("C3", "C4"),
        ("C0", "C4"),
        ("C0", "C5"),
        ("C5", "C6"),
        ("C6", "C7"),
        ("C0", "C7"),
    ),
    "CMD-A": (
        ("C0", "C1"),
        ("C1", "C2"),
        ("C2", "C3"),
        ("C3", "C4"),
        ("C0", "C4"),
        ("C0", "C5"),
        ("C5", "C6"),
        ("C6", "C7"),
        ("C0", "C7"),
    ),
    "QUERY": (
        ("C0", "C1"),
        ("C1", "C3"),
        ("C3", "C4"),
        ("C0", "C4"),
        ("C0", "C5"),
        ("C5", "C6"),
        ("C6", "C7"),
        ("C0", "C7"),
    ),
}

_SPI_PATHS: tuple[StagePath, ...] = (
    ("C0", "C5"),
    ("C5", "C6"),
    ("C6", "C7"),
    ("C0", "C7"),
)

_COMMON_ACCOUNT_APIS = frozenset(
    {
        "InsertOrder",
        "CancelOrder",
        "QueryOrderByEMTID",
        "QueryOrders",
        "QueryUnfinishedOrders",
        "QueryOrdersByPage",
        "QueryTradesByEMTID",
        "QueryTrades",
        "QueryTradesByPage",
        "QueryPosition",
        "QueryPositionByPage",
        "QueryAsset",
        "QueryFund",
        "QueryFundTransfer",
        "QueryYesterdayAsset",
    }
)
_COMMON_ACCOUNT_SPIS = frozenset(
    {
        "OnOrderEvent",
        "OnTradeEvent",
        "OnCancelOrderError",
        "OnHoldingChangeEvent",
        "OnQueryOrder",
        "OnQueryOrderByPage",
        "OnQueryTrade",
        "OnQueryTradeByPage",
        "OnQueryPosition",
        "OnQueryPositionByPage",
        "OnQueryAsset",
        "OnQueryFund",
        "OnQueryFundTransfer",
        "OnQueryYesterdayAsset",
    }
)


@dataclass(frozen=True)
class Scenario:
    """One independently reportable performance experiment."""

    kind: str
    interface: str
    tier: str
    profile: str
    workload: str
    required_paths: tuple[StagePath, ...]
    p0_minimum_successes: int
    acceptance_minimum_successes: int

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.interface}:{self.profile}:{self.workload}"

    @property
    def required_path_names(self) -> tuple[str, ...]:
        return tuple(_path_name(path) for path in self.required_paths)


def profiles_for(spec: InterfaceSpec) -> tuple[str, ...]:
    """Return the simulated-account profiles required for an interface.

    Shared account/order paths run on every applicable account type.  Credit
    and option-only paths stay isolated so their resource usage cannot hide in
    the ordinary-account baseline.
    """

    if "Credit" in spec.name:
        return ("credit",)
    if "Option" in spec.name:
        return ("option",)
    if spec.name in _COMMON_ACCOUNT_APIS or spec.name in _COMMON_ACCOUNT_SPIS:
        return ("ordinary", "credit", "option")
    return ("ordinary",)


def required_paths_for(spec: InterfaceSpec) -> tuple[StagePath, ...]:
    if spec.kind == "api":
        return _API_PATHS[spec.tier]
    return _SPI_PATHS


def sample_policy_for(spec: InterfaceSpec) -> tuple[int, int]:
    """Return P0 and release sample gates, not an order-rate instruction."""

    if spec.tier == "LIFE":
        return (10, 30)
    if spec.tier in {"CMD-H", "CB-H"}:
        return (100, 1_000)
    if spec.tier in {"CMD-A", "CB-A"}:
        return (30, 100)
    if spec.tier in {"QUERY", "CB-Q"}:
        return (100, 1_000)
    return (30, 100)


def all_scenarios() -> tuple[Scenario, ...]:
    """Expand every catalog interface into every required workload/profile."""

    scenarios: list[Scenario] = []
    for spec in all_specs():
        p0, acceptance = sample_policy_for(spec)
        paths = required_paths_for(spec)
        for profile in profiles_for(spec):
            for workload in spec.workloads:
                scenarios.append(
                    Scenario(
                        kind=spec.kind,
                        interface=spec.name,
                        tier=spec.tier,
                        profile=profile,
                        workload=workload,
                        required_paths=paths,
                        p0_minimum_successes=p0,
                        acceptance_minimum_successes=acceptance,
                    )
                )
    return tuple(scenarios)


@lru_cache(maxsize=1)
def scenario_index() -> dict[str, Scenario]:
    scenarios = all_scenarios()
    result = {scenario.key: scenario for scenario in scenarios}
    if len(result) != len(scenarios):
        raise MatrixError("duplicate scenario key in generated matrix")
    return result


def runbook_template(*, owner: str = "") -> dict[str, Any]:
    """Build an account-free manifest for fixture and evidence ownership."""

    scenarios = all_scenarios()
    return {
        "schema_version": 1,
        "purpose": "EMT gateway end-to-end performance evidence manifest",
        "instructions": (
            "Set fixture_id and owner before a live run. Do not place account, password, "
            "endpoint, symbol, order payload, or business callback payload in this file."
        ),
        "scenarios": [
            {
                "key": scenario.key,
                "tier": scenario.tier,
                "profile": scenario.profile,
                "workload": scenario.workload,
                "required_paths": list(scenario.required_path_names),
                "p0_minimum_successes": scenario.p0_minimum_successes,
                "acceptance_minimum_successes": scenario.acceptance_minimum_successes,
                "fixture_id": "",
                "owner": owner,
                "notes": "",
            }
            for scenario in scenarios
        ],
    }


def validate_runbook(manifest: dict[str, Any]) -> None:
    """Reject edited manifests that omit, duplicate, or alter catalog scenarios."""

    if manifest.get("schema_version") != 1:
        raise MatrixError("runbook schema_version must be 1")
    entries = manifest.get("scenarios")
    if not isinstance(entries, list):
        raise MatrixError("runbook scenarios must be a list")
    expected = scenario_index()
    actual_keys = [entry.get("key") for entry in entries if isinstance(entry, dict)]
    if len(actual_keys) != len(entries) or len(set(actual_keys)) != len(actual_keys):
        raise MatrixError("runbook contains missing or duplicate scenario keys")
    if set(actual_keys) != set(expected):
        missing = sorted(set(expected) - set(actual_keys))
        extra = sorted(set(actual_keys) - set(expected))
        raise MatrixError(f"runbook catalog mismatch: missing={missing} extra={extra}")
    for entry in entries:
        scenario = expected[entry["key"]]
        if entry.get("required_paths") != list(scenario.required_path_names):
            raise MatrixError(f"runbook paths were changed for {scenario.key}")
        canonical = {
            "tier": scenario.tier,
            "profile": scenario.profile,
            "workload": scenario.workload,
            "p0_minimum_successes": scenario.p0_minimum_successes,
            "acceptance_minimum_successes": scenario.acceptance_minimum_successes,
        }
        for field, expected_value in canonical.items():
            if entry.get(field) != expected_value:
                raise MatrixError(f"runbook {field} was changed for {scenario.key}")


def scenario_as_dict(scenario: Scenario) -> dict[str, Any]:
    """Serialize a scenario without leaking implementation-only tuple values."""

    result = asdict(scenario)
    result["key"] = scenario.key
    result["required_paths"] = list(scenario.required_path_names)
    return result
