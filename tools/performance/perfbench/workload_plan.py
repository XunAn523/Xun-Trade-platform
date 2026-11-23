"""Safety gates for non-steady EMT gateway performance experiments.

Steady-state traces cannot establish the burst, empty-result, pagination,
late-callback, failure, or recovery characteristics in the catalog.  This
module derives the remaining scenarios from the canonical matrix and creates
an account-free, payload-free control plan for their reviewed execution.
"""

from __future__ import annotations

from typing import Any

from .matrix import MatrixError, Scenario, scenario_index

_CONTROL_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "burst": ("approved_rate_lane", "duration_window", "backpressure_observation"),
    "cold": ("isolated_process", "cache_state"),
    "duplicate": ("duplicate_injection", "deduplication_check"),
    "empty": ("empty_dataset_fixture", "terminal_callback"),
    "failure": ("expected_error_class", "no_side_effect_check"),
    "late": ("delay_injection", "timeout_boundary"),
    "out_of_order": ("ordering_injection", "ordering_check"),
    "pagination": ("multi_page_fixture", "terminal_page_check"),
    "reconnect": ("disconnect_injection", "reconnect_check"),
    "recovery": ("restart_or_session_recovery", "reconciliation_check"),
    "slow_consumer": ("consumer_pause", "overflow_or_backpressure_check"),
    "storm": ("bounded_event_burst", "loss_and_queue_check"),
    "throttled": ("rate_limit_policy", "rate_limited_observation"),
}

_ENTRY_FIELDS = frozenset(
    {
        "key",
        "tier",
        "profile",
        "workload",
        "required_paths",
        "control_requirements",
        "fixture_id",
        "owner",
        "control_ref",
        "recovery_check",
    }
)


def nonsteady_scenarios() -> tuple[Scenario, ...]:
    """Return every matrix scenario that needs a dedicated workload control."""

    result = tuple(
        scenario for scenario in scenario_index().values() if scenario.workload != "steady"
    )
    unknown = sorted({scenario.workload for scenario in result} - set(_CONTROL_REQUIREMENTS))
    if unknown:
        raise MatrixError(f"non-steady workloads lack control requirements: {unknown}")
    return result


def workload_plan_template(*, owner: str = "") -> dict[str, Any]:
    """Build a payload-free template for all non-steady matrix scenarios."""

    scenarios = nonsteady_scenarios()
    return {
        "schema_version": 1,
        "purpose": "EMT gateway non-steady workload control plan",
        "instructions": (
            "Fill only reviewed identifiers: fixture_id, owner, control_ref, and "
            "recovery_check. Keep account, password, endpoint, symbol, order payload, "
            "business callback payload, and fault-injection details in local controlled "
            "configuration, never in this file."
        ),
        "scenarios": [
            {
                "key": scenario.key,
                "tier": scenario.tier,
                "profile": scenario.profile,
                "workload": scenario.workload,
                "required_paths": list(scenario.required_path_names),
                "control_requirements": list(_CONTROL_REQUIREMENTS[scenario.workload]),
                "fixture_id": "",
                "owner": owner,
                "control_ref": "",
                "recovery_check": "",
            }
            for scenario in scenarios
        ],
    }


def _require_nonempty_text(entry: dict[str, Any], field: str) -> None:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise MatrixError(f"workload plan {field} is required for {entry.get('key', '<unknown>')}")


def validate_workload_plan(manifest: dict[str, Any], *, require_ready: bool = False) -> None:
    """Validate exact non-steady coverage and, optionally, execution readiness.

    The plan deliberately accepts only opaque control references.  It records
    that a reviewed fixture exists without turning Git or trace artifacts into
    a store of credentials, business data, or fault-injection payloads.
    """

    if manifest.get("schema_version") != 1:
        raise MatrixError("workload plan schema_version must be 1")
    entries = manifest.get("scenarios")
    if not isinstance(entries, list):
        raise MatrixError("workload plan scenarios must be a list")
    expected = {scenario.key: scenario for scenario in nonsteady_scenarios()}
    actual_keys = [entry.get("key") for entry in entries if isinstance(entry, dict)]
    if len(actual_keys) != len(entries) or len(set(actual_keys)) != len(actual_keys):
        raise MatrixError("workload plan contains missing or duplicate scenario keys")
    if set(actual_keys) != set(expected):
        missing = sorted(set(expected) - set(actual_keys))
        extra = sorted(set(actual_keys) - set(expected))
        raise MatrixError(f"workload plan catalog mismatch: missing={missing} extra={extra}")
    for entry in entries:
        unexpected = sorted(set(entry) - _ENTRY_FIELDS)
        if unexpected:
            raise MatrixError(
                f"workload plan has prohibited fields for {entry['key']}: {unexpected}"
            )
        scenario = expected[entry["key"]]
        canonical = {
            "tier": scenario.tier,
            "profile": scenario.profile,
            "workload": scenario.workload,
            "required_paths": list(scenario.required_path_names),
            "control_requirements": list(_CONTROL_REQUIREMENTS[scenario.workload]),
        }
        for field, expected_value in canonical.items():
            if entry.get(field) != expected_value:
                raise MatrixError(f"workload plan {field} was changed for {scenario.key}")
        for field in ("fixture_id", "owner", "control_ref", "recovery_check"):
            value = entry.get(field)
            if not isinstance(value, str):
                raise MatrixError(f"workload plan {field} must be text for {scenario.key}")
            if require_ready:
                _require_nonempty_text(entry, field)
