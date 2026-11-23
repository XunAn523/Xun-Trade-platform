"""Controlled hand-off to a real gateway fixture process.

The performance package has no account details or order payloads. A caller
must provide a reviewed account-free runbook and explicitly opt in to the
simulated fixture process; that external process owns credentials and payload
construction in its local controlled configuration.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .gateway_report import build_gateway_report
from .matrix import validate_runbook
from .workload_plan import validate_workload_plan


class FixtureDriverError(RuntimeError):
    """The controlled fixture hand-off is invalid or failed."""


@dataclass(frozen=True)
class FixtureDriverSettings:
    runbook: Path
    trace_output: Path
    command: tuple[str, ...]
    execute_simulated: bool = False
    timeout_seconds: float = 900.0
    workload_plan: Path | None = None


def validate_fixture_runbook(manifest: dict[str, Any]) -> None:
    """Require a named owner and fixture ID before permitting a live process."""

    validate_runbook(manifest)
    missing = [
        entry.get("key", "<unknown>")
        for entry in manifest["scenarios"]
        if not isinstance(entry.get("fixture_id"), str)
        or not entry["fixture_id"]
        or not isinstance(entry.get("owner"), str)
        or not entry["owner"]
    ]
    if missing:
        raise FixtureDriverError(f"fixture runbook has unowned scenarios: {', '.join(missing[:3])}")


def _load_workload_plan(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FixtureDriverError("cannot read non-steady workload plan") from exc
    if not isinstance(manifest, dict):
        raise FixtureDriverError("non-steady workload plan must be a JSON object")
    try:
        validate_workload_plan(manifest, require_ready=True)
    except ValueError as exc:
        raise FixtureDriverError(str(exc)) from exc
    return manifest


def _validate_workload_plan_matches_runbook(
    runbook: dict[str, Any], workload_plan: dict[str, Any]
) -> None:
    runbook_entries = {entry["key"]: entry for entry in runbook["scenarios"]}
    mismatches = [
        entry["key"]
        for entry in workload_plan["scenarios"]
        if any(
            runbook_entries[entry["key"]].get(field) != entry[field]
            for field in ("fixture_id", "owner")
        )
    ]
    if mismatches:
        raise FixtureDriverError(
            "non-steady workload plan does not match runbook fixture ownership: "
            + ", ".join(mismatches[:3])
        )


def run_fixture_driver(settings: FixtureDriverSettings) -> dict[str, Any]:
    """Validate or explicitly launch a simulated fixture and aggregate C0--C7."""

    try:
        manifest = json.loads(settings.runbook.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FixtureDriverError("cannot read fixture runbook") from exc
    if not isinstance(manifest, dict):
        raise FixtureDriverError("fixture runbook must be a JSON object")
    validate_fixture_runbook(manifest)
    workload_plan: dict[str, Any] | None = None
    if settings.workload_plan is not None:
        workload_plan = _load_workload_plan(settings.workload_plan)
        _validate_workload_plan_matches_runbook(manifest, workload_plan)
    if not settings.execute_simulated:
        return {
            "schema_version": 1,
            "driver": "gateway-fixture",
            "executed": False,
            "safety": "dry-run; pass explicit execute_simulated only for reviewed simulated accounts",
            "scenarios": len(manifest["scenarios"]),
            "nonsteady_workload_plan": "validated" if workload_plan is not None else "not_provided",
        }
    if workload_plan is None:
        raise FixtureDriverError(
            "--workload-plan is required for external fixture execution; generate and review it first"
        )
    if not settings.command:
        raise FixtureDriverError("an explicit fixture command is required to execute")
    settings.trace_output.parent.mkdir(parents=True, exist_ok=True)
    environment = {
        **os.environ,
        "EMT_GATEWAY_FIXTURE_RUNBOOK": str(settings.runbook.resolve()),
        "EMT_GATEWAY_TRACE_PATH": str(settings.trace_output.resolve()),
        "EMT_GATEWAY_EXECUTE_SIMULATED": "1",
    }
    try:
        completed = subprocess.run(
            settings.command,
            check=False,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=settings.timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FixtureDriverError("fixture process did not complete") from exc
    if completed.returncode != 0:
        raise FixtureDriverError(f"fixture process exited with code {completed.returncode}")
    try:
        events = [
            json.loads(line)
            for line in settings.trace_output.read_text(encoding="utf-8").splitlines()
            if line
        ]
    except (OSError, json.JSONDecodeError) as exc:
        raise FixtureDriverError("fixture process produced no valid payload-free trace") from exc
    if not all(isinstance(event, dict) for event in events):
        raise FixtureDriverError("fixture trace contains a non-object event")
    report = build_gateway_report(events)
    report["driver"] = "gateway-fixture"
    report["executed"] = True
    report["fixture_process"] = {"exit_code": completed.returncode, "output_retained": False}
    return report
