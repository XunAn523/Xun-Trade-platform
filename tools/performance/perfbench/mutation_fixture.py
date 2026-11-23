"""Catalog-complete local template for controlled EMT mutation fixtures.

This module deliberately does not invoke EMT.  It owns the local configuration
contract used by mutation drivers: credentials stay in the separate account
file, while a request may only hold JSON-serializable business parameters.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from emt_gateway.schema_validation import SchemaContractError, validate_schema_instance

from .matrix import all_scenarios

_FORBIDDEN_KEYS = frozenset(
    {
        "account",
        "address",
        "endpoint",
        "fund_account",
        "host",
        "password",
        "port",
        "secret",
        "token",
    }
)


class MutationFixtureError(RuntimeError):
    """A local mutation fixture is incomplete, unsafe, or cannot be read."""


@dataclass(frozen=True)
class MutationFixtureEntry:
    """One validated, credential-free mutation case selected for a profile."""

    api_name: str
    profile: str
    enabled: bool
    request: dict[str, Any]
    rate_limit_per_second: int
    reconciliation_query: str


def _expected_keys() -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            {
                (scenario.interface, scenario.profile)
                for scenario in all_scenarios()
                if scenario.kind == "api"
                and scenario.tier in {"CMD-H", "CMD-A"}
                and scenario.workload == "steady"
            }
        )
    )


def mutation_fixture_template() -> dict[str, Any]:
    """Return an entirely disabled local mutation-fixture template.

    ``request`` is intentionally empty and cannot hold credentials.  An
    actual runner must require a separately injected secret resolver and an
    explicit armed flag before it can send a single state-changing request.
    """

    return {
        "schema_version": 1,
        "purpose": "Local simulated EMT mutation fixture; all entries start disarmed",
        "safety": {
            "armed": False,
            "instructions": (
                "Use simulated accounts only. Keep credentials and endpoint details outside this file. "
                "Every enabled command must have an approved request, rate limit, reconciliation query, "
                "and a unique client id."
            ),
        },
        "cases": [
            {
                "api": api_name,
                "profile": profile,
                "enabled": False,
                "request": {},
                "rate_limit_per_second": 0,
                "reconciliation_query": "",
                "notes": "configure locally before arming",
            }
            for api_name, profile in _expected_keys()
        ],
    }


def validate_mutation_fixture(manifest: object) -> None:
    """Reject unsafe or catalog-incomplete mutation configuration early."""

    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ValueError("mutation fixture schema_version must be 1")
    safety, cases = manifest.get("safety"), manifest.get("cases")
    if (
        not isinstance(safety, dict)
        or type(safety.get("armed")) is not bool
        or not isinstance(cases, list)
    ):
        raise ValueError("mutation fixture must declare safety.armed and cases")
    keys: list[tuple[str, str]] = []
    for entry in cases:
        if not isinstance(entry, dict):
            raise ValueError("mutation fixture cases must be objects")
        api, profile, enabled = entry.get("api"), entry.get("profile"), entry.get("enabled")
        request, rate = entry.get("request"), entry.get("rate_limit_per_second")
        reconcile = entry.get("reconciliation_query")
        if not isinstance(api, str) or not isinstance(profile, str) or type(enabled) is not bool:
            raise ValueError("mutation fixture case identity is invalid")
        if (
            not isinstance(request, dict)
            or not isinstance(rate, int)
            or rate < 0
            or not isinstance(reconcile, str)
        ):
            raise ValueError("mutation fixture request or rate limit is invalid")
        if not _is_json_value(request) or _contains_forbidden_key(request):
            raise ValueError("mutation fixture request contains unsafe or non-JSON data")
        if enabled and (not safety["armed"] or rate <= 0 or not reconcile):
            raise ValueError(
                "enabled mutation requires armed fixture, positive rate limit and reconciliation query"
            )
        keys.append((api, profile))
    if tuple(sorted(keys)) != _expected_keys():
        raise ValueError("mutation fixture catalog mismatch")
    try:
        validate_schema_instance("mutation-fixture-v1", manifest)
    except SchemaContractError as exc:
        raise ValueError(str(exc)) from exc


def load_mutation_fixture(path: Path, *, profile: str) -> tuple[MutationFixtureEntry, ...]:
    """Load locally enabled commands for one profile without exposing requests.

    The caller receives the request only in process memory so it can construct
    the native binding object.  It must never copy that mapping into a trace,
    report, journal, exception, or log message.
    """

    if profile not in {"ordinary", "credit", "option"}:
        raise MutationFixtureError("profile must be ordinary, credit or option")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MutationFixtureError("cannot read local mutation fixture") from exc
    try:
        validate_mutation_fixture(raw)
    except ValueError as exc:
        raise MutationFixtureError(str(exc)) from exc
    assert isinstance(raw, Mapping)  # Proven by validate_mutation_fixture.
    entries: list[MutationFixtureEntry] = []
    for item in raw["cases"]:
        assert isinstance(item, Mapping)
        if item["profile"] != profile or not item["enabled"]:
            continue
        request = item["request"]
        assert isinstance(request, dict)
        entries.append(
            MutationFixtureEntry(
                api_name=str(item["api"]),
                profile=profile,
                enabled=True,
                request=dict(request),
                rate_limit_per_second=int(item["rate_limit_per_second"]),
                reconciliation_query=str(item["reconciliation_query"]),
            )
        )
    return tuple(entries)


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            key.casefold() in _FORBIDDEN_KEYS or _contains_forbidden_key(item)
            for key, item in value.items()
            if isinstance(key, str)
        )
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def _is_json_value(value: object) -> bool:
    if value is None or type(value) in {bool, int, str}:
        return True
    if type(value) is float:
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False
