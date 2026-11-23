"""Local, payload-safe configuration for real read-only query fixtures.

The fixture file is deliberately separate from the account file and belongs
under ignored ``data/performance/fixtures/emt/``.  It can contain query predicates and
pagination parameters, but it must never contain credentials, endpoints, or
callback payloads.  The checked-in generator exposes every official query and
profile combination so omitted interfaces remain visible rather than silently
falling out of the real-test plan.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .gateway_readonly import ReadOnlyCase
from .matrix import all_scenarios


class QueryFixtureError(RuntimeError):
    """A local query-fixture manifest is incomplete or unsafe."""


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
_SAFE_DEFAULT_BUSINESS_ARGS: dict[str, list[Any]] = {
    # These are SDK-documented empty predicates, not account or market data.
    # They let a newly generated fixture execute the same baseline queries as
    # gateway-readonly without an avoidable Python-binding argument error.
    "QueryPosition": [""],
    "QueryOrders": [{"ticker": "", "begin_time": 0, "end_time": 0}],
    "QueryTrades": [{"ticker": "", "begin_time": 0, "end_time": 0}],
}
_SUPPORTED_PROFILES = frozenset({"ordinary", "credit", "option"})
_MIN_CALLBACK_CAPACITY = 1
_MAX_CALLBACK_CAPACITY = 65_536


@dataclass(frozen=True)
class QueryFixtureEntry:
    api_name: str
    profile: str
    enabled: bool
    business_args: tuple[Any, ...]


def _expected_keys() -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            {
                (scenario.interface, scenario.profile)
                for scenario in all_scenarios()
                if scenario.kind == "api"
                and scenario.interface.startswith("Query")
                and scenario.workload == "steady"
            }
        )
    )


def query_fixture_template() -> dict[str, Any]:
    """Generate a catalog-complete, credential-free local fixture template."""

    return {
        "schema_version": 1,
        "purpose": "Local EMT read-only query fixture; fill only enabled cases",
        "instructions": (
            "Keep this file under ignored data/performance/fixtures/emt/. business_args may contain only the "
            "arguments before session_id/request_id. Do not store credentials, account numbers, "
            "host, port, endpoint, callback payloads, or production data here."
        ),
        "minimum_callback_capacity": {
            "ordinary": _MIN_CALLBACK_CAPACITY,
            "credit": _MIN_CALLBACK_CAPACITY,
            "option": _MIN_CALLBACK_CAPACITY,
        },
        "cases": [
            {
                "api": api_name,
                "profile": profile,
                "enabled": False,
                "business_args": _SAFE_DEFAULT_BUSINESS_ARGS.get(api_name, []),
                "notes": "configure locally before enabling",
            }
            for api_name, profile in _expected_keys()
        ],
    }


def load_query_fixture(path: Path, *, profile: str) -> tuple[ReadOnlyCase, ...]:
    """Load enabled cases for one account profile without exposing their args."""

    _validate_profile(profile)
    raw = _read_manifest(path)
    entries = _validate_manifest(raw)
    selected = [
        ReadOnlyCase(entry.api_name, entry.business_args)
        for entry in entries
        if entry.profile == profile and entry.enabled
    ]
    if not selected:
        raise QueryFixtureError(f"local query fixture enables no cases for {profile}")
    return tuple(selected)


def required_callback_capacity(path: Path, *, profile: str) -> int:
    """Return the fixture-declared lower bound for its callback ring.

    A read-only fixture can contain a deliberately unbounded historical query.
    Such a query must either receive a ring large enough to retain every
    callback through its terminal marker, or be rejected before it reaches an
    EMT session.  This contract applies only to the isolated query acceptance
    runner; it never changes execution-lane or hot-path queue sizing.
    """

    _validate_profile(profile)
    raw = _read_manifest(path)
    _validate_manifest(raw)
    requirements = raw.get("minimum_callback_capacity")
    if requirements is None:
        return _MIN_CALLBACK_CAPACITY
    if not isinstance(requirements, Mapping):
        raise QueryFixtureError("minimum_callback_capacity must be an object")
    unknown_profiles = set(requirements) - _SUPPORTED_PROFILES
    if unknown_profiles:
        raise QueryFixtureError("minimum_callback_capacity has an unknown profile")
    requested = requirements.get(profile, _MIN_CALLBACK_CAPACITY)
    if type(requested) is not int or not _MIN_CALLBACK_CAPACITY <= requested <= _MAX_CALLBACK_CAPACITY:
        raise QueryFixtureError(
            f"minimum_callback_capacity for {profile} must be an integer in "
            f"{_MIN_CALLBACK_CAPACITY}..{_MAX_CALLBACK_CAPACITY}"
        )
    return requested


def select_query_cases(
    cases: tuple[ReadOnlyCase, ...],
    requested_names: tuple[str, ...] | None,
) -> tuple[ReadOnlyCase, ...]:
    """Select an explicit subset of already-enabled local fixture cases.

    The selector never enables a dormant entry or supplies its arguments.  It
    exists so a documented terminal-callback gap can be excluded from a
    low-rate P0 batch without editing the operator's local fixture.
    """

    if requested_names is None:
        return cases
    if not requested_names:
        raise QueryFixtureError("at least one --query-case value is required when selecting cases")
    if any(not name for name in requested_names):
        raise QueryFixtureError("--query-case values must be non-empty")
    if len(set(requested_names)) != len(requested_names):
        raise QueryFixtureError("--query-case values must not repeat")
    available = {case.api_name for case in cases}
    unknown = sorted(set(requested_names) - available)
    if unknown:
        raise QueryFixtureError(f"--query-case is not enabled for this profile: {unknown[0]}")
    selected_names = set(requested_names)
    selected = tuple(case for case in cases if case.api_name in selected_names)
    if not selected:
        raise QueryFixtureError("--query-case selected no enabled cases")
    return selected


def _validate_manifest(raw: object) -> tuple[QueryFixtureEntry, ...]:
    if not isinstance(raw, Mapping) or raw.get("schema_version") != 1:
        raise QueryFixtureError("query fixture schema_version must be 1")
    cases = raw.get("cases")
    if not isinstance(cases, list):
        raise QueryFixtureError("query fixture cases must be a list")
    entries: list[QueryFixtureEntry] = []
    for entry in cases:
        if not isinstance(entry, Mapping):
            raise QueryFixtureError("every query fixture case must be an object")
        api_name, profile = entry.get("api"), entry.get("profile")
        enabled, business_args = entry.get("enabled"), entry.get("business_args")
        if not isinstance(api_name, str) or not isinstance(profile, str):
            raise QueryFixtureError("query fixture api and profile must be strings")
        if type(enabled) is not bool or not isinstance(business_args, list):
            raise QueryFixtureError(
                "query fixture enabled must be bool and business_args must be a list"
            )
        if not _is_json_value(business_args) or _contains_forbidden_key(business_args):
            raise QueryFixtureError("query fixture business_args contain unsafe or non-JSON data")
        entries.append(QueryFixtureEntry(api_name, profile, enabled, tuple(business_args)))
    keys = tuple(sorted((entry.api_name, entry.profile) for entry in entries))
    expected = _expected_keys()
    if keys != expected:
        missing = sorted(set(expected) - set(keys))
        extra = sorted(set(keys) - set(expected))
        raise QueryFixtureError(
            f"query fixture catalog mismatch: missing={missing[:3]} extra={extra[:3]}"
        )
    return tuple(entries)


def _read_manifest(path: Path) -> Mapping[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QueryFixtureError("cannot read local query fixture") from exc
    if not isinstance(raw, Mapping):
        raise QueryFixtureError("query fixture must be an object")
    return raw


def _validate_profile(profile: str) -> None:
    if profile not in _SUPPORTED_PROFILES:
        raise QueryFixtureError("profile must be ordinary, credit or option")


def _is_json_value(value: object) -> bool:
    if value is None or type(value) in {bool, int, float, str}:
        return True
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    if isinstance(value, Mapping):
        return any(
            key.casefold() in _FORBIDDEN_KEYS or _contains_forbidden_key(item)
            for key, item in value.items()
            if isinstance(key, str)
        )
    return False
