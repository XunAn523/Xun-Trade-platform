"""Local semantic fixtures for read-only empty-result and cursor tests.

The regular query fixture proves that a request reaches C0--C7.  This module
adds the facts a gateway consumer needs to handle safely: an intentional empty
result, and a two-page cursor continuation.  It retains neither query rows nor
cursor-adjacent business fields beyond the opaque numeric ``reference`` needed
for the immediately following request.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .gateway_readonly import QuerySemanticExpectation, ReadOnlyCase
from .matrix import all_scenarios
from .query_fixture import QueryFixtureError, _contains_forbidden_key, _is_json_value


class QuerySemanticFixtureError(QueryFixtureError):
    """A local query-semantic fixture is incomplete, unsafe, or invalid."""


_FUTURE_EMPTY_INTERVAL = {
    "ticker": "",
    "begin_time": 20990101000000000,
    "end_time": 20990101235959999,
}
"""A valid distant future query interval that cannot match a current simulation day."""

_PAGINATION_DEFAULT = {"req_count": 1, "reference": 0, "reserved": 0}

_CONSUMER_DEFAULTS: dict[tuple[str, str, str], dict[str, Any]] = {
    # Order/trade history consumers must distinguish a successful terminal
    # empty response from a missing callback.  Future time windows are
    # read-only and avoid relying on the account's current trade history.
    **{
        (api_name, profile, "empty"): {
            "enabled": True,
            "business_args": [_FUTURE_EMPTY_INTERVAL],
        }
        for api_name in ("QueryOrders", "QueryTrades")
        for profile in ("ordinary", "credit", "option")
    },
    # Reference-data consumers need a genuine second page.  A page size of
    # one keeps the test bounded while forcing the bridge to return and reuse
    # a cursor.  These tables are independent of simulated account holdings.
    ("QueryETFByPage", "ordinary", "pagination"): {
        "enabled": True,
        "business_args": [_PAGINATION_DEFAULT],
    },
    ("QuerySecurityByPage", "ordinary", "pagination"): {
        "enabled": True,
        "business_args": [_PAGINATION_DEFAULT],
    },
}


@dataclass(frozen=True)
class QuerySemanticFixtureEntry:
    api_name: str
    profile: str
    workload: str
    enabled: bool
    business_args: tuple[Any, ...]
    expectation: QuerySemanticExpectation


def _expected_keys() -> tuple[tuple[str, str, str], ...]:
    """Return genuine consumer semantic cases from the canonical matrix."""

    return tuple(
        sorted(
            {
                (scenario.interface, scenario.profile, scenario.workload)
                for scenario in all_scenarios()
                if scenario.kind == "api"
                and scenario.interface.startswith("Query")
                and (
                    scenario.workload == "empty"
                    or (scenario.workload == "pagination" and scenario.interface.endswith("ByPage"))
                )
            }
        )
    )


def query_semantic_fixture_template(*, owner: str = "") -> dict[str, Any]:
    """Build the credential-free local semantic plan for gateway consumers."""

    if owner and not owner.strip():
        raise QuerySemanticFixtureError("semantic fixture owner must not be whitespace")
    cases: list[dict[str, Any]] = []
    for api_name, profile, workload in _expected_keys():
        configured = _CONSUMER_DEFAULTS.get((api_name, profile, workload), {})
        case: dict[str, Any] = {
            "api": api_name,
            "profile": profile,
            "workload": workload,
            "enabled": configured.get("enabled", False),
            "business_args": configured.get("business_args", []),
            "owner": owner,
        }
        if workload == "empty":
            case["expect"] = {"terminal_callback": True, "item_count": 0}
        else:
            case["expect"] = {
                "terminal_callback_per_page": True,
                "minimum_pages": 2,
                "minimum_items_per_page": 1,
                "continuation_from_callback_reference": True,
            }
        cases.append(case)
    return {
        "schema_version": 1,
        "purpose": "Local payload-free EMT query semantic fixture for gateway consumers",
        "instructions": (
            "Keep this ignored local file under data/performance/fixtures/emt/. The pre-enabled cases are read-only "
            "consumer checks: empty future order/trade history and two reference-data pages. Never store "
            "credentials, account numbers, endpoints, callback payloads, ticker values, positions, funds, "
            "orders, or traces. Leave unsupported or account-dependent cases disabled."
        ),
        "cases": cases,
    }


def load_query_semantic_fixture(
    path: Path,
    *,
    profile: str,
    workload: str,
) -> tuple[ReadOnlyCase, ...]:
    """Load locally enabled semantic cases for one profile and workload."""

    if profile not in {"ordinary", "credit", "option"}:
        raise QuerySemanticFixtureError("profile must be ordinary, credit or option")
    if workload not in {"empty", "pagination"}:
        raise QuerySemanticFixtureError("semantic workload must be empty or pagination")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QuerySemanticFixtureError("cannot read local query semantic fixture") from exc
    entries = _validate_manifest(raw)
    selected = [
        ReadOnlyCase(entry.api_name, entry.business_args, entry.expectation)
        for entry in entries
        if entry.profile == profile and entry.workload == workload and entry.enabled
    ]
    if not selected:
        raise QuerySemanticFixtureError(
            f"local query semantic fixture enables no {workload} cases for {profile}"
        )
    return tuple(selected)


def _validate_manifest(raw: object) -> tuple[QuerySemanticFixtureEntry, ...]:
    if not isinstance(raw, Mapping) or raw.get("schema_version") != 1:
        raise QuerySemanticFixtureError("query semantic fixture schema_version must be 1")
    cases = raw.get("cases")
    if not isinstance(cases, list):
        raise QuerySemanticFixtureError("query semantic fixture cases must be a list")
    entries: list[QuerySemanticFixtureEntry] = []
    for entry in cases:
        if not isinstance(entry, Mapping):
            raise QuerySemanticFixtureError("every query semantic fixture case must be an object")
        api_name, profile, workload = entry.get("api"), entry.get("profile"), entry.get("workload")
        enabled, business_args, expectation = (
            entry.get("enabled"),
            entry.get("business_args"),
            entry.get("expect"),
        )
        if not all(isinstance(value, str) for value in (api_name, profile, workload)):
            raise QuerySemanticFixtureError(
                "query semantic fixture api, profile and workload must be strings"
            )
        if type(enabled) is not bool or not isinstance(business_args, list):
            raise QuerySemanticFixtureError(
                "query semantic fixture enabled must be bool and business_args must be a list"
            )
        if not _is_json_value(business_args) or _contains_forbidden_key(business_args):
            raise QuerySemanticFixtureError(
                "query semantic fixture business_args contain unsafe or non-JSON data"
            )
        semantic_expectation = _validate_expectation(
            api_name=api_name,
            workload=workload,
            enabled=enabled,
            business_args=business_args,
            expectation=expectation,
        )
        entries.append(
            QuerySemanticFixtureEntry(
                api_name,
                profile,
                workload,
                enabled,
                tuple(business_args),
                semantic_expectation,
            )
        )
    keys = tuple(sorted((entry.api_name, entry.profile, entry.workload) for entry in entries))
    expected = _expected_keys()
    if keys != expected:
        missing = sorted(set(expected) - set(keys))
        extra = sorted(set(keys) - set(expected))
        raise QuerySemanticFixtureError(
            f"query semantic fixture catalog mismatch: missing={missing[:3]} extra={extra[:3]}"
        )
    return tuple(entries)


def _validate_expectation(
    *,
    api_name: str,
    workload: str,
    enabled: bool,
    business_args: list[Any],
    expectation: object,
) -> QuerySemanticExpectation:
    if not isinstance(expectation, Mapping):
        raise QuerySemanticFixtureError("query semantic fixture expect must be an object")
    if workload == "empty":
        if expectation != {"terminal_callback": True, "item_count": 0}:
            raise QuerySemanticFixtureError(
                "empty semantic expectation must require terminal_callback and item_count=0"
            )
        if enabled and not business_args:
            raise QuerySemanticFixtureError(
                f"enabled empty semantic fixture {api_name} needs business_args"
            )
        return QuerySemanticExpectation(workload="empty", expected_item_count=0)
    if workload == "pagination":
        required = {
            "terminal_callback_per_page": True,
            "minimum_pages": 2,
            "minimum_items_per_page": 1,
            "continuation_from_callback_reference": True,
        }
        if expectation != required:
            raise QuerySemanticFixtureError(
                "pagination semantic expectation must use the bounded two-page contract"
            )
        if enabled:
            if len(business_args) != 1 or not isinstance(business_args[0], Mapping):
                raise QuerySemanticFixtureError(
                    f"enabled pagination semantic fixture {api_name} needs one request mapping"
                )
            request = business_args[0]
            if request.get("req_count") != 1 or request.get("reference") != 0:
                raise QuerySemanticFixtureError(
                    f"enabled pagination semantic fixture {api_name} must start with req_count=1 and reference=0"
                )
        return QuerySemanticExpectation(
            workload="pagination", minimum_pages=2, minimum_items_per_page=1
        )
    raise QuerySemanticFixtureError("semantic workload must be empty or pagination")
