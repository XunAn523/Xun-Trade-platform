"""Schemathesis-generated boundary tests for the fake-only M3 control plane."""

from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path
from typing import Any, Mapping

import schemathesis
from hypothesis import given, settings
from schemathesis.core import NOT_SET

_ROOT = Path(__file__).resolve().parents[1]
_SPEC = json.loads((_ROOT / "contracts" / "control_plane" / "v1" / "openapi.yaml").read_text())
_SCHEMA = schemathesis.openapi.from_dict(_SPEC)


def _operation(method: str, path: str) -> Any:
    for result in _SCHEMA.get_all_operations():
        operation = getattr(result, "ok")()
        if operation.method.upper() == method and operation.path == path:
            return operation
    raise AssertionError(f"OpenAPI operation is missing: {method} {path}")


class _FakeInternalControlPlane:
    """No-SDK fake used only to assert generated request boundaries."""

    def __init__(self) -> None:
        self.broker_calls = 0

    def dispatch(self, case: Any) -> int:
        if case.method == "GET":
            account_id = case.path_parameters.get("account_id")
            return 200 if isinstance(account_id, str) and account_id else 400
        if case.method != "POST":
            return 405
        headers = {str(key).casefold(): value for key, value in dict(case.headers).items()}
        if headers.get("x-test-identity") != "schemathesis-fake":
            return 403
        body = case.body
        if body is NOT_SET or not isinstance(body, Mapping):
            return 400
        required = set(_SPEC["components"]["schemas"]["ControlMutation"]["required"])
        if required - set(body) or set(body) - required:
            return 400
        # Even a structurally valid generated mutation reaches only this fake
        # authorization boundary; it cannot submit or cancel at a broker.
        return 403


@settings(max_examples=20, deadline=None)
@given(_operation("GET", "/v1/accounts/{account_id}/status").as_strategy())
def test_schemathesis_generated_status_cases_stay_inside_fake_control_plane(case: Any) -> None:
    fake = _FakeInternalControlPlane()
    assert fake.dispatch(case) in {200, 400}
    assert fake.broker_calls == 0


@settings(max_examples=30, deadline=None)
@given(_operation("POST", "/v1/accounts/{account_id}/external-orders:attach").as_strategy())
def test_schemathesis_generated_mutations_require_test_identity_and_never_call_broker(
    case: Any,
) -> None:
    fake = _FakeInternalControlPlane()
    assert fake.dispatch(case) in {400, 403}
    assert fake.broker_calls == 0


class ControlPlaneSchemathesisSafetyTests(unittest.TestCase):
    def test_generated_boundary_suite_has_no_emt_sdk_or_network_transport_import(self) -> None:
        source = Path(__file__).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(__file__))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imports.append(node.module)
        forbidden = {"emt_gateway", "vnemttrader", "requests", "httpx", "socket"}
        self.assertTrue(forbidden.isdisjoint(name.split(".", maxsplit=1)[0] for name in imports))
