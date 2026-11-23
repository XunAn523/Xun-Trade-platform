"""Offline contract checks; these are not a Locust/network execution proof."""

from __future__ import annotations

import ast
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "hyperperf" / "http_smoke" / "hyperperf_http_smoke.py"


class _Response:
    def __init__(
        self,
        *,
        status: int = 200,
        payload: object = None,
        content_type: str | None = "application/json",
        error: Exception | None = None,
        json_error: Exception | None = None,
    ) -> None:
        self.status_code = status
        self.headers = {} if content_type is None else {"Content-Type": content_type}
        self.error = error
        self.json = Mock(return_value=payload, side_effect=json_error)
        self.success = Mock()
        self.failure = Mock()
        self.exited = False

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> bool:
        self.exited = True
        return False


@pytest.fixture
def scenario(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Never import/monkey-patch gevent or install dependencies in the test runner.
    locust = ModuleType("locust")
    locust.HttpUser = type("HttpUser", (), {})  # type: ignore[attr-defined]
    locust.task = lambda method: method  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "locust", locust)
    spec = importlib.util.spec_from_file_location("_hyperperf_http_smoke_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(scenario: ModuleType, response: _Response) -> SimpleNamespace:
    client = SimpleNamespace(get=Mock(return_value=response))
    user = scenario.HyperPerfHttpSmokeUser()
    user.client = client
    user.work()
    client.get.assert_called_once_with(
        "/work",
        name="GET /work",
        headers={"Accept": "application/json"},
        timeout=(2.0, 5.0),
        allow_redirects=False,
        catch_response=True,
    )
    assert response.exited
    assert response.success.call_count + response.failure.call_count == 1
    return client


@pytest.mark.parametrize(
    "content_type",
    ["application/json", "application/json; charset=utf-8", "Application/JSON"],
)
def test_work_accepts_only_valid_http_contract(scenario: ModuleType, content_type: str) -> None:
    response = _Response(payload={"accepted": True}, content_type=content_type)
    _run(scenario, response)
    response.success.assert_called_once_with()
    response.failure.assert_not_called()


@pytest.mark.parametrize(
    "status", [0, 201, 204, 301, 302, 307, 308, 400, 401, 403, 404, 409, 429, 500, 503]
)
def test_http_status_cannot_become_success(scenario: ModuleType, status: int) -> None:
    response = _Response(status=status, payload={"accepted": True})
    _run(scenario, response)
    response.failure.assert_called_once_with(f"HTTP_STATUS expected=200 actual={status}")
    response.json.assert_not_called()


@pytest.mark.parametrize(
    "content_type", [None, "", "text/html", "text/plain", "application/problem+json"]
)
def test_non_json_media_type_fails(scenario: ModuleType, content_type: str | None) -> None:
    response = _Response(payload={"accepted": True}, content_type=content_type)
    _run(scenario, response)
    response.failure.assert_called_once_with("PROTOCOL_ERROR expected application/json")
    response.json.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        True,
        False,
        1,
        "accepted",
        [],
        {},
        {"accepted": False},
        {"accepted": 1},
        {"accepted": "true"},
        {"accepted": None},
        {"accepted": True, "unknown": 1},
    ],
)
def test_invalid_business_payload_never_succeeds(scenario: ModuleType, payload: object) -> None:
    response = _Response(payload=payload)
    _run(scenario, response)
    response.failure.assert_called_once_with("BUSINESS_CONTRACT expected exactly accepted=true")


@pytest.mark.parametrize(
    "error", [ValueError("private-response"), json.JSONDecodeError("bad", "private-response", 0)]
)
def test_invalid_json_is_reported_without_response_body(
    scenario: ModuleType, error: Exception
) -> None:
    response = _Response(json_error=error)
    _run(scenario, response)
    response.failure.assert_called_once_with("PROTOCOL_ERROR invalid JSON")


@pytest.mark.parametrize(
    "error", [TimeoutError("read"), ConnectionError("connect"), OSError("reset")]
)
def test_original_transport_exception_is_preserved(scenario: ModuleType, error: Exception) -> None:
    response = _Response(status=0, error=error)
    _run(scenario, response)
    assert response.failure.call_args.args[0] is error
    response.json.assert_not_called()


def test_unexpected_json_runtime_error_is_not_swallowed(scenario: ModuleType) -> None:
    error = RuntimeError("unexpected script fault")
    response = _Response(json_error=error)
    with pytest.raises(RuntimeError) as caught:
        _run(scenario, response)
    assert caught.value is error
    response.success.assert_not_called()
    assert response.exited


def test_request_setup_exception_is_not_swallowed(scenario: ModuleType) -> None:
    user = scenario.HyperPerfHttpSmokeUser()
    error = RuntimeError("unexpected setup fault")
    user.client = SimpleNamespace(get=Mock(side_effect=error))
    with pytest.raises(RuntimeError) as caught:
        user.work()
    assert caught.value is error


def test_operation_names_match_exactly(scenario: ModuleType) -> None:
    names = json.loads(SCRIPT.with_name("operation_names.json").read_text(encoding="utf-8"))
    assert names == [scenario.OPERATION_NAME] == ["GET /work"]


def test_upload_is_self_contained_and_does_not_override_platform() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert len(imports) == 1
    assert isinstance(imports[0], ast.ImportFrom) and imports[0].module == "locust"
    assert [alias.name for alias in imports[0].names] == ["HttpUser", "task"]
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    assert len(classes) == 1
    assert isinstance(classes[0].bases[0], ast.Name) and classes[0].bases[0].id == "HttpUser"
    methods = [node.name for node in classes[0].body if isinstance(node, ast.FunctionDef)]
    assert methods == ["work"]
    for forbidden in ("proxies", "trust_env", "fire", "wait_time", "host", "spawn_rate"):
        assert not any(
            isinstance(node, ast.Attribute)
            and node.attr == forbidden
            or isinstance(node, ast.keyword)
            and node.arg == forbidden
            or isinstance(node, ast.Name)
            and node.id == forbidden
            for node in ast.walk(tree)
        )
