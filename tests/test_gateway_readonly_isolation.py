from __future__ import annotations

from types import SimpleNamespace

from tools.performance.perfbench import gateway_readonly
from tools.performance.perfbench.gateway_readonly import (
    GatewayReadOnlySettings,
    ReadOnlyCase,
    run_gateway_readonly,
)
from tools.performance.perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def test_isolated_query_retains_native_session_after_report(monkeypatch, tmp_path) -> None:
    """The throw-away acceptance path must not enter the vendor finalizer."""

    state: dict[str, object] = {}

    class FakeJournal:
        def __init__(self, path) -> None:
            state["journal_path"] = path

        def close(self) -> None:
            state["journal_closed"] = True

    class FakeCallbacks:
        def __init__(self, **kwargs) -> None:
            state["callbacks_created"] = kwargs["require_native_fastspi"]

    class FakeSession:
        api = object()
        session_id = 17

        @classmethod
        def from_loaded_sdk(cls, loaded, *, callback_ingress, settings):
            del loaded, callback_ingress, settings
            instance = cls()
            state["session"] = instance
            return instance

        def start(self):
            return SimpleNamespace(api_version="2.0", trading_day="20260914")

        def close(self) -> None:
            state["vendor_close_called"] = True

    class FakeLoader:
        def __init__(self, settings) -> None:
            del settings

        def load(self):
            return object()

    class FakeFacade:
        def __init__(self, api, **kwargs) -> None:
            del api, kwargs

        def spec_for(self, api_name: str):
            assert api_name == "QueryAsset"
            return SimpleNamespace(callback_name="OnQueryAsset")

    monkeypatch.setattr(gateway_readonly, "FileJournal", FakeJournal)
    monkeypatch.setattr(gateway_readonly, "CallbackPipeline", FakeCallbacks)
    monkeypatch.setattr(gateway_readonly, "EmtSession", FakeSession)
    monkeypatch.setattr(gateway_readonly, "EmtPlatformLoader", FakeLoader)
    monkeypatch.setattr(gateway_readonly, "EmtTraderFacade", FakeFacade)
    monkeypatch.setattr(gateway_readonly, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        gateway_readonly,
        "_invoke_read_only_case",
        lambda **kwargs: (
            {"api": "QueryAsset", "outcome": "ok", "reason": None},
            "correlation-1",
            None,
        ),
    )
    retained: list[object] = []
    report = run_gateway_readonly(
        GatewayReadOnlySettings(
            sdk_root=tmp_path / "sdk",
            runtime_dir=tmp_path / "runtime",
            accounts=SimulatedAccounts(
                host="127.0.0.1",
                port=1,
                ordinary=SimulatedAccount(account="ordinary", password="secret"),
                credit=SimulatedAccount(account="credit", password="secret"),
                option=SimulatedAccount(account="option", password="secret"),
            ),
            interval_seconds=0.25,
        ),
        cases=(ReadOnlyCase("QueryAsset"),),
        known_callback_gaps=(),
        retain_native_reference=retained.append,
    )

    assert report["driver"] == "gateway-readonly-c0-c7"
    assert retained == [state["session"]]
    assert "vendor_close_called" not in state
    assert state["journal_closed"] is True
