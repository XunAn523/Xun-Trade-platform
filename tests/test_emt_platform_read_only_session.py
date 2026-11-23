"""Tests for the isolated vendor-platform side of M2-E-02H."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.platform_loader import LoadedSdk
from emt_gateway.session import SessionSettings
from emt_gateway.tradecore_read_only import EmtPlatformReadOnlySessionFactory
from xuanling_tradecore.adapters import AccountTradingCapabilities, BrokerQuery, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtControlledReadOnlyAdapter,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import BrokerEvent, CallbackGapEvidence, TradingDayEpoch


class _Observer:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


class _FakeTraderApi:
    instances: list["_FakeTraderApi"] = []

    def __init__(self) -> None:
        self.calls: list[str] = []
        type(self).instances.append(self)

    def createTraderApi(self, client_id: int, log_dir: str, log_level: int) -> None:
        assert client_id == 101
        assert log_dir
        assert log_level == 0
        self.calls.append("create")

    def setSoftwareVersion(self, version: str) -> None:
        assert version == "xuanling-tradecore/m2-e-02h-readonly"
        self.calls.append("software")

    def setHeartBeatInterval(self, seconds: int) -> None:
        assert seconds == 10
        self.calls.append("heartbeat")

    def login(self, *args: object) -> int:
        del args
        self.calls.append("login")
        return 71

    def getApiVersion(self) -> str:
        return "2.27.0.0"

    def getTradingDay(self) -> str:
        return "2026-09-03"

    def queryUnfinishedOrders(self, session_id: int, request_id: int) -> int:
        assert session_id == 71
        self.calls.append("query")
        self.onQueryOrder(
            {"sensitive_row": "must-not-cross-boundary"},
            {"error_id": 0},
            request_id,
            True,
            session_id,
        )
        return 0

    def logout(self, session_id: int) -> None:
        assert session_id == 71
        self.calls.append("logout")

    def release(self) -> None:
        self.calls.append("release")


class _SdkProvider:
    def load(self) -> LoadedSdk:
        module = type("FakeLoadedModule", (), {"TraderApi": _FakeTraderApi})
        return LoadedSdk(library_dir=Path("C:/synthetic-sdk"), module=module)


def _mapping():
    manifest = load_builtin_emt_legacy_sim_manifest()
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


class EmtPlatformReadOnlySessionTests(unittest.TestCase):
    def test_platform_factory_only_exposes_typed_terminal_callback_and_closes(self) -> None:
        _FakeTraderApi.instances = []
        with TemporaryDirectory() as temporary:
            factory = EmtPlatformReadOnlySessionFactory(
                sdk_provider=_SdkProvider(),
                settings=SessionSettings(
                    host="synthetic-host",
                    port=19088,
                    account="synthetic-account",
                    password="synthetic-password",
                    client_id=101,
                    sdk_log_dir=Path(temporary),
                    software_version="xuanling-tradecore/m2-e-02h-readonly",
                ),
            )
            adapter = EmtControlledReadOnlyAdapter(
                _mapping(),
                session_factory=factory,
                expected_api_version="2.27.0",
                expected_trading_day=TradingDayEpoch("2026-09-03"),
                session_epoch=5,
            )
            observer = _Observer()
            adapter.start(observer)
            handle = adapter.query(
                BrokerQuery(
                    request_id="domain-query-2",
                    account_id="account-1",
                    venue_id="EMT",
                    adapter_instance_id="emt-sim-instance-1",
                    query_kind="unfinished_orders",
                    page_size=10,
                )
            )
            self.assertEqual("domain-query-2", handle.request_id)
            self.assertEqual(1, len(observer.events))
            self.assertNotIn("synthetic", observer.events[0].source_key)
            self.assertTrue(factory.snapshot().session_open)
            adapter.stop()

        api = _FakeTraderApi.instances[0]
        self.assertEqual(
            ["create", "software", "heartbeat", "login", "query", "logout", "release"],
            api.calls,
        )
        self.assertFalse(factory.snapshot().session_open)

    def test_platform_factory_requires_observer_before_loading_sdk(self) -> None:
        with TemporaryDirectory() as temporary:
            factory = EmtPlatformReadOnlySessionFactory(
                sdk_provider=_SdkProvider(),
                settings=SessionSettings(
                    host="synthetic-host",
                    port=19088,
                    account="synthetic-account",
                    password="synthetic-password",
                    client_id=101,
                    sdk_log_dir=Path(temporary),
                ),
            )
            mapping = _mapping()
            from xuanling_tradecore.adapters.emt import EmtQueryCallbackIngress

            ingress = EmtQueryCallbackIngress(
                mapping,
                trading_day=TradingDayEpoch("2026-09-03"),
                native_session_id=None,
                session_epoch=6,
            )
            with self.assertRaisesRegex(Exception, "observer must attach"):
                factory.open(callback_ingress=ingress)

    def test_platform_module_has_no_direct_vendor_binding_import(self) -> None:
        source = Path(__file__).parents[1] / "emt_gateway" / "tradecore_read_only.py"
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        imported_modules = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules.append(node.module)
        self.assertTrue(
            all(
                root not in {"vnemttrader", "emt_api"}
                for root in (module.split(".", maxsplit=1)[0] for module in imported_modules)
            )
        )


if __name__ == "__main__":
    unittest.main()
