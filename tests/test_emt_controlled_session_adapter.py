"""Tests for the injected, payload-free EMT controlled session Adapter."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import AccountTradingCapabilities, BrokerQuery, RuntimeVenueProfile
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtControlledReadOnlyAdapter,
    EmtControlledSessionError,
    EmtQueryCallbackIngress,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import BrokerEvent, CallbackGapEvidence, TradingDayEpoch

DAY = TradingDayEpoch("2026-09-02")


class _Observer:
    def __init__(self) -> None:
        self.events: list[BrokerEvent] = []
        self.gaps: list[CallbackGapEvidence] = []

    def on_broker_event(self, event: BrokerEvent) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence: CallbackGapEvidence) -> None:
        self.gaps.append(evidence)


class _Session:
    def __init__(
        self,
        *,
        native_session_id: int = 71,
        api_version: str = "2.27.0.0",
        trading_day: str = "2026-09-02",
        query_return_code: int = 0,
    ) -> None:
        self._native_session_id = native_session_id
        self._api_version = api_version
        self._trading_day = trading_day
        self.query_return_code = query_return_code
        self.request_ids: list[int] = []
        self.close_count = 0

    @property
    def native_session_id(self) -> int:
        return self._native_session_id

    @property
    def api_version(self) -> str:
        return self._api_version

    @property
    def trading_day(self) -> str:
        return self._trading_day

    def query_unfinished_orders(self, native_request_id: int) -> int:
        self.request_ids.append(native_request_id)
        return self.query_return_code

    def close(self) -> None:
        self.close_count += 1


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session
        self.ingress: EmtQueryCallbackIngress | None = None
        self.observer_was_attached = False

    def open(self, *, callback_ingress: EmtQueryCallbackIngress) -> _Session:
        self.ingress = callback_ingress
        self.observer_was_attached = callback_ingress.snapshot().observer_attached
        return self.session


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


def _query() -> BrokerQuery:
    return BrokerQuery(
        request_id="domain-query-1",
        account_id="account-1",
        venue_id="EMT",
        adapter_instance_id="emt-sim-instance-1",
        query_kind="unfinished_orders",
        page_size=10,
    )


class EmtControlledSessionAdapterTests(unittest.TestCase):
    def _adapter(self, session: _Session):
        factory = _Factory(session)
        adapter = EmtControlledReadOnlyAdapter(
            _mapping(),
            session_factory=factory,
            expected_api_version="2.27.0",
            expected_trading_day=DAY,
            session_epoch=4,
        )
        return adapter, factory

    def test_observer_attaches_before_session_open_and_only_typed_callback_crosses_boundary(
        self,
    ) -> None:
        session = _Session()
        adapter, factory = self._adapter(session)
        observer = _Observer()
        adapter.start(observer)
        self.assertTrue(factory.observer_was_attached)
        self.assertTrue(adapter.snapshot().started)
        self.assertEqual(adapter.snapshot().api_version, "2.27.0.0")
        self.assertEqual(adapter.snapshot().trading_day, "2026-09-02")

        handle = adapter.query(_query())
        self.assertEqual(handle.request_id, "domain-query-1")
        self.assertEqual(session.request_ids, [1])
        ingress = factory.ingress
        assert ingress is not None
        event = ingress.on_query_order_terminal(
            native_request_id=1,
            native_session_id=71,
            error_id=0,
            is_last=True,
            received_monotonic_ns=11,
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.normalized_summary["callback_name"], "onQueryOrder")
        self.assertNotIn("domain-query-1", event.source_key)
        self.assertEqual(observer.events, [event])

        adapter.stop()
        self.assertEqual(session.close_count, 1)
        self.assertFalse(adapter.snapshot().started)
        self.assertFalse(ingress.snapshot().observer_attached)

    def test_version_or_trading_day_drift_closes_session_without_starting_adapter(self) -> None:
        for session in (
            _Session(api_version="2.28.0"),
            _Session(trading_day="2026-09-03"),
        ):
            with self.subTest(api_version=session.api_version, trading_day=session.trading_day):
                adapter, factory = self._adapter(session)
                with self.assertRaisesRegex(
                    EmtControlledSessionError, "does not match the profile"
                ):
                    adapter.start(_Observer())
                self.assertEqual(session.close_count, 1)
                self.assertFalse(adapter.snapshot().started)
                assert factory.ingress is not None
                self.assertFalse(factory.ingress.snapshot().observer_attached)

    def test_controlled_session_adapter_has_no_sdk_or_legacy_gateway_import(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "xuanling_tradecore"
            / "adapters"
            / "emt"
            / "session_adapter.py"
        )
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        forbidden = {"emt_gateway", "vnemttrader"}
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imports.append(node.module)
        self.assertTrue(forbidden.isdisjoint(name.split(".", maxsplit=1)[0] for name in imports))


if __name__ == "__main__":
    unittest.main()
