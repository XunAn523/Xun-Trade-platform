"""Acceptance tests for the deliberately non-production QMT fake profile."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerAdapter,
    BrokerEventObserver,
    BrokerQuery,
    CancelExecutionInstruction,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerAdapter,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.adapters.mapping import MappingManifestError
from xuanling_tradecore.adapters.qmt import (
    FakeQmtBrokerAdapter,
    FakeQmtBrokerAdapterError,
    QmtAdapterProfile,
    QmtBrokerAdapter,
    QmtMappingRegistry,
    load_builtin_qmt_fake_manifest,
    qmt_route_token_binding_v1,
)
from xuanling_tradecore.domain import (
    AccountLifecycle,
    CancelRequest,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    RecoveryEvidenceProfile,
    RecoveryPlan,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.ems import (
    AccountExecutionBinding,
    AccountFenceScope,
    AdapterProfileMode,
    ReadOnlyBrokerAdapter,
    VenueAdapterRegistry,
    VenueAdapterRegistryError,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
DAY = TradingDayEpoch("2026-09-02")


class _Observer(BrokerEventObserver):
    def on_broker_event(self, event) -> None:
        del event

    def on_callback_gap(self, evidence) -> None:
        del evidence


def _runtime(manifest):
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("qmt-fake-account-1", True, False, False),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )


def _trace() -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="qmt-fake-account-1",
        strategy_id=None,
        strategy_order_id=None,
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.MANUAL,
    )


def _instruction() -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id="execution-1",
        oms_order_id="oms-1",
        account_id="qmt-fake-account-1",
        trading_day=DAY,
        consumer_trace=_trace(),
        symbol="000001",
        market="SZ",
        side="BUY",
        price_type="LIMIT",
        business_type="CASH",
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )


def _cancel_instruction() -> CancelExecutionInstruction:
    request = CancelRequest(
        cancel_request_id="cancel-1",
        oms_order_id="oms-1",
        execution_id="execution-1",
        account_id="qmt-fake-account-1",
        consumer_trace=_trace(),
        reason="operator request",
        requested_monotonic_ns=1,
    )
    return CancelExecutionInstruction(
        cancel_request=request,
        venue_order_identity=VenueOrderIdentity(
            venue_id="QMT",
            adapter_instance_id="qmt-fake-v1",
            adapter_client_scope="qmt-fake-scope-1",
            client_order_ref="Q1AAAAAAAAAAAAAAAAAAAAAAAAAA0000",
        ),
    )


def _recovery_profile() -> RecoveryEvidenceProfile:
    return RecoveryEvidenceProfile(
        profile_id="qmt-fake-recovery-v1",
        version=1,
        profile_hash=SHA_B,
        required_query_kinds=frozenset(
            {"orders", "trades", "working_orders", "assets", "positions"}
        ),
        requires_public_replay=False,
        requires_callback_gap_coverage=True,
        requires_route_token_echo=True,
        requires_negative_confirmation=False,
        requires_stable_double_scan_without_as_of=False,
        max_snapshot_age_ns=1_000,
    )


def _binding(declaration) -> AccountExecutionBinding:
    ownership = declaration.mapping.manifest.scope_ownership
    assert ownership is not None
    return AccountExecutionBinding(
        binding_id="qmt-fake-binding-1",
        binding_version=1,
        account_id=ownership.account_id,
        venue_id=ownership.venue_id,
        adapter_instance_id=ownership.adapter_instance_id,
        adapter_client_scope=ownership.adapter_client_scope,
        ownership_scope_hash=AccountFenceScope(
            ownership.account_id,
            ownership.venue_id,
            ownership.adapter_client_scope,
        ).scope_hash,
        mapping_manifest_hash=declaration.key.mapping_manifest_hash,
        capability_manifest_hash=declaration.key.capability_manifest_hash,
        journal_plan_hash=SHA_A,
        journal_routing_epoch=1,
        journal_shard_id=0,
    )


class QmtFakeProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load_builtin_qmt_fake_manifest()
        self.profile = QmtAdapterProfile(self.manifest)
        self.mapping = self.profile.activate(runtime=_runtime(self.manifest), activated_at_unix_s=0)

    def test_manifest_is_qmt_only_cash_only_and_uses_the_frozen_remark_contract(self) -> None:
        self.assertEqual(self.manifest.venue_id, "QMT")
        self.assertEqual(self.manifest.route_token_binding, qmt_route_token_binding_v1())
        self.assertEqual(self.mapping.map_execution(_instruction()).side, 1)
        self.assertFalse(self.manifest.capabilities.supports_automatic_recovery_validation)
        self.assertFalse(self.manifest.recovery.automatic_recovery_eligible)
        with self.assertRaisesRegex(MappingManifestError, "only QMT"):
            QmtMappingRegistry((load_builtin_emt_legacy_sim_manifest(),))

    def test_fake_adapter_is_structural_contract_but_never_mutates_a_broker(self) -> None:
        adapter = FakeQmtBrokerAdapter(self.mapping)
        observer = _Observer()
        self.assertIsInstance(adapter, BrokerAdapter)
        adapter.start(observer)
        query = adapter.query(
            BrokerQuery("query-1", "qmt-fake-account-1", "QMT", "qmt-fake-v1", "orders", 10)
        )
        self.assertEqual(query.adapter_id, "qmt-contract-fake-v1")
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="qmt-fake-account-1",
            venue_id="QMT",
            adapter_instance_id="qmt-fake-v1",
            trading_day=DAY,
            journal_id="journal-1",
            expected_journal_lsn=0,
        )
        evidence = adapter.collect_recovery_evidence(adapter.begin_recovery(plan, observer))
        self.assertFalse(evidence.orders_complete)
        self.assertFalse(evidence.negative_confirmation_complete)
        with self.assertRaisesRegex(FakeQmtBrokerAdapterError, "never submits an order"):
            adapter.submit(_instruction())
        with self.assertRaisesRegex(FakeQmtBrokerAdapterError, "never submits a cancel"):
            adapter.cancel(_cancel_instruction())
        self.assertEqual(adapter.snapshot().submit_attempts, 1)
        self.assertEqual(adapter.snapshot().cancel_attempts, 1)
        self.assertEqual(adapter.snapshot().query_attempts, 1)
        self.assertEqual(adapter.snapshot().recovery_attempts, 1)

    def test_registry_keeps_fake_profile_read_only_and_does_not_call_its_mutators(self) -> None:
        declaration = self.profile.registry_declaration(
            mapping=self.mapping,
            recovery_profile=_recovery_profile(),
        )
        registry = VenueAdapterRegistry((declaration,))
        eligibility = registry.evaluate(declaration.key, evaluated_wall_ns=1)
        self.assertFalse(eligibility.eligible)
        self.assertIn("CAPABILITY_RECOVERY_INCOMPLETE", eligibility.reason_codes)
        self.assertIn("RECOVERY_CERTIFICATE_FACTORY_UNAVAILABLE", eligibility.reason_codes)
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertEqual(selection.mode, AdapterProfileMode.READ_ONLY_RECONCILIATION)
        exposed = registry.create_selected_adapter(selection)
        self.assertIsInstance(exposed, ReadOnlyBrokerAdapter)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects submit"):
            exposed.submit(None)  # type: ignore[reportArgumentType]  # Wrapper rejects first.
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects cancel"):
            exposed.cancel(None)  # type: ignore[reportArgumentType]  # Wrapper rejects first.
        self.assertEqual(
            exposed.query(
                BrokerQuery("query-2", "qmt-fake-account-1", "QMT", "qmt-fake-v1", "orders", 10)
            ).request_id,
            "query-2",
        )
        wrapped = exposed.wrapped_adapter
        assert isinstance(wrapped, FakeQmtBrokerAdapter)
        self.assertEqual(
            (wrapped.snapshot().submit_attempts, wrapped.snapshot().cancel_attempts), (0, 0)
        )
        with self.assertRaisesRegex(VenueAdapterRegistryError, "factory is unavailable"):
            registry.create_recovery_certificate(selection, trading_day=DAY)

    def test_profile_never_imports_a_vendor_qmt_sdk(self) -> None:
        package_root = Path(__file__).parents[1] / "xuanling_tradecore" / "adapters" / "qmt"
        source = "\n".join(path.read_text(encoding="utf-8") for path in package_root.rglob("*.py"))
        self.assertNotIn("xtquant", source.lower())
        self.assertNotIn("xttrader", source.lower())

    def test_real_emt_and_qmt_bridge_are_peers_in_one_registry(self) -> None:
        bridge_factory = Mock(return_value=Mock())
        qmt = self.profile.bridge_registry_declaration(
            mapping=self.mapping,
            recovery_profile=_recovery_profile(),
            bridge_factory=bridge_factory,
            client_order_ref_factory=lambda _: "unused-disarmed-reference",
        )
        manifest = load_builtin_emt_legacy_sim_manifest()
        emt_profile = EmtAdapterProfile(manifest)
        runtime = RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities(
                manifest.scope_ownership.account_id, True, True, True
            ),
            scope_ownership=manifest.scope_ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
            route_token_probe_succeeded=True,
        )
        emt = emt_profile.registry_declaration(
            mapping=emt_profile.activate(runtime=runtime, activated_at_unix_s=0),
            recovery_profile=_recovery_profile(),
            read_only_port=Mock(),
        )
        registry = VenueAdapterRegistry((emt, qmt))
        bridge_factory.assert_not_called()
        for declaration, expected in ((emt, EmtBrokerAdapter), (qmt, QmtBrokerAdapter)):
            selection = registry.select(
                _binding(declaration),
                account_lifecycle=AccountLifecycle.DISARMED,
                adapter_session_active=False,
                evaluated_wall_ns=1,
            )
            exposed = registry.create_selected_adapter(selection)
            self.assertIsInstance(exposed, BrokerAdapter)
            self.assertIsInstance(exposed, ReadOnlyBrokerAdapter)
            self.assertIsInstance(exposed.wrapped_adapter, expected)
            with self.assertRaises(VenueAdapterRegistryError):
                exposed.submit(_instruction())
        bridge_factory.assert_called_once()
        bridge_factory.return_value.open.assert_not_called()


if __name__ == "__main__":
    unittest.main()
