"""Acceptance tests for EMT's no-login, no-mutation staging Adapter profile."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerAdapter,
    BrokerEventObserver,
    BrokerQuery,
    CancelExecutionInstruction,
    QueryHandle,
    RecoveryHandle,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerAdapter,
    EmtBrokerAdapterError,
    EmtCancelReceipt,
    EmtCancelRequest,
    EmtMutationPort,
    EmtMutationUnavailableError,
    EmtReadOnlyPort,
    EmtSubmitReceipt,
    EmtSubmitRequest,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.adapters.qmt import load_builtin_qmt_fake_manifest
from xuanling_tradecore.domain import (
    AccountLifecycle,
    BrokerRecoveryEvidence,
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


class _Port(EmtReadOnlyPort):
    def __init__(self) -> None:
        self.query_count = 0
        self.recovery_count = 0
        self._last_plan: RecoveryPlan | None = None

    def query(self, request: BrokerQuery) -> QueryHandle:
        self.query_count += 1
        return QueryHandle(request.request_id, "emt-gateway-p0", "emt-sim-instance-1")

    def begin_recovery(self, plan: RecoveryPlan) -> RecoveryHandle:
        self.recovery_count += 1
        self._last_plan = plan
        return RecoveryHandle(plan.recovery_run_id, "emt-gateway-p0", "emt-sim-instance-1")

    def collect_recovery_evidence(self, handle: RecoveryHandle) -> BrokerRecoveryEvidence:
        plan = self._last_plan
        assert plan is not None
        assert handle.recovery_run_id == plan.recovery_run_id
        return BrokerRecoveryEvidence(
            evidence_id=f"emt-staging-evidence:{plan.recovery_run_id}",
            recovery_run_id=plan.recovery_run_id,
            account_id=plan.account_id,
            venue_id=plan.venue_id,
            adapter_instance_id=plan.adapter_instance_id,
            trading_day=plan.trading_day,
            callback_replay_complete=False,
            orders_complete=False,
            trades_complete=False,
            working_orders_complete=False,
            risk_snapshot_complete=False,
            negative_confirmation_complete=False,
            callback_gaps=plan.callback_gaps,
        )


class _BadHandlePort(_Port):
    def query(self, request: BrokerQuery) -> QueryHandle:
        del request
        return QueryHandle("another-query", "emt-gateway-p0", "emt-sim-instance-1")


class _MutationPort(EmtMutationPort):
    def __init__(self) -> None:
        self.submits: list[EmtSubmitRequest] = []
        self.cancels: list[EmtCancelRequest] = []

    def submit(self, request: EmtSubmitRequest) -> EmtSubmitReceipt:
        self.submits.append(request)
        return EmtSubmitReceipt(order_emt_id=91, accepted_monotonic_ns=101)

    def cancel(self, request: EmtCancelRequest) -> EmtCancelReceipt:
        self.cancels.append(request)
        return EmtCancelReceipt(order_cancel_emt_id=92, accepted_monotonic_ns=102)


def _runtime(manifest):
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities("account-1", True, True, True),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )


def _trace() -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="account-1",
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
        account_id="account-1",
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
    return CancelExecutionInstruction(
        cancel_request=CancelRequest(
            cancel_request_id="cancel-1",
            oms_order_id="oms-1",
            execution_id="execution-1",
            account_id="account-1",
            consumer_trace=_trace(),
            reason="operator request",
            requested_monotonic_ns=1,
        ),
        venue_order_identity=VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id="emt-sim-instance-1",
            adapter_client_scope="7",
            client_order_ref="1",
        ),
    )


def _recovery_profile() -> RecoveryEvidenceProfile:
    return RecoveryEvidenceProfile(
        profile_id="emt-staging-recovery-v1",
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
        binding_id="emt-staging-binding-1",
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


class EmtAdapterProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load_builtin_emt_legacy_sim_manifest()
        self.profile = EmtAdapterProfile(self.manifest)
        self.mapping = self.profile.activate(runtime=_runtime(self.manifest), activated_at_unix_s=0)

    def test_profile_is_emt_only_and_retains_the_versioned_mapping(self) -> None:
        self.assertEqual(self.mapping.manifest.venue_id, "EMT")
        self.assertEqual(
            self.mapping.map_execution(_instruction()).identity.client_order_ref_field,
            "order_client_id",
        )
        with self.assertRaisesRegex(ValueError, "EMT manifest"):
            EmtAdapterProfile(load_builtin_qmt_fake_manifest())

    def test_adapter_delegates_only_read_operations_and_rejects_mutations(self) -> None:
        port = _Port()
        adapter = EmtBrokerAdapter(self.mapping, read_only_port=port)
        observer = _Observer()
        self.assertIsInstance(adapter, BrokerAdapter)
        with self.assertRaisesRegex(EmtBrokerAdapterError, "attached observer"):
            adapter.query(
                BrokerQuery("query-0", "account-1", "EMT", "emt-sim-instance-1", "orders", 10)
            )
        adapter.start(observer)
        query = adapter.query(
            BrokerQuery("query-1", "account-1", "EMT", "emt-sim-instance-1", "orders", 10)
        )
        self.assertEqual(query.request_id, "query-1")
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="account-1",
            venue_id="EMT",
            adapter_instance_id="emt-sim-instance-1",
            trading_day=DAY,
            journal_id="journal-1",
            expected_journal_lsn=0,
        )
        evidence = adapter.collect_recovery_evidence(adapter.begin_recovery(plan, observer))
        self.assertFalse(evidence.orders_complete)
        with self.assertRaisesRegex(EmtMutationUnavailableError, "submit is unavailable"):
            adapter.submit(_instruction())
        with self.assertRaisesRegex(EmtMutationUnavailableError, "cancel is unavailable"):
            adapter.cancel(_cancel_instruction())
        self.assertEqual((port.query_count, port.recovery_count), (1, 1))
        self.assertEqual(
            (adapter.snapshot().submit_attempts, adapter.snapshot().cancel_attempts), (1, 1)
        )

    def test_read_only_port_cannot_return_a_handle_from_another_request(self) -> None:
        adapter = EmtBrokerAdapter(self.mapping, read_only_port=_BadHandlePort())
        adapter.start(_Observer())
        with self.assertRaisesRegex(EmtBrokerAdapterError, "invalid handle"):
            adapter.query(
                BrokerQuery("query-1", "account-1", "EMT", "emt-sim-instance-1", "orders", 10)
            )

    def test_controlled_mutation_port_maps_and_fences_submit_and_cancel(self) -> None:
        port = _Port()
        mutation = _MutationPort()
        submit_stages: list[str] = []
        adapter = EmtBrokerAdapter(
            self.mapping,
            read_only_port=port,
            mutation_port=mutation,
            client_order_ref_factory=lambda instruction: "17",
            submit_stage_observer=lambda _instruction, stage, _observed_ns: submit_stages.append(
                stage
            ),
        )
        adapter.start(_Observer())
        accepted = adapter.submit(_instruction())
        self.assertEqual(accepted.accepted_monotonic_ns, 101)
        assert accepted.venue_order_identity is not None
        self.assertEqual(accepted.venue_order_identity.client_order_ref, "17")
        self.assertEqual(accepted.venue_order_identity.venue_order_id, "91")
        self.assertEqual(mutation.submits[0].order_client_id, 17)
        self.assertEqual(mutation.submits[0].mapped_order.market, 1)
        self.assertEqual(mutation.submits[0].mapped_order.business_type, 0)
        self.assertEqual(
            [
                "C1Q_AFTER_ADAPTER_REQUIREMENTS",
                "C1R_AFTER_CLIENT_REFERENCE",
                "C1S_AFTER_MAPPED_REQUEST",
            ],
            submit_stages,
        )

        cancel = CancelExecutionInstruction(
            cancel_request=_cancel_instruction().cancel_request,
            venue_order_identity=VenueOrderIdentity(
                venue_id="EMT",
                adapter_instance_id="emt-sim-instance-1",
                adapter_client_scope="7",
                client_order_ref="17",
                venue_order_id="91",
            ),
        )
        canceled = adapter.cancel(cancel)
        assert canceled.venue_order_identity is not None
        self.assertEqual(canceled.venue_order_identity.venue_cancel_id, "92")
        self.assertEqual(mutation.cancels[0].order_emt_id, 91)

    def test_prepared_submit_has_no_side_effect_until_dispatch_and_is_activation_fenced(
        self,
    ) -> None:
        port = _Port()
        mutation = _MutationPort()
        adapter = EmtBrokerAdapter(
            self.mapping,
            read_only_port=port,
            mutation_port=mutation,
            client_order_ref_factory=lambda instruction: "17",
        )
        observer = _Observer()
        adapter.start(observer)
        prepared = adapter.prepare_submit(_instruction())
        self.assertEqual([], mutation.submits)
        self.assertEqual(0, adapter.snapshot().submit_attempts)

        accepted = adapter.submit_prepared(prepared)
        self.assertEqual("91", accepted.venue_order_identity.venue_order_id)
        self.assertEqual(1, len(mutation.submits))
        self.assertEqual(1, adapter.snapshot().submit_attempts)

        adapter.stop()
        adapter.start(observer)
        with self.assertRaisesRegex(EmtBrokerAdapterError, "another adapter activation"):
            adapter.submit_prepared(prepared)
        self.assertEqual(1, len(mutation.submits))

        other_mutation = _MutationPort()
        other = EmtBrokerAdapter(
            self.mapping,
            read_only_port=_Port(),
            mutation_port=other_mutation,
            client_order_ref_factory=lambda instruction: "17",
        )
        other.start(_Observer())
        with self.assertRaisesRegex(EmtBrokerAdapterError, "another adapter activation"):
            other.submit_prepared(adapter.prepare_submit(_instruction()))
        self.assertEqual([], other_mutation.submits)

    def test_mutation_port_rejects_noncanonical_client_ref_and_foreign_cancel_before_call(
        self,
    ) -> None:
        port = _Port()
        mutation = _MutationPort()
        adapter = EmtBrokerAdapter(
            self.mapping,
            read_only_port=port,
            mutation_port=mutation,
            client_order_ref_factory=lambda instruction: "001",
        )
        adapter.start(_Observer())
        with self.assertRaisesRegex(EmtBrokerAdapterError, "canonical decimal"):
            adapter.submit(_instruction())
        self.assertEqual(mutation.submits, [])
        with self.assertRaisesRegex(EmtBrokerAdapterError, "another client scope"):
            adapter.cancel(
                CancelExecutionInstruction(
                    cancel_request=_cancel_instruction().cancel_request,
                    venue_order_identity=VenueOrderIdentity(
                        venue_id="EMT",
                        adapter_instance_id="emt-sim-instance-1",
                        adapter_client_scope="another-scope",
                        venue_order_id="91",
                    ),
                )
            )
        self.assertEqual(mutation.cancels, [])

    def test_registry_exposes_staging_profile_as_read_only_without_forwarding_mutations(
        self,
    ) -> None:
        port = _Port()
        declaration = self.profile.registry_declaration(
            mapping=self.mapping,
            recovery_profile=_recovery_profile(),
            read_only_port=port,
        )
        registry = VenueAdapterRegistry((declaration,))
        selection = registry.select(
            _binding(declaration),
            account_lifecycle=AccountLifecycle.DISARMED,
            adapter_session_active=False,
            evaluated_wall_ns=1,
        )
        self.assertEqual(selection.mode, AdapterProfileMode.READ_ONLY_RECONCILIATION)
        self.assertIn("CAPABILITY_RECOVERY_INCOMPLETE", selection.reason_codes)
        exposed = registry.create_selected_adapter(selection)
        self.assertIsInstance(exposed, ReadOnlyBrokerAdapter)
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects submit"):
            exposed.submit(None)  # type: ignore[reportArgumentType]  # Wrapper rejects first.
        with self.assertRaisesRegex(VenueAdapterRegistryError, "rejects cancel"):
            exposed.cancel(None)  # type: ignore[reportArgumentType]  # Wrapper rejects first.
        self.assertEqual(port.query_count, 0)
        self.assertEqual(port.recovery_count, 0)

    def test_emt_staging_adapter_has_no_sdk_or_legacy_gateway_import(self) -> None:
        root = Path(__file__).parents[1] / "xuanling_tradecore" / "adapters" / "emt"
        forbidden = {"emt_gateway", "vnemttrader"}
        for source in root.rglob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    names = (node.module,)
                else:
                    continue
                self.assertTrue(
                    forbidden.isdisjoint(name.split(".", maxsplit=1)[0] for name in names),
                    f"{source} imports a forbidden EMT runtime dependency",
                )


if __name__ == "__main__":
    unittest.main()
