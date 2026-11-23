from __future__ import annotations

import ast
import dataclasses
import unittest
from pathlib import Path

from xuanling_tradecore.domain import (
    AccountLifecycle,
    AdmissionGate,
    AdmissionGateKind,
    AttributionBinding,
    AvailabilityObjective,
    BrokerCapabilities,
    BrokerEvent,
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    BundleTrustPolicy,
    CallbackGapEvidence,
    ConsumerAccountStatusEvent,
    ConsumerApiVersion,
    ConsumerEventKind,
    ConsumerPrincipal,
    ConsumerTrace,
    DomainValidationError,
    ExecutionInstruction,
    OrderOrigin,
    ReconciliationStatus,
    RecoveryPlan,
    RecoveryRun,
    ReleaseState,
    RiskSnapshot,
    TradingDayEpoch,
    VenueOrderIdentity,
)
from xuanling_tradecore.domain._validation import require_text

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_ROOT = ROOT / "xuanling_tradecore" / "domain"
SHA256 = "a" * 64


def _trace(*, account_id: str = "account-1") -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id=account_id,
        strategy_id="strategy-1",
        strategy_order_id="strategy-order-1",
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )


def _event_base() -> dict[str, object]:
    return {
        "consumer_event_id": "consumer-event-1",
        "schema_version": 1,
        "account_id": "account-1",
        "trading_day": TradingDayEpoch("2026-08-31"),
        "journal_id": "journal-1",
        "journal_lsn": 1,
        "consumer_trace": _trace(),
    }


class TradeCoreDomainTests(unittest.TestCase):
    def test_text_validation_keeps_unicode_whitespace_contract(self) -> None:
        for value in ("plain", "a b", "a\tb", "订单 1"):
            with self.subTest(value=value):
                self.assertEqual(value, require_text(value, "sample"))
        for value in ("", " \t ", "a\nb", "a\rb", "a\vb", "a\u00a0b", "a\u2028b"):
            with self.subTest(value=value):
                with self.assertRaises(DomainValidationError):
                    require_text(value, "sample")

    def test_principal_and_binding_values_are_immutable_and_do_not_repr_personal_data(self) -> None:
        principal = ConsumerPrincipal(
            consumer_id="consumer-1",
            login_name="employee.login",
            legal_name="张三",
        )
        self.assertNotIn("employee.login", repr(principal))
        self.assertNotIn("张三", repr(principal))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            principal.active = False  # type: ignore[reportAttributeAccessIssue] # Negative test proves the frozen model rejects mutation.

        binding = AttributionBinding(
            attribution_binding_id="attribution-1",
            consumer_account_binding_id="account-binding-1",
            consumer_strategy_binding_id="strategy-binding-1",
            consumer_id="consumer-1",
            consumer_account_id="consumer-account-1",
            strategy_id="strategy-1",
            binding_version=1,
        )
        self.assertEqual("strategy-1", binding.strategy_id)
        with self.assertRaises(DomainValidationError):
            AttributionBinding(
                attribution_binding_id="attribution-2",
                consumer_account_binding_id="account-binding-1",
                consumer_strategy_binding_id=None,
                consumer_id="consumer-1",
                consumer_account_id="consumer-account-1",
                strategy_id="strategy-1",
                binding_version=1,
            )

    def test_execution_instruction_preserves_account_lineage_and_freezes_parameters(self) -> None:
        instruction = ExecutionInstruction(
            execution_id="execution-1",
            oms_order_id="oms-order-1",
            account_id="account-1",
            trading_day=TradingDayEpoch("2026-08-31"),
            consumer_trace=_trace(),
            symbol="000010",
            market="SZ",
            side="BUY",
            price_type="LIMIT",
            business_type="CASH",
            quantity=100,
            protection_price_ticks=163,
            limit_price_ticks=162,
            execution_parameters={"algorithm": "direct"},
        )
        self.assertEqual("direct", instruction.execution_parameters["algorithm"])
        with self.assertRaises(TypeError):
            instruction.execution_parameters["algorithm"] = "changed"  # type: ignore[reportIndexIssue] # Negative test exercises the immutable mapping at runtime.
        with self.assertRaises(DomainValidationError):
            ExecutionInstruction(
                execution_id="execution-2",
                oms_order_id="oms-order-1",
                account_id="another-account",
                trading_day=TradingDayEpoch("2026-08-31"),
                consumer_trace=_trace(),
                symbol="000010",
                market="SZ",
                side="BUY",
                price_type="LIMIT",
                business_type="CASH",
                quantity=100,
                protection_price_ticks=163,
            )

    def test_account_state_is_fail_closed_until_reconciliation_is_validated(self) -> None:
        recovery_gate = AdmissionGate(
            gate_id="gate-1",
            kind=AdmissionGateKind.RECONCILIATION,
            version=1,
            source_reference="recovery-run-1",
            first_observed_monotonic_ns=10,
            last_observed_monotonic_ns=11,
        )
        recovering = ConsumerAccountStatusEvent(
            **_event_base(),
            event_kind=ConsumerEventKind.ACCOUNT_STATUS,
            lifecycle=AccountLifecycle.RECOVERING,
            reconciliation_status=ReconciliationStatus.RUNNING,
            gates=(recovery_gate,),
        )
        self.assertEqual(AccountLifecycle.RECOVERING, recovering.lifecycle)
        with self.assertRaises(DomainValidationError):
            ConsumerAccountStatusEvent(
                **_event_base(),
                event_kind=ConsumerEventKind.ACCOUNT_STATUS,
                lifecycle=AccountLifecycle.READY,
                reconciliation_status=ReconciliationStatus.VALIDATED,
                gates=(recovery_gate,),
            )
        with self.assertRaises(DomainValidationError):
            ConsumerAccountStatusEvent(
                **_event_base(),
                event_kind=ConsumerEventKind.ACCOUNT_STATUS,
                lifecycle=AccountLifecycle.DISARMED,
                reconciliation_status=ReconciliationStatus.REQUIRED,
            )

    def test_broker_event_requires_stable_identity_and_event_specific_keys(self) -> None:
        identity = VenueOrderIdentity(
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            adapter_client_scope="emt-client-7",
            client_order_ref="1001",
            venue_order_id="5001",
        )
        event = BrokerEvent(
            event_id="broker-event-1",
            schema_version=1,
            source_key_schema_version=1,
            source_key="source-key-1",
            event_kind=BrokerEventKind.TRADE,
            source_kind=BrokerSourceKind.CALLBACK,
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            account_id="account-1",
            trading_day=TradingDayEpoch("2026-08-31"),
            received_monotonic_ns=100,
            venue_order_identity=identity,
            trade_key="SZ:trade-1",
            filled_qty=100,
            leaves_qty=0,
            normalized_summary={"status": "FILLED"},
        )
        self.assertEqual("FILLED", event.normalized_summary["status"])
        with self.assertRaises(TypeError):
            event.normalized_summary["status"] = "REJECTED"  # type: ignore[reportIndexIssue] # Negative test exercises the immutable mapping at runtime.
        with self.assertRaises(DomainValidationError):
            BrokerEvent(
                event_id="broker-event-2",
                schema_version=1,
                source_key_schema_version=1,
                source_key="source-key-2",
                event_kind=BrokerEventKind.TRADE,
                source_kind=BrokerSourceKind.CALLBACK,
                venue_id="QMT",
                adapter_instance_id="emt-sim-v1",
                account_id="account-1",
                trading_day=TradingDayEpoch("2026-08-31"),
                received_monotonic_ns=100,
                venue_order_identity=identity,
            )

    def test_recovery_evidence_cannot_claim_complete_with_a_gap_or_unresolved_order(self) -> None:
        gap = CallbackGapEvidence(
            evidence_id="gap-1",
            account_id="account-1",
            producer_id="spi-1",
            first_rejected_sequence=9,
            last_rejected_sequence=10,
            rejected_count=2,
            observed_monotonic_ns=100,
        )
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="account-1",
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            trading_day=TradingDayEpoch("2026-08-31"),
            journal_id="journal-1",
            expected_journal_lsn=20,
            callback_gaps=(gap,),
        )
        self.assertEqual("recovery-1", plan.recovery_run_id)
        incomplete = BrokerRecoveryEvidence(
            evidence_id="evidence-1",
            recovery_run_id="recovery-1",
            account_id="account-1",
            venue_id="EMT",
            adapter_instance_id="emt-sim-v1",
            trading_day=TradingDayEpoch("2026-08-31"),
            callback_replay_complete=True,
            orders_complete=True,
            trades_complete=True,
            working_orders_complete=True,
            risk_snapshot_complete=True,
            negative_confirmation_complete=True,
            callback_gaps=(gap,),
        )
        self.assertFalse(incomplete.is_complete)
        with self.assertRaises(DomainValidationError):
            RecoveryRun(
                recovery_run_id="recovery-1",
                account_id="account-1",
                started_monotonic_ns=100,
                completed_monotonic_ns=None,
                reconciliation_status=ReconciliationStatus.VALIDATED,
                broker_recovery_evidence_id=None,
            )

    def test_versioned_policy_and_risk_values_reject_invalid_boundaries(self) -> None:
        self.assertTrue(ConsumerApiVersion(1, 2).is_compatible_with(ConsumerApiVersion(1, 1)))
        self.assertFalse(ConsumerApiVersion(1, 1).is_compatible_with(ConsumerApiVersion(2, 0)))
        policy = BundleTrustPolicy(
            policy_id="trust-policy-1",
            policy_epoch=1,
            trusted_signer_ids=frozenset({"signer-1"}),
            revoked_signer_ids=frozenset(),
            allowed_bundle_types=frozenset({"mapping", "capability"}),
        )
        self.assertIn("signer-1", policy.trusted_signer_ids)
        self.assertTrue(
            BrokerCapabilities(
                capability_profile_id="emt-capabilities-1",
                version=1,
                venue_id="EMT",
                supports_cash=True,
                supports_margin_buy=True,
                supports_short_sell=True,
                supports_pagination=True,
                supports_public_stream_replay=True,
                echoes_client_order_ref=True,
                has_stable_venue_order_id=True,
                has_stable_trade_id=True,
                supports_complete_recovery_queries=True,
                supports_negative_confirmation=True,
            ).supports_automatic_recovery_validation
        )
        snapshot = RiskSnapshot(
            risk_snapshot_id="risk-1",
            account_id="account-1",
            trading_day=TradingDayEpoch("2026-08-31"),
            rule_bundle_hash=SHA256,
            captured_monotonic_ns=10,
            expires_monotonic_ns=20,
            available_cash_minor=100_000,
            available_margin_minor=50_000,
            sellable_quantities={"SZ.000010": 100_000},
        )
        self.assertTrue(snapshot.is_fresh_at(19))
        self.assertFalse(snapshot.is_fresh_at(20))
        with self.assertRaises(DomainValidationError):
            ReleaseState(release_epoch=1, artifact_hash="not-a-hash", release_manifest_hash=SHA256)
        with self.assertRaises(DomainValidationError):
            AvailabilityObjective(
                objective_id="availability-1",
                version=1,
                physical_media_rpo_ms=1,
                local_recovery_budget_ms=20,
                broker_wait_budget_ms=30,
                recovery_e2e_budget_ms=49,
            )

    def test_domain_has_no_vendor_or_execution_layer_imports(self) -> None:
        forbidden_roots = {"emt_gateway", "vnemttrader", "qmt", "psycopg", "sqlite3"}
        for source in DOMAIN_ROOT.rglob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported_names = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported_names = (node.module,)
                else:
                    continue
                for imported_name in imported_names:
                    self.assertNotIn(
                        imported_name.split(".", maxsplit=1)[0],
                        forbidden_roots,
                        f"{source.relative_to(ROOT)} imports a forbidden dependency",
                    )


if __name__ == "__main__":
    unittest.main()
