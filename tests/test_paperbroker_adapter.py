"""Contract tests for the TradeCore-only PaperBroker connector boundary."""

from __future__ import annotations

import json
import re
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
from xuanling_tradecore.adapters.paper import (
    PaperBrokerAdapterError,
    PaperBrokerAdapterProfile,
    PaperBrokerCancelReceipt,
    PaperBrokerDependencyError,
    PaperBrokerDependencyManifest,
    PaperBrokerDeploymentMode,
    PaperBrokerEventConflictError,
    PaperBrokerHealth,
    PaperBrokerNativeEvent,
    PaperBrokerRuntimeIdentity,
    PaperBrokerSubmitReceipt,
    load_builtin_paperbroker_contract_fixture,
    load_paperbroker_dependency_manifest_json,
)
from xuanling_tradecore.domain import (
    BrokerEventKind,
    BrokerRecoveryEvidence,
    BrokerSourceKind,
    CancelRequest,
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    RecoveryPlan,
    TradingDayEpoch,
    VenueOrderIdentity,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
DAY = TradingDayEpoch("2026-09-04")


class _Observer(BrokerEventObserver):
    def __init__(self) -> None:
        self.events = []
        self.gaps = []

    def on_broker_event(self, event) -> None:
        self.events.append(event)

    def on_callback_gap(self, evidence) -> None:
        self.gaps.append(evidence)


class _Session:
    def __init__(self, identity: PaperBrokerRuntimeIdentity) -> None:
        self.identity = identity
        self.event_sink = None
        self.closed = False
        self.submissions = []

    def open(self, event_sink) -> PaperBrokerRuntimeIdentity:
        self.event_sink = event_sink
        self.closed = False
        return self.identity

    def close(self) -> None:
        self.closed = True

    def health(self) -> PaperBrokerHealth:
        return PaperBrokerHealth(True, "READY")

    def submit(self, request) -> PaperBrokerSubmitReceipt:
        self.submissions.append(request)
        return PaperBrokerSubmitReceipt(101, "paper-order-1")

    def cancel(self, instruction) -> PaperBrokerCancelReceipt:
        del instruction
        return PaperBrokerCancelReceipt(102, "paper-cancel-1")

    def query(self, request) -> QueryHandle:
        return QueryHandle(request.request_id, "paperbroker-adapter-v1", "paperbroker-fixture-v1")

    def begin_recovery(self, plan) -> RecoveryHandle:
        return RecoveryHandle(
            plan.recovery_run_id, "paperbroker-adapter-v1", "paperbroker-fixture-v1"
        )

    def collect_recovery_evidence(self, handle) -> BrokerRecoveryEvidence:
        return BrokerRecoveryEvidence(
            evidence_id="paper-evidence-1",
            recovery_run_id=handle.recovery_run_id,
            account_id="paperbroker-fixture-account-1",
            venue_id="PAPER",
            adapter_instance_id="paperbroker-fixture-v1",
            trading_day=DAY,
            callback_replay_complete=True,
            orders_complete=True,
            trades_complete=True,
            working_orders_complete=True,
            risk_snapshot_complete=True,
            negative_confirmation_complete=True,
        )

    def emit(self, event: PaperBrokerNativeEvent) -> None:
        assert self.event_sink is not None
        self.event_sink(event)


class _ReplayOnOpenSession(_Session):
    def open(self, event_sink) -> PaperBrokerRuntimeIdentity:
        identity = super().open(event_sink)
        self.emit(
            PaperBrokerNativeEvent(
                external_event_id="replayed-during-open",
                event_kind=BrokerEventKind.ORDER,
                source_kind=BrokerSourceKind.RECOVERY_REPLAY,
                account_id="paperbroker-fixture-account-1",
                trading_day=DAY,
                received_monotonic_ns=100,
                venue_order_id="paper-order-before-ready",
            )
        )
        return identity


def _dependency() -> PaperBrokerDependencyManifest:
    return PaperBrokerDependencyManifest(
        manifest_id="paperbroker-isolated-contract-fixture-release",
        version=1,
        source_reference="isolated-contract-fixture",
        project_version="fixture-v1",
        artifact_sha256=SHA_A,
        license_identifier="TEST-ONLY",
        api_schema_reference="contracts/adapters/paperbroker/v1/isolated-fixture.json",
        api_schema_sha256=SHA_B,
        configuration_sha256="c" * 64,
        deployment_mode=PaperBrokerDeploymentMode.REMOTE_SERVICE,
        startup_owner="test-harness",
        stop_owner="test-harness",
        upgrade_policy="replace only with a new approved dependency manifest",
    )


def _runtime(manifest):
    return RuntimeVenueProfile(
        fingerprint=manifest.fingerprint,
        capabilities=manifest.capabilities,
        account_capabilities=AccountTradingCapabilities(
            "paperbroker-fixture-account-1", True, False, False
        ),
        scope_ownership=manifest.scope_ownership,
        route_token_binding=manifest.route_token_binding,
        trusted_scope_signer_ids=frozenset({"tradecore-release-signer-1"}),
        route_token_probe_succeeded=True,
    )


def _external_identity() -> PaperBrokerRuntimeIdentity:
    return PaperBrokerRuntimeIdentity(
        project_version="fixture-v1",
        artifact_sha256=SHA_A,
        api_schema_sha256=SHA_B,
        configuration_sha256="c" * 64,
        deployment_mode=PaperBrokerDeploymentMode.REMOTE_SERVICE,
    )


def _trace() -> ConsumerTrace:
    return ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="paperbroker-fixture-account-1",
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
        account_id="paperbroker-fixture-account-1",
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


class PaperBrokerAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        manifest = load_builtin_paperbroker_contract_fixture()
        self.mapping = PaperBrokerAdapterProfile(manifest, _dependency()).activate(
            runtime=_runtime(manifest), activated_at_unix_s=0
        )

    def _adapter(self, session: _Session, *, capacity: int = 2):
        return PaperBrokerAdapterProfile.create_adapter(
            self.mapping,
            dependency=_dependency(),
            session=session,
            client_order_ref_factory=lambda instruction: "P1ABC12345",
            ingress_capacity=capacity,
        )

    def test_adapter_maps_calls_and_normalizes_idempotent_events(self) -> None:
        session = _Session(_external_identity())
        adapter = self._adapter(session)
        observer = _Observer()
        self.assertIsInstance(adapter, BrokerAdapter)
        adapter.start(observer)
        accepted = adapter.submit(_instruction())
        self.assertEqual(accepted.venue_order_identity.venue_order_id, "paper-order-1")
        self.assertEqual(session.submissions[0].mapped_order.market, 1)
        event = PaperBrokerNativeEvent(
            external_event_id="event-1",
            event_kind=BrokerEventKind.ORDER,
            source_kind=BrokerSourceKind.CALLBACK,
            account_id="paperbroker-fixture-account-1",
            trading_day=DAY,
            received_monotonic_ns=110,
            client_order_ref="P1ABC12345",
            venue_order_id="paper-order-1",
            execution_id="execution-1",
            leaves_qty=100,
        )
        session.emit(event)
        session.emit(event)
        self.assertEqual(adapter.drain_ingress(), 1)
        self.assertEqual(observer.events[0].source_key_schema_version, 1)
        self.assertEqual(observer.events[0].venue_order_identity.client_order_ref, "P1ABC12345")
        self.assertEqual(adapter.snapshot().ingress_depth, 0)

    def test_observer_is_attached_before_open_can_replay_an_event(self) -> None:
        session = _ReplayOnOpenSession(_external_identity())
        adapter = self._adapter(session)
        observer = _Observer()
        adapter.start(observer)
        self.assertEqual(adapter.drain_ingress(), 1)
        self.assertEqual(observer.events[0].source_kind, BrokerSourceKind.RECOVERY_REPLAY)

    def test_duplicate_event_id_with_different_content_fails_closed(self) -> None:
        session = _Session(_external_identity())
        adapter = self._adapter(session)
        adapter.start(_Observer())
        session.emit(
            PaperBrokerNativeEvent(
                external_event_id="event-1",
                event_kind=BrokerEventKind.ORDER,
                source_kind=BrokerSourceKind.CALLBACK,
                account_id="paperbroker-fixture-account-1",
                trading_day=DAY,
                received_monotonic_ns=110,
                venue_order_id="paper-order-1",
                leaves_qty=100,
            )
        )
        with self.assertRaises(PaperBrokerEventConflictError):
            session.emit(
                PaperBrokerNativeEvent(
                    external_event_id="event-1",
                    event_kind=BrokerEventKind.ORDER,
                    source_kind=BrokerSourceKind.CALLBACK,
                    account_id="paperbroker-fixture-account-1",
                    trading_day=DAY,
                    received_monotonic_ns=111,
                    venue_order_id="paper-order-1",
                    leaves_qty=99,
                )
            )

    def test_ingress_overflow_creates_callback_gap_evidence(self) -> None:
        session = _Session(_external_identity())
        adapter = self._adapter(session, capacity=1)
        observer = _Observer()
        adapter.start(observer)
        for event_id in ("event-1", "event-2"):
            session.emit(
                PaperBrokerNativeEvent(
                    external_event_id=event_id,
                    event_kind=BrokerEventKind.ORDER,
                    source_kind=BrokerSourceKind.CALLBACK,
                    account_id="paperbroker-fixture-account-1",
                    trading_day=DAY,
                    received_monotonic_ns=110,
                    venue_order_id=event_id,
                )
            )
        self.assertEqual(len(observer.gaps), 1)
        self.assertEqual(adapter.snapshot().rejected_event_count, 1)

    def test_foreign_event_and_invalid_route_reference_fail_closed(self) -> None:
        session = _Session(_external_identity())
        adapter = self._adapter(session)
        adapter.start(_Observer())
        with self.assertRaisesRegex(PaperBrokerAdapterError, "another profile"):
            session.emit(
                PaperBrokerNativeEvent(
                    external_event_id="foreign-event",
                    event_kind=BrokerEventKind.ORDER,
                    source_kind=BrokerSourceKind.CALLBACK,
                    account_id="another-account",
                    trading_day=DAY,
                    received_monotonic_ns=110,
                    venue_order_id="paper-order-1",
                )
            )
        with self.assertRaisesRegex(PaperBrokerAdapterError, "route-token binding"):
            session.emit(
                PaperBrokerNativeEvent(
                    external_event_id="bad-route-ref",
                    event_kind=BrokerEventKind.ORDER,
                    source_kind=BrokerSourceKind.CALLBACK,
                    account_id="paperbroker-fixture-account-1",
                    trading_day=DAY,
                    received_monotonic_ns=110,
                    client_order_ref="not-approved",
                )
            )

    def test_dependency_drift_prevents_start_and_closes_session(self) -> None:
        bad_identity = PaperBrokerRuntimeIdentity(
            project_version="fixture-v2",
            artifact_sha256=SHA_A,
            api_schema_sha256=SHA_B,
            configuration_sha256="c" * 64,
            deployment_mode=PaperBrokerDeploymentMode.REMOTE_SERVICE,
        )
        session = _Session(bad_identity)
        with self.assertRaises(PaperBrokerDependencyError):
            self._adapter(session).start(_Observer())
        self.assertTrue(session.closed)

    def test_dependency_manifest_loader_is_exact_and_payload_free(self) -> None:
        dependency = _dependency()
        payload = {
            "manifest_id": dependency.manifest_id,
            "version": dependency.version,
            "source_reference": dependency.source_reference,
            "project_version": dependency.project_version,
            "artifact_sha256": dependency.artifact_sha256,
            "license_identifier": dependency.license_identifier,
            "api_schema_reference": dependency.api_schema_reference,
            "api_schema_sha256": dependency.api_schema_sha256,
            "configuration_sha256": dependency.configuration_sha256,
            "deployment_mode": dependency.deployment_mode.value,
            "startup_owner": dependency.startup_owner,
            "stop_owner": dependency.stop_owner,
            "upgrade_policy": dependency.upgrade_policy,
        }
        loaded = load_paperbroker_dependency_manifest_json(json.dumps(payload))
        self.assertEqual(loaded.manifest_hash, dependency.manifest_hash)
        payload["unexpected"] = "reject"
        with self.assertRaisesRegex(PaperBrokerDependencyError, "fields differ"):
            load_paperbroker_dependency_manifest_json(json.dumps(payload))

    def test_cancel_query_and_recovery_preserve_common_adapter_handles(self) -> None:
        session = _Session(_external_identity())
        adapter = self._adapter(session)
        observer = _Observer()
        adapter.start(observer)
        cancel = CancelRequest(
            cancel_request_id="cancel-1",
            oms_order_id="oms-1",
            execution_id="execution-1",
            account_id="paperbroker-fixture-account-1",
            consumer_trace=_trace(),
            reason="operator request",
            requested_monotonic_ns=1,
        )
        accepted = adapter.cancel(
            CancelExecutionInstruction(
                cancel_request=cancel,
                venue_order_identity=VenueOrderIdentity(
                    venue_id="PAPER",
                    adapter_instance_id="paperbroker-fixture-v1",
                    adapter_client_scope="paperbroker-fixture-scope-1",
                    venue_order_id="paper-order-1",
                ),
            )
        )
        self.assertEqual(accepted.venue_order_identity.venue_cancel_id, "paper-cancel-1")
        self.assertEqual(
            adapter.query(
                BrokerQuery(
                    "query-1",
                    "paperbroker-fixture-account-1",
                    "PAPER",
                    "paperbroker-fixture-v1",
                    "orders",
                    10,
                )
            ).request_id,
            "query-1",
        )
        plan = RecoveryPlan(
            recovery_run_id="recovery-1",
            account_id="paperbroker-fixture-account-1",
            venue_id="PAPER",
            adapter_instance_id="paperbroker-fixture-v1",
            trading_day=DAY,
            journal_id="journal-1",
            expected_journal_lsn=0,
        )
        evidence = adapter.collect_recovery_evidence(adapter.begin_recovery(plan, observer))
        self.assertTrue(evidence.is_complete)

    def test_profile_rejects_another_venue(self) -> None:
        manifest = load_builtin_paperbroker_contract_fixture()
        with self.assertRaises(ValueError):
            PaperBrokerAdapterProfile(manifest, _dependency()).activate(
                runtime=RuntimeVenueProfile(
                    fingerprint=manifest.fingerprint,
                    capabilities=manifest.capabilities,
                    account_capabilities=AccountTradingCapabilities("another", True, False, False),
                ),
                activated_at_unix_s=0,
            )

    def test_connector_package_does_not_embed_a_paper_execution_engine(self) -> None:
        package_root = Path(__file__).parents[1] / "xuanling_tradecore" / "adapters" / "paper"
        source = "\n".join(path.read_text(encoding="utf-8") for path in package_root.rglob("*.py"))
        self.assertNotIn("import tradingagents", source.lower())
        self.assertNotIn("from tradingagents", source.lower())
        self.assertIsNone(
            re.search(
                r"class\\s+(?:OrderBook|MatchingEngine|MarketData|TradingCalendar|FeeModel|SlippageModel)",
                source,
            )
        )


if __name__ == "__main__":
    unittest.main()
