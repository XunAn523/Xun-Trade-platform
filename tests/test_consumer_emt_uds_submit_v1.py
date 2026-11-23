"""A real Linux UDS through Consumer/OMS/worker to a fake EMT boundary."""

from __future__ import annotations

import base64
import os
import sys
import time
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.consumer_simulator_gate_v1 import ConsumerEmtSimulatorGateV1, EmtSimulatorPermitV1
from emt_gateway.fastspi_v2_cancel_outcome import (
    FastSpiV2CancelOutcomeDirectoryConfig,
    FastSpiV2CancelOutcomeObserver,
)
from emt_gateway.fastspi_v2_route_outcome import (
    FastSpiV2RouteOutcomeDirectory,
    FastSpiV2RouteOutcomeDirectoryConfig,
)
from emt_gateway.provisioned_execution_factory_v1 import ProvisionedEmtExecutionProfileV1
from emt_gateway.tradecore_consumer_session_v1 import (
    ConsumerEmtSessionCompositionErrorV1,
    build_provisioned_consumer_emt_adapter_v1,
)
from emt_gateway.tradecore_mmap_journal import EmtMmapNativeCallJournal
from emt_gateway.tradecore_mutation import EmtPlatformMutationPort
from xuanling_tradecore.adapters.emt import (
    EmtBrokerAdapter,
    EmtEd25519ScopeOwnershipVerifier,
    EmtSignedScopeOwnership,
    MmapRouteLedgerV2,
    canonical_scope_ownership_payload,
)
from xuanling_tradecore.consumer import (
    ConsumerAccountView,
    ConsumerUnixClientConfigV2,
    ConsumerUnixClientV2,
)
from xuanling_tradecore.consumer.bindings_v2 import ConsumerBindingSnapshotV2
from xuanling_tradecore.consumer.cursor_v2 import CursorKeyRingV2, CursorSigningKeyV2
from xuanling_tradecore.consumer.emt_callback_pump_v1 import ManagedEmtCallbackPumpV1
from xuanling_tradecore.consumer.emt_outcome_projection_v1 import EmtCommittedOutcomeProjectorV1
from xuanling_tradecore.consumer.emt_submit_composition_v1 import (
    ManagedEmtConsumerSubmitV1,
    ManagedEmtExecutionSessionV1,
)
from xuanling_tradecore.consumer.emt_submit_disposition_v1 import MmapEmtSubmitDispositionJournalV1
from xuanling_tradecore.consumer.emt_submit_uds_v1 import build_consumer_emt_submit_uds_v1
from xuanling_tradecore.consumer.emt_submit_worker_v1 import EmtConsumerSubmitWorkerV1
from xuanling_tradecore.consumer.execution_runtime_v1 import ConsumerExecutionDispatchStateV1
from xuanling_tradecore.consumer.replay_v2 import ConsumerCursorReplayServiceV2
from xuanling_tradecore.consumer.stream_registry_v2 import CursorStreamRefV2, CursorStreamRegistryV2
from xuanling_tradecore.consumer.uds_server_v2 import _OrderBookProjectionV2
from xuanling_tradecore.consumer.unix_v2 import ConsumerUnixSocketError
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ConsumerBindingRevisionV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import InMemorySecretProviderV1
from xuanling_tradecore.domain import (
    AccountLifecycle,
    CancelDeadlinePolicy,
    ConsumerPrincipal,
    ExecutionDeadlinePolicy,
    OmsOrderState,
    ReconciliationStatus,
)
from xuanling_tradecore.ems import (
    AccountExecutionRoutePlan,
    AccountExecutionRouter,
    ExecutionGuard,
    ExecutionGuardAdmissionGate,
    ExecutionGuardPolicy,
    ExecutionScheduler,
    ExecutionSchedulerPolicy,
    ExecutionSessionRuntime,
    GuardMarketSnapshot,
    KillSwitchSnapshot,
    OwnershipFenceProof,
    SessionSupervisor,
    canonical_broker_capabilities_hash,
)
from xuanling_tradecore.oms import MmapOmsFactJournalV1
from xuanling_tradecore.persistence import MmapCallbackOutcomeJournal, MmapPreNativeJournal
from xuanling_tradecore.persistence.mmap_pre_native_journal import MmapJournalRecordKind

from test_consumer_emt_submit_route_preparer_v1 import _composition, _test_mapping
from test_execution_session_runtime import _Audit, _Observer
from test_tradecore_execution_session import _Provider, _TraderBase

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="requires Linux UDS and flock")


@pytest.mark.parametrize("mode", ["basic", "controlled", "simulator"])
def test_real_uds_admits_once_and_reaches_fake_native_after_committed_facts(
    tmp_path, mode
):
    controlled_session = mode != "basic"
    simulator_session = mode == "simulator"
    journal, refs, preparer, resolver, first, _errors, original, fixture, context = _composition(
        tmp_path, include_operations=True
    )
    class Gate:
        running = False

        def admission_permitted(self):
            return self.running

        def require_account(self, account_id):
            assert self.running and account_id == "sim-account"

        def risk_snapshot(self):
            return context.risk_snapshot

    class GateMonitor:
        def __init__(self, gate): self.gate = gate
        def start(self): self.gate.running = True
        def close(self): self.gate.running = False

    gate = Gate()
    monitor = GateMonitor(gate)
    if simulator_session:
        release = preparer.activated_release
        digest = sha256(b"simulator-credential").hexdigest()
        endpoint = sha256(b"simulator-endpoint").hexdigest()
        control = SimpleNamespace(
            profile_binding=SimpleNamespace(
                binding=SimpleNamespace(
                    account_id=fixture.account.account_id,
                    consumer_account_id=fixture.account.consumer_account_id,
                    profile_id="sim-profile", profile_revision=1,
                    effective_wall_ns=0, retired_wall_ns=None,
                ),
                profile=SimpleNamespace(
                    identity=SimpleNamespace(
                        venue_id="EMT", profile_id="sim-profile",
                        adapter_instance_id=fixture.account.adapter_instance_id,
                    ),
                    state=ProfileStateV1.ACTIVE_DISARMED,
                    revision=1,
                    material_digest=SimpleNamespace(
                        credential_digest=digest, endpoint_digest=endpoint,
                        release_manifest_hash=release.state.release_manifest_hash,
                    ),
                ),
            ),
            namespace=release,
            consumer_bindings=ConsumerBindingSnapshotV2(1, (fixture.account,), (fixture.strategy,)),
            namespace_not_after_wall_ns=1000,
        )
        permit = EmtSimulatorPermitV1(
            "sim-test-approval", fixture.account.account_id, fixture.account.consumer_account_id,
            fixture.account.adapter_instance_id, "sim-profile", 101, digest, endpoint,
            release.state.namespace_manifest_hash, release.state.release_hash, 900,
        )
        gate = ConsumerEmtSimulatorGateV1(
            control=control, permit=permit, risk=context.risk_snapshot,
            now_wall_ns=lambda: 1, now_monotonic_ns=lambda: 20,
        )
        monitor = gate
    if controlled_session:
        # Test only: real bootstrap passes this exact callable at construction.
        preparer._authorized = gate.admission_permitted
    dispositions = MmapOmsFactJournalV1(
        path=tmp_path / "dispositions.mmap", journal_id="dispositions", capacity=8
    )
    native = MmapPreNativeJournal(path=tmp_path / "native.mmap", capacity=8)
    callbacks = MmapCallbackOutcomeJournal(
        journals={"callback": MmapPreNativeJournal(path=tmp_path / "callbacks.mmap", capacity=8)},
        cancel_command_journals={
            "cancel": MmapPreNativeJournal(path=tmp_path / "cancel.mmap", capacity=8)
        },
    )
    routes = MmapRouteLedgerV2(
        path=tmp_path / "routes.mmap", capacity=8, directory=preparer._directory
    )
    outcome = FastSpiV2RouteOutcomeDirectory(
        config=FastSpiV2RouteOutcomeDirectoryConfig("sim-account", "adapter-tradecore-a", "101", 8)
    )
    cancel = FastSpiV2CancelOutcomeObserver(
        config=FastSpiV2CancelOutcomeDirectoryConfig(
            "sim-account", "adapter-tradecore-a", "101", 8
        ),
        journal=callbacks,
    )
    published = []
    stream = CursorStreamRefV2(journal.journal_id, 1)
    projection = _OrderBookProjectionV2(
        order_book=original._order_book, binding=fixture.account, stream=stream
    )
    # The fixture already committed one admission before this projection was
    # installed. Warm it from the current in-memory book, not a recovery claim.
    projection._items()
    registry = CursorStreamRegistryV2(stream, (stream,))
    replay = ConsumerCursorReplayServiceV2(
        key_ring_for=lambda _: CursorKeyRingV2(
            current=CursorSigningKeyV2("test-key", b"k" * 32)
        ),
        registry_for=lambda _: registry,
        event_reader=projection,
        partition_reader=projection,
        cursor_ttl_ns=60_000_000_000,
    )

    def publish_callback(event):
        projection.publish(event)
        published.append(event)

    projector = EmtCommittedOutcomeProjectorV1(
        order_book=original._order_book,
        adapter_instance_id="adapter-tradecore-a",
        adapter_client_scope="101",
        publish=publish_callback,
        close_admission=lambda reason: runtime.close_admission(reason=reason),
    )
    real_lane = EmtMmapNativeCallJournal(
        route_resolver=resolver,
        order_outcome_directory=outcome,
        cancel_outcome_observer=cancel,
        mmap_pre_native_journals={"lane-1": native},
        mmap_callback_outcomes=callbacks,
        mmap_route_ledger=routes,
        committed_outcome_hook=projector,
        recovery_submit_guard=(gate.require_account if controlled_session else lambda _: None),
        clock=lambda: 22,
    )

    native_calls = []
    api_instances = []

    class FakeApi(_TraderBase):
        def __init__(self):
            super().__init__()
            api_instances.append(self)

        def getTradingDay(self):
            return first[0].order.trading_day.value.replace("-", "")

        def insertOrder(self, request, session_id):
            assert session_id == 71
            assert [item.kind for item in native.scan_recovery_prefix().records] == [
                MmapJournalRecordKind.ORDER_PRE_NATIVE,
                MmapJournalRecordKind.ORDER_CALL_STARTED,
            ]
            client_id = (
                request["order_client_id"] if isinstance(request, dict) else request.order_client_id
            )
            route_fact = routes.load_route_ledger().route_for_client_id(client_id)
            assert route_fact is not None and route_fact.execution_id
            native_calls.append(route_fact.execution_id)
            return 5001

    class Fence:
        scope_hash = preparer.execution_binding.ownership_scope_hash

        def require_held(self):
            return OwnershipFenceProof(self.scope_hash, "fengyu", 1, 1)

    service = None
    runner = None
    try:
        sink = MmapEmtSubmitDispositionJournalV1(
            journal=dispositions,
            account_id="sim-account",
            binding_id=fixture.account.binding_id,
            binding_generation=fixture.account.binding_version,
        )
        digest = sha256(b"test-policy").hexdigest()
        mapping = _test_mapping()
        if controlled_session:
            ownership = mapping.manifest.scope_ownership
            assert ownership is not None
            key = Ed25519PrivateKey.generate()
            public_key = key.public_key().public_bytes(
                serialization.Encoding.Raw, serialization.PublicFormat.Raw
            )
            certificate = "test://ownership-key"
            revocation = "test://ownership-revocations"
            signed = EmtSignedScopeOwnership(
                ownership=ownership,
                certificate_reference=certificate,
                revocation_reference=revocation,
                signature_b64=base64.b64encode(
                    key.sign(
                        canonical_scope_ownership_payload(
                            ownership,
                            certificate_reference=certificate,
                            revocation_reference=revocation,
                        )
                    )
                ).decode("ascii"),
            )
            material = BrokerCredentialMaterialV1(
                broker_account="test-sim",
                password="test-only",
                client_id="101",
                connection=BrokerConnectionV1("BROKER_TCP", "test-sim", 12345),
            )
            provider = InMemorySecretProviderV1(provider_kind="test")
            reference = provider.store(
                target="test-local",
                material=material,
                adapter_instance_id="adapter-tradecore-a",
                account_scope="test-profile",
                idempotency_key="test-profile-credential",
            )
            profile = AdapterProfileRevisionV1(
                identity=AdapterProfileIdentityV1(
                    "test-profile", AdapterKindV1.EMT, "EMT", "adapter-tradecore-a", "test"
                ),
                revision=1,
                state=ProfileStateV1.ACTIVE_DISARMED,
                secret_reference=reference,
                material_digest=ProfileMaterialDigestV1(
                    material.credential_digest(),
                    material.connection.endpoint_digest(),
                    sha256(b"test-envelope").hexdigest(),
                    "test-key",
                    mapping.manifest.manifest_hash,
                    canonical_broker_capabilities_hash(mapping.manifest.capabilities),
                    preparer.activated_release.state.release_manifest_hash,
                ),
                created_wall_ns=1,
                validation_evidence_id="test-validation",
                approval_id="test-approval",
            )
            runtime_profile = ProvisionedEmtExecutionProfileV1(
                profile=profile, sdk_log_dir=tmp_path / "sdk-log", expected_client_id=101
            )
            adapter_args = dict(
                mapping=mapping,
                runtime_profile=runtime_profile,
                control_binding=ConsumerBindingRevisionV1(
                    binding_revision_id=fixture.account.binding_id,
                    profile_id=profile.identity.profile_id,
                    profile_revision=profile.revision,
                    account_id=fixture.account.account_id,
                    consumer_id=fixture.account.consumer_id,
                    consumer_account_id=fixture.account.consumer_account_id,
                    account_display_name=fixture.account.account_display_name,
                    strategy_ids=(fixture.strategy.strategy_id,),
                    authorization_epoch=1,
                    effective_wall_ns=1,
                ),
                secret_provider=provider,
                sdk_provider=_Provider(FakeApi),
                signed_scope_ownership=signed,
                scope_ownership_verifier=EmtEd25519ScopeOwnershipVerifier(
                    {ownership.signer_id: public_key}
                ),
                preparer=preparer,
                native_journal=real_lane,
                projector=projector,
                release_native_inflight=lambda execution_id: scheduler.release_new_order_inflight(
                    execution_id
                ),
                expected_api_version="2.27.0",
                session_epoch=1,
                now_wall_ns=preparer._wall(),
            )
            with pytest.raises(ConsumerEmtSessionCompositionErrorV1):
                build_provisioned_consumer_emt_adapter_v1(
                    **{**adapter_args, "runtime_profile": replace(runtime_profile, expected_client_id=102)}
                )
            with pytest.raises(ConsumerEmtSessionCompositionErrorV1):
                build_provisioned_consumer_emt_adapter_v1(
                    **{
                        **adapter_args,
                        "control_binding": replace(
                            adapter_args["control_binding"], account_id="another-account"
                        ),
                    }
                )
            with pytest.raises(ConsumerEmtSessionCompositionErrorV1):
                build_provisioned_consumer_emt_adapter_v1(
                    **{
                        **adapter_args,
                        "control_binding": replace(
                            adapter_args["control_binding"], retired_wall_ns=2
                        ),
                    }
                )
            adapter = build_provisioned_consumer_emt_adapter_v1(**adapter_args)
        else:
            port = EmtPlatformMutationPort(
                FakeApi(),
                native_session_id=71,
                order_request_factory=lambda value: value,
                native_call_journal=real_lane,
                clock=lambda: 23,
            )
            adapter = EmtBrokerAdapter(
                mapping,
                read_only_port=object(),
                mutation_port=port,
                client_order_ref_factory=resolver.client_order_ref_for,
            )
        supervisor = SessionSupervisor(
            binding=preparer.execution_binding,
            activated_mapping=mapping,
            ownership_fence=Fence(),
            adapter_factory=lambda: adapter,
        )
        scheduler = ExecutionScheduler(
            account_id="sim-account",
            process_epoch_id="test-epoch",
            policy=ExecutionSchedulerPolicy(
                "test-policy",
                1,
                digest,
                4,
                4,
                4,
                4,
                1024,
                1024,
                4,
            ),
            execution_deadline_policy=ExecutionDeadlinePolicy("submit-deadline", 1, digest, 100),
            cancel_deadline_policy=CancelDeadlinePolicy("cancel-deadline", 1, digest, 100),
        )
        router = AccountExecutionRouter(
            plan=AccountExecutionRoutePlan("test-plan", 1, digest, (preparer.execution_binding,)),
            schedulers={"sim-account": scheduler},
        )
        guard = ExecutionGuardAdmissionGate(
            ExecutionGuard(
                ExecutionGuardPolicy(
                    "test-guard",
                    1,
                    digest,
                    "sim-account",
                    1000,
                    4,
                    {"strategy-1": 4},
                    {"000001": 4},
                    {"BUY": 4},
                    10_000,
                    100,
                    10000,
                )
            ),
            _Audit(),
        )
        runtime = ExecutionSessionRuntime(
            binding=preparer.execution_binding,
            router=router,
            scheduler=scheduler,
            supervisor=supervisor,
            guard_admission=guard,
        )
        worker = EmtConsumerSubmitWorkerV1(
            preparer=preparer,
            runtime=runtime,
            native_journal=real_lane,
            lane_id="lane-1",
            market_for_execution=lambda _: GuardMarketSnapshot(
                "test-market", "000001", "SZ", 10, 1, 20, 1, 100
            ),
            kill_switches_for_account=lambda _: KillSwitchSnapshot(control_version=0),
            serialized_size_for_execution=lambda _: 64,
            record_disposition=sink.record,
            now_monotonic_ns=lambda: 22,
            max_pending=2,
            max_seen=4,
        )
        managed = ManagedEmtConsumerSubmitV1(worker)
        callback_pump = (
            ManagedEmtCallbackPumpV1(runtime=runtime, failure_target=managed)
            if controlled_session else None
        )
        view = [
            ConsumerAccountView(
                "ca-1",
                "test",
                "EMT",
                "sim-test-only",
                AccountLifecycle.DISARMED,
                ReconciliationStatus.NOT_REQUIRED,
            )
        ]
        socket_root = tmp_path / "socket-root"
        socket_root.mkdir(mode=0o700)
        os.chmod(socket_root, 0o700)
        socket_path = socket_root / "consumer.sock"
        snapshot = ConsumerBindingSnapshotV2(1, (fixture.account,), (fixture.strategy,))
        with pytest.raises(ConsumerUnixSocketError):
            build_consumer_emt_submit_uds_v1(
                socket_path=socket_path,
                contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
                principal=ConsumerPrincipal("consumer-1", "test", "test"),
                allowed_peers=frozenset({(os.geteuid(), os.getegid())}),
                binding_snapshot=snapshot,
                account_view_for=lambda _: view[0],
                order_book=original._order_book,
                managed_worker=managed,
                admission_context_for=lambda *_: context,
                now_monotonic_ns=lambda: 20,
                event_replay=replay,
            )
        service = build_consumer_emt_submit_uds_v1(
            socket_path=socket_path,
            contract_root=Path(__file__).parents[1] / "contracts" / "consumer" / "v2",
            principal=ConsumerPrincipal("consumer-1", "test", "test"),
            allowed_peers=frozenset({(os.geteuid(), os.getegid())}),
            binding_snapshot=snapshot,
            account_view_for=lambda _: (
                gate.account_view(fixture.account)
                if simulator_session and view[0].lifecycle is AccountLifecycle.READY
                else view[0]
            ),
            order_book=original._order_book,
            managed_worker=managed,
            admission_context_for=lambda *_: context,
            now_monotonic_ns=lambda: 20,
            on_order_event_committed=projection.publish,
            event_replay=replay,
            managed_session=ManagedEmtExecutionSessionV1(
                runtime=runtime, observer=_Observer(), now_wall_ns=lambda: 1
            ),
            callback_pump=callback_pump,
            runtime_gate=gate if controlled_session else None,
            managed_gate=monitor if controlled_session else None,
        )
        service.start()
        runner = Thread(target=service.serve_forever)
        runner.start()
        client = ConsumerUnixClientV2(ConsumerUnixClientConfigV2(socket_path, 3))
        payload = {
            "consumer_account_id": "ca-1",
            "strategy_id": "strategy-1",
            "strategy_order_id": "uds-one",
            "trading_day": first[0].order.trading_day.value,
            "symbol": "000001",
            "market": "SZ",
            "side": "BUY",
            "price_type": "LIMIT",
            "business_type": "CASH",
            "quantity": 10,
            "protection_price_ticks": 10,
            "limit_price_ticks": 10,
        }
        request = {
            "message_type": "request",
            "schema_version": 2,
            "operation": "SubmitSignal",
            "request_id": "uds-one",
            "payload": payload,
        }
        before = journal.scan().records
        rejected = client.call(request)
        assert rejected.get("code") == "INTERNAL_ERROR"
        assert journal.scan().records == before and native_calls == []
        # This READY view is a test fixture only, not real recovery evidence.
        view[0] = ConsumerAccountView(
            "ca-1",
            "test",
            "EMT",
            "sim-test-only",
            AccountLifecycle.READY,
            ReconciliationStatus.VALIDATED,
        )
        response = client.call(request)
        assert response["status"] == "ACCEPTED"
        if simulator_session:
            assert gate.account_view(fixture.account).lifecycle is AccountLifecycle.SIMULATOR_ACTIVE
            assert gate.account_view(fixture.account).reconciliation_status is ReconciliationStatus.NOT_REQUIRED
        execution_id = response["payload"]["execution_id"]
        deadline = time.monotonic() + 3
        while sink.get(execution_id) is None and time.monotonic() < deadline:
            time.sleep(0.01)
        result = sink.get(execution_id)
        assert (
            result is not None and result.state is ConsumerExecutionDispatchStateV1.SUBMIT_ACCEPTED
        )
        assert native_calls == [execution_id]
        assert [item.kind for item in native.scan_recovery_prefix().records] == [
            MmapJournalRecordKind.ORDER_PRE_NATIVE,
            MmapJournalRecordKind.ORDER_CALL_STARTED,
            MmapJournalRecordKind.ORDER_NATIVE_ACCEPTED,
        ]
        duplicate = client.call({**request, "request_id": "uds-one-retry"})
        assert duplicate["status"] == "ACCEPTED"
        assert duplicate["payload"]["execution_id"] == execution_id
        assert native_calls == [execution_id]
        assert refs.read_committed_lease(preparer._allocator.scope, execution_id) is not None
        assert not managed.failed
        # The simulator submit-only profile must not turn a Consumer cancel
        # request into either a local pending-cancel fact or a vendor call.
        before_cancel = journal.scan().records
        cancel_response = client.call({
            "message_type": "request", "schema_version": 2,
            "operation": "CancelOrder", "request_id": "cancel-not-enabled",
            "payload": {"oms_order_id": response["payload"]["oms_order_id"], "reason": "test"},
        })
        assert cancel_response["code"] == "UNAUTHORIZED"
        assert journal.scan().records == before_cancel
        assert native_calls == [execution_id]
        if controlled_session:
            # Exercise the actual SDK bridge, callback worker, journal hook and
            # OMS projection. EMT does not echo TradeCore's execution id.
            api_instances[-1].onOrderEvent({
                "order_emt_id": 5001,
                "order_client_id": 1000,
                "order_status": 4,
                "qty_traded": 0,
                "qty_left": 10,
            })
            deadline = time.monotonic() + 3
            while not published and time.monotonic() < deadline:
                time.sleep(0.01)
            assert len(published) == 1
            assert published[0].execution_id == execution_id
            assert published[0].order_state is OmsOrderState.WORKING
            assert callback_pump is not None and callback_pump.healthy()
            plan = client.call({
                "message_type": "request", "schema_version": 2,
                "operation": "CreateReplayPartitions", "request_id": "replay-plan",
                "payload": {"consumer_account_id": "ca-1", "partition_count": 1},
            })
            assert plan["status"] == "COMPLETED"
            page = client.call({
                "message_type": "request", "schema_version": 2,
                "operation": "PollEvents", "request_id": "replay-page",
                "payload": {"cursor": plan["payload"]["partitions"][0]["cursor"], "limit": 100},
            })
            assert page["status"] == "COMPLETED"
            events = page["payload"]["events"]
            assert any(
                item["payload"].get("execution_id") == execution_id
                and item["payload"].get("order_state") == "WORKING"
                for item in events
            )
    finally:
        if service is not None:
            service.stop()
        if runner is not None:
            runner.join(timeout=3)
            assert not runner.is_alive()
        if "runtime" in locals():
            runtime.stop()
        real_lane.close()
        dispositions.close()
        refs.close()
        journal.close()
