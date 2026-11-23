"""Build one provisioned, scope-verified EMT adapter for Consumer execution.

This is a construction seam for a privileged host bootstrap. It never loads
the SDK or leases credentials until the adapter is started by its supervisor.
It neither provides recovery evidence nor opens an account for trading.
"""

from __future__ import annotations

from typing import Callable

from xuanling_tradecore.adapters import ActivatedVenueMapping
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtControlledExecutionAdapter,
    EmtEd25519ScopeOwnershipVerifier,
    EmtSignedScopeOwnership,
)
from xuanling_tradecore.consumer.emt_outcome_projection_v1 import EmtCommittedOutcomeProjectorV1
from xuanling_tradecore.consumer.emt_submit_route_preparer_v1 import (
    EmtConsumerSubmitRoutePreparerV1,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import ConsumerBindingRevisionV1
from xuanling_tradecore.control_plane.provisioning_secret_v1 import SecretProviderV1
from xuanling_tradecore.ems import canonical_broker_capabilities_hash

from .consumer_simulator_gate_v1 import ConsumerEmtSimulatorGateV1
from .provisioned_execution_factory_v1 import (
    ProvisionedEmtExecutionProfileV1,
    ProvisionedEmtExecutionSessionFactoryV1,
)
from .tradecore_execution import EmtLoadedSdkProvider, EmtPlatformExecutionSessionFactory
from .tradecore_mmap_journal import EmtMmapNativeCallJournal
from .tradecore_order_request import EmtTradeCoreOrderRequestFactory


class ConsumerEmtSessionCompositionErrorV1(RuntimeError):
    """The provisioned profile, namespace and Consumer route disagree."""


def build_provisioned_consumer_emt_adapter_v1(
    *,
    mapping: ActivatedVenueMapping,
    runtime_profile: ProvisionedEmtExecutionProfileV1,
    control_binding: ConsumerBindingRevisionV1,
    secret_provider: SecretProviderV1,
    sdk_provider: EmtLoadedSdkProvider,
    signed_scope_ownership: EmtSignedScopeOwnership,
    scope_ownership_verifier: EmtEd25519ScopeOwnershipVerifier,
    preparer: EmtConsumerSubmitRoutePreparerV1,
    native_journal: EmtMmapNativeCallJournal,
    projector: EmtCommittedOutcomeProjectorV1,
    release_native_inflight: Callable[[str], bool],
    expected_api_version: str,
    session_epoch: int,
    now_wall_ns: int,
    simulator_gate: ConsumerEmtSimulatorGateV1 | None = None,
    native_reference_retainer: Callable[[object], None] | None = None,
) -> EmtControlledExecutionAdapter:
    """Bind immutable profile, signed namespace, route and callback projection.

    The activated release and runtime profile must be obtained from their
    authoritative repositories, and the caller must hold the account fence.
    Scope verification here is not a substitute for recovery or risk gates.
    """

    release = preparer.activated_release
    directory = release.directory
    binding = preparer.execution_binding
    profile = runtime_profile.profile
    consumer_binding = preparer.consumer_binding
    ownership = mapping.manifest.scope_ownership
    client_id = directory.scope.emt_client_id
    if (
        ownership is None
        or signed_scope_ownership.ownership != ownership
        or ownership.account_id != binding.account_id
        or ownership.adapter_instance_id != binding.adapter_instance_id
        or ownership.adapter_client_scope != binding.adapter_client_scope
        or (simulator_gate is None and not ownership.is_active_at(now_wall_ns))
        or control_binding.profile_id != profile.identity.profile_id
        or control_binding.profile_revision != profile.revision
        or control_binding.binding_revision_id != consumer_binding.binding_id
        or control_binding.profile_revision != consumer_binding.binding_version
        or control_binding.account_id != consumer_binding.account_id
        or control_binding.consumer_id != consumer_binding.consumer_id
        or control_binding.consumer_account_id != consumer_binding.consumer_account_id
        or control_binding.effective_wall_ns > now_wall_ns
        or control_binding.retired_wall_ns is not None
        or directory.scope.account_id != binding.account_id
        or str(client_id) != binding.adapter_client_scope
        or runtime_profile.expected_client_id != client_id
        or directory.own.adapter_instance_id != binding.adapter_instance_id
        or profile.identity.adapter_instance_id != binding.adapter_instance_id
        or profile.material_digest.mapping_manifest_hash != mapping.manifest.manifest_hash
        or binding.mapping_manifest_hash != mapping.manifest.manifest_hash
        or profile.material_digest.capability_manifest_hash
        != canonical_broker_capabilities_hash(mapping.manifest.capabilities)
        or profile.material_digest.release_manifest_hash != release.state.release_manifest_hash
        or release.state.namespace_manifest_hash != directory.manifest_hash
        or not directory.own.active_at(now_wall_ns)
        or native_journal.route_resolver is not preparer.resolver
        or native_journal.consumer_outcome_hook is not projector
        or not native_journal.has_recovery_submit_guard
        or projector.order_book.journal is not preparer.oms_journal
        or projector.adapter_scope != (binding.adapter_instance_id, binding.adapter_client_scope)
        or not callable(release_native_inflight)
    ):
        raise ConsumerEmtSessionCompositionErrorV1("EMT Consumer session scope or evidence mismatch")

    if simulator_gate is not None:
        simulator_gate.require_mapping(mapping, wall_ns=now_wall_ns)
        if (
            simulator_gate.control.profile_binding.profile != profile
            or simulator_gate.control.profile_binding.binding != control_binding
            or native_journal.recovery_submit_guard != simulator_gate.require_account
            or preparer.admission_authorizer != simulator_gate.admission_permitted
        ):
            raise ConsumerEmtSessionCompositionErrorV1("simulator gate is not the session authority")

    def native_factory(settings):
        return EmtPlatformExecutionSessionFactory(
            sdk_provider=sdk_provider,
            settings=settings,
            order_request_factory=EmtTradeCoreOrderRequestFactory(),
            native_call_journal=native_journal,
            cancel_request_id_for_order=native_journal.cancel_request_id_for_order,
            cancel_target_identity_for_order=native_journal.cancel_target_identity_for_order,
            native_reference_retainer=native_reference_retainer,
        )

    session_factory = ProvisionedEmtExecutionSessionFactoryV1(
        runtime_profile=runtime_profile,
        secret_provider=secret_provider,
        factory_builder=native_factory,
    )
    return EmtAdapterProfile.create_controlled_execution_adapter(
        mapping,
        session_factory=session_factory,
        client_order_ref_factory=preparer.resolver.client_order_ref_for,
        expected_api_version=expected_api_version,
        expected_trading_day=directory.scope.trading_day,
        session_epoch=session_epoch,
        signed_scope_ownership=signed_scope_ownership,
        scope_ownership_verifier=scope_ownership_verifier,
        activated_namespace_release=release,
        native_reference_retainer=native_reference_retainer,
        broker_event_observer_wrapper=lambda downstream: native_journal.wrap_broker_event_observer(
            downstream, native_inflight_release=release_native_inflight
        ),
    )
