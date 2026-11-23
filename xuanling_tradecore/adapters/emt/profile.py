"""Versioned EMT profile for the no-login, read-only staging Adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ...domain import ExecutionInstruction, RecoveryEvidenceProfile, TradingDayEpoch
from ..base import BrokerEventObserver
from ..mapping import ActivatedVenueMapping, RuntimeVenueProfile, VenueMappingManifest
from .adapter import EmtAdapterSubmitStageObserver, EmtBrokerAdapter, EmtReadOnlyPort
from .disarmed_profile import EmtDisarmedProfile
from .execution_session_adapter import EmtControlledExecutionAdapter, EmtExecutionSdkSessionFactory
from .mapping import EmtMappingRegistry
from .namespace_recovery import (
    EmtAuthoritativeOrderObserverV1,
)
from .namespace_release_v1 import ActivatedNamespaceReleaseV1
from .scope_ownership import EmtEd25519ScopeOwnershipVerifier, EmtSignedScopeOwnership
from .session_adapter import EmtControlledReadOnlyAdapter, EmtReadOnlySdkSessionFactory

if TYPE_CHECKING:
    from ...ems import AdapterProfileDeclaration


@dataclass(frozen=True)
class EmtAdapterProfile:
    """The EMT profile activation/Registry seam before live SDK wiring is approved."""

    manifest: VenueMappingManifest

    def __post_init__(self) -> None:
        if self.manifest.venue_id != "EMT":
            raise ValueError("EMT Adapter profile requires an EMT manifest")

    def activate(
        self, *, runtime: RuntimeVenueProfile, activated_at_unix_s: int
    ) -> ActivatedVenueMapping:
        return EmtMappingRegistry((self.manifest,)).activate(
            venue_id="EMT",
            manifest_id=self.manifest.manifest_id,
            manifest_version=self.manifest.manifest_version,
            runtime=runtime,
            activated_at_unix_s=activated_at_unix_s,
        )

    @staticmethod
    def create_read_only_adapter(
        mapping: ActivatedVenueMapping, *, read_only_port: EmtReadOnlyPort
    ) -> EmtBrokerAdapter:
        return EmtBrokerAdapter(mapping, read_only_port=read_only_port)

    @staticmethod
    def create_controlled_read_only_adapter(
        mapping: ActivatedVenueMapping,
        *,
        session_factory: EmtReadOnlySdkSessionFactory,
        expected_api_version: str,
        expected_trading_day: TradingDayEpoch,
        session_epoch: int,
        signed_scope_ownership: EmtSignedScopeOwnership,
        scope_ownership_verifier: EmtEd25519ScopeOwnershipVerifier,
    ) -> EmtControlledReadOnlyAdapter:
        """Bind an exact signed scope claim before a native session can open."""

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("EMT controlled profile lacks scope ownership")
        if signed_scope_ownership.ownership != ownership:
            raise ValueError("EMT signed scope ownership does not match the mapping")
        verified = scope_ownership_verifier.verify(signed_scope_ownership)
        if verified != ownership:
            raise ValueError("EMT verified scope ownership does not match the mapping")
        return EmtControlledReadOnlyAdapter(
            mapping,
            session_factory=session_factory,
            expected_api_version=expected_api_version,
            expected_trading_day=expected_trading_day,
            session_epoch=session_epoch,
        )

    @staticmethod
    def create_controlled_execution_adapter(
        mapping: ActivatedVenueMapping,
        *,
        session_factory: EmtExecutionSdkSessionFactory,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        expected_api_version: str,
        expected_trading_day: TradingDayEpoch,
        session_epoch: int,
        signed_scope_ownership: EmtSignedScopeOwnership,
        scope_ownership_verifier: EmtEd25519ScopeOwnershipVerifier,
        event_ingress_capacity: int = 1024,
        broker_event_observer_wrapper: Callable[[BrokerEventObserver], BrokerEventObserver]
        | None = None,
        lifecycle_observer: Callable[[str], None] | None = None,
        submit_stage_observer: EmtAdapterSubmitStageObserver | None = None,
        native_reference_retainer: Callable[[object], None] | None = None,
        activated_namespace_release: ActivatedNamespaceReleaseV1 | None = None,
        authoritative_order_observer: EmtAuthoritativeOrderObserverV1 | None = None,
        authoritative_row_failure_observer: Callable[[str], None] | None = None,
    ) -> EmtControlledExecutionAdapter:
        """Create a DISARMED execution adapter after independent verification.

        An R1 namespace may only enter this public composition through an
        already activated signed release.  That prevents callers from
        constructing a raw directory at the session boundary and bypassing
        the release-state anti-rollback fence.
        """

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("EMT controlled execution profile lacks scope ownership")
        if signed_scope_ownership.ownership != ownership:
            raise ValueError("EMT signed scope ownership does not match the mapping")
        verified = scope_ownership_verifier.verify(signed_scope_ownership)
        if verified != ownership:
            raise ValueError("EMT verified scope ownership does not match the mapping")
        namespace_directory = None
        if activated_namespace_release is not None:
            if (
                activated_namespace_release.state.scope
                != activated_namespace_release.directory.scope
                or activated_namespace_release.state.namespace_manifest_hash
                != activated_namespace_release.directory.manifest_hash
            ):
                raise ValueError("EMT activated namespace release state is inconsistent")
            namespace_directory = activated_namespace_release.directory
        return EmtControlledExecutionAdapter(
            mapping,
            session_factory=session_factory,
            client_order_ref_factory=client_order_ref_factory,
            expected_api_version=expected_api_version,
            expected_trading_day=expected_trading_day,
            session_epoch=session_epoch,
            event_ingress_capacity=event_ingress_capacity,
            broker_event_observer_wrapper=broker_event_observer_wrapper,
            lifecycle_observer=lifecycle_observer,
            submit_stage_observer=submit_stage_observer,
            native_reference_retainer=native_reference_retainer,
            namespace_directory=namespace_directory,
            authoritative_order_observer=authoritative_order_observer,
            authoritative_row_failure_observer=authoritative_row_failure_observer,
        )

    @staticmethod
    def create_controlled_read_only_adapter_from_disarmed_profile(
        mapping: ActivatedVenueMapping,
        *,
        disarmed_profile: EmtDisarmedProfile,
        session_factory: EmtReadOnlySdkSessionFactory,
        expected_api_version: str,
        expected_trading_day: TradingDayEpoch,
        session_epoch: int,
        scope_ownership_verifier: EmtEd25519ScopeOwnershipVerifier,
    ) -> EmtControlledReadOnlyAdapter:
        """Select one claim verified against an external trust anchor.

        The serialized profile's public keys only allow offline integrity
        checks.  They must not become the authority that decides whether a
        controlled native session may be opened.
        """

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("EMT controlled profile lacks scope ownership")
        matches = tuple(
            signed
            for signed in disarmed_profile.signed_scope_ownerships
            if signed.ownership == ownership
        )
        if len(matches) != 1:
            raise ValueError(
                "EMT DISARMED profile must contain exactly one matching signed scope ownership"
            )
        return EmtAdapterProfile.create_controlled_read_only_adapter(
            mapping,
            session_factory=session_factory,
            expected_api_version=expected_api_version,
            expected_trading_day=expected_trading_day,
            session_epoch=session_epoch,
            signed_scope_ownership=matches[0],
            scope_ownership_verifier=scope_ownership_verifier,
        )

    def registry_declaration(
        self,
        *,
        mapping: ActivatedVenueMapping,
        recovery_profile: RecoveryEvidenceProfile,
        read_only_port: EmtReadOnlyPort,
    ) -> AdapterProfileDeclaration:
        """Declare a deliberately read-only profile without importing an EMT SDK."""

        from ...ems import (
            AdapterProfileDeclaration,
            AdapterProfileKey,
            canonical_broker_capabilities_hash,
        )

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("EMT profile lacks scope ownership")
        return AdapterProfileDeclaration(
            profile_id=f"{self.manifest.manifest_id}:read-only-staging",
            version=self.manifest.manifest_version,
            key=AdapterProfileKey(
                venue_id="EMT",
                adapter_instance_id=ownership.adapter_instance_id,
                capability_manifest_hash=canonical_broker_capabilities_hash(
                    mapping.runtime.capabilities
                ),
                mapping_manifest_hash=mapping.manifest.manifest_hash,
            ),
            mapping=mapping,
            recovery_profile=recovery_profile,
            adapter_factory=lambda: EmtBrokerAdapter(mapping, read_only_port=read_only_port),
            recovery_certificate_factory=None,
        )
