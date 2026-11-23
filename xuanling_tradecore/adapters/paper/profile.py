"""Versioned PaperBroker profile activation and Registry declaration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ...domain import ExecutionInstruction, RecoveryEvidenceProfile
from ..mapping import ActivatedVenueMapping, RuntimeVenueProfile, VenueMappingManifest
from .adapter import PaperBrokerAdapter
from .connector import PaperBrokerSessionPort
from .dependency import PaperBrokerDependencyManifest
from .mapping import PaperBrokerMappingRegistry

if TYPE_CHECKING:
    from ...ems import AdapterProfileDeclaration


@dataclass(frozen=True)
class PaperBrokerAdapterProfile:
    """TradeCore-side profile; it does not own the external execution project."""

    manifest: VenueMappingManifest
    dependency: PaperBrokerDependencyManifest

    def __post_init__(self) -> None:
        if self.manifest.venue_id != "PAPER":
            raise ValueError("PaperBroker Adapter profile requires a PAPER manifest")

    def activate(
        self, *, runtime: RuntimeVenueProfile, activated_at_unix_s: int
    ) -> ActivatedVenueMapping:
        return PaperBrokerMappingRegistry((self.manifest,)).activate(
            venue_id="PAPER",
            manifest_id=self.manifest.manifest_id,
            manifest_version=self.manifest.manifest_version,
            runtime=runtime,
            activated_at_unix_s=activated_at_unix_s,
        )

    @staticmethod
    def create_adapter(
        mapping: ActivatedVenueMapping,
        *,
        dependency: PaperBrokerDependencyManifest,
        session: PaperBrokerSessionPort,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        ingress_capacity: int = 1024,
    ) -> PaperBrokerAdapter:
        return PaperBrokerAdapter(
            mapping,
            dependency=dependency,
            session=session,
            client_order_ref_factory=client_order_ref_factory,
            ingress_capacity=ingress_capacity,
        )

    def registry_declaration(
        self,
        *,
        mapping: ActivatedVenueMapping,
        recovery_profile: RecoveryEvidenceProfile,
        session_factory: Callable[[], PaperBrokerSessionPort],
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
    ) -> AdapterProfileDeclaration:
        """Register the connector through the common adapter registry only."""

        from ...ems import (
            AdapterProfileDeclaration,
            AdapterProfileKey,
            canonical_broker_capabilities_hash,
        )

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("PaperBroker profile lacks scope ownership")
        return AdapterProfileDeclaration(
            profile_id=f"{self.manifest.manifest_id}:paperbroker",
            version=self.manifest.manifest_version,
            key=AdapterProfileKey(
                venue_id="PAPER",
                adapter_instance_id=ownership.adapter_instance_id,
                capability_manifest_hash=canonical_broker_capabilities_hash(
                    mapping.runtime.capabilities
                ),
                mapping_manifest_hash=mapping.manifest.manifest_hash,
            ),
            mapping=mapping,
            recovery_profile=recovery_profile,
            adapter_factory=lambda: PaperBrokerAdapterProfile.create_adapter(
                mapping,
                dependency=self.dependency,
                session=session_factory(),
                client_order_ref_factory=client_order_ref_factory,
            ),
            recovery_certificate_factory=None,
        )
