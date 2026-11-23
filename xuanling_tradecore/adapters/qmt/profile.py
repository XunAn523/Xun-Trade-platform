"""QMT profile factories at the same BrokerAdapter boundary as EMT."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from ...domain import ExecutionInstruction, RecoveryEvidenceProfile
from ..mapping import ActivatedVenueMapping, RuntimeVenueProfile, VenueMappingManifest
from .bridge_adapter import QmtBrokerAdapter
from .bridge_protocol import QmtBridgePort
from .capability import QmtCapabilityActivation
from .fake import FakeQmtBrokerAdapter
from .mapping import QmtMappingRegistry

if TYPE_CHECKING:
    from ...ems import AdapterProfileDeclaration


@dataclass(frozen=True)
class QmtAdapterProfile:
    """QMT fake and disarmed bridge composition through the common Registry."""

    manifest: VenueMappingManifest

    def __post_init__(self) -> None:
        if self.manifest.venue_id != "QMT":
            raise ValueError("QMT Adapter profile requires a QMT manifest")

    def activate(
        self, *, runtime: RuntimeVenueProfile, activated_at_unix_s: int
    ) -> ActivatedVenueMapping:
        return QmtMappingRegistry((self.manifest,)).activate(
            venue_id="QMT",
            manifest_id=self.manifest.manifest_id,
            manifest_version=self.manifest.manifest_version,
            runtime=runtime,
            activated_at_unix_s=activated_at_unix_s,
        )

    @staticmethod
    def create_fake_adapter(mapping: ActivatedVenueMapping) -> FakeQmtBrokerAdapter:
        return FakeQmtBrokerAdapter(mapping)

    @staticmethod
    def create_bridge_adapter(
        mapping: ActivatedVenueMapping,
        *,
        bridge: QmtBridgePort,
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        capability_activation: QmtCapabilityActivation | None = None,
        mutation_enabled: bool = False,
    ) -> QmtBrokerAdapter:
        """Create the production-shaped bridge adapter behind an explicit gate.

        ``mutation_enabled`` remains false until the signed QMT production
        profile carries P0 capability evidence.  Keeping the flag explicit
        prevents a merely constructed bridge from silently becoming tradable.
        """

        return QmtBrokerAdapter(
            mapping,
            bridge=bridge,
            client_order_ref_factory=client_order_ref_factory,
            capability_activation=capability_activation,
            mutation_enabled=mutation_enabled,
        )

    def registry_declaration(
        self,
        *,
        mapping: ActivatedVenueMapping,
        recovery_profile: RecoveryEvidenceProfile,
    ) -> AdapterProfileDeclaration:
        """Build the Registry declaration lazily to keep adapters independent of EMS imports."""

        from ...ems import (
            AdapterProfileDeclaration,
            AdapterProfileKey,
            canonical_broker_capabilities_hash,
        )

        ownership = mapping.manifest.scope_ownership
        if ownership is None:
            raise ValueError("QMT fake profile lacks scope ownership")
        return AdapterProfileDeclaration(
            profile_id=f"{self.manifest.manifest_id}:fake",
            version=self.manifest.manifest_version,
            key=AdapterProfileKey(
                venue_id="QMT",
                adapter_instance_id=ownership.adapter_instance_id,
                capability_manifest_hash=canonical_broker_capabilities_hash(
                    mapping.runtime.capabilities
                ),
                mapping_manifest_hash=mapping.manifest.manifest_hash,
            ),
            mapping=mapping,
            recovery_profile=recovery_profile,
            adapter_factory=lambda: FakeQmtBrokerAdapter(mapping),
            recovery_certificate_factory=None,
        )

    def bridge_registry_declaration(
        self,
        *,
        mapping: ActivatedVenueMapping,
        recovery_profile: RecoveryEvidenceProfile,
        bridge_factory: Callable[[], QmtBridgePort],
        client_order_ref_factory: Callable[[ExecutionInstruction], str],
        capability_activation: QmtCapabilityActivation | None = None,
    ) -> AdapterProfileDeclaration:
        """Register the actual QMT bridge via the same EMS contract as EMT.

        Construction performs no I/O. The factory owns the QMT-specific
        connection; Registry/EMS see only BrokerAdapter. This composition
        stays disarmed and cannot supply a recovery certificate before P3.
        """

        from dataclasses import replace

        if mapping.manifest != self.manifest:
            raise ValueError("QMT bridge mapping belongs to another profile")
        declaration = self.registry_declaration(mapping=mapping, recovery_profile=recovery_profile)
        return replace(
            declaration,
            profile_id=f"{self.manifest.manifest_id}:bridge-disarmed",
            adapter_factory=lambda: self.create_bridge_adapter(
                mapping,
                bridge=bridge_factory(),
                client_order_ref_factory=client_order_ref_factory,
                capability_activation=capability_activation,
                mutation_enabled=False,
            ),
        )
