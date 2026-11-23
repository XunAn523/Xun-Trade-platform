"""PaperBroker-specific mapping registry and packaged fixture loader."""

from __future__ import annotations

from ..mapping import (
    MappingManifestError,
    VenueMappingManifest,
    VenueMappingRegistry,
    load_packaged_mapping_manifest,
)


class PaperBrokerMappingRegistry(VenueMappingRegistry):
    """A registry that refuses non-PaperBroker mapping manifests."""

    def register(self, manifest: VenueMappingManifest) -> None:
        if manifest.venue_id != "PAPER":
            raise MappingManifestError("PaperBroker mapping registry accepts only PAPER manifests")
        super().register(manifest)


def load_builtin_paperbroker_contract_fixture() -> VenueMappingManifest:
    """Load the isolated test fixture, never an external production profile."""

    return load_packaged_mapping_manifest(
        "xuanling_tradecore.adapters.paper.manifests",
        "paperbroker-isolated-contract-fixture-v1.json",
    )
