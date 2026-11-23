"""EMT mapping-registry entry points.

This module contains only profile loading and registry naming.  The future EMT
adapter remains the sole place allowed to import its native SDK.
"""

from __future__ import annotations

from ..mapping import (
    MappingManifestError,
    VenueMappingManifest,
    VenueMappingRegistry,
    load_packaged_mapping_manifest,
)


class EmtMappingRegistry(VenueMappingRegistry):
    """A registry constrained to mappings whose venue is EMT."""

    def register(self, manifest: VenueMappingManifest) -> None:
        if manifest.venue_id != "EMT":
            raise MappingManifestError("EMT mapping registry accepts only EMT manifests")
        super().register(manifest)


def load_builtin_emt_legacy_sim_manifest() -> VenueMappingManifest:
    """Load the first EMT profile evidenced by the legacy simulation fixtures."""

    return load_packaged_mapping_manifest(
        "xuanling_tradecore.adapters.emt.manifests",
        "emt-legacy-sim-2.20.0-v2.json",
    )
