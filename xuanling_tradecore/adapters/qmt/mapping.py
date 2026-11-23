"""QMT fake-profile mapping loader; it has no QMT SDK dependency."""

from __future__ import annotations

from ..mapping import (
    MappingManifestError,
    VenueMappingManifest,
    VenueMappingRegistry,
    load_packaged_mapping_manifest,
)


class QmtMappingRegistry(VenueMappingRegistry):
    """A registry constrained to the QMT fake-profile manifest family."""

    def register(self, manifest: VenueMappingManifest) -> None:
        if manifest.venue_id != "QMT":
            raise MappingManifestError("QMT mapping registry accepts only QMT manifests")
        super().register(manifest)


def load_builtin_qmt_fake_manifest() -> VenueMappingManifest:
    """Load the no-SDK QMT fake contract fixture."""

    return load_packaged_mapping_manifest(
        "xuanling_tradecore.adapters.qmt.manifests",
        "qmt-fake-contract-v1.json",
    )
