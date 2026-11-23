"""Protected, non-secret EMT simulator profile and Consumer binding input.

This reader does not load credentials, start EMT, initialize a namespace, or
grant admission. The caller must load the signed namespace and simulator
permit separately and hold the ownership fence before using this input.
"""

from __future__ import annotations

import json
from pathlib import Path

from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    ConsumerBindingRevisionV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
    SecretReferenceV1,
)
from xuanling_tradecore.control_plane.provisioning_registry_v1 import ActiveProfileBindingV1

from .consumer_runtime_control_v1 import LinuxRootOwnedNamespaceFileV1
from .systemd_simulator_secret_v1 import SystemdSimulatorSecretProviderV1


class SimulatorProfileErrorV1(RuntimeError):
    """The protected simulator profile is malformed or no longer active."""


class ProtectedSimulatorProfileBindingV1:
    """Immutable active-binding reader for the protected simulator host config."""

    def __init__(self, active: ActiveProfileBindingV1) -> None:
        if active.profile.secret_reference.provider_kind != SystemdSimulatorSecretProviderV1._KIND:
            raise SimulatorProfileErrorV1("simulator profile requires systemd credentials")
        self._active = active

    def active_binding_and_profile(
        self, *, binding_revision_id: str, now_wall_ns: int
    ) -> ActiveProfileBindingV1:
        binding = self._active.binding
        if (
            type(now_wall_ns) is not int or now_wall_ns < 0
            or binding.binding_revision_id != binding_revision_id
            or binding.effective_wall_ns > now_wall_ns
            or binding.retired_wall_ns is not None
        ):
            raise SimulatorProfileErrorV1("simulator profile binding is not active")
        return self._active


def parse_simulator_profile_binding_v1(text: str) -> ProtectedSimulatorProfileBindingV1:
    """Parse exact public fields only; credentials are never accepted here."""

    def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate profile key")
            result[key] = value
        return result

    def fields(value: object, expected: set[str]) -> dict[str, object]:
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("unexpected profile fields")
        return value

    try:
        document = fields(json.loads(text, object_pairs_hook=unique_pairs), {"schema_version", "profile", "binding"})
        if type(document["schema_version"]) is not int or document["schema_version"] != 1:
            raise ValueError("invalid profile schema")
        profile = fields(document["profile"], {
            "profile_id", "adapter_instance_id", "account_alias", "revision", "secret_reference",
            "material_digest", "created_wall_ns", "validation_evidence_id", "approval_id",
        })
        secret = fields(profile["secret_reference"], {"provider_kind", "reference_id", "target"})
        digest = fields(profile["material_digest"], {
            "credential_digest", "endpoint_digest", "envelope_digest", "recipient_key_id",
            "mapping_manifest_hash", "capability_manifest_hash", "release_manifest_hash",
        })
        binding = fields(document["binding"], {
            "binding_revision_id", "profile_id", "profile_revision", "account_id", "consumer_id",
            "consumer_account_id", "account_display_name", "strategy_ids", "authorization_epoch",
            "effective_wall_ns",
        })
        if not isinstance(binding["strategy_ids"], list):
            raise ValueError("strategy ids must be a list")
        identity = AdapterProfileIdentityV1(
            profile_id=profile["profile_id"], adapter_kind=AdapterKindV1.EMT, venue_id="EMT",
            adapter_instance_id=profile["adapter_instance_id"], account_alias=profile["account_alias"],
        )
        revision = AdapterProfileRevisionV1(
            identity=identity, revision=profile["revision"], state=ProfileStateV1.ACTIVE_DISARMED,
            secret_reference=SecretReferenceV1(**secret), material_digest=ProfileMaterialDigestV1(**digest),
            created_wall_ns=profile["created_wall_ns"],
            validation_evidence_id=profile["validation_evidence_id"], approval_id=profile["approval_id"],
        )
        selected = ConsumerBindingRevisionV1(**{**binding, "strategy_ids": tuple(binding["strategy_ids"])})
        return ProtectedSimulatorProfileBindingV1(ActiveProfileBindingV1(revision, selected))
    except (TypeError, ValueError, KeyError) as exc:
        raise SimulatorProfileErrorV1("protected simulator profile is invalid") from exc


def load_simulator_profile_binding_v1(path: Path) -> ProtectedSimulatorProfileBindingV1:
    return parse_simulator_profile_binding_v1(LinuxRootOwnedNamespaceFileV1(path).read())
