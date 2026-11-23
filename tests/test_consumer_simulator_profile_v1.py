import json

import pytest
from emt_gateway.consumer_simulator_profile_v1 import (
    SimulatorProfileErrorV1,
    parse_simulator_profile_binding_v1,
)


def document():
    return {
        "schema_version": 1,
        "profile": {
            "profile_id": "sim-profile", "adapter_instance_id": "adapter", "account_alias": "sim",
            "revision": 1, "created_wall_ns": 1,
            "validation_evidence_id": "operator-simulator-validation", "approval_id": "approval",
            "secret_reference": {
                "provider_kind": "SYSTEMD_SIMULATOR_CREDENTIAL_V1", "reference_id": "ref", "target": "test"
            },
            "material_digest": {
                "credential_digest": "a" * 64, "endpoint_digest": "b" * 64,
                "envelope_digest": "c" * 64, "recipient_key_id": "systemd",
                "mapping_manifest_hash": "d" * 64, "capability_manifest_hash": "e" * 64,
                "release_manifest_hash": "f" * 64,
            },
        },
        "binding": {
            "binding_revision_id": "binding", "profile_id": "sim-profile", "profile_revision": 1,
            "account_id": "account", "consumer_id": "consumer", "consumer_account_id": "ca",
            "account_display_name": "sim", "strategy_ids": ["strategy"],
            "authorization_epoch": 1, "effective_wall_ns": 10,
        },
    }


def test_protected_profile_requires_matching_active_binding_and_scope():
    reader = parse_simulator_profile_binding_v1(json.dumps(document()))
    result = reader.active_binding_and_profile(binding_revision_id="binding", now_wall_ns=10)
    assert result.profile.identity.venue_id == "EMT"
    assert result.binding.strategy_ids == ("strategy",)
    with pytest.raises(SimulatorProfileErrorV1):
        reader.active_binding_and_profile(binding_revision_id="other", now_wall_ns=10)
    with pytest.raises(SimulatorProfileErrorV1):
        reader.active_binding_and_profile(binding_revision_id="binding", now_wall_ns=9)


@pytest.mark.parametrize("change", ["credential", "provider", "revision", "duplicate", "strategy"])
def test_protected_profile_rejects_secret_or_mismatched_inputs(change):
    data = document()
    if change == "credential":
        data["profile"]["password"] = "must-not-appear"
    elif change == "provider":
        data["profile"]["secret_reference"]["provider_kind"] = "IN_MEMORY_TEST"
    elif change == "revision":
        data["binding"]["profile_revision"] = 2
    elif change == "strategy":
        data["binding"]["strategy_ids"] = "strategy"
    text = json.dumps(data)
    if change == "duplicate":
        text = text[:-1] + ', "schema_version": 1}'
    with pytest.raises(SimulatorProfileErrorV1):
        parse_simulator_profile_binding_v1(text)
