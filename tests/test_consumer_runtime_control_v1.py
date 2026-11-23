import json
from dataclasses import replace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.consumer_runtime_control_v1 import (
    ConsumerEmtRuntimeControlErrorV1,
    load_consumer_emt_runtime_control_v1,
)
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceVerifier,
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseBootstrapApprovalV1,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import ConsumerBindingRevisionV1
from xuanling_tradecore.control_plane.provisioning_registry_v1 import ActiveProfileBindingV1
from xuanling_tradecore.control_plane.provisioning_secret_v1 import InMemorySecretProviderV1

from test_emt_namespace_release_v1 import _DAY, _NOW, _release, _scope
from test_provisioned_execution_factory_v1 import _profile


def test_loads_signed_namespace_and_exact_control_snapshot_from_sources(tmp_path):
    release, keys = _release(Ed25519PrivateKey.generate())
    verifier = EmtOrderClientNamespaceVerifier(keys, revoked_signer_ids=frozenset())
    store = FileNamespaceReleaseStateStoreV1(tmp_path / "state.json")
    coordinator = NamespaceReleaseActivationCoordinatorV1(verifier=verifier, state_store=store)
    coordinator.activate(
        release,
        expected_scope=_scope(),
        expected_release_manifest_hash=release.release_manifest_hash,
        expected_trust_anchor_hash=release.trust_anchor_hash,
        now_wall_ns=_NOW,
        bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
            "test-approval", release.content_hash, "test-operator", _NOW
        ),
    )
    profile = _profile(InMemorySecretProviderV1(provider_kind="EMT_TEST"),
                       adapter_instance_id="adapter-tradecore-a")
    profile = replace(profile, material_digest=replace(
        profile.material_digest, release_manifest_hash=release.release_manifest_hash
    ))
    binding = ConsumerBindingRevisionV1(
        "binding", profile.identity.profile_id, profile.revision, "sim-account", "consumer",
        "ca", "display", ("strat",), 2, 1,
    )

    class Registry:
        def active_binding_and_profile(self, *, binding_revision_id, now_wall_ns):
            assert binding_revision_id == "binding" and now_wall_ns == _NOW
            return ActiveProfileBindingV1(profile, binding)

    class Reader:
        def read(self): return json.dumps(release.as_dict())

    kwargs = dict(
        registry=Registry(), binding_revision_id="binding", release_reader=Reader(),
        namespace_verifier=verifier, namespace_state_store=store,
        expected_trust_anchor_hash=release.trust_anchor_hash,
        trading_day=_DAY, emt_client_id=101, now_wall_ns=_NOW,
    )
    snapshot = load_consumer_emt_runtime_control_v1(**kwargs)
    assert snapshot.profile_binding.binding == binding
    assert snapshot.consumer_bindings.accounts[0].adapter_instance_id == "adapter-tradecore-a"
    assert snapshot.consumer_bindings.strategies[0].strategy_id == "strat"
    assert snapshot.namespace.state.release_hash == release.content_hash
    for changes in (
        {"emt_client_id": 102},
        {"expected_trust_anchor_hash": "f" * 64},
        {"namespace_state_store": FileNamespaceReleaseStateStoreV1(tmp_path / "absent")},
    ):
        with pytest.raises(ConsumerEmtRuntimeControlErrorV1):
            load_consumer_emt_runtime_control_v1(**{**kwargs, **changes})
