import base64
import json
import os
import stat
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.consumer_runtime_control_v1 import load_consumer_emt_runtime_control_v1
from emt_gateway.consumer_simulator_gate_v1 import (
    ConsumerEmtSimulatorGateV1,
    EmtSimulatorGateErrorV1,
    EmtSimulatorPermitV1,
)
from emt_gateway.consumer_simulator_host_inputs_v1 import (
    SimulatorHostInputPathsV1,
    SimulatorHostInputsErrorV1,
    load_simulator_host_inputs_v1,
)
from emt_gateway.consumer_simulator_namespace_trust_v1 import parse_simulator_namespace_trust_v1
from emt_gateway.consumer_simulator_profile_v1 import parse_simulator_profile_binding_v1
from xuanling_tradecore.adapters.emt import (
    FileNamespaceReleaseStateStoreV1,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseBootstrapApprovalV1,
)
from xuanling_tradecore.domain import AccountLifecycle, RiskSnapshot

from test_consumer_simulator_profile_v1 import document
from test_emt_namespace_release_v1 import _DAY, _NOW, _release, _scope


def test_simulator_host_inputs_bind_signed_release_profile_and_explicit_permit(tmp_path):
    release, keys = _release(Ed25519PrivateKey.generate())
    trust = parse_simulator_namespace_trust_v1(json.dumps({
        "schema_version": 1,
        "trusted_public_keys": [
            {"signer_id": name, "public_key_b64": base64.b64encode(key).decode()}
            for name, key in keys.items()
        ],
        "revoked_signer_ids": [],
    }))
    store = FileNamespaceReleaseStateStoreV1(tmp_path / "state.json")
    NamespaceReleaseActivationCoordinatorV1(verifier=trust.verifier(), state_store=store).activate(
        release,
        expected_scope=_scope(), expected_release_manifest_hash=release.release_manifest_hash,
        expected_trust_anchor_hash=trust.trust_anchor_hash, now_wall_ns=_NOW,
        bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
            "test-approval", release.content_hash, "test-operator", _NOW
        ),
    )
    data = document()
    data["profile"]["adapter_instance_id"] = "adapter-tradecore-a"
    data["profile"]["material_digest"]["release_manifest_hash"] = release.release_manifest_hash
    data["binding"]["account_id"] = "sim-account"
    registry = parse_simulator_profile_binding_v1(json.dumps(data))

    class Reader:
        def read(self):
            return json.dumps(release.as_dict())

    control = load_consumer_emt_runtime_control_v1(
        registry=registry, binding_revision_id="binding", release_reader=Reader(),
        namespace_verifier=trust.verifier(), namespace_state_store=store,
        expected_trust_anchor_hash=trust.trust_anchor_hash,
        trading_day=_DAY, emt_client_id=101, now_wall_ns=_NOW,
    )
    permit = EmtSimulatorPermitV1(
        "approval", "sim-account", "ca", "adapter-tradecore-a", "sim-profile", 101,
        "a" * 64, "b" * 64, control.namespace.state.namespace_manifest_hash,
        control.namespace.state.release_hash, _NOW + 1,
    )
    risk = RiskSnapshot("sim-policy", "sim-account", _DAY, "a" * 64, 10, 100, 1000, 0)
    gate = ConsumerEmtSimulatorGateV1(
        control=control, permit=permit, risk=risk,
        now_wall_ns=lambda: _NOW, now_monotonic_ns=lambda: 20,
    )
    gate.start()
    assert gate.account_view(control.consumer_bindings.accounts[0]).lifecycle is AccountLifecycle.SIMULATOR_ACTIVE
    mismatch = ConsumerEmtSimulatorGateV1(
        control=control, permit=replace(permit, endpoint_digest="f" * 64), risk=risk,
        now_wall_ns=lambda: _NOW, now_monotonic_ns=lambda: 20,
    )
    with pytest.raises(EmtSimulatorGateErrorV1):
        mismatch.start()


@pytest.mark.skipif(sys.platform != "linux", reason="requires Linux ownership checks")
def test_host_loader_requires_fence_and_protected_existing_namespace_state(tmp_path, monkeypatch):
    import emt_gateway.consumer_simulator_host_inputs_v1 as host

    release, keys = _release(Ed25519PrivateKey.generate())
    trust = parse_simulator_namespace_trust_v1(json.dumps({
        "schema_version": 1,
        "trusted_public_keys": [
            {"signer_id": name, "public_key_b64": base64.b64encode(key).decode()}
            for name, key in keys.items()
        ],
        "revoked_signer_ids": [],
    }))
    state_path = tmp_path / "state.json"
    store = FileNamespaceReleaseStateStoreV1(state_path)
    activated = NamespaceReleaseActivationCoordinatorV1(
        verifier=trust.verifier(), state_store=store
    ).activate(
        release, expected_scope=_scope(), expected_release_manifest_hash=release.release_manifest_hash,
        expected_trust_anchor_hash=trust.trust_anchor_hash, now_wall_ns=_NOW,
        bootstrap_approval=NamespaceReleaseBootstrapApprovalV1(
            "test-approval", release.content_hash, "test-operator", _NOW
        ),
    )
    data = document()
    data["profile"]["adapter_instance_id"] = "adapter-tradecore-a"
    data["profile"]["material_digest"]["release_manifest_hash"] = release.release_manifest_hash
    data["binding"]["account_id"] = "sim-account"
    registry = parse_simulator_profile_binding_v1(json.dumps(data))
    permit = EmtSimulatorPermitV1(
        "approval", "sim-account", "ca", "adapter-tradecore-a", "sim-profile", 101,
        "a" * 64, "b" * 64, activated.state.namespace_manifest_hash,
        activated.state.release_hash, _NOW + 1,
    )
    monkeypatch.setattr(host, "load_simulator_profile_binding_v1", lambda _: registry)
    monkeypatch.setattr(host, "load_simulator_namespace_trust_v1", lambda _: trust)
    monkeypatch.setattr(host, "load_emt_simulator_permit_v1", lambda _: permit)
    monkeypatch.setattr(host, "LinuxRootOwnedNamespaceFileV1", lambda _: SimpleNamespace(
        read=lambda: json.dumps(release.as_dict())
    ))
    real_lstat = Path.lstat
    directories = {state_path.parent, *state_path.parent.parents}
    monkeypatch.setattr(Path, "lstat", lambda path: (
        SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=os.geteuid())
        if path in directories else real_lstat(path)
    ))
    paths = SimulatorHostInputPathsV1(
        tmp_path / "profile", tmp_path / "release", tmp_path / "trust", state_path,
        tmp_path / "permit", tmp_path / "credentials", "emt-simulator",
    )
    calls = []
    kwargs = dict(
        paths=paths, binding_revision_id="binding", trading_day=_DAY,
        risk=RiskSnapshot("sim-policy", "sim-account", _DAY, "a" * 64, 10, 100, 1000, 0),
        require_ownership=lambda: calls.append("held"),
        now_wall_ns=lambda: _NOW, now_monotonic_ns=lambda: 20,
    )
    result = load_simulator_host_inputs_v1(**kwargs)
    assert calls == ["held", "held"]
    result.gate.start()
    assert result.gate.admission_permitted()
    assert result.secret_provider.provider_kind == "SYSTEMD_SIMULATOR_CREDENTIAL_V1"
    os.chmod(state_path, 0o644)
    with pytest.raises(SimulatorHostInputsErrorV1):
        load_simulator_host_inputs_v1(**kwargs)
