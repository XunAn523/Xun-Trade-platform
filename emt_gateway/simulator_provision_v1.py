"""Build operator-approved, simulator-only signed startup documents.

No uniqueness, echo or recovery probe is asserted. Private signing material
is supplied by the operator/provisioner and never appears in public documents.
"""

from __future__ import annotations

import base64
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from xuanling_tradecore.adapters.emt import (
    EmtOrderClientNamespaceScope,
    EmtOrderClientNamespaceVerifier,
    NamespaceReleaseActivationCoordinatorV1,
    NamespaceReleaseBootstrapApprovalV1,
    OrderClientIdNamespaceLeaseV1,
    SignedNamespaceReleaseV1,
    SignedOrderClientIdNamespaceLeaseV1,
    namespace_trust_anchor_hash_v1,
    sign_disarmed_profile,
)
from xuanling_tradecore.adapters.emt.mapping import load_builtin_emt_legacy_sim_manifest
from xuanling_tradecore.adapters.emt.namespace_release_v1 import NamespaceReleaseStateV1
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import BrokerCredentialMaterialV1
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.ems import canonical_broker_capabilities_hash

from .consumer_simulator_gate_v1 import EmtSimulatorPermitV1


class _CaptureState:
    state = None

    def load(self):
        return self.state

    def commit(self, state):
        self.state = state


def build_simulator_documents_v1(
    *, material: BrokerCredentialMaterialV1, trading_day: TradingDayEpoch,
    config_root: Path, runtime_root: Path, socket_path: Path, contract_root: Path,
    sdk_root: Path, sdk_hashes: dict[str, str], release_hash: str,
    private_key: Ed25519PrivateKey, now_wall_ns: int, expires_wall_ns: int,
    allowed_peers: list[list[int]], capacity: int = 10_000, client_range_start: int = 800_000_000,
) -> tuple[dict[str, dict], NamespaceReleaseStateV1]:
    account = "emt-simulator-ordinary"
    adapter, signer = "tradecore-emt-simulator", "tradecore-simulator-operator"
    client_id = int(material.client_id)
    certificate, revocation = str(config_root / "scope-public.json"), str(config_root / "scope-revocations.json")
    public_key = private_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    public_b64 = base64.b64encode(public_key).decode("ascii")
    base = load_builtin_emt_legacy_sim_manifest()
    ownership = replace(
        base.scope_ownership, ownership_id="operator-simulator-assignment", account_id=account,
        adapter_instance_id=adapter, adapter_client_scope=str(client_id), owner_instance_id="fengyu",
        config_source_hash=material.credential_digest(), signer_id=signer,
        valid_from_wall_ns=now_wall_ns - 1, valid_until_wall_ns=expires_wall_ns,
        uniqueness_probe_hash=sha256(b"NOT_PROVED_OPERATOR_ASSIGNED_SIMULATOR_ONLY").hexdigest(),
        uniqueness_probe_succeeded=False, revoked_at_wall_ns=None,
    )
    signed_scope = sign_disarmed_profile(
        profile_id="emt-simulator-signed-scope", ownerships=(ownership,), private_keys={signer: private_key},
        certificate_references={signer: certificate}, revocation_references={signer: revocation},
    )
    manifest = replace(base, scope_ownership=ownership)
    ns_scope = EmtOrderClientNamespaceScope(account, trading_day, client_id)
    lease = OrderClientIdNamespaceLeaseV1(
        scope=ns_scope, namespace_id="tradecore-simulator-orders", owner_system_id="tradecore-simulator",
        adapter_instance_id=adapter, range_start=client_range_start,
        range_end=client_range_start + capacity - 1, manifest_version=1,
        valid_from_wall_ns=now_wall_ns - 1, valid_until_wall_ns=expires_wall_ns,
        release_manifest_hash=release_hash, signer_id=signer,
    )
    signed_lease = SignedOrderClientIdNamespaceLeaseV1(lease, certificate, revocation, "AA==")
    signed_lease = replace(signed_lease, signature_b64=base64.b64encode(private_key.sign(signed_lease.canonical_payload())).decode("ascii"))
    anchor = namespace_trust_anchor_hash_v1({signer: public_key}, revoked_signer_ids=frozenset())
    release = SignedNamespaceReleaseV1(
        release_id="operator-simulator-release", release_epoch=1, release_manifest_hash=release_hash,
        trust_anchor_hash=anchor, valid_from_wall_ns=now_wall_ns - 1, valid_until_wall_ns=expires_wall_ns,
        own=signed_lease, foreign=(), signer_id=signer, certificate_reference=certificate,
        revocation_reference=revocation, signature_b64="AA==",
    )
    release = replace(release, signature_b64=base64.b64encode(private_key.sign(release.canonical_payload())).decode("ascii"))
    store = _CaptureState()
    activated = NamespaceReleaseActivationCoordinatorV1(
        verifier=EmtOrderClientNamespaceVerifier({signer: public_key}, revoked_signer_ids=frozenset()), state_store=store,
    ).activate(
        release, expected_scope=ns_scope, expected_release_manifest_hash=release_hash,
        expected_trust_anchor_hash=anchor, now_wall_ns=now_wall_ns,
        bootstrap_approval=NamespaceReleaseBootstrapApprovalV1("operator-simulator-bootstrap", release.content_hash, "fengyu", now_wall_ns),
    )
    profile = {
        "schema_version": 1,
        "profile": {
            "profile_id": "emt-simulator-profile", "adapter_instance_id": adapter,
            "account_alias": "operator-designated-emt-simulator", "revision": 1,
            "secret_reference": {"provider_kind": "SYSTEMD_SIMULATOR_CREDENTIAL_V1", "reference_id": "emt-simulator-ordinary", "target": "emt-simulator-host"},
            "material_digest": {
                "credential_digest": material.credential_digest(), "endpoint_digest": material.connection.endpoint_digest(),
                "envelope_digest": sha256(b"SYSTEMD_SINGLE_ACCOUNT_CREDENTIAL_NOT_JWE").hexdigest(),
                "recipient_key_id": "systemd-simulator-local", "mapping_manifest_hash": manifest.manifest_hash,
                "capability_manifest_hash": canonical_broker_capabilities_hash(manifest.capabilities), "release_manifest_hash": release_hash,
            },
            "created_wall_ns": now_wall_ns, "validation_evidence_id": "operator-simulator-config-only-no-recovery-claim",
            "approval_id": "fengyu-simulator-submit-only",
        },
        "binding": {
            "binding_revision_id": "emt-simulator-binding", "profile_id": "emt-simulator-profile", "profile_revision": 1,
            "account_id": account, "consumer_id": "tradecore-simulator-consumer", "consumer_account_id": "emt-sim-ordinary",
            "account_display_name": "EMT simulated ordinary account", "strategy_ids": ["consumer-sim-strategy"],
            "authorization_epoch": 1, "effective_wall_ns": now_wall_ns,
        },
    }
    permit = EmtSimulatorPermitV1(
        "fengyu-simulator-submit-only", account, "emt-sim-ordinary", adapter, "emt-simulator-profile", client_id,
        material.credential_digest(), material.connection.endpoint_digest(), activated.directory.manifest_hash, release.content_hash, expires_wall_ns,
    )
    runtime = {
        "schema_version": 1, "runtime_root": str(runtime_root), "socket_path": str(socket_path),
        "contract_root": str(contract_root), "sdk_root": str(sdk_root), "trading_day": trading_day.value,
        "allowed_peers": allowed_peers, "capacity": capacity, "max_pending": min(512, capacity),
        "symbols": ["000001"], "owner_instance_id": "fengyu", "host_id": "tradecore-linux-simulator",
    }
    documents = {
        "profile.json": profile, "runtime.json": runtime, "permit.json": {"schema_version": 1, **asdict(permit)},
        "namespace-release.json": release.as_dict(),
        "namespace-trust.json": {"schema_version": 1, "trusted_public_keys": [{"signer_id": signer, "public_key_b64": public_b64}], "revoked_signer_ids": []},
        "scope-profile.json": signed_scope.as_dict(),
        "scope-public.json": {"schema_version": 1, "signer_id": signer, "algorithm": "Ed25519", "public_key_b64": public_b64, "valid_from_wall_ns": now_wall_ns - 1, "valid_until_wall_ns": expires_wall_ns},
        "scope-revocations.json": {"schema_version": 1, "authority": signer, "revoked_signer_ids": [], "generated_wall_ns": now_wall_ns},
        "sdk-hashes.json": sdk_hashes,
    }
    return documents, activated.state
