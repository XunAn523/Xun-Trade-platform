from contextlib import contextmanager
from dataclasses import replace

import pytest
from xuanling_tradecore.control_plane.postgres_provisioning_v1 import PostgresProvisioningRegistryV1
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    BindingActivationSpecV1,
    ProfileStateV1,
    ProvisioningErrorV1,
)
from xuanling_tradecore.control_plane.provisioning_registry_v1 import InMemoryProvisioningRegistryV1
from xuanling_tradecore.control_plane.provisioning_secret_v1 import InMemorySecretProviderV1

from test_provisioned_execution_factory_v1 import _profile


def test_atomic_in_memory_loader_rejects_early_and_retired_binding():
    profile = _profile(InMemorySecretProviderV1(provider_kind="EMT_TEST"))
    registry = InMemoryProvisioningRegistryV1()
    prepared = replace(profile, state=ProfileStateV1.PREPARED, validation_evidence_id=None, approval_id=None)
    registry.create_prepared(request_id="r1", payload_digest="1" * 64, profile=prepared)
    validated = registry.transition(
        request_id="r2", payload_digest="2" * 64, profile_id=profile.identity.profile_id,
        revision=1, target_state=ProfileStateV1.VALIDATED, validation_evidence_id="evidence-1",
    )
    binding = registry.publish_binding(
        profile=validated,
        spec=BindingActivationSpecV1("acct", "consumer", "ca", "display", ("strategy",)),
        binding_revision_id="bind-1", effective_wall_ns=10,
    )
    with pytest.raises(ProvisioningErrorV1):
        registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=10)
    active = registry.transition(
        request_id="r3", payload_digest="3" * 64, profile_id=profile.identity.profile_id,
        revision=1, target_state=ProfileStateV1.ACTIVE_DISARMED,
        validation_evidence_id="evidence-1", approval_id="approval-1",
    )
    with pytest.raises(ProvisioningErrorV1):
        registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=9)
    snapshot = registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=10)
    assert snapshot.profile == active and snapshot.binding == binding
    registry.retire_binding(profile_id=profile.identity.profile_id, revision=1, retired_wall_ns=20)
    with pytest.raises(ProvisioningErrorV1):
        registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=21)


def test_postgres_loader_uses_one_join_and_fails_closed_on_missing_row():
    profile = _profile(InMemorySecretProviderV1(provider_kind="EMT_TEST"))
    identity = profile.identity
    secret = profile.secret_reference
    digest = profile.material_digest
    binding_row = (
        "bind-1", identity.profile_id, 1, "acct", "consumer", "ca", "display", ["strategy"],
        2, 10, None,
    )
    profile_row = (
        identity.profile_id, identity.adapter_kind.value, identity.venue_id,
        identity.adapter_instance_id, identity.account_alias, 1, profile.state.value,
        secret.provider_kind, secret.reference_id, secret.target, digest.recipient_key_id,
        digest.credential_digest, digest.endpoint_digest, digest.envelope_digest,
        digest.mapping_manifest_hash, digest.capability_manifest_hash, digest.release_manifest_hash,
        profile.created_wall_ns, profile.validation_evidence_id, profile.approval_id, None,
    )

    class Cursor:
        row = None
        def __enter__(self): return self
        def __exit__(self, *_args): pass
        def execute(self, query, params=()):
            self.query, self.params = query, params
        def fetchone(self): return self.row

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): pass
        def cursor(self): return cursor

    class Registry(PostgresProvisioningRegistryV1):
        @contextmanager
        def connection(self):
            yield Connection()

    registry = Registry(dsn="test")
    with pytest.raises(ProvisioningErrorV1):
        registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=10)
    assert cursor.params == ("bind-1", 10, "ACTIVE_DISARMED", "ACTIVE_DISARMED")
    assert "p.current_revision = r.profile_revision" in cursor.query
    assert "b.retired_at IS NULL" in cursor.query
    cursor.row = binding_row + profile_row
    snapshot = registry.active_binding_and_profile(binding_revision_id="bind-1", now_wall_ns=10)
    assert snapshot.profile == profile and snapshot.binding.account_id == "acct"
