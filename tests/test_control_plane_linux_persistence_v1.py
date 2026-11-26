"""Focused contracts for W2's durable Linux control-plane components."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from tests.postgres_support import HAS_TEST_POSTGRES, PostgresTestDatabase
from xuanling_tradecore.control_plane import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    AdapterProfileRevisionV1,
    BindingActivationSpecV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ControlPrincipalV1,
    ControlRoleV1,
    InMemoryProvisioningApprovalRegistryV1,
    InMemoryProvisioningRegistryV1,
    InMemorySecretProviderV1,
    LinuxProtectedFileSecretProviderV1,
    PostgresProvisioningApprovalRegistryV1,
    PostgresProvisioningRegistryV1,
    ProfileMaterialDigestV1,
    ProfileStateV1,
    ProvisioningApprovalActionV1,
    ProvisioningApprovalV1,
    ProvisioningErrorV1,
    SecretLeasePurposeV1,
    SecretReferenceV1,
    build_linux_provisioning_server_v1,
)

_HASH = "a" * 64
_NOW = 1_700_000_000_000_000_000


def _profile() -> AdapterProfileRevisionV1:
    return AdapterProfileRevisionV1(
        identity=AdapterProfileIdentityV1(
            profile_id="profile-persistence",
            adapter_kind=AdapterKindV1.EMT,
            venue_id="EMT",
            adapter_instance_id="emt-persistence-instance",
            account_alias="sim-alias",
        ),
        revision=1,
        state=ProfileStateV1.PREPARED,
        secret_reference=SecretReferenceV1("LINUX_TEST", "secret-ref", "linux-emt"),
        material_digest=ProfileMaterialDigestV1(
            credential_digest=_HASH,
            endpoint_digest="b" * 64,
            envelope_digest="c" * 64,
            recipient_key_id="kid-1",
            mapping_manifest_hash="d" * 64,
            capability_manifest_hash="e" * 64,
            release_manifest_hash="f" * 64,
        ),
        created_wall_ns=_NOW,
    )


def _material() -> BrokerCredentialMaterialV1:
    return BrokerCredentialMaterialV1(
        broker_account="simulation-account",
        password="test-only-password",
        client_id="client-1",
        connection=BrokerConnectionV1("BROKER_TCP", "192.0.2.10", 18_000),
    )


@unittest.skipUnless(os.name == "posix", "Linux protected-file provider requires a POSIX filesystem")
class LinuxProtectedFileSecretProviderTests(unittest.TestCase):
    def test_store_lease_revoke_and_permission_fencing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            os.chmod(root, 0o700)
            provider = LinuxProtectedFileSecretProviderV1(secret_root=root)
            reference = provider.store(
                target="linux-emt",
                material=_material(),
                adapter_instance_id="emt-persistence-instance",
                account_scope="scope-hash",
                idempotency_key="request-hash",
            )
            same_reference = provider.store(
                target="linux-emt",
                material=_material(),
                adapter_instance_id="emt-persistence-instance",
                account_scope="scope-hash",
                idempotency_key="request-hash",
            )
            self.assertEqual(reference, same_reference)
            with provider.lease(
                reference=reference,
                purpose=SecretLeasePurposeV1(
                    adapter_instance_id="emt-persistence-instance",
                    account_scope="scope-hash",
                    operation="RUNTIME_BUILD",
                ),
                now_wall_ns=_NOW,
                ttl_wall_ns=1_000,
            ) as lease:
                self.assertEqual("simulation-account", lease.borrow(now_wall_ns=_NOW).broker_account)
            provider.revoke(reference=reference, reason_code="TEST_REVOKE")
            with self.assertRaisesRegex(ProvisioningErrorV1, "secret reference is invalid"):
                provider.lease(
                    reference=reference,
                    purpose=SecretLeasePurposeV1(
                        adapter_instance_id="emt-persistence-instance",
                        account_scope="scope-hash",
                        operation="RUNTIME_BUILD",
                    ),
                    now_wall_ns=_NOW,
                    ttl_wall_ns=1_000,
                )

    def test_rejects_group_readable_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            os.chmod(root, 0o755)
            with self.assertRaisesRegex(ProvisioningErrorV1, "accessible by group or other"):
                LinuxProtectedFileSecretProviderV1(secret_root=root)


class LinuxServerFactoryGuardTests(unittest.TestCase):
    def test_rejects_in_memory_registry_before_touching_tls_files(self) -> None:
        with self.assertRaisesRegex(ProvisioningErrorV1, "requires PostgreSQL registry"):
            build_linux_provisioning_server_v1(
                service=object(),
                peer_verifier=object(),
                approvals=InMemoryProvisioningApprovalRegistryV1(),
                registry=InMemoryProvisioningRegistryV1(),
                secret_providers={"EMT": InMemorySecretProviderV1()},
                listener=object(),
                tls_files=object(),
            )


@unittest.skipUnless(
    HAS_TEST_POSTGRES, "set EMT_GATEWAY_TEST_POSTGRES_DSN to run provisioning PostgreSQL tests"
)
class PostgresProvisioningRegistryContractTests(unittest.TestCase):
    def test_profile_binding_and_approval_survive_reopen(self) -> None:
        with PostgresTestDatabase() as database:
            migration_root = Path(__file__).parents[1] / "deploy" / "linux-control-plane" / "sql"
            migration_sql = (migration_root / "001_adapter_provisioning_v1.sql").read_text(
                encoding="utf-8"
            ).replace("xuanling_tradecore_cp", database.schema)
            approval_migration_sql = (migration_root / "002_adapter_provisioning_approval_facts_v1.sql").read_text(
                encoding="utf-8"
            ).replace("xuanling_tradecore_cp", database.schema)
            import psycopg

            with psycopg.connect(database.dsn) as connection:
                connection.execute(migration_sql)
                connection.execute(approval_migration_sql)
            registry = PostgresProvisioningRegistryV1(dsn=database.dsn, schema=database.schema)
            profile = _profile()
            created = registry.create_prepared(
                request_id="018f0000-0000-7000-8000-000000000701",
                payload_digest="1" * 64,
                profile=profile,
            )
            self.assertEqual(profile, created)
            validated = registry.transition(
                request_id="018f0000-0000-7000-8000-000000000702",
                payload_digest="2" * 64,
                profile_id=profile.identity.profile_id,
                revision=1,
                target_state=ProfileStateV1.VALIDATED,
                validation_evidence_id="evidence-1",
            )
            binding = registry.publish_binding(
                profile=validated,
                spec=BindingActivationSpecV1(
                    account_id="account-hash",
                    consumer_id="consumer-1",
                    consumer_account_id="strategy-account-1",
                    account_display_name="simulation",
                    strategy_ids=("strategy-1",),
                ),
                binding_revision_id="binding-1",
                effective_wall_ns=_NOW,
            )
            self.assertEqual((binding,), registry.active_bindings())
            reopened = PostgresProvisioningRegistryV1(dsn=database.dsn, schema=database.schema)
            self.assertEqual(validated, reopened.get_revision(profile_id=profile.identity.profile_id, revision=1))
            with self.assertRaises(ProvisioningErrorV1):
                reopened.active_binding_and_profile(binding_revision_id="binding-1", now_wall_ns=_NOW)
            active = registry.transition(
                request_id="018f0000-0000-7000-8000-000000000703",
                payload_digest="3" * 64,
                profile_id=profile.identity.profile_id,
                revision=1,
                target_state=ProfileStateV1.ACTIVE_DISARMED,
                validation_evidence_id="evidence-1",
                approval_id="approval-1",
            )
            selected = reopened.active_binding_and_profile(
                binding_revision_id="binding-1", now_wall_ns=_NOW
            )
            self.assertEqual((active, binding), (selected.profile, selected.binding))
            with self.assertRaises(ProvisioningErrorV1):
                reopened.active_binding_and_profile(binding_revision_id="binding-1", now_wall_ns=_NOW - 1)
            registry.retire_binding(
                profile_id=profile.identity.profile_id, revision=1, retired_wall_ns=_NOW + 10
            )
            with self.assertRaises(ProvisioningErrorV1):
                reopened.active_binding_and_profile(binding_revision_id="binding-1", now_wall_ns=_NOW + 10)
            approvals = PostgresProvisioningApprovalRegistryV1(dsn=database.dsn, schema=database.schema)
            approval = ProvisioningApprovalV1(
                approval_id="approval-1",
                action=ProvisioningApprovalActionV1.ACTIVATE,
                profile_id=profile.identity.profile_id,
                revision=1,
                approver=ControlPrincipalV1(
                    principal_id="approver-1",
                    roles=frozenset({ControlRoleV1.APPROVER}),
                    certificate_fingerprint="9" * 64,
                ),
                issued_wall_ns=_NOW,
                expires_wall_ns=_NOW + 1_000_000,
            )
            approvals.record(approval)
            self.assertEqual(
                approval.approver,
                approvals.require(
                    approval_id="approval-1",
                    action=ProvisioningApprovalActionV1.ACTIVATE,
                    profile_id=profile.identity.profile_id,
                    revision=1,
                    evaluated_wall_ns=_NOW + 1,
                ),
            )
