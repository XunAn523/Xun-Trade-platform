from __future__ import annotations

import asyncio
import json
import socket
import ssl
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, ResponseReceived, StreamEnded
from xuanling_tradecore.adapters.qmt.provisioning_edge_v1 import QmtEdgeCredentialReceiverV1
from xuanling_tradecore.control_plane.provisioning_activation_v1 import (
    ConsumerBindingSnapshotPublisherV1,
)
from xuanling_tradecore.control_plane.provisioning_approval_v1 import (
    InMemoryProvisioningApprovalRegistryV1,
    ProvisioningApprovalActionV1,
    ProvisioningApprovalV1,
)
from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    BindingActivationSpecV1,
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ControlPrincipalV1,
    ControlRoleV1,
    CredentialClaimsV1,
    PrepareProfileRequestV1,
    ProfileActionRequestV1,
    ProfileStateV1,
    ProfileValidationEvidenceV1,
    ProvisioningErrorV1,
)
from xuanling_tradecore.control_plane.provisioning_http_v1 import (
    MtlsPeerVerifierV1,
    ProvisioningHttpRequestV1,
    ProvisioningHttpRouterV1,
    TlsConnectionFactsV1,
)
from xuanling_tradecore.control_plane.provisioning_jwe_v1 import (
    CredentialRecipientV1,
    encrypt_credential_envelope_v1,
    format_rfc3339_wall_ns_v1,
)
from xuanling_tradecore.control_plane.provisioning_registry_v1 import (
    InMemoryProvisioningRegistryV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import (
    InMemorySecretProviderV1,
    SecretLeasePurposeV1,
)
from xuanling_tradecore.control_plane.provisioning_server_v1 import (
    ProvisioningHttp2ServerConfigV1,
    ProvisioningHttp2ServerV1,
    build_provisioning_server_ssl_context_v1,
)
from xuanling_tradecore.control_plane.provisioning_service_v1 import (
    LocalCredentialRecipientV1,
    ProvisioningControlServiceV1,
)

_NOW = 1_800_000_000_000_000_000
_HASH = "a" * 64


def _request_id(number: int) -> str:
    return f"018f0000-0000-7000-8000-{number:012d}"


class _PassingValidator:
    def validate(self, *, profile, evaluated_wall_ns):
        return ProfileValidationEvidenceV1(
            evidence_id=f"evidence:{profile.identity.profile_id}",
            evidence_digest="b" * 64,
            passed=True,
            reason_codes=(),
            evaluated_wall_ns=evaluated_wall_ns,
        )


class _BlockedValidator:
    def validate(self, *, profile, evaluated_wall_ns):
        del profile
        return ProfileValidationEvidenceV1(
            evidence_id="evidence:blocked",
            evidence_digest="c" * 64,
            passed=False,
            reason_codes=("CAPABILITY_MISSING",),
            evaluated_wall_ns=evaluated_wall_ns,
        )


class _SelectionGate:
    def __init__(self) -> None:
        self.selected: list[str] = []

    def __call__(self, profile) -> None:
        self.selected.append(profile.identity.adapter_instance_id)


class _RetirementGate:
    def __init__(self, reasons: tuple[str, ...] = ()) -> None:
        self.reasons = reasons

    def incompleteness_reasons(self, profile) -> tuple[str, ...]:
        del profile
        return self.reasons


class _FailOncePrepareRegistry(InMemoryProvisioningRegistryV1):
    """Simulate a durable registry outage after secret storage completed."""

    def __init__(self) -> None:
        super().__init__()
        self._failed = False

    def create_prepared(self, **kwargs):
        if not self._failed:
            self._failed = True
            raise RuntimeError("simulated registry outage")
        return super().create_prepared(**kwargs)


def _certificate(
    *,
    subject_name: str,
    public_key: rsa.RSAPublicKey,
    issuer_name: x509.Name,
    issuer_key: rsa.RSAPrivateKey,
    is_ca: bool,
    extended_usage: ObjectIdentifier | None = None,
) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject_name)]))
        .issuer_name(issuer_name)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    if extended_usage is not None:
        builder = builder.add_extension(x509.ExtendedKeyUsage([extended_usage]), critical=False)
    if not is_ca:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False
        )
    return builder.sign(private_key=issuer_key, algorithm=hashes.SHA256())


def _write_certificate_pair(
    path: Path, stem: str, certificate: x509.Certificate, key: rsa.RSAPrivateKey
) -> tuple[str, str]:
    certificate_path = path / f"{stem}.crt"
    key_path = path / f"{stem}.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(certificate_path), str(key_path)


def _unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class ProvisioningControlPlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provisioner = ControlPrincipalV1(
            "provisioner-a", frozenset({ControlRoleV1.ADAPTER_PROVISIONER}), "1" * 64
        )
        self.approver = ControlPrincipalV1(
            "approver-b", frozenset({ControlRoleV1.APPROVER}), "2" * 64
        )
        self.emt_private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.qmt_private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.emt_recipient = CredentialRecipientV1(
            target="linux-emt-secret-provider/test-host",
            key_id="emt-k1",
            public_key=self.emt_private.public_key(),
            not_after_wall_ns=_NOW + 86_400_000_000_000,
        )
        self.qmt_recipient = CredentialRecipientV1(
            target="windows-qmt-edge-secret-provider/edge-a",
            key_id="qmt-k1",
            public_key=self.qmt_private.public_key(),
            not_after_wall_ns=_NOW + 86_400_000_000_000,
        )
        self.emt_provider = InMemorySecretProviderV1(provider_kind="EMT_TEST")
        self.qmt_provider = InMemorySecretProviderV1(provider_kind="QMT_TEST")
        self.qmt_receiver = QmtEdgeCredentialReceiverV1(
            local_recipient=LocalCredentialRecipientV1(self.qmt_recipient, self.qmt_private),
            secret_provider=self.qmt_provider,
        )
        self.registry = InMemoryProvisioningRegistryV1()
        self.approvals = InMemoryProvisioningApprovalRegistryV1()
        self.publisher = ConsumerBindingSnapshotPublisherV1()
        self.selection = _SelectionGate()
        self.retirement = _RetirementGate()
        self.service = ProvisioningControlServiceV1(
            recipients={
                AdapterKindV1.EMT: self.emt_recipient,
                AdapterKindV1.QMT: self.qmt_recipient,
            },
            local_recipients={
                self.emt_recipient.target: LocalCredentialRecipientV1(
                    self.emt_recipient, self.emt_private
                )
            },
            secret_providers={self.emt_recipient.target: self.emt_provider},
            registry=self.registry,
            validator=_PassingValidator(),
            selection_gate=self.selection,
            binding_publisher=self.publisher,
            retirement_gate=self.retirement,
            qmt_relay=self.qmt_receiver,
            approvals=self.approvals,
        )

    def _identity(self, kind: AdapterKindV1, suffix: str) -> AdapterProfileIdentityV1:
        return AdapterProfileIdentityV1(
            profile_id=f"profile-{suffix}",
            adapter_kind=kind,
            venue_id=kind.value,
            adapter_instance_id=f"{kind.value.lower()}-instance-{suffix}",
            account_alias=f"alias-{suffix}",
        )

    @staticmethod
    def _material() -> BrokerCredentialMaterialV1:
        return BrokerCredentialMaterialV1(
            broker_account="sim-account",
            password="test-password-never-log",
            client_id="client-a",
            connection=BrokerConnectionV1("BROKER_TCP", "192.0.2.10", 18000),
        )

    def _prepare_request(
        self,
        *,
        kind: AdapterKindV1,
        suffix: str,
        request_number: int,
        principal: ControlPrincipalV1 | None = None,
    ) -> PrepareProfileRequestV1:
        recipient = self.emt_recipient if kind is AdapterKindV1.EMT else self.qmt_recipient
        identity = self._identity(kind, suffix)
        issuer = principal or self.provisioner
        claims = CredentialClaimsV1(
            jti=_request_id(100 + request_number),
            issuer=issuer.principal_id,
            audience=recipient.target,
            issued_at_wall_ns=_NOW,
            not_before_wall_ns=_NOW,
            expires_at_wall_ns=_NOW + 60_000_000_000,
            recipient_key_id=recipient.key_id,
        )
        envelope = encrypt_credential_envelope_v1(
            recipient=recipient,
            claims=claims,
            identity=identity,
            material=self._material(),
            profile_not_after_wall_ns=_NOW + 3_600_000_000_000,
        )
        return PrepareProfileRequestV1(
            request_id=_request_id(request_number),
            identity=identity,
            credential_envelope=envelope,
            expected_credential_key_id=recipient.key_id,
            not_after_wall_ns=_NOW + 3_600_000_000_000,
            approval_reference="change-1",
            attributes={
                "mapping_manifest_hash": _HASH,
                "capability_manifest_hash": "b" * 64,
                "release_manifest_hash": "c" * 64,
            },
        )

    def _record_approval(
        self,
        *,
        approval_id: str,
        action: ProvisioningApprovalActionV1,
        profile_id: str,
        revision: int,
    ) -> None:
        self.approvals.record(
            ProvisioningApprovalV1(
                approval_id=approval_id,
                action=action,
                profile_id=profile_id,
                revision=revision,
                approver=self.approver,
                issued_wall_ns=_NOW,
                expires_wall_ns=_NOW + 3_600_000_000_000,
            )
        )

    def _activate(self, prepared, suffix: str, number: int):
        validate_approval_id = f"approval-validate-{suffix}"
        self._record_approval(
            approval_id=validate_approval_id,
            action=ProvisioningApprovalActionV1.VALIDATE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        validated = self.service.validate(
            provisioner=self.provisioner,
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(300 + number),
                prepared.profile_id,
                prepared.revision,
                approval_id=validate_approval_id,
            ),
            now_wall_ns=_NOW + 1,
        )
        self.assertEqual(ProfileStateV1.VALIDATED, validated.state)
        activate_approval_id = f"approval-{suffix}"
        self._record_approval(
            approval_id=activate_approval_id,
            action=ProvisioningApprovalActionV1.ACTIVATE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        return self.service.activate(
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(400 + number),
                prepared.profile_id,
                prepared.revision,
                approval_id=activate_approval_id,
            ),
            binding_spec=BindingActivationSpecV1(
                account_id=f"account-{suffix}",
                consumer_id="consumer-a",
                consumer_account_id=f"consumer-{suffix}",
                account_display_name=f"Account {suffix}",
                strategy_ids=(f"strategy-{suffix}",),
            ),
            now_wall_ns=_NOW + 2,
        )

    def test_emt_prepare_validate_activate_retire_has_no_secret_receipt(self) -> None:
        request = self._prepare_request(kind=AdapterKindV1.EMT, suffix="emt", request_number=1)
        prepared = self.service.prepare(
            principal=self.provisioner, request=request, now_wall_ns=_NOW + 1
        )
        self.assertEqual(ProfileStateV1.PREPARED, prepared.state)
        self.assertNotIn("test-password-never-log", repr(prepared))
        self.assertNotIn("192.0.2.10", repr(prepared))
        self.assertEqual(
            prepared,
            self.service.prepare(principal=self.provisioner, request=request, now_wall_ns=_NOW + 1),
        )
        active = self._activate(prepared, "emt", 1)
        self.assertEqual(ProfileStateV1.ACTIVE_DISARMED, active.state)
        self.assertEqual(2, active.authorization_epoch)
        self.assertEqual(("emt-instance-emt",), tuple(self.selection.selected))
        snapshot = self.publisher.current()
        self.assertEqual(2, snapshot.authorization_epoch)
        self.assertEqual("EMT", snapshot.accounts[0].venue_id)
        self._record_approval(
            approval_id="approval-retire",
            action=ProvisioningApprovalActionV1.RETIRE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        retired = self.service.retire(
            provisioner=self.provisioner,
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(501),
                prepared.profile_id,
                prepared.revision,
                "approval-retire",
                "ROTATE",
            ),
            now_wall_ns=_NOW + 3,
        )
        self.assertEqual(ProfileStateV1.RETIRED, retired.state)
        self.assertFalse(self.publisher.current().accounts[0].active)
        profile = self.registry.get_revision(
            profile_id=prepared.profile_id, revision=prepared.revision
        )
        with self.assertRaises(ProvisioningErrorV1) as error:
            self.emt_provider.lease(
                reference=profile.secret_reference,
                purpose=SecretLeasePurposeV1("emt-instance-emt", "profile-emt", "LOGIN"),
                now_wall_ns=_NOW + 4,
                ttl_wall_ns=1,
            )
        self.assertEqual("CONTROL_SECRET_UNAVAILABLE", error.exception.code)

    def test_replay_wrong_principal_and_unfinished_retirement_fail_closed(self) -> None:
        request = self._prepare_request(kind=AdapterKindV1.EMT, suffix="failure", request_number=2)
        wrong = ControlPrincipalV1(
            "other-provisioner", frozenset({ControlRoleV1.ADAPTER_PROVISIONER}), "3" * 64
        )
        with self.assertRaises(ProvisioningErrorV1) as error:
            self.service.prepare(principal=wrong, request=request, now_wall_ns=_NOW + 1)
        self.assertEqual("CONTROL_ENVELOPE", error.exception.code)
        prepared = self.service.prepare(
            principal=self.provisioner, request=request, now_wall_ns=_NOW + 1
        )
        replay = replace(request, request_id=_request_id(3))
        with self.assertRaises(ProvisioningErrorV1) as error:
            self.service.prepare(principal=self.provisioner, request=replay, now_wall_ns=_NOW + 1)
        self.assertEqual("CONTROL_ENVELOPE_REPLAY", error.exception.code)
        self._activate(prepared, "failure", 2)
        self.retirement.reasons = ("WORKING_ORDER",)
        self._record_approval(
            approval_id="approval-retire",
            action=ProvisioningApprovalActionV1.RETIRE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        with self.assertRaises(ProvisioningErrorV1) as error:
            self.service.retire(
                provisioner=self.provisioner,
                approver=self.approver,
                request=ProfileActionRequestV1(
                    _request_id(502),
                    prepared.profile_id,
                    prepared.revision,
                    "approval-retire",
                    "ROTATE",
                ),
                now_wall_ns=_NOW + 3,
            )
        self.assertEqual("CONTROL_RETIREMENT_BLOCKED", error.exception.code)

    def test_prepare_recovers_exact_request_after_secret_then_registry_failure(self) -> None:
        registry = _FailOncePrepareRegistry()
        service = ProvisioningControlServiceV1(
            recipients={
                AdapterKindV1.EMT: self.emt_recipient,
                AdapterKindV1.QMT: self.qmt_recipient,
            },
            local_recipients={
                self.emt_recipient.target: LocalCredentialRecipientV1(
                    self.emt_recipient, self.emt_private
                )
            },
            secret_providers={self.emt_recipient.target: self.emt_provider},
            registry=registry,
            validator=_PassingValidator(),
            selection_gate=self.selection,
            binding_publisher=self.publisher,
            retirement_gate=self.retirement,
            qmt_relay=self.qmt_receiver,
            approvals=self.approvals,
        )
        request = self._prepare_request(kind=AdapterKindV1.EMT, suffix="retry", request_number=31)
        with self.assertRaisesRegex(RuntimeError, "registry outage"):
            service.prepare(principal=self.provisioner, request=request, now_wall_ns=_NOW + 1)
        prepared = service.prepare(
            principal=self.provisioner, request=request, now_wall_ns=_NOW + 2
        )
        profile = registry.get_revision(profile_id=prepared.profile_id, revision=prepared.revision)
        with self.emt_provider.lease(
            reference=profile.secret_reference,
            purpose=SecretLeasePurposeV1("emt-instance-retry", "profile-retry", "LOGIN"),
            now_wall_ns=_NOW + 3,
            ttl_wall_ns=1,
        ) as lease:
            self.assertEqual("sim-account", lease.borrow(now_wall_ns=_NOW + 3).broker_account)

    def test_activate_recovers_after_binding_commit_before_snapshot_publication(self) -> None:
        prepared = self.service.prepare(
            principal=self.provisioner,
            request=self._prepare_request(
                kind=AdapterKindV1.EMT, suffix="resume", request_number=32
            ),
            now_wall_ns=_NOW + 1,
        )
        self._record_approval(
            approval_id="approval-validate-resume",
            action=ProvisioningApprovalActionV1.VALIDATE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        validated = self.service.validate(
            provisioner=self.provisioner,
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(332),
                prepared.profile_id,
                prepared.revision,
                approval_id="approval-validate-resume",
            ),
            now_wall_ns=_NOW + 2,
        )
        spec = BindingActivationSpecV1(
            account_id="account-resume",
            consumer_id="consumer-a",
            consumer_account_id="consumer-resume",
            account_display_name="Account resume",
            strategy_ids=("strategy-resume",),
        )
        self.registry.publish_binding(
            profile=self.registry.get_revision(
                profile_id=validated.profile_id, revision=validated.revision
            ),
            spec=spec,
            binding_revision_id=f"{validated.profile_id}:r{validated.revision}",
            effective_wall_ns=_NOW + 3,
        )
        self.assertEqual((), self.publisher.current().accounts)
        self._record_approval(
            approval_id="approval-activate-resume",
            action=ProvisioningApprovalActionV1.ACTIVATE,
            profile_id=validated.profile_id,
            revision=validated.revision,
        )
        active = self.service.activate(
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(432),
                validated.profile_id,
                validated.revision,
                approval_id="approval-activate-resume",
            ),
            binding_spec=spec,
            now_wall_ns=_NOW + 3,
        )
        self.assertEqual(ProfileStateV1.ACTIVE_DISARMED, active.state)
        self.assertEqual(
            "consumer-resume", self.publisher.current().accounts[0].consumer_account_id
        )

    def test_qmt_envelope_is_decrypted_only_by_edge_and_multi_binding_isolated(self) -> None:
        emt = self.service.prepare(
            principal=self.provisioner,
            request=self._prepare_request(
                kind=AdapterKindV1.EMT, suffix="multi-emt", request_number=4
            ),
            now_wall_ns=_NOW + 1,
        )
        qmt = self.service.prepare(
            principal=self.provisioner,
            request=self._prepare_request(
                kind=AdapterKindV1.QMT, suffix="multi-qmt", request_number=5
            ),
            now_wall_ns=_NOW + 1,
        )
        self.assertEqual(
            "EMT_TEST",
            self.registry.get_revision(
                profile_id=emt.profile_id, revision=1
            ).secret_reference.provider_kind,
        )
        self.assertEqual(
            "QMT_TEST",
            self.registry.get_revision(
                profile_id=qmt.profile_id, revision=1
            ).secret_reference.provider_kind,
        )
        self._activate(emt, "multi-emt", 3)
        self._activate(qmt, "multi-qmt", 4)
        accounts = self.publisher.current().accounts
        self.assertEqual({"EMT", "QMT"}, {account.venue_id for account in accounts})
        self.assertEqual(3, self.publisher.current().authorization_epoch)

    def test_validation_failure_never_publishes_binding(self) -> None:
        self.service = ProvisioningControlServiceV1(
            recipients={
                AdapterKindV1.EMT: self.emt_recipient,
                AdapterKindV1.QMT: self.qmt_recipient,
            },
            local_recipients={
                self.emt_recipient.target: LocalCredentialRecipientV1(
                    self.emt_recipient, self.emt_private
                )
            },
            secret_providers={self.emt_recipient.target: self.emt_provider},
            registry=self.registry,
            validator=_BlockedValidator(),
            selection_gate=self.selection,
            binding_publisher=self.publisher,
            retirement_gate=self.retirement,
            qmt_relay=self.qmt_receiver,
            approvals=self.approvals,
        )
        prepared = self.service.prepare(
            principal=self.provisioner,
            request=self._prepare_request(
                kind=AdapterKindV1.EMT, suffix="blocked", request_number=6
            ),
            now_wall_ns=_NOW + 1,
        )
        self._record_approval(
            approval_id="approval-validate-blocked",
            action=ProvisioningApprovalActionV1.VALIDATE,
            profile_id=prepared.profile_id,
            revision=prepared.revision,
        )
        rejected = self.service.validate(
            provisioner=self.provisioner,
            approver=self.approver,
            request=ProfileActionRequestV1(
                _request_id(306),
                prepared.profile_id,
                prepared.revision,
                approval_id="approval-validate-blocked",
            ),
            now_wall_ns=_NOW + 2,
        )
        self.assertEqual(ProfileStateV1.REJECTED, rejected.state)
        self.assertEqual((), self.publisher.current().accounts)

    def test_http2_mtls_router_uses_handshake_certificate_not_identity_header(self) -> None:
        certificate = b"test-provisioning-client-certificate"
        http_principal = ControlPrincipalV1(
            "provisioner-http",
            frozenset({ControlRoleV1.ADAPTER_PROVISIONER}),
            sha256(certificate).hexdigest(),
        )
        router = ProvisioningHttpRouterV1(
            service=self.service,
            peer_verifier=MtlsPeerVerifierV1(
                {http_principal.certificate_fingerprint: http_principal}
            ),
            approvals=self.approvals,
        )
        response = router.handle(
            request=ProvisioningHttpRequestV1(
                method="GET",
                path="/control/v1/credential-recipients/EMT",
                body={},
                tls_facts=TlsConnectionFactsV1("TLSv1.3", "h2", certificate, True),
            ),
            now_wall_ns=_NOW,
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual("RSA-OAEP-256", response.body["alg"])
        with self.assertRaises(ProvisioningErrorV1) as error:
            TlsConnectionFactsV1("TLSv1.2", "h2", certificate, True)
        self.assertEqual("CONTROL_AUTH", error.exception.code)

    def test_router_prepares_only_jwe_and_never_returns_broker_material(self) -> None:
        certificate = b"test-provisioning-client-certificate"
        principal = ControlPrincipalV1(
            "provisioner-http",
            frozenset({ControlRoleV1.ADAPTER_PROVISIONER}),
            sha256(certificate).hexdigest(),
        )
        router = ProvisioningHttpRouterV1(
            service=self.service,
            peer_verifier=MtlsPeerVerifierV1({principal.certificate_fingerprint: principal}),
            approvals=self.approvals,
        )
        prepared = self._prepare_request(
            kind=AdapterKindV1.EMT,
            suffix="router",
            request_number=41,
            principal=principal,
        )
        response = router.handle(
            request=ProvisioningHttpRequestV1(
                method="POST",
                path="/control/v1/adapter-profiles:prepare",
                body={
                    "request_id": prepared.request_id,
                    "profile_id": prepared.identity.profile_id,
                    "adapter_kind": prepared.identity.adapter_kind.value,
                    "adapter_instance_id": prepared.identity.adapter_instance_id,
                    "account_alias": prepared.identity.account_alias,
                    "credential_envelope": prepared.credential_envelope,
                    "expected_credential_kid": prepared.expected_credential_key_id,
                    "not_after": format_rfc3339_wall_ns_v1(prepared.not_after_wall_ns),
                    "approval_reference": prepared.approval_reference,
                    "attributes": dict(prepared.attributes),
                },
                tls_facts=TlsConnectionFactsV1("TLSv1.3", "h2", certificate, True),
            ),
            now_wall_ns=_NOW + 1,
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual("PREPARED", response.body["state"])
        self.assertNotIn("test-password-never-log", repr(response.body))
        self.assertNotIn("192.0.2.10", repr(response.body))
        self.assertNotIn("credential_envelope", response.body)

    def test_openapi_declares_only_sealed_credential_carrier(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        contract = json.loads(
            (repository / "contracts" / "provisioning" / "v1" / "openapi.json").read_text(
                encoding="utf-8"
            )
        )
        schemas = contract["components"]["schemas"]
        properties = schemas["PrepareProfileRequest"]["properties"]
        self.assertIn("credential_envelope", properties)
        self.assertTrue(
            {"password", "broker_account", "host", "port", "endpoint"}.isdisjoint(properties)
        )
        self.assertEqual(
            "date-time", schemas["CredentialRecipient"]["properties"]["not_after"]["format"]
        )

    def test_tls13_mtls_http2_listener_serves_control_plane_without_consumer_socket(self) -> None:
        async def exercise() -> tuple[str, dict[str, object]]:
            ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "TradeCore Test CA")])
            ca_certificate = _certificate(
                subject_name="TradeCore Test CA",
                public_key=ca_key.public_key(),
                issuer_name=ca_name,
                issuer_key=ca_key,
                is_ca=True,
            )
            server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            server_certificate = _certificate(
                subject_name="localhost",
                public_key=server_key.public_key(),
                issuer_name=ca_name,
                issuer_key=ca_key,
                is_ca=False,
                extended_usage=ExtendedKeyUsageOID.SERVER_AUTH,
            )
            client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            client_certificate = _certificate(
                subject_name="provisioner-http",
                public_key=client_key.public_key(),
                issuer_name=ca_name,
                issuer_key=ca_key,
                is_ca=False,
                extended_usage=ExtendedKeyUsageOID.CLIENT_AUTH,
            )
            client_der = client_certificate.public_bytes(serialization.Encoding.DER)
            principal = ControlPrincipalV1(
                "provisioner-http",
                frozenset({ControlRoleV1.ADAPTER_PROVISIONER}),
                sha256(client_der).hexdigest(),
            )
            router = ProvisioningHttpRouterV1(
                service=self.service,
                peer_verifier=MtlsPeerVerifierV1({principal.certificate_fingerprint: principal}),
                approvals=self.approvals,
            )
            with tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                ca_path = root / "ca.crt"
                ca_path.write_bytes(ca_certificate.public_bytes(serialization.Encoding.PEM))
                server_certificate_path, server_key_path = _write_certificate_pair(
                    root, "server", server_certificate, server_key
                )
                client_certificate_path, client_key_path = _write_certificate_pair(
                    root, "client", client_certificate, client_key
                )
                server = ProvisioningHttp2ServerV1(
                    router=router,
                    config=ProvisioningHttp2ServerConfigV1(
                        host="127.0.0.1", port=_unused_loopback_port()
                    ),
                    ssl_context=build_provisioning_server_ssl_context_v1(
                        certificate_file=server_certificate_path,
                        private_key_file=server_key_path,
                        client_ca_file=str(ca_path),
                    ),
                )
                await server.start()
                host, port = server.addresses[0]
                client_context = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH, cafile=str(ca_path)
                )
                client_context.minimum_version = ssl.TLSVersion.TLSv1_3
                client_context.maximum_version = ssl.TLSVersion.TLSv1_3
                client_context.set_alpn_protocols(["h2"])
                client_context.load_cert_chain(client_certificate_path, client_key_path)
                try:
                    reader, writer = await asyncio.open_connection(
                        host, port, ssl=client_context, server_hostname="localhost"
                    )
                    ssl_object = writer.get_extra_info("ssl_object")
                    self.assertIsNotNone(ssl_object)
                    self.assertEqual("h2", ssl_object.selected_alpn_protocol())
                    connection = H2Connection(
                        config=H2Configuration(client_side=True, header_encoding="ascii")
                    )
                    connection.initiate_connection()
                    writer.write(connection.data_to_send())
                    await writer.drain()
                    connection.receive_data(await reader.read(65_536))
                    connection.send_headers(
                        1,
                        [
                            (":method", "GET"),
                            (":scheme", "https"),
                            (":authority", "localhost"),
                            (":path", "/control/v1/credential-recipients/EMT"),
                        ],
                        end_stream=True,
                    )
                    writer.write(connection.data_to_send())
                    await writer.drain()
                    status = ""
                    body = bytearray()
                    complete = False
                    while not complete:
                        for event in connection.receive_data(await reader.read(65_536)):
                            if isinstance(event, ResponseReceived):
                                headers = {
                                    _h2_text(name): _h2_text(value) for name, value in event.headers
                                }
                                status = headers[":status"]
                            elif isinstance(event, DataReceived):
                                body.extend(event.data)
                                connection.acknowledge_received_data(
                                    event.flow_controlled_length, event.stream_id
                                )
                            elif isinstance(event, StreamEnded):
                                complete = True
                        pending = connection.data_to_send()
                        if pending:
                            writer.write(pending)
                            await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    response_body = json.loads(body.decode("ascii"))
                    if not isinstance(response_body, dict):
                        raise AssertionError("control response is not an object")
                    return status, dict(response_body)
                finally:
                    await server.close()

        status, body = asyncio.run(exercise())
        self.assertEqual("200", status)
        self.assertEqual("RSA-OAEP-256", body["alg"])
        self.assertIn("not_after", body)
        self.assertNotIn("not_after_wall_ns", body)


def _h2_text(value: str | bytes) -> str:
    return value.decode("ascii") if isinstance(value, bytes) else value


if __name__ == "__main__":
    unittest.main()
