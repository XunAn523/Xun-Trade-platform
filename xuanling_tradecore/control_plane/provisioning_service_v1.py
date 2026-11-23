"""Orchestration service for sealed Adapter profile provisioning V1."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from typing import Mapping, Protocol

from cryptography.hazmat.primitives.asymmetric import rsa

from ..domain._validation import require_nonnegative
from .provisioning_activation_v1 import (
    ConsumerBindingSnapshotPublisherV1,
    ProfileSelectionGateV1,
)
from .provisioning_approval_v1 import (
    ProvisioningApprovalActionV1,
    ProvisioningApprovalRegistryV1,
)
from .provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileRevisionV1,
    BindingActivationSpecV1,
    ControlPrincipalV1,
    ControlRoleV1,
    PrepareProfileRequestV1,
    ProfileActionRequestV1,
    ProfileMaterialDigestV1,
    ProfileReceiptV1,
    ProfileStateV1,
    ProfileValidationEvidenceV1,
    ProvisioningErrorV1,
    SecretReferenceV1,
)
from .provisioning_jwe_v1 import (
    CredentialRecipientV1,
    VerifiedCredentialEnvelopeV1,
    decrypt_credential_envelope_v1,
)
from .provisioning_registry_v1 import ProvisioningRegistryV1
from .provisioning_secret_v1 import SecretProviderV1


def _digest_text(parts: Mapping[str, str]) -> str:
    return sha256(
        "\n".join(f"{key}={parts[key]}" for key in sorted(parts)).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class LocalCredentialRecipientV1:
    """A private key is available only on the actual credential host."""

    recipient: CredentialRecipientV1
    private_key: rsa.RSAPrivateKey  # deliberately not repr

    def __post_init__(self) -> None:
        if self.private_key.key_size < 2_048:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "recipient private key is too small")


@dataclass(frozen=True)
class OpaqueRelayStoreReceiptV1:
    """De-identified response from the QMT Windows Edge credential recipient."""

    secret_reference: SecretReferenceV1
    material_digest: ProfileMaterialDigestV1


class OpaqueQmtCredentialRelayV1(Protocol):
    """mTLS relay to QMT Edge.  Linux must never decrypt QMT envelopes."""

    def store_opaque(
        self,
        *,
        request: PrepareProfileRequestV1,
        principal: ControlPrincipalV1,
        recipient: CredentialRecipientV1,
        now_wall_ns: int,
    ) -> OpaqueRelayStoreReceiptV1:
        """Forward ciphertext to Edge and return only its opaque reference/digests."""

        ...

    def revoke_opaque(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        """Request QMT Edge to revoke its local opaque reference over mTLS."""

        ...


class ProfileValidatorV1(Protocol):
    """A DISARMED-only validator.  Implementations must not submit/cancel orders."""

    def validate(
        self, *, profile: AdapterProfileRevisionV1, evaluated_wall_ns: int
    ) -> ProfileValidationEvidenceV1:
        """Return complete capability/recovery/host-health evidence."""

        ...


class ProfileRetirementGateV1(Protocol):
    """Check working, unknown, reservation, and recovery evidence before retirement."""

    def incompleteness_reasons(self, profile: AdapterProfileRevisionV1) -> tuple[str, ...]:
        """Return empty only when it is safe to retire the exact profile revision."""

        ...


class ProvisioningControlServiceV1:
    """Fail-closed, non-hot-path lifecycle coordinator.

    No method opens an Adapter session, submits, cancels, or transitions an
    account to READY.  Native/read-only validation is supplied by a separate
    explicitly DISARMED validator port.
    """

    def __init__(
        self,
        *,
        recipients: Mapping[AdapterKindV1, CredentialRecipientV1],
        local_recipients: Mapping[str, LocalCredentialRecipientV1],
        secret_providers: Mapping[str, SecretProviderV1],
        registry: ProvisioningRegistryV1,
        validator: ProfileValidatorV1,
        selection_gate: ProfileSelectionGateV1,
        binding_publisher: ConsumerBindingSnapshotPublisherV1,
        retirement_gate: ProfileRetirementGateV1,
        qmt_relay: OpaqueQmtCredentialRelayV1 | None = None,
        approvals: ProvisioningApprovalRegistryV1,
    ) -> None:
        if set(recipients) != set(AdapterKindV1):
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "recipient set is incomplete")
        self._recipients = dict(recipients)
        self._local_recipients = dict(local_recipients)
        self._secret_providers = dict(secret_providers)
        self._registry = registry
        self._validator = validator
        self._selection_gate = selection_gate
        self._binding_publisher = binding_publisher
        self._retirement_gate = retirement_gate
        self._qmt_relay = qmt_relay
        self._approvals = approvals
        self._lock = RLock()

    def credential_recipient(
        self, *, principal: ControlPrincipalV1, adapter_kind: AdapterKindV1, now_wall_ns: int
    ) -> CredentialRecipientV1:
        principal.require_role(ControlRoleV1.ADAPTER_PROVISIONER)
        require_nonnegative(now_wall_ns, "recipient query time")
        recipient = self._recipients[adapter_kind]
        if now_wall_ns >= recipient.not_after_wall_ns:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "recipient key is expired")
        return recipient

    def prepare(
        self,
        *,
        principal: ControlPrincipalV1,
        request: PrepareProfileRequestV1,
        now_wall_ns: int,
    ) -> ProfileReceiptV1:
        """Validate one sealed profile and create a new immutable PREPARED revision."""

        principal.require_role(ControlRoleV1.ADAPTER_PROVISIONER)
        require_nonnegative(now_wall_ns, "prepare time")
        payload_digest = self._prepare_payload_digest(request)
        with self._lock:
            existing = self._registry.get_request(
                request_id=request.request_id, payload_digest=payload_digest
            )
            if existing is not None:
                profile = self._registry.get_revision(
                    profile_id=existing.profile_id, revision=existing.revision
                )
                return self._receipt(
                    request_id=request.request_id,
                    profile=profile,
                    state=existing.state,
                    authorization_epoch=existing.authorization_epoch,
                )
            recipient = self.credential_recipient(
                principal=principal,
                adapter_kind=request.identity.adapter_kind,
                now_wall_ns=now_wall_ns,
            )
            if request.expected_credential_key_id != recipient.key_id:
                raise ProvisioningErrorV1("CONTROL_RECIPIENT", "credential key ID does not match")
            if request.identity.adapter_kind is AdapterKindV1.QMT:
                stored = self._prepare_qmt(
                    principal=principal,
                    request=request,
                    recipient=recipient,
                    now_wall_ns=now_wall_ns,
                )
            else:
                stored = self._prepare_local(
                    principal=principal,
                    request=request,
                    recipient=recipient,
                    now_wall_ns=now_wall_ns,
                )
            revision = self._next_revision(request.identity.profile_id)
            profile = AdapterProfileRevisionV1(
                identity=request.identity,
                revision=revision,
                state=ProfileStateV1.PREPARED,
                secret_reference=stored.secret_reference,
                material_digest=stored.material_digest,
                created_wall_ns=now_wall_ns,
            )
            try:
                persisted = self._registry.create_prepared(
                    request_id=request.request_id,
                    payload_digest=payload_digest,
                    profile=profile,
                )
            except Exception:
                self._revoke_after_failed_prepare(stored.secret_reference)
                raise
            return self._receipt(request_id=request.request_id, profile=persisted)

    def validate(
        self,
        *,
        provisioner: ControlPrincipalV1,
        approver: ControlPrincipalV1,
        request: ProfileActionRequestV1,
        now_wall_ns: int,
    ) -> ProfileReceiptV1:
        """Run an injected DISARMED validator and record VALIDATED or REJECTED."""

        self._require_independent_roles(provisioner=provisioner, approver=approver)
        self._require_recorded_approval(
            approval_id=request.approval_id,
            action=ProvisioningApprovalActionV1.VALIDATE,
            profile_id=request.profile_id,
            revision=request.revision,
            now_wall_ns=now_wall_ns,
            approver=approver,
        )
        require_nonnegative(now_wall_ns, "validation time")
        payload_digest = self._action_payload_digest(request, action="validate")
        with self._lock:
            existing = self._registry.get_request(
                request_id=request.request_id, payload_digest=payload_digest
            )
            if existing is not None:
                profile = self._registry.get_revision(
                    profile_id=existing.profile_id, revision=existing.revision
                )
                return self._receipt(
                    request_id=request.request_id, profile=profile, state=existing.state
                )
            profile = self._registry.get_revision(
                profile_id=request.profile_id, revision=request.revision
            )
            if profile.state is not ProfileStateV1.PREPARED:
                raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "profile cannot be validated")
            evidence = self._validator.validate(profile=profile, evaluated_wall_ns=now_wall_ns)
            target = ProfileStateV1.VALIDATED if evidence.passed else ProfileStateV1.REJECTED
            transitioned = self._registry.transition(
                request_id=request.request_id,
                payload_digest=payload_digest,
                profile_id=profile.identity.profile_id,
                revision=profile.revision,
                target_state=target,
                validation_evidence_id=evidence.evidence_id if evidence.passed else None,
            )
            return self._receipt(request_id=request.request_id, profile=transitioned)

    def activate(
        self,
        *,
        approver: ControlPrincipalV1,
        request: ProfileActionRequestV1,
        binding_spec: BindingActivationSpecV1,
        now_wall_ns: int,
    ) -> ProfileReceiptV1:
        """Publish an ACTIVE_DISARMED binding successor after exact validation evidence."""

        approver.require_role(ControlRoleV1.APPROVER)
        if request.approval_id is None:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "activation requires approval")
        require_nonnegative(now_wall_ns, "activation time")
        self._require_recorded_approval(
            approval_id=request.approval_id,
            action=ProvisioningApprovalActionV1.ACTIVATE,
            profile_id=request.profile_id,
            revision=request.revision,
            now_wall_ns=now_wall_ns,
            approver=approver,
        )
        payload_digest = self._action_payload_digest(
            request,
            action="activate",
            binding_spec=binding_spec,
        )
        with self._lock:
            existing = self._registry.get_request(
                request_id=request.request_id, payload_digest=payload_digest
            )
            if existing is not None:
                profile = self._registry.get_revision(
                    profile_id=existing.profile_id, revision=existing.revision
                )
                return self._receipt(
                    request_id=request.request_id,
                    profile=profile,
                    state=existing.state,
                    authorization_epoch=existing.authorization_epoch,
                )
            profile = self._registry.get_revision(
                profile_id=request.profile_id, revision=request.revision
            )
            if profile.state is not ProfileStateV1.VALIDATED:
                raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "profile cannot be activated")
            self._selection_gate(profile)
            binding = self._registry.publish_binding(
                profile=profile,
                spec=binding_spec,
                binding_revision_id=f"{profile.identity.profile_id}:r{profile.revision}",
                effective_wall_ns=now_wall_ns,
            )
            self._binding_publisher.publish(
                authorization_epoch=binding.authorization_epoch,
                profile=profile,
                spec=binding_spec,
                binding_revision_id=binding.binding_revision_id,
            )
            transitioned = self._registry.transition(
                request_id=request.request_id,
                payload_digest=payload_digest,
                profile_id=profile.identity.profile_id,
                revision=profile.revision,
                target_state=ProfileStateV1.ACTIVE_DISARMED,
                validation_evidence_id=profile.validation_evidence_id,
                approval_id=request.approval_id,
                authorization_epoch=binding.authorization_epoch,
            )
            return self._receipt(
                request_id=request.request_id,
                profile=transitioned,
                authorization_epoch=binding.authorization_epoch,
            )

    def retire(
        self,
        *,
        provisioner: ControlPrincipalV1,
        approver: ControlPrincipalV1,
        request: ProfileActionRequestV1,
        now_wall_ns: int,
    ) -> ProfileReceiptV1:
        """Retire only fully reconciled profiles and revoke their secret reference."""

        self._require_independent_roles(provisioner=provisioner, approver=approver)
        if request.approval_id is None or request.reason_code is None:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "retirement requires approval and reason")
        require_nonnegative(now_wall_ns, "retirement time")
        self._require_recorded_approval(
            approval_id=request.approval_id,
            action=ProvisioningApprovalActionV1.RETIRE,
            profile_id=request.profile_id,
            revision=request.revision,
            now_wall_ns=now_wall_ns,
            approver=approver,
        )
        payload_digest = self._action_payload_digest(request, action="retire")
        with self._lock:
            existing = self._registry.get_request(
                request_id=request.request_id, payload_digest=payload_digest
            )
            if existing is not None:
                profile = self._registry.get_revision(
                    profile_id=existing.profile_id, revision=existing.revision
                )
                return self._receipt(
                    request_id=request.request_id, profile=profile, state=existing.state
                )
            active = self._registry.get_revision(
                profile_id=request.profile_id, revision=request.revision
            )
            if active.state not in {ProfileStateV1.ACTIVE_DISARMED, ProfileStateV1.RETIRING}:
                raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "profile cannot be retired")
            if active.state is ProfileStateV1.ACTIVE_DISARMED:
                reasons = tuple(self._retirement_gate.incompleteness_reasons(active))
                if reasons:
                    raise ProvisioningErrorV1(
                        "CONTROL_RETIREMENT_BLOCKED", "profile retirement is blocked"
                    )
                retiring = self._registry.transition(
                    request_id=f"{request.request_id}:retiring",
                    payload_digest=_digest_text({"action": "retiring", "parent": payload_digest}),
                    profile_id=active.identity.profile_id,
                    revision=active.revision,
                    target_state=ProfileStateV1.RETIRING,
                    validation_evidence_id=active.validation_evidence_id,
                    approval_id=request.approval_id,
                )
            else:
                retiring = active
            retired_bindings = self._registry.retire_binding(
                profile_id=active.identity.profile_id,
                revision=active.revision,
                retired_wall_ns=now_wall_ns,
            )
            for binding in retired_bindings:
                self._binding_publisher.retire(
                    authorization_epoch=binding.authorization_epoch,
                    binding_revision_id=binding.binding_revision_id,
                )
            self._revoke_after_failed_prepare(retiring.secret_reference, request.reason_code)
            retired = self._registry.transition(
                request_id=request.request_id,
                payload_digest=payload_digest,
                profile_id=retiring.identity.profile_id,
                revision=retiring.revision,
                target_state=ProfileStateV1.RETIRED,
                validation_evidence_id=retiring.validation_evidence_id,
                approval_id=request.approval_id,
                retired_wall_ns=now_wall_ns,
            )
            return self._receipt(request_id=request.request_id, profile=retired)

    def _prepare_local(
        self,
        *,
        principal: ControlPrincipalV1,
        request: PrepareProfileRequestV1,
        recipient: CredentialRecipientV1,
        now_wall_ns: int,
    ) -> OpaqueRelayStoreReceiptV1:
        local = self._local_recipients.get(recipient.target)
        provider = self._secret_providers.get(recipient.target)
        if local is None or provider is None or local.recipient != recipient:
            raise ProvisioningErrorV1(
                "CONTROL_RECIPIENT", "local credential recipient is unavailable"
            )
        verified = decrypt_credential_envelope_v1(
            envelope=request.credential_envelope,
            private_key=local.private_key,
            expected_recipient=recipient,
            expected_principal=principal,
            expected_identity=request.identity,
            expected_profile_not_after_wall_ns=request.not_after_wall_ns,
            now_wall_ns=now_wall_ns,
        )
        self._registry.claim_jti(
            jti=verified.claims.jti,
            target=recipient.target,
            request_id=request.request_id,
            envelope_digest=verified.envelope_digest,
        )
        reference = provider.store(
            target=recipient.target,
            material=verified.material,
            adapter_instance_id=request.identity.adapter_instance_id,
            account_scope=request.identity.profile_id,
            idempotency_key=f"{recipient.target}:{verified.claims.jti}",
        )
        return OpaqueRelayStoreReceiptV1(
            secret_reference=reference,
            material_digest=self._material_digest(verified=verified, request=request),
        )

    def _prepare_qmt(
        self,
        *,
        principal: ControlPrincipalV1,
        request: PrepareProfileRequestV1,
        recipient: CredentialRecipientV1,
        now_wall_ns: int,
    ) -> OpaqueRelayStoreReceiptV1:
        if self._qmt_relay is None:
            raise ProvisioningErrorV1(
                "CONTROL_RECIPIENT", "QMT Edge credential relay is unavailable"
            )
        if recipient.target in self._local_recipients:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "QMT recipient must not be local")
        result = self._qmt_relay.store_opaque(
            request=request,
            principal=principal,
            recipient=recipient,
            now_wall_ns=now_wall_ns,
        )
        if result.secret_reference.target != recipient.target:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "QMT relay target does not match")
        return result

    def _next_revision(self, profile_id: str) -> int:
        revision = 1
        while True:
            try:
                self._registry.get_revision(profile_id=profile_id, revision=revision)
            except ProvisioningErrorV1 as exc:
                if exc.code == "CONTROL_NOT_FOUND":
                    return revision
                raise
            revision += 1

    def _revoke_after_failed_prepare(
        self, reference: SecretReferenceV1, reason: str = "PREPARE_FAILED"
    ) -> None:
        provider = self._secret_providers.get(reference.target)
        if provider is not None:
            try:
                provider.revoke(reference=reference, reason_code=reason)
            except ProvisioningErrorV1:
                # The original prepare/retirement failure remains authoritative;
                # real deployments must emit an out-of-band redacted alert.
                pass
            return
        if self._qmt_relay is not None:
            try:
                self._qmt_relay.revoke_opaque(reference=reference, reason_code=reason)
            except ProvisioningErrorV1:
                pass

    @staticmethod
    def _material_digest(
        *, verified: VerifiedCredentialEnvelopeV1, request: PrepareProfileRequestV1
    ) -> ProfileMaterialDigestV1:
        return ProfileMaterialDigestV1(
            credential_digest=verified.material.credential_digest(),
            endpoint_digest=verified.material.connection.endpoint_digest(),
            envelope_digest=verified.envelope_digest,
            recipient_key_id=verified.claims.recipient_key_id,
            mapping_manifest_hash=request.attributes["mapping_manifest_hash"],
            capability_manifest_hash=request.attributes["capability_manifest_hash"],
            release_manifest_hash=request.attributes["release_manifest_hash"],
        )

    @staticmethod
    def _prepare_payload_digest(request: PrepareProfileRequestV1) -> str:
        return _digest_text(
            {
                "adapter_instance_id": request.identity.adapter_instance_id,
                "adapter_kind": request.identity.adapter_kind.value,
                "approval_reference": request.approval_reference,
                "attributes": "|".join(
                    f"{key}={value}" for key, value in request.attributes.items()
                ),
                "credential_envelope_digest": sha256(
                    request.credential_envelope.encode("ascii")
                ).hexdigest(),
                "expected_credential_key_id": request.expected_credential_key_id,
                "not_after_wall_ns": str(request.not_after_wall_ns),
                "profile_id": request.identity.profile_id,
                "request_id": request.request_id,
            }
        )

    @staticmethod
    def _action_payload_digest(
        request: ProfileActionRequestV1,
        *,
        action: str,
        binding_spec: BindingActivationSpecV1 | None = None,
    ) -> str:
        payload = {
            "action": action,
            "approval_id": request.approval_id or "",
            "profile_id": request.profile_id,
            "reason_code": request.reason_code or "",
            "request_id": request.request_id,
            "revision": str(request.revision),
        }
        if binding_spec is not None:
            payload.update(
                {
                    "account_id": binding_spec.account_id,
                    "consumer_account_id": binding_spec.consumer_account_id,
                    "consumer_id": binding_spec.consumer_id,
                    "strategies": "|".join(binding_spec.strategy_ids),
                }
            )
        return _digest_text(payload)

    @staticmethod
    def _require_independent_roles(
        *, provisioner: ControlPrincipalV1, approver: ControlPrincipalV1
    ) -> None:
        provisioner.require_role(ControlRoleV1.ADAPTER_PROVISIONER)
        approver.require_role(ControlRoleV1.APPROVER)
        if provisioner.principal_id == approver.principal_id:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "maker and approver must differ")

    def _require_recorded_approval(
        self,
        *,
        approval_id: str | None,
        action: ProvisioningApprovalActionV1,
        profile_id: str,
        revision: int,
        now_wall_ns: int,
        approver: ControlPrincipalV1,
    ) -> None:
        if approval_id is None:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "action requires approval")
        recorded = self._approvals.require(
            approval_id=approval_id,
            action=action,
            profile_id=profile_id,
            revision=revision,
            evaluated_wall_ns=now_wall_ns,
        )
        if recorded != approver:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "approval principal does not match")

    @staticmethod
    def _receipt(
        *,
        request_id: str,
        profile: AdapterProfileRevisionV1,
        state: ProfileStateV1 | None = None,
        authorization_epoch: int | None = None,
    ) -> ProfileReceiptV1:
        return ProfileReceiptV1(
            request_id=request_id,
            profile_id=profile.identity.profile_id,
            revision=profile.revision,
            adapter_kind=profile.identity.adapter_kind,
            state=state or profile.state,
            material_digest=profile.material_digest,
            approval_required=(state or profile.state) is not ProfileStateV1.ACTIVE_DISARMED,
            authorization_epoch=authorization_epoch,
        )
