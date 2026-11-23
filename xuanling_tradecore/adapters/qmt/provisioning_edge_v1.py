"""Windows QMT Edge-local receiver for opaque Provisioning V1 envelopes.

This module contains no QMT SDK import and no network listener.  A Windows
Edge mTLS management adapter supplies verified principals and delegates to this
receiver, which is the only component allowed to decrypt QMT credentials.
"""

from __future__ import annotations

from threading import RLock

from ...control_plane.provisioning_contracts_v1 import (
    ControlPrincipalV1,
    PrepareProfileRequestV1,
    ProfileMaterialDigestV1,
    ProvisioningErrorV1,
    SecretReferenceV1,
)
from ...control_plane.provisioning_jwe_v1 import (
    CredentialRecipientV1,
    decrypt_credential_envelope_v1,
)
from ...control_plane.provisioning_secret_v1 import SecretProviderV1
from ...control_plane.provisioning_service_v1 import (
    LocalCredentialRecipientV1,
    OpaqueRelayStoreReceiptV1,
)


class QmtEdgeCredentialReceiverV1:
    """Decrypt/store/revoke only on the QMT Windows Edge credential host."""

    def __init__(
        self,
        *,
        local_recipient: LocalCredentialRecipientV1,
        secret_provider: SecretProviderV1,
    ) -> None:
        self._local_recipient = local_recipient
        self._secret_provider = secret_provider
        self._used_jtis: dict[tuple[str, str], tuple[str, str]] = {}
        self._lock = RLock()

    @property
    def recipient(self) -> CredentialRecipientV1:
        return self._local_recipient.recipient

    def store_opaque(
        self,
        *,
        request: PrepareProfileRequestV1,
        principal: ControlPrincipalV1,
        recipient: CredentialRecipientV1,
        now_wall_ns: int,
    ) -> OpaqueRelayStoreReceiptV1:
        """Accept ciphertext after the Edge mTLS boundary verified the caller."""

        if recipient != self._local_recipient.recipient:
            raise ProvisioningErrorV1("CONTROL_RECIPIENT", "QMT credential target is invalid")
        verified = decrypt_credential_envelope_v1(
            envelope=request.credential_envelope,
            private_key=self._local_recipient.private_key,
            expected_recipient=recipient,
            expected_principal=principal,
            expected_identity=request.identity,
            expected_profile_not_after_wall_ns=request.not_after_wall_ns,
            now_wall_ns=now_wall_ns,
        )
        with self._lock:
            key = (verified.claims.jti, recipient.target)
            owner = (request.request_id, verified.envelope_digest)
            previous = self._used_jtis.get(key)
            if previous is not None and previous != owner:
                raise ProvisioningErrorV1("CONTROL_ENVELOPE_REPLAY", "credential envelope replay")
            self._used_jtis[key] = owner
            reference = self._secret_provider.store(
                target=recipient.target,
                material=verified.material,
                adapter_instance_id=request.identity.adapter_instance_id,
                account_scope=request.identity.profile_id,
                idempotency_key=f"{recipient.target}:{verified.claims.jti}",
            )
        return OpaqueRelayStoreReceiptV1(
            secret_reference=reference,
            material_digest=ProfileMaterialDigestV1(
                credential_digest=verified.material.credential_digest(),
                endpoint_digest=verified.material.connection.endpoint_digest(),
                envelope_digest=verified.envelope_digest,
                recipient_key_id=verified.claims.recipient_key_id,
                mapping_manifest_hash=request.attributes["mapping_manifest_hash"],
                capability_manifest_hash=request.attributes["capability_manifest_hash"],
                release_manifest_hash=request.attributes["release_manifest_hash"],
            ),
        )

    def revoke_opaque(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        self._secret_provider.revoke(reference=reference, reason_code=reason_code)
