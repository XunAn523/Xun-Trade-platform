"""Transport-neutral HTTP/2 mTLS request adapter for Provisioning V1.

An HTTP/2 server or a mutually-authenticated local ingress supplies TLS facts
from its completed handshake.  This module validates the facts and dispatches
only JSON control-plane requests; it never trusts user-supplied identity
headers and it never implements a data-plane or broker endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import Mapping, cast

from .provisioning_approval_v1 import (
    ProvisioningApprovalActionV1,
    ProvisioningApprovalRegistryV1,
    ProvisioningApprovalV1,
)
from .provisioning_contracts_v1 import (
    AdapterKindV1,
    AdapterProfileIdentityV1,
    BindingActivationSpecV1,
    ControlPrincipalV1,
    PrepareProfileRequestV1,
    ProfileActionRequestV1,
    ProfileReceiptV1,
    ProvisioningErrorV1,
)
from .provisioning_jwe_v1 import (
    format_rfc3339_wall_ns_v1,
    parse_rfc3339_wall_ns_v1,
)
from .provisioning_service_v1 import ProvisioningControlServiceV1


@dataclass(frozen=True)
class TlsConnectionFactsV1:
    """Facts obtained from a completed TLS handshake, not from HTTP headers."""

    tls_version: str
    alpn_protocol: str
    peer_certificate_der: bytes = field(repr=False)
    chain_verified: bool

    def __post_init__(self) -> None:
        if self.tls_version != "TLSv1.3" or self.alpn_protocol != "h2" or not self.chain_verified:
            raise ProvisioningErrorV1(
                "CONTROL_AUTH", "control transport is not TLS 1.3 mTLS HTTP/2"
            )
        if not self.peer_certificate_der:
            raise ProvisioningErrorV1("CONTROL_AUTH", "control client certificate is unavailable")


class MtlsPeerVerifierV1:
    """Map an approved certificate fingerprint to a local control principal."""

    def __init__(
        self, principals_by_certificate_fingerprint: Mapping[str, ControlPrincipalV1]
    ) -> None:
        if not principals_by_certificate_fingerprint:
            raise ProvisioningErrorV1("CONTROL_AUTH", "control trust map is empty")
        self._principals = dict(principals_by_certificate_fingerprint)

    def verify(self, facts: TlsConnectionFactsV1) -> ControlPrincipalV1:
        fingerprint = sha256(facts.peer_certificate_der).hexdigest()
        try:
            principal = self._principals[fingerprint]
        except KeyError as exc:
            raise ProvisioningErrorV1(
                "CONTROL_AUTH", "control certificate is not authorized"
            ) from exc
        if principal.certificate_fingerprint != fingerprint:
            raise ProvisioningErrorV1("CONTROL_AUTH", "control certificate mapping is inconsistent")
        return principal


@dataclass(frozen=True)
class ProvisioningHttpRequestV1:
    method: str
    path: str
    body: Mapping[str, object]
    tls_facts: TlsConnectionFactsV1


@dataclass(frozen=True)
class ProvisioningHttpResponseV1:
    status_code: int
    body: Mapping[str, object]


class ProvisioningHttpRouterV1:
    """Bounded routing adapter suitable for a real HTTP/2 mTLS server integration."""

    def __init__(
        self,
        *,
        service: ProvisioningControlServiceV1,
        peer_verifier: MtlsPeerVerifierV1,
        approvals: ProvisioningApprovalRegistryV1,
    ) -> None:
        self._service = service
        self._peer_verifier = peer_verifier
        self._approvals = approvals

    def handle(
        self, *, request: ProvisioningHttpRequestV1, now_wall_ns: int
    ) -> ProvisioningHttpResponseV1:
        """Dispatch one already-size-bounded JSON request with a verified TLS peer."""

        try:
            principal = self._peer_verifier.verify(request.tls_facts)
            if request.method == "GET" and request.path.startswith(
                "/control/v1/credential-recipients/"
            ):
                kind = AdapterKindV1(request.path.rsplit("/", maxsplit=1)[-1])
                recipient = self._service.credential_recipient(
                    principal=principal, adapter_kind=kind, now_wall_ns=now_wall_ns
                )
                return self._success(
                    {
                        "target": recipient.target,
                        "kid": recipient.key_id,
                        "alg": "RSA-OAEP-256",
                        "enc": "A256GCM",
                        "not_after": format_rfc3339_wall_ns_v1(recipient.not_after_wall_ns),
                    }
                )
            if request.method == "POST" and request.path == "/control/v1/adapter-profiles:prepare":
                receipt = self._service.prepare(
                    principal=principal,
                    request=_prepare_request(request.body),
                    now_wall_ns=now_wall_ns,
                )
                return self._success(_receipt_body(receipt))
            if request.method == "POST" and request.path == "/control/v1/approvals":
                approval = _approval_request(request.body, principal=principal)
                self._approvals.record(approval)
                return self._success({"approval_id": approval.approval_id})
            parsed = _profile_action_path(request.path)
            if request.method == "POST" and parsed is not None:
                profile_id, revision, action = parsed
                action_request = _action_request(
                    request.body, profile_id=profile_id, revision=revision
                )
                approval_action = ProvisioningApprovalActionV1(action.upper())
                if action == "validate":
                    approver = self._require_approver(
                        action_request=action_request,
                        action=approval_action,
                        now_wall_ns=now_wall_ns,
                    )
                    receipt = self._service.validate(
                        provisioner=principal,
                        approver=approver,
                        request=action_request,
                        now_wall_ns=now_wall_ns,
                    )
                    return self._success(_receipt_body(receipt))
                if action == "retire":
                    approver = self._require_approver(
                        action_request=action_request,
                        action=approval_action,
                        now_wall_ns=now_wall_ns,
                    )
                    receipt = self._service.retire(
                        provisioner=principal,
                        approver=approver,
                        request=action_request,
                        now_wall_ns=now_wall_ns,
                    )
                    return self._success(_receipt_body(receipt))
            if (
                request.method == "POST"
                and request.path == "/control/v1/consumer-bindings:activate"
            ):
                action_request = _action_request(request.body)
                approver = self._require_approver(
                    action_request=action_request,
                    action=ProvisioningApprovalActionV1.ACTIVATE,
                    now_wall_ns=now_wall_ns,
                )
                receipt = self._service.activate(
                    approver=approver,
                    request=action_request,
                    binding_spec=_binding_spec(request.body),
                    now_wall_ns=now_wall_ns,
                )
                return self._success(_receipt_body(receipt))
            raise ProvisioningErrorV1("CONTROL_NOT_FOUND", "control route is unavailable")
        except ProvisioningErrorV1 as exc:
            request_id = request.body.get("request_id")
            return ProvisioningHttpResponseV1(
                status_code=_status_for(exc.code),
                body={
                    "request_id": request_id if isinstance(request_id, str) else None,
                    "code": exc.code,
                    "retryable": exc.code in {"CONTROL_SECRET_UNAVAILABLE", "CONTROL_RUNTIME"},
                },
            )

    def _require_approver(
        self,
        *,
        action_request: ProfileActionRequestV1,
        action: ProvisioningApprovalActionV1,
        now_wall_ns: int,
    ) -> ControlPrincipalV1:
        if action_request.approval_id is None:
            raise ProvisioningErrorV1("CONTROL_APPROVAL", "control action lacks approval")
        return self._approvals.require(
            approval_id=action_request.approval_id,
            action=action,
            profile_id=action_request.profile_id,
            revision=action_request.revision,
            evaluated_wall_ns=now_wall_ns,
        )

    @staticmethod
    def _success(body: Mapping[str, object]) -> ProvisioningHttpResponseV1:
        return ProvisioningHttpResponseV1(status_code=200, body=dict(body))


def _body_object(body: Mapping[str, object]) -> Mapping[str, object]:
    return body


def _text(body: Mapping[str, object], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str):
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control text field is invalid")
    return value


def _optional_text(body: Mapping[str, object], key: str) -> str | None:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control optional text field is invalid")
    return value


def _positive_int(body: Mapping[str, object], key: str) -> int:
    value = body.get(key)
    if type(value) is not int or value <= 0:
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control integer field is invalid")
    return value


def _prepare_request(body: Mapping[str, object]) -> PrepareProfileRequestV1:
    source = _body_object(body)
    kind = AdapterKindV1(_text(source, "adapter_kind"))
    attributes_raw = source.get("attributes")
    if not isinstance(attributes_raw, Mapping):
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control attributes are invalid")
    attributes = cast(Mapping[str, str], attributes_raw)
    return PrepareProfileRequestV1(
        request_id=_text(source, "request_id"),
        identity=AdapterProfileIdentityV1(
            profile_id=_text(source, "profile_id"),
            adapter_kind=kind,
            venue_id=kind.value,
            adapter_instance_id=_text(source, "adapter_instance_id"),
            account_alias=_text(source, "account_alias"),
        ),
        credential_envelope=_text(source, "credential_envelope"),
        expected_credential_key_id=_text(source, "expected_credential_kid"),
        not_after_wall_ns=parse_rfc3339_wall_ns_v1(source.get("not_after")),
        approval_reference=_text(source, "approval_reference"),
        attributes=attributes,
    )


def _action_request(
    body: Mapping[str, object], *, profile_id: str | None = None, revision: int | None = None
) -> ProfileActionRequestV1:
    source = _body_object(body)
    return ProfileActionRequestV1(
        request_id=_text(source, "request_id"),
        profile_id=profile_id or _text(source, "profile_id"),
        revision=revision or _positive_int(source, "revision"),
        approval_id=_optional_text(source, "approval_id"),
        reason_code=_optional_text(source, "reason_code"),
    )


def _binding_spec(body: Mapping[str, object]) -> BindingActivationSpecV1:
    source = _body_object(body)
    raw_strategies = source.get("strategy_ids")
    if not isinstance(raw_strategies, list):
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control strategy IDs are invalid")
    strategies = cast(list[object], raw_strategies)
    if not all(isinstance(item, str) for item in strategies):
        raise ProvisioningErrorV1("CONTROL_REQUEST", "control strategy IDs are invalid")
    return BindingActivationSpecV1(
        account_id=_text(source, "account_id"),
        consumer_id=_text(source, "consumer_id"),
        consumer_account_id=_text(source, "consumer_account_id"),
        account_display_name=_text(source, "account_display_name"),
        strategy_ids=tuple(cast(str, item) for item in strategies),
    )


def _approval_request(
    body: Mapping[str, object], *, principal: ControlPrincipalV1
) -> ProvisioningApprovalV1:
    source = _body_object(body)
    return ProvisioningApprovalV1(
        approval_id=_text(source, "approval_id"),
        action=ProvisioningApprovalActionV1(_text(source, "action")),
        profile_id=_text(source, "profile_id"),
        revision=_positive_int(source, "revision"),
        approver=principal,
        issued_wall_ns=parse_rfc3339_wall_ns_v1(source.get("issued_at")),
        expires_wall_ns=parse_rfc3339_wall_ns_v1(source.get("expires_at")),
    )


def _profile_action_path(path: str) -> tuple[str, int, str] | None:
    prefix, marker = "/control/v1/adapter-profiles/", ":"
    if not path.startswith(prefix) or marker not in path:
        return None
    stem, action = path.rsplit(marker, maxsplit=1)
    parts = stem[len(prefix) :].split("/")
    if len(parts) != 2 or action not in {"validate", "retire"}:
        return None
    try:
        revision = int(parts[1])
    except ValueError:
        return None
    return (parts[0], revision, action) if revision > 0 else None


def _receipt_body(receipt: ProfileReceiptV1) -> Mapping[str, object]:
    return {
        "request_id": receipt.request_id,
        "profile_id": receipt.profile_id,
        "profile_revision": receipt.revision,
        "adapter_kind": receipt.adapter_kind.value,
        "state": receipt.state.value,
        "credential_digest": receipt.material_digest.credential_digest,
        "endpoint_digest": receipt.material_digest.endpoint_digest,
        "approval_required": receipt.approval_required,
        "authorization_epoch": receipt.authorization_epoch,
    }


def _status_for(code: str) -> int:
    if code == "CONTROL_NOT_FOUND":
        return 404
    if code in {"CONTROL_AUTH", "CONTROL_APPROVAL"}:
        return 401
    if code in {
        "CONTROL_IDEMPOTENCY_CONFLICT",
        "CONTROL_PROFILE_STATE",
        "CONTROL_RETIREMENT_BLOCKED",
    }:
        return 409
    return 400
