"""Secret-free contracts for the Adapter Provisioning Control Plane V1.

This module deliberately has no HTTP, TLS, broker SDK, filesystem, database,
or JWE dependency.  It defines identities and receipts only; confidential
credential material is kept in the JWE/SecretProvider boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from typing import Mapping

from ..domain._validation import (
    DomainValidationError,
    freeze_text_mapping,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)


class ProvisioningErrorV1(DomainValidationError):
    """A stable, non-sensitive provisioning failure."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = require_text(code, "provisioning error code", max_length=96)
        super().__init__(message or code)


class AdapterKindV1(str, Enum):
    EMT = "EMT"
    QMT = "QMT"


class ControlRoleV1(str, Enum):
    ADAPTER_PROVISIONER = "ADAPTER_PROVISIONER"
    APPROVER = "APPROVER"


class ProfileStateV1(str, Enum):
    PREPARED = "PREPARED"
    VALIDATED = "VALIDATED"
    ACTIVE_DISARMED = "ACTIVE_DISARMED"
    RETIRING = "RETIRING"
    RETIRED = "RETIRED"
    REJECTED = "REJECTED"


def _empty_attributes() -> Mapping[str, str]:
    return {}


def _canonical_sha256(*, domain: str, fields: Mapping[str, str]) -> str:
    """Return a domain-separated digest without revealing caller material."""

    body = "\n".join(f"{key}={fields[key]}" for key in sorted(fields))
    return sha256((domain + "\0" + body).encode("utf-8")).hexdigest()


def is_uuidv7(value: str) -> bool:
    """Check canonical UUIDv7 spelling without relying on a Python 3.10 parser."""

    if len(value) != 36 or value[14] != "7" or value[19] not in "89ab":
        return False
    if any(value[index] != "-" for index in (8, 13, 18, 23)):
        return False
    return all(character in "0123456789abcdef" for character in value.replace("-", ""))


@dataclass(frozen=True)
class ControlPrincipalV1:
    """Identity derived from a verified mTLS client certificate, never JSON."""

    principal_id: str
    roles: frozenset[ControlRoleV1]
    certificate_fingerprint: str

    def __post_init__(self) -> None:
        require_text(self.principal_id, "control principal_id", max_length=256)
        if not self.roles:
            raise ProvisioningErrorV1("CONTROL_AUTH", "control principal has no role")
        require_sha256(self.certificate_fingerprint, "control certificate fingerprint")

    def require_role(self, role: ControlRoleV1) -> None:
        if role not in self.roles:
            raise ProvisioningErrorV1("CONTROL_AUTH", "control principal lacks required role")


@dataclass(frozen=True)
class AdapterProfileIdentityV1:
    """Non-secret, immutable identity of one concrete Adapter profile revision."""

    profile_id: str
    adapter_kind: AdapterKindV1
    venue_id: str
    adapter_instance_id: str
    account_alias: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.profile_id, "profile_id"),
            (self.venue_id, "profile venue_id"),
            (self.adapter_instance_id, "profile adapter_instance_id"),
            (self.account_alias, "profile account_alias"),
        ):
            require_text(value, label, max_length=128)
        if self.venue_id != self.adapter_kind.value:
            raise ProvisioningErrorV1("CONTROL_PROFILE", "profile venue does not match adapter")


@dataclass(frozen=True)
class BrokerConnectionV1:
    """Confidential connection material; it is valid only inside a secret lease."""

    kind: str = field(repr=False)
    host: str = field(repr=False)
    port: int = field(repr=False)
    server_name: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.kind not in {"BROKER_TCP", "QMT_WINDOWS_EDGE"}:
            raise ProvisioningErrorV1("CONTROL_ENDPOINT", "connection kind is invalid")
        require_text(self.host, "connection host", max_length=255)
        if not 1 <= self.port <= 65_535:
            raise ProvisioningErrorV1("CONTROL_ENDPOINT", "connection port is invalid")
        if self.server_name is not None:
            require_text(self.server_name, "connection server_name", max_length=255)

    def endpoint_digest(self) -> str:
        return _canonical_sha256(
            domain="tradecore/endpoint/v1",
            fields={
                "host": self.host,
                "kind": self.kind,
                "port": str(self.port),
                "server_name": self.server_name or "",
            },
        )


@dataclass(frozen=True)
class BrokerCredentialMaterialV1:
    """Confidential material received only after JWE validation.

    Python cannot guarantee memory clearing for immutable strings.  This class
    is therefore non-repr and confined to the SecretProvider implementation;
    no caller may persist or log an instance.
    """

    broker_account: str = field(repr=False)
    password: str = field(repr=False)
    connection: BrokerConnectionV1 = field(repr=False)
    client_id: str | None = field(default=None, repr=False)
    password_type: str = field(default="PASSWORD", repr=False)

    def __post_init__(self) -> None:
        require_text(self.broker_account, "broker account", max_length=256)
        require_text(self.password, "broker password", max_length=4_096)
        if self.client_id is not None:
            require_text(self.client_id, "broker client_id", max_length=256)
        require_text(self.password_type, "broker password_type", max_length=64)

    def credential_digest(self) -> str:
        return _canonical_sha256(
            domain="tradecore/credential/v1",
            fields={
                "broker_account": self.broker_account,
                "client_id": self.client_id or "",
                "password": self.password,
                "password_type": self.password_type,
            },
        )


@dataclass(frozen=True)
class CredentialClaimsV1:
    """Verified JWE claims bound to a prepare request and a recipient target."""

    jti: str
    issuer: str
    audience: str
    issued_at_wall_ns: int
    not_before_wall_ns: int
    expires_at_wall_ns: int
    recipient_key_id: str

    def __post_init__(self) -> None:
        if not is_uuidv7(self.jti):
            raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential jti is not UUIDv7")
        for value, label in (
            (self.issuer, "credential issuer"),
            (self.audience, "credential audience"),
            (self.recipient_key_id, "credential recipient key ID"),
        ):
            require_text(value, label, max_length=256)
        for value, label in (
            (self.issued_at_wall_ns, "credential issued time"),
            (self.not_before_wall_ns, "credential not-before time"),
            (self.expires_at_wall_ns, "credential expiry time"),
        ):
            require_nonnegative(value, label)
        if not self.issued_at_wall_ns <= self.not_before_wall_ns < self.expires_at_wall_ns:
            raise ProvisioningErrorV1("CONTROL_ENVELOPE", "credential claim interval is invalid")
        if self.expires_at_wall_ns - self.issued_at_wall_ns > 300_000_000_000:
            raise ProvisioningErrorV1(
                "CONTROL_ENVELOPE", "credential lifetime exceeds five minutes"
            )


@dataclass(frozen=True)
class SecretReferenceV1:
    """Opaque provider handle.  It is not a credential and cannot be dereferenced by APIs."""

    provider_kind: str
    reference_id: str
    target: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.provider_kind, "secret provider kind"),
            (self.reference_id, "secret reference ID"),
            (self.target, "secret target"),
        ):
            require_text(value, label, max_length=256)


@dataclass(frozen=True)
class ProfileMaterialDigestV1:
    """Non-reversible material identity stored with one profile revision."""

    credential_digest: str
    endpoint_digest: str
    envelope_digest: str
    recipient_key_id: str
    mapping_manifest_hash: str
    capability_manifest_hash: str
    release_manifest_hash: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.credential_digest, "credential digest"),
            (self.endpoint_digest, "endpoint digest"),
            (self.envelope_digest, "envelope digest"),
            (self.mapping_manifest_hash, "mapping manifest hash"),
            (self.capability_manifest_hash, "capability manifest hash"),
            (self.release_manifest_hash, "release manifest hash"),
        ):
            require_sha256(value, label)
        require_text(self.recipient_key_id, "recipient key ID", max_length=256)


@dataclass(frozen=True)
class AdapterProfileRevisionV1:
    """One immutable secret-free revision and its lifecycle evidence."""

    identity: AdapterProfileIdentityV1
    revision: int
    state: ProfileStateV1
    secret_reference: SecretReferenceV1
    material_digest: ProfileMaterialDigestV1
    created_wall_ns: int
    validation_evidence_id: str | None = None
    approval_id: str | None = None
    retired_wall_ns: int | None = None

    def __post_init__(self) -> None:
        require_positive(self.revision, "profile revision")
        require_nonnegative(self.created_wall_ns, "profile created time")
        for value, label in (
            (self.validation_evidence_id, "profile validation evidence"),
            (self.approval_id, "profile approval"),
        ):
            if value is not None:
                require_text(value, label, max_length=256)
        if self.state is ProfileStateV1.VALIDATED and self.validation_evidence_id is None:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "validated profile lacks evidence")
        if self.state is ProfileStateV1.ACTIVE_DISARMED and (
            self.validation_evidence_id is None or self.approval_id is None
        ):
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "active profile lacks evidence")
        if self.state is ProfileStateV1.RETIRED:
            if self.retired_wall_ns is None or self.retired_wall_ns < self.created_wall_ns:
                raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "retired profile lacks time")
        elif self.retired_wall_ns is not None:
            raise ProvisioningErrorV1(
                "CONTROL_PROFILE_STATE", "non-retired profile has retirement time"
            )


@dataclass(frozen=True)
class PrepareProfileRequestV1:
    """The only control request that carries a JWE compact serialization."""

    request_id: str
    identity: AdapterProfileIdentityV1
    credential_envelope: str = field(repr=False)
    expected_credential_key_id: str
    not_after_wall_ns: int
    approval_reference: str
    attributes: Mapping[str, str] = field(default_factory=_empty_attributes)

    def __post_init__(self) -> None:
        if not is_uuidv7(self.request_id):
            raise ProvisioningErrorV1("CONTROL_REQUEST", "control request ID is not UUIDv7")
        require_text(self.credential_envelope, "credential envelope", max_length=60_000)
        require_text(self.expected_credential_key_id, "expected credential key ID", max_length=256)
        require_positive(self.not_after_wall_ns, "profile not_after")
        require_text(self.approval_reference, "approval reference", max_length=256)
        attributes = freeze_text_mapping(self.attributes, "prepare profile attributes")
        expected_attributes = {
            "mapping_manifest_hash",
            "capability_manifest_hash",
            "release_manifest_hash",
        }
        if set(attributes) != expected_attributes:
            raise ProvisioningErrorV1(
                "CONTROL_REQUEST", "prepare profile attributes are incomplete"
            )
        for key in expected_attributes:
            require_sha256(attributes[key], f"prepare profile {key}")
        object.__setattr__(self, "attributes", attributes)


@dataclass(frozen=True)
class ProfileActionRequestV1:
    """A no-secret validate, activate, retire, or status request."""

    request_id: str
    profile_id: str
    revision: int
    approval_id: str | None = None
    reason_code: str | None = None

    def __post_init__(self) -> None:
        if not is_uuidv7(self.request_id):
            raise ProvisioningErrorV1("CONTROL_REQUEST", "control request ID is not UUIDv7")
        require_text(self.profile_id, "action profile ID", max_length=128)
        require_positive(self.revision, "action profile revision")
        if self.approval_id is not None:
            require_text(self.approval_id, "action approval ID", max_length=256)
        if self.reason_code is not None:
            require_text(self.reason_code, "action reason code", max_length=128)


@dataclass(frozen=True)
class ProfileValidationEvidenceV1:
    """De-identified result of a DISARMED capability/recovery validation."""

    evidence_id: str
    evidence_digest: str
    passed: bool
    reason_codes: tuple[str, ...]
    evaluated_wall_ns: int

    def __post_init__(self) -> None:
        require_text(self.evidence_id, "validation evidence ID", max_length=256)
        require_sha256(self.evidence_digest, "validation evidence digest")
        require_nonnegative(self.evaluated_wall_ns, "validation evaluated time")
        codes = tuple(
            require_text(item, "validation reason code", max_length=128)
            for item in self.reason_codes
        )
        if len(codes) != len(set(codes)):
            raise ProvisioningErrorV1("CONTROL_VALIDATION", "validation reason codes repeat")
        if self.passed == bool(codes):
            raise ProvisioningErrorV1("CONTROL_VALIDATION", "validation state and reasons conflict")
        object.__setattr__(self, "reason_codes", codes)


@dataclass(frozen=True)
class ConsumerBindingRevisionV1:
    """Published non-secret binding successor for Consumer V2 routing."""

    binding_revision_id: str
    profile_id: str
    profile_revision: int
    account_id: str
    consumer_id: str
    consumer_account_id: str
    account_display_name: str
    strategy_ids: tuple[str, ...]
    authorization_epoch: int
    effective_wall_ns: int
    retired_wall_ns: int | None = None

    def __post_init__(self) -> None:
        for value, label in (
            (self.binding_revision_id, "binding revision ID"),
            (self.profile_id, "binding profile ID"),
            (self.account_id, "binding account ID"),
            (self.consumer_id, "binding consumer ID"),
            (self.consumer_account_id, "binding consumer account ID"),
            (self.account_display_name, "binding account display name"),
        ):
            require_text(value, label, max_length=256)
        require_positive(self.profile_revision, "binding profile revision")
        require_positive(self.authorization_epoch, "binding authorization epoch")
        require_nonnegative(self.effective_wall_ns, "binding effective time")
        strategies = tuple(
            require_text(strategy, "binding strategy ID", max_length=256)
            for strategy in self.strategy_ids
        )
        if not strategies or len(strategies) != len(set(strategies)):
            raise ProvisioningErrorV1("CONTROL_BINDING", "binding strategies are invalid")
        object.__setattr__(self, "strategy_ids", strategies)
        if self.retired_wall_ns is not None and self.retired_wall_ns < self.effective_wall_ns:
            raise ProvisioningErrorV1("CONTROL_BINDING", "binding retirement precedes activation")


@dataclass(frozen=True)
class BindingActivationSpecV1:
    """Internal, non-secret binding data approved for one active profile revision."""

    account_id: str
    consumer_id: str
    consumer_account_id: str
    account_display_name: str
    strategy_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.account_id, "binding account ID"),
            (self.consumer_id, "binding consumer ID"),
            (self.consumer_account_id, "binding consumer account ID"),
            (self.account_display_name, "binding account display name"),
        ):
            require_text(value, label, max_length=256)
        strategy_ids = tuple(
            require_text(strategy, "binding strategy ID", max_length=256)
            for strategy in self.strategy_ids
        )
        if not strategy_ids or len(strategy_ids) != len(set(strategy_ids)):
            raise ProvisioningErrorV1("CONTROL_BINDING", "binding strategies are invalid")
        object.__setattr__(self, "strategy_ids", strategy_ids)


@dataclass(frozen=True)
class ProfileReceiptV1:
    """Only API-safe information about one profile revision."""

    request_id: str
    profile_id: str
    revision: int
    adapter_kind: AdapterKindV1
    state: ProfileStateV1
    material_digest: ProfileMaterialDigestV1
    approval_required: bool
    authorization_epoch: int | None = None

    def __post_init__(self) -> None:
        if not is_uuidv7(self.request_id):
            raise ProvisioningErrorV1("CONTROL_REQUEST", "receipt request ID is not UUIDv7")
        require_text(self.profile_id, "receipt profile ID", max_length=128)
        require_positive(self.revision, "receipt profile revision")
        if self.authorization_epoch is not None:
            require_positive(self.authorization_epoch, "receipt authorization epoch")
