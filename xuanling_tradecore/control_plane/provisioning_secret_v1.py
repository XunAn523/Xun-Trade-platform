"""Opaque secret-reference and short-lease contracts for provisioning V1."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Protocol, cast
from uuid import uuid4

from ..domain._validation import require_text
from .provisioning_contracts_v1 import (
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProvisioningErrorV1,
    SecretReferenceV1,
)


@dataclass(frozen=True)
class SecretLeasePurposeV1:
    """A narrow caller purpose; it prevents reference reuse across Adapter scopes."""

    adapter_instance_id: str
    account_scope: str
    operation: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.adapter_instance_id, "secret lease adapter_instance_id"),
            (self.account_scope, "secret lease account scope"),
            (self.operation, "secret lease operation"),
        ):
            require_text(value, label, max_length=256)


class SecretLeaseV1:
    """One non-serializable, short-lived in-process borrow of secret material."""

    __slots__ = ("_expires_wall_ns", "_material", "_purpose", "_reference", "_closed")

    def __init__(
        self,
        *,
        reference: SecretReferenceV1,
        purpose: SecretLeasePurposeV1,
        expires_wall_ns: int,
        material: BrokerCredentialMaterialV1,
    ) -> None:
        self._reference = reference
        self._purpose = purpose
        self._expires_wall_ns = expires_wall_ns
        self._material: BrokerCredentialMaterialV1 | None = material
        self._closed = False

    def __repr__(self) -> str:
        return "<SecretLeaseV1 redacted>"

    @property
    def reference(self) -> SecretReferenceV1:
        return self._reference

    @property
    def purpose(self) -> SecretLeasePurposeV1:
        return self._purpose

    @property
    def expires_wall_ns(self) -> int:
        return self._expires_wall_ns

    def borrow(self, *, now_wall_ns: int) -> BrokerCredentialMaterialV1:
        """Return material only before expiry and before the lease is closed."""

        if self._closed or now_wall_ns >= self._expires_wall_ns or self._material is None:
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret lease is unavailable")
        return self._material

    def close(self) -> None:
        """Drop this lease's reference; Python cannot promise heap zeroization."""

        self._material = None
        self._closed = True

    def __enter__(self) -> "SecretLeaseV1":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


class SecretProviderV1(Protocol):
    """Provider boundary; no method permits inventory listing or raw export."""

    @property
    def provider_kind(self) -> str:
        """Return a non-secret provider implementation identity."""

        ...

    def store(
        self,
        *,
        target: str,
        material: BrokerCredentialMaterialV1,
        adapter_instance_id: str,
        account_scope: str,
        idempotency_key: str,
    ) -> SecretReferenceV1:
        """Store material under an opaque reference with an internal retry key.

        The provider never exposes or permits lookup by this key.  It exists
        solely to make a crash between secret storage and profile-revision
        durability safe to retry with the same control request.
        """

        ...

    def lease(
        self,
        *,
        reference: SecretReferenceV1,
        purpose: SecretLeasePurposeV1,
        now_wall_ns: int,
        ttl_wall_ns: int,
    ) -> SecretLeaseV1:
        """Borrow one scope-bound lease; implementations must reject revoked material."""

        ...

    def revoke(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        """Permanently prevent future leases of one secret reference."""

        ...


@dataclass
class _StoredSecretV1:
    material: BrokerCredentialMaterialV1
    target: str
    adapter_instance_id: str
    account_scope: str
    revoked: bool = False


class InMemorySecretProviderV1:
    """Test-only provider with production-equivalent reference/lease semantics.

    It intentionally has no read/list API.  Production provider adapters can
    implement the same protocol using an OS store, HSM, or a Vault agent.
    """

    def __init__(self, *, provider_kind: str = "IN_MEMORY_TEST") -> None:
        self._provider_kind = require_text(provider_kind, "secret provider kind", max_length=128)
        self._next_reference = 1
        self._stored: dict[str, _StoredSecretV1] = {}
        self._idempotency: dict[str, SecretReferenceV1] = {}
        self._lock = RLock()

    @property
    def provider_kind(self) -> str:
        return self._provider_kind

    def store(
        self,
        *,
        target: str,
        material: BrokerCredentialMaterialV1,
        adapter_instance_id: str,
        account_scope: str,
        idempotency_key: str,
    ) -> SecretReferenceV1:
        require_text(target, "secret target", max_length=256)
        require_text(adapter_instance_id, "secret adapter_instance_id", max_length=256)
        require_text(account_scope, "secret account scope", max_length=256)
        require_text(idempotency_key, "secret idempotency key", max_length=512)
        with self._lock:
            existing_reference = self._idempotency.get(idempotency_key)
            if existing_reference is not None:
                existing = self._stored[existing_reference.reference_id]
                if (
                    existing.target != target
                    or existing.adapter_instance_id != adapter_instance_id
                    or existing.account_scope != account_scope
                    or existing.material != material
                ):
                    raise ProvisioningErrorV1(
                        "CONTROL_SECRET_UNAVAILABLE", "secret retry key conflicts"
                    )
                if not existing.revoked:
                    return existing_reference
                # A known failed prepare revokes its first write.  Reusing the
                # exact same request/JWE may safely allocate a new opaque
                # reference; the old reference remains permanently revoked.
            reference_id = f"secret-{self._next_reference}"
            self._next_reference += 1
            self._stored[reference_id] = _StoredSecretV1(
                material=material,
                target=target,
                adapter_instance_id=adapter_instance_id,
                account_scope=account_scope,
            )
            reference = SecretReferenceV1(self._provider_kind, reference_id, target)
            self._idempotency[idempotency_key] = reference
            return reference

    def lease(
        self,
        *,
        reference: SecretReferenceV1,
        purpose: SecretLeasePurposeV1,
        now_wall_ns: int,
        ttl_wall_ns: int,
    ) -> SecretLeaseV1:
        if reference.provider_kind != self._provider_kind:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret provider does not own ref"
            )
        if now_wall_ns < 0:
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "lease time is invalid")
        if not 1 <= ttl_wall_ns <= 60_000_000_000:
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "lease lifetime is invalid")
        with self._lock:
            stored = self._stored.get(reference.reference_id)
            if (
                stored is None
                or stored.revoked
                or stored.target != reference.target
                or stored.adapter_instance_id != purpose.adapter_instance_id
                or stored.account_scope != purpose.account_scope
            ):
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid"
                )
            return SecretLeaseV1(
                reference=reference,
                purpose=purpose,
                expires_wall_ns=now_wall_ns + ttl_wall_ns,
                material=stored.material,
            )

    def revoke(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        require_text(reason_code, "secret revocation reason", max_length=128)
        if reference.provider_kind != self._provider_kind:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret provider does not own ref"
            )
        with self._lock:
            stored = self._stored.get(reference.reference_id)
            if stored is None or stored.target != reference.target:
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid"
                )
            stored.revoked = True


class LinuxProtectedFileSecretProviderV1:
    """Linux-only protected-file SecretProvider for the isolated control plane.

    This provider is deliberately *not* a general secret-file reader.  It has
    no list/export API and accepts only references that it created.  Each
    opaque reference resolves to one root-owned ``0600`` JSON record below an
    explicitly configured secret root.  The operating system's DAC boundary
    protects the record at rest; installations that require a KMS/HSM may
    replace this implementation while retaining the same ``SecretProviderV1``
    contract.

    It is unavailable outside POSIX hosts.  It is intended solely for the
    separately privileged provisioning service, never a strategy, Consumer,
    OMS or EMS worker.  Secret material exists in Python only while writing a
    record or while a caller holds a short ``SecretLeaseV1``.
    """

    _PROVIDER_KIND = "LINUX_PROTECTED_FILE_V1"

    def __init__(self, *, secret_root: Path, provider_kind: str = _PROVIDER_KIND) -> None:
        if os.name != "posix":
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "protected file provider requires Linux/POSIX"
            )
        self._root = secret_root
        self._provider_kind = require_text(provider_kind, "secret provider kind", max_length=128)
        self._validate_root()
        self._lock = RLock()

    @property
    def provider_kind(self) -> str:
        return self._provider_kind

    def store(
        self,
        *,
        target: str,
        material: BrokerCredentialMaterialV1,
        adapter_instance_id: str,
        account_scope: str,
        idempotency_key: str,
    ) -> SecretReferenceV1:
        require_text(target, "secret target", max_length=256)
        require_text(adapter_instance_id, "secret adapter_instance_id", max_length=256)
        require_text(account_scope, "secret account scope", max_length=256)
        require_text(idempotency_key, "secret idempotency key", max_length=512)
        reference_id = sha256(
            ("tradecore/linux-protected-file-secret/v1\\0" + idempotency_key).encode("utf-8")
        ).hexdigest()
        reference = SecretReferenceV1(self._provider_kind, reference_id, target)
        record: dict[str, object] = {
            "adapter_instance_id": adapter_instance_id,
            "account_scope": account_scope,
            "credential_digest": material.credential_digest(),
            "material": {
                "broker_account": material.broker_account,
                "client_id": material.client_id,
                "connection": {
                    "host": material.connection.host,
                    "kind": material.connection.kind,
                    "port": material.connection.port,
                    "server_name": material.connection.server_name,
                },
                "password": material.password,
                "password_type": material.password_type,
            },
            "reference_id": reference_id,
            "revoked": False,
            "target": target,
        }
        with self._lock:
            path = self._record_path(reference_id)
            if path.exists():
                prior = self._load_record(path)
                if (
                    prior.get("target") != target
                    or prior.get("adapter_instance_id") != adapter_instance_id
                    or prior.get("account_scope") != account_scope
                    or prior.get("credential_digest") != material.credential_digest()
                ):
                    raise ProvisioningErrorV1(
                        "CONTROL_SECRET_UNAVAILABLE", "secret retry key conflicts"
                    )
                if not bool(prior.get("revoked")):
                    return reference
                # The service can retry an exact request after a failed prepare.
                # Preserve the permanently revoked record and allocate a fresh
                # opaque reference without allowing the caller to choose it.
                reference_id = sha256(
                    ("tradecore/linux-protected-file-secret/retry/v1\\0" + idempotency_key + uuid4().hex).encode(
                        "utf-8"
                    )
                ).hexdigest()
                reference = SecretReferenceV1(self._provider_kind, reference_id, target)
                record["reference_id"] = reference_id
                path = self._record_path(reference_id)
            self._write_record(path, record)
            return reference

    def lease(
        self,
        *,
        reference: SecretReferenceV1,
        purpose: SecretLeasePurposeV1,
        now_wall_ns: int,
        ttl_wall_ns: int,
    ) -> SecretLeaseV1:
        if reference.provider_kind != self._provider_kind:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret provider does not own ref"
            )
        if now_wall_ns < 0 or not 1 <= ttl_wall_ns <= 60_000_000_000:
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret lease is invalid")
        with self._lock:
            record = self._load_record(self._record_path(reference.reference_id))
            if (
                record.get("reference_id") != reference.reference_id
                or record.get("target") != reference.target
                or bool(record.get("revoked"))
                or record.get("adapter_instance_id") != purpose.adapter_instance_id
                or record.get("account_scope") != purpose.account_scope
            ):
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid"
                )
            payload = record.get("material")
            if not isinstance(payload, dict):
                raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret record is corrupt")
            material_record = cast(dict[str, object], payload)
            connection_payload = material_record.get("connection")
            if not isinstance(connection_payload, dict):
                raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret record is corrupt")
            connection_record = cast(dict[str, object], connection_payload)
            try:
                material = BrokerCredentialMaterialV1(
                    broker_account=_record_text(material_record, "broker_account"),
                    password=_record_text(material_record, "password"),
                    connection=BrokerConnectionV1(
                        kind=_record_text(connection_record, "kind"),
                        host=_record_text(connection_record, "host"),
                        port=_record_int(connection_record, "port"),
                        server_name=_record_optional_text(connection_record, "server_name"),
                    ),
                    client_id=_record_optional_text(material_record, "client_id"),
                    password_type=_record_text(material_record, "password_type"),
                )
            except (TypeError, ValueError, ProvisioningErrorV1) as exc:
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret record is corrupt"
                ) from exc
            if record.get("credential_digest") != material.credential_digest():
                raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret record is corrupt")
            return SecretLeaseV1(
                reference=reference,
                purpose=purpose,
                expires_wall_ns=now_wall_ns + ttl_wall_ns,
                material=material,
            )

    def revoke(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        require_text(reason_code, "secret revocation reason", max_length=128)
        if reference.provider_kind != self._provider_kind:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret provider does not own ref"
            )
        with self._lock:
            path = self._record_path(reference.reference_id)
            record = self._load_record(path)
            if record.get("reference_id") != reference.reference_id or record.get("target") != reference.target:
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid"
                )
            if not bool(record.get("revoked")):
                record["revoked"] = True
                self._write_record(path, record)

    def _validate_root(self) -> None:
        if not self._root.is_absolute() or not self._root.is_dir():
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret root is invalid")
        mode = stat.S_IMODE(self._root.stat().st_mode)
        if mode & 0o077:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret root is accessible by group or other"
            )

    def _record_path(self, reference_id: str) -> Path:
        if len(reference_id) != 64 or any(char not in "0123456789abcdef" for char in reference_id):
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid")
        return self._root / (reference_id + ".json")

    def _load_record(self, path: Path) -> dict[str, object]:
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise ProvisioningErrorV1(
                    "CONTROL_SECRET_UNAVAILABLE", "secret record permissions are invalid"
                )
            parsed = cast(object, json.loads(path.read_text(encoding="utf-8")))
        except ProvisioningErrorV1:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "secret reference is invalid"
            ) from exc
        if not isinstance(parsed, dict):
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "secret record is corrupt")
        return cast(dict[str, object], parsed)

    def _write_record(self, path: Path, record: dict[str, object]) -> None:
        payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        descriptor: int | None = None
        try:
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(temporary, path)
            os.chmod(path, 0o600)
        except OSError as exc:
            raise ProvisioningErrorV1(
                "CONTROL_SECRET_UNAVAILABLE", "protected secret write failed"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _record_text(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise ValueError(key)
    return value


def _record_optional_text(record: dict[str, object], key: str) -> str | None:
    value = record.get(key)
    if value is None or isinstance(value, str):
        return value
    raise ValueError(key)


def _record_int(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if type(value) is not int:
        raise ValueError(key)
    return value
