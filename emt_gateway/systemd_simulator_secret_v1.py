"""One-shot simulator credential lease from a systemd credential directory.

The unit must supply a single-account secret with LoadCredential. The
credential and its endpoint are still verified against the active profile by
ProvisionedEmtExecutionSessionFactoryV1 before opening the native session.
"""

from __future__ import annotations

import json
import os
import stat
import struct
from pathlib import Path
from threading import Lock

from xuanling_tradecore.control_plane.provisioning_contracts_v1 import (
    BrokerConnectionV1,
    BrokerCredentialMaterialV1,
    ProvisioningErrorV1,
    SecretReferenceV1,
)
from xuanling_tradecore.control_plane.provisioning_secret_v1 import (
    SecretLeasePurposeV1,
    SecretLeaseV1,
)


class SystemdSimulatorSecretProviderV1:
    """Read one bounded, single-use, scope-bound secret; no store or export."""

    _KIND = "SYSTEMD_SIMULATOR_CREDENTIAL_V1"

    def __init__(
        self, *, credential_directory: Path, credential_name: str,
        reference: SecretReferenceV1, adapter_instance_id: str, account_scope: str,
    ) -> None:
        if (
            os.name != "posix"
            or not credential_directory.is_absolute()
            or ".." in credential_directory.parts
            or not credential_name
            or credential_name in {".", ".."}
            or "/" in credential_name
            or "\\" in credential_name
            or reference.provider_kind != self._KIND
            or not adapter_instance_id
            or not account_scope
        ):
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "systemd credential configuration is invalid")
        self._directory, self._name = credential_directory, credential_name
        self._reference, self._adapter, self._scope = reference, adapter_instance_id, account_scope
        self._consumed = False
        self._lock = Lock()

    @property
    def provider_kind(self) -> str:
        return self._KIND

    def store(
        self, *, target: str, material: BrokerCredentialMaterialV1,
        adapter_instance_id: str, account_scope: str, idempotency_key: str,
    ) -> SecretReferenceV1:
        raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "systemd credential is read-only")

    def revoke(self, *, reference: SecretReferenceV1, reason_code: str) -> None:
        with self._lock:
            self._consumed = True

    def lease(
        self, *, reference: SecretReferenceV1, purpose: SecretLeasePurposeV1,
        now_wall_ns: int, ttl_wall_ns: int,
    ) -> SecretLeaseV1:
        with self._lock:
            if (
                self._consumed
                or reference != self._reference
                or purpose.adapter_instance_id != self._adapter
                or purpose.account_scope != self._scope
                or purpose.operation != "EMT_EXECUTION_SESSION_OPEN"
                or type(now_wall_ns) is not int or now_wall_ns < 0
                or type(ttl_wall_ns) is not int or not 1 <= ttl_wall_ns <= 15_000_000_000
            ):
                raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "simulator credential lease is unavailable")
            self._consumed = True
            material = self._read_material()
            return SecretLeaseV1(
                reference=reference, purpose=purpose,
                expires_wall_ns=now_wall_ns + ttl_wall_ns, material=material,
            )

    def _read_material(self) -> BrokerCredentialMaterialV1:
        directory_fd = None
        credential_fd = None
        try:
            if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
                raise ValueError("safe credential open is unavailable")
            directory_fd = os.open(self._directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            directory_stat = os.fstat(directory_fd)
            if (
                not stat.S_ISDIR(directory_stat.st_mode)
                or not self._protected(directory_fd, directory_stat, is_directory=True)
            ):
                raise ValueError("credential directory is not protected")
            credential_fd = os.open(self._name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            meta = os.fstat(credential_fd)
            if (
                not stat.S_ISREG(meta.st_mode)
                or not self._protected(credential_fd, meta, is_directory=False)
                or not 1 <= meta.st_size <= 65_536
            ):
                raise ValueError("credential file is not protected")
            with os.fdopen(credential_fd, "rb", closefd=False) as handle:
                data = handle.read(65_537)
            if len(data) != meta.st_size:
                raise ValueError("credential changed while reading")

            def unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
                result: dict[str, object] = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError("duplicate credential field")
                    result[key] = value
                return result

            document = json.loads(data, object_pairs_hook=unique_pairs)
            if (
                not isinstance(document, dict)
                or set(document) != {"broker_account", "password", "client_id", "host", "port"}
                or type(document["port"]) is not int
            ):
                raise ValueError("credential schema is invalid")
            return BrokerCredentialMaterialV1(
                broker_account=document["broker_account"], password=document["password"],
                client_id=document["client_id"],
                connection=BrokerConnectionV1("BROKER_TCP", document["host"], document["port"]),
            )
        except (OSError, TypeError, ValueError, KeyError):
            raise ProvisioningErrorV1("CONTROL_SECRET_UNAVAILABLE", "simulator credential cannot be read") from None
        finally:
            if credential_fd is not None:
                os.close(credential_fd)
            if directory_fd is not None:
                os.close(directory_fd)

    @staticmethod
    def _protected(fd: int, meta: os.stat_result, *, is_directory: bool) -> bool:
        """Accept owner-private files or the exact systemd named-user ACL."""

        mode = stat.S_IMODE(meta.st_mode)
        if meta.st_uid == os.geteuid() and mode & 0o077 == 0:
            return True
        expected_mode = 0o550 if is_directory else 0o440
        permission = 5 if is_directory else 4
        if meta.st_uid != 0 or meta.st_gid != 0 or mode != expected_mode:
            return False
        try:
            raw = os.getxattr(fd, "system.posix_acl_access")
            expected = [
                (1, permission, 0xFFFFFFFF),
                (2, permission, os.geteuid()),
                (4, 0, 0xFFFFFFFF),
                (16, permission, 0xFFFFFFFF),
                (32, 0, 0xFFFFFFFF),
            ]
            return (
                len(raw) == 44
                and struct.unpack_from("<I", raw)[0] == 2
                and [struct.unpack_from("<HHI", raw, i) for i in range(4, len(raw), 8)] == expected
            )
        except (OSError, ValueError, struct.error):
            return False
