"""Fail-closed Linux CPython 3.10 EMT binding compatibility attestation."""

from __future__ import annotations

import platform
import socket
import sys
from dataclasses import dataclass
from hashlib import sha256
from typing import Mapping

from .platform_loader import (
    EmtPlatformLoader,
    LoadedSdk,
    PlatformLoadError,
    SdkLoadSettings,
    is_project_venv_runtime,
)

_REQUIRED_READ_ONLY_METHODS = (
    "createTraderApi",
    "setSoftwareVersion",
    "setHeartBeatInterval",
    "login",
    "logout",
    "release",
    "getApiVersion",
    "getTradingDay",
    "queryUnfinishedOrders",
)


@dataclass(frozen=True)
class EmtLinuxBindingAttestation:
    """Sanitized static-binding evidence for the M2-E-02H native factory."""

    schema_version: int
    host_id_sha256: str
    system: str
    machine: str
    python_runtime_scope: str
    python_implementation: str
    python_version: str
    sdk_package_sha256: str
    verified_library_files: tuple[str, ...]
    binding_module: str
    required_read_only_methods: tuple[str, ...]
    result: str

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe artifact without hostnames, paths, or credentials."""

        return {
            "schema_version": self.schema_version,
            "host_id_sha256": self.host_id_sha256,
            "system": self.system,
            "machine": self.machine,
            "python_runtime_scope": self.python_runtime_scope,
            "python_implementation": self.python_implementation,
            "python_version": self.python_version,
            "sdk_package_sha256": self.sdk_package_sha256,
            "verified_library_files": list(self.verified_library_files),
            "binding_module": self.binding_module,
            "required_read_only_methods": list(self.required_read_only_methods),
            "result": self.result,
        }


def collect_linux_binding_attestation(
    *,
    sdk_root: str,
    expected_library_hashes: Mapping[str, str],
) -> EmtLinuxBindingAttestation:
    """Load the declared Linux SDK and emit static read-only binding evidence.

    The platform loader verifies each declared library hash before the import.
    This function intentionally performs no login, query, public subscription,
    replay, order, cancel, or account operation.
    """

    if platform.system() != "Linux" or platform.machine().lower() != "x86_64":
        raise PlatformLoadError("EMT Linux attestation requires Linux x86_64")
    if not is_project_venv_runtime():
        raise PlatformLoadError(
            "EMT Linux attestation requires the project .venv interpreter; "
            "server default Python is not an accepted runtime"
        )
    if sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 10):
        raise PlatformLoadError("EMT Linux attestation requires CPython 3.10")
    loaded = EmtPlatformLoader(
        SdkLoadSettings(
            sdk_root=_as_path(sdk_root),
            expected_hashes=expected_library_hashes,
            required_python=(3, 10),
            require_project_venv=True,
        )
    ).load()
    return _attest_loaded_binding(
        loaded,
        expected_library_hashes=expected_library_hashes,
        system=platform.system(),
        machine=platform.machine(),
        project_venv_active=True,
        python_implementation=sys.implementation.name,
        python_version=".".join(str(value) for value in sys.version_info[:3]),
        host_id=socket.gethostname(),
    )


def _attest_loaded_binding(
    loaded: LoadedSdk,
    *,
    expected_library_hashes: Mapping[str, str],
    system: str,
    machine: str,
    project_venv_active: bool,
    python_implementation: str,
    python_version: str,
    host_id: str,
) -> EmtLinuxBindingAttestation:
    """Validate a loaded binding; separate for deterministic non-native tests."""

    if system != "Linux" or machine.lower() != "x86_64":
        raise PlatformLoadError("EMT Linux attestation requires Linux x86_64")
    if not project_venv_active:
        raise PlatformLoadError("EMT Linux attestation requires the project .venv interpreter")
    if python_implementation != "cpython" or not python_version.startswith("3.10."):
        raise PlatformLoadError("EMT Linux attestation requires CPython 3.10")
    if not host_id.strip():
        raise PlatformLoadError("EMT Linux attestation requires a host identity")
    verified_library_files, package_sha256 = _canonical_sdk_package(expected_library_hashes)
    trader_api_type = loaded.trader_api_type
    missing = tuple(
        method
        for method in _REQUIRED_READ_ONLY_METHODS
        if not callable(getattr(trader_api_type, method, None))
    )
    if missing:
        raise PlatformLoadError(
            "EMT Linux binding lacks required read-only methods: " + ", ".join(missing)
        )
    return EmtLinuxBindingAttestation(
        schema_version=1,
        host_id_sha256=sha256(host_id.encode("utf-8")).hexdigest(),
        system="Linux",
        machine="x86_64",
        python_runtime_scope=".venv",
        python_implementation="cpython",
        python_version=python_version,
        sdk_package_sha256=package_sha256,
        verified_library_files=verified_library_files,
        binding_module="vnemttrader",
        required_read_only_methods=_REQUIRED_READ_ONLY_METHODS,
        result="STATIC_BINDING_COMPATIBLE_FOR_M2_E_02H_READ_ONLY_ONLY",
    )


def _canonical_sdk_package(
    expected_library_hashes: Mapping[str, str],
) -> tuple[tuple[str, ...], str]:
    if not expected_library_hashes:
        raise PlatformLoadError("EMT Linux attestation requires declared SDK library hashes")
    items: list[tuple[str, str]] = []
    for name, digest in expected_library_hashes.items():
        if not isinstance(name, str) or not name or "/" in name or "\\" in name:
            raise PlatformLoadError("EMT Linux SDK hash entries must be plain file names")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise PlatformLoadError(f"invalid SHA-256 digest for {name}")
        items.append((name, digest))
    items.sort()
    canonical = "\n".join(f"{name}:{digest}" for name, digest in items).encode("ascii")
    return tuple(name for name, _ in items), sha256(canonical).hexdigest()


def _as_path(value: str):
    from pathlib import Path

    if not value.strip():
        raise PlatformLoadError("EMT Linux attestation requires an SDK root")
    return Path(value)
