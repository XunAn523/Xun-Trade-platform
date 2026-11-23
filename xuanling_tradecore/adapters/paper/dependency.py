"""Fail-closed identity for an externally maintained PaperBroker dependency.

The connector intentionally consumes a small release declaration instead of
importing an external PaperBroker project.  This makes a source, image, API,
configuration, and license change an explicit profile change.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Mapping, cast

from ...domain._validation import DomainValidationError, require_sha256, require_text


class PaperBrokerDependencyError(DomainValidationError):
    """An external PaperBroker identity is absent, malformed, or has drifted."""


class PaperBrokerDeploymentMode(str, Enum):
    """Where the separately maintained execution service is deployed."""

    REMOTE_SERVICE = "REMOTE_SERVICE"
    SIDECAR_SERVICE = "SIDECAR_SERVICE"
    MANAGED_PROCESS = "MANAGED_PROCESS"


@dataclass(frozen=True)
class PaperBrokerRuntimeIdentity:
    """The non-secret identity observed after opening an external session."""

    project_version: str
    artifact_sha256: str
    api_schema_sha256: str
    configuration_sha256: str
    deployment_mode: PaperBrokerDeploymentMode

    def __post_init__(self) -> None:
        require_text(self.project_version, "paperbroker project_version", max_length=128)
        require_sha256(self.artifact_sha256, "paperbroker artifact_sha256")
        require_sha256(self.api_schema_sha256, "paperbroker api_schema_sha256")
        require_sha256(self.configuration_sha256, "paperbroker configuration_sha256")


@dataclass(frozen=True)
class PaperBrokerDependencyManifest:
    """The versioned external dependency declaration carried by one profile.

    The values identify a released external component only.  Endpoints,
    credentials, matching rules, and raw configuration contents are purposely
    absent from this artifact.
    """

    manifest_id: str
    version: int
    source_reference: str
    project_version: str
    artifact_sha256: str
    license_identifier: str
    api_schema_reference: str
    api_schema_sha256: str
    configuration_sha256: str
    deployment_mode: PaperBrokerDeploymentMode
    startup_owner: str
    stop_owner: str
    upgrade_policy: str

    def __post_init__(self) -> None:
        require_text(self.manifest_id, "paperbroker dependency manifest_id")
        if isinstance(self.version, bool) or self.version <= 0:
            raise PaperBrokerDependencyError("paperbroker dependency version must be positive")
        for name, value in (
            ("source_reference", self.source_reference),
            ("project_version", self.project_version),
            ("license_identifier", self.license_identifier),
            ("api_schema_reference", self.api_schema_reference),
            ("startup_owner", self.startup_owner),
            ("stop_owner", self.stop_owner),
            ("upgrade_policy", self.upgrade_policy),
        ):
            require_text(value, f"paperbroker dependency {name}", max_length=256)
        require_sha256(self.artifact_sha256, "paperbroker dependency artifact_sha256")
        require_sha256(self.api_schema_sha256, "paperbroker dependency api_schema_sha256")
        require_sha256(self.configuration_sha256, "paperbroker dependency configuration_sha256")

    @property
    def manifest_hash(self) -> str:
        """Return a canonical digest suitable for a release-bundle reference."""

        payload = {
            "manifest_id": self.manifest_id,
            "version": self.version,
            "source_reference": self.source_reference,
            "project_version": self.project_version,
            "artifact_sha256": self.artifact_sha256,
            "license_identifier": self.license_identifier,
            "api_schema_reference": self.api_schema_reference,
            "api_schema_sha256": self.api_schema_sha256,
            "configuration_sha256": self.configuration_sha256,
            "deployment_mode": self.deployment_mode.value,
            "startup_owner": self.startup_owner,
            "stop_owner": self.stop_owner,
            "upgrade_policy": self.upgrade_policy,
        }
        return sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def require_runtime_identity(self, runtime: PaperBrokerRuntimeIdentity) -> None:
        """Reject a live endpoint whose release evidence differs from the profile."""

        expected = (
            self.project_version,
            self.artifact_sha256,
            self.api_schema_sha256,
            self.configuration_sha256,
            self.deployment_mode,
        )
        actual = (
            runtime.project_version,
            runtime.artifact_sha256,
            runtime.api_schema_sha256,
            runtime.configuration_sha256,
            runtime.deployment_mode,
        )
        if actual != expected:
            raise PaperBrokerDependencyError("PaperBroker runtime identity differs from profile")


_MANIFEST_FIELDS = frozenset(
    {
        "manifest_id",
        "version",
        "source_reference",
        "project_version",
        "artifact_sha256",
        "license_identifier",
        "api_schema_reference",
        "api_schema_sha256",
        "configuration_sha256",
        "deployment_mode",
        "startup_owner",
        "stop_owner",
        "upgrade_policy",
    }
)


def load_paperbroker_dependency_manifest_json(payload: str) -> PaperBrokerDependencyManifest:
    """Load exactly the non-secret dependency fields used by a PaperBroker profile."""

    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise PaperBrokerDependencyError("PaperBroker dependency manifest is not JSON") from exc
    if not isinstance(value, dict):
        raise PaperBrokerDependencyError("PaperBroker dependency manifest must be an object")
    document = cast(Mapping[str, object], value)
    if frozenset(document) != _MANIFEST_FIELDS:
        raise PaperBrokerDependencyError("PaperBroker dependency manifest fields differ from V1")
    try:
        mode = PaperBrokerDeploymentMode(document["deployment_mode"])
    except (TypeError, ValueError) as exc:
        raise PaperBrokerDependencyError("PaperBroker deployment mode is unsupported") from exc
    try:
        return PaperBrokerDependencyManifest(
            manifest_id=_require_string(document, "manifest_id"),
            version=_require_positive_int(document, "version"),
            source_reference=_require_string(document, "source_reference"),
            project_version=_require_string(document, "project_version"),
            artifact_sha256=_require_string(document, "artifact_sha256"),
            license_identifier=_require_string(document, "license_identifier"),
            api_schema_reference=_require_string(document, "api_schema_reference"),
            api_schema_sha256=_require_string(document, "api_schema_sha256"),
            configuration_sha256=_require_string(document, "configuration_sha256"),
            deployment_mode=mode,
            startup_owner=_require_string(document, "startup_owner"),
            stop_owner=_require_string(document, "stop_owner"),
            upgrade_policy=_require_string(document, "upgrade_policy"),
        )
    except DomainValidationError as exc:
        raise PaperBrokerDependencyError(str(exc)) from exc


def _require_string(document: Mapping[str, object], field_name: str) -> str:
    value = document[field_name]
    if not isinstance(value, str):
        raise PaperBrokerDependencyError(f"PaperBroker dependency {field_name} must be text")
    return value


def _require_positive_int(document: Mapping[str, object], field_name: str) -> int:
    value = document[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PaperBrokerDependencyError(
            f"PaperBroker dependency {field_name} must be a positive integer"
        )
    return value
