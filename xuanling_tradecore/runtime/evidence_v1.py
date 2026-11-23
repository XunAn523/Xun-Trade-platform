"""Redacted trace and acceptance-evidence contracts shared outside hot paths.

The structures in this module deliberately contain only immutable identifiers,
hashes and counters.  They provide a common record for Consumer, EMS,
Provisioning and performance acceptance without retaining credential material,
broker endpoints or raw vendor payloads.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping, cast

from ..domain._validation import (
    DomainValidationError,
    require_nonnegative,
    require_positive,
    require_sha256,
    require_text,
)

_COMMIT_HASH = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
_SENSITIVE_METADATA_NAMES = frozenset(
    {
        "password",
        "private_key",
        "credential_material",
        "jwe_compact",
        "secret_material",
        "raw_endpoint",
    }
)


class AcceptanceEvidenceErrorV1(DomainValidationError):
    """An acceptance record is malformed or attempts to retain sensitive data."""


class AcceptanceEvidenceStatusV1(str, Enum):
    """Explicit stage outcome; ``ACCEPTED`` never implies trading readiness."""

    ACCEPTED = "ACCEPTED"
    BLOCKED_EXTERNAL = "BLOCKED_EXTERNAL"
    CODE_COMPLETE_PENDING_ACCEPTANCE = "CODE_COMPLETE_PENDING_ACCEPTANCE"
    FAILED = "FAILED"


class EvidenceCleanupStateV1(str, Enum):
    """Whether a stage had broker-side cleanup and how it finished."""

    NOT_APPLICABLE = "NOT_APPLICABLE"
    PASSED = "PASSED"
    FAILED = "FAILED"


def _freeze_metadata(metadata: Mapping[str, str] | None) -> Mapping[str, str]:
    if metadata is None:
        return cast(Mapping[str, str], MappingProxyType({}))
    if len(metadata) > 32:
        raise AcceptanceEvidenceErrorV1("acceptance evidence metadata exceeds item limit")
    copied: dict[str, str] = {}
    for name, value in metadata.items():
        normalized_name = require_text(name, "acceptance evidence metadata name", max_length=64)
        if normalized_name.casefold() in _SENSITIVE_METADATA_NAMES:
            raise AcceptanceEvidenceErrorV1(
                "acceptance evidence metadata must not contain sensitive material"
            )
        copied[normalized_name] = require_text(
            value, "acceptance evidence metadata value", max_length=256
        )
    return cast(Mapping[str, str], MappingProxyType(copied))


@dataclass(frozen=True)
class TraceContextV1:
    """One redacted, monotonic timestamped boundary observation.

    ``scope_id_hash`` is deliberately a SHA-256 value rather than a scope,
    account or client identifier.  The caller supplies a locally unique
    ``trace_id``; this class does not generate identifiers on a hot path.
    """

    trace_id: str
    execution_id: str
    binding_id: str
    binding_generation: int
    adapter_id: str
    scope_id_hash: str
    event_cursor: str
    stage: str
    monotonic_ns: int
    result_code: str

    def __post_init__(self) -> None:
        for name, value in (
            ("trace ID", self.trace_id),
            ("execution ID", self.execution_id),
            ("binding ID", self.binding_id),
            ("adapter ID", self.adapter_id),
            ("event cursor", self.event_cursor),
            ("trace stage", self.stage),
            ("trace result code", self.result_code),
        ):
            require_text(value, name, max_length=256)
        require_positive(self.binding_generation, "trace binding generation")
        require_sha256(self.scope_id_hash, "trace scope hash")
        require_nonnegative(self.monotonic_ns, "trace monotonic timestamp")

    def as_dict(self) -> dict[str, object]:
        """Return the serializable, redacted trace record."""

        return {
            "trace_id": self.trace_id,
            "execution_id": self.execution_id,
            "binding_id": self.binding_id,
            "binding_generation": self.binding_generation,
            "adapter_id": self.adapter_id,
            "scope_id_hash": self.scope_id_hash,
            "event_cursor": self.event_cursor,
            "stage": self.stage,
            "monotonic_ns": self.monotonic_ns,
            "result_code": self.result_code,
        }


@dataclass(frozen=True)
class AcceptanceEvidenceManifestV1:
    """A machine-readable, non-secret summary of one completed stage run."""

    stage: str
    status: AcceptanceEvidenceStatusV1
    source_commit: str
    profile_digest: str
    environment_fingerprint: str
    started_wall_ns: int
    finished_wall_ns: int
    test_command: str
    total_cases: int
    failed_cases: int
    trace_pair_count: int
    trace_gap_count: int
    cleanup_state: EvidenceCleanupStateV1
    metadata: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        require_text(self.stage, "acceptance evidence stage", max_length=64)
        if _COMMIT_HASH.fullmatch(self.source_commit) is None:
            raise AcceptanceEvidenceErrorV1("acceptance evidence source commit is invalid")
        require_sha256(self.profile_digest, "acceptance evidence profile digest")
        require_sha256(self.environment_fingerprint, "acceptance evidence environment fingerprint")
        require_nonnegative(self.started_wall_ns, "acceptance evidence start time")
        require_nonnegative(self.finished_wall_ns, "acceptance evidence finish time")
        if self.finished_wall_ns < self.started_wall_ns:
            raise AcceptanceEvidenceErrorV1("acceptance evidence finish precedes start")
        require_text(self.test_command, "acceptance evidence test command", max_length=1024)
        require_nonnegative(self.total_cases, "acceptance evidence total cases")
        require_nonnegative(self.failed_cases, "acceptance evidence failed cases")
        require_nonnegative(self.trace_pair_count, "acceptance evidence trace pairs")
        require_nonnegative(self.trace_gap_count, "acceptance evidence trace gaps")
        if self.failed_cases > self.total_cases:
            raise AcceptanceEvidenceErrorV1("acceptance evidence failed cases exceed total cases")
        object.__setattr__(self, "metadata", _freeze_metadata(self.metadata))

    @property
    def passed(self) -> bool:
        """Whether the evidence records a clean accepted stage result."""

        return (
            self.status is AcceptanceEvidenceStatusV1.ACCEPTED
            and self.failed_cases == 0
            and self.cleanup_state is not EvidenceCleanupStateV1.FAILED
        )

    def as_dict(self) -> dict[str, object]:
        """Return a serializable manifest without any credential-bearing field."""

        return {
            "stage": self.stage,
            "status": self.status.value,
            "source_commit": self.source_commit,
            "profile_digest": self.profile_digest,
            "environment_fingerprint": self.environment_fingerprint,
            "started_wall_ns": self.started_wall_ns,
            "finished_wall_ns": self.finished_wall_ns,
            "test_command": self.test_command,
            "total_cases": self.total_cases,
            "failed_cases": self.failed_cases,
            "trace_pair_count": self.trace_pair_count,
            "trace_gap_count": self.trace_gap_count,
            "cleanup_state": self.cleanup_state.value,
            "metadata": dict(self.metadata or {}),
        }
