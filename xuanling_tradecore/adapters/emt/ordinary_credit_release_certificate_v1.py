"""Payload-free evidence certificate for the ordinary-and-credit EMT release.

This certificate is deliberately a *DISARMED* control-plane artifact.  It
records exactly what an isolated simulated-account acceptance observed without
embedding account identifiers, business arguments, orders, balances, callback
payloads, endpoints, or credentials.  It is readable by the generic trading
readiness evaluator, but can never assert a full-trading ``READY`` claim.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Mapping

from .trading_catalog_v1 import TRADER_API_SPECS_V1, TRADER_SPI_SPECS_V1


class EmtOrdinaryCreditReleaseCertificateError(ValueError):
    """A scoped release certificate is incomplete, altered, or unsafe."""


_STATES = frozenset(
    {
        "supported_verified",
        "unsupported_binding",
        "unsupported_account",
        "unsupported_venue",
        "blocked_semantics",
    }
)
_SOURCE_NAMES = frozenset(
    {
        "ordinary_query",
        "credit_query",
        "fund_transfer_pair",
        "credit_quota_pair",
    }
)
_CATALOG_API_NAMES = frozenset(spec.official_name for spec in TRADER_API_SPECS_V1)


def trading_catalog_sha256_v1() -> str:
    """Return the stable catalog identity used by scoped release evidence."""

    encoded = json.dumps(
        {
            "api": [spec.official_name for spec in TRADER_API_SPECS_V1],
            "spi": [spec.official_name for spec in TRADER_SPI_SPECS_V1],
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return sha256(encoded).hexdigest()


def _require_sha256(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise EmtOrdinaryCreditReleaseCertificateError(f"{field} must be a lowercase SHA-256")


def _require_text(value: object, field: str, *, max_length: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise EmtOrdinaryCreditReleaseCertificateError(f"{field} is invalid")
    return value


@dataclass(frozen=True)
class EmtScopedApiObservationV1:
    """One accepted or blocked public API observation, without payload data."""

    official_name: str
    state: str

    def __post_init__(self) -> None:
        if self.official_name not in _CATALOG_API_NAMES:
            raise EmtOrdinaryCreditReleaseCertificateError("certificate API is not catalogued")
        if self.state not in _STATES:
            raise EmtOrdinaryCreditReleaseCertificateError("certificate API state is invalid")


@dataclass(frozen=True)
class EmtOrdinaryCreditReleaseCertificateV1:
    """A strict, non-secret statement of this release's observed scope evidence."""

    certificate_id: str
    release_scope_id: str
    catalog_sha256: str
    source_report_hashes: Mapping[str, str]
    observed_api: tuple[EmtScopedApiObservationV1, ...]
    verified_control_operations: tuple[str, ...]
    blocking_items: tuple[str, ...]
    lifecycle: str
    content_sha256: str

    def __post_init__(self) -> None:
        _require_text(self.certificate_id, "certificate id", max_length=160)
        _require_text(self.release_scope_id, "release scope id", max_length=160)
        _require_sha256(self.catalog_sha256, "certificate catalog hash")
        _require_sha256(self.content_sha256, "certificate content hash")
        source_hashes = dict(self.source_report_hashes)
        if set(source_hashes) != _SOURCE_NAMES:
            raise EmtOrdinaryCreditReleaseCertificateError("certificate source reports differ")
        for source, digest in source_hashes.items():
            _require_sha256(digest, f"certificate source hash {source}")
        object.__setattr__(self, "source_report_hashes", dict(sorted(source_hashes.items())))
        observations = tuple(self.observed_api)
        if not observations or len({item.official_name for item in observations}) != len(
            observations
        ):
            raise EmtOrdinaryCreditReleaseCertificateError(
                "certificate API observations repeat or are empty"
            )
        object.__setattr__(
            self, "observed_api", tuple(sorted(observations, key=lambda item: item.official_name))
        )
        operations = tuple(self.verified_control_operations)
        if set(operations) != {"FundTransfer", "CreditQuotaTransfer"}:
            raise EmtOrdinaryCreditReleaseCertificateError(
                "certificate control-operation evidence differs"
            )
        if any(operation not in _CATALOG_API_NAMES for operation in operations):
            raise EmtOrdinaryCreditReleaseCertificateError(
                "certificate control operation is not catalogued"
            )
        object.__setattr__(self, "verified_control_operations", tuple(sorted(operations)))
        blockers = tuple(self.blocking_items)
        if not blockers or len(set(blockers)) != len(blockers):
            raise EmtOrdinaryCreditReleaseCertificateError(
                "certificate blockers repeat or are empty"
            )
        if any(not isinstance(item, str) or not item.startswith("Trader") for item in blockers):
            raise EmtOrdinaryCreditReleaseCertificateError("certificate blocker is invalid")
        object.__setattr__(self, "blocking_items", tuple(sorted(blockers)))
        if self.lifecycle != "DISARMED":
            raise EmtOrdinaryCreditReleaseCertificateError(
                "ordinary-credit certificate must remain DISARMED"
            )
        if self.content_sha256 != self.computed_content_sha256:
            raise EmtOrdinaryCreditReleaseCertificateError("certificate content hash differs")

    @property
    def computed_content_sha256(self) -> str:
        return sha256(self.canonical_bytes()).hexdigest()

    def canonical_bytes(self) -> bytes:
        payload = {
            "catalog_sha256": self.catalog_sha256,
            "certificate_id": self.certificate_id,
            "lifecycle": self.lifecycle,
            "observed_api": [
                {"official_name": item.official_name, "state": item.state}
                for item in sorted(self.observed_api, key=lambda item: item.official_name)
            ],
            "release_scope_id": self.release_scope_id,
            "source_report_hashes": dict(sorted(self.source_report_hashes.items())),
            "verified_control_operations": sorted(self.verified_control_operations),
            "blocking_items": sorted(self.blocking_items),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "certificate_id": self.certificate_id,
            "release_scope_id": self.release_scope_id,
            "catalog_sha256": self.catalog_sha256,
            "source_report_hashes": dict(sorted(self.source_report_hashes.items())),
            "observed_api": [
                {"official_name": item.official_name, "state": item.state}
                for item in self.observed_api
            ],
            "verified_control_operations": list(self.verified_control_operations),
            "blocking_items": list(self.blocking_items),
            "lifecycle": self.lifecycle,
            "content_sha256": self.content_sha256,
            "release_conclusion": "DISARMED_NOT_READY",
        }

    def readiness_blocking_items(self) -> tuple[str, ...]:
        """Return qualified blockers for the generic readiness report."""

        return tuple(
            f"release-certificate:{self.certificate_id}:{item}" for item in self.blocking_items
        )


def load_ordinary_credit_release_certificate_v1(
    raw: Mapping[str, object],
) -> EmtOrdinaryCreditReleaseCertificateV1:
    """Load the exact public certificate schema and verify its content hash."""

    expected = {
        "schema_version",
        "certificate_id",
        "release_scope_id",
        "catalog_sha256",
        "source_report_hashes",
        "observed_api",
        "verified_control_operations",
        "blocking_items",
        "lifecycle",
        "content_sha256",
        "release_conclusion",
    }
    if set(raw) != expected or raw.get("schema_version") != 1:
        raise EmtOrdinaryCreditReleaseCertificateError("certificate fields differ from V1")
    if raw.get("release_conclusion") != "DISARMED_NOT_READY":
        raise EmtOrdinaryCreditReleaseCertificateError("certificate release conclusion is invalid")
    source = raw.get("source_report_hashes")
    observed = raw.get("observed_api")
    operations = raw.get("verified_control_operations")
    blockers = raw.get("blocking_items")
    if not isinstance(source, Mapping) or not isinstance(observed, list):
        raise EmtOrdinaryCreditReleaseCertificateError(
            "certificate source or observations are invalid"
        )
    if not isinstance(operations, list) or not isinstance(blockers, list):
        raise EmtOrdinaryCreditReleaseCertificateError("certificate operation evidence is invalid")
    source_hashes = {
        _require_text(name, "certificate source name", max_length=64): _require_text(
            digest, "certificate source hash", max_length=64
        )
        for name, digest in source.items()
    }
    entries: list[EmtScopedApiObservationV1] = []
    for item in observed:
        if not isinstance(item, Mapping) or set(item) != {"official_name", "state"}:
            raise EmtOrdinaryCreditReleaseCertificateError(
                "certificate API observation fields differ"
            )
        entries.append(
            EmtScopedApiObservationV1(
                official_name=_require_text(item.get("official_name"), "certificate API name"),
                state=_require_text(item.get("state"), "certificate API state", max_length=64),
            )
        )
    return EmtOrdinaryCreditReleaseCertificateV1(
        certificate_id=_require_text(raw.get("certificate_id"), "certificate id", max_length=160),
        release_scope_id=_require_text(
            raw.get("release_scope_id"), "release scope id", max_length=160
        ),
        catalog_sha256=_require_text(
            raw.get("catalog_sha256"), "certificate catalog hash", max_length=64
        ),
        source_report_hashes=source_hashes,
        observed_api=tuple(entries),
        verified_control_operations=tuple(
            _require_text(item, "certificate control operation", max_length=128)
            for item in operations
        ),
        blocking_items=tuple(
            _require_text(item, "certificate blocker", max_length=256) for item in blockers
        ),
        lifecycle=_require_text(raw.get("lifecycle"), "certificate lifecycle", max_length=32),
        content_sha256=_require_text(
            raw.get("content_sha256"), "certificate content hash", max_length=64
        ),
    )


def certificate_with_computed_hash(
    *,
    certificate_id: str,
    release_scope_id: str,
    catalog_sha256: str,
    source_report_hashes: Mapping[str, str],
    observed_api: tuple[EmtScopedApiObservationV1, ...],
    verified_control_operations: tuple[str, ...],
    blocking_items: tuple[str, ...],
) -> EmtOrdinaryCreditReleaseCertificateV1:
    """Build a canonical DISARMED certificate without exposing a mutable hash seam."""

    provisional = EmtOrdinaryCreditReleaseCertificateV1.__new__(
        EmtOrdinaryCreditReleaseCertificateV1
    )
    object.__setattr__(provisional, "certificate_id", certificate_id)
    object.__setattr__(provisional, "release_scope_id", release_scope_id)
    object.__setattr__(provisional, "catalog_sha256", catalog_sha256)
    object.__setattr__(provisional, "source_report_hashes", dict(source_report_hashes))
    object.__setattr__(provisional, "observed_api", tuple(observed_api))
    object.__setattr__(
        provisional, "verified_control_operations", tuple(verified_control_operations)
    )
    object.__setattr__(provisional, "blocking_items", tuple(blocking_items))
    object.__setattr__(provisional, "lifecycle", "DISARMED")
    digest = provisional.computed_content_sha256
    return EmtOrdinaryCreditReleaseCertificateV1(
        certificate_id=certificate_id,
        release_scope_id=release_scope_id,
        catalog_sha256=catalog_sha256,
        source_report_hashes=source_report_hashes,
        observed_api=observed_api,
        verified_control_operations=verified_control_operations,
        blocking_items=blocking_items,
        lifecycle="DISARMED",
        content_sha256=digest,
    )
