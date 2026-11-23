"""Evidence-based release evaluator for EMT's complete trading-only surface.

This is a release gate, not an SDK invoker.  It makes missing vendor bindings,
unverified simulated-account features and hot-path regression explicit rather
than upgrading a capability from local tests to production readiness.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from .ordinary_credit_release_certificate_v1 import (
    EmtOrdinaryCreditReleaseCertificateV1,
    trading_catalog_sha256_v1,
)
from .trading_catalog_v1 import (
    TRADER_API_SPECS_V1,
    TRADER_SPI_SPECS_V1,
    EmtCapabilityState,
    TraderApiCapabilityV1,
    TraderSpiCapabilityV1,
)
from .trading_scope_v1 import ALL_TRADING_RELEASE_SCOPE_V1, EmtTradingReleaseScopeV1


class EmtTradingReadinessError(RuntimeError):
    """Capability evidence cannot form a complete trading release decision."""


class EmtEvidenceTargetV1(str, Enum):
    TRADER_API = "TraderApi"
    TRADER_SPI = "TraderSpi"


class EmtEvidenceStateV1(str, Enum):
    SUPPORTED_VERIFIED = "supported_verified"
    SUPPORTED_CONTRACT_ONLY = "supported_contract_only"
    UNSUPPORTED_BINDING = "unsupported_binding"
    UNSUPPORTED_ACCOUNT = "unsupported_account"
    UNSUPPORTED_VENUE = "unsupported_venue"
    BLOCKED_SEMANTICS = "blocked_semantics"


@dataclass(frozen=True)
class EmtCapabilityEvidenceV1:
    target: EmtEvidenceTargetV1
    official_name: str
    state: EmtEvidenceStateV1
    evidence_id: str

    def __post_init__(self) -> None:
        if not self.official_name or not self.evidence_id or len(self.evidence_id) > 160:
            raise EmtTradingReadinessError("capability evidence identity is invalid")


@dataclass(frozen=True)
class EmtTradingReadinessReportV1:
    catalog_sha256: str
    verified_api_count: int
    verified_spi_count: int
    unavailable_api: tuple[str, ...]
    unavailable_spi: tuple[str, ...]
    blocking_items: tuple[str, ...]
    all_trading_capabilities_verified: bool
    controlled_release_eligible: bool
    release_scope_id: str
    scoped_api_count: int
    scoped_spi_count: int
    all_scoped_capabilities_verified: bool
    release_certificate_id: str | None
    release_certificate_sha256: str | None
    release_certificate_lifecycle: str | None


class EmtTradingReadinessEvaluatorV1:
    """Cross-check runtime ABI, per-interface evidence and release fences."""

    def evaluate(
        self,
        *,
        api_capabilities: Iterable[TraderApiCapabilityV1],
        spi_capabilities: Iterable[TraderSpiCapabilityV1],
        evidence: Iterable[EmtCapabilityEvidenceV1],
        hot_path_regression_passed: bool,
        disarmed_profile_approved: bool,
        release_scope: EmtTradingReleaseScopeV1 = ALL_TRADING_RELEASE_SCOPE_V1,
        release_certificate: EmtOrdinaryCreditReleaseCertificateV1 | None = None,
    ) -> EmtTradingReadinessReportV1:
        api = _index_api(api_capabilities)
        spi = _index_spi(spi_capabilities)
        evidence_index = _index_evidence(evidence)
        _require_exact_catalogue(api, spi, evidence_index)
        blocking: list[str] = []
        scoped_blocking: list[str] = []
        unavailable_api: list[str] = []
        unavailable_spi: list[str] = []
        scoped_unavailable_api: list[str] = []
        scoped_unavailable_spi: list[str] = []
        verified_api = 0
        verified_spi = 0
        scoped_verified_api = 0
        scoped_verified_spi = 0
        for spec in TRADER_API_SPECS_V1:
            observed = api[spec.official_name]
            entry = evidence_index[(EmtEvidenceTargetV1.TRADER_API, spec.official_name)]
            in_scope = release_scope.allows_api_spec(spec)
            if observed.state is EmtCapabilityState.UNSUPPORTED_BINDING:
                unavailable_api.append(spec.official_name)
                if in_scope:
                    scoped_unavailable_api.append(spec.official_name)
                if entry.state is not EmtEvidenceStateV1.UNSUPPORTED_BINDING:
                    blocking.append(f"TraderApi:{spec.official_name}:binding-state-mismatch")
                    if in_scope:
                        scoped_blocking.append(
                            f"TraderApi:{spec.official_name}:binding-state-mismatch"
                        )
            elif entry.state is EmtEvidenceStateV1.SUPPORTED_VERIFIED:
                verified_api += 1
                if in_scope:
                    scoped_verified_api += 1
            else:
                item = f"TraderApi:{spec.official_name}:{entry.state.value}"
                blocking.append(item)
                if in_scope:
                    scoped_blocking.append(item)
        for spec in TRADER_SPI_SPECS_V1:
            observed = spi[spec.official_name]
            entry = evidence_index[(EmtEvidenceTargetV1.TRADER_SPI, spec.official_name)]
            in_scope = release_scope.allows_spi_spec(spec)
            if not observed.present:
                unavailable_spi.append(spec.official_name)
                if in_scope:
                    scoped_unavailable_spi.append(spec.official_name)
                if entry.state is not EmtEvidenceStateV1.UNSUPPORTED_BINDING:
                    blocking.append(f"TraderSpi:{spec.official_name}:binding-state-mismatch")
                    if in_scope:
                        scoped_blocking.append(
                            f"TraderSpi:{spec.official_name}:binding-state-mismatch"
                        )
            elif entry.state is EmtEvidenceStateV1.SUPPORTED_VERIFIED:
                verified_spi += 1
                if in_scope:
                    scoped_verified_spi += 1
            else:
                item = f"TraderSpi:{spec.official_name}:{entry.state.value}"
                blocking.append(item)
                if in_scope:
                    scoped_blocking.append(item)
        if not hot_path_regression_passed:
            blocking.append("performance:hot-path-regression-not-passed")
            scoped_blocking.append("performance:hot-path-regression-not-passed")
        if not disarmed_profile_approved:
            blocking.append("release:disarmed-profile-not-approved")
            scoped_blocking.append("release:disarmed-profile-not-approved")
        if release_certificate is not None:
            if release_certificate.release_scope_id != release_scope.scope_id:
                raise EmtTradingReadinessError(
                    "release certificate scope does not match evaluator scope"
                )
            if release_certificate.catalog_sha256 != _catalog_sha256():
                raise EmtTradingReadinessError("release certificate catalog hash does not match")
            certificate_blockers = release_certificate.readiness_blocking_items()
            blocking.extend(certificate_blockers)
            scoped_blocking.extend(certificate_blockers)
        # A controlled release may ship with explicitly disabled vendor holes,
        # but a claim of *full* trading coverage may not.
        all_verified = not unavailable_api and not unavailable_spi and not blocking
        controlled_eligible = not scoped_blocking
        return EmtTradingReadinessReportV1(
            catalog_sha256=_catalog_sha256(),
            verified_api_count=verified_api,
            verified_spi_count=verified_spi,
            unavailable_api=tuple(unavailable_api),
            unavailable_spi=tuple(unavailable_spi),
            blocking_items=tuple(scoped_blocking),
            all_trading_capabilities_verified=all_verified,
            controlled_release_eligible=controlled_eligible,
            release_scope_id=release_scope.scope_id,
            scoped_api_count=sum(
                release_scope.allows_api_spec(spec) for spec in TRADER_API_SPECS_V1
            ),
            scoped_spi_count=sum(
                release_scope.allows_spi_spec(spec) for spec in TRADER_SPI_SPECS_V1
            ),
            all_scoped_capabilities_verified=(
                not scoped_unavailable_api and not scoped_unavailable_spi and not scoped_blocking
            ),
            release_certificate_id=(
                release_certificate.certificate_id if release_certificate is not None else None
            ),
            release_certificate_sha256=(
                release_certificate.content_sha256 if release_certificate is not None else None
            ),
            release_certificate_lifecycle=(
                release_certificate.lifecycle if release_certificate is not None else None
            ),
        )


def _index_api(entries: Iterable[TraderApiCapabilityV1]) -> dict[str, TraderApiCapabilityV1]:
    result = {entry.spec.official_name: entry for entry in entries}
    if len(result) != len(TRADER_API_SPECS_V1):
        raise EmtTradingReadinessError("TraderApi runtime capability evidence is incomplete")
    return result


def _index_spi(entries: Iterable[TraderSpiCapabilityV1]) -> dict[str, TraderSpiCapabilityV1]:
    result = {entry.spec.official_name: entry for entry in entries}
    if len(result) != len(TRADER_SPI_SPECS_V1):
        raise EmtTradingReadinessError("TraderSpi runtime capability evidence is incomplete")
    return result


def _index_evidence(
    entries: Iterable[EmtCapabilityEvidenceV1],
) -> dict[tuple[EmtEvidenceTargetV1, str], EmtCapabilityEvidenceV1]:
    result: dict[tuple[EmtEvidenceTargetV1, str], EmtCapabilityEvidenceV1] = {}
    for entry in entries:
        key = (entry.target, entry.official_name)
        if key in result:
            raise EmtTradingReadinessError("duplicate capability evidence entry")
        result[key] = entry
    return result


def _require_exact_catalogue(
    api: dict[str, TraderApiCapabilityV1],
    spi: dict[str, TraderSpiCapabilityV1],
    evidence: dict[tuple[EmtEvidenceTargetV1, str], EmtCapabilityEvidenceV1],
) -> None:
    expected_api = {spec.official_name for spec in TRADER_API_SPECS_V1}
    expected_spi = {spec.official_name for spec in TRADER_SPI_SPECS_V1}
    if set(api) != expected_api or set(spi) != expected_spi:
        raise EmtTradingReadinessError("runtime capability names diverge from the static catalog")
    expected_evidence = {(EmtEvidenceTargetV1.TRADER_API, name) for name in expected_api}
    expected_evidence |= {(EmtEvidenceTargetV1.TRADER_SPI, name) for name in expected_spi}
    if set(evidence) != expected_evidence:
        raise EmtTradingReadinessError("release evidence must cover exactly every API and SPI item")


def _catalog_sha256() -> str:
    return trading_catalog_sha256_v1()
