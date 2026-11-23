from __future__ import annotations

from xuanling_tradecore.adapters.emt.trading_catalog_v1 import (
    TRADER_API_SPECS_V1,
    TRADER_SPI_SPECS_V1,
    EmtCapabilityState,
    TraderApiCapabilityV1,
    TraderSpiCapabilityV1,
)
from xuanling_tradecore.adapters.emt.trading_readiness_v1 import (
    EmtCapabilityEvidenceV1,
    EmtEvidenceStateV1,
    EmtEvidenceTargetV1,
    EmtTradingReadinessEvaluatorV1,
)
from xuanling_tradecore.adapters.emt.trading_scope_v1 import ORDINARY_CREDIT_RELEASE_SCOPE_V1


def _api(
    state: EmtCapabilityState = EmtCapabilityState.SUPPORTED,
) -> tuple[TraderApiCapabilityV1, ...]:
    return tuple(TraderApiCapabilityV1(spec, state, None) for spec in TRADER_API_SPECS_V1)


def _spi(present: bool = True) -> tuple[TraderSpiCapabilityV1, ...]:
    return tuple(
        TraderSpiCapabilityV1(spec, present, None if present else "missing")
        for spec in TRADER_SPI_SPECS_V1
    )


def _evidence(
    state: EmtEvidenceStateV1 = EmtEvidenceStateV1.SUPPORTED_VERIFIED,
) -> tuple[EmtCapabilityEvidenceV1, ...]:
    return tuple(
        [
            EmtCapabilityEvidenceV1(
                EmtEvidenceTargetV1.TRADER_API,
                spec.official_name,
                state,
                f"api-{spec.official_name}",
            )
            for spec in TRADER_API_SPECS_V1
        ]
        + [
            EmtCapabilityEvidenceV1(
                EmtEvidenceTargetV1.TRADER_SPI,
                spec.official_name,
                state,
                f"spi-{spec.official_name}",
            )
            for spec in TRADER_SPI_SPECS_V1
        ]
    )


def test_t6_requires_per_item_evidence_and_hot_path_signoff() -> None:
    report = EmtTradingReadinessEvaluatorV1().evaluate(
        api_capabilities=_api(),
        spi_capabilities=_spi(),
        evidence=_evidence(),
        hot_path_regression_passed=True,
        disarmed_profile_approved=True,
    )
    assert report.all_trading_capabilities_verified
    assert report.controlled_release_eligible
    assert report.verified_api_count == 86
    assert report.verified_spi_count == 69


def test_t6_contract_only_evidence_or_hot_path_failure_cannot_release() -> None:
    report = EmtTradingReadinessEvaluatorV1().evaluate(
        api_capabilities=_api(),
        spi_capabilities=_spi(),
        evidence=_evidence(EmtEvidenceStateV1.SUPPORTED_CONTRACT_ONLY),
        hot_path_regression_passed=False,
        disarmed_profile_approved=True,
    )
    assert not report.controlled_release_eligible
    assert any("performance:hot-path" in item for item in report.blocking_items)


def test_t6_explicitly_disabled_binding_can_be_controlled_but_never_full_coverage() -> None:
    entries = list(_api())
    first = entries[0]
    entries[0] = TraderApiCapabilityV1(
        first.spec, EmtCapabilityState.UNSUPPORTED_BINDING, "vendor absent"
    )
    evidence = list(_evidence())
    evidence[0] = EmtCapabilityEvidenceV1(
        EmtEvidenceTargetV1.TRADER_API,
        first.spec.official_name,
        EmtEvidenceStateV1.UNSUPPORTED_BINDING,
        "vendor-gap",
    )
    report = EmtTradingReadinessEvaluatorV1().evaluate(
        api_capabilities=entries,
        spi_capabilities=_spi(),
        evidence=evidence,
        hot_path_regression_passed=True,
        disarmed_profile_approved=True,
    )
    assert report.controlled_release_eligible
    assert not report.all_trading_capabilities_verified
    assert report.unavailable_api == (first.spec.official_name,)


def test_ordinary_credit_scope_excludes_option_only_evidence_from_its_release_gate() -> None:
    evidence = [
        EmtCapabilityEvidenceV1(
            EmtEvidenceTargetV1.TRADER_API,
            spec.official_name,
            (
                EmtEvidenceStateV1.UNSUPPORTED_VENUE
                if spec.domain.value == "option"
                else EmtEvidenceStateV1.SUPPORTED_VERIFIED
            ),
            f"api-{spec.official_name}",
        )
        for spec in TRADER_API_SPECS_V1
    ]
    evidence.extend(
        EmtCapabilityEvidenceV1(
            EmtEvidenceTargetV1.TRADER_SPI,
            spec.official_name,
            (
                EmtEvidenceStateV1.UNSUPPORTED_VENUE
                if spec.domain.value == "option"
                else EmtEvidenceStateV1.SUPPORTED_VERIFIED
            ),
            f"spi-{spec.official_name}",
        )
        for spec in TRADER_SPI_SPECS_V1
    )

    report = EmtTradingReadinessEvaluatorV1().evaluate(
        api_capabilities=_api(),
        spi_capabilities=_spi(),
        evidence=evidence,
        hot_path_regression_passed=True,
        disarmed_profile_approved=True,
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
    )

    assert report.release_scope_id == "emt-ordinary-credit-v1"
    assert report.controlled_release_eligible
    assert report.all_scoped_capabilities_verified
    assert not report.all_trading_capabilities_verified
    assert report.blocking_items == ()
    assert report.scoped_api_count < len(TRADER_API_SPECS_V1)
    assert report.scoped_spi_count < len(TRADER_SPI_SPECS_V1)
