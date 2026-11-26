from __future__ import annotations

from hashlib import sha256

from tools.acceptance.ordinary_credit_query_fixture_v1 import ordinary_credit_query_fixture_v1
from tools.acceptance.ordinary_credit_release_certificate_v1 import (
    build_ordinary_credit_release_certificate_v1,
)
from xuanling_tradecore.adapters.emt.ordinary_credit_release_certificate_v1 import (
    load_ordinary_credit_release_certificate_v1,
)
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


def _query_report(profile: str) -> dict[str, object]:
    fixture = ordinary_credit_query_fixture_v1()
    apis = [
        item["api"] for item in fixture["cases"] if item["profile"] == profile and item["enabled"]
    ]
    blocked = {
        "QueryIPOInfoList": ("timeout", "native_accepted_no_callback"),
        "QueryYesterdayAsset": ("timeout", "native_accepted_no_callback"),
        "QueryCreditPositionFullRate": ("timeout", "native_accepted_missing_is_last_true"),
        "QueryCreditExcessStock": ("rejected", "native_return_230113"),
        "QueryMulCreditExcessStock": ("rejected", "native_return_230113"),
    }
    outcomes = []
    observations = []
    for api in apis:
        outcome, reason = blocked.get(api, ("ok", None))
        outcomes.append({"api": api, "outcome": outcome, "reason": reason})
        observations.append(
            {
                "api": api,
                "terminal_seen": outcome == "ok",
            }
        )
    return {
        "driver": "gateway-query-fixture-c0-c7",
        "run": {
            "profile": profile,
            "operation_outcomes": outcomes,
            "operation_observations": observations,
        },
    }


def _pair_report(profile: str, command_api: str, query_api: str) -> dict[str, object]:
    driver = {
        "ordinary": "gateway-fund-transfer-pair-c0-c7",
        "credit": "gateway-credit-quota-pair-c0-c7",
    }[profile]
    return {
        "driver": driver,
        "run": {
            "profile": profile,
            "requested_pairs": 30,
            "completed_pairs": 30,
            "stopped_early": False,
            "commands": [{"api": command_api, "outcome": "ok", "reason": None}] * 60,
            "reconciliation": [{"api": query_api, "outcome": "ok", "terminal_seen": True}] * 60,
        },
    }


def _capabilities() -> tuple[tuple[TraderApiCapabilityV1, ...], tuple[TraderSpiCapabilityV1, ...]]:
    return (
        tuple(
            TraderApiCapabilityV1(spec, EmtCapabilityState.SUPPORTED, None)
            for spec in TRADER_API_SPECS_V1
        ),
        tuple(TraderSpiCapabilityV1(spec, True, None) for spec in TRADER_SPI_SPECS_V1),
    )


def _evidence() -> tuple[EmtCapabilityEvidenceV1, ...]:
    return tuple(
        [
            EmtCapabilityEvidenceV1(
                EmtEvidenceTargetV1.TRADER_API,
                spec.official_name,
                EmtEvidenceStateV1.SUPPORTED_VERIFIED,
                f"api-{spec.official_name}",
            )
            for spec in TRADER_API_SPECS_V1
        ]
        + [
            EmtCapabilityEvidenceV1(
                EmtEvidenceTargetV1.TRADER_SPI,
                spec.official_name,
                EmtEvidenceStateV1.SUPPORTED_VERIFIED,
                f"spi-{spec.official_name}",
            )
            for spec in TRADER_SPI_SPECS_V1
        ]
    )


def test_certificate_keeps_real_terminal_gaps_and_binding_holes_disarmed() -> None:
    certificate = build_ordinary_credit_release_certificate_v1(
        ordinary_query_report=_query_report("ordinary"),
        credit_query_report=_query_report("credit"),
        fund_transfer_pair_report=_pair_report("ordinary", "FundTransfer", "QueryFundTransfer"),
        credit_quota_pair_report=_pair_report(
            "credit", "CreditQuotaTransfer", "QueryCreditQuotaTransfer"
        ),
        source_report_hashes={
            name: sha256(name.encode("ascii")).hexdigest()
            for name in (
                "ordinary_query",
                "credit_query",
                "fund_transfer_pair",
                "credit_quota_pair",
            )
        },
    )
    loaded = load_ordinary_credit_release_certificate_v1(certificate)

    assert loaded.lifecycle == "DISARMED"
    assert certificate["release_conclusion"] == "DISARMED_NOT_READY"
    assert "TraderApi:QueryIPOInfoList:blocked_semantics" in loaded.blocking_items
    assert "TraderSpi:OnCreditMarginAvlTransfer:unsupported_binding" in loaded.blocking_items
    assert set(loaded.verified_control_operations) == {"FundTransfer", "CreditQuotaTransfer"}

    api, spi = _capabilities()
    readiness = EmtTradingReadinessEvaluatorV1().evaluate(
        api_capabilities=api,
        spi_capabilities=spi,
        evidence=_evidence(),
        hot_path_regression_passed=True,
        disarmed_profile_approved=True,
        release_scope=ORDINARY_CREDIT_RELEASE_SCOPE_V1,
        release_certificate=loaded,
    )
    assert readiness.release_certificate_id == loaded.certificate_id
    assert readiness.release_certificate_lifecycle == "DISARMED"
    assert not readiness.controlled_release_eligible
    assert any(
        "QueryCreditPositionFullRate:blocked_semantics" in item for item in readiness.blocking_items
    )


def test_shared_query_cannot_be_promoted_by_the_other_profile() -> None:
    ordinary = _query_report("ordinary")
    credit = _query_report("credit")
    for row in ordinary["run"]["operation_outcomes"]:
        if row["api"] == "QueryOrdersByPage":
            row.update(
                {
                    "outcome": "timeout",
                    "reason": "native_accepted_missing_is_last_true",
                }
            )
    for row in ordinary["run"]["operation_observations"]:
        if row["api"] == "QueryOrdersByPage":
            row["terminal_seen"] = False

    certificate = build_ordinary_credit_release_certificate_v1(
        ordinary_query_report=ordinary,
        credit_query_report=credit,
        fund_transfer_pair_report=_pair_report("ordinary", "FundTransfer", "QueryFundTransfer"),
        credit_quota_pair_report=_pair_report(
            "credit", "CreditQuotaTransfer", "QueryCreditQuotaTransfer"
        ),
        source_report_hashes={
            name: sha256(name.encode("ascii")).hexdigest()
            for name in (
                "ordinary_query",
                "credit_query",
                "fund_transfer_pair",
                "credit_quota_pair",
            )
        },
    )
    observed = {item["official_name"]: item["state"] for item in certificate["observed_api"]}
    assert observed["QueryOrdersByPage"] == "blocked_semantics"
