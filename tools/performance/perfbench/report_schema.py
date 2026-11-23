"""Stable execution-boundary envelope for every CLI performance artifact.

Individual drivers retain their own detailed result schemas.  The envelope in
this module is deliberately small and uniform so an artifact can never be
mistaken for an EMT/counter measurement without first declaring whether it
loaded the SDK, logged in, used the network, placed a real order, and observed
the native fast-SPI boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from emt_gateway.schema_validation import SchemaContractError, validate_schema_instance

REPORT_CONTRACT_NAME = "emt-gateway-performance-report"
REPORT_CONTRACT_VERSION = 1


class PerformanceReportSchemaError(ValueError):
    """A performance artifact lacks the required P0 execution-boundary facts."""


@dataclass(frozen=True)
class PerformanceReportBoundaries:
    """Observed execution facts; ``None`` is explicit unobserved evidence."""

    sdk_loaded: bool | None
    login: bool | None
    network: bool | None
    real_order_entry: bool | None
    native_fastspi_required: bool | None
    native_fastspi_available: bool | None

    def as_dict(self) -> dict[str, dict[str, bool | None]]:
        return {
            "sdk_loaded": _observation(self.sdk_loaded),
            "login": _observation(self.login),
            "network": _observation(self.network),
            "real_order_entry": _observation(self.real_order_entry),
            "native_fastspi_required": _observation(self.native_fastspi_required),
            "native_fastspi_available": _observation(self.native_fastspi_available),
        }


_NO_SDK_DRIVERS = frozenset(
    {
        "synthetic",
        "runbook-template",
        "workload-plan-template",
        "gateway-trace-report",
        "baseline-template",
        "gateway-acceptance-report",
        "gateway-release-readiness",
        "gateway-workload-readiness",
        "query-fixture-template",
        "query-semantic-fixture-template",
        "mutation-fixture-template",
        "gateway-callback-injection",
        "gateway-mutation-aggregate",
        "gateway-local-signal-burst",
    }
)

_READ_ONLY_EMT_DRIVERS = frozenset(
    {
        "native-readonly",
        "gateway-readonly",
        "gateway-query-fixture",
        "gateway-query-burst",
        "gateway-query-late",
        "gateway-query-semantics",
        "gateway-session-fault",
        "gateway-mutation-recovery",
    }
)

_EMT_ORDER_ENTRY_DRIVERS = frozenset(
    {
        "gateway-mutation",
        "gateway-ab-order",
        "gateway-credit-repayment",
        "gateway-credit-quota-transfer",
        "gateway-credit-quota-pair",
        "gateway-fund-transfer-pair",
        "gateway-trade-fill",
        "gateway-controlled-order",
        "gateway-option-action",
    }
)

_EXTERNAL_OR_UNCONTROLLED_DRIVERS = frozenset(
    {
        "gateway-fixture",
        "vendor-simulated-sweep",
    }
)


def finalize_performance_report(
    report: dict[str, Any],
    *,
    requested_driver: str,
) -> dict[str, Any]:
    """Attach and validate the common P0 envelope before artifact persistence.

    A third-party fixture process and the vendor sample sweep are intentionally
    marked unobserved when they execute: this harness cannot safely infer their
    SDK/login/network/order behavior.  Such output remains diagnosable but is
    not complete evidence for a performance claim.
    """

    if not isinstance(report.get("driver"), str) or not report["driver"]:
        raise PerformanceReportSchemaError("performance report requires a non-empty driver")
    if not requested_driver:
        raise PerformanceReportSchemaError("requested_driver is required")

    boundaries = boundaries_for_driver(report, requested_driver=requested_driver)
    report["report_contract"] = {
        "name": REPORT_CONTRACT_NAME,
        "schema_version": REPORT_CONTRACT_VERSION,
        "requested_driver": requested_driver,
        "boundary_completeness": (
            "complete"
            if all(value is not None for value in boundaries.__dict__.values())
            else "incomplete"
        ),
        "evidence_classification": _classification_for(requested_driver),
        "execution_boundaries": boundaries.as_dict(),
    }
    validate_performance_report(report)
    return report


def boundaries_for_driver(
    report: Mapping[str, Any],
    *,
    requested_driver: str,
) -> PerformanceReportBoundaries:
    """Derive only documented facts; do not guess external-process behavior."""

    safety = report.get("safety")
    safety_mapping = safety if isinstance(safety, Mapping) else {}
    run = report.get("run")
    run_mapping = run if isinstance(run, Mapping) else {}

    preflight = report.get("executed") is False
    if preflight or requested_driver in _NO_SDK_DRIVERS:
        defaults = PerformanceReportBoundaries(False, False, False, False, None, None)
    elif requested_driver in _READ_ONLY_EMT_DRIVERS:
        defaults = PerformanceReportBoundaries(True, True, True, False, None, None)
    elif requested_driver in _EMT_ORDER_ENTRY_DRIVERS:
        defaults = PerformanceReportBoundaries(True, True, True, True, None, None)
    elif requested_driver in _EXTERNAL_OR_UNCONTROLLED_DRIVERS:
        defaults = PerformanceReportBoundaries(None, None, None, None, None, None)
    else:
        raise PerformanceReportSchemaError(f"unknown CLI performance driver: {requested_driver}")

    return PerformanceReportBoundaries(
        sdk_loaded=_bool_or_default(safety_mapping, "sdk_loaded", defaults.sdk_loaded),
        login=_bool_or_default(safety_mapping, "login", defaults.login),
        network=_bool_or_default(safety_mapping, "network", defaults.network),
        real_order_entry=_bool_or_default(safety_mapping, "order_entry", defaults.real_order_entry),
        native_fastspi_required=_first_bool(
            safety_mapping,
            run_mapping,
            "native_fastspi_required",
            defaults.native_fastspi_required,
        ),
        native_fastspi_available=_first_bool(
            safety_mapping,
            run_mapping,
            "native_fastspi_available",
            defaults.native_fastspi_available,
        ),
    )


def validate_performance_report(report: Mapping[str, Any]) -> None:
    """Reject malformed common envelopes without interpreting driver payloads."""

    contract = report.get("report_contract")
    if not isinstance(contract, Mapping):
        raise PerformanceReportSchemaError("performance report is missing report_contract")
    if contract.get("name") != REPORT_CONTRACT_NAME:
        raise PerformanceReportSchemaError("performance report has an unknown contract name")
    if contract.get("schema_version") != REPORT_CONTRACT_VERSION:
        raise PerformanceReportSchemaError("performance report has an unsupported contract version")
    if contract.get("boundary_completeness") not in {"complete", "incomplete"}:
        raise PerformanceReportSchemaError(
            "performance report has an invalid boundary completeness"
        )
    if not isinstance(contract.get("requested_driver"), str) or not contract["requested_driver"]:
        raise PerformanceReportSchemaError("performance report has no requested_driver")
    if not isinstance(contract.get("evidence_classification"), str):
        raise PerformanceReportSchemaError("performance report has no evidence classification")

    boundaries = contract.get("execution_boundaries")
    if not isinstance(boundaries, Mapping):
        raise PerformanceReportSchemaError("performance report has no execution boundaries")
    expected = set(PerformanceReportBoundaries.__dataclass_fields__)
    if set(boundaries) != expected:
        raise PerformanceReportSchemaError("performance report execution boundary keys are invalid")
    observed_values: list[bool | None] = []
    for name in sorted(expected):
        observation = boundaries[name]
        if not isinstance(observation, Mapping) or set(observation) != {"observed", "value"}:
            raise PerformanceReportSchemaError(f"performance report boundary {name} is malformed")
        observed, value = observation["observed"], observation["value"]
        if (
            type(observed) is not bool
            or (observed and type(value) is not bool)
            or (not observed and value is not None)
        ):
            raise PerformanceReportSchemaError(
                f"performance report boundary {name} has an invalid observation"
            )
        observed_values.append(value)
    expected_completeness = (
        "complete" if all(value is not None for value in observed_values) else "incomplete"
    )
    if contract["boundary_completeness"] != expected_completeness:
        raise PerformanceReportSchemaError(
            "performance report boundary completeness disagrees with observations"
        )
    try:
        validate_schema_instance("performance-report-v1", report)
    except SchemaContractError as exc:
        raise PerformanceReportSchemaError(str(exc)) from exc


def _bool_or_default(mapping: Mapping[str, Any], key: str, default: bool | None) -> bool | None:
    value = mapping.get(key)
    return value if type(value) is bool else default


def _first_bool(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    key: str,
    default: bool | None,
) -> bool | None:
    value = first.get(key)
    if type(value) is bool:
        return value
    value = second.get(key)
    return value if type(value) is bool else default


def _observation(value: bool | None) -> dict[str, bool | None]:
    return {"observed": value is not None, "value": value}


def _classification_for(requested_driver: str) -> str:
    if requested_driver == "gateway-local-signal-burst":
        return "local_stub_reference"
    if requested_driver in _NO_SDK_DRIVERS:
        return "non_emt_or_control_artifact"
    if requested_driver in _EXTERNAL_OR_UNCONTROLLED_DRIVERS:
        return "external_or_uncontrolled_execution"
    return "emt_execution_observation"
