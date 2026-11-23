"""Build a strict, payload-free ordinary/credit EMT release certificate.

The input reports are already payload-free acceptance artifacts.  This tool
does not import the EMT SDK, read account configuration, open a session, or
write a broker-facing control file.  It copies no source-report content: the
output retains only report digests, public API names, terminal classifications,
and paired-control counts.  Its conclusion is permanently DISARMED.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from tools.acceptance.ordinary_credit_query_fixture_v1 import ordinary_credit_query_fixture_v1
from xuanling_tradecore.adapters.emt.ordinary_credit_release_certificate_v1 import (
    EmtScopedApiObservationV1,
    certificate_with_computed_hash,
    trading_catalog_sha256_v1,
)
from xuanling_tradecore.adapters.emt.trading_catalog_v1 import known_linux_binding_blockers_v1
from xuanling_tradecore.adapters.emt.trading_scope_v1 import ORDINARY_CREDIT_RELEASE_SCOPE_V1


class OrdinaryCreditReleaseEvidenceError(ValueError):
    """A supplied report cannot support the scoped release certificate."""


_QUERY_DRIVER = "gateway-query-fixture-c0-c7"
_PAIR_DRIVERS = {
    "ordinary": "gateway-fund-transfer-pair-c0-c7",
    "credit": "gateway-credit-quota-pair-c0-c7",
}
_PAIR_APIS = {
    "ordinary": ("FundTransfer", "QueryFundTransfer"),
    "credit": ("CreditQuotaTransfer", "QueryCreditQuotaTransfer"),
}
_TERMINAL_CALLBACK_BLOCKING_REASONS = frozenset(
    {
        # Legacy reports used the generic timeout before the query runner
        # retained enough callback cardinality to distinguish the cause.
        "query_timeout_or_session_end",
        # Current reports preserve the two official-terminal contract failures
        # without retaining vendor payloads or error text.
        "native_accepted_no_callback",
        "native_accepted_missing_is_last_true",
    }
)


def build_ordinary_credit_release_certificate_v1(
    *,
    ordinary_query_report: Mapping[str, object],
    credit_query_report: Mapping[str, object],
    fund_transfer_pair_report: Mapping[str, object],
    credit_quota_pair_report: Mapping[str, object],
    source_report_hashes: Mapping[str, str],
) -> dict[str, object]:
    """Build the non-secret certificate from exact ordinary/credit acceptance outputs."""

    expected = _expected_query_api_by_profile()
    observations = _merge_scoped_query_observations(
        _query_observations(
            ordinary_query_report, profile="ordinary", expected=expected["ordinary"]
        ),
        _query_observations(credit_query_report, profile="credit", expected=expected["credit"]),
    )
    _require_paired_control(
        fund_transfer_pair_report,
        profile="ordinary",
        command_api=_PAIR_APIS["ordinary"][0],
        reconciliation_api=_PAIR_APIS["ordinary"][1],
    )
    _require_paired_control(
        credit_quota_pair_report,
        profile="credit",
        command_api=_PAIR_APIS["credit"][0],
        reconciliation_api=_PAIR_APIS["credit"][1],
    )
    observations["FundTransfer"] = "supported_verified"
    observations["QueryFundTransfer"] = "supported_verified"
    observations["CreditQuotaTransfer"] = "supported_verified"
    observations["QueryCreditQuotaTransfer"] = "supported_verified"
    blockers = {
        f"TraderApi:{api}:{state}"
        for api, state in observations.items()
        if state != "supported_verified"
    }
    for official_name in known_linux_binding_blockers_v1():
        target = "TraderApi" if official_name.startswith(("Credit", "Query")) else "TraderSpi"
        blockers.add(f"{target}:{official_name}:unsupported_binding")
    digests = dict(source_report_hashes)
    if set(digests) != {
        "ordinary_query",
        "credit_query",
        "fund_transfer_pair",
        "credit_quota_pair",
    }:
        raise OrdinaryCreditReleaseEvidenceError("certificate source-report identities differ")
    for digest in digests.values():
        _require_sha256(digest, "source report digest")
    certificate_seed = sha256(
        "".join(digests[name] for name in sorted(digests)).encode("ascii")
    ).hexdigest()[:20]
    certificate = certificate_with_computed_hash(
        certificate_id=f"emt-ordinary-credit-{certificate_seed}",
        release_scope_id=ORDINARY_CREDIT_RELEASE_SCOPE_V1.scope_id,
        catalog_sha256=trading_catalog_sha256_v1(),
        source_report_hashes=digests,
        observed_api=tuple(
            EmtScopedApiObservationV1(official_name=name, state=state)
            for name, state in observations.items()
        ),
        verified_control_operations=("FundTransfer", "CreditQuotaTransfer"),
        blocking_items=tuple(blockers),
    )
    return certificate.as_dict()


def _expected_query_api_by_profile() -> dict[str, set[str]]:
    fixture = ordinary_credit_query_fixture_v1()
    result = {"ordinary": set(), "credit": set()}
    for entry in fixture["cases"]:
        if entry["enabled"] and entry["profile"] in result:
            result[entry["profile"]].add(entry["api"])
    if len(result["ordinary"]) != 24 or len(result["credit"]) != 22:
        raise OrdinaryCreditReleaseEvidenceError("ordinary-credit fixture coverage changed")
    return result


def _query_observations(
    report: Mapping[str, object],
    *,
    profile: str,
    expected: set[str],
) -> dict[str, str]:
    if report.get("driver") != _QUERY_DRIVER:
        raise OrdinaryCreditReleaseEvidenceError("query report driver differs")
    run = _object(report.get("run"), "query report run")
    if run.get("profile") != profile:
        raise OrdinaryCreditReleaseEvidenceError("query report profile differs")
    outcomes = run.get("operation_outcomes")
    observations = run.get("operation_observations")
    if not isinstance(outcomes, list) or not isinstance(observations, list):
        raise OrdinaryCreditReleaseEvidenceError("query report outcomes are invalid")
    outcome_by_api = _api_row_index(outcomes, "query outcome")
    observation_by_api = _api_row_index(observations, "query observation")
    if set(outcome_by_api) != expected or set(observation_by_api) != expected:
        raise OrdinaryCreditReleaseEvidenceError("query report API coverage differs from fixture")
    result: dict[str, str] = {}
    for api in sorted(expected):
        outcome = outcome_by_api[api]
        observation = observation_by_api[api]
        status, reason = outcome.get("outcome"), outcome.get("reason")
        terminal = observation.get("terminal_seen")
        if status == "ok":
            if terminal is not True:
                raise OrdinaryCreditReleaseEvidenceError(
                    "successful query lacks a terminal callback"
                )
            result[api] = "supported_verified"
        elif status == "timeout" and reason in _TERMINAL_CALLBACK_BLOCKING_REASONS:
            result[api] = "blocked_semantics"
        elif status == "rejected" and reason == "native_return_230113":
            result[api] = "unsupported_venue"
        else:
            raise OrdinaryCreditReleaseEvidenceError("query report outcome is not certificate-safe")
    return result


def _merge_scoped_query_observations(
    *profiles: Mapping[str, str],
) -> dict[str, str]:
    """Merge shared API evidence without allowing one profile to hide a blocker.

    The V1 certificate intentionally exposes public API names rather than
    account/profile identifiers.  Some read-only APIs are probed by both the
    ordinary and credit fixtures, so a simple dictionary overwrite could turn
    an ordinary-profile failure into a false success when the credit report is
    merged second.  Preserve any non-success state; if two different blocking
    states occur, retain a deterministic blocking state.  Source report hashes
    continue to retain the profile-specific evidence identity.
    """

    merged: dict[str, str] = {}
    for profile in profiles:
        for api, state in profile.items():
            current = merged.get(api)
            if current is None or current == "supported_verified":
                merged[api] = state
            elif state == "supported_verified" or state == current:
                continue
            else:
                merged[api] = min(current, state)
    return merged


def _require_paired_control(
    report: Mapping[str, object],
    *,
    profile: str,
    command_api: str,
    reconciliation_api: str,
) -> None:
    if report.get("driver") != _PAIR_DRIVERS[profile]:
        raise OrdinaryCreditReleaseEvidenceError("control-pair report driver differs")
    run = _object(report.get("run"), "control-pair report run")
    if run.get("profile") != profile or run.get("requested_pairs") != 30:
        raise OrdinaryCreditReleaseEvidenceError("control-pair profile or requested count differs")
    if run.get("completed_pairs") != 30 or run.get("stopped_early") is not False:
        raise OrdinaryCreditReleaseEvidenceError("control-pair did not close every pair")
    commands = run.get("commands")
    reconciliation = run.get("reconciliation")
    if not isinstance(commands, list) or not isinstance(reconciliation, list):
        raise OrdinaryCreditReleaseEvidenceError("control-pair records are invalid")
    if len(commands) != 60 or len(reconciliation) != 60:
        raise OrdinaryCreditReleaseEvidenceError("control-pair record count differs")
    for command in commands:
        if not isinstance(command, Mapping) or command.get("api") != command_api:
            raise OrdinaryCreditReleaseEvidenceError("control-pair command API differs")
        if command.get("outcome") != "ok" or command.get("reason") is not None:
            raise OrdinaryCreditReleaseEvidenceError("control-pair command is not accepted")
    for item in reconciliation:
        if not isinstance(item, Mapping) or item.get("api") != reconciliation_api:
            raise OrdinaryCreditReleaseEvidenceError("control-pair reconciliation API differs")
        if item.get("outcome") != "ok" or item.get("terminal_seen") is not True:
            raise OrdinaryCreditReleaseEvidenceError("control-pair reconciliation is incomplete")


def _api_row_index(rows: list[object], label: str) -> dict[str, Mapping[str, object]]:
    result: dict[str, Mapping[str, object]] = {}
    for row in rows:
        mapping = _object(row, label)
        api = mapping.get("api")
        if not isinstance(api, str) or not api or api in result:
            raise OrdinaryCreditReleaseEvidenceError(f"{label} API identity is invalid")
        result[api] = mapping
    return result


def _object(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OrdinaryCreditReleaseEvidenceError(f"{label} is not an object")
    return value


def _require_sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise OrdinaryCreditReleaseEvidenceError(f"{label} is invalid")


def _read_report(path: Path) -> tuple[Mapping[str, object], str]:
    try:
        payload = path.read_bytes()
        raw: Any = json.loads(payload)
    except (OSError, json.JSONDecodeError) as exc:
        raise OrdinaryCreditReleaseEvidenceError("certificate input report is unreadable") from exc
    if not isinstance(raw, Mapping):
        raise OrdinaryCreditReleaseEvidenceError("certificate input report is not an object")
    return raw, sha256(payload).hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ordinary-query-report", type=Path, required=True)
    parser.add_argument("--credit-query-report", type=Path, required=True)
    parser.add_argument("--fund-transfer-pair-report", type=Path, required=True)
    parser.add_argument("--credit-quota-pair-report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise OrdinaryCreditReleaseEvidenceError("certificate output already exists")
    ordinary, ordinary_hash = _read_report(args.ordinary_query_report)
    credit, credit_hash = _read_report(args.credit_query_report)
    fund, fund_hash = _read_report(args.fund_transfer_pair_report)
    quota, quota_hash = _read_report(args.credit_quota_pair_report)
    certificate = build_ordinary_credit_release_certificate_v1(
        ordinary_query_report=ordinary,
        credit_query_report=credit,
        fund_transfer_pair_report=fund,
        credit_quota_pair_report=quota,
        source_report_hashes={
            "ordinary_query": ordinary_hash,
            "credit_query": credit_hash,
            "fund_transfer_pair": fund_hash,
            "credit_quota_pair": quota_hash,
        },
    )
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(certificate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except OSError as exc:
        raise OrdinaryCreditReleaseEvidenceError("certificate output cannot be written") from exc
    print(
        json.dumps(
            {
                "certificate_id": certificate["certificate_id"],
                "content_sha256": certificate["content_sha256"],
                "lifecycle": certificate["lifecycle"],
                "release_conclusion": certificate["release_conclusion"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
