from __future__ import annotations

from tools.acceptance.t6_emt_query_acceptance import build_t6_query_acceptance


def _report(profile: str, outcome: str, terminal_seen: bool) -> dict[str, object]:
    return {
        "driver": "gateway-query-fixture-c0-c7",
        "run": {
            "profile": profile,
            "operation_outcomes": [
                {"api": "QueryAsset", "outcome": outcome, "reason": None}
            ],
            "operation_observations": [
                {
                    "api": "QueryAsset",
                    "terminal_seen": terminal_seen,
                    "callback_count": 1 if terminal_seen else 0,
                }
            ],
        },
    }


def test_t6_query_acceptance_requires_each_query_terminal_callback() -> None:
    result = build_t6_query_acceptance(
        (_report("ordinary", "ok", True), _report("credit", "timeout", False))
    )

    assert result["summary"] == {
        "query_cases": 2,
        "terminal_callback_verified": 1,
        "blocked_semantics": 1,
        "unsupported_venue": 0,
        "timeouts": 1,
        "native_rejected": 0,
    }
    cases = {case["profile"]: case for case in result["cases"]}  # type: ignore[index]
    assert cases["ordinary"]["verification"] == "terminal_callback_observed"
    assert cases["credit"]["verification"] == "blocked_semantics"


def test_t6_query_acceptance_classifies_documented_6077_as_unsupported_venue() -> None:
    result = build_t6_query_acceptance(
        (_report("option", "rejected", False),)
    )
    # A generic rejection has no entitlement classification.
    assert result["cases"][0]["verification"] == "blocked_semantics"

    report = _report("option", "rejected", False)
    report["run"]["operation_outcomes"][0]["reason"] = "native_return_6077"  # type: ignore[index]
    classified = build_t6_query_acceptance((report,))
    assert classified["summary"]["unsupported_venue"] == 1
    assert classified["summary"]["blocked_semantics"] == 0
    assert classified["cases"][0]["verification"] == "unsupported_venue"
