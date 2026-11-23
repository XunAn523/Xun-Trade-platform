from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_option_actions import (
    OptionActionError,
    OptionActionSettings,
    run_option_action,
    select_option_action_plan,
)
from perfbench.mutation_fixture import MutationFixtureEntry, mutation_fixture_template
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def _entry(
    api_name: str, request: dict[object, object], reconciliation: str
) -> MutationFixtureEntry:
    return MutationFixtureEntry(
        api_name=api_name,
        profile="option",
        enabled=True,
        request=request,  # type: ignore[arg-type]  # synthetic request exercises adapter mapping only
        rate_limit_per_second=1,
        reconciliation_query=reconciliation,
    )


def _combined_request() -> dict[str, object]:
    return {
        "market": 1,
        "quantity": 1,
        "side": 31,
        "business_type": 13,
        "opt_comb_info": {
            "strategy_id": "CNSJC",
            "comb_num": "",
            "num_legs": 2,
            "leg_detail": [
                {
                    "leg_security_id": "90007051",
                    "leg_cntr_type": 1,
                    "leg_side": 1,
                    "leg_covered": 0,
                    "leg_qty": 1,
                },
                {
                    "leg_security_id": "90007052",
                    "leg_cntr_type": 1,
                    "leg_side": 2,
                    "leg_covered": 0,
                    "leg_qty": 1,
                },
            ],
        },
    }


class OptionActionTests(unittest.TestCase):
    def test_plan_accepts_each_bounded_option_action(self) -> None:
        combined = select_option_action_plan(
            (
                _entry(
                    "InsertOptionCombinedOrder",
                    _combined_request(),
                    "QueryOptionCombinedOrderByEMTID",
                ),
            )
        )
        exercise = select_option_action_plan(
            (
                _entry(
                    "InsertOptionExerciseOrder",
                    {
                        "market": 1,
                        "business_type": 11,
                        "quantity": 1,
                        "ticker": ["90007051", ""],
                    },
                    "QueryPosition",
                ),
            )
        )
        self.assertEqual("InsertOptionCombinedOrder", combined.entry.api_name)
        self.assertEqual("QueryPosition", exercise.reconciliation_query)

    def test_plan_rejects_multiple_or_expansive_actions(self) -> None:
        with self.assertRaisesRegex(OptionActionError, "exactly one"):
            select_option_action_plan(
                (
                    _entry(
                        "InsertOptionCombinedOrder",
                        _combined_request(),
                        "QueryOptionCombinedOrderByEMTID",
                    ),
                    _entry(
                        "InsertOptionExerciseOrder",
                        {
                            "market": 1,
                            "business_type": 11,
                            "quantity": 1,
                            "ticker": ["90007051", ""],
                        },
                        "QueryPosition",
                    ),
                )
            )
        invalid = _combined_request()
        invalid["quantity"] = 2
        with self.assertRaisesRegex(OptionActionError, "one Shenzhen"):
            select_option_action_plan(
                (_entry("InsertOptionCombinedOrder", invalid, "QueryOptionCombinedOrderByEMTID"),)
            )

    def test_preflight_never_loads_sdk_or_sends_an_option_action(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "password"),
            credit=SimulatedAccount("credit", "password"),
            option=SimulatedAccount("option", "password"),
        )
        with TemporaryDirectory() as directory:
            fixture = Path(directory) / "fixture.json"
            template = mutation_fixture_template()
            template["safety"]["armed"] = True
            entry = next(
                item
                for item in template["cases"]
                if item["api"] == "InsertOptionCombinedOrder" and item["profile"] == "option"
            )
            entry.update(
                {
                    "enabled": True,
                    "request": _combined_request(),
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOptionCombinedOrderByEMTID",
                }
            )
            fixture.write_text(json.dumps(template), encoding="utf-8")
            report = run_option_action(
                OptionActionSettings(
                    sdk_root=Path(directory) / "sdk-not-loaded",
                    runtime_dir=Path(directory) / "runtime",
                    accounts=accounts,
                    fixture=fixture,
                )
            )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(["InsertOptionCombinedOrder"], report["run"]["planned_commands"])
