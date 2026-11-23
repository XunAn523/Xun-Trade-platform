from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from emt_gateway.query_registry import QuerySemanticObservation
from perfbench.gateway_readonly import (
    GatewayReadOnlyError,
    GatewayReadOnlySettings,
    _burst_probe_passed,
    _classify_incomplete_terminal_callback,
    _delay_after_c5,
    _payload_free_operation_observation,
    _select_evidence_events,
    _write_payload_free_trace,
    known_callback_gaps_for,
    read_only_cases_for,
)
from perfbench.matrix import all_scenarios
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from perfbench.trace import GatewayTraceCollector


class GatewayReadOnlyTests(unittest.TestCase):
    def test_default_callback_capacity_matches_high_cardinality_gateway_default(self) -> None:
        accounts = SimulatedAccounts(
            host="localhost",
            port=1,
            ordinary=SimulatedAccount("ordinary", "secret"),
            credit=SimulatedAccount("credit", "secret"),
            option=SimulatedAccount("option", "secret"),
        )
        self.assertEqual(
            4096,
            GatewayReadOnlySettings(Path("sdk"), Path("runtime"), accounts).callback_capacity,
        )

    def test_evidence_selection_retains_only_selected_query_spi_traces(self) -> None:
        events = [
            {
                "correlation_id": "api",
                "kind": "api",
                "interface": "QueryFundTransfer",
                "profile": "ordinary",
                "workload": "steady",
            },
            {
                "correlation_id": "selected",
                "kind": "spi",
                "interface": "OnQueryFundTransfer",
                "profile": "ordinary",
                "workload": "steady",
            },
            {
                "correlation_id": "other-profile",
                "kind": "spi",
                "interface": "OnQueryFundTransfer",
                "profile": "credit",
                "workload": "steady",
            },
            {
                "correlation_id": "other-callback",
                "kind": "spi",
                "interface": "OnQueryFund",
                "profile": "ordinary",
                "workload": "steady",
            },
            {"event_type": "metric", "metric": "callback_ring_depth", "value": 1},
        ]
        selected = _select_evidence_events(
            events,
            evidence_correlation_ids={"api"},
            query_callback_names={"OnQueryFundTransfer"},
            profile="ordinary",
            workload="steady",
        )
        self.assertEqual(
            ["api", "selected", None], [event.get("correlation_id") for event in selected]
        )

    def test_profile_case_selection_only_uses_matrix_approved_profiles(self) -> None:
        scenarios = {scenario.key for scenario in all_scenarios()}
        for profile in ("ordinary", "credit", "option"):
            for case in read_only_cases_for(profile):
                self.assertIn(f"api:{case.api_name}:{profile}:steady", scenarios)
        self.assertGreater(len(read_only_cases_for("ordinary")), len(read_only_cases_for("credit")))

    def test_known_callback_gaps_are_explicitly_outside_full_chain_cases(self) -> None:
        full_chain_names = {case.api_name for case in read_only_cases_for("ordinary")}
        gap_names = {case.api_name for case in known_callback_gaps_for("ordinary")}
        self.assertEqual({"QueryIPOInfoList", "QueryYesterdayAsset"}, gap_names)
        self.assertFalse(full_chain_names & gap_names)
        self.assertEqual((), known_callback_gaps_for("credit"))
        option_full_chain_names = {case.api_name for case in read_only_cases_for("option")}
        self.assertEqual(
            {"QueryFund"}, {case.api_name for case in known_callback_gaps_for("option")}
        )
        self.assertNotIn("QueryFund", option_full_chain_names)

    def test_low_rate_guard_rejects_unapproved_iteration_or_interval(self) -> None:
        accounts = SimulatedAccounts(
            host="localhost",
            port=1,
            ordinary=SimulatedAccount("ordinary", "secret"),
            credit=SimulatedAccount("credit", "secret"),
            option=SimulatedAccount("option", "secret"),
        )
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                iterations=11,
            ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                interval_seconds=0.1,
            ).validate()
        GatewayReadOnlySettings(
            sdk_root=Path("sdk"),
            runtime_dir=Path("runtime"),
            accounts=accounts,
            callback_capacity=4096,
        ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=accounts,
                callback_capacity=65_537,
            ).validate()

    def test_burst_guard_requires_a_bounded_nontrivial_lane(self) -> None:
        accounts = SimulatedAccounts(
            host="localhost",
            port=1,
            ordinary=SimulatedAccount("ordinary", "secret"),
            credit=SimulatedAccount("credit", "secret"),
            option=SimulatedAccount("option", "secret"),
        )
        GatewayReadOnlySettings(
            Path("sdk"),
            Path("runtime"),
            accounts,
            workload="burst",
            iterations=25,
            burst_width=4,
        ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                Path("sdk"), Path("runtime"), accounts, workload="burst", burst_width=1
            ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                Path("sdk"), Path("runtime"), accounts, workload="burst", burst_width=5
            ).validate()

    def test_burst_expansion_requires_every_probe_request_to_complete(self) -> None:
        self.assertTrue(_burst_probe_passed([{"outcome": "ok"}, {"outcome": "ok"}]))
        self.assertFalse(_burst_probe_passed([]))
        self.assertFalse(_burst_probe_passed([{"outcome": "ok"}, {"outcome": "timeout"}]))

    def test_late_guard_keeps_delay_explicit_and_bounded(self) -> None:
        accounts = SimulatedAccounts(
            host="localhost",
            port=1,
            ordinary=SimulatedAccount("ordinary", "secret"),
            credit=SimulatedAccount("credit", "secret"),
            option=SimulatedAccount("option", "secret"),
        )
        GatewayReadOnlySettings(
            Path("sdk"),
            Path("runtime"),
            accounts,
            workload="late",
            iterations=100,
            late_delay_seconds=0.1,
        ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                Path("sdk"), Path("runtime"), accounts, workload="late"
            ).validate()
        with self.assertRaises(GatewayReadOnlyError):
            GatewayReadOnlySettings(
                Path("sdk"), Path("runtime"), accounts, late_delay_seconds=0.1
            ).validate()

    def test_late_delay_starts_only_after_c5_exists(self) -> None:
        trace = GatewayTraceCollector()
        trace.begin(
            "late-query", kind="api", interface="QueryAsset", profile="ordinary", workload="late"
        )
        trace.mark("late-query", "C1")
        trace.mark("late-query", "C3")
        trace.mark("late-query", "C4")
        trace.mark("late-query", "C5")
        self.assertTrue(
            _delay_after_c5(
                trace=trace,
                correlation_id="late-query",
                timeout_seconds=0.1,
                delay_seconds=0.01,
            )
        )

    def test_trace_archive_serializes_only_collector_events(self) -> None:
        events = [
            {
                "correlation_id": "correlation-1",
                "kind": "api",
                "interface": "QueryAsset",
                "profile": "ordinary",
                "workload": "steady",
                "stage": "C0",
                "timestamp_ns": 1,
            }
        ]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "readonly.traces.jsonl"
            _write_payload_free_trace(path, events)
            restored = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events, restored)

    def test_operation_observation_retains_only_safe_completion_cardinality(self) -> None:
        observation = QuerySemanticObservation(
            correlation_id="correlation-1",
            api_name="QueryCreditDebtInfo",
            callback_count=3,
            item_count=2,
            terminal_seen=True,
            terminal_flag_states="false,true",
            query_reference=987654,
        )
        result = _payload_free_operation_observation(
            {"api": "QueryCreditDebtInfo", "outcome": "ok", "reason": None},
            observation,
        )
        self.assertEqual(
            {
                "api": "QueryCreditDebtInfo",
                "outcome": "ok",
                "reason": None,
                "terminal_seen": True,
                "terminal_flag_states": "false,true",
                "callback_count": 3,
                "item_count": 2,
            },
            result,
        )
        self.assertNotIn("query_reference", result)

    def test_timeout_classification_distinguishes_no_callback_from_missing_terminal(self) -> None:
        no_callback = {"api": "QueryIPOInfoList", "outcome": "timeout", "reason": "old"}
        _classify_incomplete_terminal_callback(
            no_callback,
            QuerySemanticObservation(
                correlation_id="ipo",
                api_name="QueryIPOInfoList",
                callback_count=0,
                item_count=0,
                terminal_seen=False,
                terminal_flag_states="none",
                query_reference=None,
            ),
        )
        self.assertEqual("native_accepted_no_callback", no_callback["reason"])

        no_terminal = {
            "api": "QueryCreditPositionFullRate",
            "outcome": "timeout",
            "reason": "old",
        }
        _classify_incomplete_terminal_callback(
            no_terminal,
            QuerySemanticObservation(
                correlation_id="full-rate",
                api_name="QueryCreditPositionFullRate",
                callback_count=1,
                item_count=0,
                terminal_seen=False,
                terminal_flag_states="false",
                query_reference=None,
            ),
        )
        self.assertEqual("native_accepted_missing_is_last_true", no_terminal["reason"])
