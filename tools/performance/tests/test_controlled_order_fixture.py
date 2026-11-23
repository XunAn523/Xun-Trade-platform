from __future__ import annotations

import io
import json
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from perfbench.cli import main
from perfbench.controlled_order_fixture import (
    ControlledOrderFixtureError,
    load_controlled_order_fixture,
)
from perfbench.gateway_controlled_order import (
    ControlledOrderError,
    ControlledOrderSettings,
    _OwnedTradeObserver,
    _local_latency_gate_evidence,
    _targeted_command_latency,
    run_controlled_order_experiment,
)
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from perfbench.trace import TraceRecord
from emt_gateway.gateway import CallbackRecord

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "emt-controlled-order-fixture.example.json"
)


def _accounts() -> SimulatedAccounts:
    return SimulatedAccounts(
        host="localhost",
        port=1,
        ordinary=SimulatedAccount("ordinary", "not-a-real-password"),
        credit=SimulatedAccount("credit", "not-a-real-password"),
        option=SimulatedAccount("option", "not-a-real-password"),
    )


class ControlledOrderFixtureTests(unittest.TestCase):
    def test_cancel_error_observer_requires_the_original_and_cancel_native_ids(self) -> None:
        observer = _OwnedTradeObserver()
        observer.own(101)

        def record(*, order_emt_id: int, operation_native_id: int) -> CallbackRecord:
            return CallbackRecord(
                callback_correlation_id="callback",
                callback_name="OnCancelOrderError",
                profile="ordinary",
                workload="steady",
                callback_entry_ns=1,
                ring_enqueue_ns=1,
                order_emt_id=order_emt_id,
                operation_native_id=operation_native_id,
                producer_id="test",
                callback_seq=1,
            )

        # A late error for an earlier cancel is evidence for neither the new
        # duplicate cancel nor the original order under test.
        observer.on_ack(record(order_emt_id=101, operation_native_id=201), 2)
        self.assertFalse(observer.wait_for_cancel_error(101, 202, 0.0))
        observer.on_ack(record(order_emt_id=101, operation_native_id=202), 3)
        self.assertTrue(observer.wait_for_cancel_error(101, 202, 0.0))

    def test_local_fixture_is_disarmed_and_has_bounded_deterministic_samples(self) -> None:
        fixture = load_controlled_order_fixture(_FIXTURE_PATH)
        self.assertFalse(fixture.safety.armed)
        self.assertEqual("ordinary", fixture.safety.account_profile)
        self.assertEqual(("000010",), fixture.safety.allowed_tickers)
        self.assertEqual(100, fixture.safety.max_order_quantity)
        self.assertEqual(8.0, fixture.safety.cancel_timeout_seconds)
        self.assertEqual(40, sum(item.cycles for item in fixture.scenarios))
        self.assertEqual(80, sum(item.cycles for item in fixture.scenarios) * 2)
        sell = next(item for item in fixture.scenarios if item.scenario_id == "fill_sell")
        first = sell.order_request(seed=fixture.safety.random_seed, cycle=1)
        second = sell.order_request(seed=fixture.safety.random_seed, cycle=1)
        self.assertEqual(first, second)
        self.assertIn(first["price"], {1.57, 1.58, 1.59})
        self.assertEqual(100, first["quantity"])

    def test_preflight_does_not_load_sdk_or_send_a_command(self) -> None:
        report = run_controlled_order_experiment(
            ControlledOrderSettings(
                sdk_root=Path("sdk-must-not-load"),
                runtime_dir=Path("runtime-must-not-create"),
                accounts=_accounts(),
                fixture=_FIXTURE_PATH,
            )
        )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(40, report["run"]["planned_order_submissions"])
        self.assertEqual(80, report["run"]["maximum_native_commands"])

    def test_execute_rejects_disarmed_fixture_before_sdk_load_even_when_other_gates_are_bypassed(
        self,
    ) -> None:
        for skip_local_latency_gate in (False, True):
            with (
                self.subTest(skip_local_latency_gate=skip_local_latency_gate),
                self.assertRaisesRegex(ControlledOrderError, "disarmed"),
            ):
                run_controlled_order_experiment(
                    ControlledOrderSettings(
                        sdk_root=Path("sdk-must-not-load"),
                        runtime_dir=Path("runtime-must-not-create"),
                        accounts=_accounts(),
                        fixture=_FIXTURE_PATH,
                        skip_local_latency_gate=skip_local_latency_gate,
                        execute_simulated=True,
                    )
                )

    def test_execute_requires_passing_account_free_gate_before_sdk_load(self) -> None:
        raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
        raw["safety"]["armed"] = True
        with TemporaryDirectory() as directory:
            fixture_path = Path(directory) / "armed-fixture.json"
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ControlledOrderError, "local-latency-gate-report"):
                run_controlled_order_experiment(
                    ControlledOrderSettings(
                        sdk_root=Path("sdk-must-not-load"),
                        runtime_dir=Path("runtime-must-not-create"),
                        accounts=_accounts(),
                        fixture=fixture_path,
                        execute_simulated=True,
                    )
                )

    def test_execute_rejects_a_failed_gate_before_sdk_load(self) -> None:
        raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
        raw["safety"]["armed"] = True
        failed_gate = {
            "driver": "gateway-local-signal-burst",
            "safety": {"order_entry": False},
            "run": {"signals_per_second": 1, "arrival_pattern": "uniform"},
            "one_signal_per_second_local_gate": {"applicable": True, "passed": False},
        }
        with TemporaryDirectory() as directory:
            fixture_path = Path(directory) / "armed-fixture.json"
            gate_path = Path(directory) / "failed-gate.json"
            fixture_path.write_text(json.dumps(raw), encoding="utf-8")
            gate_path.write_text(json.dumps(failed_gate), encoding="utf-8")
            with self.assertRaisesRegex(ControlledOrderError, "did not pass"):
                run_controlled_order_experiment(
                    ControlledOrderSettings(
                        sdk_root=Path("sdk-must-not-load"),
                        runtime_dir=Path("runtime-must-not-create"),
                        accounts=_accounts(),
                        fixture=fixture_path,
                        local_latency_gate_report=gate_path,
                        execute_simulated=True,
                    )
                )

    def test_explicit_gate_bypass_is_auditable(self) -> None:
        evidence = _local_latency_gate_evidence(
            ControlledOrderSettings(
                sdk_root=Path("sdk-must-not-load"),
                runtime_dir=Path("runtime-must-not-create"),
                accounts=_accounts(),
                fixture=_FIXTURE_PATH,
                skip_local_latency_gate=True,
                execute_simulated=True,
            )
        )
        self.assertEqual("explicitly_bypassed", evidence["mode"])
        self.assertIsNone(evidence["passed"])
        self.assertIsNone(evidence["report"])

    def test_fixture_rejects_an_expanded_scenario_budget(self) -> None:
        raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
        raw["scenarios"][0]["cycles"] = 11
        with TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(ControlledOrderFixtureError, "1..10"):
                load_controlled_order_fixture(path)

    def test_fixture_rejects_profile_whitelist_quantity_and_cancel_timeout_expansion(self) -> None:
        cases = (
            ("account_profile", "credit", "account_profile"),
            ("allowed_tickers", ["000010", "000001"], "allowed_tickers"),
            ("max_order_quantity", 200, "max_order_quantity"),
            ("cancel_timeout_seconds", 31.0, "cancel_timeout_seconds"),
        )
        for key, value, message in cases:
            with self.subTest(key=key), TemporaryDirectory() as directory:
                raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
                raw["safety"][key] = value
                path = Path(directory) / "fixture.json"
                path.write_text(json.dumps(raw), encoding="utf-8")
                with self.assertRaisesRegex(ControlledOrderFixtureError, message):
                    load_controlled_order_fixture(path)

    def test_cli_has_no_arming_switch(self) -> None:
        with (
            patch.object(
                sys,
                "argv",
                [
                    "emt-gateway-perf",
                    "--driver",
                    "gateway-controlled-order",
                    "--arm",
                    "--output",
                    "ignored.json",
                ],
            ),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main()

    def test_unknown_selected_scenario_is_rejected(self) -> None:
        fixture = load_controlled_order_fixture(_FIXTURE_PATH)
        with self.assertRaisesRegex(ControlledOrderFixtureError, "unknown"):
            fixture.selected(("not-approved",))

    def test_targeted_summary_contains_the_five_requested_segments(self) -> None:
        records = (
            TraceRecord(
                correlation_id="insert",
                kind="api",
                interface="InsertOrder",
                profile="ordinary",
                workload="steady",
                timestamps_ns={
                    "C0": 10,
                    "C1": 12,
                    "C2": 20,
                    "C3": 25,
                    "C4": 50,
                    "C5": 70,
                    "C6": 80,
                    "C7": 100,
                },
                outcome="ok",
            ),
        )
        summary = _targeted_command_latency(records)
        latency = summary["InsertOrder"]["latency"]
        self.assertEqual(10, latency["C0_to_C2"]["p99_ns"])
        self.assertEqual(2, latency["C0_to_C1"]["p99_ns"])
        self.assertEqual(8, latency["C1_to_C2"]["p99_ns"])
        self.assertEqual(5, latency["C2_to_C3"]["p99_ns"])
        self.assertEqual(25, latency["C3_to_C4"]["p99_ns"])
        self.assertEqual(30, latency["C2_to_C4"]["p999_ns"])
        self.assertEqual(20, latency["C4_to_C5"]["p99_ns"])
        self.assertEqual(20, latency["C6_to_C7"]["p999_ns"])
