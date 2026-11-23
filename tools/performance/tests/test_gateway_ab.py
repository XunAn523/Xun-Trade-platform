from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_ab import (
    DirectTiming,
    GatewayAbError,
    GatewayAbSettings,
    _DirectCallbackProbe,
    _summarize_ab_delta,
    _summarize_direct,
    _summarize_gateway,
    run_gateway_ab,
)
from perfbench.mutation_fixture import mutation_fixture_template
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from perfbench.trace import TraceRecord


def _accounts() -> SimulatedAccounts:
    return SimulatedAccounts(
        host="localhost",
        port=1,
        ordinary=SimulatedAccount("ordinary", "secret"),
        credit=SimulatedAccount("credit", "secret"),
        option=SimulatedAccount("option", "secret"),
    )


def _fixture(path: Path) -> None:
    fixture = mutation_fixture_template()
    fixture["safety"]["armed"] = True
    insert = next(
        item
        for item in fixture["cases"]
        if item["api"] == "InsertOrder" and item["profile"] == "ordinary"
    )
    cancel = next(
        item
        for item in fixture["cases"]
        if item["api"] == "CancelOrder" and item["profile"] == "ordinary"
    )
    insert.update(
        {
            "enabled": True,
            "request": {
                "ticker": "000001",
                "market": 1,
                "side": 1,
                "price_type": 1,
                "price": 1.0,
                "quantity": 100,
                "business_type": 0,
            },
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryOrderByEMTID",
        }
    )
    cancel.update(
        {
            "enabled": True,
            "request": {},
            "rate_limit_per_second": 1,
            "reconciliation_query": "QueryOrderByEMTID",
        }
    )
    path.write_text(json.dumps(fixture), encoding="utf-8")


def _gateway_record(
    interface: str, *, c0: int, c2: int, c4: int, c5: int, c6: int, c7: int
) -> TraceRecord:
    return TraceRecord(
        correlation_id=f"{interface}-{c0}",
        kind="api",
        interface=interface,
        profile="ordinary",
        workload="steady",
        timestamps_ns={"C0": c0, "C2": c2, "C4": c4, "C5": c5, "C6": c6, "C7": c7},
        outcome="ok",
    )


class GatewayAbTests(unittest.TestCase):
    def test_direct_callback_probe_matches_insert_callback_that_arrives_before_return(self) -> None:
        probe = _DirectCallbackProbe()
        pending = probe.begin("InsertOrder")
        probe.observe("OnOrderEvent", order_emt_id=71, operation_native_id=None)
        probe.mark_return(pending, 71, pending.started_ns + 10)
        callback_ns = probe.finish(pending, 0.01)
        self.assertIsNotNone(callback_ns)
        self.assertGreaterEqual(callback_ns or 0, pending.started_ns)

    def test_direct_callback_probe_uses_cancel_operation_id_not_order_id(self) -> None:
        probe = _DirectCallbackProbe()
        pending = probe.begin("CancelOrder")
        probe.observe("OnOrderEvent", order_emt_id=7, operation_native_id=8)
        probe.mark_return(pending, 9, pending.started_ns + 10)
        self.assertIsNone(probe.finish(pending, 0.0))

    def test_summary_reports_all_requested_gateway_segments_and_signed_delta(self) -> None:
        direct = [DirectTiming(1, "InsertOrder", "ok", None, 100, 120, 150)]
        gateway_record = _gateway_record(
            "InsertOrder", c0=200, c2=210, c4=250, c5=290, c6=300, c7=320
        )
        direct_summary = _summarize_direct(direct)
        gateway_summary = _summarize_gateway([(1, "InsertOrder", gateway_record)])
        delta = _summarize_ab_delta(direct, [(1, "InsertOrder", gateway_record)])

        self.assertEqual(20, direct_summary["InsertOrder"]["latency"]["C0_to_C4"]["p99_ns"])
        self.assertEqual(10, gateway_summary["InsertOrder"]["latency"]["C0_to_C2"]["p999_ns"])
        self.assertEqual(40, gateway_summary["InsertOrder"]["latency"]["C2_to_C4"]["p99_ns"])
        self.assertEqual(40, gateway_summary["InsertOrder"]["latency"]["C4_to_C5"]["p99_ns"])
        self.assertEqual(10, gateway_summary["InsertOrder"]["latency"]["C5_to_C6"]["p99_ns"])
        self.assertEqual(20, gateway_summary["InsertOrder"]["latency"]["C6_to_C7"]["p99_ns"])
        self.assertEqual(
            30, delta["InsertOrder"]["gateway_minus_native_direct"]["C0_to_C4"]["p99_ns"]
        )
        self.assertEqual(
            40, delta["InsertOrder"]["gateway_minus_native_direct"]["C0_to_C5"]["p999_ns"]
        )

    def test_preflight_validates_fixture_without_loading_sdk_or_sending_commands(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = root / "mutation-fixture.json"
            _fixture(fixture)
            report = run_gateway_ab(
                GatewayAbSettings(
                    sdk_root=root / "sdk-not-loaded",
                    runtime_dir=root / "runtime",
                    accounts=_accounts(),
                    fixture=fixture,
                    cycles=2,
                )
            )
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(8, report["run"]["planned_commands"])
        self.assertEqual(
            ["C0_to_C2", "C2_to_C4", "C5_to_C6", "C6_to_C7"],
            report["stage_contract"]["not_applicable_for_native_direct"],
        )

    def test_rejects_unapproved_command_rate(self) -> None:
        with self.assertRaises(GatewayAbError):
            GatewayAbSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=_accounts(),
                fixture=Path("fixture"),
                interval_seconds=0.5,
            ).validate()
