from __future__ import annotations

import json
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from unittest.mock import patch

from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import InMemoryJournal
from perfbench.cli import main
from perfbench.gateway_mutation import (
    GatewayMutationError,
    GatewayMutationSettings,
    _CallbackDrainer,
    _MutationCallbackIngress,
    _write_payload_free_trace,
    run_gateway_mutation,
    select_order_cancel_plan,
)
from perfbench.mutation_fixture import (
    MutationFixtureError,
    load_mutation_fixture,
    mutation_fixture_template,
    validate_mutation_fixture,
)
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts
from perfbench.trace import GatewayTraceCollector


class MutationFixtureTests(unittest.TestCase):
    def test_cli_emits_a_schema_valid_mutation_fixture_without_report_finalization(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory) / "mutation-fixture.json"
            with patch.object(
                sys,
                "argv",
                [
                    "emt-gateway-perf",
                    "--driver",
                    "mutation-fixture-template",
                    "--output",
                    str(output),
                ],
            ), redirect_stdout(StringIO()):
                self.assertEqual(0, main())
            fixture = json.loads(output.read_text(encoding="utf-8"))
        validate_mutation_fixture(fixture)
        self.assertFalse(fixture["safety"]["armed"])

    def test_query_callback_router_forwards_only_safe_semantic_metadata(self) -> None:
        class Receiver:
            def __init__(self) -> None:
                self.calls: list[tuple[str, dict[str, object]]] = []

            def receive(self, callback_name: str, **metadata: object) -> str:
                self.calls.append((callback_name, metadata))
                return "ok"

        commands, queries = Receiver(), Receiver()
        ingress = _MutationCallbackIngress(commands=commands, queries=queries)
        self.assertEqual(
            "ok",
            ingress.receive(
                "OnQueryOrder",
                profile="ordinary",
                workload="steady",
                request_id=7,
                is_last=True,
                query_item_present=True,
                query_reference=3,
            ),
        )
        self.assertFalse(commands.calls)
        self.assertEqual("OnQueryOrder", queries.calls[0][0])
        self.assertEqual(True, queries.calls[0][1]["query_item_present"])
        self.assertEqual(3, queries.calls[0][1]["query_reference"])

        marker = object()
        self.assertEqual(
            "ok",
            ingress.receive(
                "OnOrderEvent",
                profile="ordinary",
                workload="steady",
                order_emt_id=11,
                ledger_fact=marker,  # type: ignore[arg-type]  # marker is a focused ledger test double
            ),
        )
        self.assertEqual(marker, commands.calls[0][1]["ledger_fact"])

    def test_template_is_complete_and_disarmed(self) -> None:
        template = mutation_fixture_template()
        validate_mutation_fixture(template)
        self.assertFalse(template["safety"]["armed"])
        self.assertTrue(template["cases"])
        self.assertTrue(all(not entry["enabled"] for entry in template["cases"]))

    def test_enabled_case_requires_arming_rate_and_reconciliation(self) -> None:
        template = mutation_fixture_template()
        entry = template["cases"][0]
        entry["enabled"] = True
        with self.assertRaisesRegex(ValueError, "enabled mutation"):
            validate_mutation_fixture(template)

    def test_loader_selects_only_armed_local_profile_entries(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        entry = next(
            item
            for item in template["cases"]
            if item["api"] == "InsertOrder" and item["profile"] == "ordinary"
        )
        entry.update(
            {
                "enabled": True,
                "request": {"ticker": "000001", "market": 1},
                "rate_limit_per_second": 1,
                "reconciliation_query": "QueryOrderByEMTID",
            }
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            entries = load_mutation_fixture(path, profile="ordinary")
        self.assertEqual(("InsertOrder",), tuple(entry.api_name for entry in entries))
        self.assertEqual({"ticker": "000001", "market": 1}, entries[0].request)

    def test_loader_rejects_sensitive_request(self) -> None:
        template = mutation_fixture_template()
        entry = template["cases"][0]
        entry["request"] = {"password": "synthetic-must-not-be-here"}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(MutationFixtureError, "unsafe"):
                load_mutation_fixture(path, profile=entry["profile"])

    def test_order_cancel_plan_is_bounded_and_preflight_never_loads_sdk(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        insert = next(
            item
            for item in template["cases"]
            if item["api"] == "InsertOrder" and item["profile"] == "ordinary"
        )
        cancel = next(
            item
            for item in template["cases"]
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
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            trace_path = Path(directory) / "should-not-exist.traces.jsonl"
            path.write_text(json.dumps(template), encoding="utf-8")
            entries = load_mutation_fixture(path, profile="ordinary")
            plan = select_order_cancel_plan(entries)
            report = run_gateway_mutation(
                GatewayMutationSettings(
                    sdk_root=Path(directory) / "sdk-not-loaded",
                    runtime_dir=Path(directory) / "runtime",
                    fixture=path,
                    trace_output=trace_path,
                    cycles=10,
                    accounts=SimulatedAccounts(
                        host="test-host",
                        port=1,
                        ordinary=SimulatedAccount("ordinary", "not-a-real-password"),
                        credit=SimulatedAccount("credit", "not-a-real-password"),
                        option=SimulatedAccount("option", "not-a-real-password"),
                    ),
                )
            )
        self.assertEqual(100, plan.order_request["quantity"])
        self.assertFalse(report["executed"])
        self.assertFalse(report["safety"]["sdk_loaded"])
        self.assertEqual(10, report["run"]["planned_cycles"])
        self.assertEqual(20, report["run"]["maximum_commands"])
        self.assertFalse(trace_path.exists())

    def test_option_order_cancel_plan_allows_only_one_opening_contract(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        insert = next(
            item
            for item in template["cases"]
            if item["api"] == "InsertOrder" and item["profile"] == "option"
        )
        cancel = next(
            item
            for item in template["cases"]
            if item["api"] == "CancelOrder" and item["profile"] == "option"
        )
        insert.update(
            {
                "enabled": True,
                "request": {
                    "ticker": "90007051",
                    "market": 1,
                    "side": 1,
                    "price_type": 1,
                    "price": 0.839,
                    "quantity": 1,
                    "business_type": 10,
                    "position_effect": 1,
                    "ep_id": 0,
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
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            plan = select_order_cancel_plan(
                load_mutation_fixture(path, profile="option"), profile="option"
            )
        self.assertEqual("option", plan.profile)
        self.assertEqual("90007051", plan.order_request["ticker"])

    def test_credit_order_cancel_plan_accepts_bounded_cash_equity_order(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        insert = next(
            item
            for item in template["cases"]
            if item["api"] == "InsertOrder" and item["profile"] == "credit"
        )
        cancel = next(
            item
            for item in template["cases"]
            if item["api"] == "CancelOrder" and item["profile"] == "credit"
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
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            plan = select_order_cancel_plan(
                load_mutation_fixture(path, profile="credit"), profile="credit"
            )
        self.assertEqual("credit", plan.profile)
        self.assertEqual("000001", plan.order_request["ticker"])

    def test_credit_order_cancel_plan_accepts_one_margin_buy_or_short_sell_lot(self) -> None:
        for side in (21, 22):
            template = mutation_fixture_template()
            template["safety"]["armed"] = True
            insert = next(
                item
                for item in template["cases"]
                if item["api"] == "InsertOrder" and item["profile"] == "credit"
            )
            cancel = next(
                item
                for item in template["cases"]
                if item["api"] == "CancelOrder" and item["profile"] == "credit"
            )
            insert.update(
                {
                    "enabled": True,
                    "request": {
                        "ticker": "000001",
                        "market": 1,
                        "side": side,
                        "price_type": 1,
                        "price": 1.0,
                        "quantity": 100,
                        "business_type": 4,
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
            with TemporaryDirectory() as directory:
                path = Path(directory) / "mutation-fixture.json"
                path.write_text(json.dumps(template), encoding="utf-8")
                plan = select_order_cancel_plan(
                    load_mutation_fixture(path, profile="credit"), profile="credit"
                )
            self.assertEqual(4, plan.order_request["business_type"])
            self.assertEqual(side, plan.order_request["side"])
            self.assertEqual(100, plan.order_request["quantity"])

    def test_credit_margin_fixture_rejects_nonstandard_lot(self) -> None:
        entries = (
            type(
                "Entry",
                (),
                {
                    "api_name": "InsertOrder",
                    "request": {
                        "ticker": "000001",
                        "market": 1,
                        "side": 21,
                        "price_type": 1,
                        "price": 1.0,
                        "quantity": 99,
                        "business_type": 4,
                    },
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOrderByEMTID",
                },
            )(),
            type(
                "Entry",
                (),
                {
                    "api_name": "CancelOrder",
                    "request": {},
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOrderByEMTID",
                },
            )(),
        )
        with self.assertRaisesRegex(GatewayMutationError, "100-share lot"):
            select_order_cancel_plan(entries, profile="credit")  # type: ignore[arg-type]  # malformed fixture validates rejection path

    def test_credit_margin_fixture_rejects_more_than_one_cycle_before_sdk_load(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        insert = next(
            item
            for item in template["cases"]
            if item["api"] == "InsertOrder" and item["profile"] == "credit"
        )
        cancel = next(
            item
            for item in template["cases"]
            if item["api"] == "CancelOrder" and item["profile"] == "credit"
        )
        insert.update(
            {
                "enabled": True,
                "request": {
                    "ticker": "000001",
                    "market": 1,
                    "side": 21,
                    "price_type": 1,
                    "price": 1.0,
                    "quantity": 100,
                    "business_type": 4,
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
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "not-a-real-password"),
            credit=SimulatedAccount("credit", "not-a-real-password"),
            option=SimulatedAccount("option", "not-a-real-password"),
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(GatewayMutationError, "exactly one"):
                run_gateway_mutation(
                    GatewayMutationSettings(
                        sdk_root=Path(directory) / "sdk-not-loaded",
                        runtime_dir=Path(directory) / "runtime",
                        fixture=path,
                        accounts=accounts,
                        profile="credit",
                        cycles=2,
                    )
                )

    def test_option_order_plan_rejects_nonopening_or_expansive_contracts(self) -> None:
        entries = (
            type(
                "Entry",
                (),
                {
                    "api_name": "InsertOrder",
                    "request": {
                        "ticker": "90007051",
                        "market": 1,
                        "side": 1,
                        "price_type": 1,
                        "price": 0.839,
                        "quantity": 2,
                        "business_type": 10,
                        "position_effect": 1,
                        "ep_id": 0,
                    },
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOrderByEMTID",
                },
            )(),
            type(
                "Entry",
                (),
                {
                    "api_name": "CancelOrder",
                    "request": {},
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOrderByEMTID",
                },
            )(),
        )
        with self.assertRaisesRegex(GatewayMutationError, "one opening contract"):
            select_order_cancel_plan(entries, profile="option")  # type: ignore[arg-type]  # malformed fixture validates rejection path

    def test_controlled_batch_rejects_more_than_ten_cycles(self) -> None:
        accounts = SimulatedAccounts(
            host="test-host",
            port=1,
            ordinary=SimulatedAccount("ordinary", "not-a-real-password"),
            credit=SimulatedAccount("credit", "not-a-real-password"),
            option=SimulatedAccount("option", "not-a-real-password"),
        )
        settings = GatewayMutationSettings(
            sdk_root=Path("sdk-not-loaded"),
            runtime_dir=Path("runtime"),
            fixture=Path("fixture-not-read"),
            accounts=accounts,
            cycles=11,
        )
        with self.assertRaisesRegex(GatewayMutationError, "1..10"):
            settings.validate()

    def test_background_drainer_durably_acks_callback_without_a_command_wait(self) -> None:
        trace = GatewayTraceCollector()
        callbacks = CallbackPipeline(
            journal=InMemoryJournal(),
            trace=trace,
            lookup_source=lambda record: None,
            capacity=4,
        )
        callback_id = callbacks.receive(
            "OnOrderEvent", profile="ordinary", workload="steady", order_emt_id=1
        )
        self.assertIsNotNone(callback_id)
        drainer = _CallbackDrainer(callbacks)
        drainer.start()
        try:
            deadline = monotonic() + 1.0
            while monotonic() < deadline:
                record = next(
                    (item for item in trace.records() if item.correlation_id == callback_id), None
                )
                if record is not None and record.outcome == "ok":
                    break
                sleep(0.001)
            else:
                self.fail("background drainer did not ACK the queued callback")
        finally:
            drainer.close()
        self.assertIsNone(drainer.failure)

    def test_trace_artifact_contains_only_serialized_trace_events(self) -> None:
        events = [
            {
                "correlation_id": "trace-1",
                "kind": "api",
                "interface": "InsertOrder",
                "profile": "ordinary",
                "workload": "steady",
                "stage": "C0",
                "timestamp_ns": 1,
            }
        ]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation.traces.jsonl"
            _write_payload_free_trace(path, events)
            restored = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events, restored)

    def test_order_cycle_rejects_extra_enabled_command(self) -> None:
        template = mutation_fixture_template()
        template["safety"]["armed"] = True
        entries = []
        for api_name in ("InsertOrder", "CancelOrder", "FundTransfer"):
            entry = next(
                item
                for item in template["cases"]
                if item["api"] == api_name and item["profile"] == "ordinary"
            )
            entry.update(
                {
                    "enabled": True,
                    "request": {},
                    "rate_limit_per_second": 1,
                    "reconciliation_query": "QueryOrderByEMTID",
                }
            )
            entries.append(entry)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mutation-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(GatewayMutationError, "exactly enabled"):
                select_order_cancel_plan(load_mutation_fixture(path, profile="ordinary"))
