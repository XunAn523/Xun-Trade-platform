from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.gateway_readonly import ReadOnlyCase
from perfbench.query_fixture import (
    QueryFixtureError,
    load_query_fixture,
    query_fixture_template,
    required_callback_capacity,
    select_query_cases,
)


class QueryFixtureTests(unittest.TestCase):
    def test_catalog_complete_template_loads_only_locally_enabled_profile_cases(self) -> None:
        template = query_fixture_template()
        self.assertTrue(template["cases"])
        position = next(
            item
            for item in template["cases"]
            if item["api"] == "QueryPosition" and item["profile"] == "ordinary"
        )
        self.assertEqual([""], position["business_args"])
        entry = next(
            item
            for item in template["cases"]
            if item["api"] == "QueryAsset" and item["profile"] == "ordinary"
        )
        entry["enabled"] = True
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            cases = load_query_fixture(path, profile="ordinary")
        self.assertEqual(("QueryAsset",), tuple(case.api_name for case in cases))

    def test_callback_capacity_contract_is_optional_but_validated_when_present(self) -> None:
        template = query_fixture_template()
        template["minimum_callback_capacity"]["ordinary"] = 8192
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            self.assertEqual(8192, required_callback_capacity(path, profile="ordinary"))

            template["minimum_callback_capacity"]["ordinary"] = True
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QueryFixtureError, "must be an integer"):
                required_callback_capacity(path, profile="ordinary")

    def test_missing_catalog_entry_or_sensitive_argument_is_rejected(self) -> None:
        template = query_fixture_template()
        template["cases"].pop()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QueryFixtureError, "catalog mismatch"):
                load_query_fixture(path, profile="ordinary")

        template = query_fixture_template()
        entry = next(
            item
            for item in template["cases"]
            if item["api"] == "QueryOrders" and item["profile"] == "ordinary"
        )
        entry["enabled"] = True
        entry["business_args"] = [{"password": "synthetic-must-not-be-here"}]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-fixture.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QueryFixtureError, "unsafe"):
                load_query_fixture(path, profile="ordinary")

    def test_explicit_selection_stays_within_enabled_cases(self) -> None:
        cases = (ReadOnlyCase("QueryAsset"), ReadOnlyCase("QueryPosition", ("",)))
        selected = select_query_cases(cases, ("QueryPosition",))
        self.assertEqual(("QueryPosition",), tuple(case.api_name for case in selected))
        with self.assertRaisesRegex(QueryFixtureError, "not enabled"):
            select_query_cases(cases, ("QueryFund",))
        with self.assertRaisesRegex(QueryFixtureError, "must not repeat"):
            select_query_cases(cases, ("QueryAsset", "QueryAsset"))
