from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from perfbench.query_semantic_fixture import (
    QuerySemanticFixtureError,
    load_query_semantic_fixture,
    query_semantic_fixture_template,
)


class QuerySemanticFixtureTests(unittest.TestCase):
    def test_template_is_catalog_complete_and_preconfigures_safe_consumer_cases(self) -> None:
        template = query_semantic_fixture_template(owner="fengyu")
        self.assertEqual(105, len(template["cases"]))
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-semantics.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            empty = load_query_semantic_fixture(path, profile="ordinary", workload="empty")
            pagination = load_query_semantic_fixture(
                path, profile="ordinary", workload="pagination"
            )
        self.assertEqual(("QueryOrders", "QueryTrades"), tuple(case.api_name for case in empty))
        self.assertEqual(
            ("QueryETFByPage", "QuerySecurityByPage"), tuple(case.api_name for case in pagination)
        )
        self.assertTrue(all(case.semantic and case.semantic.workload == "empty" for case in empty))
        self.assertTrue(
            all(case.semantic and case.semantic.minimum_pages == 2 for case in pagination)
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-semantics.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            for profile in ("credit", "option"):
                cases = load_query_semantic_fixture(path, profile=profile, workload="empty")
                self.assertEqual(
                    ("QueryOrders", "QueryTrades"), tuple(case.api_name for case in cases)
                )

    def test_manifest_rejects_catalog_drift_and_unsafe_business_arguments(self) -> None:
        template = query_semantic_fixture_template()
        template["cases"].pop()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-semantics.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QuerySemanticFixtureError, "catalog mismatch"):
                load_query_semantic_fixture(path, profile="ordinary", workload="empty")

        template = query_semantic_fixture_template()
        entry = next(
            item
            for item in template["cases"]
            if item["api"] == "QueryOrders"
            and item["profile"] == "ordinary"
            and item["workload"] == "empty"
        )
        entry["business_args"] = [{"password": "synthetic-must-not-be-here"}]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-semantics.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QuerySemanticFixtureError, "unsafe"):
                load_query_semantic_fixture(path, profile="ordinary", workload="empty")

    def test_enabled_pagination_requires_the_bounded_first_page_contract(self) -> None:
        template = query_semantic_fixture_template()
        entry = next(
            item
            for item in template["cases"]
            if item["api"] == "QueryETFByPage"
            and item["profile"] == "ordinary"
            and item["workload"] == "pagination"
        )
        entry["business_args"] = [{"req_count": 2, "reference": 0, "reserved": 0}]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "query-semantics.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            with self.assertRaisesRegex(QuerySemanticFixtureError, "req_count=1"):
                load_query_semantic_fixture(path, profile="ordinary", workload="pagination")
