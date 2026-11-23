"""Validation for the P0 compatibility-baseline manifest itself."""

from __future__ import annotations

import unittest
from importlib import import_module

from tests.compatibility_baseline import COMPATIBILITY_BASELINE

_EXPECTED_COMPONENTS = {
    "OrderGateway",
    "FileJournal",
    "TraderSpi Bridge",
    "QueryRegistry",
    "AsyncPostgresOrderLedger",
}


def _flatten(suite: unittest.TestSuite) -> list[unittest.TestCase]:
    cases: list[unittest.TestCase] = []
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            cases.extend(_flatten(item))
        else:
            cases.append(item)
    return cases


class CompatibilityBaselineManifestTests(unittest.TestCase):
    def test_manifest_covers_the_five_existing_hot_path_components(self) -> None:
        self.assertEqual(_EXPECTED_COMPONENTS, {item.component for item in COMPATIBILITY_BASELINE})
        self.assertEqual(
            len(COMPATIBILITY_BASELINE), len({item.component for item in COMPATIBILITY_BASELINE})
        )

    def test_sources_and_test_modules_are_importable(self) -> None:
        for item in COMPATIBILITY_BASELINE:
            with self.subTest(component=item.component):
                self.assertIsNotNone(import_module(item.source_module))
                self.assertIsNotNone(import_module(item.test_module))

    def test_every_baseline_case_resolves_to_exactly_one_test(self) -> None:
        loader = unittest.TestLoader()
        seen_test_ids: set[str] = set()

        for item in COMPATIBILITY_BASELINE:
            for baseline_test in item.tests:
                with self.subTest(component=item.component, test_id=baseline_test.test_id):
                    self.assertTrue(baseline_test.test_id.startswith(f"{item.test_module}."))
                    self.assertNotIn(baseline_test.test_id, seen_test_ids)
                    seen_test_ids.add(baseline_test.test_id)
                    cases = _flatten(loader.loadTestsFromName(baseline_test.test_id))
                    self.assertEqual(1, len(cases))
                    self.assertFalse(
                        any(case.__class__.__name__ == "_FailedTest" for case in cases)
                    )

    def test_postgres_cases_are_explicitly_marked(self) -> None:
        postgres_cases = [
            baseline_test
            for item in COMPATIBILITY_BASELINE
            for baseline_test in item.tests
            if baseline_test.requires_postgres
        ]
        self.assertEqual(2, len(postgres_cases))
        self.assertTrue(all("PostgresOrderLedgerTests" in item.test_id for item in postgres_cases))
