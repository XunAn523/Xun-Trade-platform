from __future__ import annotations

import unittest

from perfbench.driver import SyntheticGatewayDriver
from perfbench.runner import CoverageError, assert_full_coverage, run


class RunnerTests(unittest.TestCase):
    def test_synthetic_run_covers_the_full_catalog(self) -> None:
        report = run(SyntheticGatewayDriver(), iterations=1)
        self.assertEqual({"trader_api": 86, "trader_spi": 69}, report["catalog"])
        self.assertGreater(len(report["scenarios"]), 155)
        assert_full_coverage(report)

    def test_missing_catalog_entry_is_rejected(self) -> None:
        report = {"coverage": {"api": [], "spi": []}}
        with self.assertRaises(CoverageError):
            assert_full_coverage(report)
