from __future__ import annotations

import unittest

from perfbench.catalog import TRADER_API, TRADER_SPI, all_specs


class CatalogTests(unittest.TestCase):
    def test_target_catalog_counts_and_uniqueness(self) -> None:
        self.assertEqual(86, len(TRADER_API))
        self.assertEqual(69, len(TRADER_SPI))
        self.assertEqual(len(TRADER_API), len(set(TRADER_API)))
        self.assertEqual(len(TRADER_SPI), len(set(TRADER_SPI)))

    def test_every_interface_has_a_tier_and_workload(self) -> None:
        specs = all_specs()
        self.assertEqual(155, len(specs))
        self.assertTrue(all(spec.tier and spec.workloads for spec in specs))
