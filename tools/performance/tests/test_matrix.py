from __future__ import annotations

import unittest

from perfbench.catalog import TRADER_API, TRADER_SPI
from perfbench.matrix import (
    MatrixError,
    all_scenarios,
    runbook_template,
    scenario_index,
    validate_runbook,
)


class MatrixTests(unittest.TestCase):
    def test_every_catalog_interface_expands_to_a_nonempty_scenario_set(self) -> None:
        scenarios = all_scenarios()
        api = {scenario.interface for scenario in scenarios if scenario.kind == "api"}
        spi = {scenario.interface for scenario in scenarios if scenario.kind == "spi"}
        self.assertEqual(set(TRADER_API), api)
        self.assertEqual(set(TRADER_SPI), spi)
        self.assertEqual(len(scenarios), len(scenario_index()))
        self.assertTrue(all(scenario.required_paths for scenario in scenarios))
        self.assertTrue(all(scenario.p0_minimum_successes > 0 for scenario in scenarios))

    def test_common_order_path_is_measured_for_all_account_profiles(self) -> None:
        profiles = {
            scenario.profile
            for scenario in all_scenarios()
            if scenario.kind == "api" and scenario.interface == "InsertOrder"
        }
        self.assertEqual({"ordinary", "credit", "option"}, profiles)

    def test_template_is_complete_and_rejects_removed_entry(self) -> None:
        manifest = runbook_template()
        validate_runbook(manifest)
        manifest["scenarios"].pop()
        with self.assertRaises(MatrixError):
            validate_runbook(manifest)

    def test_template_rejects_weakened_sample_gate(self) -> None:
        manifest = runbook_template()
        manifest["scenarios"][0]["p0_minimum_successes"] = 1
        with self.assertRaises(MatrixError):
            validate_runbook(manifest)

    def test_template_can_initialize_a_non_sensitive_owner(self) -> None:
        manifest = runbook_template(owner="performance-owner")
        validate_runbook(manifest)
        self.assertTrue(
            all(entry["owner"] == "performance-owner" for entry in manifest["scenarios"])
        )

    def test_callback_paths_include_ring_durable_and_client_ack(self) -> None:
        callback = next(
            scenario for scenario in all_scenarios() if scenario.interface == "OnOrderEvent"
        )
        self.assertEqual(
            ("C0_to_C5", "C5_to_C6", "C6_to_C7", "C0_to_C7"),
            callback.required_path_names,
        )
