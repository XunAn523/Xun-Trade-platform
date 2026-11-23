from __future__ import annotations

import unittest

from tools.acceptance.f2_capability_contract import run


class F2CapabilityContractTests(unittest.TestCase):
    def test_contract_prepares_documented_credit_shapes_without_native_side_effects(self) -> None:
        report = run()
        self.assertEqual("passed", report["acceptance_status"])
        self.assertTrue(all(report["checks"].values()))
        self.assertEqual(0, report["native_mutation_calls"])
        self.assertFalse(report["native_sdk_loaded"])


if __name__ == "__main__":
    unittest.main()
