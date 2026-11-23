"""Tests for the fail-closed V1/V2 fixed-record coverage matrix."""

from __future__ import annotations

import unittest

from emt_gateway.fastspi_v2_equivalence import (
    FastSpiV2CallbackField,
    FastSpiV2EquivalenceMatrix,
    FastSpiV2EquivalenceRequirement,
    FastSpiV2EquivalenceSurface,
    legacy_emt_fastspi_v2_equivalence_matrix,
)
from emt_gateway.spi_bridge import OFFICIAL_TRADER_SPI


class FastSpiV2EquivalenceTests(unittest.TestCase):
    def test_extended_layout_proves_all_declared_scalar_surfaces(self) -> None:
        matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        self.assertTrue(matrix.runtime_equivalent)
        order = next(
            requirement
            for requirement in matrix.requirements_for("OnOrderEvent")
            if requirement.surface is FastSpiV2EquivalenceSurface.ORDER_LEDGER_FACT
        )
        self.assertEqual((), order.missing_fields)
        matrix.assert_runtime_equivalent()

    def test_trade_and_paged_query_surfaces_are_present_and_deterministic(self) -> None:
        matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        trade = next(
            requirement
            for requirement in matrix.requirements_for("OnTradeEvent")
            if requirement.surface is FastSpiV2EquivalenceSurface.TRADE_LEDGER_FACT
        )
        self.assertEqual((), trade.missing_fields)
        paged = next(
            requirement
            for requirement in matrix.requirements_for("OnQueryOrderByPage")
            if requirement.surface is FastSpiV2EquivalenceSurface.QUERY_CORRELATION
        )
        self.assertEqual((), paged.missing_fields)
        self.assertEqual(
            matrix.certificate_sha256, legacy_emt_fastspi_v2_equivalence_matrix().certificate_sha256
        )

    def test_matrix_rejects_any_missing_or_unknown_official_callback(self) -> None:
        base = legacy_emt_fastspi_v2_equivalence_matrix()
        with self.assertRaisesRegex(ValueError, "exactly the official callback set"):
            FastSpiV2EquivalenceMatrix(
                requirements=tuple(
                    requirement
                    for requirement in base.requirements
                    if requirement.callback_name != OFFICIAL_TRADER_SPI[-1]
                )
            )
        unknown = FastSpiV2EquivalenceRequirement(
            callback_name="OnUnknown",
            surface=FastSpiV2EquivalenceSurface.CALLBACK_DELIVERY,
            required_fields=frozenset({FastSpiV2CallbackField.CALLBACK_KIND}),
            available_fields=frozenset({FastSpiV2CallbackField.CALLBACK_KIND}),
        )
        with self.assertRaisesRegex(ValueError, "exactly the official callback set"):
            FastSpiV2EquivalenceMatrix(requirements=base.requirements + (unknown,))

    def test_every_official_callback_retains_a_generic_delivery_obligation(self) -> None:
        matrix = legacy_emt_fastspi_v2_equivalence_matrix()
        self.assertEqual(
            set(OFFICIAL_TRADER_SPI),
            {
                requirement.callback_name
                for requirement in matrix.requirements
                if requirement.surface is FastSpiV2EquivalenceSurface.CALLBACK_DELIVERY
            },
        )


if __name__ == "__main__":
    unittest.main()
