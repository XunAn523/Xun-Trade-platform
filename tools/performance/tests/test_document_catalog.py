from __future__ import annotations

import re
import unittest
from pathlib import Path

from perfbench.catalog import TRADER_API, TRADER_SPI

ROOT = Path(__file__).resolve().parents[3]
MATRIX = ROOT / "docs" / "02-接口与数据" / "02-EMT交易与账户接口全覆盖矩阵.md"
API_PERFORMANCE = (
    ROOT / "docs" / "04-测试验收与性能" / "03-EMT接口性能" / "交易接口性能" / "README.md"
)
SPI_PERFORMANCE = (
    ROOT / "docs" / "04-测试验收与性能" / "03-EMT接口性能" / "回调接口性能" / "README.md"
)


class DocumentCatalogTests(unittest.TestCase):
    def test_catalog_matches_the_authoritative_matrix(self) -> None:
        matrix = MATRIX.read_text(encoding="utf-8")
        api_block = re.search(r"## 2\..*?(?=## 3\.)", matrix, re.DOTALL)
        spi_block = re.search(r"## 3\..*?(?=## 4\.)", matrix, re.DOTALL)
        self.assertIsNotNone(api_block)
        self.assertIsNotNone(spi_block)
        api_names = set(
            re.findall(r"^\|\s*`([A-Za-z][A-Za-z0-9_]*)`", api_block.group(0), re.MULTILINE)
        )
        api_names.discard("OnHoldingChangeEvent")  # explanatory row, not an API method
        spi_names = set(re.findall(r"`(On[A-Za-z0-9_]+)`", spi_block.group(0)))
        self.assertEqual(set(TRADER_API), api_names)
        self.assertEqual(set(TRADER_SPI), spi_names)

    def test_every_catalog_entry_is_documented_in_its_performance_contract(self) -> None:
        api_contract = API_PERFORMANCE.read_text(encoding="utf-8")
        spi_contract = SPI_PERFORMANCE.read_text(encoding="utf-8")
        missing_api = [name for name in TRADER_API if f"`{name}`" not in api_contract]
        missing_spi = [name for name in TRADER_SPI if f"`{name}`" not in spi_contract]
        self.assertEqual([], missing_api)
        self.assertEqual([], missing_spi)
