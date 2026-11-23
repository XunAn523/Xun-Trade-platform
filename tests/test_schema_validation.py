from __future__ import annotations

import json
import unittest
from pathlib import Path

from emt_gateway.schema_validation import (
    SchemaContractError,
    schema_names,
    schema_validator,
    validate_schema_instance,
)


class SchemaValidationTests(unittest.TestCase):
    def test_bundled_schemas_are_draft_2020_12_contracts(self) -> None:
        self.assertEqual(
            (
                "controlled-order-fixture-v1",
                "mutation-fixture-v1",
                "performance-report-v1",
            ),
            schema_names(),
        )
        for name in schema_names():
            with self.subTest(name=name):
                self.assertEqual(
                    "https://json-schema.org/draft/2020-12/schema",
                    schema_validator(name).schema["$schema"],
                )

    def test_controlled_fixture_example_satisfies_bundled_schema(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "tools"
            / "performance"
            / "fixtures"
            / "emt-controlled-order-fixture.example.json"
        )
        validate_schema_instance(
            "controlled-order-fixture-v1", json.loads(path.read_text(encoding="utf-8"))
        )

    def test_schema_failure_has_a_path_but_does_not_echo_payload(self) -> None:
        boundaries = {
            name: {"observed": True, "value": False}
            for name in (
                "sdk_loaded",
                "login",
                "network",
                "real_order_entry",
                "native_fastspi_required",
                "native_fastspi_available",
            )
        }
        with self.assertRaisesRegex(SchemaContractError, "report_contract.schema_version"):
            validate_schema_instance(
                "performance-report-v1",
                {
                    "driver": "synthetic",
                    "report_contract": {
                        "name": "emt-gateway-performance-report",
                        "schema_version": 9,
                        "requested_driver": "synthetic",
                        "boundary_completeness": "complete",
                        "evidence_classification": "non_emt_or_control_artifact",
                        "execution_boundaries": boundaries,
                    },
                },
            )

    def test_unknown_contract_is_rejected(self) -> None:
        with self.assertRaisesRegex(SchemaContractError, "unknown schema"):
            schema_validator("unreviewed-v9")
