from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA_PATH = (
    _ROOT
    / "contracts"
    / "evidence"
    / "v1"
    / "schemas"
    / "functional-package-evidence-manifest-v1.schema.json"
)
_EXAMPLE_PATH = (
    _ROOT
    / "contracts"
    / "evidence"
    / "v1"
    / "examples"
    / "functional-package-evidence-manifest.example.json"
)
_F0_EVIDENCE_PATH = (
    _ROOT / "artifacts" / "acceptance" / "f0" / "2026-09-09-f0-quality-baseline.evidence.json"
)
_F1_EVIDENCE_PATH = (
    _ROOT / "artifacts" / "acceptance" / "f1" / "2026-09-09-f1-recovery-implementation.blocked.json"
)
_SENSITIVE_SCAN = _ROOT / "tools" / "quality" / "check_sensitive_content.py"


class FunctionalPackageEvidenceContractTests(unittest.TestCase):
    def test_example_satisfies_the_versioned_evidence_contract(self) -> None:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        errors = list(
            Draft202012Validator(schema).iter_errors(
                json.loads(_EXAMPLE_PATH.read_text(encoding="utf-8"))
            )
        )
        self.assertEqual([], errors)

    def test_evidence_contract_rejects_raw_host_like_reference(self) -> None:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
        instance = json.loads(_EXAMPLE_PATH.read_text(encoding="utf-8"))
        instance["evidence_references"][0]["reference"] = "https://host.example/private-report"
        errors = list(Draft202012Validator(schema).iter_errors(instance))
        self.assertTrue(errors)

    def test_versioned_f0_evidence_satisfies_the_contract(self) -> None:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
        evidence = json.loads(_F0_EVIDENCE_PATH.read_text(encoding="utf-8"))
        errors = list(Draft202012Validator(schema).iter_errors(evidence))
        self.assertEqual([], errors)
        self.assertEqual("F0", evidence["package_id"])
        self.assertEqual("passed", evidence["acceptance_status"])

    def test_versioned_f1_blocked_evidence_satisfies_the_contract(self) -> None:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
        evidence = json.loads(_F1_EVIDENCE_PATH.read_text(encoding="utf-8"))
        errors = list(Draft202012Validator(schema).iter_errors(evidence))
        self.assertEqual([], errors)
        self.assertEqual("F1", evidence["package_id"])
        self.assertEqual("blocked", evidence["acceptance_status"])

    def test_sensitive_content_scanner_reports_only_location_and_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sample = root / "sample.toml"
            sample.write_text('password = "' + "not-for-git-" + "123456" + '"\n', encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(_SENSITIVE_SCAN),
                    "--root",
                    str(root),
                    "--paths",
                    "sample.toml",
                ],
                capture_output=True,
                encoding="utf-8",
                check=False,
            )
        self.assertEqual(1, completed.returncode)
        self.assertIn("SCS002:sample.toml:1", completed.stderr)
        self.assertNotIn("not-for-git-123456", completed.stderr)
