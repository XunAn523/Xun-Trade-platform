from __future__ import annotations

import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator
from xuanling_tradecore.runtime import (
    AcceptanceEvidenceErrorV1,
    AcceptanceEvidenceManifestV1,
    AcceptanceEvidenceStatusV1,
    EvidenceCleanupStateV1,
    TraceContextV1,
)


class AcceptanceEvidenceV1Tests(unittest.TestCase):
    def _trace(self) -> TraceContextV1:
        return TraceContextV1(
            trace_id="trace-1",
            execution_id="execution-1",
            binding_id="binding-1",
            binding_generation=3,
            adapter_id="EMT",
            scope_id_hash="a" * 64,
            event_cursor="ce2_AAAAAAAAAAAAAAAAAAAAAAAAAAA",
            stage="C1",
            monotonic_ns=123,
            result_code="ACCEPTED",
        )

    def test_trace_matches_the_versioned_redacted_contract(self) -> None:
        root = Path(__file__).resolve().parents[1]
        schema_path = root / "contracts" / "consumer" / "v2" / "schemas" / "trace.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(self._trace().as_dict())

    def test_manifest_rejects_sensitive_metadata_and_inconsistent_counts(self) -> None:
        arguments = {
            "stage": "S0",
            "status": AcceptanceEvidenceStatusV1.ACCEPTED,
            "source_commit": "b" * 40,
            "profile_digest": "c" * 64,
            "environment_fingerprint": "d" * 64,
            "started_wall_ns": 1,
            "finished_wall_ns": 2,
            "test_command": "python -m pytest tests/test_acceptance_evidence_v1.py -q",
            "total_cases": 5,
            "failed_cases": 0,
            "trace_pair_count": 0,
            "trace_gap_count": 0,
            "cleanup_state": EvidenceCleanupStateV1.NOT_APPLICABLE,
        }
        manifest = AcceptanceEvidenceManifestV1(**arguments, metadata={"runner": "local"})
        self.assertTrue(manifest.passed)
        self.assertNotIn("password", repr(manifest))
        self.assertEqual("local", manifest.as_dict()["metadata"]["runner"])
        with self.assertRaises(AcceptanceEvidenceErrorV1):
            AcceptanceEvidenceManifestV1(**arguments, metadata={"password": "synthetic-do-not-store"})
        with self.assertRaises(AcceptanceEvidenceErrorV1):
            AcceptanceEvidenceManifestV1(**{**arguments, "failed_cases": 6})


if __name__ == "__main__":
    unittest.main()
