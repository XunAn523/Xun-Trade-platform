from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from perfbench.cli import _collect_trace_inputs, _select_trace_inputs


class TraceExclusionTests(unittest.TestCase):
    def test_directory_collection_is_recursive_and_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            direct = root / "direct.traces.jsonl"
            nested = root / "nested" / "nested.traces.jsonl"
            direct.write_text("{}\n", encoding="utf-8")
            nested.parent.mkdir()
            nested.write_text("{}\n", encoding="utf-8")
            collected = _collect_trace_inputs([direct], [root])
        self.assertEqual([direct, nested], collected)

    def test_exclusion_is_persisted_and_keeps_remaining_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid.traces.jsonl"
            invalid = root / "invalid.traces.jsonl"
            selected, provenance = _select_trace_inputs(
                [valid, invalid],
                [invalid],
                "local harness correlation bug; not counter evidence",
            )
        self.assertEqual([valid], selected)
        self.assertEqual(2, provenance["input_batches"])
        self.assertEqual(1, provenance["included_batches"])
        self.assertEqual(["invalid.traces.jsonl"], provenance["excluded_trace_files"])

    def test_exclusion_requires_a_reason_and_a_supplied_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid.traces.jsonl"
            invalid = root / "invalid.traces.jsonl"
            with self.assertRaisesRegex(ValueError, "reason"):
                _select_trace_inputs([valid, invalid], [invalid], "")
            with self.assertRaisesRegex(ValueError, "also be supplied"):
                _select_trace_inputs([valid], [invalid], "reviewed")

    def test_reason_without_an_exclusion_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            valid = Path(directory) / "valid.traces.jsonl"
            with self.assertRaisesRegex(ValueError, "requires at least one"):
                _select_trace_inputs([valid], [], "reviewed")
