from __future__ import annotations

import copy
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tools.performance import consumer_uds_replay_probe
from xuanling_tradecore.consumer.replay_evidence_v2 import build_replay_partition_evidence_v2
from xuanling_tradecore.domain import ConsumerEventPositionV2, RedisStreamIdV1


def plan():
    ends = [ConsumerEventPositionV2(RedisStreamIdV1(n, 0), 0).to_external_id() for n in (8, 100)]
    return {
        "journal_id": "test-journal",
        "stream_generation": 1,
        "event_count": 5,
        "high_watermark_position": ends[1],
        "consumer_account_id": "private-account",
        "partitions": [
            {
                "partition_id": 0,
                "after_position": None,
                "high_watermark_position": ends[0],
                "event_count": 2,
                "cursor": "private-token-0",
            },
            {
                "partition_id": 1,
                "after_position": ends[0],
                "high_watermark_position": ends[1],
                "event_count": 3,
                "cursor": "private-token-1",
            },
        ],
    }


class ReplayEvidenceTests(unittest.TestCase):
    def test_contract_only_probe_exports_plan_without_poll_or_checkpoint(self):
        config = SimpleNamespace(
            venue_id="NO_ADAPTER_BENCHMARK",
            adapter_instance_id="disabled-no-dispatch",
            socket_path=Path.cwd() / "unused.sock",
            consumer_account_id="private-account",
        )
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "evidence.json"
            args = [
                "probe",
                "--config",
                "unused.json",
                "--partitions",
                "2",
                "--expected-events",
                "5",
                "--contract-only",
                "--output",
                str(output),
            ]
            with (
                patch("sys.argv", args),
                patch.object(
                    consumer_uds_replay_probe,
                    "load_consumer_uds_server_config_v2",
                    return_value=config,
                ),
                patch.object(consumer_uds_replay_probe, "ConsumerUnixClientV2") as factory,
                redirect_stdout(StringIO()),
            ):
                factory.return_value.call.return_value = {"status": "COMPLETED", "payload": plan()}
                consumer_uds_replay_probe.main()
                factory.return_value.call.assert_called_once()
                self.assertEqual(
                    "CreateReplayPartitions",
                    factory.return_value.call.call_args.args[0]["operation"],
                )
            saved = json.loads(output.read_text())
            self.assertFalse(saved["checkpoint_evidence"])
            self.assertTrue(saved["plan_evidence"]["bounds_valid"])
            self.assertNotIn("private-token", output.read_text())

    def test_canonical_fields_begin_and_non_contiguous_positions(self):
        original = plan()
        before = copy.deepcopy(original)
        result = build_replay_partition_evidence_v2(original)
        self.assertEqual(before, original)
        self.assertTrue(result["bounds_valid"])
        self.assertFalse(result["checkpoint_evidence"])
        self.assertFalse(result["cursor_signature_verified"])
        self.assertTrue(result["partitions"][0]["after_position_is_begin"])
        self.assertEqual(
            result["partitions"][0]["high_watermark_position"],
            result["partitions"][1]["after_position"],
        )
        text = json.dumps(result)
        self.assertNotIn("private-token", text)
        self.assertNotIn("private-account", text)

    def test_missing_is_not_null_and_aliases_cannot_replace_contract(self):
        for key in ("after_position", "high_watermark_position", "event_count", "cursor"):
            with self.subTest(key=key):
                value = plan()
                part = value["partitions"][0]
                part.pop(key)
                part.update(start_position=None, end_position=None)
                with self.assertRaisesRegex(ValueError, "missing"):
                    build_replay_partition_evidence_v2(value)

    def test_invalid_bounds_counts_and_tokens_fail_closed(self):
        changes = [
            ("after_position", None),
            ("high_watermark_position", None),
            ("high_watermark_position", "not-a-position"),
            ("event_count", 0),
            ("event_count", True),
            ("event_count", 4),
            ("cursor", ""),
            ("cursor", "private-token-0"),
            ("partition_id", True),
        ]
        for key, replacement in changes:
            with self.subTest(key=key, replacement=replacement):
                value = plan()
                value["partitions"][1][key] = replacement
                with self.assertRaises(ValueError):
                    build_replay_partition_evidence_v2(value)
        value = plan()
        value["partitions"][1]["high_watermark_position"] = value["partitions"][0][
            "high_watermark_position"
        ]
        with self.assertRaisesRegex(ValueError, "strictly increase"):
            build_replay_partition_evidence_v2(value)

    def test_empty_plan_is_valid_not_performance_acceptance(self):
        value = plan()
        value.update(partitions=[], event_count=0, high_watermark_position=None)
        result = build_replay_partition_evidence_v2(value)
        self.assertEqual([], result["partitions"])
        self.assertFalse(result["checkpoint_evidence"])
        value["event_count"] = 1
        with self.assertRaises(ValueError):
            build_replay_partition_evidence_v2(value)

    def test_suffix_begins_at_exact_durable_predecessor_not_begin(self):
        value = plan()
        after = ConsumerEventPositionV2(RedisStreamIdV1(2, 0), 0).to_external_id()
        value["partitions"][0]["after_position"] = after
        with self.assertRaises(ValueError):
            build_replay_partition_evidence_v2(value)
        result = build_replay_partition_evidence_v2(value, expected_after_position=after)
        self.assertEqual(after, result["interval_after_position"])
        self.assertEqual(after, result["partitions"][0]["after_position"])
        self.assertFalse(result["partitions"][0]["after_position_is_begin"])
        self.assertFalse(result["checkpoint_evidence"])

    def test_empty_suffix_does_not_invent_a_new_high_watermark(self):
        value = plan()
        value.update(partitions=[], event_count=0, high_watermark_position=None)
        after = ConsumerEventPositionV2(RedisStreamIdV1(2, 0), 0).to_external_id()
        result = build_replay_partition_evidence_v2(value, expected_after_position=after)
        self.assertIsNone(result["high_watermark_position"])
        self.assertEqual(after, result["interval_after_position"])

    def test_top_level_identity_and_watermark_are_required(self):
        for key in ("journal_id", "stream_generation", "high_watermark_position"):
            value = plan()
            value.pop(key)
            with self.assertRaises(ValueError):
                build_replay_partition_evidence_v2(value)
        for key, replacement in (
            ("stream_generation", True),
            ("event_count", -1),
            ("journal_id", ""),
            ("high_watermark_position", None),
        ):
            value = plan()
            value[key] = replacement
            with self.assertRaises(ValueError):
                build_replay_partition_evidence_v2(value)
