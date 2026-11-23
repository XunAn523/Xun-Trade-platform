from __future__ import annotations

import ast
import hashlib
import json
import unittest
from pathlib import Path

import msgspec
from jsonschema import Draft202012Validator
from xuanling_tradecore.codec import (
    CODEC_SCHEMA_VERSION,
    CodecBoundaryError,
    DomainEventEnvelope,
    ExternalAdapterReplayPayload,
    FixtureOrderConfig,
    FixturePriceBand,
    PaperBrokerReplayPayload,
    decode_domain_event_json,
    decode_external_adapter_replay_messagepack,
    decode_fixture_config_json,
    decode_paper_replay_messagepack,
    encode_domain_event_json,
    encode_external_adapter_replay_messagepack,
    encode_fixture_config_json,
    encode_paper_replay_messagepack,
    run_msgspec_codec_benchmark,
)

ROOT = Path(__file__).resolve().parents[1]


def _event() -> DomainEventEnvelope:
    return DomainEventEnvelope(
        schema_version=CODEC_SCHEMA_VERSION,
        event_id="event-1",
        event_kind="ORDER",
        account_id="account-1",
        trading_day="2026-08-31",
        journal_id="journal-1",
        journal_lsn=1,
        execution_id="execution-1",
        source_key="order:1001:working",
    )


def _replay() -> ExternalAdapterReplayPayload:
    return ExternalAdapterReplayPayload(
        schema_version=CODEC_SCHEMA_VERSION,
        replay_id="replay-1",
        replay_sequence=1,
        event_kind="TRADE",
        account_id="account-1",
        trading_day="2026-08-31",
        symbol="000010",
        side="BUY",
        price_ticks=161,
        quantity=100,
        source_key="external:trade:1",
    )


def _fixture() -> FixtureOrderConfig:
    return FixtureOrderConfig(
        schema_version=CODEC_SCHEMA_VERSION,
        fixture_id="codec-fixture-1",
        environment="external-adapter-fixture",
        armed=False,
        symbol="000010",
        market="SZ",
        side="BUY",
        quantity=100,
        price_band=FixturePriceBand(157, 163, 1),
    )


class TradeCoreCodecTests(unittest.TestCase):
    def test_typed_json_and_messagepack_boundaries_round_trip_deterministically(self) -> None:
        event = _event()
        fixture = _fixture()
        replay = _replay()
        self.assertEqual(event, decode_domain_event_json(encode_domain_event_json(event)))
        self.assertEqual(fixture, decode_fixture_config_json(encode_fixture_config_json(fixture)))
        self.assertEqual(
            replay,
            decode_external_adapter_replay_messagepack(
                encode_external_adapter_replay_messagepack(replay)
            ),
        )
        self.assertEqual(encode_domain_event_json(event), encode_domain_event_json(event))
        self.assertEqual(encode_fixture_config_json(fixture), encode_fixture_config_json(fixture))
        self.assertEqual(
            encode_external_adapter_replay_messagepack(replay),
            encode_external_adapter_replay_messagepack(replay),
        )
        self.assertIs(PaperBrokerReplayPayload, ExternalAdapterReplayPayload)
        self.assertEqual(
            encode_paper_replay_messagepack(replay),
            encode_external_adapter_replay_messagepack(replay),
        )
        self.assertEqual(
            replay,
            decode_paper_replay_messagepack(encode_paper_replay_messagepack(replay)),
        )

    def test_unknown_wrong_version_and_armed_payloads_fail_closed(self) -> None:
        raw_event = json.loads(encode_domain_event_json(_event()))
        raw_event["unapproved"] = "value"
        with self.assertRaises(CodecBoundaryError):
            decode_domain_event_json(json.dumps(raw_event).encode("utf-8"))

        raw_event = json.loads(encode_domain_event_json(_event()))
        raw_event["schema_version"] = 2
        with self.assertRaisesRegex(CodecBoundaryError, "schema_version"):
            decode_domain_event_json(json.dumps(raw_event).encode("utf-8"))

        raw_fixture = json.loads(encode_fixture_config_json(_fixture()))
        raw_fixture["armed"] = True
        with self.assertRaisesRegex(CodecBoundaryError, "disarmed"):
            decode_fixture_config_json(json.dumps(raw_fixture).encode("utf-8"))

    def test_fixed_point_schema_rejects_float_and_invalid_price_band(self) -> None:
        raw_replay = msgspec.msgpack.decode(encode_external_adapter_replay_messagepack(_replay()))
        raw_replay["price_ticks"] = 1.61
        with self.assertRaises(CodecBoundaryError):
            decode_external_adapter_replay_messagepack(msgspec.msgpack.encode(raw_replay))

        raw_fixture = json.loads(encode_fixture_config_json(_fixture()))
        raw_fixture["price_band"]["minimum_price_ticks"] = 164
        with self.assertRaisesRegex(CodecBoundaryError, "maximum precedes"):
            decode_fixture_config_json(json.dumps(raw_fixture).encode("utf-8"))

    def test_small_benchmark_reports_only_measurement_metadata(self) -> None:
        report = run_msgspec_codec_benchmark(iterations=24, warmup=2, rounds=4)
        self.assertEqual("msgspec", report.library)
        self.assertEqual(3, len(report.measurements))
        self.assertTrue(all(item.encoded_bytes > 0 for item in report.measurements))
        self.assertTrue(all(item.msgspec_encode_mean_ns > 0 for item in report.measurements))
        self.assertNotIn("account-benchmark", json.dumps(report.as_dict()))

    def test_frozen_schemas_golden_payloads_and_asset_hashes_match_runtime(self) -> None:
        contract_root = ROOT / "contracts" / "codec" / "v1"
        manifest = json.loads((contract_root / "asset-manifest.json").read_text(encoding="utf-8"))
        for relative_path, expected_hash in manifest["assets"].items():
            content = (contract_root / relative_path).read_bytes()
            # JSON contracts are versioned as LF text in `.gitattributes`.
            # Normalize a Windows checkout before hashing so the manifest
            # asserts the repository's canonical artifact, not local EOLs.
            if relative_path.endswith(".json"):
                content = content.replace(b"\r\n", b"\n")
            self.assertEqual(expected_hash, hashlib.sha256(content).hexdigest())

        schemas = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in (contract_root / "schemas").glob("*.json")
        }
        for schema in schemas.values():
            Draft202012Validator.check_schema(schema)
        domain_event = json.loads(
            (contract_root / "golden" / "domain-event.json").read_text(encoding="utf-8")
        )
        fixture = json.loads(
            (contract_root / "golden" / "fixture-config.json").read_text(encoding="utf-8")
        )
        replay = msgspec.msgpack.decode(
            (contract_root / "golden" / "external-adapter-replay.msgpack").read_bytes()
        )
        Draft202012Validator(schemas["domain-event.schema.json"]).validate(domain_event)
        Draft202012Validator(schemas["fixture-config.schema.json"]).validate(fixture)
        Draft202012Validator(schemas["external-adapter-replay.schema.json"]).validate(replay)
        decode_domain_event_json(json.dumps(domain_event, separators=(",", ":")).encode())
        decode_fixture_config_json(json.dumps(fixture, separators=(",", ":")).encode())
        decode_external_adapter_replay_messagepack(msgspec.msgpack.encode(replay))
        self.assertNotIn("matching_engine", json.dumps(schemas))

    def test_msgspec_is_confined_to_typed_boundary_package_and_not_callback_ingress(self) -> None:
        codec_sources = tuple((ROOT / "xuanling_tradecore" / "codec").rglob("*.py"))
        self.assertTrue(codec_sources)
        self.assertTrue(
            any(
                any(
                    alias.name == "msgspec"
                    for node in ast.walk(ast.parse(source.read_text(encoding="utf-8")))
                    if isinstance(node, ast.Import)
                    for alias in node.names
                )
                for source in codec_sources
            )
        )
        for source in (ROOT / "emt_gateway").rglob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported = tuple(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module is not None:
                    imported = (node.module,)
                else:
                    continue
                self.assertNotIn("msgspec", tuple(name.split(".", 1)[0] for name in imported))


if __name__ == "__main__":
    unittest.main()
