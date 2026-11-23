from __future__ import annotations

import json
import shutil
import struct
import tempfile
import unittest
import zlib
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator
from xuanling_tradecore.consumer import (
    HEADER_SIZE_BYTES,
    ConsumerAccountOrderSnapshotV2,
    ConsumerEventPageV2,
    ConsumerSchemaNegotiationError,
    ConsumerVersionRange,
    ConsumerWireError,
    ConsumerWireErrorCode,
    MessageKind,
    PositionedConsumerEventV2,
    decode_frame,
    encode_frame,
    load_contract_identity,
    negotiate_consumer_versions,
    validate_payload,
)
from xuanling_tradecore.domain import (
    ConsumerAccountRiskEvent,
    ConsumerApiVersion,
    ConsumerEventCursorV2,
    ConsumerEventKind,
    ConsumerEventPositionV2,
    ConsumerTrace,
    DomainValidationError,
    ErrorCategory,
    OrderOrigin,
    RedisStreamIdV1,
    TradingDayEpoch,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = ROOT / "contracts" / "consumer" / "v2"


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"fixture is not an object: {path}")
    return cast(dict[str, Any], value)


def _event(position: ConsumerEventPositionV2) -> PositionedConsumerEventV2:
    trace = ConsumerTrace(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        account_id="account-1",
        strategy_id="strategy-1",
        strategy_order_id="strategy-order-1",
        attribution_binding_id="attribution-1",
        binding_version=1,
        order_origin=OrderOrigin.STRATEGY,
    )
    return PositionedConsumerEventV2(
        position=position,
        event=ConsumerAccountRiskEvent(
            consumer_event_id=f"event-{position.to_external_id()}",
            event_kind=ConsumerEventKind.ACCOUNT_RISK,
            schema_version=2,
            account_id="account-1",
            trading_day=TradingDayEpoch("2026-09-01"),
            journal_id="journal-1",
            journal_lsn=position.stream_id.redis_ms,
            consumer_trace=trace,
            risk_snapshot_id=None,
            error_category=ErrorCategory.RISK,
        ),
    )


def _cursor(position: ConsumerEventPositionV2) -> ConsumerEventCursorV2:
    return ConsumerEventCursorV2(
        consumer_id="consumer-1",
        consumer_account_id="consumer-account-1",
        journal_id="journal-1",
        stream_generation=1,
        after_position=position,
        authorization_epoch=1,
        issued_at_wall_ns=1,
        expires_at_wall_ns=2,
        cursor_key_id="cursor-key-1",
    )


class ConsumerWireV2ContractTests(unittest.TestCase):
    def test_all_schemas_and_positive_fixtures_validate(self) -> None:
        schemas = {path.name: _json(path) for path in (CONTRACT_ROOT / "schemas").glob("*.json")}
        for schema in schemas.values():
            Draft202012Validator.check_schema(schema)
        for fixture_path in (CONTRACT_ROOT / "fixtures" / "positive").glob("*.json"):
            kind = fixture_path.name.rsplit(".", maxsplit=2)[-2]
            validate_payload(_json(fixture_path), schemas[f"{kind}.schema.json"])

    def test_all_negative_fixtures_fail_closed(self) -> None:
        for fixture_path in (CONTRACT_ROOT / "fixtures" / "negative").glob("*.json"):
            fixture = _json(fixture_path)
            schema = _json(CONTRACT_ROOT / "schemas" / fixture["schema"])
            with self.assertRaises(ConsumerWireError) as raised:
                validate_payload(fixture["payload"], schema)
            self.assertEqual(fixture["expected_error"], raised.exception.code)

    def test_reused_validator_preserves_negative_fixture_error_codes(self) -> None:
        for fixture_path in (CONTRACT_ROOT / "fixtures" / "negative").glob("*.json"):
            fixture = _json(fixture_path)
            schema = _json(CONTRACT_ROOT / "schemas" / fixture["schema"])
            validator = Draft202012Validator(schema)
            with self.subTest(fixture=fixture_path.name):
                with self.assertRaises(ConsumerWireError) as raised:
                    validate_payload(fixture["payload"], schema, compiled_validator=validator)
                self.assertEqual(fixture["expected_error"], raised.exception.code)

    def test_golden_request_frame_is_exact_and_round_trips(self) -> None:
        payload = _json(CONTRACT_ROOT / "fixtures" / "positive" / "submit-signal.request.json")
        encoded = encode_frame(MessageKind.REQUEST, 0x0102030405060708, payload)
        golden = (CONTRACT_ROOT / "golden" / "submit-signal.request.bin").read_bytes()
        self.assertEqual(golden, encoded)
        decoded = decode_frame(golden)
        self.assertEqual(MessageKind.REQUEST, decoded.header.message_kind)
        self.assertEqual(0x0102030405060708, decoded.header.correlation_id)
        self.assertEqual(payload, decoded.payload)

    def test_decoder_keeps_immutable_frame_boundary(self) -> None:
        frame = encode_frame(MessageKind.REQUEST, 1, {"message_type": "request"})
        for mutable_or_view in (bytearray(frame), memoryview(frame)):
            with self.subTest(frame_type=type(mutable_or_view).__name__):
                with self.assertRaises(ConsumerWireError) as raised:
                    decode_frame(mutable_or_view)
                self.assertEqual(ConsumerWireErrorCode.FRAME_INCOMPLETE, raised.exception.code)

    def test_header_corruption_truncation_crc_and_trailing_bytes_are_rejected(self) -> None:
        frame = encode_frame(MessageKind.REQUEST, 1, {"message_type": "request"})
        cases = (
            (frame[: HEADER_SIZE_BYTES - 1], ConsumerWireErrorCode.FRAME_INCOMPLETE),
            (b"BAD!" + frame[4:], ConsumerWireErrorCode.BAD_MAGIC),
            (frame[:7] + b"\x01" + frame[8:], ConsumerWireErrorCode.INVALID_FLAGS),
            (frame[:-1], ConsumerWireErrorCode.FRAME_INCOMPLETE),
            (frame + b"x", ConsumerWireErrorCode.INVALID_JSON),
            (frame[:-1] + bytes([frame[-1] ^ 1]), ConsumerWireErrorCode.PAYLOAD_CRC_MISMATCH),
        )
        for damaged, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(ConsumerWireError) as raised:
                    decode_frame(damaged)
                self.assertEqual(code, raised.exception.code)

    def test_invalid_utf8_remains_distinct_from_invalid_json(self) -> None:
        invalid_utf8 = bytearray(encode_frame(MessageKind.REQUEST, 1, {}))
        invalid_utf8[-2:] = b"\xff}"
        invalid_utf8[28:32] = struct.pack(">I", zlib.crc32(invalid_utf8[HEADER_SIZE_BYTES:]))
        with self.assertRaises(ConsumerWireError) as raised:
            decode_frame(bytes(invalid_utf8))
        self.assertEqual(ConsumerWireErrorCode.INVALID_UTF8, raised.exception.code)

        invalid_json = bytearray(encode_frame(MessageKind.REQUEST, 1, {}))
        invalid_json[-2:] = b",}"
        invalid_json[28:32] = struct.pack(">I", zlib.crc32(invalid_json[HEADER_SIZE_BYTES:]))
        with self.assertRaises(ConsumerWireError) as raised:
            decode_frame(bytes(invalid_json))
        self.assertEqual(ConsumerWireErrorCode.INVALID_JSON, raised.exception.code)

    def test_unknown_message_kind_and_reserved_header_field_are_rejected(self) -> None:
        frame = bytearray(encode_frame(MessageKind.REQUEST, 1, {}))
        frame[5] = 99
        with self.assertRaises(ConsumerWireError) as raised:
            decode_frame(bytes(frame))
        self.assertEqual(ConsumerWireErrorCode.UNSUPPORTED_HEADER_VERSION, raised.exception.code)

        frame = bytearray(encode_frame(MessageKind.REQUEST, 1, {}))
        frame[14:16] = struct.pack(">H", 1)
        with self.assertRaises(ConsumerWireError) as raised:
            decode_frame(bytes(frame))
        self.assertEqual(ConsumerWireErrorCode.UNSUPPORTED_HEADER_VERSION, raised.exception.code)

    def test_error_catalog_is_unique_and_covers_runtime_codes(self) -> None:
        catalog = _json(CONTRACT_ROOT / "error-codes.json")
        entries = cast(list[dict[str, object]], catalog["codes"])
        codes = tuple(cast(str, entry["code"]) for entry in entries)
        self.assertEqual(len(codes), len(set(codes)))
        runtime_codes = {
            value
            for name, value in vars(ConsumerWireErrorCode).items()
            if name.isupper() and isinstance(value, str)
        }
        self.assertTrue(runtime_codes.issubset(codes))
        self.assertTrue(all(entry["advances_cursor"] is False for entry in entries))

    def test_declared_contract_hashes_detect_asset_drift(self) -> None:
        identity = load_contract_identity(CONTRACT_ROOT)
        self.assertEqual(2, identity.api_version.major)
        self.assertEqual(HEADER_SIZE_BYTES, identity.header_size_bytes)
        with tempfile.TemporaryDirectory() as temporary_directory:
            copy = Path(temporary_directory) / "v2"
            shutil.copytree(CONTRACT_ROOT, copy)
            schema = copy / "schemas" / "request.schema.json"
            schema.write_bytes(schema.read_bytes() + b" ")
            with self.assertRaises(DomainValidationError):
                load_contract_identity(copy)

    def test_declared_contract_hashes_are_stable_across_text_line_endings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            copy = Path(temporary_directory) / "v2"
            shutil.copytree(CONTRACT_ROOT, copy)
            for path in (
                *tuple((copy / "schemas").glob("*.json")),
                copy / "compatibility-matrix.json",
            ):
                path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
            identity = load_contract_identity(copy)
        self.assertEqual(2, identity.api_version.major)

    def test_api_and_event_schema_negotiation_selects_highest_common_or_rejects(self) -> None:
        offered = ConsumerVersionRange(
            minimum_api_version=ConsumerApiVersion(2, 0),
            maximum_api_version=ConsumerApiVersion(2, 2),
            minimum_event_schema_version=2,
            maximum_event_schema_version=3,
        )
        api_version, event_version = negotiate_consumer_versions(
            offered,
            (ConsumerApiVersion(1, 9), ConsumerApiVersion(2, 0), ConsumerApiVersion(2, 1)),
            (1, 2),
        )
        self.assertEqual(ConsumerApiVersion(2, 1), api_version)
        self.assertEqual(2, event_version)
        incompatible = ConsumerVersionRange(
            minimum_api_version=ConsumerApiVersion(3, 0),
            maximum_api_version=ConsumerApiVersion(3, 0),
            minimum_event_schema_version=3,
            maximum_event_schema_version=3,
        )
        with self.assertRaises(ConsumerSchemaNegotiationError) as raised:
            negotiate_consumer_versions(
                incompatible,
                (ConsumerApiVersion(2, 0),),
                (2,),
            )
        self.assertEqual("UPGRADE_REQUIRED", raised.exception.code)

    def test_v2_event_page_and_snapshot_use_numeric_positions(self) -> None:
        first = ConsumerEventPositionV2(RedisStreamIdV1(9, 10), 0)
        second = ConsumerEventPositionV2(RedisStreamIdV1(10, 0), 0)
        cursor = _cursor(second)
        page = ConsumerEventPageV2(
            items=(_event(first), _event(second)),
            high_watermark_position=second,
            has_more=False,
            next_cursor=cursor,
        )
        self.assertEqual((first, second), tuple(item.position for item in page.items))
        snapshot = ConsumerAccountOrderSnapshotV2(
            consumer_account_id="consumer-account-1",
            snapshot_position=second,
            next_cursor=cursor,
            orders=(_event(second).event,),
        )
        self.assertEqual(second, snapshot.snapshot_position)
        with self.assertRaises(DomainValidationError):
            ConsumerEventPageV2(
                items=(_event(second), _event(first)),
                high_watermark_position=second,
                has_more=False,
                next_cursor=cursor,
            )


if __name__ == "__main__":
    unittest.main()
