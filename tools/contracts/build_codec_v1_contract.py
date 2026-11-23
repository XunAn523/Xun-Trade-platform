"""Build deterministic schemas and golden payloads for the isolated msgspec trial."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import msgspec
from xuanling_tradecore.codec import (
    CODEC_SCHEMA_VERSION,
    DomainEventEnvelope,
    ExternalAdapterReplayPayload,
    FixtureOrderConfig,
    FixturePriceBand,
    encode_domain_event_json,
    encode_external_adapter_replay_messagepack,
    encode_fixture_config_json,
)

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = ROOT / "contracts" / "codec" / "v1"


def _schema(payload_type: type[object]) -> dict[str, object]:
    value = msgspec.json.schema(payload_type)
    value["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    value["x-runtime-semantic-validation"] = (
        "Required after structural decode; see xuanling_tradecore.codec.typed_payloads."
    )
    return value


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    schemas = {
        "domain-event.schema.json": _schema(DomainEventEnvelope),
        "external-adapter-replay.schema.json": _schema(ExternalAdapterReplayPayload),
        "fixture-config.schema.json": _schema(FixtureOrderConfig),
    }
    for name, schema in schemas.items():
        _write_json(CONTRACT_ROOT / "schemas" / name, schema)

    event = DomainEventEnvelope(
        schema_version=CODEC_SCHEMA_VERSION,
        event_id="event-golden-1",
        event_kind="ORDER",
        account_id="account-fixture",
        trading_day="2026-09-01",
        journal_id="journal-fixture",
        journal_lsn=42,
        execution_id="execution-fixture-1",
        source_key="order:fixture:42",
    )
    replay = ExternalAdapterReplayPayload(
        schema_version=CODEC_SCHEMA_VERSION,
        replay_id="replay-golden-1",
        replay_sequence=1,
        event_kind="TRADE",
        account_id="account-fixture",
        trading_day="2026-09-01",
        symbol="000001",
        side="BUY",
        price_ticks=1010,
        quantity=100,
        source_key="external:trade:fixture:1",
    )
    fixture = FixtureOrderConfig(
        schema_version=CODEC_SCHEMA_VERSION,
        fixture_id="codec-fixture-1",
        environment="external-adapter-fixture",
        armed=False,
        symbol="000001",
        market="SZ",
        side="BUY",
        quantity=100,
        price_band=FixturePriceBand(1000, 1020, 1),
    )
    golden_directory = CONTRACT_ROOT / "golden"
    golden_directory.mkdir(parents=True, exist_ok=True)
    (golden_directory / "domain-event.json").write_bytes(encode_domain_event_json(event))
    (golden_directory / "external-adapter-replay.msgpack").write_bytes(
        encode_external_adapter_replay_messagepack(replay)
    )
    (golden_directory / "fixture-config.json").write_bytes(encode_fixture_config_json(fixture))

    files = tuple(
        sorted(
            path
            for path in CONTRACT_ROOT.rglob("*")
            if path.is_file() and path.name != "asset-manifest.json"
        )
    )
    manifest = {
        "schema_version": 1,
        "codec_library": "msgspec",
        "codec_library_version": msgspec.__version__,
        "assets": {
            path.relative_to(CONTRACT_ROOT).as_posix(): sha256(path.read_bytes()).hexdigest()
            for path in files
        },
    }
    _write_json(CONTRACT_ROOT / "asset-manifest.json", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
