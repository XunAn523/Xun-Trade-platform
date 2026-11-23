"""Typed, non-hot-path serialization boundaries used by the M0 msgspec trial."""

from .benchmark import CodecBenchmarkMeasurement, CodecBenchmarkReport, run_msgspec_codec_benchmark
from .typed_payloads import (
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
)

__all__ = [
    "CODEC_SCHEMA_VERSION",
    "CodecBenchmarkMeasurement",
    "CodecBenchmarkReport",
    "CodecBoundaryError",
    "DomainEventEnvelope",
    "ExternalAdapterReplayPayload",
    "FixtureOrderConfig",
    "FixturePriceBand",
    "PaperBrokerReplayPayload",
    "decode_domain_event_json",
    "decode_external_adapter_replay_messagepack",
    "decode_fixture_config_json",
    "decode_paper_replay_messagepack",
    "encode_domain_event_json",
    "encode_external_adapter_replay_messagepack",
    "encode_fixture_config_json",
    "encode_paper_replay_messagepack",
    "run_msgspec_codec_benchmark",
]
