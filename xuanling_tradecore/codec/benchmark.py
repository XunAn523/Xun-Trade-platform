"""Repeatable, no-I/O microbenchmark for the isolated M0 msgspec codec trial."""

from __future__ import annotations

import json
import platform
import statistics
import sys
from dataclasses import asdict, dataclass
from time import perf_counter_ns
from typing import Callable, Mapping, TypeVar

import msgspec

from .typed_payloads import (
    CODEC_SCHEMA_VERSION,
    DomainEventEnvelope,
    ExternalAdapterReplayPayload,
    FixtureOrderConfig,
    FixturePriceBand,
    decode_domain_event_json,
    decode_external_adapter_replay_messagepack,
    decode_fixture_config_json,
    encode_domain_event_json,
    encode_external_adapter_replay_messagepack,
    encode_fixture_config_json,
)

_Payload = TypeVar("_Payload")


@dataclass(frozen=True)
class CodecBenchmarkMeasurement:
    """One batch-averaged codec measurement; it is evidence, not a capacity gate."""

    workload: str
    protocol: str
    encoded_bytes: int
    msgspec_encode_mean_ns: int
    msgspec_decode_mean_ns: int
    msgspec_encode_p99_ns: int
    msgspec_decode_p99_ns: int
    stdlib_encode_mean_ns: int | None = None
    stdlib_decode_mean_ns: int | None = None


@dataclass(frozen=True)
class CodecBenchmarkReport:
    """A reproducible local experiment report that deliberately contains no business payload."""

    schema_version: int
    library: str
    library_version: str
    python_version: str
    platform: str
    iterations: int
    warmup: int
    rounds: int
    measurements: tuple[CodecBenchmarkMeasurement, ...]

    def as_dict(self) -> Mapping[str, object]:
        """Return a JSON-ready report without retaining any sample payload."""

        return asdict(self)


def run_msgspec_codec_benchmark(
    *,
    iterations: int = 50_000,
    warmup: int = 5_000,
    rounds: int = 20,
) -> CodecBenchmarkReport:
    """Benchmark only the explicitly typed boundary codecs with bounded local samples."""

    if iterations < rounds or warmup < 0 or rounds < 1:
        raise ValueError("iterations, warmup, and rounds are invalid")
    event = DomainEventEnvelope(
        schema_version=CODEC_SCHEMA_VERSION,
        event_id="event-benchmark-1",
        event_kind="ORDER",
        account_id="account-benchmark",
        trading_day="2026-08-31",
        journal_id="journal-benchmark",
        journal_lsn=42,
        execution_id="execution-benchmark-1",
        source_key="order:1001:working",
    )
    replay = ExternalAdapterReplayPayload(
        schema_version=CODEC_SCHEMA_VERSION,
        replay_id="replay-benchmark-1",
        replay_sequence=1,
        event_kind="TRADE",
        account_id="account-benchmark",
        trading_day="2026-08-31",
        symbol="000010",
        side="BUY",
        price_ticks=161,
        quantity=100,
        source_key="paper:trade:1",
    )
    fixture = FixtureOrderConfig(
        schema_version=CODEC_SCHEMA_VERSION,
        fixture_id="fixture-benchmark-1",
        environment="external-adapter-fixture",
        armed=False,
        symbol="000010",
        market="SZ",
        side="BUY",
        quantity=100,
        price_band=FixturePriceBand(157, 163, 1),
    )
    measurements = (
        _json_measurement(
            workload="domain_event_json",
            value=event,
            encode=encode_domain_event_json,
            decode=decode_domain_event_json,
            iterations=iterations,
            warmup=warmup,
            rounds=rounds,
        ),
        _json_measurement(
            workload="fixture_config_json",
            value=fixture,
            encode=encode_fixture_config_json,
            decode=decode_fixture_config_json,
            iterations=iterations,
            warmup=warmup,
            rounds=rounds,
        ),
        _messagepack_measurement(
            value=replay,
            iterations=iterations,
            warmup=warmup,
            rounds=rounds,
        ),
    )
    return CodecBenchmarkReport(
        schema_version=1,
        library="msgspec",
        library_version=msgspec.__version__,
        python_version=sys.version.split()[0],
        platform=platform.platform(),
        iterations=iterations,
        warmup=warmup,
        rounds=rounds,
        measurements=measurements,
    )


def _json_measurement(
    *,
    workload: str,
    value: _Payload,
    encode: Callable[[_Payload], bytes],
    decode: Callable[[bytes], _Payload],
    iterations: int,
    warmup: int,
    rounds: int,
) -> CodecBenchmarkMeasurement:
    encoded = encode(value)
    if decode(encoded) != value:
        raise RuntimeError("msgspec JSON benchmark round trip changed the typed value")
    plain = msgspec.to_builtins(value)
    stdlib_encoded = json.dumps(plain, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if json.loads(stdlib_encoded) != plain:
        raise RuntimeError("stdlib JSON benchmark round trip changed the plain value")
    _warm(encode, value, warmup)
    _warm(decode, encoded, warmup)
    _warm(_stdlib_json_encode, plain, warmup)
    _warm(json.loads, stdlib_encoded, warmup)
    msgspec_encode = _measure(lambda: encode(value), iterations=iterations, rounds=rounds)
    msgspec_decode = _measure(lambda: decode(encoded), iterations=iterations, rounds=rounds)
    stdlib_encode = _measure(
        lambda: _stdlib_json_encode(plain), iterations=iterations, rounds=rounds
    )
    stdlib_decode = _measure(
        lambda: json.loads(stdlib_encoded), iterations=iterations, rounds=rounds
    )
    return CodecBenchmarkMeasurement(
        workload=workload,
        protocol="json",
        encoded_bytes=len(encoded),
        msgspec_encode_mean_ns=msgspec_encode.mean_ns,
        msgspec_decode_mean_ns=msgspec_decode.mean_ns,
        msgspec_encode_p99_ns=msgspec_encode.p99_ns,
        msgspec_decode_p99_ns=msgspec_decode.p99_ns,
        stdlib_encode_mean_ns=stdlib_encode.mean_ns,
        stdlib_decode_mean_ns=stdlib_decode.mean_ns,
    )


def _messagepack_measurement(
    *,
    value: ExternalAdapterReplayPayload,
    iterations: int,
    warmup: int,
    rounds: int,
) -> CodecBenchmarkMeasurement:
    encoded = encode_external_adapter_replay_messagepack(value)
    if decode_external_adapter_replay_messagepack(encoded) != value:
        raise RuntimeError("msgspec MessagePack benchmark round trip changed the typed value")
    _warm(encode_external_adapter_replay_messagepack, value, warmup)
    _warm(decode_external_adapter_replay_messagepack, encoded, warmup)
    encode_measurement = _measure(
        lambda: encode_external_adapter_replay_messagepack(value),
        iterations=iterations,
        rounds=rounds,
    )
    decode_measurement = _measure(
        lambda: decode_external_adapter_replay_messagepack(encoded),
        iterations=iterations,
        rounds=rounds,
    )
    return CodecBenchmarkMeasurement(
        workload="external_adapter_replay_messagepack",
        protocol="messagepack",
        encoded_bytes=len(encoded),
        msgspec_encode_mean_ns=encode_measurement.mean_ns,
        msgspec_decode_mean_ns=decode_measurement.mean_ns,
        msgspec_encode_p99_ns=encode_measurement.p99_ns,
        msgspec_decode_p99_ns=decode_measurement.p99_ns,
    )


@dataclass(frozen=True)
class _Timing:
    mean_ns: int
    p99_ns: int


def _warm(operation: Callable[[_Payload], object], argument: _Payload, iterations: int) -> None:
    for _ in range(iterations):
        operation(argument)


def _measure(operation: Callable[[], object], *, iterations: int, rounds: int) -> _Timing:
    operations_per_round = iterations // rounds
    remaining = iterations % rounds
    per_operation_samples: list[int] = []
    for round_index in range(rounds):
        count = operations_per_round + (1 if round_index < remaining else 0)
        started_ns = perf_counter_ns()
        for _ in range(count):
            operation()
        elapsed_ns = perf_counter_ns() - started_ns
        per_operation_samples.append(elapsed_ns // count)
    ordered = sorted(per_operation_samples)
    p99_index = min(len(ordered) - 1, (len(ordered) * 99 + 99) // 100 - 1)
    return _Timing(
        mean_ns=int(statistics.fmean(per_operation_samples)),
        p99_ns=ordered[p99_index],
    )


def _stdlib_json_encode(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
