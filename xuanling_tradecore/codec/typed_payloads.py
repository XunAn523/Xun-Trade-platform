"""Explicit schemas for the M0 ``msgspec`` boundary-codec trial.

These envelopes are intentionally not OMS Journal records and are not broker
SDK callback objects.  They apply only at typed JSON/MessagePack boundaries:
consumer-visible domain-event exchange, normalized external-adapter replay, and
controlled fixture loading. The existing FileJournal V1 binary protocol and
the P0 callback-copy path remain untouched.
"""

from __future__ import annotations

import msgspec

CODEC_SCHEMA_VERSION = 1


class CodecBoundaryError(ValueError):
    """An external typed payload does not satisfy the approved M0 boundary schema."""


class DomainEventEnvelope(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """A compact, payload-free view of a durable domain event for exchange only."""

    schema_version: int
    event_id: str
    event_kind: str
    account_id: str
    trading_day: str
    journal_id: str
    journal_lsn: int
    execution_id: str | None = None
    source_key: str | None = None


class ExternalAdapterReplayPayload(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """A normalized replay fixture; it does not model an external venue's internals."""

    schema_version: int
    replay_id: str
    replay_sequence: int
    event_kind: str
    account_id: str
    trading_day: str
    symbol: str
    side: str
    price_ticks: int
    quantity: int
    source_key: str


class FixturePriceBand(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Fixed-point limit-price band used only by typed controlled-fixture loading."""

    minimum_price_ticks: int
    maximum_price_ticks: int
    tick_size: int


class FixtureOrderConfig(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """One controlled codec fixture configuration, never a live order request."""

    schema_version: int
    fixture_id: str
    environment: str
    armed: bool
    symbol: str
    market: str
    side: str
    quantity: int
    price_band: FixturePriceBand


_DOMAIN_EVENT_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
_FIXTURE_CONFIG_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
_EXTERNAL_REPLAY_MSGPACK_ENCODER = msgspec.msgpack.Encoder(order="deterministic")
_DOMAIN_EVENT_JSON_DECODER = msgspec.json.Decoder(type=DomainEventEnvelope, strict=True)
_FIXTURE_CONFIG_JSON_DECODER = msgspec.json.Decoder(type=FixtureOrderConfig, strict=True)
_EXTERNAL_REPLAY_MSGPACK_DECODER = msgspec.msgpack.Decoder(
    type=ExternalAdapterReplayPayload,
    strict=True,
)

# Reader/source compatibility for the pre-acceptance M0 trial name. This is an
# alias of the generic boundary schema, not a PaperBroker matching model.
PaperBrokerReplayPayload = ExternalAdapterReplayPayload


def encode_domain_event_json(payload: DomainEventEnvelope) -> bytes:
    """Encode one version-checked domain-event exchange envelope as deterministic JSON."""

    return _DOMAIN_EVENT_JSON_ENCODER.encode(_check_domain_event(payload))


def decode_domain_event_json(payload: bytes) -> DomainEventEnvelope:
    """Decode strict JSON without exposing a raw codec error or raw payload."""

    try:
        decoded = _DOMAIN_EVENT_JSON_DECODER.decode(payload)
    except (msgspec.DecodeError, msgspec.ValidationError) as error:
        raise CodecBoundaryError("domain event JSON does not match the approved schema") from error
    return _check_domain_event(decoded)


def encode_external_adapter_replay_messagepack(payload: ExternalAdapterReplayPayload) -> bytes:
    """Encode one version-checked normalized adapter replay fact as MessagePack."""

    return _EXTERNAL_REPLAY_MSGPACK_ENCODER.encode(_check_external_adapter_replay(payload))


def decode_external_adapter_replay_messagepack(payload: bytes) -> ExternalAdapterReplayPayload:
    """Decode strict MessagePack without exposing a raw codec error or raw payload."""

    try:
        decoded = _EXTERNAL_REPLAY_MSGPACK_DECODER.decode(payload)
    except (msgspec.DecodeError, msgspec.ValidationError) as error:
        raise CodecBoundaryError(
            "external adapter replay MessagePack does not match the approved schema"
        ) from error
    return _check_external_adapter_replay(decoded)


def encode_paper_replay_messagepack(payload: ExternalAdapterReplayPayload) -> bytes:
    """Compatibility alias; PaperBroker remains an external downstream system."""

    return encode_external_adapter_replay_messagepack(payload)


def decode_paper_replay_messagepack(payload: bytes) -> ExternalAdapterReplayPayload:
    """Compatibility alias for artifacts produced by the early M0 trial."""

    return decode_external_adapter_replay_messagepack(payload)


def encode_fixture_config_json(payload: FixtureOrderConfig) -> bytes:
    """Encode a version-checked, non-armed fixture configuration as deterministic JSON."""

    return _FIXTURE_CONFIG_JSON_ENCODER.encode(_check_fixture_config(payload))


def decode_fixture_config_json(payload: bytes) -> FixtureOrderConfig:
    """Decode strict fixture JSON without falling back to untyped dictionaries."""

    try:
        decoded = _FIXTURE_CONFIG_JSON_DECODER.decode(payload)
    except (msgspec.DecodeError, msgspec.ValidationError) as error:
        raise CodecBoundaryError("fixture JSON does not match the approved schema") from error
    return _check_fixture_config(decoded)


def _check_domain_event(payload: DomainEventEnvelope) -> DomainEventEnvelope:
    _require_schema_version(payload.schema_version, "domain event")
    _require_text(payload.event_id, "domain event id")
    _require_text(payload.event_kind, "domain event kind")
    _require_text(payload.account_id, "domain event account id")
    _require_text(payload.trading_day, "domain event trading day")
    _require_text(payload.journal_id, "domain event journal id")
    _require_nonnegative(payload.journal_lsn, "domain event journal lsn")
    if payload.execution_id is not None:
        _require_text(payload.execution_id, "domain event execution id")
    if payload.source_key is not None:
        _require_text(payload.source_key, "domain event source key")
    return payload


def _check_external_adapter_replay(
    payload: ExternalAdapterReplayPayload,
) -> ExternalAdapterReplayPayload:
    _require_schema_version(payload.schema_version, "external adapter replay")
    _require_text(payload.replay_id, "external adapter replay id")
    _require_positive(payload.replay_sequence, "external adapter replay sequence")
    _require_text(payload.event_kind, "external adapter replay event kind")
    _require_text(payload.account_id, "external adapter replay account id")
    _require_text(payload.trading_day, "external adapter replay trading day")
    _require_text(payload.symbol, "external adapter replay symbol")
    _require_text(payload.side, "external adapter replay side")
    _require_nonnegative(payload.price_ticks, "external adapter replay price ticks")
    _require_positive(payload.quantity, "external adapter replay quantity")
    _require_text(payload.source_key, "external adapter replay source key")
    return payload


def _check_fixture_config(payload: FixtureOrderConfig) -> FixtureOrderConfig:
    _require_schema_version(payload.schema_version, "fixture")
    _require_text(payload.fixture_id, "fixture id")
    _require_text(payload.environment, "fixture environment")
    if payload.armed:
        raise CodecBoundaryError("M0 typed fixture codec accepts only disarmed configurations")
    _require_text(payload.symbol, "fixture symbol")
    _require_text(payload.market, "fixture market")
    _require_text(payload.side, "fixture side")
    _require_positive(payload.quantity, "fixture quantity")
    band = payload.price_band
    _require_nonnegative(band.minimum_price_ticks, "fixture minimum price ticks")
    _require_nonnegative(band.maximum_price_ticks, "fixture maximum price ticks")
    _require_positive(band.tick_size, "fixture price tick size")
    if band.maximum_price_ticks < band.minimum_price_ticks:
        raise CodecBoundaryError("fixture price band maximum precedes minimum")
    return payload


def _require_schema_version(value: int, name: str) -> None:
    if value != CODEC_SCHEMA_VERSION:
        raise CodecBoundaryError(f"{name} schema_version is not supported")


def _require_text(value: str, name: str) -> None:
    if not value or not value.strip() or len(value) > 256:
        raise CodecBoundaryError(f"{name} is invalid")


def _require_nonnegative(value: int, name: str) -> None:
    if isinstance(value, bool) or value < 0:
        raise CodecBoundaryError(f"{name} is invalid")


def _require_positive(value: int, name: str) -> None:
    if isinstance(value, bool) or value <= 0:
        raise CodecBoundaryError(f"{name} is invalid")
