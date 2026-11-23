"""Strict, secret-free configuration parsing for a Windows QMT Edge scope."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, cast

from ...domain._validation import require_text
from .native_engine_contract import QmtNativeContractError


class QmtEdgeConfigError(RuntimeError):
    """A Windows Edge configuration is incomplete, unsafe, or cross-scope."""


_SCHEMA = "TradeCoreQmtEdgeConfigV1"
_PLACEHOLDER_PREFIX = "REPLACE_"


def _require_hex(value: object, *, name: str, size: int) -> bytes:
    text = require_text(value, name, max_length=size * 2)
    if text.startswith(_PLACEHOLDER_PREFIX):
        raise QmtEdgeConfigError(f"QMT Edge {name} remains a deployment placeholder")
    try:
        encoded = bytes.fromhex(text)
    except ValueError as error:
        raise QmtEdgeConfigError(f"QMT Edge {name} is not hexadecimal") from error
    if len(encoded) != size:
        raise QmtEdgeConfigError(f"QMT Edge {name} has an invalid size")
    return encoded


def _require_path(value: object, *, name: str) -> Path:
    text = require_text(value, name, max_length=1024)
    if text.startswith(_PLACEHOLDER_PREFIX):
        raise QmtEdgeConfigError(f"QMT Edge {name} remains a deployment placeholder")
    return Path(text)


def _optional_deployment_text(value: object, *, name: str) -> str | None:
    if value is None:
        return None
    text = require_text(value, name, max_length=256)
    return None if text.startswith(_PLACEHOLDER_PREFIX) else text


@dataclass(frozen=True)
class QmtNativeSubmitRouteV1:
    """A P0-proven mapping from TradeCore mapped enums to one QMT call shape."""

    business_type: int
    side: int
    price_type: int
    op_type: int
    order_type: int
    qmt_price_type: int


@dataclass(frozen=True)
class QmtNativePolicyV1:
    """The approved native argument policy; it is absent in disarmed P0 mode."""

    strategy_name: str
    account_type: str
    price_tick_divisor: int
    market_suffixes: Mapping[int, str]
    submit_routes: tuple[QmtNativeSubmitRouteV1, ...]
    query_data_types: Mapping[str, str]

    def route_for(
        self, *, business_type: int, side: int, price_type: int
    ) -> QmtNativeSubmitRouteV1:
        for route in self.submit_routes:
            if (
                route.business_type == business_type
                and route.side == side
                and route.price_type == price_type
            ):
                return route
        raise QmtEdgeConfigError("QMT Edge native policy lacks the mapped submit route")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "QmtNativePolicyV1":
        required = {
            "strategy_name",
            "account_type",
            "price_tick_divisor",
            "market_suffixes",
            "submit_routes",
            "query_data_types",
        }
        if set(raw) != required:
            raise QmtEdgeConfigError("QMT Edge native policy fields are incomplete")
        divisor = raw["price_tick_divisor"]
        if type(divisor) is not int or not 1 <= divisor <= 1_000_000_000:
            raise QmtEdgeConfigError("QMT Edge native policy price tick divisor is invalid")
        suffixes_object = raw["market_suffixes"]
        if not isinstance(suffixes_object, Mapping):
            raise QmtEdgeConfigError("QMT Edge native policy market suffixes are invalid")
        suffixes: dict[int, str] = {}
        for key, value in cast(Mapping[object, object], suffixes_object).items():
            try:
                numeric_key = int(
                    require_text(key, "QMT Edge native market key", max_length=16)
                )
            except ValueError as error:
                raise QmtEdgeConfigError("QMT Edge native policy market key is invalid") from error
            suffixes[numeric_key] = require_text(value, "QMT Edge native market suffix", max_length=16)
        raw_routes = raw["submit_routes"]
        if not isinstance(raw_routes, list) or not raw_routes:
            raise QmtEdgeConfigError("QMT Edge native policy submit routes are invalid")
        routes: list[QmtNativeSubmitRouteV1] = []
        for item in cast(list[object], raw_routes):
            if not isinstance(item, Mapping):
                raise QmtEdgeConfigError("QMT Edge native policy submit route is invalid")
            route = cast(Mapping[object, object], item)
            expected_route_fields = {
                "business_type",
                "side",
                "price_type",
                "op_type",
                "order_type",
                "qmt_price_type",
            }
            if set(route) != expected_route_fields or any(
                type(value) is not int for value in route.values()
            ):
                raise QmtEdgeConfigError("QMT Edge native policy submit route fields are invalid")
            routes.append(
                QmtNativeSubmitRouteV1(
                    business_type=cast(int, route["business_type"]),
                    side=cast(int, route["side"]),
                    price_type=cast(int, route["price_type"]),
                    op_type=cast(int, route["op_type"]),
                    order_type=cast(int, route["order_type"]),
                    qmt_price_type=cast(int, route["qmt_price_type"]),
                )
            )
        if len({(route.business_type, route.side, route.price_type) for route in routes}) != len(
            routes
        ):
            raise QmtEdgeConfigError("QMT Edge native policy repeats a submit route")
        data_types_object = raw["query_data_types"]
        if not isinstance(data_types_object, Mapping):
            raise QmtEdgeConfigError("QMT Edge native policy query data types are invalid")
        data_types: dict[str, str] = {}
        for key, value in cast(Mapping[object, object], data_types_object).items():
            data_types[require_text(key, "QMT Edge query kind", max_length=64)] = require_text(
                value, "QMT Edge native query type", max_length=64
            )
        return cls(
            strategy_name=require_text(raw["strategy_name"], "QMT Edge native strategy", max_length=64),
            account_type=require_text(raw["account_type"], "QMT Edge native account type", max_length=16),
            price_tick_divisor=divisor,
            market_suffixes=suffixes,
            submit_routes=tuple(routes),
            query_data_types=data_types,
        )


@dataclass(frozen=True)
class QmtEdgeConfigV1:
    """One local scope's validated configuration, excluding secret contents."""

    scope_digest: bytes
    generation: bytes
    command_ring_path: Path
    event_ring_path: Path
    atomic_dll_path: Path
    database_path: Path
    ring_capacity: int
    sqlite_synchronous: str
    qmt_timer_period: str
    qmt_timer_start_time: str
    max_drain: int
    mode: str
    allow_mutation: bool
    callback_attributes: Mapping[str, str]
    account_id: str | None
    adapter_instance_id: str | None
    adapter_client_scope: str | None
    native_policy: QmtNativePolicyV1 | None

    @property
    def is_disarmed(self) -> bool:
        return self.mode == "PROBE" and not self.allow_mutation

    @classmethod
    def load(cls, path: Path) -> "QmtEdgeConfigV1":
        try:
            raw_object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise QmtEdgeConfigError("QMT Edge configuration cannot be read as UTF-8 JSON") from error
        if not isinstance(raw_object, dict):
            raise QmtEdgeConfigError("QMT Edge configuration must be an object")
        raw = cast(dict[str, object], raw_object)
        if raw.get("schema") != _SCHEMA:
            raise QmtEdgeConfigError("QMT Edge configuration schema is unsupported")
        callback_object = raw.get("callback_attributes")
        if not isinstance(callback_object, dict):
            raise QmtEdgeConfigError("QMT Edge callback_attributes must be an object")
        callback_attributes = cast(dict[str, object], callback_object)
        required_callback_fields = {
            "event_key",
            "client_order_ref",
            "venue_order_id",
            "trade_key",
            "filled_qty",
            "leaves_qty",
            "error_code",
        }
        if set(callback_attributes) != required_callback_fields:
            raise QmtEdgeConfigError("QMT Edge callback attribute keys are incomplete")
        normalized_attributes = {
            key: require_text(value, f"QMT Edge callback attribute {key}", max_length=128)
            for key, value in callback_attributes.items()
        }
        raw_capacity, raw_max_drain = raw.get("ring_capacity"), raw.get("max_drain")
        if (
            type(raw_capacity) is not int
            or type(raw_max_drain) is not int
        ):
            raise QmtEdgeConfigError("QMT Edge ring capacity or max drain is invalid")
        capacity, max_drain = raw_capacity, raw_max_drain
        if not 2 <= capacity <= 65536 or not 1 <= max_drain <= capacity:
            raise QmtEdgeConfigError("QMT Edge ring capacity or max drain is outside its bounded range")
        synchronous = require_text(raw.get("sqlite_synchronous"), "QMT Edge sqlite_synchronous")
        if synchronous not in {"NORMAL", "FULL"}:
            raise QmtEdgeConfigError("QMT Edge sqlite_synchronous must be NORMAL or FULL")
        mode = require_text(raw.get("mode"), "QMT Edge mode")
        mutation = raw.get("allow_mutation")
        if mode not in {"PROBE", "ARMED"} or type(mutation) is not bool:
            raise QmtEdgeConfigError("QMT Edge mode or mutation flag is invalid")
        if mode == "PROBE" and mutation:
            raise QmtEdgeConfigError("QMT Edge PROBE mode cannot allow mutation")
        if mode == "ARMED" and not mutation:
            raise QmtEdgeConfigError("QMT Edge ARMED mode lacks a mutation approval")
        raw_policy = raw.get("native_policy")
        if raw_policy is not None and not isinstance(raw_policy, Mapping):
            raise QmtEdgeConfigError("QMT Edge native_policy must be an object")
        native_policy = (
            QmtNativePolicyV1.from_mapping(cast(Mapping[str, object], raw_policy))
            if raw_policy is not None
            else None
        )
        account_id = _optional_deployment_text(raw.get("account_id"), name="QMT Edge account_id")
        adapter_instance_id = _optional_deployment_text(
            raw.get("adapter_instance_id"), name="QMT Edge adapter_instance_id"
        )
        adapter_client_scope = _optional_deployment_text(
            raw.get("adapter_client_scope"), name="QMT Edge adapter_client_scope"
        )
        if mode == "ARMED" and (
            account_id is None
            or adapter_instance_id is None
            or adapter_client_scope is None
            or native_policy is None
        ):
            raise QmtEdgeConfigError("QMT Edge ARMED mode lacks approved native identity or policy")
        return cls(
            scope_digest=_require_hex(raw.get("scope_digest_hex"), name="scope digest", size=32),
            generation=_require_hex(raw.get("generation_hex"), name="generation", size=16),
            command_ring_path=_require_path(raw.get("command_ring_path"), name="command ring path"),
            event_ring_path=_require_path(raw.get("event_ring_path"), name="event ring path"),
            atomic_dll_path=_require_path(raw.get("atomic_dll_path"), name="atomic DLL path"),
            database_path=_require_path(raw.get("database_path"), name="database path"),
            ring_capacity=capacity,
            sqlite_synchronous=synchronous,
            qmt_timer_period=require_text(raw.get("qmt_timer_period"), "QMT Edge timer period"),
            qmt_timer_start_time=require_text(
                raw.get("qmt_timer_start_time"), "QMT Edge timer start time"
            ),
            max_drain=max_drain,
            mode=mode,
            allow_mutation=mutation,
            callback_attributes=normalized_attributes,
            account_id=account_id,
            adapter_instance_id=adapter_instance_id,
            adapter_client_scope=adapter_client_scope,
            native_policy=native_policy,
        )

    def require_native_assets(self) -> None:
        """Validate assets required before an Edge scope can open its local rings."""

        if not self.atomic_dll_path.is_file():
            raise QmtEdgeConfigError("QMT Edge atomic DLL is missing")
        for path in (self.command_ring_path, self.event_ring_path):
            if path.exists() and not path.is_file():
                raise QmtEdgeConfigError("QMT Edge ring path is not a regular file")
        if self.database_path.exists() and not self.database_path.is_file():
            raise QmtEdgeConfigError("QMT Edge database path is not a regular file")


def require_native_payload_size(payload: bytes) -> bytes:
    """Translate native ABI failures without exposing request contents."""

    if len(payload) > 388:
        raise QmtNativeContractError("QMT Edge native payload exceeds the ring ABI")
    return payload
