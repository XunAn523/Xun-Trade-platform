"""P0-policy-bound conversion from canonical Edge records to binary QMT frames."""

from __future__ import annotations

from hashlib import sha256
from time import monotonic_ns
from typing import Mapping

from .edge_command import QmtEdgeCommandKind, QmtEdgeCommandV1
from .edge_config import QmtEdgeConfigError, QmtNativePolicyV1
from .native_engine_contract import QmtNativeFrameKind, QmtNativeFrameV1
from .native_payload import (
    QmtNativeCancelPayloadV1,
    QmtNativeQueryPayloadV1,
    QmtNativeSubmitPayloadV1,
)


class QmtEdgeNativeEncodingError(RuntimeError):
    """A canonical command cannot be safely converted to an approved QMT call."""


class QmtEdgeNativeEncoder:
    """Encode only P0-approved routes; never infer a QMT enum or price scale."""

    def __init__(self, policy: QmtNativePolicyV1) -> None:
        self._policy = policy

    def encode(self, command: QmtEdgeCommandV1, *, command_sequence: int) -> QmtNativeFrameV1:
        if command_sequence <= 0:
            raise QmtEdgeNativeEncodingError("QMT Edge command sequence must be positive")
        try:
            kind, payload = self._encode_payload(command)
        except (KeyError, TypeError, ValueError, QmtEdgeConfigError) as error:
            raise QmtEdgeNativeEncodingError("QMT Edge command violates the approved native policy") from error
        now = monotonic_ns()
        return QmtNativeFrameV1(
            sequence=command_sequence,
            kind=kind,
            request_digest=self._request_digest(command),
            intent_digest=bytes.fromhex(command.command_hash),
            timestamps_ns=(0, now, 0, 0),
            payload=payload,
        )

    def _encode_payload(self, command: QmtEdgeCommandV1) -> tuple[QmtNativeFrameKind, bytes]:
        if command.kind is QmtEdgeCommandKind.SUBMIT:
            return QmtNativeFrameKind.SUBMIT, self._encode_submit(
                command.fields, command.account_id
            ).encode()
        if command.kind is QmtEdgeCommandKind.CANCEL:
            return QmtNativeFrameKind.CANCEL, self._encode_cancel(command.fields, command.account_id).encode()
        if command.kind is QmtEdgeCommandKind.QUERY:
            return QmtNativeFrameKind.QUERY, self._encode_query(command.fields, command.account_id).encode()
        raise QmtEdgeNativeEncodingError("QMT recovery begin requires a P3 query plan, not one native frame")

    def _encode_submit(
        self, fields: Mapping[str, object], account_id: str
    ) -> QmtNativeSubmitPayloadV1:
        business_type = _require_integer(fields, "business_type")
        side = _require_integer(fields, "side")
        price_type = _require_integer(fields, "price_type")
        route = self._policy.route_for(
            business_type=business_type, side=side, price_type=price_type
        )
        market = _require_integer(fields, "market")
        suffix = self._policy.market_suffixes.get(market)
        if suffix is None:
            raise QmtEdgeNativeEncodingError("QMT Edge native policy lacks a market suffix")
        symbol = _require_text(fields, "symbol")
        return QmtNativeSubmitPayloadV1(
            op_type=route.op_type,
            order_type=route.order_type,
            price_type=route.qmt_price_type,
            price=_require_integer(fields, "limit_price_ticks") / self._policy.price_tick_divisor,
            volume=_require_integer(fields, "quantity"),
            account_id=account_id,
            order_code=f"{symbol}{suffix}",
            strategy_name=self._policy.strategy_name,
            user_order_id=_require_text(fields, "client_order_ref"),
        )

    def _encode_cancel(self, fields: Mapping[str, object], account_id: str) -> QmtNativeCancelPayloadV1:
        venue_order_id = _require_text(fields, "venue_order_id")
        return QmtNativeCancelPayloadV1(
            order_id=venue_order_id,
            account_id=account_id,
            account_type=self._policy.account_type,
        )

    def _encode_query(self, fields: Mapping[str, object], account_id: str) -> QmtNativeQueryPayloadV1:
        query_kind = _require_text(fields, "query_kind")
        data_type = self._policy.query_data_types.get(query_kind)
        if data_type is None:
            raise QmtEdgeNativeEncodingError("QMT Edge native policy lacks this query kind")
        return QmtNativeQueryPayloadV1(
            account_id=account_id,
            account_type=self._policy.account_type,
            data_type=data_type,
            strategy_name=self._policy.strategy_name,
        )

    @staticmethod
    def _request_digest(command: QmtEdgeCommandV1) -> bytes:
        if command.kind is QmtEdgeCommandKind.SUBMIT:
            reference = _require_text(command.fields, "client_order_ref")
            return sha256(reference.encode("utf-8")).digest()
        return sha256(command.request_id.encode("utf-8")).digest()


def _require_integer(fields: Mapping[str, object], name: str) -> int:
    value = fields[name]
    if type(value) is not int:
        raise QmtEdgeNativeEncodingError(f"QMT Edge command {name} is not an integer")
    return value


def _require_text(fields: Mapping[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise QmtEdgeNativeEncodingError(f"QMT Edge command {name} is not text")
    return value
