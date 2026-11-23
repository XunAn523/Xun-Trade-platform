"""QMT order_remark route-token codec contract with checksum validation."""

from __future__ import annotations

import base64
import re
import zlib
from dataclasses import dataclass

from ...domain import AdapterRouteToken, RouteTokenBinding
from ..route_token import RouteTokenCodecError

_QMT_REMARK = re.compile(r"Q(?P<version>[1-9])(?P<body>[A-Z2-7]{26})(?P<crc>[0-9A-F]{4})\Z")


def qmt_route_token_binding_v1() -> RouteTokenBinding:
    """Return the frozen QMT fake-profile binding; production QMT is out of scope."""

    return RouteTokenBinding(
        binding_id="qmt-order-remark-v1",
        version=1,
        manifest_hash="8b381c1ab5a796ceb5e3f099be11b197195981e5d0614a22926a05d24b161e5a",
        venue_id="QMT",
        native_field_name="order_remark",
        native_field_type="utf8",
        max_encoded_bytes=32,
        allowed_characters=r"^Q1[A-Z2-7]{26}[0-9A-F]{4}$",
        write_path="QmtOrder.order_remark",
        order_callback_paths=("QmtOrderCallback.order_remark",),
        trade_callback_paths=("QmtTradeCallback.order_remark",),
        cancel_error_paths=("QmtCancelError.order_remark",),
        query_paths=("QmtOrderQuery[].order_remark", "QmtTradeQuery[].order_remark"),
        replay_paths=("QmtRecovery.orders[].order_remark", "QmtRecovery.trades[].order_remark"),
        normalization_version=1,
    )


@dataclass(frozen=True)
class QmtOrderRemarkCodec:
    """Reversible 32-byte ASCII QMT order_remark with version and CRC16 tag."""

    binding: RouteTokenBinding = qmt_route_token_binding_v1()

    def __post_init__(self) -> None:
        if (
            self.binding.venue_id != "QMT"
            or self.binding.native_field_name != "order_remark"
            or self.binding.native_field_type != "utf8"
        ):
            raise RouteTokenCodecError("QMT codec binding identity is invalid")

    def encode(
        self,
        token: AdapterRouteToken,
        *,
        allocated_client_order_ref: str | None = None,
    ) -> str:
        if allocated_client_order_ref is not None:
            raise RouteTokenCodecError("QMT order_remark cannot be supplied by a caller")
        if token.version != 1 or len(token.token_bytes) != 16:
            raise RouteTokenCodecError("QMT V1 requires a 16-byte route token")
        body = base64.b32encode(token.token_bytes).decode("ascii").rstrip("=")
        checksum = zlib.crc32(token.token_bytes) & 0xFFFF
        value = f"Q1{body}{checksum:04X}"
        self._require_native_shape(value)
        return value

    def normalize_extracted(
        self,
        *,
        path: str,
        native_value: object,
        expected_token: AdapterRouteToken | None,
    ) -> AdapterRouteToken:
        self._require_declared_path(path)
        if not isinstance(native_value, str):
            raise RouteTokenCodecError("QMT extracted order_remark must be text")
        self._require_native_shape(native_value)
        match = _QMT_REMARK.fullmatch(native_value)
        if match is None:
            raise RouteTokenCodecError("QMT order_remark is not canonical")
        try:
            raw = base64.b32decode(match.group("body") + "======", casefold=False)
        except ValueError as exc:
            raise RouteTokenCodecError("QMT order_remark body is invalid") from exc
        checksum = zlib.crc32(raw) & 0xFFFF
        if f"{checksum:04X}" != match.group("crc"):
            raise RouteTokenCodecError("QMT order_remark checksum mismatch")
        token = AdapterRouteToken(version=int(match.group("version")), token_bytes=raw)
        if self.encode(token) != native_value:
            raise RouteTokenCodecError("QMT order_remark is not canonical")
        if expected_token is not None and token != expected_token:
            raise RouteTokenCodecError("QMT order_remark conflicts with the route directory")
        return token

    def _require_declared_path(self, path: str) -> None:
        declared = (
            self.binding.order_callback_paths
            + self.binding.trade_callback_paths
            + self.binding.cancel_error_paths
            + self.binding.query_paths
            + self.binding.replay_paths
        )
        if path not in declared:
            raise RouteTokenCodecError("QMT extraction path is not declared by the binding")

    def _require_native_shape(self, value: str) -> None:
        try:
            encoded = value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise RouteTokenCodecError("QMT order_remark must be ASCII") from exc
        if (
            len(encoded) > self.binding.max_encoded_bytes
            or re.fullmatch(self.binding.allowed_characters, value) is None
        ):
            raise RouteTokenCodecError("QMT order_remark violates its binding shape")
