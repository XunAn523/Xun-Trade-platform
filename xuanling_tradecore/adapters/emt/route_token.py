"""EMT route-token codec for the manifest-declared uint32 order_client_id field."""

from __future__ import annotations

import re
from dataclasses import dataclass

from ...domain import AdapterRouteToken, RouteTokenBinding
from ..route_token import RouteTokenCodecError
from .identity import EmtClientOrderIdAllocator


@dataclass(frozen=True)
class EmtAdapterRouteTokenCodec:
    """Bind opaque core tokens to Journal-allocated EMT decimal references."""

    binding: RouteTokenBinding

    def __post_init__(self) -> None:
        if self.binding.venue_id != "EMT":
            raise RouteTokenCodecError("EMT codec requires an EMT route token binding")
        if self.binding.native_field_name != "order_client_id":
            raise RouteTokenCodecError("EMT codec requires the order_client_id field")
        if self.binding.native_field_type != "uint32":
            raise RouteTokenCodecError("EMT codec requires a uint32 native field")

    def encode(
        self,
        token: AdapterRouteToken,
        *,
        allocated_client_order_ref: str | None = None,
    ) -> str:
        if allocated_client_order_ref is None:
            raise RouteTokenCodecError("EMT encoding requires a Journal-allocated reference")
        value = str(EmtClientOrderIdAllocator.parse_client_order_ref(allocated_client_order_ref))
        if value != allocated_client_order_ref:
            raise RouteTokenCodecError("EMT client reference is not canonical decimal")
        self._require_native_shape(value)
        if token.version != 1:
            raise RouteTokenCodecError("EMT V1 binding requires route token version 1")
        return value

    def normalize_extracted(
        self,
        *,
        path: str,
        native_value: object,
        expected_token: AdapterRouteToken | None,
    ) -> AdapterRouteToken:
        self._require_declared_path(path)
        if expected_token is None:
            raise RouteTokenCodecError(
                "EMT uint32 references require the recovered route directory token"
            )
        if isinstance(native_value, bool) or not isinstance(native_value, (int, str)):
            raise RouteTokenCodecError("EMT extracted order_client_id has an invalid type")
        value = str(native_value)
        normalized = str(EmtClientOrderIdAllocator.parse_client_order_ref(value))
        if value != normalized:
            raise RouteTokenCodecError("EMT extracted client reference is not canonical decimal")
        self._require_native_shape(normalized)
        return expected_token

    def _require_declared_path(self, path: str) -> None:
        declared = (
            self.binding.order_callback_paths
            + self.binding.trade_callback_paths
            + self.binding.cancel_error_paths
            + self.binding.query_paths
            + self.binding.replay_paths
        )
        if path not in declared:
            raise RouteTokenCodecError("EMT extraction path is not declared by the binding")

    def _require_native_shape(self, value: str) -> None:
        if (
            len(value.encode("ascii")) > self.binding.max_encoded_bytes
            or re.fullmatch(self.binding.allowed_characters, value) is None
        ):
            raise RouteTokenCodecError("EMT client reference violates its binding shape")
