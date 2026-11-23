"""Fixed binary payloads for the QMT local SPSC rings.

These records deliberately describe only the minimal QMT call fields.  Edge
constructs them from a P0-approved mapping; arbitrary dictionaries, JSON and
SDK objects are never placed into a shared-memory slot.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .native_engine_contract import QmtNativeContractError


def _pack_text(value: str, size: int, name: str) -> bytes:
    if not value:
        raise QmtNativeContractError(f"QMT native {name} must not be empty")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise QmtNativeContractError(f"QMT native {name} is not UTF-8") from error
    if len(encoded) >= size or b"\x00" in encoded:
        raise QmtNativeContractError(f"QMT native {name} exceeds fixed payload capacity")
    return encoded + (b"\x00" * (size - len(encoded)))


def _unpack_text(value: bytes, name: str) -> str:
    raw = value.split(b"\x00", 1)[0]
    if not raw:
        raise QmtNativeContractError(f"QMT native {name} is empty")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise QmtNativeContractError(f"QMT native {name} is not UTF-8") from error


@dataclass(frozen=True)
class QmtNativeSubmitPayloadV1:
    """The fixed call arguments used by one QMT ``passorder`` invocation."""

    op_type: int
    order_type: int
    price_type: int
    price: float
    volume: int
    account_id: str
    order_code: str
    strategy_name: str
    user_order_id: str

    _STRUCT = struct.Struct("<iiidq64s64s64s64s")

    def __post_init__(self) -> None:
        if self.volume <= 0:
            raise QmtNativeContractError("QMT native submit volume must be positive")

    def encode(self) -> bytes:
        return self._STRUCT.pack(
            self.op_type,
            self.order_type,
            self.price_type,
            self.price,
            self.volume,
            _pack_text(self.account_id, 64, "account_id"),
            _pack_text(self.order_code, 64, "order_code"),
            _pack_text(self.strategy_name, 64, "strategy_name"),
            _pack_text(self.user_order_id, 64, "user_order_id"),
        )

    @classmethod
    def decode(cls, raw: bytes) -> "QmtNativeSubmitPayloadV1":
        if len(raw) != cls._STRUCT.size:
            raise QmtNativeContractError("QMT native submit payload size is invalid")
        (
            op_type,
            order_type,
            price_type,
            price,
            volume,
            account_id,
            order_code,
            strategy_name,
            user_order_id,
        ) = cls._STRUCT.unpack(raw)
        return cls(
            op_type=op_type,
            order_type=order_type,
            price_type=price_type,
            price=price,
            volume=volume,
            account_id=_unpack_text(account_id, "account_id"),
            order_code=_unpack_text(order_code, "order_code"),
            strategy_name=_unpack_text(strategy_name, "strategy_name"),
            user_order_id=_unpack_text(user_order_id, "user_order_id"),
        )


@dataclass(frozen=True)
class QmtNativeCancelPayloadV1:
    """The fixed arguments used by one exact-target QMT ``cancel`` call."""

    order_id: str
    account_id: str
    account_type: str

    _STRUCT = struct.Struct("<64s64s16s")

    def encode(self) -> bytes:
        return self._STRUCT.pack(
            _pack_text(self.order_id, 64, "cancel order_id"),
            _pack_text(self.account_id, 64, "cancel account_id"),
            _pack_text(self.account_type, 16, "cancel account_type"),
        )

    @classmethod
    def decode(cls, raw: bytes) -> "QmtNativeCancelPayloadV1":
        if len(raw) != cls._STRUCT.size:
            raise QmtNativeContractError("QMT native cancel payload size is invalid")
        order_id, account_id, account_type = cls._STRUCT.unpack(raw)
        return cls(
            order_id=_unpack_text(order_id, "cancel order_id"),
            account_id=_unpack_text(account_id, "cancel account_id"),
            account_type=_unpack_text(account_type, "cancel account_type"),
        )


@dataclass(frozen=True)
class QmtNativeQueryPayloadV1:
    """A bounded, read-only QMT detail query request."""

    account_id: str
    account_type: str
    data_type: str
    strategy_name: str

    _STRUCT = struct.Struct("<64s16s16s64s")

    def encode(self) -> bytes:
        return self._STRUCT.pack(
            _pack_text(self.account_id, 64, "query account_id"),
            _pack_text(self.account_type, 16, "query account_type"),
            _pack_text(self.data_type, 16, "query data_type"),
            _pack_text(self.strategy_name, 64, "query strategy_name"),
        )

    @classmethod
    def decode(cls, raw: bytes) -> "QmtNativeQueryPayloadV1":
        if len(raw) != cls._STRUCT.size:
            raise QmtNativeContractError("QMT native query payload size is invalid")
        account_id, account_type, data_type, strategy_name = cls._STRUCT.unpack(raw)
        return cls(
            account_id=_unpack_text(account_id, "query account_id"),
            account_type=_unpack_text(account_type, "query account_type"),
            data_type=_unpack_text(data_type, "query data_type"),
            strategy_name=_unpack_text(strategy_name, "query strategy_name"),
        )
