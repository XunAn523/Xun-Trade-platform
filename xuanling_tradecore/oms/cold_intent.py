"""Storage-neutral cold intent lookup and committed handoff ports."""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from typing import Protocol

import msgspec

from .order_book import OmsOrderBookError


class ColdIntentIndexError(OmsOrderBookError):
    pass


class ColdIntentIndexBusy(ColdIntentIndexError):
    pass


@dataclass(frozen=True)
class ColdIntentKey:
    account_id: str
    strategy_id: str
    strategy_order_id: str

    def __post_init__(self) -> None:
        if any(
            type(v) is not str or not 1 <= len(v) <= 128
            for v in (self.account_id, self.strategy_id, self.strategy_order_id)
        ):
            raise ColdIntentIndexError("cold intent key is invalid")

    def encoded(self) -> bytes:
        return msgspec.json.encode((self.account_id, self.strategy_id, self.strategy_order_id))


@dataclass(frozen=True)
class ColdIntentResolution:
    key: ColdIntentKey
    generation: int
    owner_nonce: bytes
    checkpoint: bytes | None
    lookup_epoch: int | None = None


@dataclass(frozen=True)
class ColdArchiveReceipt:
    generation: int
    owner_nonce: bytes
    keys: tuple[ColdIntentKey, ...]
    digests: tuple[bytes, ...]


class ColdIntentStore(Protocol):
    def probe(self, key: ColdIntentKey) -> ColdIntentResolution | None: ...

    def validate(self, result: ColdIntentResolution | ColdArchiveReceipt) -> bool: ...


class ColdIntentResolver(Protocol):
    """Enqueue only; storage work and completion run outside the submit lane."""

    def resolve(self, key: ColdIntentKey) -> Future[ColdIntentResolution]: ...
