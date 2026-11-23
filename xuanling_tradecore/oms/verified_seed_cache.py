"""Bounded proof memo for immutable admission seeds, never current order state.

Only the cold projection owner uses this cache. New proofs and LRU promotions
are published after its SQL FULL commit; aborts cannot publish speculative data.
Every hit rechecks namespace/profile, SHA-256 AND the complete incoming bytes.
"""

from __future__ import annotations

# These two private-protocol participants deliberately share owner-controlled
# state inside this module; no caller receives the cache's entry mapping.
# pyright: reportPrivateUsage=false
from collections import OrderedDict
from hashlib import sha256
from threading import get_ident

from .order_book import OmsOrderBookError
from .recovery import OmsRecoverySeedV1

SeedContext = tuple[str, str, str]  # Journal, profile hash, projection generation
SeedKey = tuple[str, str]  # account, OMS order; never a caller's opaque cache key


class VerifiedAdmissionSeedCache:
    MAXIMUM_ORDERS = 1024
    MAXIMUM_BYTES = 16 * 1024**2

    def __init__(self, context: SeedContext) -> None:
        if (
            len(context) != 3
            or not all(type(value) is str and value for value in context)
            or any(len(value) != 64 for value in context[1:])
        ):
            raise OmsOrderBookError("invalid admission proof namespace")
        self._context = context
        self._owner = get_ident()
        self._entries: OrderedDict[SeedKey, OmsRecoverySeedV1] = OrderedDict()
        self._bytes = self.hits = self.misses = self.evictions = 0
        self._active: AdmissionSeedTransaction | None = None
        self._closed = False

    def _check(self, context: SeedContext) -> None:
        if self._closed or get_ident() != self._owner or context != self._context:
            raise OmsOrderBookError("admission proof owner/profile/namespace changed")

    def begin(self, context: SeedContext) -> AdmissionSeedTransaction:
        self._check(context)
        if self._active is not None:
            raise OmsOrderBookError("nested admission proof transaction")
        self._active = AdmissionSeedTransaction(self, context)
        return self._active

    @property
    def entries(self) -> int:
        self._check(self._context)
        return len(self._entries)

    @property
    def encoded_bytes(self) -> int:
        self._check(self._context)
        return self._bytes

    def close(self) -> None:
        self._check(self._context)
        if self._active is not None:
            self._active.abort()
        self._entries.clear()
        self._bytes = 0
        self._closed = True


class AdmissionSeedTransaction:
    def __init__(self, cache: VerifiedAdmissionSeedCache, context: SeedContext) -> None:
        self._cache, self._context = cache, context
        self._staged: OrderedDict[SeedKey, OmsRecoverySeedV1] = OrderedDict()
        self._done = False

    def _check(self) -> None:
        self._cache._check(self._context)
        if self._done or self._cache._active is not self:
            raise OmsOrderBookError("admission proof transaction is not active")

    def resolve(self, encoded: bytes, *, account_id: str, oms_order_id: str) -> OmsRecoverySeedV1:
        self._check()
        if type(encoded) is not bytes or not 0 < len(encoded) <= 64 * 1024:
            raise OmsOrderBookError("admission seed exceeds proof budget")
        if not account_id or not oms_order_id:
            raise OmsOrderBookError("admission proof identity is missing")
        key = (account_id, oms_order_id)
        proof = self._staged.get(key) or self._cache._entries.get(key)
        if proof is not None:
            if sha256(encoded).hexdigest() != proof.seed_hash or encoded != proof.encoded:
                raise OmsOrderBookError("immutable admission proof bytes/hash changed")
            self._cache.hits += 1
        else:
            if len(self._staged) >= 256:
                raise OmsOrderBookError("admission proof transaction exceeds order budget")
            proof = OmsRecoverySeedV1.decode_and_verify(encoded)
            self._cache.misses += 1
        if (proof.order.order.account_id, proof.order.order.oms_order_id) != key:
            raise OmsOrderBookError("admission proof belongs to another order")
        self._staged[key] = proof
        self._staged.move_to_end(key)
        return proof

    def publish_after_full_commit(self) -> None:
        self._check()
        cache = self._cache
        for key, proof in self._staged.items():
            previous = cache._entries.pop(key, None)
            if previous is not None:
                cache._bytes -= len(previous.encoded)
            cache._entries[key] = proof
            cache._bytes += len(proof.encoded)
            while len(cache._entries) > cache.MAXIMUM_ORDERS or cache._bytes > cache.MAXIMUM_BYTES:
                _, removed = cache._entries.popitem(last=False)
                cache._bytes -= len(removed.encoded)
                cache.evictions += 1
        self.abort()

    def abort(self) -> None:
        self._cache._check(self._context)
        if not self._done:
            self._staged.clear()
            self._done = True
            self._cache._active = None
