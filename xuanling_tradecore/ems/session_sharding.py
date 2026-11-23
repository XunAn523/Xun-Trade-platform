"""Deterministic, fail-closed routing for multiple native sessions of one account.

An account may own more than one *independently attested* Adapter client scope.
Each scope is one native SDK session and has its own scheduler lane.  This
module chooses a lane only for new executions.  It never provides a fallback:
cancels and callbacks are resolved from the exact native identity that was
returned by the selected session.

The implementation is deliberately SDK-independent.  A platform composition
must create one adapter, callback drain worker and Journal transport per
``AccountSessionShardBinding``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import blake2s
from threading import RLock
from typing import Mapping

from xuntrade_omsems.compute import session_shards

from ..domain import (
    BrokerEvent,
    BrokerEventKind,
    CancelRequest,
    ExecutionInstruction,
    VenueOrderIdentity,
)
from ..domain._validation import require_positive, require_sha256, require_text
from .ownership import AccountFenceScope, canonical_account_scope_hash
from .scheduler import ExecutionScheduler


class AccountSessionShardingError(RuntimeError):
    """A session-shard plan or native identity is not exact and safe to route."""


@dataclass(frozen=True)
class AccountSessionShardBinding:
    """One attested native session lane for a single logical account.

    ``adapter_client_scope`` must name the exact independently signed EMT
    client scope.  It is intentionally part of both the plan and every
    returned ``VenueOrderIdentity``; this prevents a cancel or callback from
    moving to another live session merely because it has the same account.
    """

    shard_id: str
    account_id: str
    venue_id: str
    adapter_instance_id: str
    adapter_client_scope: str
    ownership_scope_hash: str
    mapping_manifest_hash: str
    capability_manifest_hash: str
    journal_plan_hash: str
    journal_routing_epoch: int
    journal_shard_id: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("shard_id", self.shard_id),
            ("account_id", self.account_id),
            ("venue_id", self.venue_id),
            ("adapter_instance_id", self.adapter_instance_id),
            ("adapter_client_scope", self.adapter_client_scope),
        ):
            require_text(value, f"account session shard {field_name}", max_length=256)
        require_positive(
            self.journal_routing_epoch,
            "account session shard journal_routing_epoch",
        )
        if self.journal_shard_id < 0:
            raise AccountSessionShardingError("journal_shard_id must be nonnegative")
        for field_name, value in (
            ("ownership_scope_hash", self.ownership_scope_hash),
            ("mapping_manifest_hash", self.mapping_manifest_hash),
            ("capability_manifest_hash", self.capability_manifest_hash),
            ("journal_plan_hash", self.journal_plan_hash),
        ):
            require_sha256(value, f"account session shard {field_name}")
        expected_scope_hash = canonical_account_scope_hash(
            AccountFenceScope(
                account_id=self.account_id,
                venue_id=self.venue_id,
                adapter_client_scope=self.adapter_client_scope,
            )
        )
        if self.ownership_scope_hash != expected_scope_hash:
            raise AccountSessionShardingError(
                "session shard ownership scope does not match account, venue and Adapter scope"
            )


@dataclass(frozen=True)
class AccountSessionShardPlan:
    """Immutable same-account session topology with no implicit failover."""

    plan_id: str
    version: int
    plan_hash: str
    account_id: str
    shards: tuple[AccountSessionShardBinding, ...]

    def __post_init__(self) -> None:
        require_text(self.plan_id, "account session shard plan_id")
        require_positive(self.version, "account session shard plan version")
        require_sha256(self.plan_hash, "account session shard plan_hash")
        require_text(self.account_id, "account session shard account_id")
        if not self.shards:
            raise AccountSessionShardingError("account session shard plan is empty")
        if len(self.shards) > 32:
            raise AccountSessionShardingError("account session shard plan exceeds 32 scopes")
        if any(shard.account_id != self.account_id for shard in self.shards):
            raise AccountSessionShardingError("session shard plan crosses logical accounts")
        for values, description in (
            (tuple(shard.shard_id for shard in self.shards), "shard_id"),
            (
                tuple(
                    (
                        shard.venue_id,
                        shard.adapter_instance_id,
                        shard.adapter_client_scope,
                    )
                    for shard in self.shards
                ),
                "Adapter native scope",
            ),
            (tuple(shard.ownership_scope_hash for shard in self.shards), "ownership scope"),
        ):
            if len(values) != len(set(values)):
                raise AccountSessionShardingError(
                    f"account session shard plan repeats {description}"
                )
        if (
            len({shard.journal_plan_hash for shard in self.shards}) != 1
            or len({shard.journal_routing_epoch for shard in self.shards}) != 1
        ):
            raise AccountSessionShardingError(
                "all session shards must use one Journal plan and routing epoch"
            )

    def shard_for_execution_id(
        self, *, account_id: str, execution_id: str
    ) -> AccountSessionShardBinding:
        """Select the same immutable scope before an execution plan is materialized."""

        if account_id != self.account_id:
            raise AccountSessionShardingError(
                "execution belongs to another account session-shard plan"
            )
        digest = blake2s(
            require_text(execution_id, "session-shard execution_id").encode("utf-8"),
            digest_size=16,
            person=b"tcshard1",
        ).digest()
        index = int.from_bytes(digest, "big") % len(self.shards)
        return self.shards[index]

    def shards_for_execution_ids(
        self, *, account_id: str, execution_ids: tuple[str, ...], workers: int = 1
    ) -> tuple[AccountSessionShardBinding, ...]:
        """Batch the v1 route calculation in C++ against this exact account plan."""
        if account_id != self.account_id:
            raise AccountSessionShardingError(
                "execution belongs to another account session-shard plan"
            )
        if type(execution_ids) is not tuple or len(execution_ids) > 1_000_000:
            raise AccountSessionShardingError("execution IDs require a bounded tuple")
        for execution_id in execution_ids:
            require_text(execution_id, "session-shard execution_id")
        return tuple(
            self.shards[index]
            for index in session_shards(execution_ids, len(self.shards), workers=workers)
        )


@dataclass(frozen=True)
class AccountSessionShardLane:
    """The chosen shard plus its isolated pre-native scheduler."""

    binding: AccountSessionShardBinding
    scheduler: ExecutionScheduler


@dataclass(frozen=True)
class AccountSessionShardCallbackRoute:
    """One exact callback ownership decision plus a target-scope event view."""

    binding: AccountSessionShardBinding
    event: BrokerEvent


class AccountSessionShardCallbackRouter:
    """Demultiplex a shared EMT callback feed by pre-registered client reference.

    EMT's Python SDK may deliver all callbacks for one account through one of
    several concurrently logged-in client sessions.  The source session scope
    in that delivery is therefore not proof of the submitting session.  This
    router accepts a callback only when its client reference was registered
    before the native call, then rewrites *only the local adapter scope* to
    the already-attested target scope.  All order identity and state checks
    remain the responsibility of the target session's existing callback
    Journal composition.  Unknown callbacks have no owned route and return
    ``None``; there is never a fallback lane.
    """

    def __init__(self, *, plan: AccountSessionShardPlan) -> None:
        self._plan = plan
        self._bindings_by_id = {binding.shard_id: binding for binding in plan.shards}
        self._bindings_by_client_ref: dict[str, AccountSessionShardBinding] = {}
        self._sealed_bindings_by_client_ref: Mapping[str, AccountSessionShardBinding] | None = None
        self._lock = RLock()

    def register_pre_native(
        self, *, binding: AccountSessionShardBinding, client_order_ref: str
    ) -> None:
        """Reserve one exactly owned client reference before the SDK call."""

        require_text(client_order_ref, "session-shard callback client_order_ref", max_length=256)
        expected = self._bindings_by_id.get(binding.shard_id)
        if expected != binding:
            raise AccountSessionShardingError(
                "callback registration binding is absent from the session-shard plan"
            )
        with self._lock:
            if self._sealed_bindings_by_client_ref is not None:
                raise AccountSessionShardingError(
                    "callback registration is closed after the session-shard router is sealed"
                )
            existing = self._bindings_by_client_ref.get(client_order_ref)
            if existing is not None and existing != binding:
                raise AccountSessionShardingError(
                    "callback client reference is already registered to another session shard"
                )
            self._bindings_by_client_ref[client_order_ref] = binding

    def seal(self) -> int:
        """Freeze registrations before native callback workers are started.

        A burst pre-registers every owned client reference before the first
        adapter starts.  Copying that table once makes the callback lookup a
        lock-free read on CPython's immutable post-start topology while keeping
        registration errors explicit and fail-closed.
        """

        with self._lock:
            if self._sealed_bindings_by_client_ref is None:
                self._sealed_bindings_by_client_ref = dict(self._bindings_by_client_ref)
            return len(self._sealed_bindings_by_client_ref)

    def route_event(self, event: BrokerEvent) -> AccountSessionShardCallbackRoute | None:
        """Return the sole registered target for an order/trade callback.

        A callback without a usable client reference cannot prove ownership to
        this process, so it is intentionally not converted into a target event.
        """

        if event.event_kind not in {BrokerEventKind.ORDER, BrokerEventKind.TRADE}:
            return None
        if event.account_id != self._plan.account_id:
            raise AccountSessionShardingError(
                "BrokerEvent belongs to another account session-shard plan"
            )
        identity = event.venue_order_identity
        if identity is None or identity.client_order_ref is None:
            return None
        sealed = self._sealed_bindings_by_client_ref
        if sealed is None:
            # Startup/recovery callers may route before their plan is sealed,
            # but live callback workers always use the immutable fast path.
            with self._lock:
                binding = self._bindings_by_client_ref.get(identity.client_order_ref)
        else:
            binding = sealed.get(identity.client_order_ref)
        if binding is None:
            return None
        if (
            event.venue_id != binding.venue_id
            or event.adapter_instance_id != binding.adapter_instance_id
            or identity.venue_id != binding.venue_id
            or identity.adapter_instance_id != binding.adapter_instance_id
        ):
            raise AccountSessionShardingError(
                "callback venue identity does not match its registered session shard"
            )
        rebound_identity = replace(identity, adapter_client_scope=binding.adapter_client_scope)
        return AccountSessionShardCallbackRoute(
            binding=binding,
            event=replace(event, venue_order_identity=rebound_identity),
        )


class AccountSessionShardRouter:
    """Route one account across fixed native client scopes deterministically.

    The byte-stable BLAKE2s selection avoids CPython's randomized ``hash()``.
    A plan change is an explicit control-plane change because it has a new
    plan hash/version; callers must not silently add or remove live scopes.
    """

    def __init__(
        self,
        *,
        plan: AccountSessionShardPlan,
        schedulers: Mapping[str, ExecutionScheduler],
    ) -> None:
        expected_ids = {shard.shard_id for shard in plan.shards}
        if set(schedulers) != expected_ids:
            raise AccountSessionShardingError(
                "scheduler shard ids must exactly match the session shard plan"
            )
        copied = dict(schedulers)
        for shard_id, scheduler in copied.items():
            if scheduler.account_id != plan.account_id:
                raise AccountSessionShardingError(
                    f"scheduler for session shard {shard_id} has another account"
                )
        self._plan = plan
        self._shards = tuple(plan.shards)
        self._lanes_by_id = {
            shard.shard_id: AccountSessionShardLane(shard, copied[shard.shard_id])
            for shard in self._shards
        }
        self._lanes_by_native_scope = {
            (
                shard.venue_id,
                shard.adapter_instance_id,
                shard.adapter_client_scope,
            ): self._lanes_by_id[shard.shard_id]
            for shard in self._shards
        }

    @property
    def plan(self) -> AccountSessionShardPlan:
        return self._plan

    def route_execution(self, instruction: ExecutionInstruction) -> AccountSessionShardLane:
        """Select one fixed session lane from the immutable execution identity."""

        return self.route_execution_id(
            account_id=instruction.account_id,
            execution_id=instruction.execution_id,
        )

    def route_execution_id(self, *, account_id: str, execution_id: str) -> AccountSessionShardLane:
        """Select a lane before the immutable execution plan is materialized."""

        binding = self._plan.shard_for_execution_id(
            account_id=account_id,
            execution_id=execution_id,
        )
        return self._lanes_by_id[binding.shard_id]

    def route_cancel(
        self,
        request: CancelRequest,
        venue_order_identity: VenueOrderIdentity,
    ) -> AccountSessionShardLane:
        if request.account_id != self._plan.account_id:
            raise AccountSessionShardingError(
                "cancel belongs to another account session-shard plan"
            )
        return self._lane_for_native_identity(venue_order_identity)

    def route_execution_ids(
        self, *, account_id: str, execution_ids: tuple[str, ...], workers: int = 1
    ) -> tuple[AccountSessionShardLane, ...]:
        """Resolve a batch against this exact immutable account plan."""
        return tuple(
            self._lanes_by_id[binding.shard_id]
            for binding in self._plan.shards_for_execution_ids(
                account_id=account_id, execution_ids=execution_ids, workers=workers
            )
        )

    def route_event(self, event: BrokerEvent) -> AccountSessionShardLane:
        if event.account_id != self._plan.account_id:
            raise AccountSessionShardingError(
                "BrokerEvent belongs to another account session-shard plan"
            )
        identity = event.venue_order_identity
        if identity is None:
            raise AccountSessionShardingError(
                "BrokerEvent lacks the native identity required for session-shard routing"
            )
        if (
            event.venue_id != identity.venue_id
            or event.adapter_instance_id != identity.adapter_instance_id
        ):
            raise AccountSessionShardingError(
                "BrokerEvent venue identity differs from its native identity"
            )
        return self._lane_for_native_identity(identity)

    def _lane_for_native_identity(self, identity: VenueOrderIdentity) -> AccountSessionShardLane:
        key = (
            identity.venue_id,
            identity.adapter_instance_id,
            identity.adapter_client_scope,
        )
        try:
            return self._lanes_by_native_scope[key]
        except KeyError as exc:
            raise AccountSessionShardingError(
                "native identity is absent from the account session-shard plan"
            ) from exc
