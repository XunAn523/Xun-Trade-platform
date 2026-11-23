"""Immutable account-to-venue execution routing without profile failover."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..domain import BrokerEvent, CancelRequest, ExecutionInstruction, VenueOrderIdentity
from ..domain._validation import require_positive, require_sha256, require_text
from .ownership import AccountFenceScope, canonical_account_scope_hash
from .scheduler import ExecutionScheduler


class AccountExecutionRoutingError(RuntimeError):
    """An account, event or native identity cannot be routed exactly."""


@dataclass(frozen=True)
class AccountExecutionBinding:
    """One static account route fixed to a venue, Adapter scope and Journal plan."""

    binding_id: str
    binding_version: int
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
            ("binding_id", self.binding_id),
            ("account_id", self.account_id),
            ("venue_id", self.venue_id),
            ("adapter_instance_id", self.adapter_instance_id),
            ("adapter_client_scope", self.adapter_client_scope),
        ):
            require_text(value, f"account execution {field_name}", max_length=256)
        require_positive(self.binding_version, "account execution binding_version")
        require_positive(self.journal_routing_epoch, "account execution journal_routing_epoch")
        if self.journal_shard_id < 0:
            raise AccountExecutionRoutingError("journal_shard_id must be nonnegative")
        for field_name, value in (
            ("ownership_scope_hash", self.ownership_scope_hash),
            ("mapping_manifest_hash", self.mapping_manifest_hash),
            ("capability_manifest_hash", self.capability_manifest_hash),
            ("journal_plan_hash", self.journal_plan_hash),
        ):
            require_sha256(value, f"account execution {field_name}")
        expected_scope_hash = canonical_account_scope_hash(
            AccountFenceScope(
                account_id=self.account_id,
                venue_id=self.venue_id,
                adapter_client_scope=self.adapter_client_scope,
            )
        )
        if self.ownership_scope_hash != expected_scope_hash:
            raise AccountExecutionRoutingError(
                "binding ownership scope does not match account, venue and Adapter scope"
            )


@dataclass(frozen=True)
class AccountExecutionRoutePlan:
    """Hash-addressed static routes; profile switching is deliberately absent."""

    plan_id: str
    version: int
    plan_hash: str
    bindings: tuple[AccountExecutionBinding, ...]

    def __post_init__(self) -> None:
        require_text(self.plan_id, "account execution route plan_id")
        require_positive(self.version, "account execution route plan version")
        require_sha256(self.plan_hash, "account execution route plan_hash")
        if not self.bindings:
            raise AccountExecutionRoutingError("account execution route plan is empty")
        account_ids = tuple(binding.account_id for binding in self.bindings)
        binding_ids = tuple(binding.binding_id for binding in self.bindings)
        native_scopes = tuple(
            (
                binding.venue_id,
                binding.adapter_instance_id,
                binding.adapter_client_scope,
            )
            for binding in self.bindings
        )
        ownership_scopes = tuple(binding.ownership_scope_hash for binding in self.bindings)
        for values, description in (
            (account_ids, "account_id"),
            (binding_ids, "binding_id"),
            (native_scopes, "Adapter native scope"),
            (ownership_scopes, "ownership scope"),
        ):
            if len(values) != len(set(values)):
                raise AccountExecutionRoutingError(
                    f"account execution route plan repeats {description}"
                )
        if (
            len({binding.journal_plan_hash for binding in self.bindings}) != 1
            or len({binding.journal_routing_epoch for binding in self.bindings}) != 1
        ):
            raise AccountExecutionRoutingError(
                "all account execution routes must use one Journal plan and routing epoch"
            )

    def binding_for(self, account_id: str) -> AccountExecutionBinding:
        for binding in self.bindings:
            if binding.account_id == account_id:
                return binding
        raise AccountExecutionRoutingError("account is absent from the execution route plan")


@dataclass(frozen=True)
class AccountExecutionLane:
    """Resolved scheduler lane and immutable route evidence."""

    binding: AccountExecutionBinding
    scheduler: ExecutionScheduler


class AccountExecutionRouter:
    """Resolve orders, cancels and callbacks to one exact account lane."""

    def __init__(
        self,
        *,
        plan: AccountExecutionRoutePlan,
        schedulers: Mapping[str, ExecutionScheduler],
    ) -> None:
        expected_accounts = {binding.account_id for binding in plan.bindings}
        if set(schedulers) != expected_accounts:
            raise AccountExecutionRoutingError(
                "scheduler accounts must exactly match the execution route plan"
            )
        copied = dict(schedulers)
        for account_id, scheduler in copied.items():
            if scheduler.account_id != account_id:
                raise AccountExecutionRoutingError(
                    "scheduler account identity does not match its route key"
                )
        self._plan = plan
        self._bindings = {binding.account_id: binding for binding in plan.bindings}
        self._schedulers = copied

    @property
    def plan(self) -> AccountExecutionRoutePlan:
        return self._plan

    def route_execution(self, instruction: ExecutionInstruction) -> AccountExecutionLane:
        return self._lane_for_account(instruction.account_id)

    def route_cancel(
        self,
        request: CancelRequest,
        venue_order_identity: VenueOrderIdentity,
    ) -> AccountExecutionLane:
        lane = self._lane_for_account(request.account_id)
        self._require_native_identity(lane.binding, venue_order_identity)
        return lane

    def route_event(self, event: BrokerEvent) -> AccountExecutionLane:
        lane = self._lane_for_account(event.account_id)
        binding = lane.binding
        if (
            event.venue_id != binding.venue_id
            or event.adapter_instance_id != binding.adapter_instance_id
        ):
            raise AccountExecutionRoutingError(
                "BrokerEvent venue or Adapter instance does not match its account route"
            )
        if event.venue_order_identity is not None:
            self._require_native_identity(binding, event.venue_order_identity)
        return lane

    def _lane_for_account(self, account_id: str) -> AccountExecutionLane:
        try:
            binding = self._bindings[account_id]
            scheduler = self._schedulers[account_id]
        except KeyError as exc:
            raise AccountExecutionRoutingError(
                "account is absent from the static execution route plan"
            ) from exc
        return AccountExecutionLane(binding=binding, scheduler=scheduler)

    @staticmethod
    def _require_native_identity(
        binding: AccountExecutionBinding,
        identity: VenueOrderIdentity,
    ) -> None:
        if (
            identity.venue_id != binding.venue_id
            or identity.adapter_instance_id != binding.adapter_instance_id
            or identity.adapter_client_scope != binding.adapter_client_scope
        ):
            raise AccountExecutionRoutingError(
                "venue order identity does not match the account Adapter scope"
            )
