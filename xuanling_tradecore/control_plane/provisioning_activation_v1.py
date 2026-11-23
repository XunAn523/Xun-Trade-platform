"""Binding publication and exact Adapter runtime lookup for Provisioning V1."""

from __future__ import annotations

from dataclasses import replace
from threading import RLock
from typing import Callable

from ..adapters import BrokerAdapter
from ..consumer.bindings_v2 import ConsumerBindingSnapshotV2
from ..domain import ConsumerAccountBinding, ConsumerStrategyBinding
from .provisioning_contracts_v1 import (
    AdapterProfileRevisionV1,
    BindingActivationSpecV1,
    ProfileStateV1,
    ProvisioningErrorV1,
)


class ConsumerBindingSnapshotPublisherV1:
    """Atomic in-process source of Consumer V2 binding snapshots.

    Persistent binding evidence belongs to the provisioning registry; this
    object publishes an immutable live projection only after activation.
    """

    def __init__(self, initial: ConsumerBindingSnapshotV2 | None = None) -> None:
        self._snapshot = initial or ConsumerBindingSnapshotV2(
            authorization_epoch=1, accounts=(), strategies=()
        )
        self._lock = RLock()

    def current(self) -> ConsumerBindingSnapshotV2:
        with self._lock:
            return self._snapshot

    def publish(
        self,
        *,
        authorization_epoch: int,
        profile: AdapterProfileRevisionV1,
        spec: BindingActivationSpecV1,
        binding_revision_id: str,
    ) -> ConsumerBindingSnapshotV2:
        """Append one exact account binding and strategy set at a newer epoch."""

        if profile.state not in {ProfileStateV1.VALIDATED, ProfileStateV1.ACTIVE_DISARMED}:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "profile cannot publish binding")
        with self._lock:
            current = self._snapshot
            existing_accounts = [
                item for item in current.accounts if item.binding_id == binding_revision_id
            ]
            if existing_accounts:
                existing = existing_accounts[0]
                existing_strategy_ids = tuple(
                    item.strategy_id
                    for item in current.strategies
                    if item.consumer_account_binding_id == binding_revision_id
                )
                if (
                    len(existing_accounts) == 1
                    and existing.active
                    and existing.binding_version == profile.revision
                    and existing.consumer_id == spec.consumer_id
                    and existing.consumer_account_id == spec.consumer_account_id
                    and existing.account_id == spec.account_id
                    and existing.account_display_name == spec.account_display_name
                    and existing.venue_id == profile.identity.venue_id
                    and existing.adapter_instance_id == profile.identity.adapter_instance_id
                    and existing_strategy_ids == spec.strategy_ids
                    and current.authorization_epoch == authorization_epoch
                ):
                    return current
                raise ProvisioningErrorV1("CONTROL_BINDING", "binding revision conflicts")
            if authorization_epoch <= current.authorization_epoch:
                raise ProvisioningErrorV1("CONTROL_BINDING", "binding epoch is not monotonic")
            if any(
                item.consumer_id == spec.consumer_id
                and item.consumer_account_id == spec.consumer_account_id
                and item.active
                for item in current.accounts
            ):
                raise ProvisioningErrorV1("CONTROL_BINDING", "consumer account is already active")
            account = ConsumerAccountBinding(
                binding_id=binding_revision_id,
                binding_version=profile.revision,
                consumer_id=spec.consumer_id,
                consumer_account_id=spec.consumer_account_id,
                account_id=spec.account_id,
                account_display_name=spec.account_display_name,
                venue_id=profile.identity.venue_id,
                adapter_instance_id=profile.identity.adapter_instance_id,
            )
            strategies = tuple(
                ConsumerStrategyBinding(
                    binding_id=f"{binding_revision_id}:strategy:{index}",
                    binding_version=profile.revision,
                    consumer_id=spec.consumer_id,
                    consumer_account_id=spec.consumer_account_id,
                    consumer_account_binding_id=binding_revision_id,
                    strategy_id=strategy_id,
                )
                for index, strategy_id in enumerate(spec.strategy_ids, start=1)
            )
            self._snapshot = ConsumerBindingSnapshotV2(
                authorization_epoch=authorization_epoch,
                accounts=current.accounts + (account,),
                strategies=current.strategies + strategies,
            )
            return self._snapshot

    def retire(
        self,
        *,
        authorization_epoch: int,
        binding_revision_id: str,
    ) -> ConsumerBindingSnapshotV2:
        """Publish a successor where exactly one account and its strategies are inactive."""

        with self._lock:
            current = self._snapshot
            if authorization_epoch <= current.authorization_epoch:
                raise ProvisioningErrorV1("CONTROL_BINDING", "binding epoch is not monotonic")
            selected = [item for item in current.accounts if item.binding_id == binding_revision_id]
            if len(selected) != 1 or not selected[0].active:
                raise ProvisioningErrorV1("CONTROL_BINDING", "active binding was not found")
            accounts = tuple(
                replace(item, active=False) if item.binding_id == binding_revision_id else item
                for item in current.accounts
            )
            strategies = tuple(
                replace(item, active=False)
                if item.consumer_account_binding_id == binding_revision_id
                else item
                for item in current.strategies
            )
            self._snapshot = ConsumerBindingSnapshotV2(
                authorization_epoch=authorization_epoch,
                accounts=accounts,
                strategies=strategies,
            )
            return self._snapshot


class AdapterRuntimeRegistryV1:
    """Resolve a Consumer binding to one already-constructed exact Adapter.

    It deliberately cannot resolve by host, account alias, or a strategy input.
    A caller must provide the binding already authorized by Consumer V2.
    """

    def __init__(self) -> None:
        self._adapters: dict[tuple[str, str], BrokerAdapter] = {}
        self._lock = RLock()

    def register(self, *, profile: AdapterProfileRevisionV1, adapter: BrokerAdapter) -> None:
        if profile.state is not ProfileStateV1.ACTIVE_DISARMED:
            raise ProvisioningErrorV1("CONTROL_PROFILE_STATE", "runtime profile is not active")
        if (
            adapter.adapter_instance_id != profile.identity.adapter_instance_id
            or adapter.capabilities.venue_id != profile.identity.venue_id
        ):
            raise ProvisioningErrorV1("CONTROL_RUNTIME", "Adapter does not match profile identity")
        key = (profile.identity.venue_id, profile.identity.adapter_instance_id)
        with self._lock:
            if key in self._adapters:
                raise ProvisioningErrorV1(
                    "CONTROL_RUNTIME", "Adapter runtime is already registered"
                )
            self._adapters[key] = adapter

    def resolve(self, binding: ConsumerAccountBinding) -> BrokerAdapter:
        key = (binding.venue_id, binding.adapter_instance_id)
        with self._lock:
            try:
                return self._adapters[key]
            except KeyError as exc:
                raise ProvisioningErrorV1(
                    "CONTROL_RUNTIME", "Adapter runtime is unavailable"
                ) from exc

    def detach(self, binding: ConsumerAccountBinding) -> None:
        key = (binding.venue_id, binding.adapter_instance_id)
        with self._lock:
            self._adapters.pop(key, None)


ProfileSelectionGateV1 = Callable[[AdapterProfileRevisionV1], None]
