"""Immutable local consumer binding snapshot and authorization checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from ..domain import ConsumerAccountBinding, ConsumerPrincipal, ConsumerStrategyBinding
from ..domain._validation import DomainValidationError, require_positive


class ConsumerBindingError(DomainValidationError):
    """A local consumer binding cannot authorize a data-plane request."""


@dataclass(frozen=True)
class ConsumerBindingSnapshotV2:
    authorization_epoch: int
    accounts: tuple[ConsumerAccountBinding, ...]
    strategies: tuple[ConsumerStrategyBinding, ...]
    _accounts_by_consumer: Mapping[str, tuple[ConsumerAccountBinding, ...]] = field(
        init=False, repr=False, compare=False
    )
    _account_by_external_id: Mapping[tuple[str, str], ConsumerAccountBinding] = field(
        init=False, repr=False, compare=False
    )
    _strategy_by_external_id: Mapping[tuple[str, str, str], ConsumerStrategyBinding] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        require_positive(self.authorization_epoch, "consumer binding authorization_epoch")
        accounts, strategies = tuple(self.accounts), tuple(self.strategies)
        if len({item.binding_id for item in accounts}) != len(accounts):
            raise ConsumerBindingError("consumer binding snapshot repeats account binding")
        if len({item.binding_id for item in strategies}) != len(strategies):
            raise ConsumerBindingError("consumer binding snapshot repeats strategy binding")
        account_by_id = {item.binding_id: item for item in accounts}
        for strategy in strategies:
            account = account_by_id.get(strategy.consumer_account_binding_id)
            if account is None or (strategy.consumer_id, strategy.consumer_account_id) != (
                account.consumer_id,
                account.consumer_account_id,
            ):
                raise ConsumerBindingError(
                    "consumer strategy binding has no matching account binding"
                )
        object.__setattr__(self, "accounts", accounts)
        object.__setattr__(self, "strategies", strategies)
        grouped: dict[str, list[ConsumerAccountBinding]] = {}
        external_accounts: dict[tuple[str, str], ConsumerAccountBinding] = {}
        for account in accounts:
            if account.active:
                grouped.setdefault(account.consumer_id, []).append(account)
                external_accounts.setdefault(
                    (account.consumer_id, account.consumer_account_id), account
                )
        object.__setattr__(
            self,
            "_accounts_by_consumer",
            MappingProxyType({key: tuple(value) for key, value in grouped.items()}),
        )
        object.__setattr__(
            self,
            "_account_by_external_id",
            MappingProxyType(external_accounts),
        )
        object.__setattr__(
            self,
            "_strategy_by_external_id",
            MappingProxyType(_first_active_strategy_by_external_id(strategies)),
        )

    def require_principal(self, principal: ConsumerPrincipal) -> None:
        if not principal.active or principal.consumer_id not in self._accounts_by_consumer:
            raise ConsumerBindingError("consumer principal is not active in binding snapshot")

    def accounts_for(self, consumer_id: str) -> tuple[ConsumerAccountBinding, ...]:
        return self._accounts_by_consumer.get(consumer_id, ())

    def require_account(
        self, *, consumer_id: str, consumer_account_id: str
    ) -> ConsumerAccountBinding:
        account = self._account_by_external_id.get((consumer_id, consumer_account_id))
        if account is None:
            raise ConsumerBindingError("consumer account is not authorized")
        return account

    def require_strategy(
        self, *, consumer_id: str, consumer_account_id: str, strategy_id: str
    ) -> ConsumerStrategyBinding:
        account = self._account_by_external_id.get((consumer_id, consumer_account_id))
        strategy = self._strategy_by_external_id.get(
            (consumer_id, consumer_account_id, strategy_id)
        )
        if (
            account is None
            or strategy is None
            or strategy.consumer_account_binding_id != account.binding_id
        ):
            raise ConsumerBindingError("consumer account or strategy is not authorized")
        return strategy


def _first_active_strategy_by_external_id(
    strategies: tuple[ConsumerStrategyBinding, ...],
) -> dict[tuple[str, str, str], ConsumerStrategyBinding]:
    result: dict[tuple[str, str, str], ConsumerStrategyBinding] = {}
    for item in strategies:
        if item.active:
            result.setdefault((item.consumer_id, item.consumer_account_id, item.strategy_id), item)
    return result
