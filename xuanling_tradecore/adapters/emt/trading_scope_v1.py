"""Immutable product scopes for the EMT trading/account adapter.

The static EMT catalog deliberately remains complete: a product release may
disable a domain, but it must never make the corresponding SDK names vanish.
This module supplies the second, runtime-facing fence.  A scoped module checks
it before either a query lane or a mutation lane can reach the SDK owner.
"""

from __future__ import annotations

from dataclasses import dataclass

from .trading_catalog_v1 import (
    EmtTradingDomain,
    TraderApiSpecV1,
    TraderSpiSpecV1,
    trader_api_spec_v1,
    trader_spi_spec_v1,
)


class EmtTradingReleaseScopeError(PermissionError):
    """An otherwise catalogued EMT capability is disabled for this release."""


@dataclass(frozen=True)
class EmtTradingReleaseScopeV1:
    """Explicit allow-list of EMT domains exposed by one product release.

    The release identity is deliberately not an account permission.  It is a
    deployment-level boundary, while :mod:`trading_permissions_v1` still
    enforces the strategy's narrow permission grant within an enabled domain.
    """

    scope_id: str
    enabled_domains: frozenset[EmtTradingDomain]

    def __post_init__(self) -> None:
        if not self.scope_id or not self.enabled_domains:
            raise EmtTradingReleaseScopeError("release scope identity and domains are required")
        if not all(isinstance(domain, EmtTradingDomain) for domain in self.enabled_domains):
            raise EmtTradingReleaseScopeError("release scope contains an unknown EMT domain")

    def allows_api_spec(self, spec: TraderApiSpecV1) -> bool:
        return spec.domain in self.enabled_domains

    def allows_spi_spec(self, spec: TraderSpiSpecV1) -> bool:
        return spec.domain in self.enabled_domains

    def require_api(self, official_name: str) -> TraderApiSpecV1:
        spec = trader_api_spec_v1(official_name)
        if not self.allows_api_spec(spec):
            raise EmtTradingReleaseScopeError(
                "CAPABILITY_UNAVAILABLE: "
                f"{official_name} is disabled by release scope {self.scope_id}"
            )
        return spec

    def require_spi(self, official_name: str) -> TraderSpiSpecV1:
        spec = trader_spi_spec_v1(official_name)
        if not self.allows_spi_spec(spec):
            raise EmtTradingReleaseScopeError(
                "CAPABILITY_UNAVAILABLE: "
                f"{official_name} is disabled by release scope {self.scope_id}"
            )
        return spec


ALL_TRADING_RELEASE_SCOPE_V1 = EmtTradingReleaseScopeV1(
    scope_id="emt-all-trading-v1",
    enabled_domains=frozenset(EmtTradingDomain),
)


# Ordinary securities includes account/fund, DMA and public trading-reference
# queries.  Credit is enabled separately.  Option methods and callbacks remain
# catalogued but cannot reach the module in this product release.
ORDINARY_CREDIT_RELEASE_SCOPE_V1 = EmtTradingReleaseScopeV1(
    scope_id="emt-ordinary-credit-v1",
    enabled_domains=frozenset(
        {
            EmtTradingDomain.LIFECYCLE,
            EmtTradingDomain.SECURITIES,
            EmtTradingDomain.FUND,
            EmtTradingDomain.CREDIT,
            EmtTradingDomain.REFERENCE,
            EmtTradingDomain.DMA,
        }
    ),
)
