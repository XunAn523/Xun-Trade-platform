"""Payload-free F2 cash and margin capability-contract acceptance.

This probe proves only the local, fail-closed contract: the signed EMT
manifest can construct the documented cash, margin-buy and short-sell request
shapes, and a profile without a requested credit capability is rejected before
the native mutation port.  It deliberately does not log into an SDK session
or submit a credit order; a mapping declaration is not evidence that a venue
has accepted a financing transaction.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from emt_gateway.tradecore_order_request import EmtTradeCoreOrderRequestFactory
from xuanling_tradecore.adapters import (
    AccountTradingCapabilities,
    BrokerEventObserver,
    RuntimeVenueProfile,
)
from xuanling_tradecore.adapters.emt import (
    EmtAdapterProfile,
    EmtBrokerAdapter,
    EmtMutationPort,
    EmtSubmitReceipt,
    EmtSubmitRequest,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.domain import (
    ConsumerTrace,
    ExecutionInstruction,
    OrderOrigin,
    TradingDayEpoch,
)

# This is the manifest's logical account token, not a real account identifier.
_ACCOUNT_ID = "account-1"
_TRADING_DAY = TradingDayEpoch("2026-09-09")


class F2CapabilityContractError(RuntimeError):
    """The local F2 capability contract is not fail-closed."""


class _Observer(BrokerEventObserver):
    def on_broker_event(self, event: object) -> None:
        del event

    def on_callback_gap(self, evidence: object) -> None:
        del evidence


class _ReadOnlyPort:
    """No query is issued by this contract probe."""


@dataclass
class _CountingMutationPort(EmtMutationPort):
    submit_calls: int = 0
    cancel_calls: int = 0

    def submit(self, request: EmtSubmitRequest) -> EmtSubmitReceipt:
        del request
        self.submit_calls += 1
        return EmtSubmitReceipt(order_emt_id=1, accepted_monotonic_ns=1)

    def cancel(self, request: object) -> object:
        del request
        self.cancel_calls += 1
        raise AssertionError("F2 capability contract must not invoke cancel")


def _instruction(*, side: str, business_type: str, key: str) -> ExecutionInstruction:
    return ExecutionInstruction(
        execution_id=f"f2-capability-{key}",
        oms_order_id=f"f2-oms-{key}",
        account_id=_ACCOUNT_ID,
        trading_day=_TRADING_DAY,
        consumer_trace=ConsumerTrace(
            consumer_id="f2-contract",
            consumer_account_id=_ACCOUNT_ID,
            account_id=_ACCOUNT_ID,
            strategy_id="f2-capability-contract",
            strategy_order_id=f"f2-capability-{key}",
            attribution_binding_id="f2-capability-binding",
            binding_version=1,
            order_origin=OrderOrigin.MANUAL,
        ),
        symbol="000001",
        market="SZ",
        side=side,
        price_type="LIMIT",
        business_type=business_type,
        quantity=100,
        protection_price_ticks=1_000,
        limit_price_ticks=999,
    )


def _mapping(*, supports_margin_buy: bool, supports_short_sell: bool):
    manifest = load_builtin_emt_legacy_sim_manifest()
    ownership = manifest.scope_ownership
    if ownership is None:  # pragma: no cover - built-in manifest is versioned with ownership.
        raise F2CapabilityContractError("F2 manifest lacks scope ownership")
    return EmtAdapterProfile(manifest).activate(
        runtime=RuntimeVenueProfile(
            fingerprint=manifest.fingerprint,
            capabilities=manifest.capabilities,
            account_capabilities=AccountTradingCapabilities(
                _ACCOUNT_ID,
                supports_cash=True,
                supports_margin_buy=supports_margin_buy,
                supports_short_sell=supports_short_sell,
            ),
            scope_ownership=ownership,
            route_token_binding=manifest.route_token_binding,
            trusted_scope_signer_ids=frozenset({ownership.signer_id}),
            route_token_probe_succeeded=True,
        ),
        activated_at_unix_s=0,
    )


def _adapter(mapping, mutation_port: _CountingMutationPort) -> EmtBrokerAdapter:
    adapter = EmtBrokerAdapter(
        mapping,
        read_only_port=_ReadOnlyPort(),
        mutation_port=mutation_port,
        client_order_ref_factory=lambda _instruction: "17",
    )
    adapter.start(_Observer())
    return adapter


def run() -> dict[str, Any]:
    """Run the closed local capability matrix and return a sanitized result."""

    checks = {
        "cash_request_prepared": False,
        "margin_buy_request_prepared": False,
        "short_sell_request_prepared": False,
        "margin_buy_disabled_before_native": False,
        "short_sell_disabled_before_native": False,
        "native_mutation_port_unused": False,
    }
    enabled_port = _CountingMutationPort()
    disabled_port = _CountingMutationPort()
    enabled_adapter = _adapter(
        _mapping(supports_margin_buy=True, supports_short_sell=True), enabled_port
    )
    disabled_adapter = _adapter(
        _mapping(supports_margin_buy=False, supports_short_sell=False), disabled_port
    )
    try:
        factory = EmtTradeCoreOrderRequestFactory()
        cash = enabled_adapter.prepare_submit(
            _instruction(side="BUY", business_type="CASH", key="cash")
        )
        margin = enabled_adapter.prepare_submit(
            _instruction(side="MARGIN_BUY", business_type="MARGIN", key="margin")
        )
        short = enabled_adapter.prepare_submit(
            _instruction(side="SHORT_SELL", business_type="MARGIN", key="short")
        )
        cash_request = factory(cash.request)
        margin_request = factory(margin.request)
        short_request = factory(short.request)
        if (cash_request["side"], cash_request["business_type"]) != (1, 0):
            raise F2CapabilityContractError("cash mapping is not the reviewed request shape")
        checks["cash_request_prepared"] = True
        if (margin_request["side"], margin_request["business_type"]) != (21, 4):
            raise F2CapabilityContractError("margin-buy mapping is not the reviewed request shape")
        checks["margin_buy_request_prepared"] = True
        if (short_request["side"], short_request["business_type"]) != (22, 4):
            raise F2CapabilityContractError("short-sell mapping is not the reviewed request shape")
        checks["short_sell_request_prepared"] = True
        for instruction, check_name in (
            (
                _instruction(side="MARGIN_BUY", business_type="MARGIN", key="margin-disabled"),
                "margin_buy_disabled_before_native",
            ),
            (
                _instruction(side="SHORT_SELL", business_type="MARGIN", key="short-disabled"),
                "short_sell_disabled_before_native",
            ),
        ):
            try:
                disabled_adapter.submit(instruction)
            except ValueError:
                checks[check_name] = True
            else:
                raise F2CapabilityContractError(
                    "disabled credit capability reached a mutation port"
                )
        if (
            enabled_port.submit_calls
            or enabled_port.cancel_calls
            or disabled_port.submit_calls
            or disabled_port.cancel_calls
        ):
            raise F2CapabilityContractError("F2 capability contract invoked a native mutation port")
        checks["native_mutation_port_unused"] = True
    finally:
        enabled_adapter.stop()
        disabled_adapter.stop()
    return {
        "schema_version": "tradecore-f2-capability-contract-v1",
        "driver": "f2-capability-contract",
        "environment": "local-contract-only",
        "acceptance_status": "passed" if all(checks.values()) else "failed",
        "checks": checks,
        "native_sdk_loaded": False,
        "native_mutation_permitted": False,
        "native_mutation_calls": 0,
        "ready_claim": False,
        "automatic_recovery_claim": False,
        "payloads_retained": False,
        "credentials_retained": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"acceptance_status": report["acceptance_status"]}))
    return 0 if report["acceptance_status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
