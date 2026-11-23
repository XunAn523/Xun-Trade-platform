"""Shared, Redis-free support for controlled EMT mmap performance probes."""

from __future__ import annotations

import base64
import json
import os
import secrets
import time
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from emt_gateway.session import SessionError, normalize_emt_trading_day
from emt_gateway.tradecore_mmap_journal import EmtCancelRoute, EmtSubmitRoute
from xuanling_tradecore.adapters import (
    CancelExecutionInstruction,
    canonical_venue_order_target_hash,
)
from xuanling_tradecore.adapters.emt import (
    EmtEd25519ScopeOwnershipVerifier,
    EmtSignedScopeOwnership,
    load_builtin_emt_legacy_sim_manifest,
)
from xuanling_tradecore.adapters.emt.scope_ownership import canonical_scope_ownership_payload
from xuanling_tradecore.domain import (
    CancelRequest,
    ConsumerTrace,
    DecisionContext,
    ExecutionInstruction,
    OmsOrder,
    OmsOrderState,
    OrderOrigin,
    VenueOrderIdentity,
)
from xuanling_tradecore.oms import (
    OmsFactKind,
    OmsJournalFact,
    OmsRecoverySeedV1,
    OrderBookOrder,
    Reservation,
    ReservationRequirement,
    ReservationStatus,
)
from xuanling_tradecore.persistence import (
    CancelRouteCommand,
    MmapAdmissionEnvelopeV1,
    PreNativeRouteCommand,
)

from .controlled_order_fixture import ControlledOrderFixtureError, load_controlled_order_fixture


class TradeCoreMmapEmtPerfError(RuntimeError):
    """A controlled mmap performance probe cannot safely continue."""


@dataclass(frozen=True)
class TightBurstOrderPlan:
    """Validated input for one 499-order probe.

    The gateway deliberately does not load the EMT Quote API.  A strategy or
    external read-only market process chooses the candidate before C0, or an
    already accepted controlled fixture is explicitly selected for a
    historical-comparability run.  Only request fields are returned to the
    benchmark in memory; results retain hashes, never ticker, price, limits,
    funds or quote content.
    """

    request: dict[str, Any]
    plan_sha256: str
    selection_mode: str
    source_fixture_sha256: str | None


_TIGHT_BURST_PLAN_COMMON_KEYS = frozenset(
    {
        "schema_version",
        "purpose",
        "environment",
        "selection_mode",
        "expires_at_unix_s",
        "ticker",
        "market",
        "side",
        "price_type",
        "price",
        "quantity",
        "price_tick",
        "max_total_buy_notional",
    }
)

_TIGHT_BURST_LIVE_PRECHECK_KEYS = frozenset(
    {
        "best_bid_price",
        "last_price",
        "lower_limit_price",
        "upper_limit_price",
    }
)

_TIGHT_BURST_HISTORICAL_FIXTURE_KEYS = frozenset(
    {
        "price_min",
        "price_max",
        "source_fixture_sha256",
    }
)


def load_tight_burst_order_plan(
    path: Path, *, burst_width: int, now_unix_s: int | None = None
) -> TightBurstOrderPlan:
    """Load one validated plan without adding a Quote API path.

    The plan is validated before the C0 timing window and is never embedded in
    a benchmark report.  ``external-read-only-market-preflight`` requires a
    fresh SSE quote-derived passive buy.  ``historical-controlled-fixture``
    deliberately preserves a previously accepted simulated test request and
    carries its fixture fingerprint; it is not evidence of current quotes.
    """

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TradeCoreMmapEmtPerfError("cannot read tight-burst order plan") from exc
    if not isinstance(raw, dict):
        raise TradeCoreMmapEmtPerfError("tight-burst order plan schema is invalid")
    if raw["schema_version"] != 1 or raw["purpose"] != "tradecore-tight-499-order-plan":
        raise TradeCoreMmapEmtPerfError("tight-burst order plan identity is invalid")
    if raw["environment"] != "simulated":
        raise TradeCoreMmapEmtPerfError("tight-burst order plan is not simulated")
    selection_mode = raw.get("selection_mode")
    if selection_mode == "external-read-only-market-preflight":
        required_keys = _TIGHT_BURST_PLAN_COMMON_KEYS | _TIGHT_BURST_LIVE_PRECHECK_KEYS
    elif selection_mode == "historical-controlled-fixture":
        required_keys = _TIGHT_BURST_PLAN_COMMON_KEYS | _TIGHT_BURST_HISTORICAL_FIXTURE_KEYS
    else:
        raise TradeCoreMmapEmtPerfError("tight-burst order plan selection mode is invalid")
    if set(raw) != required_keys:
        raise TradeCoreMmapEmtPerfError("tight-burst order plan schema is invalid")
    expires_at = raw["expires_at_unix_s"]
    if expires_at is None:
        # The user may keep a historical simulated-fixture comparison plan
        # indefinitely. A live-market preflight is deliberately never allowed
        # to opt out of expiry: its quote-derived passive price would become
        # misleading after the market changes.
        if selection_mode != "historical-controlled-fixture":
            raise TradeCoreMmapEmtPerfError("live tight-burst order plan requires an expiry")
    else:
        if type(expires_at) is not int or expires_at < 1:
            raise TradeCoreMmapEmtPerfError("tight-burst order plan expiry is invalid")
        now = int(time.time()) if now_unix_s is None else now_unix_s
        if type(now) is not int or now > expires_at:
            raise TradeCoreMmapEmtPerfError("tight-burst order plan has expired")
    ticker = raw["ticker"]
    if (
        not isinstance(ticker, str)
        or len(ticker) != 6
        or not ticker.isascii()
        or not ticker.isdigit()
    ):
        raise TradeCoreMmapEmtPerfError("tight-burst order plan ticker is invalid")
    if raw["market"] not in (1, 2) or raw["side"] != 1 or raw["price_type"] != 1:
        raise TradeCoreMmapEmtPerfError("tight-burst order plan must be a cash-equity limit buy")
    quantity = raw["quantity"]
    if type(quantity) is not int or quantity < 100 or quantity % 100:
        raise TradeCoreMmapEmtPerfError("tight-burst order quantity must be a positive board lot")
    values: dict[str, Decimal] = {}
    numeric_keys = {"price", "price_tick", "max_total_buy_notional"}
    if selection_mode == "external-read-only-market-preflight":
        numeric_keys |= _TIGHT_BURST_LIVE_PRECHECK_KEYS
    else:
        numeric_keys |= {"price_min", "price_max"}
    for key in numeric_keys:
        try:
            value = Decimal(str(raw[key]))
        except (InvalidOperation, ValueError) as exc:
            raise TradeCoreMmapEmtPerfError(
                "tight-burst order plan numeric field is invalid"
            ) from exc
        if not value.is_finite() or value <= 0:
            raise TradeCoreMmapEmtPerfError("tight-burst order plan numeric field is invalid")
        values[key] = value
    price, tick = values["price"], values["price_tick"]
    if tick != Decimal("0.01"):
        raise TradeCoreMmapEmtPerfError("tight-burst cash-equity tick must be 0.01")
    if selection_mode == "external-read-only-market-preflight":
        # This comparable live profile intentionally tests one SSE
        # cash-equity passive buy.  It must not silently change venue, side or
        # order type as part of a purported live-market preflight.
        if raw["market"] != 2:
            raise TradeCoreMmapEmtPerfError("live tight-burst plan must be an SSE limit buy")
        lower, upper = values["lower_limit_price"], values["upper_limit_price"]
        best_bid, last = values["best_bid_price"], values["last_price"]
        if lower >= upper or not lower < price < upper:
            raise TradeCoreMmapEmtPerfError("tight-burst price is outside the daily range")
        if price >= best_bid:
            raise TradeCoreMmapEmtPerfError("tight-burst price is not passive against best bid")
        if price - lower < tick * 5 or last - lower < tick * 10:
            raise TradeCoreMmapEmtPerfError("tight-burst candidate is too close to the lower limit")
    else:
        price_min, price_max = values["price_min"], values["price_max"]
        source_fixture_sha256 = raw["source_fixture_sha256"]
        if (
            not isinstance(source_fixture_sha256, str)
            or len(source_fixture_sha256) != 64
            or any(character not in "0123456789abcdef" for character in source_fixture_sha256)
        ):
            raise TradeCoreMmapEmtPerfError("historical tight-burst fixture fingerprint is invalid")
        if price_min > price_max or not price_min <= price <= price_max:
            raise TradeCoreMmapEmtPerfError(
                "historical tight-burst price is outside its fixture range"
            )
    if (price / tick) != (price / tick).to_integral_value():
        raise TradeCoreMmapEmtPerfError("tight-burst price is not tick aligned")
    if price * quantity * burst_width > values["max_total_buy_notional"]:
        raise TradeCoreMmapEmtPerfError("tight-burst plan exceeds its preflight buy-notional cap")
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return TightBurstOrderPlan(
        request={
            "ticker": ticker,
            "market": raw["market"],
            "side": 1,
            "price_type": 1,
            "price": float(price),
            "quantity": quantity,
            "business_type": 0,
        },
        plan_sha256=sha256(canonical).hexdigest(),
        selection_mode=selection_mode,
        source_fixture_sha256=(
            str(raw["source_fixture_sha256"])
            if selection_mode == "historical-controlled-fixture"
            else None
        ),
    )


def require_armed_simulated_fixture(path: Path) -> bytes:
    """Return an explicitly armed simulated fixture's exact source bytes.

    Every path that can call ``InsertOrder`` must pass this gate, including
    legacy fixture paths that do not use a materialized tight-burst plan.  A
    path that merely starts an EMT session deliberately bypasses it elsewhere;
    it cannot create an order.  Returning the bytes lets the tight-plan gate
    subsequently bind a reviewed plan to the very fixture that was armed.
    """

    try:
        raw_bytes = path.read_bytes()
        raw = json.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TradeCoreMmapEmtPerfError("cannot read tight-burst control fixture") from exc
    if not isinstance(raw, dict):
        raise TradeCoreMmapEmtPerfError("tight-burst control fixture is invalid")
    safety = raw.get("safety")
    if not isinstance(safety, dict) or safety.get("environment") != "simulated":
        raise TradeCoreMmapEmtPerfError("tight-burst control fixture is not simulated")
    if safety.get("armed") is not True:
        raise TradeCoreMmapEmtPerfError("tight-burst control fixture is not armed")
    return raw_bytes


def require_armed_tight_burst_fixture(path: Path, *, expected_sha256: str | None) -> None:
    """Prove an armed fixture is the exact reviewed source of a tight plan.

    A historical plan deliberately carries only its source digest into the
    timing report.  Before it can release a real simulated order, this gate
    re-reads the ignored fixture and requires both its explicit ``armed``
    switch and (when supplied) the exact source bytes that the plan was
    materialized from.  Session preflight never calls this function.
    """

    raw_bytes = require_armed_simulated_fixture(path)
    if expected_sha256 is not None:
        if (
            len(expected_sha256) != 64
            or any(character not in "0123456789abcdef" for character in expected_sha256)
            or sha256(raw_bytes).hexdigest() != expected_sha256
        ):
            raise TradeCoreMmapEmtPerfError(
                "tight-burst control fixture does not match the order-plan source"
            )


class _Milestones:
    """Persist payload-free lifecycle markers outside the measured window."""

    def __init__(self, path: Path | None, *, flush_on_mark: bool = False) -> None:
        self._path = path
        self._flush_on_mark = flush_on_mark
        self._entries: list[str] = []
        self._lock = Lock()

    def mark(self, stage: str) -> None:
        if self._path is None:
            return
        with self._lock:
            self._entries.append(f"{time.monotonic_ns()} {stage}\n")
        if self._flush_on_mark:
            self.flush()

    def flush(self) -> None:
        if self._path is None:
            return
        with self._lock:
            if not self._entries:
                return
            entries, self._entries = self._entries, []
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            handle.writelines(entries)


def _normalized_trading_day(value: str) -> str:
    try:
        return normalize_emt_trading_day(value)
    except SessionError as exc:
        raise TradeCoreMmapEmtPerfError(
            "trading day must be a real YYYY-MM-DD or YYYYMMDD calendar date"
        ) from exc


def _stop_adapter(adapter: Any, milestones: _Milestones) -> None:
    milestones.mark("adapter_stopping")
    try:
        adapter.stop()
    except BaseException:
        milestones.mark("adapter_stop_failed")
        raise
    milestones.mark("adapter_stopped")


def _exit_isolated_native_process(report: dict[str, Any]) -> None:
    summary = {"driver": report["driver"], "accepted": report["verification"]["accepted"]}
    os.write(1, (json.dumps(summary) + "\n").encode("utf-8"))
    os._exit(0 if report["verification"]["accepted"] else 2)


def _load_order_fixture(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    for item in raw.get("cases", []):
        if item.get("api") == "InsertOrder" and item.get("profile") == "ordinary":
            request = item.get("request")
            if isinstance(request, dict) and request:
                return dict(request)
    try:
        controlled = load_controlled_order_fixture(path)
        scenario = controlled.selected(("cancel_buy",))[0]
        return scenario.order_request(seed=controlled.safety.random_seed, cycle=0)
    except (ControlledOrderFixtureError, IndexError):
        return _legacy_controlled_scenario_request(raw)


def _legacy_controlled_scenario_request(raw: dict[str, Any]) -> dict[str, Any]:
    safety, scenarios = raw.get("safety"), raw.get("scenarios")
    if (
        not isinstance(safety, dict)
        or safety.get("environment") != "simulated"
        or not isinstance(scenarios, list)
    ):
        raise TradeCoreMmapEmtPerfError("controlled-order fixture is not simulated")
    candidate = next(
        (
            item
            for item in scenarios
            if isinstance(item, dict)
            and (item.get("id") == "cancel_buy" or item.get("objective") == "cancel")
        ),
        None,
    )
    if not isinstance(candidate, dict):
        raise TradeCoreMmapEmtPerfError("cancel cleanup scenario is unavailable")
    policy = candidate.get("price_policy")
    if (
        not isinstance(policy, dict)
        or type(candidate.get("ticker")) is not str
        or not candidate["ticker"].isascii()
        or not candidate["ticker"].isdigit()
        or type(candidate.get("market")) is not int
        or candidate["market"] not in {1, 2}
        or type(candidate.get("side")) is not int
        or candidate["side"] not in {1, 2}
        or candidate.get("price_type") != 1
        or type(candidate.get("quantity")) is not int
        or not 1 <= candidate["quantity"] <= 100
    ):
        raise TradeCoreMmapEmtPerfError("controlled-order scenario exceeds the test envelope")
    try:
        minimum = Decimal(str(policy["min"]))
        tick = Decimal(str(policy["tick"]))
    except (InvalidOperation, KeyError) as exc:
        raise TradeCoreMmapEmtPerfError("controlled-order price policy is invalid") from exc
    if not minimum.is_finite() or not tick.is_finite() or minimum <= 0 or tick <= 0:
        raise TradeCoreMmapEmtPerfError("controlled-order price policy is invalid")
    return {
        "ticker": candidate["ticker"],
        "market": candidate["market"],
        "side": candidate["side"],
        "price_type": candidate["price_type"],
        "price": float(minimum),
        "quantity": candidate["quantity"],
        "business_type": 0,
    }


def _activated_test_manifest(*, logical_account_id: str, client_id: int):
    base = load_builtin_emt_legacy_sim_manifest()
    ownership = base.scope_ownership
    assert ownership is not None
    signer = "tradecore-ephemeral-sim-perf-v1"
    test_ownership = replace(
        ownership,
        ownership_id="tradecore-emt-mmap-perf-ordinary-v1",
        account_id=logical_account_id,
        adapter_instance_id="emt-tradecore-mmap-perf-ordinary-v1",
        adapter_client_scope=str(client_id),
        owner_instance_id="fengyu",
        signer_id=signer,
        config_source_hash=sha256(b"tradecore-mmap-emt-perf").hexdigest(),
        uniqueness_probe_hash=sha256(b"tradecore-mmap-emt-perf-probe").hexdigest(),
        uniqueness_probe_succeeded=True,
    )
    key = Ed25519PrivateKey.generate()
    certificate_reference, revocation_reference = (
        "ephemeral://tradecore-mmap-perf",
        "ephemeral://none",
    )
    signed = EmtSignedScopeOwnership(
        ownership=test_ownership,
        certificate_reference=certificate_reference,
        revocation_reference=revocation_reference,
        signature_b64=base64.b64encode(
            key.sign(
                canonical_scope_ownership_payload(
                    test_ownership,
                    certificate_reference=certificate_reference,
                    revocation_reference=revocation_reference,
                )
            )
        ).decode("ascii"),
    )
    public = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    return (
        replace(base, scope_ownership=test_ownership),
        signed,
        EmtEd25519ScopeOwnershipVerifier({signer: public}),
    )


def _submit_plan(
    *,
    cycle: int,
    fixture: dict[str, Any],
    binding: Any,
    trading_day: Any,
    ownership: Any,
    client_order_ref: int | None = None,
    strategy_order_id: str | None = None,
    native_start_budget_ns: int = 5_000_000_000,
    execution_suffix: str | None = None,
):
    if client_order_ref is not None and (
        type(client_order_ref) is not int or not 1 <= client_order_ref <= 900_000_000
    ):
        raise TradeCoreMmapEmtPerfError("client order reference is outside the safe test range")
    if native_start_budget_ns <= 0:
        raise TradeCoreMmapEmtPerfError("native start budget must be positive")
    nonce = client_order_ref or (secrets.randbelow(800_000_000) + 100_000_000)
    suffix = execution_suffix or f"{time.monotonic_ns():x}-{cycle}"
    trace = ConsumerTrace(
        "perf", "perf", binding.account_id, None, strategy_order_id, "perf", 1, OrderOrigin.MANUAL
    )
    intent_hash = sha256(f"{suffix}:intent".encode()).hexdigest()
    instruction = ExecutionInstruction(
        execution_id=f"exec-{suffix}",
        oms_order_id=f"oms-{suffix}",
        account_id=binding.account_id,
        trading_day=trading_day,
        consumer_trace=trace,
        symbol=str(fixture["ticker"]),
        market="SZ" if fixture["market"] == 1 else "SH",
        side="BUY" if fixture["side"] == 1 else "SELL",
        price_type="LIMIT",
        business_type="CASH",
        quantity=int(fixture["quantity"]),
        protection_price_ticks=int(round(float(fixture["price"]) * 100)),
        limit_price_ticks=int(round(float(fixture["price"]) * 100)),
    )
    context = DecisionContext(
        decision_context_id=f"decision-{suffix}",
        account_id=binding.account_id,
        trading_day=trading_day,
        risk_snapshot_id=f"risk-{suffix}",
        risk_rule_bundle_hash=sha256(b"perf-risk").hexdigest(),
        protection_price_ticks=instruction.protection_price_ticks,
        fee_model_version="perf",
        margin_model_version="perf",
        reservation_model_version="perf",
        decided_monotonic_ns=time.monotonic_ns(),
        decision_bundle_hash=sha256(b"perf-decision").hexdigest(),
        decision_bundle_epoch=1,
    )
    requirement = ReservationRequirement(symbol=instruction.symbol, cash_minor=1)
    order = OrderBookOrder(
        order=OmsOrder(
            instruction.oms_order_id,
            binding.account_id,
            trading_day,
            trace,
            intent_hash,
            OrderOrigin.MANUAL,
            OmsOrderState.ACCEPTED,
            time.monotonic_ns(),
        ),
        execution=instruction,
        decision_context=context,
        reservation=Reservation(
            f"res-{suffix}",
            instruction.oms_order_id,
            binding.account_id,
            trading_day,
            context.risk_snapshot_id,
            context.decision_context_id,
            requirement,
            requirement,
            ReservationStatus.ACTIVE,
            0,
            time.monotonic_ns(),
        ),
    )
    facts = tuple(
        OmsJournalFact(
            f"{suffix}:{kind.value}",
            kind,
            binding.account_id,
            instruction.oms_order_id,
            instruction.execution_id,
            {"kind": kind.value},
        )
        for kind in (
            OmsFactKind.ORDER_ATTRIBUTED,
            OmsFactKind.RESERVATION_CREATED,
            OmsFactKind.CALL_STARTED,
        )
    )
    command = PreNativeRouteCommand(
        account_id=binding.account_id,
        command_id=f"cmd-{suffix}",
        execution_id=instruction.execution_id,
        oms_order_id=instruction.oms_order_id,
        client_order_ref=str(nonce),
        route_token_digest=sha256(f"route:{suffix}".encode()).hexdigest(),
        binding_manifest_hash=sha256(b"tradecore-mmap-emt-perf-binding").hexdigest(),
        intent_hash=intent_hash,
        decision_context_id=context.decision_context_id,
        native_start_deadline_ns=time.monotonic_ns() + native_start_budget_ns,
        fact_envelope=MmapAdmissionEnvelopeV1.build(
            account_id=binding.account_id,
            facts=facts,
            maximum_fact_count=128,
            maximum_encoded_bytes=1024 * 1024,
        ),
        recovery_seed=OmsRecoverySeedV1.build(order),
    )
    return instruction, EmtSubmitRoute(
        binding,
        command,
        VenueOrderIdentity(
            "EMT",
            ownership.adapter_instance_id,
            ownership.adapter_client_scope,
            client_order_ref=str(nonce),
        ),
    )


def _cancel_plan(*, instruction: Any, target: Any, target_route_payload_hash: str, binding: Any):
    suffix = instruction.execution_id.removeprefix("exec-")
    request = CancelRequest(
        f"cancel-{suffix}",
        instruction.oms_order_id,
        instruction.execution_id,
        binding.account_id,
        instruction.consumer_trace,
        "bounded simulated cleanup",
        time.monotonic_ns(),
    )
    facts = tuple(
        OmsJournalFact(
            f"{suffix}:{kind.value}",
            kind,
            binding.account_id,
            instruction.oms_order_id,
            instruction.execution_id,
            {"cancel_request_id": request.cancel_request_id},
        )
        for kind in (OmsFactKind.CANCEL_REQUESTED, OmsFactKind.CANCEL_STARTED)
    )
    command = CancelRouteCommand(
        account_id=binding.account_id,
        cancel_request_id=request.cancel_request_id,
        oms_order_id=instruction.oms_order_id,
        execution_id=instruction.execution_id,
        target_client_order_ref=target.client_order_ref or "",
        target_route_payload_hash=target_route_payload_hash,
        venue_order_identity_hash=canonical_venue_order_target_hash(target),
        native_start_deadline_ns=time.monotonic_ns() + 5_000_000_000,
        fact_envelope=MmapAdmissionEnvelopeV1.build(
            account_id=binding.account_id,
            facts=facts,
            maximum_fact_count=128,
            maximum_encoded_bytes=1024 * 1024,
        ),
    )
    return CancelExecutionInstruction(request, target), EmtCancelRoute(binding, command, target)
