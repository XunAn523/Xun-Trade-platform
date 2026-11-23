"""Strict local fixture for the four controlled 000010 order scenarios.

The existing catalog-complete mutation fixture intentionally has one command
per API/profile.  This separate, ignored fixture adds the one missing concept
needed for a real low-rate experiment: a named scenario with a deterministic
limit-price range.  It is deliberately narrow and rejects all other symbols,
quantities, prices, rates, and environments.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from emt_gateway.schema_validation import SchemaContractError, validate_schema_instance


class ControlledOrderFixtureError(RuntimeError):
    """A controlled real-order fixture is malformed or unsafe."""


_EXPECTED_SCENARIOS = {
    "fill_sell": ("fill", 2, Decimal("1.57"), Decimal("1.59"), "OnTradeEvent"),
    "fill_buy": ("fill", 1, Decimal("1.61"), Decimal("1.63"), "OnTradeEvent"),
    "cancel_buy": ("cancel", 1, Decimal("1.57"), Decimal("1.59"), "QueryOrderByEMTID"),
    "cancel_sell": ("cancel", 2, Decimal("1.61"), Decimal("1.63"), "QueryOrderByEMTID"),
}
_TICK = Decimal("0.01")
_MAX_SCENARIO_CYCLES = 10
_MAX_COMMANDS = 80


@dataclass(frozen=True)
class ControlledOrderSafety:
    armed: bool
    account_profile: str
    allowed_tickers: tuple[str, ...]
    random_seed: int
    min_command_interval_seconds: float
    cancel_timeout_seconds: float
    max_order_quantity: int
    max_total_submissions: int
    max_total_commands: int
    max_total_buy_notional: Decimal
    max_total_sell_quantity: int


@dataclass(frozen=True)
class PricePolicy:
    minimum: Decimal
    maximum: Decimal

    @property
    def slots(self) -> int:
        return int((self.maximum - self.minimum) / _TICK) + 1

    def sample(self, *, seed: int, scenario_id: str, cycle: int) -> float:
        digest = sha256(f"{seed}:{scenario_id}:{cycle}".encode("ascii")).digest()
        slot = int.from_bytes(digest[:8], "big") % self.slots
        return float(self.minimum + _TICK * slot)


@dataclass(frozen=True)
class ControlledOrderScenario:
    scenario_id: str
    objective: str
    ticker: str
    side: int
    quantity: int
    price_policy: PricePolicy
    cycles: int

    def order_request(self, *, seed: int, cycle: int) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "market": 1,
            "side": self.side,
            "price_type": 1,
            "price": self.price_policy.sample(seed=seed, scenario_id=self.scenario_id, cycle=cycle),
            "quantity": self.quantity,
            "business_type": 0,
        }


@dataclass(frozen=True)
class ControlledOrderFixture:
    safety: ControlledOrderSafety
    scenarios: tuple[ControlledOrderScenario, ...]

    def selected(
        self, scenario_ids: tuple[str, ...] | None = None
    ) -> tuple[ControlledOrderScenario, ...]:
        if scenario_ids is None:
            return self.scenarios
        wanted = set(scenario_ids)
        selected = tuple(scenario for scenario in self.scenarios if scenario.scenario_id in wanted)
        if len(selected) != len(wanted):
            unknown = ", ".join(sorted(wanted - {item.scenario_id for item in selected}))
            raise ControlledOrderFixtureError(f"unknown controlled-order scenario: {unknown}")
        return selected


def load_controlled_order_fixture(path: Path) -> ControlledOrderFixture:
    """Load and validate the local fixture without touching the SDK or EMT."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ControlledOrderFixtureError("cannot read local controlled-order fixture") from exc
    try:
        return _parse_fixture(raw)
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ControlledOrderFixtureError(str(exc)) from exc


def _parse_fixture(raw: object) -> ControlledOrderFixture:
    if not isinstance(raw, Mapping) or raw.get("schema_version") != 1:
        raise ValueError("controlled-order fixture schema_version must be 1")
    if raw.get("purpose") != "controlled simulated 000010 order latency experiment":
        raise ValueError("controlled-order fixture purpose is invalid")
    safety_raw, scenarios_raw = raw.get("safety"), raw.get("scenarios")
    if not isinstance(safety_raw, Mapping) or not isinstance(scenarios_raw, list):
        raise ValueError("controlled-order fixture must declare safety and scenarios")
    safety = _parse_safety(safety_raw)
    scenarios = tuple(_parse_scenario(item, safety=safety) for item in scenarios_raw)
    if {scenario.scenario_id for scenario in scenarios} != set(_EXPECTED_SCENARIOS) or len(
        scenarios
    ) != 4:
        raise ValueError(
            "controlled-order fixture must contain the four approved scenarios exactly once"
        )
    _validate_totals(safety, scenarios)
    try:
        validate_schema_instance("controlled-order-fixture-v1", raw)
    except SchemaContractError as exc:
        raise ValueError(str(exc)) from exc
    return ControlledOrderFixture(safety=safety, scenarios=scenarios)


def _parse_safety(raw: Mapping[str, object]) -> ControlledOrderSafety:
    required = {
        "armed",
        "environment",
        "account_profile",
        "allowed_tickers",
        "random_seed",
        "min_command_interval_seconds",
        "cancel_timeout_seconds",
        "max_order_quantity",
        "max_total_submissions",
        "max_total_commands",
        "max_total_buy_notional",
        "max_total_sell_quantity",
        "instructions",
    }
    if set(raw) != required:
        raise ValueError("controlled-order fixture safety keys are invalid")
    armed, environment, seed = raw["armed"], raw["environment"], raw["random_seed"]
    profile, allowed_tickers = raw["account_profile"], raw["allowed_tickers"]
    interval, cancel_timeout = raw["min_command_interval_seconds"], raw["cancel_timeout_seconds"]
    max_order_quantity = raw["max_order_quantity"]
    submissions, commands = raw["max_total_submissions"], raw["max_total_commands"]
    sell_quantity = raw["max_total_sell_quantity"]
    if type(armed) is not bool or environment != "simulated" or type(seed) is not int:
        raise ValueError(
            "controlled-order fixture must be a simulated fixture with a boolean armed flag and integer seed"
        )
    if profile != "ordinary":
        raise ValueError("controlled-order fixture account_profile must be ordinary")
    if not isinstance(allowed_tickers, list) or allowed_tickers != ["000010"]:
        raise ValueError(
            "controlled-order fixture allowed_tickers must be the approved 000010 whitelist"
        )
    if type(interval) not in {int, float} or float(interval) < 1.0:
        raise ValueError(
            "controlled-order fixture requires a command interval of at least one second"
        )
    if type(cancel_timeout) not in {int, float} or not 3.0 <= float(cancel_timeout) <= 30.0:
        raise ValueError("controlled-order fixture cancel_timeout_seconds must be in 3..30")
    if type(max_order_quantity) is not int or max_order_quantity != 100:
        raise ValueError(
            "controlled-order fixture max_order_quantity must be the approved 100 shares"
        )
    if type(submissions) is not int or type(commands) is not int or type(sell_quantity) is not int:
        raise ValueError("controlled-order fixture command and quantity caps must be integers")
    if submissions < 1 or commands < submissions or commands > _MAX_COMMANDS or sell_quantity < 0:
        raise ValueError("controlled-order fixture caps are unsafe")
    buy_notional = _money(raw["max_total_buy_notional"], "max_total_buy_notional")
    if buy_notional <= 0:
        raise ValueError("controlled-order fixture maximum buy notional must be positive")
    return ControlledOrderSafety(
        armed=armed,
        account_profile=profile,
        allowed_tickers=tuple(allowed_tickers),
        random_seed=seed,
        min_command_interval_seconds=float(interval),
        cancel_timeout_seconds=float(cancel_timeout),
        max_order_quantity=max_order_quantity,
        max_total_submissions=submissions,
        max_total_commands=commands,
        max_total_buy_notional=buy_notional,
        max_total_sell_quantity=sell_quantity,
    )


def _parse_scenario(raw: object, *, safety: ControlledOrderSafety) -> ControlledOrderScenario:
    if not isinstance(raw, Mapping):
        raise ValueError("controlled-order scenarios must be objects")
    required = {
        "id",
        "objective",
        "ticker",
        "market",
        "side",
        "price_type",
        "quantity",
        "cycles",
        "rate_limit_per_second",
        "price_policy",
        "reconciliation",
        "notes",
    }
    if set(raw) != required:
        raise ValueError("controlled-order scenario keys are invalid")
    scenario_id, objective, side = raw["id"], raw["objective"], raw["side"]
    expected = _EXPECTED_SCENARIOS.get(scenario_id) if isinstance(scenario_id, str) else None
    if expected is None:
        raise ValueError("controlled-order scenario id is not approved")
    expected_objective, expected_side, minimum, maximum, reconciliation = expected
    if (
        objective != expected_objective
        or side != expected_side
        or raw["ticker"] not in safety.allowed_tickers
        or raw["market"] != 1
        or raw["price_type"] != 1
        or raw["quantity"] != safety.max_order_quantity
        or raw["rate_limit_per_second"] != 1
        or raw["reconciliation"] != reconciliation
    ):
        raise ValueError(f"controlled-order scenario {scenario_id} exceeds its approved envelope")
    cycles = raw["cycles"]
    if type(cycles) is not int or not 1 <= cycles <= _MAX_SCENARIO_CYCLES:
        raise ValueError("controlled-order scenario cycles must be in 1..10")
    policy_raw = raw["price_policy"]
    if not isinstance(policy_raw, Mapping) or set(policy_raw) != {"min", "max", "tick"}:
        raise ValueError("controlled-order scenario price_policy is invalid")
    policy = PricePolicy(
        minimum=_money(policy_raw["min"], "price_policy.min"),
        maximum=_money(policy_raw["max"], "price_policy.max"),
    )
    tick = _money(policy_raw["tick"], "price_policy.tick")
    if policy.minimum != minimum or policy.maximum != maximum or tick != _TICK:
        raise ValueError(f"controlled-order scenario {scenario_id} price range is not approved")
    return ControlledOrderScenario(
        scenario_id=scenario_id,
        objective=expected_objective,
        ticker=str(raw["ticker"]),
        side=expected_side,
        quantity=int(raw["quantity"]),
        price_policy=policy,
        cycles=cycles,
    )


def _validate_totals(
    safety: ControlledOrderSafety, scenarios: tuple[ControlledOrderScenario, ...]
) -> None:
    submissions = sum(scenario.cycles for scenario in scenarios)
    # Every submitted order has one cancel attempt: cancellation cases exercise
    # their target behavior, while fill cases remove a possible remainder.
    commands = submissions * 2
    buy_notional = sum(
        scenario.price_policy.maximum * scenario.quantity * scenario.cycles
        for scenario in scenarios
        if scenario.side == 1
    )
    sell_quantity = sum(
        scenario.quantity * scenario.cycles for scenario in scenarios if scenario.side == 2
    )
    if submissions > safety.max_total_submissions or commands > safety.max_total_commands:
        raise ValueError("controlled-order fixture scenario count exceeds its command cap")
    if (
        buy_notional > safety.max_total_buy_notional
        or sell_quantity > safety.max_total_sell_quantity
    ):
        raise ValueError(
            "controlled-order fixture scenario notional or sell quantity exceeds its cap"
        )


def _money(value: object, name: str) -> Decimal:
    if type(value) not in {int, float, str}:
        raise ValueError(f"controlled-order fixture {name} must be numeric")
    decimal = Decimal(str(value))
    if not decimal.is_finite() or decimal.as_tuple().exponent < -2:
        raise ValueError(f"controlled-order fixture {name} must have at most two decimal places")
    return decimal
