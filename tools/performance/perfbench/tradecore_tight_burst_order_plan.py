"""Materialize one local, payload-free-reported historical 499-order plan.

This tool deliberately runs on the EMT test host. It derives a narrowly
bounded plan from the already-local controlled simulated-order fixture, so a
Git deployment never needs to transfer the ignored order-plan file itself.
"""

from __future__ import annotations

import argparse
import json
import time
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any

from .controlled_order_fixture import ControlledOrderFixtureError, load_controlled_order_fixture
from .tradecore_mmap_emt_support import TradeCoreMmapEmtPerfError, load_tight_burst_order_plan


def _decimal_text(value: Decimal) -> str:
    return format(value, ".2f")


def _legacy_controlled_fixture_plan_fields(
    fixture: Path,
) -> tuple[str, int, Decimal, Decimal, int, str]:
    """Read only the constrained legacy simulated fixture fields we need.

    Older test hosts keep the predecessor fixture schema without account
    profile and ticker-list fields. It remains acceptable only when its
    ``cancel_buy`` scenario stays inside the same 100-share, tick-aligned
    passive-buy envelope. No legacy field is carried through unchecked.
    """

    try:
        raw = json.loads(fixture.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TradeCoreMmapEmtPerfError("cannot read legacy controlled-order fixture") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise TradeCoreMmapEmtPerfError("legacy controlled-order fixture is invalid")
    safety, scenarios = raw.get("safety"), raw.get("scenarios")
    if (
        not isinstance(safety, dict)
        or safety.get("environment") != "simulated"
        or type(safety.get("armed")) is not bool
        or not isinstance(scenarios, list)
    ):
        raise TradeCoreMmapEmtPerfError("legacy controlled-order fixture is not simulated")
    candidate = next(
        (item for item in scenarios if isinstance(item, dict) and item.get("id") == "cancel_buy"),
        None,
    )
    if not isinstance(candidate, dict):
        raise TradeCoreMmapEmtPerfError("legacy cancel-buy scenario is unavailable")
    ticker = candidate.get("ticker")
    market, side, price_type, quantity = (
        candidate.get("market"),
        candidate.get("side"),
        candidate.get("price_type"),
        candidate.get("quantity"),
    )
    policy = candidate.get("price_policy")
    if (
        not isinstance(ticker, str)
        or len(ticker) != 6
        or not ticker.isascii()
        or not ticker.isdigit()
        or market not in {1, 2}
        or side != 1
        or price_type != 1
        or type(quantity) is not int
        or quantity < 100
        or quantity % 100
        or not isinstance(policy, dict)
    ):
        raise TradeCoreMmapEmtPerfError("legacy cancel-buy scenario exceeds the plan envelope")
    try:
        minimum = Decimal(str(policy["min"]))
        maximum = Decimal(str(policy["max"]))
        tick = Decimal(str(policy["tick"]))
    except (InvalidOperation, KeyError, ValueError) as exc:
        raise TradeCoreMmapEmtPerfError("legacy cancel-buy price policy is invalid") from exc
    if (
        not minimum.is_finite()
        or not maximum.is_finite()
        or not minimum > 0
        or minimum > maximum
        or tick != Decimal("0.01")
    ):
        raise TradeCoreMmapEmtPerfError("legacy cancel-buy price policy is invalid")
    return ticker, market, minimum, maximum, quantity, sha256(fixture.read_bytes()).hexdigest()


def historical_controlled_fixture_order_plan(
    *, fixture: Path, expires_at_unix_s: int | None, now_unix_s: int | None = None
) -> dict[str, Any]:
    """Build the fixed passive buy plan approved by the controlled fixture.

    The output always uses the lower bound of the approved ``cancel_buy``
    range. It is historical-comparability evidence, not a live-quote claim.
    """

    if expires_at_unix_s is not None:
        now = int(time.time()) if now_unix_s is None else now_unix_s
        if type(expires_at_unix_s) is not int or not now < expires_at_unix_s <= now + 86_400:
            raise TradeCoreMmapEmtPerfError(
                "historical tight-burst plan expiry must be within the next 24 hours"
            )
    try:
        controlled = load_controlled_order_fixture(fixture)
    except ControlledOrderFixtureError:
        ticker, market, minimum, maximum, quantity, source_fixture_sha256 = (
            _legacy_controlled_fixture_plan_fields(fixture)
        )
    else:
        try:
            scenario = controlled.selected(("cancel_buy",))[0]
        except IndexError as exc:  # Defensive: ``selected`` validates the identifier.
            raise TradeCoreMmapEmtPerfError(
                "controlled cancel-buy scenario is unavailable"
            ) from exc
        if scenario.side != 1 or scenario.quantity != controlled.safety.max_order_quantity:
            raise TradeCoreMmapEmtPerfError(
                "controlled cancel-buy scenario exceeds the plan envelope"
            )
        ticker, market, minimum, maximum, quantity = (
            scenario.ticker,
            1,
            scenario.price_policy.minimum,
            scenario.price_policy.maximum,
            scenario.quantity,
        )
        try:
            source_fixture_sha256 = sha256(fixture.read_bytes()).hexdigest()
        except OSError as exc:
            raise TradeCoreMmapEmtPerfError("cannot fingerprint controlled-order fixture") from exc
    return {
        "schema_version": 1,
        "purpose": "tradecore-tight-499-order-plan",
        "environment": "simulated",
        "selection_mode": "historical-controlled-fixture",
        "expires_at_unix_s": expires_at_unix_s,
        "ticker": ticker,
        "market": market,
        "side": 1,
        "price_type": 1,
        "price": _decimal_text(minimum),
        "quantity": quantity,
        "price_tick": "0.01",
        "price_min": _decimal_text(minimum),
        "price_max": _decimal_text(maximum),
        # The controlled fixture's ordinary low-rate cap does not describe a
        # 499-order burst. Set the smallest valid aggregate cap instead of
        # importing a broader notional allowance into this benchmark plan.
        "max_total_buy_notional": _decimal_text(minimum * quantity * 499),
        "source_fixture_sha256": source_fixture_sha256,
    }


def materialize_historical_controlled_fixture_order_plan(
    *, fixture: Path, output: Path, expires_at_unix_s: int | None, now_unix_s: int | None = None
) -> str:
    """Create a new local plan once, validate it, and return only its hash."""

    raw = historical_controlled_fixture_order_plan(
        fixture=fixture,
        expires_at_unix_s=expires_at_unix_s,
        now_unix_s=now_unix_s,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("x", encoding="utf-8") as handle:
            json.dump(raw, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except FileExistsError as exc:
        raise TradeCoreMmapEmtPerfError("historical tight-burst plan already exists") from exc
    except OSError as exc:
        raise TradeCoreMmapEmtPerfError("cannot write historical tight-burst plan") from exc
    try:
        loaded = load_tight_burst_order_plan(output, burst_width=499, now_unix_s=now_unix_s)
    except BaseException:
        output.unlink(missing_ok=True)
        raise
    return loaded.plan_sha256


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Materialize a local historical controlled-fixture 499-order plan"
    )
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    expiry_group = parser.add_mutually_exclusive_group(required=True)
    expiry_group.add_argument("--expires-at-unix-s", type=int)
    expiry_group.add_argument("--no-expiry", action="store_true")
    args = parser.parse_args()
    plan_sha256 = materialize_historical_controlled_fixture_order_plan(
        fixture=args.fixture,
        output=args.output,
        expires_at_unix_s=None if args.no_expiry else args.expires_at_unix_s,
    )
    # Intentionally omit ticker, price, account/profile, credentials and
    # request body. The benchmark records this hash as configuration evidence.
    print(json.dumps({"materialized": True, "plan_sha256": plan_sha256}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
