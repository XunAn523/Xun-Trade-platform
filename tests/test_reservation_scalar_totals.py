from __future__ import annotations

import random
from dataclasses import replace
from unittest.mock import patch

import pytest
from xuanling_tradecore.domain import TradingDayEpoch
from xuanling_tradecore.oms import (
    Reservation,
    ReservationBook,
    ReservationRequirement,
    ReservationRiskError,
    ReservationStatus,
)

from test_tradecore_oms import _risk_snapshot


def reservation(n, requirement):
    return Reservation(
        f"r-{n}",
        f"o-{n}",
        "broker-account-1",
        TradingDayEpoch("2026-08-31"),
        "risk-1",
        "decision-1",
        requirement,
        requirement,
        ReservationStatus.ACTIVE,
        0,
        10,
    )


def reference_allowed(rows, requirement, risk):
    active = [r.active_requirement for r in rows if r.is_active]
    if sum(r.cash_minor for r in active) + requirement.cash_minor > risk.available_cash_minor:
        return False
    if sum(r.margin_minor for r in active) + requirement.margin_minor > risk.available_margin_minor:
        return False
    same = [r for r in active if r.symbol == requirement.symbol]
    return sum(
        r.sellable_quantity for r in same
    ) + requirement.sellable_quantity <= risk.sellable_quantities.get(
        requirement.symbol, 0
    ) and sum(
        r.borrowable_quantity for r in same
    ) + requirement.borrowable_quantity <= risk.borrowable_quantities.get(requirement.symbol, 0)


def test_scalar_cached_arithmetic_matches_full_scan_across_lifecycle():
    rng = random.Random(38123)
    book = ReservationBook()
    rows = {}
    risk = _risk_snapshot(cash_minor=5000)
    serial = 0
    for _ in range(400):
        if not rows or rng.randrange(3) == 0:
            req = ReservationRequirement(
                rng.choice(["SZ.000010", "OTHER"]),
                rng.randrange(100),
                rng.randrange(50),
                rng.randrange(10),
                rng.randrange(5),
            )
            row = reservation(serial, req)
            serial += 1
            book.create(row)
            book.create(row)  # exact retry remains counted once
            rows[row.oms_order_id] = row
        else:
            key = rng.choice(list(rows))
            row = rows[key]
            if row.is_active:
                leaves = rng.choice([0, 25, 50])
                adjusted = book.adjust_to_leaves(
                    oms_order_id=key, total_quantity=100, leaves_quantity=leaves
                )
                book.replace_committed(adjusted)
                rows[key] = adjusted
            else:
                book.forget_released(key)
                del rows[key]
        for symbol in ("SZ.000010", "OTHER"):
            req = ReservationRequirement(
                symbol,
                rng.randrange(5000),
                rng.randrange(5000),
                rng.randrange(10000),
                rng.randrange(5000),
            )
            allowed = reference_allowed(rows.values(), req, risk)
            if allowed:
                book.ensure_available(req, risk)
            else:
                with pytest.raises(ReservationRiskError):
                    book.ensure_available(req, risk)


def test_check_create_release_do_not_construct_throwaway_requirements():
    book = ReservationBook()
    row = reservation(1, ReservationRequirement("SZ.000010", 1000, 100, 1, 1))
    req = ReservationRequirement("SZ.000010", 10)
    released = replace(
        row,
        active_requirement=ReservationRequirement.zero_for("SZ.000010"),
        status=ReservationStatus.RELEASED,
        reservation_revision=1,
    )
    risk = _risk_snapshot()
    with patch.object(
        ReservationRequirement,
        "__post_init__",
        side_effect=AssertionError("unexpected temporary domain object"),
    ):
        book.create(row)
        book.ensure_available(req, risk)
        book.replace_committed(released)
        book.ensure_available(req, risk)
    assert book.reservation_for(row.oms_order_id) == released
