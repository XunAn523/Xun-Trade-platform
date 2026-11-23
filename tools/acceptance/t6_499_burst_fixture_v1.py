"""Materialize the reviewed 499-order simulated-burst fixture on the test host.

The source is the repository's controlled-order template.  This generator
changes only the explicit T6 scope: it arms a simulated fixture, permits 499
passive 100-share buys, and reserves 499 paired cleanup actions.  It contains
no credentials, endpoint, account, venue order identifier or result payload.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tools.performance.perfbench.tradecore_tight_burst_order_plan import (
    historical_controlled_fixture_order_plan,
)

_BURST_WIDTH = 499
_MAX_BUY_NOTIONAL = 78_343.00


def _template_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "performance"
        / "fixtures"
        / "emt-controlled-order-fixture.example.json"
    )


def t6_499_burst_fixture_v1() -> dict[str, Any]:
    """Return a bounded simulated fixture eligible for one 499-order burst."""

    raw: object = json.loads(_template_path().read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError("T6 controlled-order template is invalid")
    safety = raw.get("safety")
    if not isinstance(safety, dict):
        raise RuntimeError("T6 controlled-order safety is invalid")
    safety.update(
        {
            "armed": True,
            "max_total_submissions": _BURST_WIDTH,
            "max_total_commands": _BURST_WIDTH * 2,
            "max_total_buy_notional": _MAX_BUY_NOTIONAL,
            "instructions": (
                "T6 simulated-only 499-order burst. Release exactly 499 passive 100-share "
                "orders once, wait the EMT action-rate cooldown, then serially cancel each "
                "accepted order. No automatic retry; retain terminal reconciliation evidence."
            ),
        }
    )
    return raw


def write_t6_499_burst_fixture(output: Path) -> str:
    """Write once and validate the exact fixture used to bind the tight plan."""

    raw = t6_499_burst_fixture_v1()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("x", encoding="utf-8") as handle:
            json.dump(raw, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
    except FileExistsError as exc:
        raise RuntimeError("T6 499 burst fixture output already exists") from exc
    # The ordinary controlled-order parser intentionally caps a sequential
    # experiment at 40 orders.  T6 uses a separately bounded 499-burst plan,
    # so validate its stricter passive-buy extraction instead of weakening the
    # ordinary parser's cap.
    historical_controlled_fixture_order_plan(fixture=output, expires_at_unix_s=None)
    return output.name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    name = write_t6_499_burst_fixture(args.output)
    print(json.dumps({"output": name, "burst_width": _BURST_WIDTH, "simulated_only": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
