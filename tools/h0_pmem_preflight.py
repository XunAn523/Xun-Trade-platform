"""Run the read-only H0 persistent-memory Go/No-Go probe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from xuanling_tradecore.persistence import (
    collect_h0_host_snapshot,
    evaluate_h0,
    load_h0_evidence,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence",
        type=Path,
        help="approved H0 physical-media and recovery-drill evidence JSON",
    )
    return parser


def main() -> int:
    """Print a non-sensitive JSON report and return nonzero for an H0 No-Go."""

    arguments = _parser().parse_args()
    evidence = None if arguments.evidence is None else load_h0_evidence(arguments.evidence)
    report = evaluate_h0(collect_h0_host_snapshot(), evidence)
    print(json.dumps(report.as_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0 if report.eligible else 2


if __name__ == "__main__":
    raise SystemExit(main())
