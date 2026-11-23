"""Run the isolated M0 msgspec boundary-codec experiment without any broker I/O."""

from __future__ import annotations

import argparse
import json

from xuanling_tradecore.codec import run_msgspec_codec_benchmark


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=50_000)
    parser.add_argument("--warmup", type=int, default=5_000)
    parser.add_argument("--rounds", type=int, default=20)
    return parser


def main() -> int:
    """Print one JSON report; the caller chooses whether and where to retain it."""

    arguments = _parser().parse_args()
    report = run_msgspec_codec_benchmark(
        iterations=arguments.iterations,
        warmup=arguments.warmup,
        rounds=arguments.rounds,
    )
    print(json.dumps(report.as_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
