"""Write a no-login EMT TraderApi/TraderSpi ABI attestation JSON report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from xuanling_tradecore.adapters.emt.trading_attestation_v1 import build_trading_abi_attestation_v1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=args.sdk_root)).load()
    report = build_trading_abi_attestation_v1(loaded.trader_api_type)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
