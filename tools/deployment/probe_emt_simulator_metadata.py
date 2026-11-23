"""Disposable simulator login metadata probe: no query/order/cancel calls."""

import argparse
import json
import os
from pathlib import Path

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)
from emt_gateway.session import normalize_emt_trading_day
from emt_gateway.spi_bridge import DeferredCallbackIngress, build_trader_spi_bridge


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--sdk-hashes-file", type=Path, required=True)
    parser.add_argument("--log-dir", type=Path, required=True)
    parser.add_argument("--client-id", type=int, default=123)
    args = parser.parse_args()
    if not 1 <= args.client_id <= 127:
        parser.error("invalid client id")
    source = json.loads(args.accounts_file.read_text(encoding="utf-8-sig"))
    loaded = EmtPlatformLoader(SdkLoadSettings(args.sdk_root, expected_hashes=json.loads(args.sdk_hashes_file.read_text()))).load()

    args.log_dir.mkdir(mode=0o700, exist_ok=False)
    # Register every SPI callback even for a metadata-only login: public
    # events can arrive during login and an empty native subclass is unsafe.
    bridge = build_trader_spi_bridge(loaded.trader_api_type, callback_ingress=DeferredCallbackIngress(), account_profile="ordinary")
    api = bridge()
    with suppress_native_console_output():
        api.createTraderApi(args.client_id, str(args.log_dir), 0)
        api.setSoftwareVersion("tradecore-simulator-metadata")
        api.setHeartBeatInterval(10)
        api.subscribePublicTopic(2)
        session_id = api.login(source["gateway"]["host"], source["gateway"]["port"], source["ordinary"]["account"], source["ordinary"]["password"], 1, "")
    if not session_id:
        error = api.getApiLastError()
        message = str(error.get("error_msg", "")) if isinstance(error, dict) else ""
        for secret in (source["gateway"]["host"], source["ordinary"]["account"], source["ordinary"]["password"]):
            message = message.replace(secret, "<redacted>")
        result = {"login_succeeded": False, "error_id": error.get("error_id") if isinstance(error, dict) else None, "safe_error_message": message[:256], "mutation_calls": 0}
        os.write(1, (json.dumps(result, ensure_ascii=False) + "\n").encode())
        os._exit(2)
    result = {"login_succeeded": True, "api_version": api.getApiVersion(), "trading_day": normalize_emt_trading_day(api.getTradingDay()), "mutation_calls": 0}
    os.write(1, (json.dumps(result) + "\n").encode())
    # This process owns no orders. Exit directly instead of invoking the
    # vendor wrapper finalizer; the OS closes its read-only session sockets.
    os._exit(0)


if __name__ == "__main__":
    main()
