"""Boundable EMT vendor-SDK login probe without gateway or trading calls.

The invocation sequence mirrors the vendor's ``test/tradertest.py`` sample:
create ``TraderApi``, subscribe to the public topic, call synchronous
``login`` and, only after a successful return, ``logout`` then ``release``.
It deliberately performs no query, order, cancellation, credit or fund API.
Credentials and endpoint values are read only from the ignored local simulated
account file and are never printed.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from emt_gateway.platform_loader import (
    EmtPlatformLoader,
    SdkLoadSettings,
    suppress_native_console_output,
)
from tools.performance.perfbench.simulated_accounts import load_simulated_accounts


def _error_id(api: Any) -> int | None:
    try:
        error = api.getApiLastError()
    except Exception:
        return None
    if isinstance(error, dict) and isinstance(error.get("error_id"), int):
        return error["error_id"]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Minimal official EMT SDK login probe; no business API is invoked"
    )
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--accounts-file", type=Path, required=True)
    parser.add_argument("--profile", choices=("ordinary", "credit", "option"), required=True)
    parser.add_argument("--client-id", type=int, default=94)
    parser.add_argument("--local-ip", default="")
    parser.add_argument("--sdk-log-dir", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.client_id <= 255:
        parser.error("--client-id must be in 1..255")

    accounts = load_simulated_accounts(args.accounts_file)
    account = getattr(accounts, args.profile)
    args.sdk_log_dir.mkdir(parents=True, exist_ok=True)
    loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=args.sdk_root)).load()

    class OfficialLoginProbeApi(loaded.trader_api_type):
        pass

    api = OfficialLoginProbeApi()
    session_id = 0
    try:
        # This is the vendor documented sequence from test/tradertest.py.
        with suppress_native_console_output():
            api.createTraderApi(args.client_id, str(args.sdk_log_dir), 0)
            api.setSoftwareVersion("emt-gateway/official-sdk-login-probe")
            api.setHeartBeatInterval(10)
            api.subscribePublicTopic(2)
            session_id = int(
                api.login(
                    accounts.host,
                    accounts.port,
                    account.account,
                    account.password,
                    1,
                    args.local_ip,
                )
            )
        print("LOGIN_RETURNED=true")
        print(f"LOGIN_SUCCEEDED={session_id != 0}")
        print(f"LOGIN_ERROR_ID={_error_id(api)}")
        return 0 if session_id else 2
    finally:
        try:
            if session_id:
                api.logout(session_id)
        finally:
            api.release()


if __name__ == "__main__":
    raise SystemExit(main())
