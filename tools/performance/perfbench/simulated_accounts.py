"""Local-only simulated EMT account configuration.

The configuration file belongs below the repository's ignored ``data/``
directory.  It is parsed at runtime and is never copied into reports.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class AccountConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class SimulatedAccount:
    account: str
    password: str


@dataclass(frozen=True)
class SimulatedAccounts:
    host: str
    port: int
    ordinary: SimulatedAccount
    credit: SimulatedAccount
    option: SimulatedAccount


def _account(data: dict[str, Any], name: str) -> SimulatedAccount:
    value = data.get(name)
    if not isinstance(value, dict):
        raise AccountConfigurationError(f"missing object: {name}")
    account = value.get("account")
    password = value.get("password")
    if not isinstance(account, str) or not account:
        raise AccountConfigurationError(f"{name}.account must be a non-empty string")
    if not isinstance(password, str) or not password:
        raise AccountConfigurationError(f"{name}.password must be a non-empty string")
    if any(marker in account or marker in password for marker in ("<", ">")):
        raise AccountConfigurationError(
            f"{name} still contains template angle brackets; replace the entire placeholder value"
        )
    return SimulatedAccount(account=account, password=password)


def load_simulated_accounts(path: Path) -> SimulatedAccounts:
    """Read and validate a local config without returning it to callers' reports."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AccountConfigurationError(f"account configuration does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise AccountConfigurationError(f"invalid JSON in account configuration: {path}") from exc
    if not isinstance(raw, dict):
        raise AccountConfigurationError("account configuration root must be an object")
    gateway = raw.get("gateway")
    if not isinstance(gateway, dict):
        raise AccountConfigurationError("missing object: gateway")
    host = gateway.get("host")
    port = gateway.get("port")
    if not isinstance(host, str) or not host:
        raise AccountConfigurationError("gateway.host must be a non-empty string")
    if "<" in host or ">" in host:
        raise AccountConfigurationError("gateway.host still contains template angle brackets")
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise AccountConfigurationError("gateway.port must be an integer in 1..65535")
    return SimulatedAccounts(
        host=host,
        port=port,
        ordinary=_account(raw, "ordinary"),
        credit=_account(raw, "credit"),
        option=_account(raw, "option"),
    )
