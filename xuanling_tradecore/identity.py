"""Product identity and side-effect-free runtime control-root addressing.

The model in this module does not create, move, or validate filesystem state.
Those operations belong to the later DISARMED-only runtime-control migration.
Keeping this boundary pure lets M0 establish the canonical name without
silently creating a second control root for an existing account.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath

PROJECT_NAME = "Xuanling TradeCore"
PROJECT_DISTRIBUTION = "xuanling-tradecore"
LEGACY_COMPATIBILITY_PACKAGE = "emt_gateway"
DEFAULT_RUNTIME_CONTROL_ROOT = PurePosixPath("/var/lib/xuanling-tradecore/control")

_SCOPE_HASH = re.compile(r"[0-9a-f]{64}\Z")


class ProjectIdentityError(ValueError):
    """A product identity or control-root address violates the M0 contract."""


@dataclass(frozen=True)
class RuntimeControlRoot:
    """Address the stable, product-level control root without performing I/O."""

    path: PurePosixPath = DEFAULT_RUNTIME_CONTROL_ROOT

    def __post_init__(self) -> None:
        if not self.path.is_absolute():
            raise ProjectIdentityError("runtime control root must be an absolute path")
        if self.path.name != "control":
            raise ProjectIdentityError("runtime control root must end with the control directory")

    @classmethod
    def default(cls) -> RuntimeControlRoot:
        """Return the canonical TradeCore control-root address."""

        return cls()

    def account_control_root(self, scope_hash: str) -> PurePosixPath:
        """Return an account control path for one canonical SHA-256 scope hash.

        The caller must still obtain the ownership fence and prove the root's
        device, permissions, and persisted contents before any I/O is allowed.
        """

        if _SCOPE_HASH.fullmatch(scope_hash) is None:
            raise ProjectIdentityError("account control scope must be a lowercase SHA-256 hex hash")
        return self.path / "accounts" / scope_hash
