"""Xuanling TradeCore public identity and vendor-neutral foundations.

This package deliberately has no import of EMT, QMT, PaperBroker, or the
legacy :mod:`emt_gateway` package.  Venue-specific code may only enter the
core through future BrokerAdapter implementations.
"""

from .identity import (
    DEFAULT_RUNTIME_CONTROL_ROOT,
    LEGACY_COMPATIBILITY_PACKAGE,
    PROJECT_DISTRIBUTION,
    PROJECT_NAME,
    ProjectIdentityError,
    RuntimeControlRoot,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_RUNTIME_CONTROL_ROOT",
    "LEGACY_COMPATIBILITY_PACKAGE",
    "PROJECT_DISTRIBUTION",
    "PROJECT_NAME",
    "ProjectIdentityError",
    "RuntimeControlRoot",
    "__version__",
]
