"""Validation helpers shared by immutable TradeCore domain values.

The domain package deliberately validates only deterministic, local invariants.
It does not access a database, a filesystem, a network service, or a broker
SDK.  Signature verification and durable state checks belong to later control
and persistence layers.
"""

from __future__ import annotations

import os
import re
from types import MappingProxyType
from typing import Collection, Mapping, cast

_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
# Python's Unicode ``\s`` has the same membership as ``str.isspace()``.
# Keep the exact existing whitespace rule, but scan bounded values in C
# instead of invoking a Python generator for every character on the OMS path.
_DISALLOWED_WHITESPACE = re.compile(r"[^\S \t]")
_EMPTY_TEXT_MAPPING = cast(Mapping[str, str], MappingProxyType({}))

# Only exact builtin str keys in this immutable, bounded list are eligible.
# Values (including all account/order data) are never cached or skipped.
_KNOWN_OMS_FACT_KEYS = frozenset(
    {
        "intent_hash",
        "attribution_binding_id",
        "binding_version",
        "decision_context_id",
        "risk_snapshot_id",
        "reservation_id",
        "reservation_revision",
        "reservation_status",
        "symbol",
        "cash_minor",
        "margin_minor",
        "sellable_quantity",
        "borrowable_quantity",
    }
)
_DIAGNOSTIC_KNOWN_FACT_KEYS = os.environ.get("TRADECORE_DIAGNOSTIC_KNOWN_FACT_KEYS") == "1"


class DomainValidationError(ValueError):
    """A domain value violates a deterministic fail-closed invariant."""


def require_text(value: object, field_name: str, *, max_length: int = 256) -> str:
    """Return a bounded, non-control-character text value or fail closed."""

    if not isinstance(value, str):
        raise DomainValidationError(f"{field_name} must be text")
    if not value or not value.strip():
        raise DomainValidationError(f"{field_name} must not be blank")
    if len(value) > max_length:
        raise DomainValidationError(f"{field_name} exceeds its maximum length")
    # ``isprintable`` is a C-level fast gate for the overwhelmingly common
    # identifier path. Values containing an allowed tab still fall through to
    # the exact historical whitespace expression, preserving its semantics.
    if not value.isprintable() and _DISALLOWED_WHITESPACE.search(value) is not None:
        raise DomainValidationError(f"{field_name} contains a control character")
    return value


def require_positive(value: object, field_name: str) -> int:
    """Return a positive integer without accepting bool as an integer."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DomainValidationError(f"{field_name} must be a positive integer")
    return value


def require_nonnegative(value: object, field_name: str) -> int:
    """Return a non-negative integer without accepting bool as an integer."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DomainValidationError(f"{field_name} must be a non-negative integer")
    return value


# The original validators remain the fallback for bool, subclasses, invalid
# integers and every other input; no value, account or order data is cached.
_original_require_positive = require_positive
_original_require_nonnegative = require_nonnegative


def _require_positive_builtin_fast(value: object, field_name: str) -> int:
    if type(value) is int and value > 0:
        return value
    return _original_require_positive(value, field_name)


def _require_nonnegative_builtin_fast(value: object, field_name: str) -> int:
    if type(value) is int and value >= 0:
        return value
    return _original_require_nonnegative(value, field_name)


if os.environ.get("TRADECORE_DIAGNOSTIC_BUILTIN_INT_VALIDATION") == "1":
    # Candidate-only startup choice. The default functions and error paths are
    # unchanged; real LOW/HOT/fault/HIGH qualification is still mandatory.
    require_positive = _require_positive_builtin_fast
    require_nonnegative = _require_nonnegative_builtin_fast


def require_sha256(value: object, field_name: str) -> str:
    """Return a canonical lower-case SHA-256 hex digest."""

    if not isinstance(value, str) or _SHA256_HEX.fullmatch(value) is None:
        raise DomainValidationError(f"{field_name} must be a lowercase SHA-256 hex digest")
    return value


def freeze_text_mapping(
    value: Mapping[str, str] | None,
    field_name: str,
    *,
    max_items: int = 32,
    value_max_length: int = 256,
) -> Mapping[str, str]:
    """Copy a small text map into an immutable mapping proxy.

    Domain events use this only for bounded normalized summaries.  Raw broker
    payloads, credentials, identities, and unbounded error strings must never
    be inserted here.
    """

    if value is None:
        return _EMPTY_TEXT_MAPPING
    if len(value) > max_items:
        raise DomainValidationError(f"{field_name} exceeds its maximum item count")
    if value is _EMPTY_TEXT_MAPPING or (type(value) is dict and not value):
        return _EMPTY_TEXT_MAPPING
    value = cast(Mapping[str, str], value)
    copied: dict[str, str] = {}
    known_fact_keys = _DIAGNOSTIC_KNOWN_FACT_KEYS and field_name == "OMS fact attributes"
    for key, item in value.items():
        checked_key = (
            key
            if known_fact_keys and type(key) is str and key in _KNOWN_OMS_FACT_KEYS
            else require_text(key, f"{field_name} key", max_length=64)
        )
        copied[checked_key] = require_text(
            item,
            f"{field_name} value",
            max_length=value_max_length,
        )
    return cast(Mapping[str, str], MappingProxyType(copied))


def freeze_nonnegative_int_mapping(
    value: Mapping[str, int] | None,
    field_name: str,
    *,
    max_items: int = 4096,
) -> Mapping[str, int]:
    """Copy a bounded symbol-to-fixed-integer quantity map immutably."""

    if value is None:
        return cast(Mapping[str, int], MappingProxyType({}))
    if len(value) > max_items:
        raise DomainValidationError(f"{field_name} exceeds its maximum item count")
    copied: dict[str, int] = {}
    for key, item in value.items():
        copied[require_text(key, f"{field_name} key", max_length=64)] = require_nonnegative(
            item,
            f"{field_name} value",
        )
    return cast(Mapping[str, int], MappingProxyType(copied))


def freeze_texts(
    values: Collection[str],
    field_name: str,
    *,
    max_items: int = 64,
    max_length: int = 256,
) -> frozenset[str]:
    """Validate and freeze a bounded text set."""

    if len(values) > max_items:
        raise DomainValidationError(f"{field_name} exceeds its maximum item count")
    return frozenset(require_text(item, field_name, max_length=max_length) for item in values)
