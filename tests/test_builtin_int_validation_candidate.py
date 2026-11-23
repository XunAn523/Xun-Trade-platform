from __future__ import annotations

import os
import subprocess
import sys
from decimal import Decimal

import pytest
from xuanling_tradecore.domain._validation import (
    _original_require_nonnegative,
    _original_require_positive,
    _require_nonnegative_builtin_fast,
    _require_positive_builtin_fast,
)


class UnusualInt(int):
    def __lt__(self, other: object) -> bool:
        return True

    def __le__(self, other: object) -> bool:
        return True


class RaisingInt(int):
    def __lt__(self, other: object) -> bool:
        raise RuntimeError("comparison failure")

    def __le__(self, other: object) -> bool:
        raise RuntimeError("comparison failure")


def outcome(function, value):
    try:
        result = function(value, "test field")
        return ("returned", result, type(result), result is value)
    except Exception as error:
        return ("raised", type(error), str(error))


@pytest.mark.parametrize(
    "value",
    [
        None, False, True, -1, 0, 1, 2**63, -(2**128), 2**128,
        "1", 1.0, Decimal(1), UnusualInt(1), UnusualInt(-1), RaisingInt(1), object(),
    ],
)
def test_fast_path_preserves_results_errors_and_subclass_semantics(value):
    assert outcome(_require_positive_builtin_fast, value) == outcome(_original_require_positive, value)
    assert outcome(_require_nonnegative_builtin_fast, value) == outcome(
        _original_require_nonnegative, value
    )


@pytest.mark.parametrize("enabled", ["0", "1"])
def test_startup_selects_expected_validator_without_changing_default(enabled):
    code = (
        "from xuanling_tradecore.domain import _validation as v;"
        "print(v.require_positive.__name__,v.require_nonnegative.__name__)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "TRADECORE_DIAGNOSTIC_BUILTIN_INT_VALIDATION": enabled},
        check=True,
        text=True,
        capture_output=True,
        timeout=15,
    )
    expected = (
        "_require_positive_builtin_fast _require_nonnegative_builtin_fast"
        if enabled == "1"
        else "require_positive require_nonnegative"
    )
    assert result.stdout.strip() == expected
