from __future__ import annotations

import json
import os
import random
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from hashlib import blake2s
from pathlib import Path

import pytest
from xuntrade_omsems import compute

ROOT = Path(__file__).resolve().parents[1]


def reference_route(value: str, shards: int) -> int:
    return (
        int.from_bytes(blake2s(value.encode(), digest_size=16, person=b"tcshard1").digest(), "big")
        % shards
    )


def test_journal_uses_selected_kernel():
    from emt_gateway.journal import crc32c

    assert crc32c is compute.crc32c
    assert crc32c(b"123456789") == 0xE3069283


def test_crc_matches_reference_for_lengths_and_unaligned_slices():
    rng = random.Random(523)
    for length in [0, 1, 7, 8, 9, 15, 63, 64, 65, 8191, 8192, 65537]:
        data = rng.randbytes(length + 16)
        for offset in range(16):
            value = data[offset : offset + length]
            assert compute.crc32c(value) == compute.crc32c_python(value)
    with pytest.raises(TypeError):
        compute.crc32c(bytearray(b"mutable"))


@pytest.mark.parametrize("shards", [1, 2, 3, 7, 16, 31, 32])
def test_batch_routes_match_existing_wire_algorithm(shards):
    rng = random.Random(523)
    ids = (
        tuple("a" * n for n in [1, 63, 64, 65, 127, 128, 129, 255, 256])
        + (
            "荨安交易",
            "😀" * 256,
            "a\x00b",
        )
        + tuple(f"execution-{rng.getrandbits(128):032x}" for _ in range(1000))
    )
    assert compute.session_shards(ids, shards) == tuple(reference_route(x, shards) for x in ids)


def test_parallel_routes_and_concurrent_calls_are_deterministic():
    ids = tuple(f"execution-{i}" for i in range(12000))
    expected = tuple(reference_route(x, 31) for x in ids)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: compute.session_shards(ids, 31, workers=4), range(8)))
    assert all(result == expected for result in results)


def test_assessment_boundaries_and_overflow():
    maximum = compute.MAX_INT64
    rows = (
        (100, 12, 1200, 1200),
        (100, 12, 1199, 1200),
        (100, 12, 1200, 1199),
        (maximum, 1, maximum, maximum),
        (maximum, 2, maximum, maximum),
        (0, 12, 1200, 1200),
        (-1, 1, 10, 10),
        (1, 1, -1, 1),
        (1, 1, 1, -1),
    )
    expected = (
        (1200, 0),
        (1200, 4),
        (1200, 3),
        (maximum, 0),
        (0, 2),
        (0, 1),
        (0, 1),
        (0, 1),
        (0, 1),
    )
    assert compute.assess_orders(rows) == expected
    assert compute.assess_orders(rows * 3000, workers=8) == expected * 3000


def test_assessment_randomized_against_unbounded_integer_arithmetic():
    rng = random.Random(523)
    rows = tuple(
        (
            rng.randrange(-1, 1 << 40),
            rng.randrange(-1, 1 << 25),
            rng.randrange(-1, 1 << 62),
            rng.randrange(-1, 1 << 62),
        )
        for _ in range(10000)
    )
    expected = []
    for quantity, price, cash, limit in rows:
        notional = quantity * price
        if quantity <= 0 or price <= 0 or cash < 0 or limit < 0:
            expected.append((0, 1))
        elif notional > compute.MAX_INT64:
            expected.append((0, 2))
        else:
            expected.append((notional, 3 if notional > limit else 4 if notional > cash else 0))
    assert compute.assess_orders(rows, workers=4) == tuple(expected)


def test_missing_native_fails_when_required_and_can_be_explicitly_diagnosed():
    script = """
import importlib.abc, sys
class MissingNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'xuntrade_omsems._native':
            raise ModuleNotFoundError('extension deliberately unavailable')
sys.meta_path.insert(0, MissingNative())
from xuntrade_omsems.compute import backend_info
print(backend_info()['engine'])
"""
    for mode, expected_status in (("native", 1), ("auto", 0), ("python", 0)):
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "XUNTRADE_BACKEND": mode},
        )
        assert result.returncode == expected_status
        if mode == "native":
            assert "required XunTradeOMSEMS C++ engine is unavailable" in result.stderr
        else:
            assert result.stdout.strip() == "python"


@pytest.mark.parametrize(
    "rows,error",
    [
        (((True, 1, 10, 10),), TypeError),
        (((1.0, 1, 10, 10),), TypeError),
        (((1, 2, 3),), TypeError),
        (([1, 2, 3, 4],), TypeError),
        (((1 << 63, 1, 1, 1),), OverflowError),
        (((),), TypeError),
    ],
)
def test_malformed_assessments_fail_before_computation(rows, error):
    with pytest.raises(error):
        compute.assess_orders(rows)


@pytest.mark.parametrize("value", [True, 0, -1, 33, 1.0])
def test_invalid_worker_and_shard_counts(value):
    with pytest.raises((TypeError, ValueError)):
        compute.session_shards(("execution",), value)
    with pytest.raises((TypeError, ValueError)):
        compute.assess_orders((), workers=value)


@pytest.mark.parametrize(
    "ids,error",
    [
        (("",), ValueError),
        (("a" * 257,), ValueError),
        ((1,), TypeError),
        (("\ud800",), UnicodeEncodeError),
        ("abc", TypeError),
    ],
)
def test_invalid_route_inputs(ids, error):
    with pytest.raises(error):
        compute.session_shards(ids, 3)


def test_batch_budget_and_empty_batches():
    assert compute.assess_orders(()) == ()
    assert compute.session_shards((), 3) == ()
    with pytest.raises(ValueError):
        compute.session_shards(("x",) * (compute.MAX_BATCH_SIZE + 1), 3)
    with pytest.raises(ValueError):
        compute.assess_orders(((1, 1, 1, 1),) * (compute.MAX_BATCH_SIZE + 1))


def test_native_and_diagnostic_backend_agree_in_separate_processes():
    script = """
import json
from xuntrade_omsems.compute import assess_orders, session_shards, crc32c
print(json.dumps([assess_orders(((100,12,1200,1200),(2**63-1,2,1,1))),
                  session_shards(('execution-1','荨安','a'*256),31),crc32c(b'123456789')]))
"""
    outputs = []
    for backend in ("python", "auto"):
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "XUNTRADE_BACKEND": backend, "PYTHONUTF8": "1"},
        )
        outputs.append(json.loads(completed.stdout))
    assert outputs[0] == outputs[1]


def test_explicit_native_requirement_matches_reported_backend():
    if compute.backend_info()["engine"] == "cpp20":
        compute.require_native()
    else:
        with pytest.raises(RuntimeError):
            compute.require_native()


def test_native_error_does_not_silently_fall_back(monkeypatch):
    class Broken:
        @staticmethod
        def assess_orders(*args):
            raise RuntimeError("native computation failed")

    monkeypatch.setattr(compute, "_native", Broken())
    with pytest.raises(RuntimeError, match="native computation failed"):
        compute.assess_orders(((1, 1, 1, 1),))
