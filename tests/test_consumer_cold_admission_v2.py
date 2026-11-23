from __future__ import annotations

from concurrent.futures import Future
from dataclasses import replace
from types import SimpleNamespace

import pytest
from xuanling_tradecore.consumer.cold_admission_v2 import (
    BoundedColdAdmissionsV2,
    ColdAdmissionRequestV2,
    ColdAdmissionResumeV2,
    ConsumerColdLookupRequiredV2,
    admission_resolution,
    cold_admission_scope,
)
from xuanling_tradecore.oms.cold_intent import ColdIntentKey, ColdIntentResolution

KEY = ColdIntentKey("account", "strategy", "ref")


def setup(capacity=1):
    futures = []

    def resolve(_):
        future = Future()
        future.set_running_or_notify_cancel()
        futures.append(future)
        return future

    resolver = SimpleNamespace(resolve=resolve)
    return BoundedColdAdmissionsV2(capacity), ConsumerColdLookupRequiredV2(KEY, resolver), futures


def proof(key=KEY):
    return ColdIntentResolution(key, 1, b"signed-by-storage", None)


def test_capacity_covers_future_completed_and_write_ready_until_ack():
    pool, required, futures = setup()
    try:
        assert pool.offer(1, required)
        assert not pool.offer(2, required)
        assert not futures[0].cancel()
        futures[0].set_result(proof())
        pool.clear_notification()
        result = pool.take()
        assert result.resolution == proof()
        assert not isinstance(result.required, BaseException)
        assert pool.outstanding == 1
        assert not pool.offer(2, required)  # fair WRITE queue still owns this slot
        pool.release(1)
        assert pool.offer(2, required)
        futures[1].set_result(proof())
        pool.take()
        pool.release(2)
    finally:
        pool.close()


@pytest.mark.parametrize("error", [OSError("private"), SystemExit("thread abort")])
def test_storage_errors_complete_without_becoming_negative_proof(error):
    pool, required, futures = setup()
    try:
        pool.offer(1, required)
        futures[0].set_exception(error)
        result = pool.take()
        assert result.resolution is None and isinstance(result.error, Exception)
        assert result.error.__traceback__ is None
        assert result.error.__context__ is None
        assert "private" not in str(result.error)
        pool.release(1)
    finally:
        pool.close()


@pytest.mark.parametrize(
    "bad",
    [
        proof(ColdIntentKey("wrong", "strategy", "ref")),
        replace(proof(), checkpoint=b"x" * (256 * 1024 + 1)),
    ],
)
def test_wrong_scope_and_oversize_payload_fail_closed(bad):
    pool, required, futures = setup()
    try:
        pool.offer(1, required)
        futures[0].set_result(bad)
        result = pool.take()
        assert result.resolution is None and isinstance(result.error, RuntimeError)
        pool.release(1)
    finally:
        pool.close()


def test_resolve_enqueue_exception_rolls_back_slot():
    pool, required, _ = setup()
    required.resolver.resolve = lambda _: (_ for _ in ()).throw(OSError("offline"))
    with pytest.raises(OSError):
        pool.offer(1, required)
    assert pool.outstanding == 0
    pool.close()


def test_scope_is_bound_to_resolver_and_is_reset_after_failure():
    pool, required, _ = setup()
    try:
        resume = ColdAdmissionResumeV2(
            ColdAdmissionRequestV2(required.key, required.resolver), proof()
        )
        assert admission_resolution(required.resolver) is None
        with pytest.raises(RuntimeError), cold_admission_scope(resume):
            assert admission_resolution(required.resolver) == proof()
            assert admission_resolution(SimpleNamespace(resolve=lambda _: None)) is None
            raise RuntimeError("abort")
        assert admission_resolution(required.resolver) is None
    finally:
        pool.close()


def test_unfinished_future_fails_drain_then_can_finish_without_lost_token():
    pool, required, futures = setup()
    pool.offer(1, required)
    with pytest.raises(RuntimeError, match="drain timed out"):
        pool.close(timeout=0.001)
    assert not pool.offer(2, required)
    futures[0].set_result(proof())
    pool.close()
    assert pool.take().resolution == proof()
    pool.release(1)


@pytest.mark.parametrize("capacity", [True, 0, -1, 1025, 1.5])
def test_strict_capacity(capacity):
    with pytest.raises(ValueError):
        BoundedColdAdmissionsV2(capacity)
