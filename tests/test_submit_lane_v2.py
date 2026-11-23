from contextvars import ContextVar
from threading import Event, get_ident
from time import monotonic, monotonic_ns, sleep
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.cold_admission_v2 import ConsumerColdLookupRequiredV2
from xuanling_tradecore.consumer.submit_lane_v2 import (
    BoundedSubmitLaneV2,
    SubmissionPrecheckFailedV2,
)
from xuanling_tradecore.oms.cold_intent import ColdIntentKey


def take(lane):
    deadline = monotonic() + 3
    while monotonic() < deadline:
        result = lane.take()
        if result is not None:
            return result
        sleep(0.001)
    raise AssertionError("no bounded submission completion")


def test_one_fifo_owner_context_and_completed_results_keep_their_capacity():
    stop, entered, release = Event(), Event(), Event()
    lane = BoundedSubmitLaneV2(capacity=2, maximum_response_bytes=16, stopped=stop)
    context = ContextVar("test-context", default=0)
    calls = []

    def work():
        calls.append((context.get(), get_ident()))
        entered.set()
        assert release.wait(3)
        return b"ok"

    lane.start()
    try:
        context.set(1)
        first = lane.offer(1, work, deadline_ns=monotonic_ns() + 3_000_000_000)
        assert first and entered.wait(2)
        context.set(2)
        assert lane.offer(2, work, deadline_ns=monotonic_ns() + 3_000_000_000)
        assert lane.full and lane.outstanding == 2
        assert lane.offer(3, work, deadline_ns=monotonic_ns() + 3_000_000_000) is None
        with pytest.raises(ValueError, match="duplicate"):
            lane.offer(1, work, deadline_ns=monotonic_ns() + 3_000_000_000)
        assert not first.cancel_before_start()  # already started, never claimed canceled
        release.set()
        results = [take(lane), take(lane)]
        assert [r.token for r in results] == [1, 2]
        assert [v for v, _ in calls] == [1, 2]
        assert len({tid for _, tid in calls}) == 1 and calls[0][1] != get_ident()
        assert all(r.started and r.response == b"ok" and not r.outcome_unknown for r in results)
        assert lane.outstanding == 0
    finally:
        release.set()
        lane.close()


@pytest.mark.parametrize("fence", ("cancel", "stop", "deadline"))
def test_observed_prestart_fence_never_runs_queued_work(fence):
    entered, release, stop = Event(), Event(), Event()
    lane = BoundedSubmitLaneV2(capacity=2, maximum_response_bytes=16, stopped=stop)
    calls = []

    def running():
        entered.set()
        assert release.wait(3)
        return b"first"

    lane.start()
    try:
        assert lane.offer(1, running, deadline_ns=monotonic_ns() + 3_000_000_000)
        assert entered.wait(2)
        ticket = lane.offer(
            2,
            lambda: calls.append("forbidden") or b"second",
            deadline_ns=1 if fence == "deadline" else monotonic_ns() + 3_000_000_000,
        )
        assert ticket
        if fence == "cancel":
            assert ticket.cancel_before_start()
        elif fence == "stop":
            stop.set()
        release.set()
        assert take(lane).started
        result = take(lane)
        assert not result.started and result.response is None and not result.outcome_unknown
        assert calls == []
    finally:
        release.set()
        lane.close()


@pytest.mark.parametrize("failure", ("exception", "base_exception", "oversize", "type"))
def test_unknown_execution_is_never_an_automatic_retry_or_negative_receipt(failure):
    lane = BoundedSubmitLaneV2(capacity=1, maximum_response_bytes=16, stopped=Event())
    calls = []
    trace = {}

    def work():
        calls.append("possibly committed")
        if failure == "exception":
            raise OSError("private storage details")
        if failure == "base_exception":
            raise KeyboardInterrupt("private details")
        return b"x" * 17 if failure == "oversize" else "not bytes"

    lane.start()
    try:
        assert lane.offer(1, work, deadline_ns=monotonic_ns() + 3_000_000_000, trace=trace)
        result = take(lane)
        assert result.started and result.outcome_unknown and result.response is result.cold is None
        assert calls == ["possibly committed"]
        assert "private" not in repr(result)
        assert trace["submit_owner_started_ns"] <= trace["submit_owner_finished_ns"]
        assert lane.failed
        assert lane.offer(2, lambda: b"must not run", deadline_ns=monotonic_ns() + 10**9) is None
    finally:
        with pytest.raises(RuntimeError, match="outcome unknown"):
            lane.close()


def test_cold_requirement_copies_only_key_and_port_not_exception_stack():
    lane = BoundedSubmitLaneV2(capacity=1, maximum_response_bytes=16, stopped=Event())
    key, resolver = ColdIntentKey("a", "s", "o"), object()

    def work():
        raise ConsumerColdLookupRequiredV2(key, resolver)

    lane.start()
    try:
        assert lane.offer(1, work, deadline_ns=monotonic_ns() + 3_000_000_000)
        result = take(lane)
        assert result.started and not result.outcome_unknown and result.response is None
        assert result.cold.key == key and result.cold.resolver is resolver
        assert not isinstance(result.cold, BaseException)
    finally:
        lane.close()


def test_known_schema_precheck_failure_does_not_poison_or_run_the_handler():
    lane = BoundedSubmitLaneV2(capacity=1, maximum_response_bytes=16, stopped=Event())

    def rejected():
        raise SubmissionPrecheckFailedV2()

    lane.start()
    try:
        assert lane.offer(1, rejected, deadline_ns=monotonic_ns() + 3_000_000_000)
        result = take(lane)
        assert result.precheck_failed and not result.outcome_unknown and not lane.failed
        assert lane.offer(2, lambda: b"ok", deadline_ns=monotonic_ns() + 3_000_000_000)
        assert take(lane).response == b"ok"
    finally:
        lane.close()


def test_close_timeout_is_a_failure_and_completed_result_remains_collectable():
    entered, release = Event(), Event()
    lane = BoundedSubmitLaneV2(capacity=1, maximum_response_bytes=16, stopped=Event())

    def work():
        entered.set()
        assert release.wait(3)
        return b"committed"

    lane.start()
    try:
        assert lane.offer(1, work, deadline_ns=monotonic_ns() + 3_000_000_000)
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="timed out"):
            lane.close(timeout=0.01)
        assert lane.offer(2, work, deadline_ns=monotonic_ns() + 3_000_000_000) is None
        release.set()
        result = take(lane)
        assert result.started and result.response == b"committed"
    finally:
        release.set()
        lane.close()
    lane.clear_notification()  # closed notification is not a false drain failure


def test_notification_failure_cannot_silently_abandon_or_restart_queued_work():
    lane = BoundedSubmitLaneV2(capacity=1, maximum_response_bytes=16, stopped=Event())
    # Windows socketpair uses loopback TCP: closing its peer does not require
    # the very next send to fail. Inject the notification syscall failure
    # deterministically, rather than assume POSIX AF_UNIX close semantics.
    with patch.object(lane, "_writer", wraps=lane._writer) as notifier:
        notifier.send.side_effect = OSError("injected notification failure")
        lane.start()
        assert lane.offer(1, lambda: b"result", deadline_ns=monotonic_ns() + 3_000_000_000)
        assert take(lane).response == b"result"
        with pytest.raises(RuntimeError, match="notification failed"):
            lane.close()
    assert lane.offer(2, lambda: b"no", deadline_ns=monotonic_ns() + 3_000_000_000) is None


@pytest.mark.parametrize("capacity", (True, 0, 129, 1.5))
def test_invalid_capacity_is_refused_without_starting_owner(capacity):
    with pytest.raises(ValueError):
        BoundedSubmitLaneV2(capacity=capacity, maximum_response_bytes=16, stopped=Event())
