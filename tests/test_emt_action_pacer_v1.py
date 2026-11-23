from collections import deque

import pytest
from tools.performance.perfbench.emt_action_pacer_v1 import (
    EmtActionPacerErrorV1,
    EmtActionPacerV1,
)


def test_2000_actions_never_exceed_499_in_any_rolling_second():
    clock = [0]
    starts = []

    def fake_sleep(seconds):
        clock[0] += round(seconds * 1_000_000_000)

    pacer = EmtActionPacerV1(
        actions_per_second=499, now_ns=lambda: clock[0], sleep_seconds=fake_sleep
    )
    for _ in range(2_000):
        pacer.call(lambda: starts.append(clock[0]))
    window = deque()
    for timestamp in starts:
        while window and timestamp - window[0] >= 1_000_000_000:
            window.popleft()
        window.append(timestamp)
        assert len(window) <= 499
    assert starts[-1] >= 1_999 * 2_004_009


def test_failed_action_still_consumes_pacing_slot():
    clock = [0]
    pacer = EmtActionPacerV1(
        actions_per_second=499,
        now_ns=lambda: clock[0],
        sleep_seconds=lambda seconds: clock.__setitem__(0, clock[0] + round(seconds * 1e9)),
    )
    with pytest.raises(ValueError):
        pacer.call(lambda: (_ for _ in ()).throw(ValueError("failed")))
    assert pacer.call(lambda: clock[0]) >= 2_004_009


def test_clock_error_latches_pacer_closed():
    clock = [100]
    pacer = EmtActionPacerV1(actions_per_second=499, now_ns=lambda: clock[0])
    pacer.call(lambda: None)
    clock[0] = 99
    with pytest.raises(EmtActionPacerErrorV1):
        pacer.call(lambda: None)
    clock[0] = 200
    with pytest.raises(EmtActionPacerErrorV1):
        pacer.call(lambda: None)


def test_slow_sdk_call_does_not_catch_up_after_completion():
    clock = [0]
    starts = []
    pacer = EmtActionPacerV1(
        actions_per_second=499,
        now_ns=lambda: clock[0],
        sleep_seconds=lambda seconds: clock.__setitem__(0, clock[0] + round(seconds * 1e9)),
    )

    def slow_action():
        starts.append(clock[0])
        clock[0] += 2_000_000_000

    pacer.call(slow_action)
    pacer.call(lambda: starts.append(clock[0]))
    assert starts[1] - 2_000_000_000 >= 2_004_009


@pytest.mark.parametrize("rate", [0, 500, 2000, True])
def test_invalid_or_over_limit_rate_is_rejected(rate):
    with pytest.raises(ValueError):
        EmtActionPacerV1(actions_per_second=rate)
